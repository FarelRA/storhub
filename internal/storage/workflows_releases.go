package storage

import (
	"context"
	"errors"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"net/http"
	"sort"
)

// validateSnapshotRefs checks a snapshot against live server state before
// a rollback/revert commits it. Structural validation is total; asset
// existence is checked LAZILY and TARGETED: only releases the snapshot
// references are examined, and a release's full asset list is paginated
// only when its embedded view is untrustworthy (at/above the truncation
// danger band, or an expected ID is missing from it). The old form built
// an index over every asset of every release - O(total assets) transient
// memory per call, three calls per rollback.
//
// op names the caller ("rollback"/"revert") and prefixes every error so a
// shared validator does not misattribute failures to rollback when it also
// serves the revert path (revert-path refetch).
//
// Range geometry is enforced, not just membership (range-geometry enforcement): a reverted
// chunk pointing at a live asset with an out-of-range window previously
// committed successfully and failed later as a 416 at read time. Every
// chunk must satisfy AssetOffset >= 0 and AssetOffset+Size <= asset Size.
// Sizes ride the same targeted listing as membership (embedded view when
// trusted, paginated ListReleaseAssets otherwise).
//
// The release list itself stays a fresh (uncached) listReleases: the point
// of the re-checks around the commit is to catch deletions that landed
// after the previous check, which a TTL cache would hide.
// A targeted GET /releases/assets/{id} would replace the per-release
// fallback entirely; the ghapi client exposes no such method, so the
// fallback paginates ListReleaseAssets instead.
func (h *StorHub) validateSnapshotRefs(ctx context.Context, project, op string, metadata *RepoMetadata) error {
	// Structural validation first - chunk/file size consistency
	// (chunks beyond EOF, negative geometry, dangling references,
	// totals) is verified here, not assumed from elsewhere.
	if err := metadata.Validate(); err != nil {
		return fmt.Errorf("%s metadata failed validation: %w", op, err)
	}
	releases, err := h.listReleases(ctx, project)
	if err != nil {
		return err
	}
	releaseIndex := make(map[string]ghapi.Release, len(releases))
	for _, release := range releases {
		releaseIndex[release.TagName] = release
	}
	// Collect the asset IDs the snapshot actually references, per release,
	// plus every chunk's (assetID -> window end) for the geometry check.
	type chunkWindow struct {
		assetID int64
		end     int64 // AssetOffset+Size; AssetOffset<0 handled by Validate above
	}
	referenced := make(map[string]map[int64]struct{})
	windows := make(map[string][]chunkWindow)
	for path, file := range metadata.Files() {
		for _, chunkName := range file.Chunks {
			// A dangling chunk reference must fail validation outright.
			// Skipping it here would bless a snapshot whose bytes cannot be
			// downloaded after commit.
			chunk, ok := metadata.Chunks()[chunkName]
			if !ok {
				return fmt.Errorf("%s metadata references missing chunk %d (file %s)", op, chunkName, path)
			}
			// Structural size/offset sanity beyond Validate(): negative
			// geometry can never address real bytes.
			if chunk.Size < 0 || chunk.Offset < 0 || chunk.AssetOffset < 0 {
				return fmt.Errorf("%s metadata chunk %d has invalid geometry (offset %d, size %d, assetOffset %d)", op, chunkName, chunk.Offset, chunk.Size, chunk.AssetOffset)
			}
			ids := referenced[chunk.Release]
			if ids == nil {
				ids = make(map[int64]struct{})
				referenced[chunk.Release] = ids
			}
			ids[chunk.AssetID] = struct{}{}
			windows[chunk.Release] = append(windows[chunk.Release], chunkWindow{assetID: chunk.AssetID, end: chunk.AssetOffset + chunk.Size})
		}
	}
	for tag, ids := range referenced {
		release, ok := releaseIndex[tag]
		if !ok {
			return fmt.Errorf("%s metadata references missing release: %s", op, tag)
		}
		sizes := make(map[int64]int64, len(release.Assets))
		for _, asset := range release.Assets {
			sizes[asset.ID] = asset.Size
		}
		missing := false
		for id := range ids {
			if _, ok := sizes[id]; !ok {
				missing = true
				break
			}
		}
		// The embedded asset view is truncated near the ceiling, so a
		// missing ID (or a release already in the danger band) proves
		// nothing: resolve the true membership with one targeted list.
		if missing || len(release.Assets) >= embeddedAssetTrustLimit {
			assets, err := h.gh.ListReleaseAssets(ctx, h.owner, project, release.ID)
			if err != nil {
				return fmt.Errorf("verify assets of release %s: %w", tag, err)
			}
			sizes = make(map[int64]int64, len(assets))
			for _, asset := range assets {
				sizes[asset.ID] = asset.Size
			}
		}
		for id := range ids {
			if _, ok := sizes[id]; !ok {
				return fmt.Errorf("%s metadata references missing asset %d in release %s", op, id, tag)
			}
		}
		// Hard reject out-of-range windows against the resolved sizes.
		for _, w := range windows[tag] {
			size, ok := sizes[w.assetID]
			if !ok {
				continue // already reported as missing above
			}
			if w.end > size {
				return fmt.Errorf("%s metadata chunk window [..%d) exceeds asset %d size %d in release %s", op, w.end, w.assetID, size, tag)
			}
		}
	}
	return nil
}

// validateMetadataSnapshot is the rollback-facing entry point, retained for
// callers outside this slice (verbs.go calls it five times): it validates
// with the "rollback" op prefix.
func (h *StorHub) validateMetadataSnapshot(ctx context.Context, project string, metadata *RepoMetadata) error {
	return h.validateSnapshotRefs(ctx, project, "rollback", metadata)
}

// sortReleasesOldestFirst orders releases by numeric v tag ascending so
// uploads pack elders full before opening new headroom. Tags without a
// numeric suffix keep listed order after all numeric ones.
func sortReleasesOldestFirst(releases []ghapi.Release) []ghapi.Release {
	out := append([]ghapi.Release(nil), releases...)
	sort.SliceStable(out, func(i, j int) bool {
		ni, oki := meta.ParseNumericReleaseTag(out[i].TagName)
		nj, okj := meta.ParseNumericReleaseTag(out[j].TagName)
		switch {
		case oki && okj:
			return ni < nj
		case oki:
			return true
		case okj:
			return false
		default:
			return false
		}
	})
	return out
}

func (h *StorHub) getOrCreateUploadRelease(ctx context.Context, project string, metadata *RepoMetadata, requiredSlots int) (string, string, error) {
	releases, err := h.listReleasesCached(ctx, project)
	if err != nil {
		return "", "", err
	}
	// Oldest-first: pack elders full before opening new headroom, and
	// consume curated empty releases instead of stranding them. GitHub
	// lists newest-first (and the mock sorts tags as strings, where
	// "v10" < "v9"), so the preference is enforced here and is
	// independent of listing order. Non-numeric tags sort last, stable.
	releases = sortReleasesOldestFirst(releases)
	if requiredSlots <= 0 {
		if len(releases) > 0 {
			r := releases[0]
			if _, err := metadata.EnsureRelease(r.TagName, h.config.Now().UnixNano()); err != nil {
				return "", "", err
			}
			return r.TagName, r.UploadURL, nil
		}
	} else {
		for _, r := range releases {
			count, err := h.releaseAssetCount(ctx, project, r)
			if err != nil {
				return "", "", err
			}
			if count+requiredSlots <= releaseAssetCap {
				if _, err := metadata.EnsureRelease(r.TagName, h.config.Now().UnixNano()); err != nil {
					return "", "", err
				}
				return r.TagName, r.UploadURL, nil
			}
		}
	}
	tag, err := h.getNextReleaseTag(metadata, releases)
	if err != nil {
		return "", "", err
	}
	release, err := h.createRelease(ctx, project, tag, "StorHub storage "+tag)
	if err != nil {
		return "", "", err
	}
	if _, err := metadata.EnsureRelease(tag, h.config.Now().UnixNano()); err != nil {
		return "", "", err
	}
	return tag, release.UploadURL, nil
}

// ensureChunkReleases registers every release holding new chunks in the
// authoritative metadata so purge cannot delete live data and
// rollback validation can resolve chunk references. Rotation may spread one
// file's chunks across releases; ensuring only the originally targeted tag
// would strand the rotated chunks.
func ensureChunkReleases(meta *RepoMetadata, chunks []ChunkInfo, now int64) error {
	seen := make(map[string]struct{}, len(chunks))
	for _, c := range chunks {
		if _, ok := seen[c.Release]; ok {
			continue
		}
		seen[c.Release] = struct{}{}
		// A chunk with no release tag is not in any release: ensuring
		// "" would only create a junk catalog entry (EnsureRelease
		// rejects it). Callers pass release-tagged chunks; the skip
		// keeps one degenerate record from failing the whole mutation.
		if c.Release == "" {
			continue
		}
		if _, err := meta.EnsureRelease(c.Release, now); err != nil {
			return err
		}
	}
	return nil
}

// embeddedAssetTrustLimit bounds how far the asset array embedded in a
// release object may be trusted for capacity decisions. GitHub truncates it
// near the ceiling (worst observed skew: 57 assets on storhub-web v14), so
// at or above this limit the true count is resolved through the paginated
// ListReleaseAssets endpoint instead.
const embeddedAssetTrustLimit = 900

// releaseAssetCount returns the number of assets in a release for capacity
// decisions: the embedded count when safely below the ceiling, the true
// paginated count inside the danger band.
func (h *StorHub) releaseAssetCount(ctx context.Context, project string, r ghapi.Release) (int, error) {
	// A cached entry carrying upload placeholders (legacy ID -1 bumps)
	// cannot be trusted for picker math: the embedded list may be a
	// truncated view with local guesses appended, arbitrarily far from
	// server truth. Resolve the real count instead.
	for _, a := range r.Assets {
		if a.ID < 0 {
			return h.trueReleaseAssetCount(ctx, project, r)
		}
	}
	if len(r.Assets) < embeddedAssetTrustLimit {
		return len(r.Assets), nil
	}
	return h.trueReleaseAssetCount(ctx, project, r)
}

// trueReleaseAssetCount resolves a release's asset count through the
// paginated ListReleaseAssets endpoint instead of the embedded array.
func (h *StorHub) trueReleaseAssetCount(ctx context.Context, project string, r ghapi.Release) (int, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return 0, err
	}
	assets, err := h.gh.ListReleaseAssets(ctx, h.owner, project, r.ID)
	if err != nil {
		return 0, err
	}
	return len(assets), nil
}

func (h *StorHub) getNextReleaseTag(metadata *RepoMetadata, releases []ghapi.Release) (string, error) {
	maxVersion := 0
	for tag := range metadata.Releases() {
		if n, ok := meta.ParseNumericReleaseTag(tag); ok && n > maxVersion {
			maxVersion = n
		}
	}
	for _, release := range releases {
		if n, ok := meta.ParseNumericReleaseTag(release.TagName); ok && n > maxVersion {
			maxVersion = n
		}
	}
	return fmt.Sprintf("v%d", maxVersion+1), nil
}

func (h *StorHub) createRelease(ctx context.Context, project, tag, name string) (*ghapi.Release, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	release, err := h.gh.CreateRelease(ctx, h.owner, project, tag, name)
	if err != nil {
		// Double-create race: a rival writer created this tag between our
		// list and our create (suspected once in prod: v17/v18's shared
		// timestamp). Reuse the rival's release instead of failing.
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusUnprocessableEntity && isAlreadyExists(apiErr) {
			existing, getErr := h.gh.GetReleaseByTag(ctx, h.owner, project, tag)
			if getErr != nil {
				return nil, fmt.Errorf("create release %s lost race, reuse failed: %w", tag, getErr)
			}
			h.addReleaseToCache(project, existing)
			return existing, nil
		}
		return nil, err
	}
	h.addReleaseToCache(project, release)
	return release, nil
}

func (h *StorHub) listReleases(ctx context.Context, project string) ([]ghapi.Release, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	releases, err := h.gh.ListReleases(ctx, h.owner, project)
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil, shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		return nil, err
	}
	h.setCachedReleases(project, releases)
	return releases, nil
}

func (h *StorHub) listReleasesCached(ctx context.Context, project string) ([]ghapi.Release, error) {
	// The shared view is read-only for the picker (it only reads tag/URL/ID
	// and asset IDs); no per-read deep copy of every asset array.
	if cached, ok := h.cachedReleasesView(project); ok {
		return cached, nil
	}
	return h.listReleases(ctx, project)
}

func (h *StorHub) deleteReleaseByID(ctx context.Context, project string, releaseID int64) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	err := h.gh.DeleteReleaseByID(ctx, h.owner, project, releaseID)
	if err == nil {
		h.invalidateReleaseCache(project)
	}
	return err
}

func (h *StorHub) deleteAssetByID(ctx context.Context, project string, assetID int64) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	err := h.gh.DeleteAssetByID(ctx, h.owner, project, assetID)
	if err == nil {
		h.invalidateReleaseCache(project)
	}
	return err
}
