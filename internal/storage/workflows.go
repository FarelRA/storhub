package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/go-git/go-git/v6/plumbing"
	gogitobject "github.com/go-git/go-git/v6/plumbing/object"
)

const metadataFilePath = ".storhub/metadata.json"

func (h *StorHub) uploadChunks(ctx context.Context, project, releaseTag, uploadURL string, planner *chunking.StreamingChunker, prepare func(remaining int) (string, string, error)) ([]ChunkInfo, error) {
	sink := h.newChunkSink(ctx, project, releaseTag, uploadURL, planner.NumChunks(), prepare)
	for i := 0; i < planner.NumChunks(); i++ {
		chunk, err := planner.GetChunk(i)
		if err != nil {
			return sink.results, err
		}
		if err := sink.put(chunk, chunk.Size(), chunk.Offset()); err != nil {
			return sink.results, err
		}
	}
	return sink.results, nil
}

// chunkSink uploads chunk payloads one at a time, accumulating ChunkInfos
// and rotating to a fresh release whenever the server reports the current
// one full. It is the single home of name-collision retries and
// release-full rotation; every upload loop (planner windows, reader
// windows, inline edits, rewritten ranges) funnels through put so a stale
// release choice can never strand an upload.
//
// Rotation terminates: each rotation invalidates the release cache and
// re-resolves against a fresh server list (with true counts near the
// ceiling), so a repeat pick means a concurrent writer filled it in the
// millisecond race window, and the next re-list observes that fill.
type chunkSink struct {
	hub        *StorHub
	ctx        context.Context
	project    string
	namer      *assetNamer
	total      int // planned chunk count, for remaining-slot computation
	results    []ChunkInfo
	releaseTag string
	uploadURL  string
	// prepare resolves a fresh release with room for the given remaining
	// chunk count. Invoked at most once per full release encountered.
	prepare func(remaining int) (tag, url string, err error)
}

func (h *StorHub) newChunkSink(ctx context.Context, project, releaseTag, uploadURL string, total int, prepare func(int) (string, string, error)) *chunkSink {
	return &chunkSink{
		hub: h, ctx: ctx, project: project,
		namer:      newAssetNamer(),
		total:      total,
		results:    make([]ChunkInfo, 0, total),
		releaseTag: releaseTag, uploadURL: uploadURL,
		prepare: prepare,
	}
}

// put uploads one chunk payload. The transport rewinds the reader per
// attempt. The returned ChunkInfo carries the release that actually holds
// the bytes, which may differ from the sink's initial target after a
// rotation. Partial results stay in s.results for the caller to compensate
// on error; put itself never deletes.
//
// Rotation is capped at maxReleaseRotations (central tunables, client.go):
// each rotation re-resolves against a fresh server list, so a repeat pick
// means a concurrent writer filled it in the race window — but a
// persistently-full set (many concurrent writers, no headroom) previously
// re-listed + re-uploaded forever. Exceeding the cap fails loudly instead.
func (s *chunkSink) put(reader io.ReadSeeker, size, offset int64) error {
	nameRetries := 0
	rotations := 0
	for {
		assetName, err := s.namer.Next()
		if err != nil {
			return err
		}
		assetID, err := s.hub.uploadAssetStreaming(s.ctx, s.project, s.releaseTag, s.uploadURL, assetName, reader, size)
		if err == nil {
			s.results = append(s.results, ChunkInfo{Size: size, Offset: offset, Release: s.releaseTag, AssetID: assetID, AssetOffset: 0})
			return nil
		}
		if isReleaseFull(err) {
			rotations++
			if rotations > maxReleaseRotations {
				return fmt.Errorf("upload chunk (offset %d): release %s full after %d rotations; concurrent writers hold every release at the %d-asset ceiling, retry the upload", offset, s.releaseTag, maxReleaseRotations, releaseAssetCap)
			}
			s.hub.debugf("upload release full, rotating release=%s uploaded=%d/%d rotation=%d", s.releaseTag, len(s.results), s.total, rotations)
			s.hub.invalidateReleaseCache(s.project)
			tag, url, err := s.prepare(s.total - len(s.results))
			if err != nil {
				return err
			}
			s.releaseTag, s.uploadURL = tag, url
			continue
		}
		if isAlreadyExists(err) {
			s.hub.debugf("upload chunk asset name collision, retry asset=%s", assetName)
			nameRetries++
			if nameRetries >= maxNameRetries {
				return fmt.Errorf("upload chunk failed after %d name retries", maxNameRetries)
			}
			continue
		}
		return fmt.Errorf("upload chunk (offset %d): %w", offset, err)
	}
}

func (h *StorHub) ensureRepo(ctx context.Context, project string) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	exists, err := h.repoExists(ctx, project)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := h.gh.CreateRepo(ctx, project, h.config.RepoDescription, !h.config.CreatePublicRepo, true); err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == 422 && isRepoAlreadyExistsError(apiErr) {
			exists, existsErr := h.repoExists(ctx, project)
			if existsErr == nil && exists {
				h.setRepoState(project, true)
				return nil
			}
		}
		return fmt.Errorf("ensure repository: %w", err)
	}
	h.setRepoState(project, true)
	return nil
}

func (h *StorHub) repoExists(ctx context.Context, project string) (bool, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return false, err
	}
	h.repoMu.RLock()
	exists, ok := h.repoState[project]
	h.repoMu.RUnlock()
	if ok {
		return exists, nil
	}
	exists, err := h.gh.RepoExists(ctx, h.owner, project)
	if err != nil {
		return false, err
	}
	h.setRepoState(project, exists)
	return exists, nil
}

func (h *StorHub) getAuthenticatedUser(ctx context.Context) (string, error) {
	return h.gh.GetAuthenticatedUser(ctx)
}

func (h *StorHub) loadRepoMetadata(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if meta, sha, ok := h.cachedRepoMetadata(project); ok {
		return meta, sha, nil
	}
	return h.loadRepoMetadataFresh(ctx, project)
}

func (h *StorHub) loadRepoMetadataReadonly(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if meta, sha, ok := h.cachedRepoMetadataReadonly(project); ok {
		return meta, sha, nil
	}
	return h.loadRepoMetadataFresh(ctx, project)
}

func (h *StorHub) loadRepoMetadataFresh(ctx context.Context, project string) (*RepoMetadata, string, error) {
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "load metadata start")
	data, sha, found, err := h.readIndexHead(ctx, project)
	if err != nil {
		logging.Warn(h.projectLogger(project), "load metadata failed", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, "", err
	}
	if !found {
		exists, existsErr := h.repoExists(ctx, project)
		if existsErr != nil {
			return nil, "", existsErr
		}
		if !exists {
			return nil, "", shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		// Brand-new (or wiped) project: start on the split layout (version 5).
		m := NewRepoMetadata(project)
		pendingOps := h.journalReplayForLoad(project, m)
		h.storeRepoMetadata(project, m, "", pendingOps, 0)
		// The returned tree may be published directly (hydration swaps it
		// into pm.meta); journal replay invalidates the indexes, so rebuild
		// before handing it out.
		m.RebuildIndexes()
		logging.Info(h.projectLogger(project), "load metadata initialized empty repository metadata", "elapsed", h.config.Now().UTC().Sub(started))
		return m, "", nil
	}
	m, objectCount, err := h.loadIndexTree(ctx, project, data)
	if err != nil {
		logging.Warn(h.projectLogger(project), "load metadata failed", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, "", err
	}
	pendingOps := h.journalReplayForLoad(project, m)
	// NOTE: sha is the CAS token for the document that was found (manifest
	// blob sha for a split project, metadata blob sha for a legacy one). It
	// must never be consumed as a git ref: pins capture chunk layouts instead.
	h.storeRepoMetadata(project, m, sha, pendingOps, objectCount)
	m.RebuildIndexes()
	logging.Debug(h.projectLogger(project), "load metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "sha", shortSHA(sha), "bytes", len(data), "split", m.IsSplit())
	return m, sha, nil
}

func (h *StorHub) commitRepoMetadata(ctx context.Context, project string, metadata *RepoMetadata, previousSHA, message string) (string, string, error) {
	started := h.config.Now().UTC()
	logging.Info(h.projectLogger(project), "commit metadata start", "message", message, "previous_sha", shortSHA(previousSHA))
	if err := h.ensureOwner(ctx); err != nil {
		return "", "", err
	}
	metadata.Normalize(project, h.config.Now().Unix())
	metadata.LastMod = h.config.Now().Unix()
	metadata.RecomputeStats()
	if err := metadata.Validate(); err != nil {
		return "", "", fmt.Errorf("validate metadata: %w", err)
	}
	// The split index (version 5) is the only write layout: rollback and
	// cleanup republish the manifest, re-pointing the index at this tree's
	// objects (rollback-as-revert, no force-push). A legacy project's first
	// such commit also migrates it to the split layout.
	metadata.MarkSplit()
	var objectCount uint64
	if pm := h.lookupProjectMeta(project); pm != nil {
		pm.mu.RLock()
		objectCount = pm.objectCount
		pm.mu.RUnlock()
	}
	commitSHA, contentSHA, newCount, err := h.publishIndex(ctx, project, metadata, previousSHA, message, objectCount)
	if err != nil {
		logging.Error(h.projectLogger(project), "commit metadata failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return "", "", err
	}
	h.storeRepoMetadata(project, metadata, contentSHA, nil, newCount)
	h.clearSizeCapped(project)
	logging.Info(h.projectLogger(project), "commit metadata complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "commit_sha", shortSHA(commitSHA), "content_sha", shortSHA(contentSHA), "objects", newCount)
	return commitSHA, contentSHA, nil
}

func (h *StorHub) listMetadataRevisions(ctx context.Context, project string) ([]MetadataRevision, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	path, err := h.activeIndexPath(ctx, project)
	if err != nil {
		return nil, err
	}
	if repo := h.getGitRepo(project); repo != nil {
		revisions, err := repo.listFileCommits(ctx, path)
		if err != nil {
			// Infrastructure failures must not masquerade as "project not
			// found"; propagate them so callers can retry or report.
			return nil, fmt.Errorf("list metadata revisions: %w", err)
		}
		if len(revisions) == 0 {
			return nil, shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		return revisions, nil
	}
	commits, err := h.gh.ListFileCommits(ctx, h.owner, project, path)
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil, shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		return nil, err
	}
	revisions := make([]MetadataRevision, 0, len(commits))
	for _, commit := range commits {
		revisions = append(revisions, MetadataRevision{CommitSHA: commit.SHA, Message: commit.Message, CommittedAt: commit.CommittedAt.Unix()})
	}
	return revisions, nil
}

// activeIndexPath returns the repo path carrying the project's current index
// history: the split manifest when the project is (or will be) version 5,
// else the legacy metadata blob. Revision listing and history walks must
// follow the active layout or they see an empty history for a split project.
func (h *StorHub) activeIndexPath(ctx context.Context, project string) (string, error) {
	if pm := h.lookupProjectMeta(project); pm != nil {
		pm.mu.RLock()
		split := pm.meta.IsSplit()
		pm.mu.RUnlock()
		if split {
			return indexFilePath, nil
		}
	}
	data, _, found, err := h.readIndexHead(ctx, project)
	if err != nil {
		return "", err
	}
	if found && meta.IsManifest(data) {
		return indexFilePath, nil
	}
	return metadataFilePath, nil
}

func (h *StorHub) getMetadataRevision(ctx context.Context, project, commitSHA string) (*RepoMetadata, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	// Detect the layout AT THIS REVISION by shape: a split-era commit carries
	// the manifest, a legacy-era commit the metadata blob. Reading the
	// manifest first makes a legacy revision still readable across the
	// migration boundary (the grace window) and a split revision load objects.
	if data, found, err := h.readIndexRevision(ctx, project, commitSHA); err != nil {
		return nil, err
	} else if found {
		m, _, err := h.loadIndexTreeAtRef(ctx, project, commitSHA, data)
		if err != nil {
			return nil, fmt.Errorf("parse metadata revision: %w", err)
		}
		m.Normalize(project, h.config.Now().Unix())
		if err := m.Validate(); err != nil {
			return nil, fmt.Errorf("validate metadata revision: %w", err)
		}
		return m, nil
	}
	return nil, shfs.NotFound(fmt.Sprintf("metadata revision %s", shortSHA(commitSHA)))
}

// readIndexRevision fetches the manifest or metadata blob at a specific commit
// SHA. Callers distinguish the layout with meta.IsManifest(data). Dispatches
// through readIndexDoc (index.go), the single git-vs-REST point.
func (h *StorHub) readIndexRevision(ctx context.Context, project, commitSHA string) ([]byte, bool, error) {
	if d, _, ok, rerr := h.readIndexDoc(ctx, project, indexFilePath, commitSHA); rerr != nil {
		return nil, false, rerr
	} else if ok {
		return d, true, nil
	}
	d, _, ok, rerr := h.readIndexDoc(ctx, project, metadataFilePath, commitSHA)
	if rerr != nil {
		return nil, false, rerr
	}
	return d, ok, nil
}

// loadIndexTreeAtRef materializes a revision's tree, detecting the layout by
// shape and fetching split objects at the same ref so a historical manifest
// resolves its historical objects.
func (h *StorHub) loadIndexTreeAtRef(ctx context.Context, project, ref string, data []byte) (*RepoMetadata, uint64, error) {
	if !meta.IsManifest(data) {
		m := NewRepoMetadata(project)
		if err := m.FromJSON(data); err != nil {
			return nil, 0, err
		}
		return m, 0, nil
	}
	manifest, err := meta.ParseManifest(data)
	if err != nil {
		return nil, 0, err
	}
	fetched := func(sha string) ([]byte, error) { return h.fetchObjectAtRef(ctx, project, ref, sha) }
	loaded, err := meta.LoadTree(manifest, fetched)
	if err != nil {
		return nil, 0, err
	}
	loaded.Project = project
	return loaded, manifest.ObjectCount, nil
}

// fetchObjectAtRef loads one index object pinned to a commit SHA (cache is
// content-addressed and layout-agnostic, so it serves any ref). Backend
// bytes come from readObjectBytes (objects.go); the ref-pinned error
// wording is preserved.
func (h *StorHub) fetchObjectAtRef(ctx context.Context, project, ref, sha string) ([]byte, error) {
	cache := h.objectCacheFor(project)
	if data, ok := cache.get(sha); ok {
		return data, nil
	}
	data, err := h.readObjectBytes(ctx, project, ref, sha)
	if err != nil {
		return nil, fmt.Errorf("fetch object %s at %s: %w", shortSHA(sha), shortSHA(ref), err)
	}
	if meta.ObjectSHA(data) != sha {
		return nil, fmt.Errorf("object %s failed content verification at %s", shortSHA(sha), shortSHA(ref))
	}
	cache.put(sha, data)
	return data, nil
}

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
// serves the revert path (audit 24).
//
// Range geometry is enforced, not just membership (audit 18): a reverted
// chunk pointing at a live asset with an out-of-range window previously
// committed successfully and failed later as a 416 at read time. Every
// chunk must satisfy AssetOffset >= 0 and AssetOffset+Size <= asset Size.
// Sizes ride the same targeted listing as membership (embedded view when
// trusted, paginated ListReleaseAssets otherwise).
//
// The release list itself stays a fresh (uncached) listReleases: the point
// of the re-checks around the commit is to catch deletions that landed
// after the previous check, which a TTL cache would hide.
// NEEDS-INTEGRATION(13): a targeted GET /releases/assets/{id} would replace
// the per-release fallback entirely; the ghapi client exposes no such
// method today, so the fallback paginates ListReleaseAssets instead.
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
			if _, err := metadata.EnsureRelease(r.TagName, h.config.Now().Unix()); err != nil {
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
				if _, err := metadata.EnsureRelease(r.TagName, h.config.Now().Unix()); err != nil {
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
	if _, err := metadata.EnsureRelease(tag, h.config.Now().Unix()); err != nil {
		return "", "", err
	}
	return tag, release.UploadURL, nil
}

// ensureChunkReleases registers every release holding new chunks in the
// authoritative metadata so PurgeUntracked cannot delete live data and
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

func (h *StorHub) deleteRepo(ctx context.Context, project string) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	if err := h.gh.DeleteRepo(ctx, h.owner, project); err != nil {
		return err
	}
	// Stop the commit loop + drop the metadata cache entry (cascading
	// residue if it was resident), then cascade unconditionally: a deleted
	// repo must leave no gitRepos/objCaches/repoState/releaseCache entry
	// even if it was never resident in metaCache. releaseProjectResidue is
	// idempotent, so the double call is safe.
	h.invalidateRepoMetadata(project)
	h.releaseProjectResidue(project)
	return nil
}

func (h *StorHub) uploadAssetStreaming(ctx context.Context, project, releaseTag, uploadURL, assetName string, reader io.ReadSeeker, size int64) (int64, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return 0, err
	}
	assetID, err := h.gh.UploadAsset(ctx, uploadURL, assetName, reader, size)
	if err != nil {
		return 0, err
	}
	h.bumpCachedReleaseAssetCount(project, releaseTag, assetID)
	return assetID, nil
}

func (h *StorHub) downloadAssetStream(ctx context.Context, project string, assetID, start, end int64) (io.ReadCloser, int64, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, 0, err
	}
	return h.gh.DownloadAssetStream(ctx, h.owner, project, assetID, start, end)
}

func (h *StorHub) fillAssetRange(ctx context.Context, project string, chunk ChunkInfo, dst []byte) error {
	if int64(len(dst)) != chunk.Size {
		return fmt.Errorf("asset range size mismatch: expected buffer %d, got %d", chunk.Size, len(dst))
	}
	return h.withAssetRangeReader(ctx, project, chunk, func(reader io.Reader) error {
		read, err := io.ReadFull(reader, dst)
		if err != nil {
			return err
		}
		if int64(read) != chunk.Size {
			return fmt.Errorf("asset range size mismatch: expected %d, got %d", chunk.Size, read)
		}
		return nil
	})
}

func (h *StorHub) withAssetRangeReader(ctx context.Context, project string, chunk ChunkInfo, fn func(io.Reader) error) error {
	// Single-attempt closure over the open→read→close sequence; withRetry
	// (retry.go) owns the backoff/sleep shape shared with purgeRetry and
	// downloadChunkWithRetry. Both open and read errors gate on
	// isRetryableDownloadError, preserving the old two-phase semantics.
	attempt := func() error {
		reader, _, err := h.downloadAssetStream(ctx, project, chunk.AssetID, chunk.AssetOffset, chunk.AssetOffset+chunk.Size-1)
		if err != nil {
			return err
		}
		err = fn(reader)
		if closeErr := reader.Close(); err == nil {
			err = closeErr
		}
		return err
	}
	// The old "exhausted retries" fallthrough was unreachable (the last
	// attempt returns its error directly); withRetry preserves that by
	// returning the final attempt's error.
	return h.withRetry(ctx, "asset-range", h.config.MaxRetries+1, isRetryableDownloadError, attempt)
}

func (h *StorHub) setRepoState(project string, exists bool) {
	h.repoMu.Lock()
	defer h.repoMu.Unlock()
	h.repoState[project] = exists
}

// forgetRepoState drops the cached existence bool for a project that no
// longer exists, so a deleted project's entry cannot linger forever.
func (h *StorHub) forgetRepoState(project string) {
	h.repoMu.Lock()
	defer h.repoMu.Unlock()
	delete(h.repoState, project)
}

// releaseProjectResidue tears down every per-project map entry besides the
// metadata cache: the git mirror handle (closing the *git.Repository and
// removing its claimed cache dir), the object cache handle, the repo-state
// bool, and the release list. Eviction and project deletion must cascade
// here or a long-lived server leaks one heavy entry per create/delete churn.
//
// It takes each map's own lock and never metaMu, so callers may hold metaMu
// (eviction) or no lock at all (deleteRepo) without inverting lock order.
func (h *StorHub) releaseProjectResidue(project string) {
	h.gitMu.Lock()
	repo := h.gitRepos[project]
	delete(h.gitRepos, project)
	h.gitMu.Unlock()
	if repo != nil {
		if err := repo.release(true); err != nil {
			logging.Warn(h.projectLogger(project), "release git mirror on eviction failed", "err", err)
		}
	}
	// Free the in-memory handle first, then remove its disk dir best-effort
	// (audit 17/31): evicted/deleted projects previously accumulated
	// objects/<owner__proj>/ dirs until the next process-start
	// ReapOrphanedCaches, unbounded across churn despite the per-project
	// count+byte caps. I/O runs after the map lock, matching the git
	// mirror above. A concurrent objectCacheFor may recreate the dir;
	// RemoveAll on a live path is still safe (cache misses refetch).
	h.objCacheMu.Lock()
	objCache := h.objCaches[project]
	delete(h.objCaches, project)
	h.objCacheMu.Unlock()
	if objCache != nil {
		// Best-effort: eviction must not fail on a wedged disk.
		_ = os.RemoveAll(objCache.dir)
	}
	h.forgetRepoState(project)
	h.invalidateReleaseCache(project)
	// Drop the cached per-project logger too, so project churn cannot grow
	// the logger cache without bound.
	h.loggers.Delete(project)
	// Close the op-journal handle/file: eviction must not leak one open FD
	// plus map entries per churned project until Shutdown.
	h.closeProjectJournal(project)
}

// cachedMeta is the single home of the shared-pointer cache read:
// requireHydrated=false serves any resident entry (loadRepoMetadata path),
// requireHydrated=true misses on unhydrated entries whose EMPTY tree is not
// remote truth (loadRepoMetadataReadonly path). cachedRepoMetadata and
// cachedRepoMetadataReadonly are thin wrappers (verbs.go calls both).
func (h *StorHub) cachedMeta(project string, requireHydrated bool) (*RepoMetadata, string, bool) {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return nil, "", false
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if requireHydrated && !pm.hydrated {
		// An unhydrated entry carries an EMPTY tree that is not remote
		// truth; serving it lets a cold-cache mutation commit over real
		// remote state. Miss instead: the caller falls through to a
		// fresh load.
		return nil, "", false
	}
	// Share the immutable current pointer: published trees are never
	// mutated in place (cloneForWrite/publishTreeLocked discipline), so a
	// reader holding the pointer after releasing the lock sees a frozen
	// snapshot. Cloning here was a full deep copy per read (O(tree)
	// allocations); the indexes were built at store/publish time and
	// stay valid.
	return pm.meta, pm.sha, true
}

func (h *StorHub) cachedRepoMetadata(project string) (*RepoMetadata, string, bool) {
	return h.cachedMeta(project, false)
}

func (h *StorHub) cachedRepoMetadataReadonly(project string) (*RepoMetadata, string, bool) {
	return h.cachedMeta(project, true)
}

// cloneForWrite returns a private, mutable copy of a published metadata tree.
//
// Published trees (pm.meta) are shared with lock-free readers, so no code
// may mutate one in place. Mutation sites take a copy here, apply their
// changes, and publish with publishTreeLocked. The copy is the metadata
// engine's Clone: the four stored maps are copied while their immutable
// entry VALUES are shared, and a clean derived index is shared read-only, so
// the copy is cheap. The first tracked mutation drops the copy to a private
// dirty derived state (the engine's owner/mapsShared guard), never writing
// into maps the published tree still reads.
func cloneForWrite(m *RepoMetadata) *RepoMetadata {
	return m.Clone()
}

// cowTree is the historical spelling of cloneForWrite, retained for
// callers outside this slice (commit.go, verbs.go, runtime.go): new code
// uses cloneForWrite.
func cowTree(m *RepoMetadata) *RepoMetadata {
	return cloneForWrite(m)
}

// publishTreeLocked swaps a mutated COW copy in as the new shared truth.
// It rebuilds the derived indexes so the published tree is clean and
// exclusively owned: a lock-free reader's index read (NLink/DirNLink/
// FindFilesByInode) then hits a fresh index and never triggers a rebuild
// write that would race other readers. Caller holds pm.mu for writing.
func publishTreeLocked(pm *projectMetadata, tree *RepoMetadata) {
	tree.RebuildIndexes()
	pm.meta = tree
}

// storeRepoMetadata caches remote truth for a project. The tree's own version
// records its layout (split vs legacy) and objectCount carries the running
// hint. When pendingOps is non-nil (a crash-recovery journal replayed onto the
// loaded state), the ops become the project's pending stack: dirty stays set
// and the journal is kept until the next commit lands them.
//
// The apply-back is version-guarded: when the entry still carries local
// work (dirty, or a non-empty op stack), remote truth must NOT clobber it -
// discarding acknowledged mutations, the pending stack, and the crash journal
// here is data loss. The loader still receives the freshly read values; the
// local tree keeps its stale CAS token, so the next commit conflicts and
// rebases onto the remote state instead of silently overwriting it. Shared
// state that is replaced bumps pm.version so an in-flight transaction's
// version guard observes the swap.
func (h *StorHub) storeRepoMetadata(project string, meta *RepoMetadata, sha string, pendingOps []Op, objectCount uint64) {
	clone := meta.Clone()
	clone.RebuildIndexes()

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if len(pendingOps) > 0 {
		pm.meta = clone
		pm.sha = sha
		pm.hydrated = true
		pm.objectCount = objectCount
		// The rebase baseline moves to the freshly loaded state.
		pm.baseTree = clone
		pm.opStack.clear()
		for _, op := range pendingOps {
			pm.opStack.append(op)
		}
		pm.dirty = true
		pm.version++
		pm.mu.Unlock()
		return
	}
	if pm.dirty || len(pm.opStack.ops) > 0 {
		// Pending local work outranks the remote snapshot: keep the tree,
		// the stack, the dirty flag, and the journal exactly as they are.
		pm.hydrated = true
		pm.mu.Unlock()
		return
	}
	pm.meta = clone
	pm.sha = sha
	pm.hydrated = true
	pm.objectCount = objectCount
	// The rebase baseline moves to the freshly loaded state.
	pm.baseTree = clone
	pm.dirty = false // Just stored, so not dirty
	pm.opStack.clear()
	pm.version++
	pm.mu.Unlock()
	h.journalRewrite(project, nil)
}

// journalReplayForLoad replays the crash-recovery journal onto a freshly
// loaded remote state, but only for a cache entry that has never been
// hydrated (cold start). A hydrated project with pending ops is in its
// normal commit cycle; replaying there would resurrect discarded state.
// Returns the replayed ops for the pending stack, or nil.
func (h *StorHub) journalReplayForLoad(project string, meta *RepoMetadata) []Op {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if ok {
		pm.mu.RLock()
		cold := !pm.hydrated
		pm.mu.RUnlock()
		if !cold {
			return nil
		}
	}
	ops := h.journalRead(project)
	if len(ops) == 0 {
		return nil
	}
	ops = dropSupersededOps(project, h.projectLogger(project), meta, ops)
	if len(ops) == 0 {
		return nil
	}
	if err := applyOps(meta, ops); err != nil {
		logging.Error(h.projectLogger(project), "op journal replay failed; pending ops discarded", "err", err)
		h.journalRewrite(project, nil)
		return nil
	}
	meta.Normalize(project, h.config.Now().Unix())
	meta.RecomputeStats()
	logging.Info(h.projectLogger(project), "op journal replayed onto remote state", "ops", len(ops))
	return ops
}

// dropSupersededOps removes journal ops that a full-state assertion must not
// re-apply over newer remote state: a journal rewrite that failed after
// a commit leaves committed ops in the file, and a cold replay would then
// assert them over entries the world has since moved past. An op whose
// timestamp predates the change time of the entry it targets is stale and is
// dropped; renames consult renameSupersededByUpstream (rebase.go: a stale
// rename would clobber a newer target unconditionally at apply time);
// catalog ops without a resolvable target are kept - replay applies them
// defensively.
func dropSupersededOps(project string, logger *slog.Logger, meta *RepoMetadata, ops []Op) []Op {
	kept := make([]Op, 0, len(ops))
	for _, op := range ops {
		path := opPath(op)
		var changedAt int64
		var exists bool
		switch {
		case op.Type == OpMkdir || (op.Type == OpSetattr && op.File == nil && op.Dir != nil):
			if d, ok := meta.Dirs()[path]; ok {
				changedAt, exists = max(d.ChangedAt, d.ModifiedAt), true
			}
		case isStateClass(op.Type) || isDeleteClass(op.Type):
			if f, ok := meta.Files()[path]; ok {
				changedAt, exists = f.ChangedAt, true
			}
		default:
			// Renames overwrite their target unconditionally at apply
			// time, so a stale one is destructive (unlike single-target
			// state ops, which the timestamp guard already drops).
			if op.Type == OpRename && renameSupersededByUpstream(meta, op) {
				to := ""
				if len(op.Paths) == 2 {
					to = op.Paths[1]
				}
				logging.Warn(logger, "op journal rename superseded by newer remote state; skipped",
					"project", project, "op", op.Type, "to", to, "op_ts", op.Timestamp)
				continue
			}
			kept = append(kept, op)
			continue
		}
		if exists && changedAt > op.Timestamp {
			logging.Warn(logger, "op journal entry superseded by newer remote state; skipped",
				"project", project, "op", op.Type, "path", path, "op_ts", op.Timestamp, "remote_ts", changedAt)
			continue
		}
		kept = append(kept, op)
	}
	return kept
}

func (h *StorHub) invalidateRepoMetadata(project string) {
	h.metaMu.Lock()
	pm, ok := h.metaCache[project]
	if ok {
		delete(h.metaCache, project)
		// Teardown mirrors eviction: without stopping the loop, deleting
		// the cache entry would leak a live commit goroutine per deleted
		// repo (and a duplicate loop if the project returns). metaMu→pm.mu
		// ordering matches getOrCreateProjectMeta and eviction.
		pm.mu.Lock()
		pm.stopped = true
		close(pm.stopCh)
		pm.mu.Unlock()
	}
	h.metaMu.Unlock()
	if ok {
		// Cascade outside metaMu: the git mirror release does directory
		// I/O and must not stall cache readers.
		h.releaseProjectResidue(project)
	}
}

func isMetadataNotFound(err error) bool {
	if err == nil {
		return false
	}
	// The git backend surfaces a missing metadata file as an fs-not-exist
	// error chain (readFileHead) or a go-git file-not-found (readFileRef);
	// match sentinels, never message text.
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, shfs.ErrNotFound) {
		return true
	}
	if errors.Is(err, gogitobject.ErrFileNotFound) || errors.Is(err, plumbing.ErrObjectNotFound) {
		return true
	}
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) && apiErr.NotFound() {
		return true
	}
	return false
}

func isRepoAlreadyExistsError(apiErr *ghapi.APIError) bool {
	if apiErr == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(apiErr.Message))
	return strings.Contains(message, "already exists") || strings.Contains(message, "name already exists")
}
