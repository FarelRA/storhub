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
func (s *chunkSink) put(reader io.ReadSeeker, size, offset int64) error {
	const maxNameRetries = 5
	nameRetries := 0
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
			s.hub.debugf("upload release full, rotating release=%s uploaded=%d/%d", s.releaseTag, len(s.results), s.total)
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
	h.repoMu.Lock()
	exists, ok := h.repoState[project]
	h.repoMu.Unlock()
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
		h.storeRepoMetadata(project, *m, "", pendingOps, 0)
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
	h.storeRepoMetadata(project, *m, sha, pendingOps, objectCount)
	m.RebuildIndexes()
	logging.Debug(h.projectLogger(project), "load metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "sha", shortSHA(sha), "bytes", len(data), "split", m.IsSplit())
	return m, sha, nil
}

func (h *StorHub) commitRepoMetadata(ctx context.Context, project string, metadata RepoMetadata, previousSHA, message string) (string, string, error) {
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
	commitSHA, contentSHA, newCount, err := h.publishIndex(ctx, project, &metadata, previousSHA, message, objectCount)
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
// SHA. Callers distinguish the layout with meta.IsManifest(data).
func (h *StorHub) readIndexRevision(ctx context.Context, project, commitSHA string) ([]byte, bool, error) {
	if repo := h.getGitRepo(project); repo != nil {
		if d, err := repo.readFileRef(ctx, commitSHA, indexFilePath); err == nil {
			return d, true, nil
		} else if !isMetadataNotFound(err) {
			return nil, false, err
		}
		d, err := repo.readFileRef(ctx, commitSHA, metadataFilePath)
		if err == nil {
			return d, true, nil
		}
		if isMetadataNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	d, _, err := h.gh.GetFileContent(ctx, h.owner, project, indexFilePath, commitSHA)
	if err == nil {
		return d, true, nil
	}
	var apiErr *ghapi.APIError
	if !errors.As(err, &apiErr) || !apiErr.NotFound() {
		return nil, false, err
	}
	d, _, err = h.gh.GetFileContent(ctx, h.owner, project, metadataFilePath, commitSHA)
	if err == nil {
		return d, true, nil
	}
	if e, ok := err.(*ghapi.APIError); ok && e.NotFound() {
		return nil, false, nil
	}
	return nil, false, err
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
// content-addressed and layout-agnostic, so it serves any ref).
func (h *StorHub) fetchObjectAtRef(ctx context.Context, project, ref, sha string) ([]byte, error) {
	cache := h.objectCacheFor(project)
	if data, ok := cache.get(sha); ok {
		return data, nil
	}
	var data []byte
	var err error
	if repo := h.getGitRepo(project); repo != nil {
		data, err = repo.readFileRef(ctx, ref, objectRepoPath(sha))
	} else {
		if err = h.ensureOwner(ctx); err != nil {
			return nil, err
		}
		data, _, err = h.gh.GetFileContent(ctx, h.owner, project, objectRepoPath(sha), ref)
	}
	if err != nil {
		return nil, fmt.Errorf("fetch object %s at %s: %w", shortSHA(sha), shortSHA(ref), err)
	}
	if meta.ObjectSHA(data) != sha {
		return nil, fmt.Errorf("object %s failed content verification at %s", shortSHA(sha), shortSHA(ref))
	}
	cache.put(sha, data)
	return data, nil
}

// validateMetadataSnapshot checks a snapshot against live server state
// before a rollback/revert commits it. Structural validation is total;
// asset existence is checked LAZILY and TARGETED: only releases the
// snapshot references are examined, and a release's full asset list is
// paginated only when its embedded view is untrustworthy (at/above the
// truncation danger band, or an expected ID is missing from it). The old
// form built an index over every asset of every release - O(total assets)
// transient memory per call, three calls per rollback.
//
// The release list itself stays a fresh (uncached) listReleases: the point
// of the re-checks around the commit is to catch deletions that landed
// after the previous check, which a TTL cache would hide.
// NEEDS-INTEGRATION(13): a targeted GET /releases/assets/{id} would replace
// the per-release fallback entirely; the ghapi client exposes no such
// method today, so the fallback paginates ListReleaseAssets instead.
func (h *StorHub) validateMetadataSnapshot(ctx context.Context, project string, metadata *RepoMetadata) error {
	// Structural validation first - chunk/file size consistency
	// (chunks beyond EOF, negative geometry, dangling references,
	// totals) is verified here, not assumed from elsewhere.
	if err := metadata.Validate(); err != nil {
		return fmt.Errorf("rollback metadata failed validation: %w", err)
	}
	releases, err := h.listReleases(ctx, project)
	if err != nil {
		return err
	}
	releaseIndex := make(map[string]ghapi.Release, len(releases))
	for _, release := range releases {
		releaseIndex[release.TagName] = release
	}
	// Collect the asset IDs the snapshot actually references, per release.
	referenced := make(map[string]map[int64]struct{})
	for path, file := range metadata.Files() {
		for _, chunkName := range file.Chunks {
			// A dangling chunk reference must fail validation outright.
			// Skipping it here would bless a snapshot whose bytes cannot be
			// downloaded after commit.
			chunk, ok := metadata.Chunks()[chunkName]
			if !ok {
				return fmt.Errorf("rollback metadata references missing chunk %d (file %s)", chunkName, path)
			}
			// Structural size/offset sanity beyond Validate(): negative
			// geometry can never address real bytes.
			if chunk.Size < 0 || chunk.Offset < 0 {
				return fmt.Errorf("rollback metadata chunk %d has invalid geometry (offset %d, size %d)", chunkName, chunk.Offset, chunk.Size)
			}
			ids := referenced[chunk.Release]
			if ids == nil {
				ids = make(map[int64]struct{})
				referenced[chunk.Release] = ids
			}
			ids[chunk.AssetID] = struct{}{}
		}
	}
	for tag, ids := range referenced {
		release, ok := releaseIndex[tag]
		if !ok {
			return fmt.Errorf("rollback metadata references missing release: %s", tag)
		}
		present := make(map[int64]struct{}, len(release.Assets))
		for _, asset := range release.Assets {
			present[asset.ID] = struct{}{}
		}
		missing := false
		for id := range ids {
			if _, ok := present[id]; !ok {
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
			present = make(map[int64]struct{}, len(assets))
			for _, asset := range assets {
				present[asset.ID] = struct{}{}
			}
		}
		for id := range ids {
			if _, ok := present[id]; !ok {
				return fmt.Errorf("rollback metadata references missing asset %d in release %s", id, tag)
			}
		}
	}
	return nil
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
		for _, r := range releases {
			metadata.EnsureRelease(r.TagName, h.config.Now().Unix())
			return r.TagName, r.UploadURL, nil
		}
	} else {
		for _, r := range releases {
			count, err := h.releaseAssetCount(ctx, project, r)
			if err != nil {
				return "", "", err
			}
			if count+requiredSlots <= 1000 {
				metadata.EnsureRelease(r.TagName, h.config.Now().Unix())
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
	metadata.EnsureRelease(tag, h.config.Now().Unix())
	return tag, release.UploadURL, nil
}

// ensureChunkReleases registers every release holding new chunks in the
// authoritative metadata so PurgeUntracked cannot delete live data and
// rollback validation can resolve chunk references. Rotation may spread one
// file's chunks across releases; ensuring only the originally targeted tag
// would strand the rotated chunks.
func ensureChunkReleases(meta *RepoMetadata, chunks []ChunkInfo, now int64) {
	seen := make(map[string]struct{}, len(chunks))
	for _, c := range chunks {
		if _, ok := seen[c.Release]; ok {
			continue
		}
		seen[c.Release] = struct{}{}
		meta.EnsureRelease(c.Release, now)
	}
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
	assetID, err := h.gh.UploadAsset(ctx, h.owner, project, releaseTag, uploadURL, assetName, reader, size)
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
	for attempt := 0; attempt <= h.config.MaxRetries; attempt++ {
		reader, _, err := h.downloadAssetStream(ctx, project, chunk.AssetID, chunk.AssetOffset, chunk.AssetOffset+chunk.Size-1)
		if err != nil {
			if !isRetryableDownloadError(err) || attempt == h.config.MaxRetries {
				return err
			}
			delay := h.retryDelay(attempt, extractAPIError(err))
			h.debugf("asset range open retry project=%s asset=%d attempt=%d delay=%s err=%v", project, chunk.AssetID, attempt+1, delay, err)
			if sleepErr := h.config.Sleep(ctx, delay); sleepErr != nil {
				return sleepErr
			}
			continue
		}
		err = fn(reader)
		closeErr := reader.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
		if err == nil {
			return nil
		}
		if !isRetryableDownloadError(err) || attempt == h.config.MaxRetries {
			return err
		}
		delay := h.retryDelay(attempt, extractAPIError(err))
		h.debugf("asset range read retry project=%s asset=%d attempt=%d delay=%s err=%v", project, chunk.AssetID, attempt+1, delay, err)
		if sleepErr := h.config.Sleep(ctx, delay); sleepErr != nil {
			return sleepErr
		}
	}
	return errors.New("asset range read exhausted retries")
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
	h.objCacheMu.Lock()
	delete(h.objCaches, project)
	h.objCacheMu.Unlock()
	h.forgetRepoState(project)
	h.invalidateReleaseCache(project)
	// Drop the cached per-project logger too, so project churn cannot grow
	// the logger cache without bound.
	h.loggers.Delete(project)
}

func (h *StorHub) cachedRepoMetadata(project string) (*RepoMetadata, string, bool) {
	h.metaMu.RLock()
	entry, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return nil, "", false
	}
	// Share the immutable current pointer: published trees are never
	// mutated in place (cowTree/publishTreeLocked discipline), so a reader
	// holding the pointer after releasing the lock sees a frozen snapshot.
	// Cloning here was a full deep copy per read (O(tree) allocations); the
	// indexes were built at store/publish time and stay valid.
	entry.mu.RLock()
	meta := entry.meta
	sha := entry.sha
	entry.mu.RUnlock()
	return meta, sha, true
}

func (h *StorHub) cachedRepoMetadataReadonly(project string) (*RepoMetadata, string, bool) {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return nil, "", false
	}
	pm.mu.RLock()
	// An unhydrated entry carries an EMPTY tree that is not remote truth;
	// serving it lets a cold-cache mutation commit over real remote state.
	// Miss instead: the caller falls through to a fresh load.
	if !pm.hydrated {
		pm.mu.RUnlock()
		return nil, "", false
	}
	meta := pm.meta
	sha := pm.sha
	pm.mu.RUnlock()
	return meta, sha, true
}

// cowTree returns a private, mutable copy of a published metadata tree.
//
// Published trees (pm.meta) are shared with lock-free readers, so no code
// may mutate one in place. Mutation sites take a copy here, apply their
// changes, and publish with publishTreeLocked. The copy is the metadata
// engine's Clone: the four stored maps are copied while their immutable
// entry VALUES are shared, and a clean derived index is shared read-only, so
// the copy is cheap. The first tracked mutation drops the copy to a private
// dirty derived state (the engine's owner/mapsShared guard), never writing
// into maps the published tree still reads.
func cowTree(m *RepoMetadata) *RepoMetadata {
	c := m.Clone()
	return &c
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
func (h *StorHub) storeRepoMetadata(project string, meta RepoMetadata, sha string, pendingOps []Op, objectCount uint64) {
	clone := meta.Clone()
	clone.RebuildIndexes()

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if len(pendingOps) > 0 {
		pm.meta = &clone
		pm.sha = sha
		pm.hydrated = true
		pm.objectCount = objectCount
		// The rebase baseline moves to the freshly loaded state.
		pm.baseTree = &clone
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
	pm.meta = &clone
	pm.sha = sha
	pm.hydrated = true
	pm.objectCount = objectCount
	// The rebase baseline moves to the freshly loaded state.
	pm.baseTree = &clone
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
// dropped; ops without a resolvable single target (renames, catalog ops) are
// kept - replay applies them defensively.
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
