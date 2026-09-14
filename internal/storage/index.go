package storage

import (
	"context"
	"errors"
	"fmt"

	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// The v2 index layout behind a version gate. Reads understand both layouts
// always; the WRITE path uses v2 only when the project is already v2 or the
// IndexV2 config opts it in (a v1 project then migrates on its first write).
// The manifest is the single CAS point; content-addressed objects are written
// idempotently before it, so a crash between object writes and the manifest
// CAS leaves only unreferenced garbage for `storhub prune` to reclaim.

// readIndexHead fetches the project's current index: the v2 manifest when
// present, otherwise the v1 metadata blob. found=false means neither exists
// (brand-new or wiped project). The returned sha is the CAS token for the
// blob that was found (manifest blob sha for v2, metadata blob sha for v1).
func (h *StorHub) readIndexHead(ctx context.Context, project string) (data []byte, sha string, isV2, found bool, err error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, "", false, false, err
	}
	if repo := h.getGitRepo(project); repo != nil {
		if d, rerr := repo.readFileHead(ctx, indexFilePath); rerr == nil {
			return d, repo.headCommitSHA(), true, true, nil
		} else if !isMetadataNotFound(rerr) {
			return nil, "", false, false, rerr
		}
		d, rerr := repo.readFileHead(ctx, metadataFilePath)
		if rerr == nil {
			return d, repo.headCommitSHA(), false, true, nil
		}
		if isMetadataNotFound(rerr) {
			return nil, "", false, false, nil
		}
		return nil, "", false, false, rerr
	}
	// REST: try the manifest, then the v1 blob.
	d, s, rerr := h.gh.GetFileContent(ctx, h.owner, project, indexFilePath, "")
	if rerr == nil {
		return d, s, true, true, nil
	}
	var apiErr *ghapi.APIError
	if !errors.As(rerr, &apiErr) || !apiErr.NotFound() {
		return nil, "", false, false, rerr
	}
	d, s, rerr = h.gh.GetFileContent(ctx, h.owner, project, metadataFilePath, "")
	if rerr == nil {
		return d, s, false, true, nil
	}
	if e, ok := rerr.(*ghapi.APIError); ok && e.NotFound() {
		return nil, "", false, false, nil
	}
	return nil, "", false, false, rerr
}

// loadIndexTree materializes a flat RepoMetadata from an index blob (v2
// manifest or v1 document), fetching v2 objects through the cache + repo.
func (h *StorHub) loadIndexTree(ctx context.Context, project string, data []byte, isV2 bool) (*RepoMetadata, uint64, error) {
	m := NewRepoMetadata(project)
	if !isV2 {
		if err := m.FromJSON(data); err != nil {
			return nil, 0, fmt.Errorf("parse metadata: %w", err)
		}
		m.Normalize(project, h.config.Now().Unix())
		if err := m.Validate(); err != nil {
			return nil, 0, fmt.Errorf("validate metadata: %w", err)
		}
		return m, 0, nil
	}
	manifest, err := meta.ParseManifest(data)
	if err != nil {
		return nil, 0, err
	}
	fetched := func(sha string) ([]byte, error) { return h.fetchObject(ctx, project, sha) }
	loaded, err := meta.LoadTree(manifest, fetched)
	if err != nil {
		return nil, 0, fmt.Errorf("load v2 index: %w", err)
	}
	loaded.Project = project
	loaded.Normalize(project, h.config.Now().Unix())
	if err := loaded.Validate(); err != nil {
		return nil, 0, fmt.Errorf("validate v2 index: %w", err)
	}
	return loaded, manifest.ObjectCount, nil
}

// publishIndex performs ONE commit attempt for the given layout: it writes
// any new content-addressed objects, then compare-and-swaps the manifest (v2)
// or the metadata blob (v1). It returns the new running object count so the
// caller can carry the threshold hint forward. A 409 from the CAS propagates
// unchanged so the commit loop can rebase and retry.
func (h *StorHub) publishIndex(ctx context.Context, project string, tree *meta.RepoMetadata, prevSHA, message string, isV2 bool, prevObjectCount uint64) (commitSHA, contentSHA string, newObjectCount uint64, err error) {
	if !isV2 {
		blob, merr := tree.ToJSON()
		if merr != nil {
			return "", "", prevObjectCount, fmt.Errorf("marshal metadata: %w", merr)
		}
		if len(blob) > maxMetadataBytes {
			return "", "", prevObjectCount, &oversizeError{size: len(blob), limit: maxMetadataBytes}
		}
		if repo := h.getGitRepo(project); repo != nil {
			commitSHA, contentSHA, err = repo.writeCommitPushCAS(ctx, metadataFilePath, blob, message, prevSHA)
		} else {
			commitSHA, contentSHA, err = h.gh.PutFileContent(ctx, h.owner, project, metadataFilePath, blob, prevSHA, message)
		}
		return commitSHA, contentSHA, prevObjectCount, err
	}

	res, merr := meta.BuildTree(tree)
	if merr != nil {
		return "", "", prevObjectCount, fmt.Errorf("build index tree: %w", merr)
	}
	// Per-object admission (v2 replaces the whole-blob ceiling): no single
	// object may exceed the contents-API limit. A breach means one directory
	// holds an enormous number of entries; the remedy is structural, not a
	// ceiling raise.
	for _, data := range res.Objects {
		if len(data) > maxMetadataBytes {
			return "", "", prevObjectCount, &oversizeError{size: len(data), limit: maxMetadataBytes}
		}
	}
	// Objects genuinely new to this client (not cached) drive the running
	// count; cached objects are already upstream.
	newCount := h.countNewObjects(project, res.Objects)
	objectCount := prevObjectCount + newCount

	if repo := h.getGitRepo(project); repo != nil {
		// Git: objects and manifest land in one commit (atomic).
		files := make(map[string][]byte, len(res.Objects)+1)
		for sha, data := range res.Objects {
			files[objectRepoPath(sha)] = data
		}
		manifest := h.buildManifest(project, tree, res, objectCount)
		mb, merr := meta.MarshalManifest(manifest)
		if merr != nil {
			return "", "", prevObjectCount, merr
		}
		files[indexFilePath] = mb
		commitSHA, contentSHA, err = repo.writeCommitPushCASMulti(ctx, files, message, prevSHA)
		if err == nil {
			h.cacheObjects(project, res.Objects)
		}
		return commitSHA, contentSHA, objectCount, err
	}

	// REST: write objects idempotently, then CAS the manifest.
	written, werr := h.writeObjects(ctx, project, res.Objects)
	if werr != nil {
		return "", "", prevObjectCount, werr
	}
	objectCount = prevObjectCount + uint64(written)
	manifest := h.buildManifest(project, tree, res, objectCount)
	mb, merr := meta.MarshalManifest(manifest)
	if merr != nil {
		return "", "", prevObjectCount, merr
	}
	commitSHA, contentSHA, err = h.gh.PutFileContent(ctx, h.owner, project, indexFilePath, mb, prevSHA, message)
	return commitSHA, contentSHA, objectCount, err
}

func (h *StorHub) buildManifest(project string, tree *meta.RepoMetadata, res *meta.TreeResult, objectCount uint64) *meta.ManifestV2 {
	return &meta.ManifestV2{
		Version:      meta.ManifestVersion,
		Project:      project,
		TreeRoot:     res.RootSHA,
		ChunkBuckets: res.ChunkBuckets,
		Releases:     res.ReleasesSHA,
		ObjectCount:  objectCount,
		NextInode:    tree.NextInode,
		NextChunkID:  tree.NextChunkID,
		Stats:        meta.ManifestStats{Files: tree.TotalFiles, Bytes: tree.TotalSize},
		LastMod:      tree.LastMod,
	}
}

func (h *StorHub) countNewObjects(project string, objects map[string][]byte) uint64 {
	cache := h.objectCacheFor(project)
	var n uint64
	for sha := range objects {
		if !cache.contains(sha) {
			n++
		}
	}
	return n
}

func (h *StorHub) cacheObjects(project string, objects map[string][]byte) {
	cache := h.objectCacheFor(project)
	for sha, data := range objects {
		cache.put(sha, data)
	}
}

// oversizeError marks a v1 commit that breached the blob ceiling so the
// commit loop can arm the D4 admission marker without string matching.
type oversizeError struct {
	size  int
	limit int
}

func (e *oversizeError) Error() string {
	return fmt.Sprintf("metadata too large: %d bytes (max %d); run `storhub prune` to reclaim history, or enable the v2 split index (index_v2) to remove the ceiling", e.size, e.limit)
}

// warnHistoryThreshold logs once per threshold crossing that a v2 project has
// accumulated many index objects under full-history retention, pointing at
// the granular prune. The counter is advisory; the warning never blocks a
// commit.
func (h *StorHub) warnHistoryThreshold(project string, pm *projectMetadata) {
	pm.mu.Lock()
	count := pm.objectCount
	warned := pm.historyWarned
	pm.mu.Unlock()
	limit := h.config.HistoryWarnObjects
	if limit == 0 {
		return
	}
	if count >= limit {
		if !warned {
			logging.Warn(h.projectLogger(project), "index object accumulation under full-history retention",
				"objects", count, "threshold", limit, "remedy", "storhub prune (history|objects|assets)")
			pm.mu.Lock()
			pm.historyWarned = true
			pm.mu.Unlock()
		}
		return
	}
	// Below the threshold, re-arm so a later crossing warns again.
	if warned {
		pm.mu.Lock()
		pm.historyWarned = false
		pm.mu.Unlock()
	}
}
