package storage

import (
	"context"
	"errors"
	"fmt"

	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// The split index layout (metadata version 5) behind a version gate. Reads
// understand every version always; the write path always produces the split
// layout, so a legacy single-blob project migrates on its first write.
// The manifest is the single CAS point; content-addressed objects are written
// idempotently before it, so a crash between object writes and the manifest
// CAS leaves only unreferenced garbage for `storhub prune` to reclaim.

// readIndexHead fetches the project's current index: the split manifest when
// present, otherwise the legacy metadata blob. found=false means neither exists
// (brand-new or wiped project). The returned sha is the CAS token for the
// blob that was found (manifest blob sha for split, metadata blob sha for
// legacy).
func (h *StorHub) readIndexHead(ctx context.Context, project string) (data []byte, sha string, split, found bool, err error) {
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
	// REST: try the manifest, then the legacy blob.
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

// loadIndexTree materializes a flat RepoMetadata from an index blob (the
// version-5 manifest or a legacy document), fetching split objects through the cache + repo.
func (h *StorHub) loadIndexTree(ctx context.Context, project string, data []byte, split bool) (*RepoMetadata, uint64, error) {
	m := NewRepoMetadata(project)
	if !split {
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
		return nil, 0, fmt.Errorf("load split index: %w", err)
	}
	loaded.Project = project
	loaded.Normalize(project, h.config.Now().Unix())
	if err := loaded.Validate(); err != nil {
		return nil, 0, fmt.Errorf("validate split index: %w", err)
	}
	return loaded, manifest.ObjectCount, nil
}

// publishIndex performs ONE commit attempt: it writes any new content-addressed
// objects, then compare-and-swaps the manifest (the version-5 split layout,
// the default and latest write path). It returns the new running object count
// so the caller can carry the threshold hint forward. A 409 from the CAS
// propagates unchanged so the commit loop can rebase and retry.
func (h *StorHub) publishIndex(ctx context.Context, project string, tree *meta.RepoMetadata, prevSHA, message string, prevObjectCount uint64) (commitSHA, contentSHA string, newObjectCount uint64, err error) {
	res, merr := meta.BuildTree(tree)
	if merr != nil {
		return "", "", prevObjectCount, fmt.Errorf("build index tree: %w", merr)
	}
	// Per-object admission: no single object may exceed the contents-API
	// limit. A breach means one directory holds an enormous number of
	// entries; the remedy is structural, not a ceiling raise.
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

func (h *StorHub) buildManifest(project string, tree *meta.RepoMetadata, res *meta.TreeResult, objectCount uint64) *meta.Manifest {
	return &meta.Manifest{
		Version:      meta.CurrentVersion,
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

// oversizeError marks a commit whose single index object breached the
// contents-API limit so the commit loop can arm the D4 admission marker
// without string matching.
type oversizeError struct {
	size  int
	limit int
}

func (e *oversizeError) Error() string {
	return fmt.Sprintf("metadata too large: one index object is %d bytes (max %d); distribute entries across subdirectories or run `storhub prune` to reclaim", e.size, e.limit)
}

// warnHistoryThreshold logs once per threshold crossing that a split project has
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
