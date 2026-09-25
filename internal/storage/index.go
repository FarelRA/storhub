package storage

import (
	"context"
	"fmt"

	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// The split index layout (metadata version 5) behind a version gate. Reads
// understand every version always; the write path always produces the split
// layout, so a legacy single-blob project migrates on its first write.
// The manifest is the single CAS point; content-addressed objects are written
// idempotently before it, so a crash between object writes and the manifest
// CAS leaves only unreferenced garbage for `storhub prune` to reclaim.

// isNotFoundErr is the single NotFound dispatch for backend reads: it
// recognizes every absence shape (API errors, git sentinels, OS errors)
// through isMetadataNotFound, so git and REST absences never need
// separate checks.
func isNotFoundErr(err error) bool {
	return isMetadataNotFound(err)
}

// readIndexDoc fetches ONE index document (manifest or legacy blob) at a
// ref: "" means HEAD. It is the single backend-dispatch point (single-flight backend dispatch)
// behind readIndexHead/readIndexRevision and the object fetchers
// (fetchObjectAt/fetchObjectAtRef via readObjectBytes): git reads go
// through the mirror, REST through GetFileContent. found=false
// means absent at this ref (not an error). The returned sha is the CAS
// token for the document found (manifest blob sha on REST, HEAD commit sha
// on git). Callers distinguish the layout with meta.IsManifest(data).
func (h *StorHub) readIndexDoc(ctx context.Context, project, path, ref string) (data []byte, sha string, found bool, err error) {
	if repo := h.getGitRepo(project); repo != nil {
		if ref == "" {
			// HEAD reads keep the atomic (data, sha) pairing: a
			// concurrent sync advancing HEAD between a content read
			// and a separate HEAD read would pair content at commit
			// N with token N+1, passing the next CAS pre-check while
			// built from N (silent overwrite of N+1).
			d, s, rerr := readIndexHeadGit(ctx, repo, path)
			if rerr != nil {
				if isMetadataNotFound(rerr) {
					return nil, "", false, nil
				}
				return nil, "", false, rerr
			}
			return d, s, true, nil
		}
		data, rerr := repo.readFileRef(ctx, ref, path)
		if rerr != nil {
			if isMetadataNotFound(rerr) {
				return nil, "", false, nil
			}
			return nil, "", false, rerr
		}
		// Git tokens are commit SHAs: echo the requested ref.
		return data, ref, true, nil
	}
	if err := h.ensureOwner(ctx); err != nil {
		return nil, "", false, err
	}
	d, s, rerr := h.gh.GetFileContent(ctx, h.owner, project, path, ref)
	if rerr != nil {
		if isNotFoundErr(rerr) {
			return nil, "", false, nil
		}
		return nil, "", false, rerr
	}
	return d, s, true, nil
}

// readIndexHead fetches the project's current index: the split manifest when
// present, otherwise the legacy metadata blob. found=false means neither exists
// (brand-new or wiped project).
func (h *StorHub) readIndexHead(ctx context.Context, project string) (data []byte, sha string, found bool, err error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, "", false, err
	}
	if d, s, ok, rerr := h.readIndexDoc(ctx, project, indexFilePath, ""); rerr != nil {
		return nil, "", false, rerr
	} else if ok {
		return d, s, true, nil
	}
	return h.readIndexDoc(ctx, project, metadataFilePath, "")
}

// readIndexHeadGit reads one index document from the git mirror, pairing the
// content with the HEAD commit atomically. readFileHead syncs and reads,
// but a concurrent sync can advance HEAD before a separate headCommitSHA
// call, pairing content at commit N with token N+1 - the next CAS would
// then pass its pre-check while the tree was built from N, silently
// overwriting N+1. Re-reading the file AT the returned sha makes (data,
// sha) a consistent pair: readFileRef pins the sha under the sync lock, so
// the bytes are always the bytes of the commit the token names. If HEAD
// moved between the reads, the pinned read simply returns the newer commit's
// content; if the path cannot be resolved at that sha, fall back to the
// original pairing (best effort, matching the pre-fix behavior).
func readIndexHeadGit(ctx context.Context, repo *gitRepo, path string) ([]byte, string, error) {
	d, err := repo.readFileHead(ctx, path)
	if err != nil {
		return nil, "", err
	}
	sha := repo.headCommitSHA()
	if sha == "" {
		return d, "", nil
	}
	if pinned, perr := repo.readFileRef(ctx, sha, path); perr == nil {
		return pinned, sha, nil
	}
	return d, sha, nil
}

// loadIndexTree materializes a flat RepoMetadata from an index document,
// detecting the layout by shape: a version-5 manifest loads its objects
// through the cache + repo; a legacy blob parses directly. The returned tree
// carries the matching document version (5 or <=4).
func (h *StorHub) loadIndexTree(ctx context.Context, project string, data []byte) (*RepoMetadata, uint64, error) {
	if !meta.IsManifest(data) {
		m := NewRepoMetadata(project)
		if err := m.FromJSON(data); err != nil {
			return nil, 0, fmt.Errorf("parse metadata: %w", err)
		}
		m.Normalize(project, h.config.Now().UnixNano())
		if err := m.Validate(); err != nil {
			return nil, 0, fmt.Errorf("validate metadata: %w", err)
		}
		return m, 0, nil
	}
	manifest, err := meta.ParseManifest(data)
	if err != nil {
		return nil, 0, err
	}
	// Batch load against one pinned HEAD (commit-admission batching): the per-object
	// fetchObject path syncs (fetch+hard-reset) per object, so a cold
	// load paid O(objects) serialized syncs on one mutex. Pin once,
	// resolve every object against the pin with no further syncs.
	fetched, ferr := h.pinnedFetcher(ctx, project)
	if ferr != nil {
		return nil, 0, ferr
	}
	loaded, err := meta.LoadTreeParallel(manifest, fetched)
	if err != nil {
		return nil, 0, fmt.Errorf("load split index: %w", err)
	}
	loaded.Project = project
	loaded.Normalize(project, h.config.Now().UnixNano())
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
//
// The build streams (BuildTreeStream) through the project's retained
// TreeCache with known=object-cache-membership: unchanged subtrees are
// neither re-marshalled nor retained, so a commit no longer materializes
// the full Objects map duplicating tree bytes (object-cache residency: filesByParent +
// dirs + byBucket + full objects map per commit). Only genuinely-new
// objects drive the running count and the upload set.
//
// A streaming-build failure fails the commit loud: there is no fallback
// builder, so a streaming bug surfaces instead of succeeding silently
// through an older path.
func (h *StorHub) publishIndex(ctx context.Context, project string, tree *meta.RepoMetadata, prevSHA, message string, prevObjectCount uint64) (commitSHA, contentSHA string, newObjectCount uint64, err error) {
	refs, objects, scratch, oerr := h.buildIndexStream(ctx, project, tree)
	if oerr != nil {
		return "", "", prevObjectCount, oerr
	}
	// Only streamed (genuinely new) objects drive the running count;
	// cache-known objects are already upstream.
	objectCount := prevObjectCount + uint64(len(objects))

	if repo := h.getGitRepo(project); repo != nil {
		// Git: objects and manifest land in one commit (atomic).
		files := make(map[string][]byte, len(objects)+1)
		for sha, data := range objects {
			files[objectRepoPath(sha)] = data
		}
		manifest := h.buildManifest(project, tree, refs, objectCount)
		mb, merr := meta.MarshalManifest(manifest)
		if merr != nil {
			return "", "", prevObjectCount, merr
		}
		files[indexFilePath] = mb
		commitSHA, contentSHA, err = repo.writeCommitPushCASMulti(ctx, files, message, prevSHA)
		if err == nil {
			h.cacheObjects(project, objects)
			h.rememberTreeCache(project, tree, scratch)
		}
		return commitSHA, contentSHA, objectCount, err
	}

	// REST: write objects idempotently, then CAS the manifest.
	written, werr := h.writeObjects(ctx, project, objects)
	if werr != nil {
		return "", "", prevObjectCount, werr
	}
	objectCount = prevObjectCount + uint64(written)
	manifest := h.buildManifest(project, tree, refs, objectCount)
	mb, merr := meta.MarshalManifest(manifest)
	if merr != nil {
		return "", "", prevObjectCount, merr
	}
	// The manifest itself is a contents-API document too: a
	// pathological bucket list can push it past the limit even when every
	// object fits. Surfacing that as an oversizeError arms the size-ceiling marker
	// instead of livelocking the retry loop on a bare 422.
	if err := checkManifestSize(mb); err != nil {
		return "", "", objectCount, err
	}
	commitSHA, contentSHA, err = h.gh.PutFileContent(ctx, h.owner, project, indexFilePath, mb, prevSHA, message)
	if err == nil {
		h.rememberTreeCache(project, tree, scratch)
	}
	return commitSHA, contentSHA, objectCount, err
}

// buildIndexStream runs the streaming build: new/changed objects are
// emitted into the returned map (skipping cache-known shas), unchanged
// subtrees are skipped via the project's retained TreeCache. The map
// holds only the delta, not the whole index.
func (h *StorHub) buildIndexStream(ctx context.Context, project string, tree *meta.RepoMetadata) (*meta.TreeRefs, map[string][]byte, *meta.TreeCache, error) {
	_ = ctx
	cache := h.objectCacheFor(project)
	pm := h.lookupProjectMeta(project)
	// Build into a scratch copy of the shared cache, never the shared
	// cache itself: putNodes during the build mark objects "already
	// stored", and a build that never commits (conflict, error, shutdown
	// race) must not poison the shared baseline with objects it never
	// uploaded. The scratch is swapped in by rememberTreeCache only on
	// commit success; on failure it is dropped and the shared baseline
	// still describes what is actually upstream.
	var scratch *meta.TreeCache
	if pm != nil {
		pm.mu.Lock()
		scratch = pm.treeCache.Clone()
		pm.mu.Unlock()
	} else {
		scratch = meta.NewTreeCache()
	}
	objects := make(map[string][]byte)
	known := func(sha string) bool { return cache.contains(sha) }
	emit := func(sha string, data []byte) error {
		if len(data) > maxMetadataBytes {
			return &oversizeError{size: len(data), limit: maxMetadataBytes}
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		objects[sha] = cp
		return nil
	}
	refs, err := meta.BuildTreeStream(tree, scratch, known, emit)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build index tree: %w", err)
	}
	return refs, objects, scratch, nil
}

// rememberTreeCache swaps a successful build's scratch cache in as the
// project's new baseline: every object the scratch skipped was either
// emitted by this build (uploaded before the manifest CAS) or skipped on
// the previous baseline (uploaded by the commit that installed it), so
// "cache hit" stays equivalent to "already stored upstream". A nil scratch
// resets the baseline to empty instead of keeping stale entries: the next
// streaming build then re-emits by object-cache membership, wasteful but
// never wrong. A missing entry (evicted mid-commit) simply drops the
// cache: the next commit rebuilds uncached.
func (h *StorHub) rememberTreeCache(project string, tree *RepoMetadata, scratch *meta.TreeCache) {
	pm := h.lookupProjectMeta(project)
	if pm == nil {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if scratch == nil {
		pm.treeCache = meta.NewTreeCache()
		return
	}
	// Revalidate the bound against the committed tree (shas, not bytes).
	if tree != nil {
		dirs := len(tree.Dirs())
		files := len(tree.Files())
		if dirs+files > treeCacheMaxEntries {
			pm.treeCache = meta.NewTreeCache()
			return
		}
	}
	pm.treeCache = scratch
}

// checkManifestSize bounds the serialized manifest against the contents-API
// limit. Kept separate so the ceiling is testable without building a
// pathologically large bucket list.
func checkManifestSize(mb []byte) error {
	if len(mb) > maxMetadataBytes {
		return &oversizeError{size: len(mb), limit: maxMetadataBytes}
	}
	return nil
}

// buildManifest renders the split manifest from one streaming build:
// TreeRefs carries the same references as the old full-map result without
// the whole objects map.
func (h *StorHub) buildManifest(project string, tree *meta.RepoMetadata, refs *meta.TreeRefs, objectCount uint64) *meta.Manifest {
	return &meta.Manifest{
		Version:      meta.CurrentVersion,
		Project:      project,
		TreeRoot:     refs.RootSHA,
		ChunkBuckets: refs.ChunkBuckets,
		Releases:     refs.ReleasesSHA,
		ObjectCount:  objectCount,
		NextInode:    tree.NextInode,
		NextChunkID:  tree.NextChunkID,
		Stats:        meta.ManifestStats{Files: tree.TotalFiles, Bytes: tree.TotalSize},
		LastMod:      tree.LastMod,
	}
}

func (h *StorHub) cacheObjects(project string, objects map[string][]byte) {
	cache := h.objectCacheFor(project)
	for sha, data := range objects {
		cache.put(sha, data)
	}
}

// oversizeError marks a commit whose single index object breached the
// contents-API limit so the commit loop can arm the size-ceiling admission marker
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
