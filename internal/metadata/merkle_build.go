package metadata

import (
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"sort"
	"time"

	"github.com/FarelRA/storhub/internal/logging"
)

// BuildTreeStream serializes a flat RepoMetadata into the Merkle object set
// ONE OBJECT AT A TIME: each new or changed object is handed to emit before
// the next is built, so the caller can upload/free it instead of holding
// the entire serialized index in RAM (the whole-tree objects map was ~20%
// of allocations).
//
//   - cache (optional): reuse across commits. A node/bucket/releases object
//     whose inputs are identical to the cached build is skipped entirely
//     (not re-marshalled, not emitted - the caller already stored it).
//   - known (optional): reports whether a sha is already present in the
//     object store; freshly marshalled objects it accepts are not emitted.
//
// Callers must pass a normalized tree (deterministic entry ordering), same
// as BuildTree. Returns the manifest references.
func BuildTreeStream(meta *RepoMetadata, cache *TreeCache, known func(sha string) bool, emit TreeEmitter) (*TreeRefs, error) {
	started := time.Now()
	objects := 0
	if emit != nil {
		inner := emit
		emit = func(sha string, data []byte) error {
			objects++
			return inner(sha, data)
		}
	}
	logging.Debug(metaLog(), "metadata build start")
	refs, err := buildTreeStream(meta, cache, known, emit)
	if err != nil {
		logging.Error(metaLog(), "metadata build failed", "err", err, "elapsed", time.Since(started))
		return nil, err
	}
	logging.Debug(metaLog(), "metadata build complete", "objects", objects, "buckets", len(refs.ChunkBuckets), "elapsed", time.Since(started))
	return refs, nil
}

// buildTreeStream is the build body behind the logging wrapper above.
func buildTreeStream(meta *RepoMetadata, cache *TreeCache, known func(sha string) bool, emit TreeEmitter) (*TreeRefs, error) {
	if err := checkTreeParents(meta); err != nil {
		return nil, err
	}
	filesByParent := groupTreeFiles(meta)
	dirs := sortedTreeDirs(meta)
	nodeSHA, err := emitTreeNodes(meta, cache, known, emit, dirs, filesByParent)
	if err != nil {
		return nil, err
	}

	buckets, err := streamChunkBuckets(meta, cache, known, emit)
	if err != nil {
		return nil, err
	}

	rel := ReleasesObject{Releases: meta.releases}
	if rel.Releases == nil {
		rel.Releases = map[string]ReleaseRef{}
	}
	releasesSHA, err := streamReleases(rel, cache, known, emit)
	if err != nil {
		return nil, err
	}

	return &TreeRefs{
		RootSHA:      nodeSHA[""],
		ChunkBuckets: buckets,
		ReleasesSHA:  releasesSHA,
	}, nil
}

// checkTreeParents is the round-trip identity guard: every non-root entry's
// parent must exist (or its group would never be serialized), and no path
// may be both a file and a directory (Validate rejects this, but the build
// must not silently emit an unloadable object set when called on an
// unvalidated tree).
func checkTreeParents(meta *RepoMetadata) error {
	for p := range meta.files {
		if parent := parentPath(p); parent != "" {
			if _, ok := meta.dirs[parent]; !ok {
				return fmt.Errorf("build tree: file %q has no parent directory %q", p, parent)
			}
		}
	}
	for p := range meta.dirs {
		if parent := parentPath(p); parent != "" {
			if _, ok := meta.dirs[parent]; !ok {
				return fmt.Errorf("build tree: directory %q has no parent directory %q", p, parent)
			}
		}
	}
	for p := range meta.files {
		if _, ok := meta.dirs[p]; ok {
			return fmt.Errorf("build tree: path %q is both a file and a directory", p)
		}
		if p == "" {
			return fmt.Errorf("build tree: file entry with empty path")
		}
	}
	return nil
}

// groupTreeFiles buckets file entries by parent directory, keyed by base
// name within the node.
func groupTreeFiles(meta *RepoMetadata) map[string]map[string]FileMeta {
	filesByParent := make(map[string]map[string]FileMeta, len(meta.files))
	for p, f := range meta.files {
		parent := parentPath(p)
		if filesByParent[parent] == nil {
			filesByParent[parent] = make(map[string]FileMeta)
		}
		filesByParent[parent][path.Base(p)] = f
	}
	return filesByParent
}

// sortedTreeDirs lists every directory (root sentinel "" plus each stored
// dir) deepest-first, so a node's child shas exist before it is hashed.
func sortedTreeDirs(meta *RepoMetadata) []string {
	dirs := make([]string, 0, len(meta.dirs)+1)
	dirs = append(dirs, "")
	for p := range meta.dirs {
		dirs = append(dirs, p)
	}
	sort.Slice(dirs, func(i, j int) bool { return nodeDepth(dirs[i]) > nodeDepth(dirs[j]) })
	return dirs
}

// emitTreeNodes builds every directory node deepest-first, emitting new or
// changed objects, and returns each directory's node sha.
func emitTreeNodes(meta *RepoMetadata, cache *TreeCache, known func(sha string) bool, emit TreeEmitter, dirs []string, filesByParent map[string]map[string]FileMeta) (map[string]string, error) {
	subdirShas := make(map[string]map[string]string, len(dirs))
	nodeSHA := make(map[string]string, len(dirs))
	for _, d := range dirs {
		nodeMeta := meta.dirs[d]
		if d == "" {
			nodeMeta = meta.Root
		}
		files, subdirs := filesByParent[d], subdirShas[d]
		if cached := cache.node(d); cached != nil && nodeInputsEqual(cached, nodeMeta, files, subdirs) {
			// Byte-identical node since the cached build: already stored.
			nodeSHA[d] = cached.sha
			linkChild(subdirShas, d, cached.sha)
			continue
		}
		node := TreeNode{Files: files, Subdirs: subdirs}
		node.Meta = nodeMeta
		data, err := json.Marshal(node)
		if err != nil {
			return nil, fmt.Errorf("marshal tree node %q: %w", d, err)
		}
		sha := ObjectSHA(data)
		nodeSHA[d] = sha
		if cache != nil {
			cache.putNode(d, &cachedNode{meta: nodeMeta, files: files, subdirs: subdirs, sha: sha})
		}
		if known == nil || !known(sha) {
			if err := emit(sha, data); err != nil {
				return nil, err
			}
		}
		linkChild(subdirShas, d, sha)
	}
	return nodeSHA, nil
}

// linkChild records dir's node sha under its parent's child map. The root
// ("") has no parent and is never linked.
func linkChild(subdirShas map[string]map[string]string, dir, sha string) {
	if dir == "" {
		return
	}
	parent := parentPath(dir)
	if subdirShas[parent] == nil {
		subdirShas[parent] = make(map[string]string)
	}
	subdirShas[parent][path.Base(dir)] = sha
}

func (c *TreeCache) node(dirPath string) *cachedNode {
	if c == nil {
		return nil
	}
	return c.nodes[dirPath]
}

func (c *TreeCache) putNode(dirPath string, cn *cachedNode) { c.nodes[dirPath] = cn }

// nodeInputsEqual decides byte-equality of a serialized TreeNode without
// marshalling it: identical DirMeta, identical inline file entries, and
// identical child shas produce identical JSON (encoding/json sorts map
// keys), and any difference in those inputs changes the bytes.
func nodeInputsEqual(cached *cachedNode, meta DirMeta, files map[string]FileMeta, subdirs map[string]string) bool {
	if !dirMetaEqual(cached.meta, meta) {
		return false
	}
	if !fileMapEqual(cached.files, files) {
		return false
	}
	return stringMapEqual(cached.subdirs, subdirs)
}

// fileMapsEqual compares node file entries by content: nil and empty maps
// are EQUAL. encoding/json omits both nil and len-0 maps under omitempty,
// so the two serialize to identical bytes and distinguishing them only
// causes spurious cache misses and re-emits.
func fileMapEqual(a, b map[string]FileMeta) bool {
	return maps.EqualFunc(a, b, fileMetaEqual)
}

func stringMapEqual(a, b map[string]string) bool {
	return maps.Equal(a, b)
}

// streamChunkBuckets builds each bucket object and emits it unless the
// cache proves it unchanged or the caller reports its sha already known.
func streamChunkBuckets(meta *RepoMetadata, cache *TreeCache, known func(sha string) bool, emit TreeEmitter) ([]string, error) {
	if len(meta.chunks) == 0 {
		return nil, nil
	}
	byBucket := make(map[int64]map[int64]ChunkInfo)
	for id, info := range meta.chunks {
		idx := id / ChunkBucketSize
		if byBucket[idx] == nil {
			byBucket[idx] = make(map[int64]ChunkInfo)
		}
		byBucket[idx][id] = info
	}
	indexes := make([]int64, 0, len(byBucket))
	for idx := range byBucket {
		indexes = append(indexes, idx)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })

	shas := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		chunks := byBucket[idx]
		if cache != nil {
			if cached := cache.buckets[idx]; cached != nil && maps.Equal(cached.chunks, chunks) {
				shas = append(shas, cached.sha)
				continue
			}
		}
		b := ChunkBucket{Index: idx, Chunks: chunks}
		data, err := json.Marshal(b)
		if err != nil {
			return nil, fmt.Errorf("marshal chunk bucket %d: %w", idx, err)
		}
		sha := ObjectSHA(data)
		if cache != nil {
			cache.buckets[idx] = &cachedBucket{chunks: chunks, sha: sha}
		}
		if known == nil || !known(sha) {
			if err := emit(sha, data); err != nil {
				return nil, err
			}
		}
		shas = append(shas, sha)
	}
	return shas, nil
}

func streamReleases(rel ReleasesObject, cache *TreeCache, known func(sha string) bool, emit TreeEmitter) (string, error) {
	if cache != nil && cache.releases != nil && maps.Equal(cache.releases.releases, rel.Releases) {
		return cache.releases.sha, nil
	}
	data, err := json.Marshal(rel)
	if err != nil {
		return "", fmt.Errorf("marshal releases: %w", err)
	}
	sha := ObjectSHA(data)
	if cache != nil {
		// Copy: rel.releases aliases the live meta.releases map, which the
		// caller keeps mutating; the cache must hold a snapshot.
		cache.releases = &cachedReleases{releases: maps.Clone(rel.Releases), sha: sha}
	}
	if known == nil || !known(sha) {
		if err := emit(sha, data); err != nil {
			return "", err
		}
	}
	return sha, nil
}

// BuildTree serializes a flat RepoMetadata into the Merkle object set. The
// flat model is unchanged; this is purely a serialization step. Callers must
// pass a normalized tree so entry ordering (and therefore object shas) is
// deterministic. A tree whose entries have missing parent directories is
// rejected: nodes are built only for stored directories, so an orphaned file
// or directory group would be silently dropped and the round-trip would
// quietly lose data.
//
// Prefer BuildTreeStream on the commit path: this convenience wrapper
// materializes the ENTIRE object set at once (the memory profile the
// streaming API exists to avoid).
func BuildTree(meta *RepoMetadata) (*TreeResult, error) {
	objects := make(map[string][]byte)
	refs, err := BuildTreeStream(meta, nil, nil, func(sha string, data []byte) error {
		objects[sha] = append([]byte(nil), data...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &TreeResult{
		RootSHA:      refs.RootSHA,
		ChunkBuckets: refs.ChunkBuckets,
		ReleasesSHA:  refs.ReleasesSHA,
		Objects:      objects,
	}, nil
}
