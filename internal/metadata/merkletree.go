package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// The v6 index splits the single metadata blob into a Merkle hierarchy of
// content-addressed objects plus a small manifest (the only CAS point). The
// in-memory RepoMetadata model stays FLAT; this file is purely a
// (de)serialization layer between the flat maps and the object set.
//
// Layout of one project's index:
//
//	.storhub/index.json                 the manifest (Manifest, version 6)
//	.storhub/objects/<2-hex>/<62-hex>   content-addressed objects (sha256)
//
// Object kinds: TreeNode (one directory), ChunkBucket (a range of chunk
// records), ReleasesObject (the whole release catalog). A node references its
// subdirectories by child sha, so an unchanged subtree dedups to one object
// and a mutation rewrites only the chain from the changed node to the root.

// ChunkBucketSize is the number of chunk IDs packed into one bucket object.
// Bucketing by id/ChunkBucketSize keeps objects small and gives append
// locality: freshly allocated chunks land in the highest bucket, so older
// buckets stay immutable and dedup across commits.
const ChunkBucketSize = 65536

// Manifest is the v6 index manifest: the single contended CAS point of a
// split-layout project. Its Version field carries the ONE metadata document
// version (maxMetadataVersion = 6); there is no separate index-format number.
// Everything the manifest does not name lives in objects. Stats are an
// ADVISORY hint (recomputed authoritatively on load); the counters live here
// because the manifest is small, always loaded, and already the CAS point.
type Manifest struct {
	Version      int           `json:"v"`
	Project      string        `json:"p"`
	TreeRoot     string        `json:"tr"`
	ChunkBuckets []string      `json:"cb,omitempty"`
	Releases     string        `json:"rl"`
	ObjectCount  uint64        `json:"oc,omitempty"`
	NextInode    uint64        `json:"ni,omitempty"`
	NextChunkID  int64         `json:"nc,omitempty"`
	Stats        ManifestStats `json:"st"`
	// LastMod is a Unix NANOSECONDS timestamp (time.Time.UnixNano).
	LastMod int64 `json:"lm,omitempty"`
}

// ManifestStats is the advisory file/byte hint carried in the manifest.
type ManifestStats struct {
	Files int   `json:"f"`
	Bytes int64 `json:"b"`
}

// TreeNode is one directory's serialized node: its own attributes plus its
// immediate children. File entries are inline (full state); subdirectories
// are referenced by child-node sha.
type TreeNode struct {
	Meta    DirMeta             `json:"m"`
	Files   map[string]FileMeta `json:"f,omitempty"`
	Subdirs map[string]string   `json:"s,omitempty"`
}

// ChunkBucket holds the chunk records whose IDs fall in
// [Index*ChunkBucketSize, (Index+1)*ChunkBucketSize).
type ChunkBucket struct {
	Index  int64               `json:"i"`
	Chunks map[int64]ChunkInfo `json:"c"`
}

// ReleasesObject is the whole release catalog. Dozens of entries forever, so
// it never needs splitting.
type ReleasesObject struct {
	Releases map[string]ReleaseRef `json:"r"`
}

// TreeResult is the object set produced by BuildTree plus the manifest
// references into it.
type TreeResult struct {
	RootSHA      string
	ChunkBuckets []string
	ReleasesSHA  string
	Objects      map[string][]byte
}

// TreeRefs are the manifest references produced by one (streaming) build.
type TreeRefs struct {
	RootSHA      string
	ChunkBuckets []string
	ReleasesSHA  string
}

// TreeEmitter receives one Merkle object per call, as soon as it is built.
// The data slice is only valid for the duration of the call: retain it with
// a copy. An error aborts the build.
type TreeEmitter func(sha string, data []byte) error

// TreeCache carries per-object build state across commits so an unchanged
// subtree is neither re-marshalled nor re-emitted: content equality is
// decided by exact structural comparison of a node's inputs (its DirMeta,
// its inline file entries, its child shas), which is equivalent to byte
// equality of the serialized node. A cache is valid only for the tree it
// was built from plus the caller's retained object store (an object the
// cache skips was emitted to the caller before); it is NOT safe for
// concurrent use.
type TreeCache struct {
	nodes    map[string]*cachedNode
	buckets  map[int64]*cachedBucket
	releases *cachedReleases
}

type cachedNode struct {
	meta    DirMeta
	files   map[string]FileMeta
	subdirs map[string]string
	sha     string
}

type cachedBucket struct {
	chunks map[int64]ChunkInfo
	sha    string
}

type cachedReleases struct {
	releases map[string]ReleaseRef
	sha      string
}

// NewTreeCache returns an empty build cache for one project's commit loop.
func NewTreeCache() *TreeCache {
	return &TreeCache{
		nodes:   make(map[string]*cachedNode),
		buckets: make(map[int64]*cachedBucket),
	}
}

// Clone returns an isolated copy of the cache for one build attempt: all
// cache writes replace whole entry pointers (never mutate in place), so
// sharing the pointed-to entries is safe — the copy observes the same
// baseline while the build's putNodes land only in the copy. The caller
// swaps the copy in on commit success and drops it on failure, which is
// what keeps "cache hit" equivalent to "already stored upstream": a failed
// build must never poison the shared cache with objects it never uploaded.
// Nil-safe: a nil cache clones to an empty one.
func (c *TreeCache) Clone() *TreeCache {
	out := NewTreeCache()
	if c == nil {
		return out
	}
	for p, n := range c.nodes {
		out.nodes[p] = n
	}
	for i, b := range c.buckets {
		out.buckets[i] = b
	}
	out.releases = c.releases
	return out
}

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

// ObjectSHA is the content address of an object's canonical bytes.
func ObjectSHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ObjectPath maps an object sha to its repo-relative path
// (objects/<2-hex>/<62-hex>).
func ObjectPath(sha string) string {
	if len(sha) <= 2 {
		return "objects/" + sha
	}
	return "objects/" + sha[:2] + "/" + sha[2:]
}

// IsManifest reports whether a serialized blob is a split-index manifest
// (as opposed to a v1-v5 single metadata document). Detection is by shape: a
// manifest carries a non-empty tree root. Version 6 is current; version 5
// with a tree root is a seconds-era manifest that ParseManifest/LoadTree
// still accepts (and migrates to nanoseconds on load). A version-5 document
// WITHOUT a tree root is a current blob, not a manifest.
func IsManifest(data []byte) bool {
	probe, err := probeVersion(data)
	if err != nil {
		return false
	}
	if probe.V == nil || probe.TreeRoot == "" {
		return false
	}
	return *probe.V == maxMetadataVersion || *probe.V == maxBlobVersion
}

// ParseManifest decodes a manifest. Version 6 is current; version 5 is the
// seconds-era manifest, still accepted so LoadTree can migrate its
// timestamps to nanoseconds on load. Every object reference must be a
// sha256 content address (64-char lowercase hex): the storage layer builds
// repo paths from these strings via ObjectPath, so arbitrary text must never
// survive the parse boundary.
func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	if m.Version != maxMetadataVersion && m.Version != maxBlobVersion {
		return nil, fmt.Errorf("manifest version %d is not %d", m.Version, maxMetadataVersion)
	}
	if m.TreeRoot == "" {
		return nil, fmt.Errorf("manifest has no tree root")
	}
	if !isContentSHA(m.TreeRoot) {
		return nil, fmt.Errorf("manifest tree root %q is not a sha256 content address", m.TreeRoot)
	}
	if m.Releases != "" && !isContentSHA(m.Releases) {
		return nil, fmt.Errorf("manifest releases sha %q is not a sha256 content address", m.Releases)
	}
	for i, sha := range m.ChunkBuckets {
		if !isContentSHA(sha) {
			return nil, fmt.Errorf("manifest chunk bucket %d sha %q is not a sha256 content address", i, sha)
		}
	}
	return &m, nil
}

// isContentSHA reports whether s is a sha256 hex digest: exactly 64
// lowercase hex characters.
func isContentSHA(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// MarshalManifest serializes a manifest deterministically.
func MarshalManifest(m *Manifest) ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	return data, nil
}

// LoadTree reconstructs a flat RepoMetadata from a manifest, fetching each
// referenced object through getObject (which the caller backs with the
// content-addressed cache + repo). Every object is verified against its
// content address here, so a bit-rotted cache entry cannot poison the tree
// even if the fetch layer returns it unchecked. The returned tree is not yet
// normalized; callers Normalize/RecomputeStats as they do after any load.
func LoadTree(manifest *Manifest, getObject func(sha string) ([]byte, error)) (*RepoMetadata, error) {
	if manifest == nil {
		return nil, fmt.Errorf("nil manifest")
	}
	meta := newBareRepoMetadata()
	meta.Version = maxMetadataVersion
	meta.Project = manifest.Project
	meta.NextInode = manifest.NextInode
	meta.NextChunkID = manifest.NextChunkID
	meta.LastMod = manifest.LastMod
	if err := loadNode(meta, "", manifest.TreeRoot, getObject, map[string]bool{}); err != nil {
		return nil, err
	}
	for _, sha := range manifest.ChunkBuckets {
		data, err := getObject(sha)
		if err != nil {
			return nil, fmt.Errorf("load chunk bucket %s: %w", shortObj(sha), err)
		}
		if err := verifyObject(sha, data, "chunk bucket"); err != nil {
			return nil, err
		}
		var b ChunkBucket
		if err := json.Unmarshal(data, &b); err != nil {
			return nil, fmt.Errorf("decode chunk bucket %s: %w", shortObj(sha), err)
		}
		for id, info := range b.Chunks {
			// Buckets are content-addressed by id range: a mis-bucketed
			// or duplicated id is corruption, not data. (Negative ids
			// can quotient-match bucket 0 under truncating division, so
			// they are rejected outright.)
			if id < 0 {
				return nil, fmt.Errorf("chunk bucket %s: negative chunk id %d", shortObj(sha), id)
			}
			if id/ChunkBucketSize != b.Index {
				return nil, fmt.Errorf("chunk bucket %s: chunk id %d does not belong to bucket index %d", shortObj(sha), id, b.Index)
			}
			if _, dup := meta.chunks[id]; dup {
				return nil, fmt.Errorf("chunk bucket %s: duplicate chunk id %d", shortObj(sha), id)
			}
			meta.chunks[id] = info
		}
	}
	if manifest.Releases != "" {
		data, err := getObject(manifest.Releases)
		if err != nil {
			return nil, fmt.Errorf("load releases %s: %w", shortObj(manifest.Releases), err)
		}
		if err := verifyObject(manifest.Releases, data, "releases"); err != nil {
			return nil, err
		}
		var rel ReleasesObject
		if err := json.Unmarshal(data, &rel); err != nil {
			return nil, fmt.Errorf("decode releases: %w", err)
		}
		for tag, ref := range rel.Releases {
			meta.releases[tag] = ref
		}
	}
	if manifest.Version < maxMetadataVersion {
		// Seconds-era manifest: its objects carry seconds timestamps.
		migrateTreeTimesToNano(meta)
	}
	meta.RecomputeStats()
	// Reconcile the allocation counters against the content actually loaded:
	// a stale or regressed manifest must not yield a tree whose counters sit
	// behind live ids (callers of LoadTree may never Normalize).
	meta.reconcileCounters()
	return meta, nil
}

// loadNode recursively loads a directory node and its subtree. chain is
// the sha chain from the root to this node's parent: a sha repeating on
// its OWN ancestor chain is a corrupt cycle, but the same object shared
// by two paths (BuildTree dedups structurally identical subtrees to one
// object) is legitimate sharing and is applied once per path.
func loadNode(meta *RepoMetadata, dirPath, sha string, getObject func(string) ([]byte, error), chain map[string]bool) error {
	if sha == "" {
		return fmt.Errorf("empty node sha at %q", dirPath)
	}
	if chain[sha] {
		return fmt.Errorf("cycle in tree objects at %q (sha %s)", dirPath, shortObj(sha))
	}
	chain[sha] = true
	defer delete(chain, sha)
	data, err := getObject(sha)
	if err != nil {
		return fmt.Errorf("load tree node %q: %w", dirPath, err)
	}
	if err := verifyObject(sha, data, fmt.Sprintf("tree node %q", dirPath)); err != nil {
		return err
	}
	var node TreeNode
	if err := json.Unmarshal(data, &node); err != nil {
		return fmt.Errorf("decode tree node %q: %w", dirPath, err)
	}
	if dirPath == "" {
		meta.Root = node.Meta
	} else {
		meta.dirs[dirPath] = node.Meta
	}
	for name, f := range node.Files {
		meta.files[joinStored(dirPath, name)] = f
	}
	for name, child := range node.Subdirs {
		if err := loadNode(meta, joinStored(dirPath, name), child, getObject, chain); err != nil {
			return err
		}
	}
	return nil
}

func joinStored(dirPath, name string) string {
	if dirPath == "" {
		return name
	}
	return dirPath + "/" + name
}

// verifyObject enforces the content address: bytes whose sha256 does not
// match the sha they were fetched by are corruption, not data.
func verifyObject(sha string, data []byte, what string) error {
	if got := ObjectSHA(data); got != sha {
		return fmt.Errorf("%s %s failed content-address check (sha256 %s)", what, shortObj(sha), shortObj(got))
	}
	return nil
}

func nodeDepth(p string) int {
	if p == "" {
		return 0
	}
	return strings.Count(p, "/") + 1
}

func shortObj(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// loadTreeFetchParallelism bounds concurrent object fetches during a
// parallel tree load. Loads are network-bound, so the bound sits above
// NumCPU while staying a polite upstream citizen per load (the
// per-minute rate governor still applies on top).
const loadTreeFetchParallelism = 8

// LoadTreeParallel reconstructs the same tree as LoadTree, fetching
// independent objects concurrently: sibling subtrees, chunk buckets, and
// the release catalog share no data dependencies (content-addressed and
// immutable), so a cold load pays one round per tree LEVEL instead of one
// per object. Verification, cycle detection, and error wordings match
// LoadTree; fetched bytes are memoized, so a deduped object shared by two
// paths still travels the wire once.
func LoadTreeParallel(manifest *Manifest, getObject func(sha string) ([]byte, error)) (*RepoMetadata, error) {
	if manifest == nil {
		return nil, fmt.Errorf("nil manifest")
	}
	if manifest.TreeRoot == "" {
		return nil, fmt.Errorf("empty node sha at %q", "")
	}
	l := &parallelTreeLoader{
		meta:    newBareRepoMetadata(),
		fetched: make(map[string][]byte),
		sem:     make(chan struct{}, loadTreeFetchParallelism),
		fetch:   getObject,
	}
	l.meta.Version = maxMetadataVersion
	l.meta.Project = manifest.Project
	l.meta.NextInode = manifest.NextInode
	l.meta.NextChunkID = manifest.NextChunkID
	l.meta.LastMod = manifest.LastMod
	// The root gates everything (its child list is discovered from its
	// bytes), so it loads inline; every independent fetch fans out below.
	rootData, err := l.get(manifest.TreeRoot)
	if err != nil {
		return nil, fmt.Errorf("load tree node %q: %w", "", err)
	}
	if err := verifyObject(manifest.TreeRoot, rootData, fmt.Sprintf("tree node %q", "")); err != nil {
		return nil, err
	}
	var root TreeNode
	if err := json.Unmarshal(rootData, &root); err != nil {
		return nil, fmt.Errorf("decode tree node %q: %w", "", err)
	}
	l.meta.Root = root.Meta
	for name, f := range root.Files {
		l.meta.files[name] = f
	}
	chain := []string{manifest.TreeRoot}
	for name, child := range root.Subdirs {
		l.spawnSubtree(name, child, chain)
	}
	for _, sha := range manifest.ChunkBuckets {
		l.spawnBucket(sha)
	}
	if manifest.Releases != "" {
		l.spawnReleases(manifest.Releases)
	}
	l.wg.Wait()
	if l.err != nil {
		return nil, l.err
	}
	if manifest.Version < maxMetadataVersion {
		// Seconds-era manifest: its objects carry seconds timestamps.
		migrateTreeTimesToNano(l.meta)
	}
	l.meta.RecomputeStats()
	// Same tail as LoadTree: counters reconcile against loaded content.
	l.meta.reconcileCounters()
	return l.meta, nil
}

// parallelTreeLoader is the shared state of one LoadTreeParallel call.
// Tree inserts and the fetch memo serialize on mu; fetch interrogation
// itself runs off-lock so concurrent GETs overlap. stop short-circuits
// new work after the first failure; in-flight fetches still land (bounded
// waste on corrupt input, never a hang).
type parallelTreeLoader struct {
	meta    *RepoMetadata
	mu      sync.Mutex
	fetched map[string][]byte
	sem     chan struct{}
	wg      sync.WaitGroup
	stop    atomic.Bool
	errOnce sync.Once
	err     error
	fetch   func(string) ([]byte, error)
}

func (l *parallelTreeLoader) fail(err error) {
	l.errOnce.Do(func() {
		l.err = err
		l.stop.Store(true)
	})
}

// spawn runs fn with bounded parallelism: a free slot goes to a new
// goroutine, a saturated loader runs fn inline in the caller. Blocking
// for a slot while holding one would deadlock wide+deep trees (every
// slot held by a spawner waiting for a slot), and queueing unbounded
// goroutines behind a saturated link trades a network wait for a memory
// pile-up. Saturation already means the link is fully used, so inline
// fallback loses no throughput.
func (l *parallelTreeLoader) spawn(fn func()) {
	if l.stop.Load() {
		return
	}
	l.wg.Add(1)
	select {
	case l.sem <- struct{}{}:
		go func() {
			defer func() { <-l.sem; l.wg.Done() }()
			fn()
		}()
	default:
		defer l.wg.Done()
		fn()
	}
}

// get fetches one object, memoized: a deduped subtree shared by two paths
// is fetched once and applied per path. Concurrent first-flights may
// duplicate one fetch (idempotent GET, identical bytes); correctness never
// depends on uniqueness.
func (l *parallelTreeLoader) get(sha string) ([]byte, error) {
	l.mu.Lock()
	if data, ok := l.fetched[sha]; ok {
		l.mu.Unlock()
		return data, nil
	}
	l.mu.Unlock()
	data, err := l.fetch(sha)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.fetched[sha] = data
	l.mu.Unlock()
	return data, nil
}

func (l *parallelTreeLoader) spawnSubtree(name, sha string, chain []string) {
	// The child chain copies: siblings extend the shared parent chain
	// concurrently, and append may reuse the backing array.
	childChain := make([]string, len(chain)+1)
	copy(childChain, chain)
	childChain[len(chain)] = sha
	dirPath := name
	l.spawn(func() { l.loadSubtree(dirPath, sha, childChain) })
}

func (l *parallelTreeLoader) loadSubtree(dirPath, sha string, chain []string) {
	if l.stop.Load() {
		return
	}
	// chain already ends in sha (extended by the spawner); membership is
	// tested against the ancestors only.
	if slices.Contains(chain[:len(chain)-1], sha) {
		l.fail(fmt.Errorf("cycle in tree objects at %q (sha %s)", dirPath, shortObj(sha)))
		return
	}
	data, err := l.get(sha)
	if err != nil {
		l.fail(fmt.Errorf("load tree node %q: %w", dirPath, err))
		return
	}
	if err := verifyObject(sha, data, fmt.Sprintf("tree node %q", dirPath)); err != nil {
		l.fail(err)
		return
	}
	var node TreeNode
	if err := json.Unmarshal(data, &node); err != nil {
		l.fail(fmt.Errorf("decode tree node %q: %w", dirPath, err))
		return
	}
	l.mu.Lock()
	l.meta.dirs[dirPath] = node.Meta
	for name, f := range node.Files {
		l.meta.files[joinStored(dirPath, name)] = f
	}
	l.mu.Unlock()
	for name, child := range node.Subdirs {
		l.spawnSubtree(joinStored(dirPath, name), child, chain)
	}
}

func (l *parallelTreeLoader) spawnBucket(sha string) {
	l.spawn(func() { l.loadBucket(sha) })
}

func (l *parallelTreeLoader) loadBucket(sha string) {
	if l.stop.Load() {
		return
	}
	data, err := l.get(sha)
	if err != nil {
		l.fail(fmt.Errorf("load chunk bucket %s: %w", shortObj(sha), err))
		return
	}
	if err := verifyObject(sha, data, "chunk bucket"); err != nil {
		l.fail(err)
		return
	}
	var b ChunkBucket
	if err := json.Unmarshal(data, &b); err != nil {
		l.fail(fmt.Errorf("decode chunk bucket %s: %w", shortObj(sha), err))
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, info := range b.Chunks {
		if id < 0 {
			l.fail(fmt.Errorf("chunk bucket %s: negative chunk id %d", shortObj(sha), id))
			return
		}
		if id/ChunkBucketSize != b.Index {
			l.fail(fmt.Errorf("chunk bucket %s: chunk id %d does not belong to bucket index %d", shortObj(sha), id, b.Index))
			return
		}
		if _, dup := l.meta.chunks[id]; dup {
			l.fail(fmt.Errorf("chunk bucket %s: duplicate chunk id %d", shortObj(sha), id))
			return
		}
		l.meta.chunks[id] = info
	}
}

func (l *parallelTreeLoader) spawnReleases(sha string) {
	l.spawn(func() { l.loadReleases(sha) })
}

func (l *parallelTreeLoader) loadReleases(sha string) {
	if l.stop.Load() {
		return
	}
	data, err := l.get(sha)
	if err != nil {
		l.fail(fmt.Errorf("load releases %s: %w", shortObj(sha), err))
		return
	}
	if err := verifyObject(sha, data, "releases"); err != nil {
		l.fail(err)
		return
	}
	var rel ReleasesObject
	if err := json.Unmarshal(data, &rel); err != nil {
		l.fail(fmt.Errorf("decode releases: %w", err))
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for tag, ref := range rel.Releases {
		l.meta.releases[tag] = ref
	}
}
