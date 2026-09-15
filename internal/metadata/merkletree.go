package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
)

// The v5 index splits the single metadata blob into a Merkle hierarchy of
// content-addressed objects plus a small manifest (the only CAS point). The
// in-memory RepoMetadata model stays FLAT; this file is purely a
// (de)serialization layer between the flat maps and the object set.
//
// Layout of one project's index:
//
//	.storhub/index.json                 the manifest (Manifest, version 5)
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

// Manifest is the v5 index manifest: the single contended CAS point of a
// split-layout project. Its Version field carries the ONE metadata document
// version (maxMetadataVersion = 5); there is no separate index-format number.
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
	LastMod      int64         `json:"lm,omitempty"`
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

// IsManifest reports whether a serialized blob is a v5 split-index manifest
// (as opposed to a v1-v4 single metadata document). Detection is by shape: a
// manifest carries the current document version and a non-empty tree root.
func IsManifest(data []byte) bool {
	var probe struct {
		V        *int   `json:"v"`
		TreeRoot string `json:"tr"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return false
	}
	return probe.V != nil && *probe.V == maxMetadataVersion && probe.TreeRoot != ""
}

// ParseManifest decodes a v5 manifest. Every object reference must be a
// sha256 content address (64-char lowercase hex): the storage layer builds
// repo paths from these strings via ObjectPath, so arbitrary text must never
// survive the parse boundary.
func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}
	if m.Version != maxMetadataVersion {
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

// MarshalManifest serializes a v5 manifest deterministically.
func MarshalManifest(m *Manifest) ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	return data, nil
}

// BuildTree serializes a flat RepoMetadata into the Merkle object set. The
// flat model is unchanged; this is purely a serialization step. Callers must
// pass a normalized tree so entry ordering (and therefore object shas) is
// deterministic. A tree whose entries have missing parent directories is
// rejected: nodes are built only for stored directories, so an orphaned file
// or directory group would be silently dropped and the round-trip would
// quietly lose data.
func BuildTree(meta *RepoMetadata) (*TreeResult, error) {
	objects := make(map[string][]byte)

	// Round-trip identity guard: every non-root entry's parent must exist,
	// or its group would never be serialized.
	for p := range meta.Files {
		if parent := parentPath(p); parent != "" {
			if _, ok := meta.Dirs[parent]; !ok {
				return nil, fmt.Errorf("build tree: file %q has no parent directory %q", p, parent)
			}
		}
	}
	for p := range meta.Dirs {
		if parent := parentPath(p); parent != "" {
			if _, ok := meta.Dirs[parent]; !ok {
				return nil, fmt.Errorf("build tree: directory %q has no parent directory %q", p, parent)
			}
		}
	}

	// Group files by parent directory (keyed by base name within the node).
	filesByParent := make(map[string]map[string]FileMeta, len(meta.Files))
	for p, f := range meta.Files {
		parent := parentPath(p)
		if filesByParent[parent] == nil {
			filesByParent[parent] = make(map[string]FileMeta)
		}
		filesByParent[parent][path.Base(p)] = f
	}

	// Every directory (root sentinel "" plus each stored dir) becomes a node.
	dirs := make([]string, 0, len(meta.Dirs)+1)
	dirs = append(dirs, "")
	for p := range meta.Dirs {
		dirs = append(dirs, p)
	}
	// Deepest-first so a node's child shas exist before the node is hashed.
	sort.Slice(dirs, func(i, j int) bool { return nodeDepth(dirs[i]) > nodeDepth(dirs[j]) })

	subdirShas := make(map[string]map[string]string, len(dirs))
	nodeSHA := make(map[string]string, len(dirs))
	for _, d := range dirs {
		node := TreeNode{Files: filesByParent[d], Subdirs: subdirShas[d]}
		if d == "" {
			node.Meta = meta.Root
		} else {
			node.Meta = meta.Dirs[d]
		}
		data, err := json.Marshal(node)
		if err != nil {
			return nil, fmt.Errorf("marshal tree node %q: %w", d, err)
		}
		sha := ObjectSHA(data)
		objects[sha] = data
		nodeSHA[d] = sha
		if d != "" {
			parent := parentPath(d)
			if subdirShas[parent] == nil {
				subdirShas[parent] = make(map[string]string)
			}
			subdirShas[parent][path.Base(d)] = sha
		}
	}

	buckets, err := buildChunkBuckets(meta, objects)
	if err != nil {
		return nil, err
	}

	rel := ReleasesObject{Releases: meta.Releases}
	if rel.Releases == nil {
		rel.Releases = map[string]ReleaseRef{}
	}
	relData, err := json.Marshal(rel)
	if err != nil {
		return nil, fmt.Errorf("marshal releases: %w", err)
	}
	releasesSHA := ObjectSHA(relData)
	objects[releasesSHA] = relData

	return &TreeResult{
		RootSHA:      nodeSHA[""],
		ChunkBuckets: buckets,
		ReleasesSHA:  releasesSHA,
		Objects:      objects,
	}, nil
}

func buildChunkBuckets(meta *RepoMetadata, objects map[string][]byte) ([]string, error) {
	if len(meta.Chunks) == 0 {
		return nil, nil
	}
	byBucket := make(map[int64]map[int64]ChunkInfo)
	for id, info := range meta.Chunks {
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
		b := ChunkBucket{Index: idx, Chunks: byBucket[idx]}
		data, err := json.Marshal(b)
		if err != nil {
			return nil, fmt.Errorf("marshal chunk bucket %d: %w", idx, err)
		}
		sha := ObjectSHA(data)
		objects[sha] = data
		shas = append(shas, sha)
	}
	return shas, nil
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
	meta := &RepoMetadata{
		Version:     maxMetadataVersion,
		Project:     manifest.Project,
		NextInode:   manifest.NextInode,
		NextChunkID: manifest.NextChunkID,
		LastMod:     manifest.LastMod,
		Dirs:        make(map[string]DirMeta),
		Files:       make(map[string]FileMeta),
		Chunks:      make(map[int64]ChunkInfo),
		Releases:    make(map[string]ReleaseRef),
	}
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
			meta.Chunks[id] = info
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
			meta.Releases[tag] = ref
		}
	}
	meta.RecomputeStats()
	// Reconcile the allocation counters against the content actually loaded:
	// a stale or regressed manifest must not yield a tree whose counters sit
	// behind live ids (callers of LoadTree may never Normalize).
	meta.reconcileCounters()
	return meta, nil
}

// loadNode recursively loads a directory node and its subtree. seen guards
// against a corrupt manifest whose child shas form a cycle.
func loadNode(meta *RepoMetadata, dirPath, sha string, getObject func(string) ([]byte, error), seen map[string]bool) error {
	if sha == "" {
		return fmt.Errorf("empty node sha at %q", dirPath)
	}
	if seen[sha] {
		return fmt.Errorf("cycle in tree objects at %q (sha %s)", dirPath, shortObj(sha))
	}
	seen[sha] = true
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
		meta.Dirs[dirPath] = node.Meta
	}
	for name, f := range node.Files {
		meta.Files[joinStored(dirPath, name)] = f
	}
	for name, child := range node.Subdirs {
		if err := loadNode(meta, joinStored(dirPath, name), child, getObject, seen); err != nil {
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
