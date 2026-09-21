package metadata

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

// ChunkBucketSize is the fixed content bucket width for tree streaming.
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
