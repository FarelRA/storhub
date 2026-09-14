package metadata

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// sampleTree builds a normalized metadata tree with nested dirs, files with
// chunks, a symlink, xattrs, and a release catalog - everything a round-trip
// must preserve.
func sampleTree(t *testing.T) *RepoMetadata {
	t.Helper()
	now := int64(1000)
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("docs", now)
	m.EnsureDirectory("docs/2024", now)
	m.EnsureDirectory("photos", now)
	m.EnsureRelease("v1", now)
	m.EnsureRelease("v2", now+10)

	m.UpsertFile("docs/readme.md", FileMeta{
		Size: 4, Mode: 0o644, UploadedAt: now, ModifiedAt: now,
		Chunks: []int64{1}, XAttrs: XAttrMap{"user.tag": []byte("keep")},
	}, now)
	m.Chunks[1] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11}
	m.UpsertFile("docs/2024/jan.txt", FileMeta{
		Size: 3, Mode: 0o600, UID: 1000, GID: 1000, UploadedAt: now, ModifiedAt: now,
		Chunks: []int64{2},
	}, now)
	m.Chunks[2] = ChunkInfo{Size: 3, Offset: 0, Release: "v2", AssetID: 22}
	m.UpsertFile("photos/link", FileMeta{
		Symlink: "../docs/readme.md", Mode: 0o777, UploadedAt: now, ModifiedAt: now,
	}, now)
	m.UpsertFile("top.txt", FileMeta{
		Size: 1, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{3},
	}, now)
	m.Chunks[3] = ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 33}
	m.Root.XAttrs = XAttrMap{"user.root": []byte("r")}
	m.Normalize("demo", now)
	if err := m.Validate(); err != nil {
		t.Fatalf("sample tree invalid: %v", err)
	}
	return m
}

func manifestFrom(t *testing.T, m *RepoMetadata, res *TreeResult) *ManifestV2 {
	t.Helper()
	return &ManifestV2{
		Version:      ManifestVersion,
		Project:      m.Project,
		TreeRoot:     res.RootSHA,
		ChunkBuckets: res.ChunkBuckets,
		Releases:     res.ReleasesSHA,
		NextInode:    m.NextInode,
		NextChunkID:  m.NextChunkID,
		Stats:        ManifestStats{Files: m.TotalFiles, Bytes: m.TotalSize},
		LastMod:      m.LastMod,
	}
}

func TestMerkleRoundTripIdentity(t *testing.T) {
	m := sampleTree(t)
	res, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build tree: %v", err)
	}
	manifest := manifestFrom(t, m, res)
	loaded, err := LoadTree(manifest, func(sha string) ([]byte, error) {
		data, ok := res.Objects[sha]
		if !ok {
			return nil, fmt.Errorf("missing object %s", sha)
		}
		return data, nil
	})
	if err != nil {
		t.Fatalf("load tree: %v", err)
	}
	loaded.Normalize("demo", m.LastMod)
	if err := loaded.Validate(); err != nil {
		t.Fatalf("loaded invalid: %v", err)
	}
	// Deep equality of the semantic content: compare canonical JSON of the
	// normalized trees (field order fixed, map keys sorted by encoding/json).
	a, _ := json.Marshal(m)
	b, _ := json.Marshal(loaded)
	if string(a) != string(b) {
		t.Fatalf("round-trip not identity:\n want %s\n  got %s", a, b)
	}
}

func TestMerkleDedupIdenticalSubtrees(t *testing.T) {
	now := int64(500)
	m := NewRepoMetadata("demo")
	// Two structurally identical directories (same file names, same sizes,
	// same modes, same times) but distinct inodes. The node CONTENT differs
	// only by inode, so to test dedup we make the inodes equal too by hand.
	m.EnsureDirectory("a", now)
	m.EnsureDirectory("b", now)
	m.Dirs["a"] = DirMeta{Inode: 10, CreatedAt: now, ModifiedAt: now, Mode: 0o755}
	m.Dirs["b"] = DirMeta{Inode: 10, CreatedAt: now, ModifiedAt: now, Mode: 0o755}
	m.Files["a/x"] = FileMeta{Inode: 20, Size: 2, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{}}
	m.Files["b/x"] = FileMeta{Inode: 20, Size: 2, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{}}
	m.Normalize("demo", now)

	res, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// a and b nodes must be the SAME object (deduped): exactly one node
	// object for both, plus the root node.
	nodeCount := 0
	for _, data := range res.Objects {
		var probe TreeNode
		if err := json.Unmarshal(data, &probe); err == nil && looksLikeTreeNode(data) {
			nodeCount++
		}
	}
	// Root + one shared child node (a and b deduped to one object).
	if nodeCount != 2 {
		t.Fatalf("expected 2 distinct tree node objects (root + deduped child), got %d", nodeCount)
	}
}

func looksLikeTreeNode(data []byte) bool {
	// A TreeNode has the compact keys m/f/s; a ChunkBucket has i/c; a
	// ReleasesObject has r. Distinguish by the presence of "m".
	return strings.Contains(string(data), `"m":`)
}

func TestMerkleMutationRewritesOnlyChain(t *testing.T) {
	m := sampleTree(t)
	before, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build before: %v", err)
	}
	// Map each directory path to its node sha by walking the loaded tree.
	beforeShas := map[string]string{}
	collectNodeShas(t, m, before, beforeShas)

	// Mutate one deep file's mode (a leaf change).
	f := m.Files["docs/2024/jan.txt"]
	f.Mode = 0o640
	m.Files["docs/2024/jan.txt"] = f
	m.Normalize("demo", m.LastMod)

	after, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build after: %v", err)
	}
	afterShas := map[string]string{}
	collectNodeShas(t, m, after, afterShas)

	// The changed file's directory (docs/2024), its ancestors (docs, root)
	// must have new shas. An unrelated sibling subtree (photos) must not.
	if beforeShas["docs/2024"] == afterShas["docs/2024"] {
		t.Fatal("expected docs/2024 node sha to change")
	}
	if beforeShas["docs"] == afterShas["docs"] {
		t.Fatal("expected docs node sha to change (child moved)")
	}
	if beforeShas[""] == afterShas[""] {
		t.Fatal("expected root node sha to change")
	}
	if beforeShas["photos"] != afterShas["photos"] {
		t.Fatal("expected photos subtree sha to be UNCHANGED (chain-only rewrite)")
	}
}

// collectNodeShas walks the tree objects and records dirPath -> node sha by
// re-deriving the structure (test helper only; not used by production code).
func collectNodeShas(t *testing.T, m *RepoMetadata, res *TreeResult, out map[string]string) {
	t.Helper()
	// Rebuild the path->sha map by loading nodes and matching content.
	var walk func(dirPath, sha string)
	walk = func(dirPath, sha string) {
		out[dirPath] = sha
		data := res.Objects[sha]
		var node TreeNode
		if err := json.Unmarshal(data, &node); err != nil {
			t.Fatalf("unmarshal node %s: %v", dirPath, err)
		}
		for name, child := range node.Subdirs {
			p := name
			if dirPath != "" {
				p = dirPath + "/" + name
			}
			walk(p, child)
		}
	}
	walk("", res.RootSHA)
}

func TestMerkleChunkBucketsByIndex(t *testing.T) {
	now := int64(700)
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("d", now)
	m.EnsureRelease("v1", now)
	// One chunk in bucket 0, one in bucket 1, one in bucket 2.
	ids := []int64{5, ChunkBucketSize + 7, 2*ChunkBucketSize + 9}
	for _, id := range ids {
		m.Chunks[id] = ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: id}
	}
	m.Files["d/f"] = FileMeta{Inode: 9, Size: 3, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: ids}
	m.NextChunkID = 3*ChunkBucketSize + 1
	m.Normalize("demo", now)

	res, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(res.ChunkBuckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(res.ChunkBuckets))
	}
	// Buckets must be ordered by index ascending.
	for i, sha := range res.ChunkBuckets {
		var b ChunkBucket
		if err := json.Unmarshal(res.Objects[sha], &b); err != nil {
			t.Fatalf("unmarshal bucket: %v", err)
		}
		if b.Index != int64(i) {
			t.Fatalf("bucket %d has index %d (want ordered)", i, b.Index)
		}
	}
	// Round-trip preserves every chunk record.
	manifest := manifestFrom(t, m, res)
	loaded, err := LoadTree(manifest, func(sha string) ([]byte, error) {
		d, ok := res.Objects[sha]
		if !ok {
			return nil, fmt.Errorf("missing %s", sha)
		}
		return d, nil
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, id := range ids {
		if _, ok := loaded.Chunks[id]; !ok {
			t.Fatalf("chunk %d lost in round-trip", id)
		}
	}
}

func TestObjectAddressing(t *testing.T) {
	data := []byte(`{"hello":"world"}`)
	sha := ObjectSHA(data)
	if len(sha) != 64 {
		t.Fatalf("sha256 hex must be 64 chars, got %d", len(sha))
	}
	// Deterministic: same bytes -> same sha.
	if ObjectSHA(data) != sha {
		t.Fatal("ObjectSHA not deterministic")
	}
	// Different bytes -> different sha.
	if ObjectSHA([]byte(`{"hello":"wrld"}`)) == sha {
		t.Fatal("collision on different content")
	}
	// Path: objects/<2-hex>/<62-hex>.
	p := ObjectPath(sha)
	if !strings.HasPrefix(p, "objects/"+sha[:2]+"/") {
		t.Fatalf("bad object path %q", p)
	}
	if strings.TrimPrefix(p, "objects/"+sha[:2]+"/") != sha[2:] {
		t.Fatalf("bad object path tail %q", p)
	}
}

func TestMerkleEmptyTreeRoundTrips(t *testing.T) {
	m := NewRepoMetadata("demo")
	m.Normalize("demo", 42)
	res, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build empty: %v", err)
	}
	manifest := manifestFrom(t, m, res)
	loaded, err := LoadTree(manifest, func(sha string) ([]byte, error) {
		d, ok := res.Objects[sha]
		if !ok {
			return nil, fmt.Errorf("missing %s", sha)
		}
		return d, nil
	})
	if err != nil {
		t.Fatalf("load empty: %v", err)
	}
	loaded.Normalize("demo", 42)
	if len(loaded.Files) != 0 || len(loaded.Dirs) != 0 || loaded.Root.Inode != m.Root.Inode {
		t.Fatalf("empty round-trip lost root or gained entries: %+v", loaded)
	}
}
