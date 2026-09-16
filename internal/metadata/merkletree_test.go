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
	m.chunks[1] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11}
	m.UpsertFile("docs/2024/jan.txt", FileMeta{
		Size: 3, Mode: 0o600, UID: 1000, GID: 1000, UploadedAt: now, ModifiedAt: now,
		Chunks: []int64{2},
	}, now)
	m.chunks[2] = ChunkInfo{Size: 3, Offset: 0, Release: "v2", AssetID: 22}
	m.UpsertFile("photos/link", FileMeta{
		Symlink: "../docs/readme.md", Mode: 0o777, UploadedAt: now, ModifiedAt: now,
	}, now)
	m.UpsertFile("top.txt", FileMeta{
		Size: 1, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{3},
	}, now)
	m.chunks[3] = ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 33}
	m.Root.XAttrs = XAttrMap{"user.root": []byte("r")}
	m.Normalize("demo", now)
	if err := m.Validate(); err != nil {
		t.Fatalf("sample tree invalid: %v", err)
	}
	return m
}

func manifestFrom(t *testing.T, m *RepoMetadata, res *TreeResult) *Manifest {
	t.Helper()
	return &Manifest{
		Version:      CurrentVersion,
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
	t.Parallel()
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
	t.Parallel()
	now := int64(500)
	m := NewRepoMetadata("demo")
	// Two structurally identical directories (same file names, same sizes,
	// same modes, same times) but distinct inodes. The node CONTENT differs
	// only by inode, so to test dedup we make the inodes equal too by hand.
	m.EnsureDirectory("a", now)
	m.EnsureDirectory("b", now)
	m.dirs["a"] = DirMeta{Inode: 10, CreatedAt: now, ModifiedAt: now, Mode: 0o755}
	m.dirs["b"] = DirMeta{Inode: 10, CreatedAt: now, ModifiedAt: now, Mode: 0o755}
	m.files["a/x"] = FileMeta{Inode: 20, Size: 2, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{}}
	m.files["b/x"] = FileMeta{Inode: 20, Size: 2, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{}}
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
	t.Parallel()
	m := sampleTree(t)
	before, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build before: %v", err)
	}
	// Map each directory path to its node sha by walking the loaded tree.
	beforeShas := map[string]string{}
	collectNodeShas(t, m, before, beforeShas)

	// Mutate one deep file's mode (a leaf change).
	f := m.files["docs/2024/jan.txt"]
	f.Mode = 0o640
	m.files["docs/2024/jan.txt"] = f
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
	t.Parallel()
	now := int64(700)
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("d", now)
	m.EnsureRelease("v1", now)
	// One chunk in bucket 0, one in bucket 1, one in bucket 2.
	ids := []int64{5, ChunkBucketSize + 7, 2*ChunkBucketSize + 9}
	for _, id := range ids {
		m.chunks[id] = ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: id}
	}
	m.files["d/f"] = FileMeta{Inode: 9, Size: 3, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: ids}
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
		if _, ok := loaded.chunks[id]; !ok {
			t.Fatalf("chunk %d lost in round-trip", id)
		}
	}
}

// BuildTree must not silently drop entries whose parent directory is
// missing from Dirs - the round-trip identity would be broken without a word.
func TestBuildTreeRejectsOrphanEntries(t *testing.T) {
	t.Parallel()
	orphanFile := NewRepoMetadata("demo")
	orphanFile.files["ghost/f"] = FileMeta{Inode: 5, Size: 0}
	if _, err := BuildTree(orphanFile); err == nil {
		t.Fatal("file under a missing parent directory built silently")
	}
	orphanDir := NewRepoMetadata("demo")
	orphanDir.dirs["ghost/deep"] = DirMeta{Inode: 6, CreatedAt: 1, ModifiedAt: 1}
	if _, err := BuildTree(orphanDir); err == nil {
		t.Fatal("directory under a missing parent built silently")
	}
	// A normalized tree with complete parents still builds.
	m := sampleTree(t)
	if _, err := BuildTree(m); err != nil {
		t.Fatalf("valid tree rejected: %v", err)
	}
}

// LoadTree must reconcile the allocation counters against the content it
// loaded; a stale manifest must not yield a tree whose counters sit behind
// live ids (callers like loadIndexTreeAtRef never Normalize).
func TestLoadTreeReconcilesCounters(t *testing.T) {
	t.Parallel()
	m := sampleTree(t)
	res, err := BuildTree(m)
	if err != nil {
		t.Fatal(err)
	}
	manifest := manifestFrom(t, m, res)
	manifest.NextInode = 2
	manifest.NextChunkID = 1
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
	maxInode := loaded.Root.Inode
	for _, d := range loaded.dirs {
		if d.Inode > maxInode {
			maxInode = d.Inode
		}
	}
	for _, f := range loaded.files {
		if f.Inode > maxInode {
			maxInode = f.Inode
		}
	}
	if loaded.NextInode <= maxInode {
		t.Fatalf("NextInode %d not past max live inode %d", loaded.NextInode, maxInode)
	}
	maxChunk := int64(0)
	for id := range loaded.chunks {
		if id > maxChunk {
			maxChunk = id
		}
	}
	if loaded.NextChunkID <= maxChunk {
		t.Fatalf("NextChunkID %d not past max live chunk %d", loaded.NextChunkID, maxChunk)
	}
	// A fresh manifest's counters survive untouched (round-trip identity).
	fresh, err := LoadTree(manifestFrom(t, m, res), func(sha string) ([]byte, error) {
		return res.Objects[sha], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.NextInode != m.NextInode || fresh.NextChunkID != m.NextChunkID {
		t.Fatalf("healthy counters were disturbed: ni %d->%d, nc %d->%d",
			m.NextInode, fresh.NextInode, m.NextChunkID, fresh.NextChunkID)
	}
}

// Manifest references are content addresses; ParseManifest must reject
// anything that is not 64-char lowercase hex before ObjectPath builds repo
// paths out of arbitrary text.
func TestParseManifestValidatesSHAShape(t *testing.T) {
	t.Parallel()
	good := &Manifest{
		Version: CurrentVersion, Project: "demo",
		TreeRoot: strings.Repeat("a", 64), Releases: strings.Repeat("b", 64),
		ChunkBuckets: []string{strings.Repeat("c", 64)},
	}
	data, err := MarshalManifest(good)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseManifest(data); err != nil {
		t.Fatalf("well-shaped manifest rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Manifest){
		"uppercase tree root":  func(mf *Manifest) { mf.TreeRoot = strings.Repeat("A", 64) },
		"short tree root":      func(mf *Manifest) { mf.TreeRoot = "deadbeef" },
		"path traversal root":  func(mf *Manifest) { mf.TreeRoot = "../../etc/passwd" },
		"bad releases sha":     func(mf *Manifest) { mf.Releases = "x" },
		"bad chunk bucket sha": func(mf *Manifest) { mf.ChunkBuckets = []string{"nope"} },
	} {
		bad := *good
		mutate(&bad)
		raw, err := MarshalManifest(&bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseManifest(raw); err == nil {
			t.Fatalf("%s accepted by ParseManifest", name)
		}
	}
}

// LoadTree must reject an object whose bytes no longer hash to the sha it
// was referenced by (bit-rot in the caller's cache must not poison the tree).
func TestLoadTreeVerifiesContentAddresses(t *testing.T) {
	t.Parallel()
	m := sampleTree(t)
	res, err := BuildTree(m)
	if err != nil {
		t.Fatal(err)
	}
	manifest := manifestFrom(t, m, res)
	rotted := make(map[string][]byte, len(res.Objects))
	for sha, data := range res.Objects {
		rotted[sha] = data
	}
	rotted[manifest.TreeRoot] = append([]byte{}, rotted[manifest.TreeRoot][:len(rotted[manifest.TreeRoot])-1]...)
	_, err = LoadTree(manifest, func(sha string) ([]byte, error) {
		d, ok := rotted[sha]
		if !ok {
			return nil, fmt.Errorf("missing %s", sha)
		}
		return d, nil
	})
	if err == nil || !strings.Contains(err.Error(), "content-address") {
		t.Fatalf("rotted root object accepted: %v", err)
	}
}

func TestObjectAddressing(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	if len(loaded.files) != 0 || len(loaded.dirs) != 0 || loaded.Root.Inode != m.Root.Inode {
		t.Fatalf("empty round-trip lost root or gained entries: %+v", loaded)
	}
}
