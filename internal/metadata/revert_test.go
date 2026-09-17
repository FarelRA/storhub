package metadata

import (
	"testing"
)

func buildTree(t *testing.T, mutate func(m *RepoMetadata)) *RepoMetadata {
	t.Helper()
	m := NewRepoMetadata("demo")
	mutate(m)
	m.Normalize("demo", 1000)
	if err := m.Validate(); err != nil {
		t.Fatalf("fixture invalid: %v", err)
	}
	return m
}

func clonePtr(m *RepoMetadata) *RepoMetadata { c := m.Clone(); return c }

// putTestChunk stores a chunk through the tracked PutChunk mutator instead
// of a white-box map write, so fixtures exercise the same size/index/stats
// accounting production writes use (white-box writes mask drift).
func putTestChunk(t *testing.T, m *RepoMetadata, id int64, info ChunkInfo) {
	t.Helper()
	if err := m.PutChunk(id, info); err != nil {
		t.Fatalf("put test chunk %d: %v", id, err)
	}
}

func TestRevertFileRestoresHistoricalContent(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("docs", 100)
		putTestChunk(t, m, 1, ChunkInfo{Size: 5, Offset: 0, Release: "v1", AssetID: 11})
		m.UpsertFile("docs/a.txt", FileMeta{Size: 5, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	cur := clonePtr(hist)
	// Current diverges: a.txt overwritten with a new chunk.
	putTestChunk(t, cur, 2, ChunkInfo{Size: 9, Offset: 0, Release: "v1", AssetID: 22})
	cur.UpsertFile("docs/a.txt", FileMeta{Size: 9, Mode: 0o644, UploadedAt: 200, ModifiedAt: 200, Chunks: []int64{2}}, 200)
	cur.Normalize("demo", 200)

	if err := RevertSubtree(cur, hist, "docs/a.txt", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	cur.Normalize("demo", 300)
	f := cur.FindFile("docs/a.txt")
	if f == nil || f.Size != 5 {
		t.Fatalf("reverted file wrong: %+v", f)
	}
	// The reverted chunk must resolve to the historical asset.
	if len(f.Chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %v", f.Chunks)
	}
	if c := cur.chunks[f.Chunks[0]]; c.AssetID != 11 {
		t.Fatalf("reverted chunk lost its asset: %+v", c)
	}
}

func TestRevertRestoresDeletedFileAndParent(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("keep", 100)
		m.EnsureDirectory("gone", 100)
		putTestChunk(t, m, 1, ChunkInfo{Size: 3, Offset: 0, Release: "v1", AssetID: 11})
		m.UpsertFile("gone/x.txt", FileMeta{Size: 3, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	cur := clonePtr(hist)
	cur.RemoveFile("gone/x.txt")
	cur.RemoveDirectory("gone")
	cur.Normalize("demo", 200)
	if cur.HasDirectory("gone") {
		t.Fatal("fixture: gone dir should be removed in current")
	}

	if err := RevertSubtree(cur, hist, "gone/x.txt", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	cur.Normalize("demo", 300)
	if !cur.HasDirectory("gone") {
		t.Fatal("revert must restore the missing parent directory")
	}
	if cur.FindFile("gone/x.txt") == nil {
		t.Fatal("reverted file not restored")
	}
}

func TestRevertDirectorySubtree(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("tree", 100)
		m.EnsureDirectory("tree/sub", 100)
		putTestChunk(t, m, 1, ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 11})
		m.UpsertFile("tree/a", FileMeta{Size: 1, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		m.UpsertFile("tree/sub/b", FileMeta{Size: 1, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	cur := clonePtr(hist)
	// Current deleted the whole subtree and added an unrelated file.
	cur.RemoveFile("tree/a")
	cur.RemoveFile("tree/sub/b")
	cur.RemoveDirectory("tree/sub")
	cur.RemoveDirectory("tree")
	cur.UpsertFile("unrelated", FileMeta{Size: 2, Mode: 0o644, UploadedAt: 200, ModifiedAt: 200}, 200)
	cur.Normalize("demo", 200)

	if err := RevertSubtree(cur, hist, "tree", 300); err != nil {
		t.Fatalf("revert subtree: %v", err)
	}
	cur.Normalize("demo", 300)
	if cur.FindFile("tree/a") == nil || cur.FindFile("tree/sub/b") == nil {
		t.Fatalf("subtree not fully restored: %v", cur.Files())
	}
	if cur.FindFile("unrelated") == nil {
		t.Fatal("revert must not touch paths outside the subtree")
	}
}

func TestRevertRemovesPathAbsentInHistory(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("docs", 100)
	})
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("docs", 100)
		putTestChunk(t, m, 1, ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 11})
		m.UpsertFile("docs/new", FileMeta{Size: 1, Mode: 0o644, UploadedAt: 200, ModifiedAt: 200, Chunks: []int64{1}}, 200)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	if err := RevertSubtree(cur, hist, "docs/new", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	cur.Normalize("demo", 300)
	if cur.FindFile("docs/new") != nil {
		t.Fatal("revert of a path absent in history must remove it")
	}
}

func TestRevertRemapsCollidingInodeAndChunk(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		putTestChunk(t, m, 5, ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 55})
		m.UpsertFile("d/target", FileMeta{Size: 4, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{5}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	// Current reused chunk id 5 for DIFFERENT content and holds inode of
	// d/target under another path (collision).
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		putTestChunk(t, m, 5, ChunkInfo{Size: 77, Offset: 0, Release: "v1", AssetID: 999})
		m.UpsertFile("d/other", FileMeta{Size: 77, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{5}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	targetInode := hist.FindFile("d/target").Inode
	// Force a collision: give d/other the same inode the historical target had.
	of := *cur.FindFile("d/other")
	of.Inode = targetInode
	cur.WriteFileDirect("d/other", of)

	if err := RevertSubtree(cur, hist, "d/target", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	cur.Normalize("demo", 300)
	tf := cur.FindFile("d/target")
	if tf == nil {
		t.Fatal("target not restored")
	}
	// The restored file must NOT share an inode with d/other.
	if tf.Inode == cur.FindFile("d/other").Inode {
		t.Fatal("revert clobbered the colliding inode instead of remapping")
	}
	// And its chunk must point at the historical asset (55), not 999.
	if c := cur.chunks[tf.Chunks[0]]; c.AssetID != 55 {
		t.Fatalf("reverted chunk not remapped: %+v", c)
	}
	// d/other must be untouched, INCLUDING its chunk record (the collision
	// must not clobber the live chunk that shares the id).
	other := cur.FindFile("d/other")
	if other == nil || other.Size != 77 {
		t.Fatal("revert damaged the colliding current file")
	}
	if len(other.Chunks) != 1 {
		t.Fatalf("d/other chunk list changed: %v", other.Chunks)
	}
	if c := cur.chunks[other.Chunks[0]]; c.AssetID != 999 {
		t.Fatalf("revert clobbered the colliding live chunk: %+v", c)
	}
}

func TestRevertRestoresMissingRelease(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		if _, err := m.EnsureRelease("v9", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
		putTestChunk(t, m, 1, ChunkInfo{Size: 2, Offset: 0, Release: "v9", AssetID: 11})
		m.UpsertFile("d/f", FileMeta{Size: 2, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
	})
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
	})
	if err := RevertSubtree(cur, hist, "d/f", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	if _, ok := cur.releases["v9"]; !ok {
		t.Fatal("revert must restore the release a reverted chunk references")
	}
}

// Reusing a historical id must advance dst's allocation
// counters past it. The collision test above only covers taken-id remap; this
// covers counter regression: a revision whose persisted ni/nc sit behind the
// ids revert restores.
func TestRevertAdvancesCountersPastReusedIDs(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		putTestChunk(t, m, 500, ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 55})
		m.UpsertFile("d/f", FileMeta{Size: 4, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{500}}, 100)
		f := *m.FindFile("d/f")
		f.Inode = 400
		m.WriteFileDirect("d/f", f)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
	})
	// Simulate a legacy revision whose counters regressed behind history.
	cur.NextChunkID = 11
	cur.NextInode = 12

	if err := RevertSubtree(cur, hist, "d/f", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	if _, live := cur.chunks[500]; !live {
		t.Fatal("fixture: historical chunk 500 not restored")
	}
	if cur.NextChunkID <= 500 {
		t.Fatalf("NextChunkID %d not advanced past reused chunk 500", cur.NextChunkID)
	}
	if cur.NextInode <= 400 {
		t.Fatalf("NextInode %d not advanced past reused inode 400", cur.NextInode)
	}
	// Allocations inside the same operation must not hand out the live ids.
	for i := 0; i < 300; i++ {
		if id := cur.allocateChunkID(); id <= 500 {
			t.Fatalf("AllocateChunkID returned %d while chunk 500 is live", id)
		}
	}
	for i := 0; i < 300; i++ {
		if ino := cur.allocateInode(); ino <= 400 {
			t.Fatalf("AllocateInode returned %d while inode 400 is live", ino)
		}
	}
}

// Reverting one name of a hardlink family must reuse the family inode
// when the colliding live path shares that inode in src too - the sibling is
// related, not unrelated live data.
func TestRevertKeepsHardlinkFamilyIntact(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		putTestChunk(t, m, 1, ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11})
		m.UpsertFile("d/a", FileMeta{Size: 4, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		m.UpsertFile("d/b", FileMeta{Size: 4, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		// Hardlink: d/b shares d/a's inode in history.
		b := *m.FindFile("d/b")
		b.Inode = m.FindFile("d/a").Inode
		m.WriteFileDirect("d/b", b)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	familyInode := hist.FindFile("d/a").Inode
	cur := clonePtr(hist)
	cur.RemoveFile("d/a")
	cur.Normalize("demo", 200)

	if err := RevertSubtree(cur, hist, "d/a", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	a := cur.FindFile("d/a")
	if a == nil {
		t.Fatal("d/a not restored")
	}
	if a.Inode != familyInode {
		t.Fatalf("hardlink family split: restored d/a inode %d, want family inode %d", a.Inode, familyInode)
	}
	if n := cur.NLink(familyInode); n != 2 {
		t.Fatalf("family nlink = %d, want 2", n)
	}
	cur.Normalize("demo", 300)
	if err := cur.Validate(); err != nil {
		t.Fatalf("reverted tree invalid: %v", err)
	}
}

// A colliding holder that is NOT family in src (unrelated
// live data) must still force a remap.
func TestRevertRemapsInodeForUnrelatedHolder(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		putTestChunk(t, m, 1, ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11})
		m.UpsertFile("d/a", FileMeta{Size: 4, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	familyInode := hist.FindFile("d/a").Inode
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		putTestChunk(t, m, 2, ChunkInfo{Size: 9, Offset: 0, Release: "v1", AssetID: 22})
		m.UpsertFile("d/b", FileMeta{Size: 9, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{2}}, 100)
		// d/b is unrelated in src but squats on the historical inode.
		b := *m.FindFile("d/b")
		b.Inode = familyInode
		m.WriteFileDirect("d/b", b)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	if err := RevertSubtree(cur, hist, "d/a", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	a := cur.FindFile("d/a")
	if a == nil {
		t.Fatal("d/a not restored")
	}
	if a.Inode == familyInode {
		t.Fatal("unrelated holder must force a remap")
	}
}

// The helper ensureAncestors must deep-clone the source directory; a shallow struct
// copy aliases src's XAttrs map (and its values) into dst, so a preview
// mutation leaks into the source tree the commit reverts from.
func TestEnsureAncestorsDeepClonesXAttrs(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("x", 100)
		xd := m.dirs["x"]
		xd.XAttrs = XAttrMap{"user.a": []byte("1")}
		m.dirs["x"] = xd
		putTestChunk(t, m, 1, ChunkInfo{Size: 2, Offset: 0, Release: "v1", AssetID: 11})
		m.UpsertFile("x/f", FileMeta{Size: 2, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	cur := clonePtr(hist)
	cur.RemoveFile("x/f")
	cur.RemoveDirectory("x")
	cur.Normalize("demo", 200)

	if err := RevertSubtree(cur, hist, "x/f", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	xd, ok := cur.dirs["x"]
	if !ok {
		t.Fatal("ancestor x not restored")
	}
	xd.XAttrs["user.new"] = []byte("v")
	if _, aliased := hist.dirs["x"].XAttrs["user.new"]; aliased {
		t.Fatal("dst mutation leaked into src: XAttrs map aliased")
	}
	xd.XAttrs["user.a"] = []byte("mutated")
	if string(hist.dirs["x"].XAttrs["user.a"]) != "1" {
		t.Fatal("dst mutation leaked into src: XAttrs value aliased")
	}
}

// Dropping a dangling source chunk reference must not leave the restored
// file declaring bytes it can no longer serve.
func TestRevertDanglingChunkAdjustsSize(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		putTestChunk(t, m, 1, ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11})
		putTestChunk(t, m, 2, ChunkInfo{Size: 4, Offset: 4, Release: "v1", AssetID: 12})
		m.UpsertFile("d/f", FileMeta{Size: 8, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1, 2}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatalf("seed release: %v", err)
		}
	})
	// Corrupt history: chunk 2's record is gone while the file still lists it.
	delete(hist.chunks, 2)
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
	})
	if err := RevertSubtree(cur, hist, "d/f", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	f := cur.FindFile("d/f")
	if f == nil {
		t.Fatal("file not restored")
	}
	if len(f.Chunks) != 1 {
		t.Fatalf("dangling reference not dropped: %v", f.Chunks)
	}
	if f.Size != 4 {
		t.Fatalf("restored size %d still counts dropped chunk bytes, want 4", f.Size)
	}
}

func TestRevertRejectsRoot(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {})
	cur := buildTree(t, func(m *RepoMetadata) {})
	if err := RevertSubtree(cur, hist, "", 300); err == nil {
		t.Fatal("reverting the root must be rejected")
	}
}
