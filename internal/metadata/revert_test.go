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

func clonePtr(m *RepoMetadata) *RepoMetadata { c := m.Clone(); return &c }

func TestRevertFileRestoresHistoricalContent(t *testing.T) {
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("docs", 100)
		m.Chunks[1] = ChunkInfo{Size: 5, Offset: 0, Release: "v1", AssetID: 11}
		m.UpsertFile("docs/a.txt", FileMeta{Size: 5, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		m.EnsureRelease("v1", 100)
	})
	cur := clonePtr(hist)
	// Current diverges: a.txt overwritten with a new chunk.
	cur.Chunks[2] = ChunkInfo{Size: 9, Offset: 0, Release: "v1", AssetID: 22}
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
	if c := cur.Chunks[f.Chunks[0]]; c.AssetID != 11 {
		t.Fatalf("reverted chunk lost its asset: %+v", c)
	}
}

func TestRevertRestoresDeletedFileAndParent(t *testing.T) {
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("keep", 100)
		m.EnsureDirectory("gone", 100)
		m.Chunks[1] = ChunkInfo{Size: 3, Offset: 0, Release: "v1", AssetID: 11}
		m.UpsertFile("gone/x.txt", FileMeta{Size: 3, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		m.EnsureRelease("v1", 100)
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
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("tree", 100)
		m.EnsureDirectory("tree/sub", 100)
		m.Chunks[1] = ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 11}
		m.UpsertFile("tree/a", FileMeta{Size: 1, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		m.UpsertFile("tree/sub/b", FileMeta{Size: 1, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		m.EnsureRelease("v1", 100)
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
		t.Fatalf("subtree not fully restored: %v", cur.Files)
	}
	if cur.FindFile("unrelated") == nil {
		t.Fatal("revert must not touch paths outside the subtree")
	}
}

func TestRevertRemovesPathAbsentInHistory(t *testing.T) {
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("docs", 100)
	})
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("docs", 100)
		m.Chunks[1] = ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 11}
		m.UpsertFile("docs/new", FileMeta{Size: 1, Mode: 0o644, UploadedAt: 200, ModifiedAt: 200, Chunks: []int64{1}}, 200)
		m.EnsureRelease("v1", 100)
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
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		m.Chunks[5] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 55}
		m.UpsertFile("d/target", FileMeta{Size: 4, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{5}}, 100)
		m.EnsureRelease("v1", 100)
	})
	// Current reused chunk id 5 for DIFFERENT content and holds inode of
	// d/target under another path (collision).
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		m.Chunks[5] = ChunkInfo{Size: 77, Offset: 0, Release: "v1", AssetID: 999}
		m.UpsertFile("d/other", FileMeta{Size: 77, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{5}}, 100)
		m.EnsureRelease("v1", 100)
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
	if c := cur.Chunks[tf.Chunks[0]]; c.AssetID != 55 {
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
	if c := cur.Chunks[other.Chunks[0]]; c.AssetID != 999 {
		t.Fatalf("revert clobbered the colliding live chunk: %+v", c)
	}
}

func TestRevertRestoresMissingRelease(t *testing.T) {
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
		m.EnsureRelease("v9", 100)
		m.Chunks[1] = ChunkInfo{Size: 2, Offset: 0, Release: "v9", AssetID: 11}
		m.UpsertFile("d/f", FileMeta{Size: 2, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
	})
	cur := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("d", 100)
	})
	if err := RevertSubtree(cur, hist, "d/f", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	if _, ok := cur.Releases["v9"]; !ok {
		t.Fatal("revert must restore the release a reverted chunk references")
	}
}

func TestRevertRejectsRoot(t *testing.T) {
	hist := buildTree(t, func(m *RepoMetadata) {})
	cur := buildTree(t, func(m *RepoMetadata) {})
	if err := RevertSubtree(cur, hist, "", 300); err == nil {
		t.Fatal("reverting the root must be rejected")
	}
}
