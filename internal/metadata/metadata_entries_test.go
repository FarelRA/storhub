package metadata

import (
	"reflect"
	"testing"
)

// Fix-cons F5 RED-first pins for the C5 single-form fixes.

func TestGetChunkRoundTrip(t *testing.T) {
	m := NewRepoMetadata("p")
	if err := m.PutChunk(7, ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 1}); err != nil {
		t.Fatal(err)
	}
	got, ok := m.GetChunk(7)
	if !ok || got.Size != 4 {
		t.Fatalf("GetChunk(7) = %+v,%v, want size 4,true", got, ok)
	}
	if _, ok := m.GetChunk(99); ok {
		t.Fatal("GetChunk(99) must report missing")
	}
}

func TestReplaceFileMatchesWriteFileDirect(t *testing.T) {
	now := int64(1700000000)
	build := func() *RepoMetadata {
		m := NewRepoMetadata("p")
		m.UpsertFile("a.txt", FileMeta{Size: 3, Mode: 0o644, Chunks: []int64{}, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, now)
		return m
	}
	repl := FileMeta{Size: 8, Mode: 0o600, Chunks: []int64{}, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}
	a, b := build(), build()
	ino := a.FindFile("a.txt").Inode
	repl.Inode = ino
	if !a.ReplaceFile("a.txt", repl) {
		t.Fatal("ReplaceFile on existing path must return true")
	}
	b.WriteFileDirect("a.txt", repl)
	fa, fb := a.FindFile("a.txt"), b.FindFile("a.txt")
	if !reflect.DeepEqual(*fa, *fb) {
		t.Fatalf("ReplaceFile != WriteFileDirect:\n%+v\n%+v", *fa, *fb)
	}
	if a.TotalFiles != b.TotalFiles || a.TotalSize != b.TotalSize {
		t.Fatal("ReplaceFile must maintain stats exactly like WriteFileDirect")
	}
	if c := build(); c.ReplaceFile("missing.txt", repl) {
		t.Fatal("ReplaceFile on missing path must return false")
	}
}

func TestValidateStoredPathKeyRejectsEscapes(t *testing.T) {
	for _, bad := range []string{"../escape", "a/../../b", "a/", "a//b", "/abs"} {
		if err := validateStoredPathKey(bad); err == nil {
			t.Errorf("validateStoredPathKey(%q) must fail", bad)
		}
	}
	if err := validateStoredPathKey("a/b.txt"); err != nil {
		t.Errorf("canonical key must pass: %v", err)
	}
}
