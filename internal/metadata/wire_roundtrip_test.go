package metadata

import (
	"bytes"
	"testing"
)

func TestWireFormatRoundTripByteIdentical(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("wire")
	m.EnsureDirectory("a", 1700000000000000000)
	m.UpsertFile("a/f.txt", FileMeta{Size: 12, Inode: 7, Mode: 0o644, UID: 1, GID: 1, Chunks: []int64{3}}, 1700000001000000000)
	if err := m.PutChunk(3, ChunkInfo{Size: 12, Offset: 0, Release: "r1"}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	if _, err := m.EnsureRelease("r1", 1700000002000000000); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	b1, err := m.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var back RepoMetadata
	if err := back.FromJSON(b1); err != nil {
		t.Fatal(err)
	}
	b2, err := back.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("wire format not stable across round trip:\n%d bytes vs %d bytes", len(b1), len(b2))
	}
	if len(back.Files()) != 1 || len(back.Dirs()) != 1 || len(back.Chunks()) != 1 || len(back.Releases()) != 1 {
		t.Fatalf("maps lost in round trip: %d/%d/%d/%d", len(back.Files()), len(back.Dirs()), len(back.Chunks()), len(back.Releases()))
	}
}
