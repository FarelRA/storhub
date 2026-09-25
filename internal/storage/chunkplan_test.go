package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// A declared negative body size must fail loud with ErrInvalidArgument
// instead of falling through to slot planning with a nonsense size.
func TestReplaceReaderRejectsNegativeSize(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	_, err := hub.ReplaceFileFromReaderContext(context.Background(), "sizecheck", "f.txt", strings.NewReader("hello"), shfs.WithSize(-1))
	if !errors.Is(err, shfs.ErrInvalidArgument) {
		t.Fatalf("negative size must fail with ErrInvalidArgument, got %v", err)
	}
}

// The reader upload path must land every part at its own file offset:
// a multi-part replace round-trips byte-identical through read-back.
func TestReplaceReaderRoundTripsMultiPart(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seed"))
	if _, err := hub.UploadFileContext(ctx, "readerroundtrip", "data.bin", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	payload := []byte("0123456789ABCDEFGHIJ")
	if _, err := hub.ReplaceFileFromReaderContext(ctx, "readerroundtrip", "data.bin", bytes.NewReader(payload), shfs.WithSize(int64(len(payload)))); err != nil {
		t.Fatalf("reader replace: %v", err)
	}
	got, err := hub.ReadFileAtContext(ctx, "readerroundtrip", "data.bin", 0, int64(len(payload)))
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round trip mismatch: got %q, want %q", got, payload)
	}
}

// ceil(size/chunkSize) planning for release slots and part counts.
func TestCheckedChunkCountRoundsUp(t *testing.T) {
	t.Parallel()
	cases := []struct {
		size, chunkSize int64
		want            int
	}{
		{0, 4, 0},
		{1, 4, 1},
		{8, 4, 2},
		{9, 4, 3},
		{-3, 4, 0},
	}
	for _, tc := range cases {
		got, err := checkedChunkCount(tc.size, tc.chunkSize)
		if err != nil {
			t.Fatalf("checkedChunkCount(%d, %d): %v", tc.size, tc.chunkSize, err)
		}
		if got != tc.want {
			t.Fatalf("checkedChunkCount(%d, %d) = %d, want %d", tc.size, tc.chunkSize, got, tc.want)
		}
	}
}

// Chunks at or past EOF are dropped and a straddling tail is trimmed to
// the logical size; survivors come back sorted by file offset.
func TestTrimChunksDropsPastEOF(t *testing.T) {
	t.Parallel()
	in := []ChunkInfo{
		{Offset: 8, Size: 4},
		{Offset: 4, Size: 4},
		{Offset: 0, Size: 4},
	}
	got := trimChunks(in, 6)
	want := []ChunkInfo{
		{Offset: 0, Size: 4},
		{Offset: 4, Size: 2},
	}
	if len(got) != len(want) {
		t.Fatalf("trimChunks = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].Offset != want[i].Offset || got[i].Size != want[i].Size {
			t.Fatalf("trimChunks = %+v, want %+v", got, want)
		}
	}
	if out := trimChunks(in, 0); len(out) != 0 {
		t.Fatalf("trimChunks to empty must drop everything, got %+v", out)
	}
	if out := trimChunks(nil, 10); len(out) != 0 {
		t.Fatalf("trimChunks of nothing must stay empty, got %+v", out)
	}
}
