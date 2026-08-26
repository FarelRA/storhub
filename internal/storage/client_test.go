package storage

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestFillChunkIgnoresFragmentedReads pins the upload-splitting fix: HTTP
// bodies deliver in arbitrary small reads, and each sealed chunk must span
// a FULL buffer, not a single Read() return.
func TestFillChunkIgnoresFragmentedReads(t *testing.T) {
	fragmented := &fragmentReader{piece: 3, remaining: 15}
	buf := make([]byte, 10)

	n, err := fillChunk(fragmented, buf)
	if err != nil || n != 10 {
		t.Fatalf("first chunk: got %d bytes, err=%v; want full buffer", n, err)
	}
	n, err = fillChunk(fragmented, buf)
	if err != nil || n != 5 {
		t.Fatalf("tail chunk: got %d bytes, err=%v; want remaining 5", n, err)
	}
	if _, err := fragmented.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after tail, got %v", err)
	}

	// Exact multiple: no spurious empty final chunk.
	exact := bytes.NewReader(make([]byte, 20))
	if n, err := fillChunk(exact, buf); err != nil || n != 10 {
		t.Fatalf("exact fill 1: %d, %v", n, err)
	}
	if n, err := fillChunk(exact, buf); err != nil || n != 10 {
		t.Fatalf("exact fill 2: %d, %v", n, err)
	}
	if n, err := fillChunk(exact, buf); err != nil || n != 0 {
		t.Fatalf("post-EOF: %d, %v; want clean 0", n, err)
	}
}

type fragmentReader struct {
	piece     int
	remaining int
}

func (f *fragmentReader) Read(p []byte) (int, error) {
	if f.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), f.piece, f.remaining)
	for i := range n {
		p[i] = 'x'
	}
	f.remaining -= n
	return n, nil
}
