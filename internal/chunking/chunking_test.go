package chunking

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestStreamingChunkerReadsAndClampsChunkSizes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.bin")
	data := []byte("abcdefghij")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	chunker, err := NewStreamingChunker(path, "blob", 4)
	if err != nil {
		t.Fatalf("new chunker: %v", err)
	}
	defer chunker.Close()
	if chunker.NumChunks() != 3 {
		t.Fatalf("expected 3 chunks, got %d", chunker.NumChunks())
	}
	chunk, err := chunker.GetChunk(1)
	if err != nil {
		t.Fatalf("get chunk: %v", err)
	}
	buf, err := io.ReadAll(chunk)
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if string(buf) != "efgh" || chunk.Offset() != 4 || chunk.Size() != 4 || chunk.Name() != "blob.part002" || chunk.Index() != 1 {
		t.Fatalf("unexpected chunk: data=%q offset=%d size=%d name=%q index=%d", buf, chunk.Offset(), chunk.Size(), chunk.Name(), chunk.Index())
	}
	if _, err := chunk.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek chunk: %v", err)
	}
	if _, err := chunker.GetChunk(99); err == nil {
		t.Fatal("expected out-of-range error")
	}
	clamped, err := NewStreamingChunker(path, "blob", MaxReleaseAssetSize+1)
	if err != nil {
		t.Fatalf("new clamped chunker: %v", err)
	}
	defer clamped.Close()
	if clamped.NumChunks() != 1 {
		t.Fatalf("expected clamped single chunk, got %d", clamped.NumChunks())
	}
}

func TestChunkerErrorEdges(t *testing.T) {
	if _, err := NewStreamingChunker(filepath.Join(t.TempDir(), "missing.bin"), "blob", 4); err == nil {
		t.Fatal("expected missing file error")
	}
	path := filepath.Join(t.TempDir(), "empty.bin")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}
	chunker, err := NewStreamingChunker(path, "blob", 4)
	if err != nil {
		t.Fatalf("new empty chunker: %v", err)
	}
	defer chunker.Close()
	if chunker.NumChunks() != 0 {
		t.Fatalf("empty file must yield zero chunks, got %d", chunker.NumChunks())
	}
	if _, err := chunker.GetChunk(0); err == nil {
		t.Fatal("expected out-of-range error for empty file chunk 0")
	}
	if _, err := chunker.GetChunk(-1); err == nil {
		t.Fatal("expected negative chunk index error")
	}
}
