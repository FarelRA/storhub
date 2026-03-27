package chunking

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
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

func TestCRC32CHelpersAndIntegrityFlows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crc.bin")
	data := []byte("abcdefghij")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	first, err := CalculateCRC32CReader(bytes.NewReader(data), 0)
	if err != nil {
		t.Fatalf("crc reader: %v", err)
	}
	streamed, err := CalculateCRC32CStreaming(path, 0)
	if err != nil {
		t.Fatalf("crc streaming: %v", err)
	}
	if first != streamed {
		t.Fatalf("crc mismatch: %s vs %s", first, streamed)
	}
	meta := FileMetadata{Name: "crc.bin", Size: int64(len(data)), CRC32C: first, Chunks: []ChunkInfo{{Index: 1, Offset: 5, Size: 5, CRC32C: formatCRC32C(checksumBytes(data[5:]))}, {Index: 0, Offset: 0, Size: 5, CRC32C: formatCRC32C(checksumBytes(data[:5]))}}}
	combined, err := CalculateChunkedIntegrity(path, meta, 0)
	if err != nil {
		t.Fatalf("chunked integrity: %v", err)
	}
	if combined != first {
		t.Fatalf("unexpected combined checksum: %s", combined)
	}
	if err := VerifyFileIntegrity(path, meta, 0); err != nil {
		t.Fatalf("verify integrity: %v", err)
	}
	if _, err := CombineChunkCRC32Cs(nil); err != nil || formatCRC32C(0) == "" {
		t.Fatalf("expected empty combine success, got %v", err)
	}
	badMeta := meta
	badMeta.Chunks[0].CRC32C = "deadbeef"
	if _, err := CalculateChunkedIntegrityReader(bytes.NewReader(data), badMeta, 1); err == nil {
		t.Fatal("expected chunk mismatch error")
	}
	badMeta = meta
	badMeta.CRC32C = "deadbeef"
	if err := VerifyFileIntegrity(path, badMeta, 1); err == nil {
		t.Fatal("expected file mismatch error")
	}
	if _, err := CombineChunkCRC32Cs([]ChunkInfo{{Index: 0, Size: 1, CRC32C: "nothex"}}); err == nil {
		t.Fatal("expected invalid checksum parse error")
	}
	if _, err := CalculateChunkedIntegrityReader(bytes.NewReader([]byte("x")), FileMetadata{Name: "x", Size: 1}, 1); err == nil {
		t.Fatal("expected missing chunks error")
	}
	if got, err := CalculateChunkedIntegrityReader(bytes.NewReader(nil), FileMetadata{Name: "empty", Size: 0}, 1); err != nil || got != formatCRC32C(0) {
		t.Fatalf("expected empty-file checksum, got %q %v", got, err)
	}
}

func TestHashingReadSeekerAndHelpers(t *testing.T) {
	h := newHashingReadSeeker(bytes.NewReader([]byte("hello")))
	buf, err := io.ReadAll(h)
	if err != nil {
		t.Fatalf("read hashing seeker: %v", err)
	}
	if string(buf) != "hello" || h.Checksum() == "" {
		t.Fatalf("unexpected hashing seeker state: %q %q", buf, h.Checksum())
	}
	beforeReset := h.Checksum()
	if _, err := h.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek reset: %v", err)
	}
	if h.Checksum() == beforeReset {
		t.Fatal("expected checksum reset after seek to zero")
	}
	chunks := []ChunkInfo{{Index: 2, Offset: 5}, {Index: 0, Offset: 0}, {Index: 1, Offset: 5}}
	stableSortChunks(chunks)
	got := []int{chunks[0].Index, chunks[1].Index, chunks[2].Index}
	if !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Fatalf("unexpected chunk sort order: %v", got)
	}
	if formatCRC32C(15) != "0000000f" {
		t.Fatalf("unexpected crc format: %s", formatCRC32C(15))
	}
	if value, err := parseCRC32C("0000000f"); err != nil || value != 15 {
		t.Fatalf("unexpected parsed crc: %d %v", value, err)
	}
	if _, err := parseCRC32C("zz"); err == nil {
		t.Fatal("expected parse error")
	}
	if errorsNew("boom").Error() != "boom" {
		t.Fatal("unexpected errorsNew value")
	}
	if crc32cCombine(10, 20, 0) != 10 {
		t.Fatal("expected zero-length combine to preserve first crc")
	}
}

type failingReadSeeker struct{}

func (failingReadSeeker) Read([]byte) (int, error)       { return 0, nil }
func (failingReadSeeker) Seek(int64, int) (int64, error) { return 0, errors.New("seek failed") }

func TestCalculateCRC32CReaderSeekFailure(t *testing.T) {
	if _, err := CalculateCRC32CReader(failingReadSeeker{}, 1); err == nil {
		t.Fatal("expected seek failure")
	}
}

func TestChunkerAndIntegrityErrorEdges(t *testing.T) {
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
	chunk, err := chunker.GetChunk(0)
	if err != nil || chunk.Size() != 0 {
		t.Fatalf("unexpected empty chunk: %+v %v", chunk, err)
	}
	if _, err := chunker.GetChunk(-1); err == nil {
		t.Fatal("expected negative chunk index error")
	}
	if sum, err := checksumReaderAtRange(bytes.NewReader([]byte("abc")), 0, 0, 1); err != nil || sum != 0 {
		t.Fatalf("unexpected zero-length checksum: %d %v", sum, err)
	}
	if _, err := combineChunkCRC32Cs(nil); err == nil {
		t.Fatal("expected internal combine error for empty chunks")
	}
}

func checksumBytes(data []byte) uint32 {
	sum, err := CalculateCRC32CReader(bytes.NewReader(data), 1)
	if err != nil {
		panic(err)
	}
	value, err := parseCRC32C(sum)
	if err != nil {
		panic(err)
	}
	return value
}
