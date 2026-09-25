package chunking

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestStreamingChunkerReadsAndClampsChunkSizes(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "input.bin")
	data := []byte("abcdefghij")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	chunker, err := NewStreamingChunker(path, 4)
	if err != nil {
		t.Fatalf("new chunker: %v", err)
	}
	defer func() { _ = chunker.Close() }()
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
	if string(buf) != "efgh" || chunk.Offset() != 4 || chunk.Size() != 4 {
		t.Fatalf("unexpected chunk: data=%q offset=%d size=%d", buf, chunk.Offset(), chunk.Size())
	}
	if _, err := chunk.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek chunk: %v", err)
	}
	if _, err := chunker.GetChunk(99); err == nil {
		t.Fatal("expected out-of-range error")
	}
	clamped, err := NewStreamingChunker(path, MaxReleaseAssetSize+1)
	if err != nil {
		t.Fatalf("new clamped chunker: %v", err)
	}
	defer func() { _ = clamped.Close() }()
	if clamped.NumChunks() != 1 {
		t.Fatalf("expected clamped single chunk, got %d", clamped.NumChunks())
	}
}

func TestChunkerErrorEdges(t *testing.T) {
	t.Parallel()
	if _, err := NewStreamingChunker(filepath.Join(t.TempDir(), "missing.bin"), 4); err == nil {
		t.Fatal("expected missing file error")
	}
	path := filepath.Join(t.TempDir(), "empty.bin")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}
	chunker, err := NewStreamingChunker(path, 4)
	if err != nil {
		t.Fatalf("new empty chunker: %v", err)
	}
	defer func() { _ = chunker.Close() }()
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

// Past-999 coverage now pins offset geometry: every reader must cover
// exactly its own slice with no gaps or overlaps.
func TestChunkOffsetsAcrossManyChunks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "wide.bin")
	buf := make([]byte, 1000)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	chunker, err := NewStreamingChunker(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = chunker.Close() }()
	if got := chunker.NumChunks(); got != 1000 {
		t.Fatalf("expected 1000 chunks, got %d", got)
	}
	for _, i := range []int{0, 1, 998, 999} {
		c, err := chunker.GetChunk(i)
		if err != nil {
			t.Fatal(err)
		}
		if c.Offset() != int64(i) || c.Size() != 1 {
			t.Fatalf("chunk %d covers offset=%d size=%d, want offset=%d size=1", i, c.Offset(), c.Size(), i)
		}
	}
}

// TestNormalizedSizeReportsAdjustment pins the pure-helper contract: the
// size clamps into range and the flag reports whether the input was
// adjusted, so callers log the decision with their own logger.
func TestNormalizedSizeReportsAdjustment(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      int64
		want    int64
		changed bool
	}{
		{-5, DefaultChunkSize, true},
		{0, DefaultChunkSize, true},
		{4, 4, false},
		{MaxReleaseAssetSize, MaxReleaseAssetSize, false},
		{MaxReleaseAssetSize + 1, MaxReleaseAssetSize, true},
	}
	for _, tc := range cases {
		got, changed := NormalizedSize(tc.in)
		if got != tc.want || changed != tc.changed {
			t.Fatalf("NormalizedSize(%d) = (%d, %v), want (%d, %v)", tc.in, got, changed, tc.want, tc.changed)
		}
	}
}

// recordSink is a slog.Handler capturing record messages for assertions.
type recordSink struct {
	records *[]string
}

func (s recordSink) Enabled(context.Context, slog.Level) bool { return true }
func (s recordSink) Handle(_ context.Context, r slog.Record) error {
	*s.records = append(*s.records, r.Message)
	return nil
}
func (s recordSink) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s recordSink) WithGroup(string) slog.Handler      { return s }

// TestNormalizedSizeEmitsNoLogs pins the layering fix: the pure helper must
// not touch slog.Default, so embedding the library without configuring the
// process logger emits no stderr traffic. The old code logged a Debug on
// the default path and a Warn on the clamp path.
func TestNormalizedSizeEmitsNoLogs(t *testing.T) {
	// NOT parallel: the process-global slog.Default swap below is
	// process-wide, and parallel siblings (e.g. chunker planning tests)
	// log through it; their records would pollute this assertion.
	var records []string
	prev := slog.Default()
	slog.SetDefault(slog.New(recordSink{records: &records}))
	defer slog.SetDefault(prev)
	_, _ = NormalizedSize(0)
	_, _ = NormalizedSize(MaxReleaseAssetSize + 1)
	_, _ = NormalizedSize(4)
	if len(records) != 0 {
		t.Fatalf("NormalizedSize must not log, got %v", records)
	}
}
