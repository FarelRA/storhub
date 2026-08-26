package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fragmentReader delivers at most `piece` bytes per Read, like real HTTP
// bodies do - the exact condition that used to shatter uploads into
// per-read assets.
type fragmentReader struct {
	piece     int
	remaining int
	data      byte
}

func (f *fragmentReader) Read(p []byte) (int, error) {
	if f.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), f.piece, f.remaining)
	for i := range n {
		p[i] = f.data
	}
	f.remaining -= n
	return n, nil
}

// TestWindowReaderReplayAfterFailure pins the tee-mirror contract: a window
// that fails mid-stream is retried by reading the spooled prefix from disk
// and only the unconsumed suffix from live - and the reassembled bytes are
// identical to the original.
func TestWindowReaderReplayAfterFailure(t *testing.T) {
	const size = 8
	live := &fragmentReader{piece: 3, remaining: size, data: 'A'}

	win, cleanup, err := newWindowReader(live, size)
	defer cleanup()
	if err != nil {
		t.Fatalf("newWindowReader: %v", err)
	}
	defer func() { _ = win.Close() }()

	// Simulate attempt 1 dying after 5 bytes: read them, discard.
	head := make([]byte, 5)
	if _, err := io.ReadFull(win, head); err != nil {
		t.Fatalf("attempt 1: %v", err)
	}
	if !bytes.Equal(head, []byte("AAAAA")) {
		t.Fatalf("head mismatch: %q", head)
	}
	if win.mirrored != 5 {
		t.Fatalf("mirrored=%d want 5", win.mirrored)
	}
	if live.remaining != 3 { // attempt 1 drew exactly its 5 bytes from live
		t.Fatalf("attempt-1 live draw wrong: remaining=%d want 3", live.remaining)
	}

	// Attempt 2 rewinds; bytes must come back identical without touching live.
	if _, err := win.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("rewind: %v", err)
	}
	got, err := io.ReadAll(win)
	if err != nil || len(got) != size {
		t.Fatalf("replay: %d bytes, err=%v", len(got), err)
	}
	if string(got) != strings.Repeat("A", size) {
		t.Fatalf("replayed content mismatch: %q", got)
	}
	// Completing the window legitimately consumed the final 3 live bytes -
	// never more, never a re-read of the mirrored prefix.
	if live.remaining != 0 {
		t.Fatalf("live remaining after completion=%d want 0", live.remaining)
	}
}

// TestSpoolDirUnderCacheBaseRest pins the location convention:
// <CacheBase>/rest/upload-*.
func TestSpoolDirUnderCacheBaseRest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	t.Setenv("STORHUB_CACHE_DIR", dir)

	win, cleanup, err := newWindowReader(strings.NewReader("x"), 1)
	defer cleanup()
	if err != nil {
		t.Fatalf("newWindowReader: %v", err)
	}
	defer func() { _ = win.Close() }()

	base := filepath.Join(dir, "rest")
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) == 0 {
		t.Fatalf("expected spool dir under %s: err=%v entries=%d", base, err, len(entries))
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "upload-") {
			t.Fatalf("spool dir %q does not follow upload-* convention", e.Name())
		}
	}
}
