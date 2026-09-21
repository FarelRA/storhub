package fusefs

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Handler-level read-visibility tests: no mount needed, direct handle
// calls. A read-only handle must observe another handle's uncommitted
// overlay writes for the same inode (POSIX read-your-writes across
// handles), including O_APPEND positioning and the merged-writeback
// shape the kernel legally sends under writeback caching.

// readVisBytes reads length bytes at off through the handle and returns
// a copy of the payload, failing fast on errno.
func readVisBytes(t *testing.T, h *storhubHandle, off int64, length int) []byte {
	t.Helper()
	dest := make([]byte, length)
	res, errno := h.Read(context.Background(), dest, off)
	if errno != 0 {
		t.Fatalf("read off=%d len=%d: errno=%v", off, length, errno)
	}
	got, _ := res.Bytes(dest)
	return append([]byte(nil), got...)
}

// visPayloadStub serves fixed content for pinned (clean-span) reads.
func visPayloadStub(payload string) *stubHub {
	data := []byte(payload)
	return &stubHub{
		readFileAt: func(_ context.Context, _ string, _ string, off, length int64) ([]byte, error) {
			if off >= int64(len(data)) {
				return []byte{}, nil
			}
			end := off + length
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			return append([]byte(nil), data[off:end]...), nil
		},
	}
}

func TestReadVisSecondHandleSeesUncommittedWrites(t *testing.T) {
	t.Parallel()
	fsys := mustMount(t, &stubHub{}, "", Options{})
	ctx := context.Background()
	const inode = uint64(41)
	w, err := fsys.newHandle(ctx, inode, "vis.bin", syscall.O_RDWR, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new writer handle: %v", err)
	}
	defer func() { _ = w.Release(ctx) }()
	want := []byte("datashouldmatch")
	if n, errno := w.Write(ctx, want, 0); errno != 0 || int(n) != len(want) {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	r, err := fsys.newHandle(ctx, inode, "vis.bin", syscall.O_RDONLY, nil)
	if err != nil {
		t.Fatalf("new reader handle: %v", err)
	}
	defer func() { _ = r.Release(ctx) }()
	r.pinned = &pinnedContent{file: meta.FileMeta{Inode: inode, Size: 0}}
	if r.writeState != nil {
		t.Fatal("read-only handle must not attach a writeState")
	}
	if got := readVisBytes(t, r, 0, len(want)); !bytes.Equal(got, want) {
		t.Fatalf("cross-handle read: want %q, got %q", want, got)
	}
	if got := readVisBytes(t, r, 5, 6); !bytes.Equal(got, want[5:11]) {
		t.Fatalf("cross-handle subspan read: want %q, got %q", want[5:11], got)
	}
}

func TestReadVisMergesDirtyAndCleanSpans(t *testing.T) {
	t.Parallel()
	fsys := mustMount(t, visPayloadStub("0123456789"), "", Options{})
	ctx := context.Background()
	const inode = uint64(42)
	w, err := fsys.newHandle(ctx, inode, "vis-mix.bin", syscall.O_RDWR, &writeBootstrap{baseSize: 10})
	if err != nil {
		t.Fatalf("new writer handle: %v", err)
	}
	defer func() { _ = w.Release(ctx) }()
	if n, errno := w.Write(ctx, []byte("ABC"), 4); errno != 0 || n != 3 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	r, err := fsys.newHandle(ctx, inode, "vis-mix.bin", syscall.O_RDONLY, nil)
	if err != nil {
		t.Fatalf("new reader handle: %v", err)
	}
	defer func() { _ = r.Release(ctx) }()
	r.pinned = &pinnedContent{file: meta.FileMeta{Inode: inode, Size: 10}}
	if got := readVisBytes(t, r, 0, 10); string(got) != "0123ABC789" {
		t.Fatalf("merged read: want %q, got %q", "0123ABC789", got)
	}
	if got := readVisBytes(t, r, 4, 3); string(got) != "ABC" {
		t.Fatalf("dirty-only read: want %q, got %q", "ABC", got)
	}
	if got := readVisBytes(t, r, 8, 2); string(got) != "89" {
		t.Fatalf("clean-tail read: want %q, got %q", "89", got)
	}
	if got := readVisBytes(t, r, 8, 10); string(got) != "89" {
		t.Fatalf("EOF-clamped read: want %q, got %q", "89", got)
	}
	if got := readVisBytes(t, r, 10, 4); len(got) != 0 {
		t.Fatalf("past-EOF read: want empty, got %q", got)
	}
}

func TestReadVisOAppendMergedWriteback(t *testing.T) {
	t.Parallel()
	fsys := mustMount(t, &stubHub{}, "", Options{})
	ctx := context.Background()
	const inode = uint64(43)
	w, err := fsys.newHandle(ctx, inode, "vis-append.bin", syscall.O_WRONLY|syscall.O_APPEND, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new append handle: %v", err)
	}
	defer func() { _ = w.Release(ctx) }()
	appendAt := func(data string, off int64) {
		t.Helper()
		if n, errno := w.Write(ctx, []byte(data), off); errno != 0 || int(n) != len(data) {
			t.Fatalf("append %q at %d: n=%d errno=%v", data, off, n, errno)
		}
	}
	// Normal appends land at EOF.
	appendAt("ABCD", 0)
	if w.writeState.logicalSize != 4 {
		t.Fatalf("size after first append: want 4, got %d", w.writeState.logicalSize)
	}
	// Merged writeback: the kernel replays the full dirty page [0,7),
	// covering the 4 acked bytes plus the 3 new tail bytes. The server
	// must honor the offset (rewriting the prefix identically, landing
	// the tail once), not force the whole payload to EOF (which gave
	// kernel 7 vs server 11).
	appendAt("ABCDEFG", 0)
	if w.writeState.logicalSize != 7 {
		t.Fatalf("size after merged writeback: want 7, got %d", w.writeState.logicalSize)
	}
	// Duplicate retransmit of the same page must be idempotent.
	appendAt("ABCDEFG", 0)
	if w.writeState.logicalSize != 7 {
		t.Fatalf("size after retransmit: want 7, got %d", w.writeState.logicalSize)
	}
	// Second wave merged over the larger page.
	appendAt("ABCDEFGHIJ", 0)
	if w.writeState.logicalSize != 10 {
		t.Fatalf("size after second wave: want 10, got %d", w.writeState.logicalSize)
	}
	if got := readVisBytes(t, w, 0, 10); string(got) != "ABCDEFGHIJ" {
		t.Fatalf("owner read: want %q, got %q", "ABCDEFGHIJ", got)
	}
	// A second read-only handle observes the same uncommitted tail.
	r, err := fsys.newHandle(ctx, inode, "vis-append.bin", syscall.O_RDONLY, nil)
	if err != nil {
		t.Fatalf("new reader handle: %v", err)
	}
	defer func() { _ = r.Release(ctx) }()
	r.pinned = &pinnedContent{file: meta.FileMeta{Inode: inode, Size: 0}}
	if got := readVisBytes(t, r, 0, 10); string(got) != "ABCDEFGHIJ" {
		t.Fatalf("cross-handle append read: want %q, got %q", "ABCDEFGHIJ", got)
	}
}

func TestReadVisPoisonedOverlayFailsClosed(t *testing.T) {
	t.Parallel()
	fsys := mustMount(t, &stubHub{}, "", Options{})
	ctx := context.Background()
	const inode = uint64(44)
	w, err := fsys.newHandle(ctx, inode, "vis-poison.bin", syscall.O_RDWR, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new writer handle: %v", err)
	}
	defer func() { _ = w.Release(ctx) }()
	if n, errno := w.Write(ctx, []byte("x"), 0); errno != 0 || n != 1 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	r, err := fsys.newHandle(ctx, inode, "vis-poison.bin", syscall.O_RDONLY, nil)
	if err != nil {
		t.Fatalf("new reader handle: %v", err)
	}
	defer func() { _ = r.Release(ctx) }()
	r.pinned = &pinnedContent{file: meta.FileMeta{Inode: inode, Size: 0}}
	w.writeState.mu.Lock()
	w.writeState.poisoned = true
	w.writeState.mu.Unlock()
	if _, errno := r.Read(ctx, make([]byte, 1), 0); errno != syscall.EIO {
		t.Fatalf("poisoned cross-handle read: want EIO, got %v", errno)
	}
}

func TestReadVisUncontendedFallsBackToPinned(t *testing.T) {
	t.Parallel()
	fsys := mustMount(t, visPayloadStub("0123456789"), "", Options{})
	ctx := context.Background()
	const inode = uint64(45)
	r, err := fsys.newHandle(ctx, inode, "vis-idle.bin", syscall.O_RDONLY, nil)
	if err != nil {
		t.Fatalf("new reader handle: %v", err)
	}
	defer func() { _ = r.Release(ctx) }()
	r.pinned = &pinnedContent{file: meta.FileMeta{Inode: inode, Size: 10}}
	if got := readVisBytes(t, r, 2, 4); string(got) != "2345" {
		t.Fatalf("pinned read: want %q, got %q", "2345", got)
	}
	// A live writer with nothing uncommitted must not change the path.
	w, err := fsys.newHandle(ctx, inode, "vis-idle.bin", syscall.O_RDWR, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new writer handle: %v", err)
	}
	defer func() { _ = w.Release(ctx) }()
	if got := readVisBytes(t, r, 2, 4); string(got) != "2345" {
		t.Fatalf("pinned read with idle writer: want %q, got %q", "2345", got)
	}
}
