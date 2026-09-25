package fusefs

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/metadata"
)

// A 4-byte write inside chunk [64,128) of a 1024-byte file must commit
// exactly the dirty span, not the chunk-aligned planned window the
// sibling rungs consume: patch is the byte-precise rung (page splitting
// bounds memory while preserving exact spans), and the backend patch
// verb takes arbitrary spans with no window requirement. Enlarging to
// the window would inflate uploads and force hub reads for clean bytes
// the temp already covers.
func TestCommitPatchCoversDirtySpan(t *testing.T) {
	t.Parallel()
	var got []shfs.RangeEdit
	hub := &stubHub{
		chunkSize: 64,
		readFileAt: func(_ context.Context, _, _ string, _, length int64) ([]byte, error) {
			return make([]byte, length), nil
		},
	}
	hub.patchRanges = func(edits []shfs.RangeEdit) (*metadata.FileMeta, error) {
		got = append([]shfs.RangeEdit{}, edits...)
		return &metadata.FileMeta{}, nil
	}
	fsys := mustMount(t, hub, "", Options{})
	ctx := context.Background()
	h, err := fsys.newHandle(ctx, 71, "planned.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 1024})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(ctx, []byte{1, 2, 3, 4}, 100); errno != 0 || n != 4 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.commit(ctx); errno != 0 {
		t.Fatalf("commit: %v", errno)
	}
	if len(got) != 1 {
		t.Fatalf("precise patch must commit one edit, got %+v", got)
	}
	if got[0].Start != 100 || got[0].DeleteSize != 4 || int64(len(got[0].Data)) != 4 {
		t.Fatalf("precise patch must cover [100,104), got start=%d delete=%d len=%d",
			got[0].Start, got[0].DeleteSize, len(got[0].Data))
	}
}

// The size setter moves temp length, dirty cover, and logical size as
// one unit: growing then shrinking must leave no stale dirty span above
// the size and no temp tail past it.
func TestLogicalSizeSetterMovesTempAndRanges(t *testing.T) {
	t.Parallel()
	fsys := mustMount(t, &stubHub{}, "", Options{})
	ctx := context.Background()
	h, err := fsys.newHandle(ctx, 72, "sized.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 8})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	ws := h.loadWriteState()
	ws.opMu.Lock()
	ws.mu.Lock()
	defer func() {
		ws.mu.Unlock()
		fsys.unlockOpMu(&ws.opMu)
	}()
	if err := ws.setLogicalSizeLocked(16); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if ws.logicalSize != 16 {
		t.Fatalf("size after grow: want 16, got %d", ws.logicalSize)
	}
	ws.markDirtyLocked(0, 16)
	if err := ws.setLogicalSizeLocked(4); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	if ws.logicalSize != 4 {
		t.Fatalf("size after shrink: want 4, got %d", ws.logicalSize)
	}
	for _, r := range ws.dirtyRanges {
		if r.End > 4 {
			t.Fatalf("dirty span %+v survives above the size", r)
		}
	}
	fi, err := ws.temp.Stat()
	if err != nil {
		t.Fatalf("temp stat: %v", err)
	}
	if fi.Size() != 4 {
		t.Fatalf("temp length after shrink: want 4, got %d", fi.Size())
	}
}

// The overlay truncate path must identify itself in debug logs: the
// direct-hub truncate for non-owners lives elsewhere, and the two were
// indistinguishable.
func TestOverlayTruncateLogsChosenPath(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	fsys := mustMount(t, &stubHub{}, "", Options{Logger: logger, Debug: true})
	ctx := context.Background()
	h, err := fsys.newHandle(ctx, 73, "trunc.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 8})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	ws := h.loadWriteState()
	ws.opMu.Lock()
	ws.mu.Lock()
	setErr := ws.setSizeLocked(4)
	ws.mu.Unlock()
	fsys.unlockOpMu(&ws.opMu)
	if setErr != nil {
		t.Fatalf("truncate: %v", setErr)
	}
	if !strings.Contains(buf.String(), "overlay truncate") {
		t.Fatalf("overlay truncate must log its path, got %q", buf.String())
	}
}

// Regrow through the overlay page size must still serve zeros: a tiny
// configured page exercises the copyPageSize loop instead of the old
// fixed 32 KiB buffer.
func TestSetSizeRegrowUsesOverlayPage(t *testing.T) {
	t.Parallel()
	fsys := mustMount(t, &stubHub{}, "", Options{OverlayBufferSize: 4})
	ctx := context.Background()
	h, err := fsys.newHandle(ctx, 74, "regrow.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 8})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	ws := h.loadWriteState()
	ws.opMu.Lock()
	ws.mu.Lock()
	if err := ws.setSizeLocked(16); err != nil {
		ws.mu.Unlock()
		fsys.unlockOpMu(&ws.opMu)
		t.Fatalf("regrow: %v", err)
	}
	if !ws.coversRangeLocked(8, 16) {
		ws.mu.Unlock()
		fsys.unlockOpMu(&ws.opMu)
		t.Fatalf("regrown span must be dirty, got %+v", ws.dirtyRanges)
	}
	dest := make([]byte, 8)
	n, readErr := ws.readIntoLocked(ctx, dest, 8)
	ws.mu.Unlock()
	fsys.unlockOpMu(&ws.opMu)
	if readErr != nil || n != 8 {
		t.Fatalf("regrow read: n=%d err=%v", n, readErr)
	}
	for i, b := range dest {
		if b != 0 {
			t.Fatalf("regrown byte %d reads %d, want zero", i, b)
		}
	}
}

// Hole bytes share one zeroing spelling: a partially filled span must
// come back with a zero tail.
func TestZeroSpanClearsTail(t *testing.T) {
	t.Parallel()
	buf := []byte{9, 9, 9, 9, 9, 9}
	zeroSpan(buf[2:])
	for i, b := range buf {
		want := byte(9)
		if i >= 2 {
			want = 0
		}
		if b != want {
			t.Fatalf("byte %d is %d, want %d", i, b, want)
		}
	}
}

// Dirty ranges are the shared span type: chunk planning must accept
// shared spans directly, and the local merge and total must agree with
// the shared ones.
func TestDirtyRangesUseSharedSpanType(t *testing.T) {
	t.Parallel()
	ws := &inodeWriteState{}
	shared := []shfs.ByteRange{{Start: 0, End: 1}, {Start: 2, End: 3}, {Start: 4, End: 5}, {Start: 6, End: 7}}
	if !ws.shouldChunkRewriteLocked(shared) {
		t.Fatal("four planned spans must trigger the chunk-rewrite rung")
	}
}

func TestRangeMergeMatchesSharedMerge(t *testing.T) {
	t.Parallel()
	first := ByteRange{Start: 10, End: 20}
	second := ByteRange{Start: 15, End: 25}
	merged := mergeByteRange(nil, first)
	merged = mergeByteRange(merged, second)
	want := shfs.MergeByteRanges([]shfs.ByteRange{first, second})
	if len(merged) != len(want) {
		t.Fatalf("merge must agree with the shared merge: got %+v want %+v", merged, want)
	}
	for i := range merged {
		if merged[i] != want[i] {
			t.Fatalf("merge must agree with the shared merge: got %+v want %+v", merged, want)
		}
	}
}

func TestDirtyBytesMatchTotal(t *testing.T) {
	t.Parallel()
	ws := &inodeWriteState{dirtyRanges: []ByteRange{{Start: 0, End: 10}, {Start: 20, End: 25}}}
	if ws.dirtyBytesLocked() != 15 {
		t.Fatalf("dirty bytes must total 15, got %d", ws.dirtyBytesLocked())
	}
	if ws.dirtyBytesLocked() != totalByteRanges(ws.dirtyRanges) {
		t.Fatal("dirty bytes must match the shared total")
	}
}
