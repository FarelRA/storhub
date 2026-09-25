package fusefs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestWriteStateAndRangeHelpers(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	fsys := mustMount(t, &stubHub{chunkSize: 4}, cacheDir, Options{OverlayBufferSize: 4})
	temp, err := os.CreateTemp(cacheDir, "inode*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	state := &inodeWriteState{fs: fsys, inode: 1, temp: temp, tempPath: temp.Name(), baseSize: 10, logicalSize: 10}
	state.markDirtyLocked(2, 5)
	state.markDirtyLocked(5, 8)
	if got := state.dirtyBytesLocked(); got != 6 {
		t.Fatalf("unexpected dirty bytes: %d", got)
	}
	baseTemp, err := os.CreateTemp(cacheDir, "inodebase*")
	if err != nil {
		t.Fatalf("create base temp file: %v", err)
	}
	state.baseTemp = baseTemp
	state.baseTempPath = baseTemp.Name()
	if err := state.setSizeLocked(12); err != nil {
		t.Fatalf("grow file: %v", err)
	}
	if err := state.setSizeLocked(6); err != nil {
		t.Fatalf("shrink file: %v", err)
	}
	if state.baseSize != 10 {
		t.Fatalf("expected committed base size 10, got %d", state.baseSize)
	}
	if err := state.setSizeLocked(-1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("expected invalid size error, got %v", err)
	}
	planned := state.plannedRangesLocked()
	if len(planned) == 0 || planned[0].Start != 0 {
		t.Fatalf("unexpected planned ranges: %+v", planned)
	}
	state.dirtyRanges = []ByteRange{{Start: 0, End: 6}}
	if !state.shouldReplaceLocked([]ByteRange{{Start: 0, End: 6}}) {
		t.Fatal("expected replace heuristic to trigger")
	}
	if !state.shouldChunkRewriteLocked([]ByteRange{{Start: 0, End: 1}, {Start: 4, End: 5}}) {
		t.Fatal("expected chunk rewrite heuristic to trigger")
	}
	if merged := mergeByteRange([]ByteRange{{Start: 0, End: 2}}, ByteRange{Start: 2, End: 5}); len(merged) != 1 || merged[0].End != 5 {
		t.Fatalf("unexpected merged ranges: %+v", merged)
	}
	if total := totalByteRanges([]ByteRange{{Start: 0, End: 2}, {Start: 5, End: 7}}); total != 4 {
		t.Fatalf("unexpected total bytes: %d", total)
	}
	state.closeWriteTemp()
	if _, err := os.Stat(temp.Name()); !os.IsNotExist(err) {
		t.Fatalf("expected temp cleanup, got %v", err)
	}
}

func TestRefreshBaseSnapshotLockedUpdatesCachedBase(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	fsys := mustMount(t, &stubHub{chunkSize: 4}, cacheDir, Options{OverlayBufferSize: 4})
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	base, err := os.CreateTemp(cacheDir, "inodebase*")
	if err != nil {
		t.Fatalf("create base temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("abXYef"), 0); err != nil {
		t.Fatalf("seed working temp: %v", err)
	}
	if _, err := base.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatalf("seed base temp: %v", err)
	}
	state := &inodeWriteState{
		fs:           fsys,
		inode:        1,
		temp:         working,
		tempPath:     working.Name(),
		baseTemp:     base,
		baseTempPath: base.Name(),
		baseSize:     6,
		logicalSize:  6,
		dirtyRanges:  []ByteRange{{Start: 2, End: 4}},
	}
	if err := state.refreshBaseSnapshotLocked(); err != nil {
		t.Fatalf("refresh base snapshot: %v", err)
	}
	got := make([]byte, 6)
	if _, err := state.baseTemp.ReadAt(got, 0); err != nil && err != io.EOF {
		t.Fatalf("read refreshed base temp: %v", err)
	}
	if string(got) != "abXYef" {
		t.Fatalf("unexpected refreshed base temp: %q", got)
	}
	state.closeWriteTemp()
}

func TestCreateCommittedSnapshotUsesWorkingTempForFullyDirtyFile(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	fsys := mustMount(t, &stubHub{chunkSize: 4}, cacheDir, Options{OverlayBufferSize: 4})
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatalf("seed working temp: %v", err)
	}
	state := &inodeWriteState{
		fs:          fsys,
		inode:       1,
		temp:        working,
		tempPath:    working.Name(),
		baseSize:    8,
		logicalSize: 6,
		dirtyRanges: []ByteRange{{Start: 0, End: 6}},
	}
	snapshotPath, err := state.createCommittedSnapshotLocked(context.Background())
	if err != nil {
		t.Fatalf("create committed snapshot: %v", err)
	}
	defer func() { _ = os.Remove(snapshotPath) }()
	got, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read committed snapshot: %v", err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("unexpected committed snapshot: %q", got)
	}
	state.closeWriteTemp()
}

func TestCreateCommittedSnapshotUsesWorkingTempAfterTruncateToZero(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	fsys := mustMount(t, &stubHub{chunkSize: 4}, cacheDir, Options{OverlayBufferSize: 4})
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatalf("seed working temp: %v", err)
	}
	state := &inodeWriteState{
		fs:                fsys,
		inode:             1,
		temp:              working,
		tempPath:          working.Name(),
		baseSize:          10,
		logicalSize:       6,
		dirtyRanges:       []ByteRange{{Start: 2, End: 6}},
		tempAuthoritative: true,
	}
	snapshotPath, err := state.createCommittedSnapshotLocked(context.Background())
	if err != nil {
		t.Fatalf("create committed snapshot after truncate: %v", err)
	}
	defer func() { _ = os.Remove(snapshotPath) }()
	got, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read committed snapshot: %v", err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("unexpected committed snapshot after truncate: %q", got)
	}
	state.closeWriteTemp()
}

func TestCreateCommittedSnapshotZeroFillsSparseAuthoritativeTemp(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	fsys := mustMount(t, &stubHub{chunkSize: 4}, cacheDir, Options{OverlayBufferSize: 4})
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("tail"), 4); err != nil {
		t.Fatalf("seed sparse working temp: %v", err)
	}
	state := &inodeWriteState{
		fs:                fsys,
		inode:             1,
		temp:              working,
		tempPath:          working.Name(),
		logicalSize:       8,
		tempAuthoritative: true,
	}
	snapshotPath, err := state.createCommittedSnapshotLocked(context.Background())
	if err != nil {
		t.Fatalf("create committed snapshot from sparse authoritative temp: %v", err)
	}
	defer func() { _ = os.Remove(snapshotPath) }()
	got, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read committed snapshot: %v", err)
	}
	want := append([]byte{0, 0, 0, 0}, []byte("tail")...)
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected sparse committed snapshot: %v", got)
	}
	state.closeWriteTemp()
}

func TestReplaceInputPathLockedReusesWorkingTempForAuthoritativeData(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	fsys := mustMount(t, &stubHub{chunkSize: 4}, cacheDir, Options{OverlayBufferSize: 4})
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatalf("seed working temp: %v", err)
	}
	state := &inodeWriteState{
		fs:                fsys,
		inode:             1,
		temp:              working,
		tempPath:          working.Name(),
		logicalSize:       6,
		tempAuthoritative: true,
	}
	inputPath, cleanup, err := state.replaceInputPathLocked(context.Background())
	if err != nil {
		t.Fatalf("replace input path: %v", err)
	}
	if cleanup {
		t.Fatal("expected working temp reuse without cleanup")
	}
	if inputPath != working.Name() {
		t.Fatalf("expected working temp path %q, got %q", working.Name(), inputPath)
	}
	state.closeWriteTemp()
}

func TestSequentialWriteCommitReplacesFile(t *testing.T) {
	t.Parallel()
	var replacedPath string
	var replacedBytes []byte
	fsys, err := New(&stubHub{
		replaceFile: func(_ context.Context, _ string, target, inputPath string) (*meta.FileMeta, error) {
			data, err := os.ReadFile(inputPath)
			if err != nil {
				return nil, err
			}
			replacedPath = target
			replacedBytes = data
			return &meta.FileMeta{Size: int64(len(data))}, nil
		},
	}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("abcdefghij"), 0); errno != 0 || n != 10 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release handle: %v", errno)
	}
	if replacedPath != "demo.bin" {
		t.Fatalf("unexpected replace target=%q", replacedPath)
	}
	if string(replacedBytes) != "abcdefghij" {
		t.Fatalf("expected final payload %q, got %q", "abcdefghij", replacedBytes)
	}
	// Owner contract: the handle owned its inode-* overlay and removed it
	// itself on Release; nothing reaps leftovers anymore.
	assertCacheDirClean(t, fsys.cacheDir)
}

// Shrinking then regrowing must serve zeros for the regrown region, never
// stale bytes from before the shrink.
func TestSetSizeRegrowServesZeros(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{chunkSize: 4}, "demo", Options{CacheDir: t.TempDir(), OverlayBufferSize: 4})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	state := &inodeWriteState{fs: fsys, inode: 9, path: "grow.bin", refs: 1}
	fsys.mu.Lock()
	fsys.writeStates[9] = state
	fsys.mu.Unlock()
	if err := state.materializeBootstrap(8); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := state.temp.WriteAt([]byte("ABCDEFGH"), 0); err != nil {
		t.Fatalf("seed temp: %v", err)
	}
	state.mu.Lock()
	state.baseSize = 8
	state.logicalSize = 8
	state.tempAuthoritative = false
	if err := state.setSizeLocked(4); err != nil {
		t.Fatalf("shrink: %v", err)
	}
	if err := state.setSizeLocked(7); err != nil {
		t.Fatalf("regrow: %v", err)
	}
	buf := make([]byte, 3)
	if _, err := state.temp.ReadAt(buf, 4); err != nil {
		t.Fatalf("read regrown region: %v", err)
	}
	state.mu.Unlock()
	if string(buf) != "\x00\x00\x00" {
		t.Fatalf("regrown region must be zeros, got %q", buf)
	}
}

// newPatchTestState builds a write state over a baseSize-byte file whose
// temp already holds the full working content, with dirty tracking armed.
func newPatchTestState(t *testing.T, fsys *Filesystem, inode uint64, baseSize int64, content string) *inodeWriteState {
	t.Helper()
	state := &inodeWriteState{fs: fsys, inode: inode, path: "patch.bin", refs: 1}
	fsys.mu.Lock()
	fsys.writeStates[inode] = state
	fsys.mu.Unlock()
	if err := state.materializeBootstrap(baseSize); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := state.temp.WriteAt([]byte(content), 0); err != nil {
		t.Fatalf("seed temp: %v", err)
	}
	state.mu.Lock()
	state.baseSize = baseSize
	state.logicalSize = int64(len(content))
	state.tempAuthoritative = false
	state.markDirtyLocked(baseSize, int64(len(content)))
	state.mu.Unlock()
	return state
}

// TestCommitPatchBatchRetryAfterFailure pins the idempotency contract of
// the batched patch ladder: a commit is ONE operation, so failure leaves
// EVERY range dirty and the retry replays the identical batch - no range
// can be half-applied, and nothing duplicates.
func TestCommitPatchBatchRetryAfterFailure(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var batches [][]shfs.RangeEdit
	failFirst := true
	hub := &stubHub{chunkSize: 4}
	hub.patchRanges = func(edits []shfs.RangeEdit) (*meta.FileMeta, error) {
		mu.Lock()
		defer mu.Unlock()
		if failFirst {
			failFirst = false
			return nil, syscall.EIO
		}
		batches = append(batches, append([]shfs.RangeEdit(nil), edits...))
		return &meta.FileMeta{}, nil
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	const inode = uint64(21)
	// Base 4 bytes, working content 12 bytes, two DISJOINT dirty ranges
	// ([4,8) and [9,11)) so the batch carries two separate edits.
	state := &inodeWriteState{fs: fsys, inode: inode, path: "patch.bin", refs: 1}
	fsys.mu.Lock()
	fsys.writeStates[inode] = state
	fsys.mu.Unlock()
	if err := state.materializeBootstrap(4); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := state.temp.WriteAt([]byte("ABCDEFGHIJKL"), 0); err != nil {
		t.Fatalf("seed temp: %v", err)
	}
	state.mu.Lock()
	state.baseSize = 4
	state.logicalSize = 12
	state.markDirtyLocked(4, 8)
	state.markDirtyLocked(9, 11)
	state.mu.Unlock()
	planned := []ByteRange{{Start: 4, End: 8}, {Start: 9, End: 11}}
	h := newPatchTestHandle(fsys, inode, state)

	// Caller contract: hold state.mu on entry; commitPatch releases it on
	// every return path.
	state.mu.Lock()
	errno := h.commitPatch(context.Background(), "patch.bin", 4, 12, append([]ByteRange(nil), planned...), shfs.MetadataPatch{}, &commitNotifies{})
	if errno == 0 {
		t.Fatal("expected the injected batch failure to surface")
	}
	state.mu.Lock()
	remaining := append([]ByteRange(nil), state.dirtyRanges...)
	logical := state.logicalSize
	state.mu.Unlock()
	if len(remaining) != 2 || remaining[0] != (ByteRange{Start: 4, End: 8}) || remaining[1] != (ByteRange{Start: 9, End: 11}) {
		t.Fatalf("failed batch must leave every range dirty, got %+v", remaining)
	}
	if logical != 12 {
		t.Fatalf("failure must not lose the logical size: %d", logical)
	}

	// Retry: the identical batch is replayed and succeeds.
	state.mu.Lock()
	errno = h.commitPatch(context.Background(), "patch.bin", 4, 12, remaining, shfs.MetadataPatch{}, &commitNotifies{})
	if errno != 0 {
		t.Fatalf("retry failed: %v", errno)
	}
	state.mu.Lock()
	dirtyLeft := len(state.dirtyRanges)
	base := state.baseSize
	state.mu.Unlock()
	if dirtyLeft != 0 {
		t.Fatalf("successful retry must clear every dirty range, got %d left", dirtyLeft)
	}
	if base != 12 {
		t.Fatalf("retry must promote logical size to base, got %d", base)
	}
	mu.Lock()
	defer mu.Unlock()
	if hub.patchRangeCalls != 1 || len(batches) != 1 {
		t.Fatalf("retry must issue exactly one successful batch, got calls=%d batches=%d", hub.patchRangeCalls, len(batches))
	}
	replay := batches[0]
	if len(replay) != 2 || replay[0].Start != 4 || replay[1].Start != 9 {
		t.Fatalf("batch must carry both edits ascending, got %+v", replay)
	}
	if replay[0].DeleteSize != 0 || string(replay[0].Data) != "EFGH" {
		t.Fatalf("edit [4,8) is a pure insert of EFGH, got %+v", replay[0])
	}
	if replay[1].DeleteSize != 0 || string(replay[1].Data) != "JK" {
		t.Fatalf("edit [9,11) is a pure insert of JK, got %+v", replay[1])
	}
}

// TestCommitPatchCancellationKeepsRangesResumable proves a cancelled commit
// retains the dirty set: the next Fsync/Release retries from where the
// cancellation hit.
func TestCommitPatchCancellationKeepsRangesResumable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	hub := &stubHub{chunkSize: 4}
	hub.patchRanges = func(_ []shfs.RangeEdit) (*meta.FileMeta, error) {
		return nil, context.Canceled
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	const inode = uint64(22)
	state := newPatchTestState(t, fsys, inode, 4, "ABCDEFGH")
	h := newPatchTestHandle(fsys, inode, state)

	cancel()
	state.mu.Lock()
	errno := h.commitPatch(ctx, "patch.bin", 4, 8, []ByteRange{{Start: 4, End: 8}}, shfs.MetadataPatch{}, &commitNotifies{})
	if errno == 0 {
		t.Fatal("cancelled commit must fail")
	}
	state.mu.Lock()
	remaining := len(state.dirtyRanges)
	state.mu.Unlock()
	if remaining == 0 {
		t.Fatal("cancelled commit must retain dirty ranges for retry")
	}
}

// TestConcurrentFDsShareWriteState exercises two handles writing through one
// shared inodeWriteState: writes serialize on opMu, both land in the overlay,
// and the final commit uploads the merged content exactly once.
func TestConcurrentFDsShareWriteState(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var patched []struct {
		off    int64
		delete int64
		edit   string
	}
	hub := &stubHub{chunkSize: 64}
	hub.patchRanges = func(edits []shfs.RangeEdit) (*meta.FileMeta, error) {
		mu.Lock()
		for _, e := range edits {
			patched = append(patched, struct {
				off    int64
				delete int64
				edit   string
			}{e.Start, e.DeleteSize, string(e.Data)})
		}
		mu.Unlock()
		return &meta.FileMeta{}, nil
	}
	hub.statPath = func(_ context.Context, _, _ string) (*shfs.EntryInfo, error) {
		return &shfs.EntryInfo{Path: "shared.bin", Size: 16, Mode: 0o644}, nil
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir(), Debug: true})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	const inode = uint64(30)
	// base 16 + 10 dirty bytes keeps the ladder on the patch rung
	// (10*2 < 26, 10*4 < 26*3), so the shared-overlay commit is a ranged
	// patch rather than a wholesale replace.
	state, err := fsys.acquireWriteState(context.Background(), inode, "shared.bin", &writeBootstrap{baseSize: 16})
	if err != nil {
		t.Fatalf("acquire write state: %v", err)
	}
	mkHandle := func() *storhubHandle { return newPatchTestHandle(fsys, inode, state) }
	h1, h2 := mkHandle(), mkHandle()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, errno := h1.Write(context.Background(), []byte("HELLO"), 16); errno != 0 {
			t.Errorf("write h1: %v", errno)
		}
	}()
	go func() {
		defer wg.Done()
		if _, errno := h2.Write(context.Background(), []byte("WORLD"), 21); errno != 0 {
			t.Errorf("write h2: %v", errno)
		}
	}()
	wg.Wait()

	state.mu.Lock()
	t.Logf("pre-release: base=%d logical=%d dirty=%+v dirtyBytes=%d tempAuth=%v",
		state.baseSize, state.logicalSize, state.dirtyRanges, state.dirtyBytesLocked(), state.tempAuthoritative)
	pl := state.plannedRangesLocked()
	fs := maxInt64(state.baseSize, state.logicalSize)
	db := state.dirtyBytesLocked()
	t.Logf("planned=%+v rewrite=%v replace=%v fileSize=%d dirtyBytes=%d arms=%v,%v,%v",
		pl, state.shouldChunkRewriteLocked(pl), state.shouldReplaceLocked(pl), fs, db,
		db*4 >= fs*3, len(pl) >= 12 && db*3 >= fs, db*2 >= fs)
	state.mu.Unlock()
	if errno := h1.Release(context.Background()); errno != 0 {
		t.Fatalf("release h1: %v", errno)
	}
	// h1's release must not tear down the shared overlay while h2 lives.
	state.mu.Lock()
	alive := state.temp != nil
	size := state.logicalSize
	dirtyLeft := len(state.dirtyRanges)
	state.mu.Unlock()
	if !alive {
		t.Fatal("shared overlay was destroyed while a second handle still held it")
	}
	if size != 26 || dirtyLeft != 0 {
		t.Fatalf("post-commit state: size=%d dirty=%d", size, dirtyLeft)
	}
	if errno := h2.Release(context.Background()); errno != 0 {
		t.Fatalf("release h2: %v", errno)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(patched) != 1 {
		t.Fatalf("expected exactly one ranged patch for the merged dirty set, got %+v", patched)
	}
	if patched[0].off != 16 || patched[0].delete != 0 || patched[0].edit != "HELLOWORLD" {
		t.Fatalf("patch content mismatch: %+v", patched[0])
	}
}
