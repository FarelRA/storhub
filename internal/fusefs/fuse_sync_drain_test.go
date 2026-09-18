package fusefs

// Phase 3 sync durability (fsync/O_SYNC/close drain) and commit
// notification ordering tests. All handle level: a recording fake hub
// proves ordering by event sequence, never by timing.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// dsyncFlagForTest is the O_DSYNC-only open flag, or zero where the
// platform has no distinct O_DSYNC spelling (the test then skips).
func dsyncFlagForTest() uint32 {
	return dsyncOnlyFlag
}

// drainProbeHub wraps stubHub and records the commit-verb/drain event
// order plus drain failures. Embedding keeps it compatible with the Hub
// interface on both sides of the DrainProjectContext landing.
type drainProbeHub struct {
	*stubHub
	mu       sync.Mutex
	events   []string
	drainErr error
}

func (d *drainProbeHub) record(event string) {
	d.mu.Lock()
	d.events = append(d.events, event)
	d.mu.Unlock()
}

func (d *drainProbeHub) eventLog() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.events...)
}

func (d *drainProbeHub) DrainProjectContext(context.Context, string) error {
	d.record("drain")
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.drainErr
}

// PatchFileRangesContext invokes the recording func without touching the
// embedded stub's unsynchronized call counters: concurrent committers
// would race them (the race detector proved it), and the probe's own
// event log is the assertion source here.
func (d *drainProbeHub) PatchFileRangesContext(_ context.Context, _, _ string, edits []shfs.RangeEdit) (*meta.FileMeta, error) {
	fn := d.patchRanges
	if fn == nil {
		return nil, nil
	}
	return fn(edits)
}

func (d *drainProbeHub) setDrainErr(err error) {
	d.mu.Lock()
	d.drainErr = err
	d.mu.Unlock()
}

// syncDrainFixture builds a filesystem whose hub records patch and drain
// events. chunkSize 64 with a 16-byte base and small appends keeps commits
// on the patch rung so PatchFileRangesContext is the observable commit
// verb. Returns the filesystem, the probe, and an open write handle.
func syncDrainFixture(t *testing.T, flags uint32) (*Filesystem, *drainProbeHub, *storhubHandle) {
	t.Helper()
	probe := &drainProbeHub{stubHub: &stubHub{chunkSize: 64}}
	probe.patchRanges = func(edits []shfs.RangeEdit) (*meta.FileMeta, error) {
		probe.record("patch")
		return &meta.FileMeta{}, nil
	}
	probe.statPath = func(_ context.Context, _, target string) (*shfs.EntryInfo, error) {
		return &shfs.EntryInfo{Path: target, Inode: 7, Size: 16, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1}, nil
	}
	probe.loadReadonly = func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
		repo := meta.NewRepoMetadata("demo")
		repo.RebuildIndexes()
		return repo, "sha", nil
	}
	fsys, err := New(probe, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	h, err := fsys.newHandle(context.Background(), 7, "sync.bin", flags, &writeBootstrap{baseSize: 16})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	return fsys, probe, h
}

func indexOf(events []string, want string) int {
	for i, e := range events {
		if e == want {
			return i
		}
	}
	return -1
}

// Fsync must commit and then drain, in that order, proven by the
// recorded event sequence rather than by timing.
func TestFsyncDrainsAfterCommit(t *testing.T) {
	t.Parallel()
	_, probe, h := syncDrainFixture(t, syscall.O_WRONLY)
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := h.Fsync(context.Background(), 0); errno != 0 {
		t.Fatalf("fsync: %v", errno)
	}
	events := probe.eventLog()
	patch, drain := indexOf(events, "patch"), indexOf(events, "drain")
	if patch < 0 || drain < 0 {
		t.Fatalf("fsync must commit then drain, got events %v", events)
	}
	if drain < patch {
		t.Fatalf("drain must follow the commit, got events %v", events)
	}
}

// A buffered write must not drain: durability waits for Flush, Fsync, or
// Release, so the per-write path pays zero added latency.
func TestBufferedWriteDoesNotDrain(t *testing.T) {
	t.Parallel()
	_, probe, h := syncDrainFixture(t, syscall.O_WRONLY)
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	for _, e := range probe.eventLog() {
		if e == "drain" {
			t.Fatalf("buffered write must not drain, got events %v", probe.eventLog())
		}
	}
	if errno := h.Flush(context.Background()); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	if indexOf(probe.eventLog(), "drain") < 0 {
		t.Fatalf("flush must drain, got events %v", probe.eventLog())
	}
}

// O_SYNC writes must land remotely before Write returns: both the commit
// verb and the drain are observable synchronously on return.
func TestOSyncWriteIsDurableOnReturn(t *testing.T) {
	t.Parallel()
	_, probe, h := syncDrainFixture(t, syscall.O_WRONLY|syscall.O_SYNC)
	n, errno := h.Write(context.Background(), []byte("hello"), 16)
	if errno != 0 || n != 5 {
		t.Fatalf("o_sync write: n=%d errno=%v", n, errno)
	}
	events := probe.eventLog()
	if indexOf(events, "patch") < 0 || indexOf(events, "drain") < 0 {
		t.Fatalf("O_SYNC write must commit plus drain before returning, got events %v", events)
	}
}

// O_DSYNC is honored exactly like O_SYNC: the commit granularity cannot
// distinguish data from metadata durability, so a data-only barrier is
// still a full commit plus drain.
func TestODsyncWriteIsDurableOnReturn(t *testing.T) {
	t.Parallel()
	flag := dsyncFlagForTest()
	if flag == 0 {
		t.Skip("no distinct O_DSYNC spelling on this platform")
	}
	_, probe, h := syncDrainFixture(t, syscall.O_WRONLY|flag)
	n, errno := h.Write(context.Background(), []byte("hello"), 16)
	if errno != 0 || n != 5 {
		t.Fatalf("o_dsync write: n=%d errno=%v", n, errno)
	}
	events := probe.eventLog()
	if indexOf(events, "patch") < 0 || indexOf(events, "drain") < 0 {
		t.Fatalf("O_DSYNC write must commit plus drain before returning, got events %v", events)
	}
}

// A drain failure must surface EIO without quarantining the overlay: the
// bytes are uploaded and published and the journal owns recovery, so a
// quarantine would double-replay via redrive plus the preserved overlay.
func TestDrainFailureReturnsEIOWithoutQuarantine(t *testing.T) {
	t.Parallel()
	fsys, probe, h := syncDrainFixture(t, syscall.O_WRONLY)
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	probe.setDrainErr(errors.New("remote commit unavailable"))
	if errno := h.Fsync(context.Background(), 0); errno != syscall.EIO {
		t.Fatalf("drain failure must surface EIO, got %v", errno)
	}
	assertNoQuarantine(t, fsys, h)
	// Recovery is journal-owned: once the backend is back, a retry
	// drains cleanly with the same bytes.
	probe.setDrainErr(nil)
	if errno := h.Fsync(context.Background(), 0); errno != 0 {
		t.Fatalf("retry after drain failure: %v", errno)
	}
}

// The Release path applies the same rule: commit failure quarantines,
// drain failure returns EIO with normal (non-preserving) cleanup.
func TestReleaseDrainFailureReturnsEIOWithoutQuarantine(t *testing.T) {
	t.Parallel()
	fsys, probe, h := syncDrainFixture(t, syscall.O_WRONLY)
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	probe.setDrainErr(errors.New("remote commit unavailable"))
	if errno := h.Release(context.Background()); errno != syscall.EIO {
		t.Fatalf("release drain failure must surface EIO, got %v", errno)
	}
	if inv, err := fsys.RecoveryInventory(); err != nil || len(inv) != 0 {
		t.Fatalf("release drain failure must not quarantine, inventory=%v err=%v", inv, err)
	}
}

// An O_SYNC write whose drain fails reports EIO without quarantining;
// the staged bytes stay dirty for a later fsync or close to retry.
func TestOSyncDrainFailureReturnsEIOWithoutQuarantine(t *testing.T) {
	t.Parallel()
	fsys, probe, h := syncDrainFixture(t, syscall.O_WRONLY|syscall.O_SYNC)
	probe.setDrainErr(errors.New("remote commit unavailable"))
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != syscall.EIO {
		t.Fatalf("o_sync drain failure must surface EIO, got %v", errno)
	}
	assertNoQuarantine(t, fsys, h)
}

// assertNoQuarantine proves the no-quarantine half of the drain-failure
// contract: nothing preserved for manual recovery, and the write state
// is not poisoned (a later retry can still commit it).
func assertNoQuarantine(t *testing.T, fsys *Filesystem, h *storhubHandle) {
	t.Helper()
	if inv, err := fsys.RecoveryInventory(); err != nil || len(inv) != 0 {
		t.Fatalf("drain failure must not quarantine, inventory=%v err=%v", inv, err)
	}
	if h.writeState != nil {
		h.writeState.mu.Lock()
		poisoned := h.writeState.poisoned
		h.writeState.mu.Unlock()
		if poisoned {
			t.Fatal("drain failure must not poison the write state")
		}
	}
}

// Commit notifications must be emitted after the inode opMu is released,
// never across a synchronous kernel round trip while it is held. The
// probe hook runs on the async notify dispatch and asserts the lock is
// free; a dispatch under the old synchronous-under-opMu code would fail
// the TryLock.
func TestCommitNotificationsEmitAfterOpMuRelease(t *testing.T) {
	oldConnected := fsConnectedFunc
	oldContent := notifyContentFunc
	t.Cleanup(func() {
		fsConnectedFunc = oldConnected
		notifyContentFunc = oldContent
	})
	fsConnectedFunc = func(*Filesystem) bool { return true }
	dispatched := make(chan bool, 16)
	notifyContentFunc = func(node *storhubNode) {
		free := false
		if node != nil && node.fs != nil {
			if st := node.fs.writeStateForInode(node.inode); st != nil {
				free = st.opMu.TryLock()
				if free {
					st.opMu.Unlock()
				}
			} else {
				free = true
			}
		}
		dispatched <- free
	}
	_, _, h := syncDrainFixture(t, syscall.O_WRONLY)
	fsys := h.fs
	const inode = uint64(7)
	fsys.mu.RLock()
	node := fsys.nodes[inode]
	fsys.mu.RUnlock()
	if node == nil {
		node = fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "sync.bin", Inode: inode, Size: 16, Mode: 0o644})
	}
	_ = node
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := h.Fsync(context.Background(), 0); errno != 0 {
		t.Fatalf("fsync: %v", errno)
	}
	timeout := time.After(10 * time.Second)
	for {
		select {
		case free := <-dispatched:
			if !free {
				t.Fatal("content notification dispatched while opMu was held")
			}
			return
		case <-timeout:
			t.Fatal("content notification was never dispatched; invalidations must not be dropped")
		}
	}
}

// TestConcurrentAppendFlushStress hammers the previously-cyclic
// interleaving: concurrent O_APPEND writers plus flushers on one inode,
// with a slow commit verb widening every race window. The overall
// deadline bounds the run so CI can never hang: a wedge surfaces as a
// loud timeout FAIL, never a stuck suite.
func TestConcurrentAppendFlushStress(t *testing.T) {
	const writers = 8
	const appendsPerWriter = 25
	const flushers = 2
	const flushIterations = 100
	const stressBudget = 90 * time.Second

	done := make(chan struct{})
	go func() {
		defer close(done)
		runConcurrentAppendFlushStress(t, writers, appendsPerWriter, flushers, flushIterations)
	}()
	select {
	case <-done:
	case <-time.After(stressBudget):
		t.Fatal("concurrent append plus flush stress wedged: timed out")
	}
}

func runConcurrentAppendFlushStress(t *testing.T, writers, appendsPerWriter, flushers, flushIterations int) {
	t.Helper()
	probe := &drainProbeHub{stubHub: &stubHub{chunkSize: 1 << 20}}
	// Remote model: the fake backend applies every committed verb to an
	// in-memory image, so the final readback proves each appended byte
	// landed remotely exactly once. Commits serialize on opMu, so the
	// model needs its own mutex only against readFileAt. Every verb
	// records one "commit" event for the invalidation-coverage check.
	var remoteMu sync.Mutex
	var remote []byte
	applyPatchLocked := func(e shfs.RangeEdit) error {
		if e.Start < 0 || e.Start > int64(len(remote)) || e.Start+e.DeleteSize > int64(len(remote)) {
			return fmt.Errorf("patch %+v outside remote image len=%d", e, len(remote))
		}
		next := append(append([]byte(nil), remote[:e.Start]...), e.Data...)
		remote = append(next, remote[e.Start+e.DeleteSize:]...)
		return nil
	}
	probe.patchRanges = func(edits []shfs.RangeEdit) (*meta.FileMeta, error) {
		time.Sleep(2 * time.Millisecond)
		remoteMu.Lock()
		for _, e := range edits {
			if err := applyPatchLocked(e); err != nil {
				remoteMu.Unlock()
				t.Errorf("stress remote: %v", err)
				return nil, err
			}
		}
		size := int64(len(remote))
		remoteMu.Unlock()
		probe.record("commit")
		return &meta.FileMeta{Size: size}, nil
	}
	probe.replaceFile = func(_ context.Context, _, _ string, inputPath string) (*meta.FileMeta, error) {
		time.Sleep(2 * time.Millisecond)
		content, err := os.ReadFile(inputPath)
		if err != nil {
			t.Errorf("stress remote replace read: %v", err)
			return nil, err
		}
		remoteMu.Lock()
		remote = content
		size := int64(len(remote))
		remoteMu.Unlock()
		probe.record("commit")
		return &meta.FileMeta{Size: size}, nil
	}
	probe.truncateFile = func(_ context.Context, _, _ string, size int64) (*meta.FileMeta, error) {
		remoteMu.Lock()
		if size < int64(len(remote)) {
			remote = append([]byte(nil), remote[:size]...)
		} else {
			remote = append(remote, make([]byte, size-int64(len(remote)))...)
		}
		remoteMu.Unlock()
		probe.record("commit")
		return &meta.FileMeta{Size: size}, nil
	}
	probe.readFileAt = func(_ context.Context, _, _ string, off, length int64) ([]byte, error) {
		remoteMu.Lock()
		defer remoteMu.Unlock()
		if off >= int64(len(remote)) {
			return []byte{}, nil
		}
		end := off + length
		if end > int64(len(remote)) {
			end = int64(len(remote))
		}
		return append([]byte(nil), remote[off:end]...), nil
	}
	probe.statPath = func(_ context.Context, _, target string) (*shfs.EntryInfo, error) {
		return &shfs.EntryInfo{Path: target, Inode: 9, Size: 0, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1}, nil
	}
	probe.loadReadonly = func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
		repo := meta.NewRepoMetadata("demo")
		repo.RebuildIndexes()
		return repo, "sha", nil
	}
	fsys, err := New(probe, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	const inode = uint64(9)
	base, err := fsys.newHandle(context.Background(), inode, "stress.bin", syscall.O_WRONLY|syscall.O_APPEND, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	defer func() { _ = base.Release(context.Background()) }()

	var wg sync.WaitGroup
	errCh := make(chan error, writers+flushers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			payload := []byte{byte('a' + w)}
			for i := 0; i < appendsPerWriter; i++ {
				// Append emulation: an offset at or past the overlay EOF
				// takes the O_APPEND force-to-EOF branch, exactly as the
				// kernel's append writeback does. Each write serializes
				// on opMu and lands once at the then-current EOF. The
				// pacing keeps appends in flight while flushers commit,
				// so every commit races concurrent writers.
				if _, errno := base.Write(context.Background(), payload, 1<<62); errno != 0 {
					errCh <- fmt.Errorf("stress write: errno %v", errno)
					return
				}
				time.Sleep(200 * time.Microsecond)
			}
		}(w)
	}
	for f := 0; f < flushers; f++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < flushIterations; i++ {
				if errno := base.Flush(context.Background()); errno != 0 {
					errCh <- fmt.Errorf("stress flush: errno %v", errno)
					return
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("stress worker failed: %v", err)
	default:
	}
	if errno := base.Flush(context.Background()); errno != 0 {
		t.Fatalf("final flush: %v", errno)
	}
	// Byte conservation: every append landed exactly once, in some
	// interleaving, with no loss and no duplication.
	total := writers * appendsPerWriter
	got := make([]byte, total)
	res, errno := base.Read(context.Background(), got, 0)
	if errno != 0 {
		t.Fatalf("stress readback: %v", errno)
	}
	data, _ := res.Bytes(got)
	if len(data) != total {
		t.Fatalf("append bytes lost or duplicated: got %d want %d", len(data), total)
	}
	counts := make(map[byte]int)
	for _, b := range data {
		counts[b]++
	}
	for w := 0; w < writers; w++ {
		if counts[byte('a'+w)] != appendsPerWriter {
			t.Fatalf("writer %d bytes: got %d want %d", w, counts[byte('a'+w)], appendsPerWriter)
		}
	}
	// Every successful commit emits at least one content plus one entry
	// invalidation; prove none were dropped on the touched paths.
	commits := 0
	for _, e := range probe.eventLog() {
		if e == "commit" {
			commits++
		}
	}
	if commits == 0 {
		t.Fatal("stress run committed nothing; the interleaving was not exercised")
	}
	t.Logf("stress run: %d commits over %d appended bytes", commits, total)
	if got, want := fsys.Invalidations(), uint64(2*commits); got < want {
		t.Fatalf("missed invalidations: %d commits need >= %d, got %d", commits, want, got)
	}
}
