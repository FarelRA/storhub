package fusefs

import (
	"context"
	"runtime"
	"syscall"
	"testing"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestSetlkwReleasesAfterFuncGoroutine is the goroutine-count regression
// for the context.AfterFunc leak: go-fuse's per-request context is never
// closed on normal completion, so a Setlkw that drops the stop-func parks
// one goroutine (pinning the whole Filesystem through its closure) per
// successful blocking lock. The fix captures the stop-func and defers it;
// a completed request must leave no goroutine behind.
func TestSetlkwReleasesAfterFuncGoroutine(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 42, "locked.bin", syscall.O_RDONLY, nil)
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	runtime.GC()
	baseline := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		// A cancellable context that is deliberately never canceled:
		// this is exactly the shape of go-fuse's per-request Done() on
		// normal completion.
		ctx, cancel := context.WithCancel(context.Background())
		lk := fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_WRLCK}
		if errno := h.Setlkw(ctx, 1, &lk, 0); errno != 0 {
			cancel()
			t.Fatalf("setlkw %d: errno=%v", i, errno)
		}
		_ = cancel
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if runtime.NumGoroutine() <= baseline+5 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Setlkw leaked goroutines: baseline=%d now=%d (stop-func must be deferred)", baseline, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestReadAheadBytesCapped pins the MaxReadAhead units fix: the kernel
// readahead window is the normal FUSE cap, never the (multi-GB) chunk
// size.
func TestReadAheadBytesCapped(t *testing.T) {
	t.Parallel()
	if got := readAheadBytes(chunking.DefaultChunkSize); got != maxReadAheadBytes {
		t.Fatalf("default chunk size must clamp to %d, got %d", maxReadAheadBytes, got)
	}
	if got := readAheadBytes(1 << 20); got != 1<<20 {
		t.Fatalf("small chunk size must pass through, got %d", got)
	}
	if got := readAheadBytes(0); got != maxReadAheadBytes {
		t.Fatalf("zero chunk size must normalize then clamp, got %d", got)
	}
}

// TestMarkDirtyLockedMergesInPlace pins the merge semantics of the
// allocation-free rewrite: sorted, disjoint, touching ranges coalesce.
func TestMarkDirtyLockedMergesInPlace(t *testing.T) {
	t.Parallel()
	w := &inodeWriteState{}
	w.markDirtyLocked(10, 20)
	w.markDirtyLocked(30, 40)
	w.markDirtyLocked(50, 60)
	w.markDirtyLocked(25, 45)
	want := []ByteRange{{Start: 10, End: 20}, {Start: 25, End: 45}, {Start: 50, End: 60}}
	assertRanges(t, w.dirtyRanges, want)
	w.markDirtyLocked(18, 26) // bridges the first two
	assertRanges(t, w.dirtyRanges, []ByteRange{{Start: 10, End: 45}, {Start: 50, End: 60}})
	w.markDirtyLocked(45, 50) // touches both ends of the gap
	assertRanges(t, w.dirtyRanges, []ByteRange{{Start: 10, End: 60}})
	w.markDirtyLocked(0, 100) // absorbs everything
	assertRanges(t, w.dirtyRanges, []ByteRange{{Start: 0, End: 100}})
	w.markDirtyLocked(70, 80) // contained: no change
	assertRanges(t, w.dirtyRanges, []ByteRange{{Start: 0, End: 100}})
	w.markDirtyLocked(200, 210) // append after
	assertRanges(t, w.dirtyRanges, []ByteRange{{Start: 0, End: 100}, {Start: 200, End: 210}})
	w.markDirtyLocked(-5, 3) // clamps negative start, extends head
	assertRanges(t, w.dirtyRanges, []ByteRange{{Start: 0, End: 100}, {Start: 200, End: 210}})
}

func assertRanges(t *testing.T, got, want []ByteRange) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ranges = %+v, want %+v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("ranges[%d] = %+v, want %+v (full: %+v)", i, got[i], want[i], got)
		}
	}
}

// TestNotifyCoalescesDuplicates pins the post-commit invalidation
// batching: while one notification for a target is pending, duplicates
// for the same (kind, node, name) collapse into it; a different kind or
// name does not.
func TestNotifyCoalescesDuplicates(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := &storhubNode{fs: fsys}
	key := notifyKey{kind: notifyKindEntry, node: node, name: "a"}
	if !fsys.beginNotify(key) {
		t.Fatal("first beginNotify must dispatch")
	}
	if fsys.beginNotify(key) {
		t.Fatal("duplicate beginNotify must coalesce into the pending one")
	}
	otherName := notifyKey{kind: notifyKindEntry, node: node, name: "b"}
	if !fsys.beginNotify(otherName) {
		t.Fatal("a different name must not coalesce")
	}
	otherKind := notifyKey{kind: notifyKindDelete, node: node, name: "a"}
	if !fsys.beginNotify(otherKind) {
		t.Fatal("a different kind must not coalesce")
	}
	fsys.endNotify(key)
	if !fsys.beginNotify(key) {
		t.Fatal("beginNotify after endNotify must dispatch again")
	}
}

// TestNotifySlotsBoundCaller pins the concurrency bound: once every slot
// is held, beginNotify blocks the mutation path (backpressure) instead of
// spawning another notify goroutine, and resumes when a slot frees.
func TestNotifySlotsBoundCaller(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	for i := 0; i < maxConcurrentNotifies; i++ {
		fsys.notifySlots <- struct{}{}
	}
	key := notifyKey{kind: notifyKindEntry, node: &storhubNode{fs: fsys}, name: "x"}
	blocked := make(chan bool, 1)
	go func() { blocked <- fsys.beginNotify(key) }()
	select {
	case <-blocked:
		t.Fatal("beginNotify must block while all notify slots are held")
	case <-time.After(100 * time.Millisecond):
	}
	<-fsys.notifySlots
	select {
	case ok := <-blocked:
		if !ok {
			t.Fatal("beginNotify must dispatch once a slot frees")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("beginNotify did not resume after a slot freed")
	}
}

// TestDirHandleServesListingSnapshot exercises the READDIRPLUS seam: the
// handle streams the listing and keeps a per-child attribute snapshot so
// the kernel's entry fills never re-stat.
func TestDirHandleServesListingSnapshot(t *testing.T) {
	t.Parallel()
	now := int64(99)
	hub := &stubHub{
		readDir: func(context.Context, string, string) ([]shfs.DirEntry, error) {
			return []shfs.DirEntry{
				{Name: "sub", Path: "sub", IsDir: true, Inode: 2, Mode: 0o755, NLink: 2, UID: 1000, GID: 1000, CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now},
				{Name: "f.txt", Path: "f.txt", Kind: meta.NodeKindFile, Size: 7, Inode: 3, Mode: 0o640, NLink: 1, UID: 1001, GID: 1001, ModifiedAt: now},
			}, nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	fh, flags, errno := fsys.root.OpendirHandle(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("opendir: errno=%v", errno)
	}
	if flags != 0 {
		t.Fatalf("opendir flags = %#x, want 0", flags)
	}
	dh, ok := fh.(*storhubDirHandle)
	if !ok {
		t.Fatalf("opendir handle type %T", fh)
	}
	var _ gofusefs.FileReaddirenter = dh
	var _ gofusefs.FileLookuper = dh
	var _ gofusefs.FileSeekdirer = dh
	var names []string
	for {
		de, errno := dh.Readdirent(context.Background())
		if errno != 0 {
			t.Fatalf("readdirent: errno=%v", errno)
		}
		if de == nil {
			break
		}
		names = append(names, de.Name)
	}
	want := []string{".", "..", "sub", "f.txt"}
	if len(names) != len(want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("names[%d] = %q, want %q", i, names[i], want[i])
		}
	}
	entry := dh.infos["f.txt"]
	if entry == nil {
		t.Fatal("listing snapshot must carry f.txt")
	}
	if entry.UID != 1001 || entry.GID != 1001 || entry.Size != 7 || entry.Inode != 3 || entry.Mode != 0o640 || entry.NLink != 1 || entry.Path != "f.txt" || entry.ModifiedAt != now {
		t.Fatalf("snapshot entry lost attributes: %+v", entry)
	}
	if _, plus := dh.infos["."]; plus {
		t.Fatal("virtual entries must not carry attribute fills")
	}
	if errno := dh.Seekdir(context.Background(), 2); errno != 0 {
		t.Fatalf("seekdir: errno=%v", errno)
	}
	de, errno := dh.Readdirent(context.Background())
	if errno != 0 || de == nil || de.Name != "sub" || de.Off != 3 {
		t.Fatalf("seekdir replay: de=%+v errno=%v", de, errno)
	}
}

// TestReadOnlyMountRaisesKernelTimeouts pins the revalidation-storm fix:
// read-only mounts default to long entry/attr timeouts, explicit values
// still win, and writable mounts keep the 60s defaults.
func TestReadOnlyMountRaisesKernelTimeouts(t *testing.T) {
	t.Parallel()
	ro, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir(), ExtraMountOpts: []string{"noatime", "ro"}})
	if err != nil {
		t.Fatalf("new ro filesystem: %v", err)
	}
	defer func() { _ = ro.Close() }()
	if got := ro.Options().EntryTimeout; got != readOnlyEntryTimeout {
		t.Fatalf("read-only EntryTimeout = %v, want %v", got, readOnlyEntryTimeout)
	}
	if got := ro.Options().AttrTimeout; got != readOnlyAttrTimeout {
		t.Fatalf("read-only AttrTimeout = %v, want %v", got, readOnlyAttrTimeout)
	}
	explicit, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir(), ExtraMountOpts: []string{"ro"}, EntryTimeout: time.Second, AttrTimeout: time.Second})
	if err != nil {
		t.Fatalf("new explicit-timeout filesystem: %v", err)
	}
	defer func() { _ = explicit.Close() }()
	if explicit.Options().EntryTimeout != time.Second || explicit.Options().AttrTimeout != time.Second {
		t.Fatal("explicit timeouts must win over the read-only defaults")
	}
	rw, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new rw filesystem: %v", err)
	}
	defer func() { _ = rw.Close() }()
	if rw.Options().EntryTimeout != 60*time.Second || rw.Options().AttrTimeout != 60*time.Second {
		t.Fatalf("writable mount timeouts changed: %v/%v", rw.Options().EntryTimeout, rw.Options().AttrTimeout)
	}
}
