package fusefs

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// versionHub wraps a Hub with a fake per-project version counter, standing
// in for StorHub.ProjectVersion (which the pcHub test double does not
// implement). Bumping it simulates a cross-surface (REST/CLI) publish that
// swaps shared truth without touching the FUSE layer.
type versionHub struct {
	Hub
	mu      sync.Mutex
	version uint64
	present bool
}

func newVersionHub(h Hub) *versionHub {
	return &versionHub{Hub: h, present: true}
}

func (h *versionHub) ProjectVersion(project string) (uint64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.present || project == "" {
		return 0, false
	}
	return h.version, true
}

func (h *versionHub) bumpVersion() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.version++
}

// TestProjectVersionWatcherBaselineAndChange pins the subscriber contract:
// the first observation establishes the baseline (never a change), a steady
// counter reports no change, and any movement reports exactly one change.
// A hub without the counter capability never reports changes.
func TestProjectVersionWatcherBaselineAndChange(t *testing.T) {
	t.Parallel()
	hub := newVersionHub(newPCHub())
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	w := fsys.NewProjectVersionWatcher()
	if cur, changed := w.Check(); changed || cur != 0 {
		t.Fatalf("first check must baseline without change, got cur=%d changed=%v", cur, changed)
	}
	if _, changed := w.Check(); changed {
		t.Fatal("steady counter must not report change")
	}
	hub.bumpVersion()
	cur, changed := w.Check()
	if !changed || cur != 1 {
		t.Fatalf("bumped counter must report change once, got cur=%d changed=%v", cur, changed)
	}
	if _, changed := w.Check(); changed {
		t.Fatal("change must be reported exactly once per movement")
	}

	plain, err := New(newPCHub(), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = plain.Close() }()
	pw := plain.NewProjectVersionWatcher()
	if _, changed := pw.Check(); changed {
		t.Fatal("hub without a version counter must never report change (expiry fallback)")
	}
	if _, ok := plain.HubProjectVersion(); ok {
		t.Fatal("hub without a version counter must report ok=false")
	}
}

// TestVersionFanoutTargetedInvalidation proves the O(affected) fan-out
// without a mount: after a simulated cross-surface publish (backend mutated
// directly, version bumped, no FUSE notify), the watcher detects the move
// and exactly one targeted invalidation covers the affected entry.
func TestVersionFanoutTargetedInvalidation(t *testing.T) {
	t.Parallel()
	inner := newPCHub()
	hub := newVersionHub(inner)
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	w := fsys.NewProjectVersionWatcher()
	if _, changed := w.Check(); changed {
		t.Fatal("baseline must not report change")
	}

	inner.mu.Lock()
	inner.files["fanout.txt"] = &pcFile{data: []byte("v2"), mode: 0o644, inode: inner.nextInode}
	inner.byInode[inner.nextInode] = inner.files["fanout.txt"]
	inner.nextInode++
	inner.mu.Unlock()
	hub.bumpVersion()

	cur, changed := w.Check()
	if !changed || cur != 1 {
		t.Fatalf("simulated REST publish must be detected, got cur=%d changed=%v", cur, changed)
	}
	before := fsys.Invalidations()
	fsys.notifyEntryForPath("", "fanout.txt")
	if got := fsys.Invalidations() - before; got != 1 {
		t.Fatalf("exactly one targeted invalidation per affected entry, got %d", got)
	}
	if _, changed := w.Check(); changed {
		t.Fatal("no further change without further mutation")
	}
}

// TestVersionFanoutMountFreshness is the kernel-visible proof: with entry
// and attr timeouts far beyond the test duration, a stat that turns fresh
// after a hub-side mutation plus targeted invalidation proves invalidation,
// not expiry.
func TestVersionFanoutMountFreshness(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("mount test needs /dev/fuse: %v", err)
	}
	inner := newPCHub()
	hub := newVersionHub(inner)
	parent := t.TempDir()
	mountPoint := filepath.Join(parent, "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatal(err)
	}
	fsys, err := New(hub, "demo", Options{
		CacheDir:        filepath.Join(parent, "cache"),
		EntryTimeout:    10 * time.Minute,
		AttrTimeout:     10 * time.Minute,
		NegativeTimeout: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	// Seed backend truth directly (no FUSE mutation, so no commit-time
	// async invalidation is ever in flight to race the fixture below).
	inner.mu.Lock()
	inner.files["fanout.txt"] = &pcFile{data: []byte("v1"), mode: 0o644, inode: inner.nextInode}
	inner.byInode[inner.nextInode] = inner.files["fanout.txt"]
	inner.nextInode++
	inner.mu.Unlock()

	if err := fsys.Mount(mountPoint); err != nil {
		t.Skipf("mount: %v", err)
	}
	defer func() { _ = fsys.Unmount() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(mountPoint); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Skipf("mount never ready: %v", err)
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}

	full := filepath.Join(mountPoint, "fanout.txt")
	// Prime every kernel cache line (entry, attr, page) with read-only
	// traffic: reads never notify, so nothing evicts these until the
	// explicit targeted invalidation below.
	if got, err := os.ReadFile(full); err != nil || string(got) != "v1" {
		t.Fatalf("prime read = %q, %v; want %q", got, err, "v1")
	}

	w := fsys.NewProjectVersionWatcher()
	if _, changed := w.Check(); changed {
		t.Fatal("baseline must not report change")
	}

	// Simulated REST-side publish: backend truth moves with no FUSE notify.
	inner.mu.Lock()
	f := inner.files["fanout.txt"]
	if f == nil {
		inner.mu.Unlock()
		t.Fatal("seeded file missing from backend")
	}
	f.data = append([]byte(nil), []byte("v2-longer")...)
	inner.touchLocked(f)
	inner.mu.Unlock()
	hub.bumpVersion()

	// The kernel cache is stale by construction (10-minute timeouts): this
	// pins the fixture so a fresh stat below cannot be mistaken for
	// expiry or for the kernel revalidating on its own.
	if fi, err := os.Stat(full); err != nil || fi.Size() != 2 {
		t.Fatalf("fixture: stat must still serve the cached size 2, got %v, %v", fiSize(fi), err)
	}

	cur, changed := w.Check()
	if !changed || cur != 1 {
		t.Fatalf("hub-side publish must be detected, got cur=%d changed=%v", cur, changed)
	}
	// Targeted invalidation of exactly the affected entry: namespace plus
	// page cache, no sweeps, no global invalidate.
	fsys.notifyEntryForPath("", "fanout.txt")
	fsys.mu.RLock()
	node := fsys.nodeForPathLocked("fanout.txt")
	fsys.mu.RUnlock()
	if node == nil {
		t.Fatal("affected entry must have a live node")
	}
	fsys.notifyKernelContentChanged(node.inode)

	deadline = time.Now().Add(10 * time.Second)
	for {
		fi, err := os.Stat(full)
		if err == nil && fi.Size() == int64(len("v2-longer")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stat never turned fresh after targeted invalidation (size still %v, err %v)", fiSize(fi), err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		got, err := os.ReadFile(full)
		if err == nil && string(got) == "v2-longer" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("read never turned fresh after targeted invalidation (got %q, err %v)", got, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func fiSize(fi os.FileInfo) int64 {
	if fi == nil {
		return -1
	}
	return fi.Size()
}

// TestProjectVersionPollerDrivesSubscriber pins the background subscriber:
// a version move fires the callback, and stop ends delivery.
func TestProjectVersionPollerDrivesSubscriber(t *testing.T) {
	t.Parallel()
	hub := newVersionHub(newPCHub())
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	var fired atomic.Uint64
	w := fsys.NewProjectVersionWatcher()
	if _, changed := w.Check(); changed {
		t.Fatal("pre-poll check must baseline without change")
	}
	stop := w.StartPoll(5*time.Millisecond, func(cur uint64) { fired.Add(1) })
	defer stop()
	hub.bumpVersion()
	deadline := time.Now().Add(5 * time.Second)
	for fired.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("poller never fired after a version move")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	n := fired.Load()
	time.Sleep(50 * time.Millisecond)
	if fired.Load() != n {
		t.Fatal("stop must end delivery")
	}
}
