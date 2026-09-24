package fusefs

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
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
	f.data = append([]byte(nil), []byte("v2longer")...)
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
		if err == nil && fi.Size() == int64(len("v2longer")) {
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
		if err == nil && string(got) == "v2longer" {
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

// pushHub is a Hub double with a StorHub-shaped publish fan-out: a
// sequence-numbered path ring plus a subscriber set. publish appends to
// the ring and pokes subscribers asynchronously, mirroring
// notePublishedPathsLocked's harvest-then-async-dispatch contract, so
// fuse-side tests prove the push wiring instead of the deleted poller.
type pushHub struct {
	Hub
	mu      sync.Mutex
	seq     uint64
	recent  []pushEntry
	subs    map[uint64]func()
	nextSub uint64
}

type pushEntry struct {
	seq   uint64
	paths []string // nil = unknown scope
}

func newPushHub(h Hub) *pushHub {
	return &pushHub{Hub: h, subs: make(map[uint64]func())}
}

func (h *pushHub) PublishedPathsSince(_ string, since uint64) ([]string, bool, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	current := h.seq
	if len(h.recent) == 0 {
		return nil, false, current
	}
	if oldest := h.recent[0].seq; since+1 < oldest {
		return nil, true, current
	}
	seen := make(map[string]struct{})
	for _, entry := range h.recent {
		if entry.seq <= since {
			continue
		}
		if entry.paths == nil {
			return nil, true, current
		}
		for _, p := range entry.paths {
			seen[p] = struct{}{}
		}
	}
	var paths []string
	for p := range seen {
		paths = append(paths, p)
	}
	return paths, false, current
}

// SubscribeProjectPublishes registers a push subscriber: every later
// publish pokes onPublish asynchronously. It returns the current cursor
// (captured under the same hold as the insert, so no publish slips
// between) plus an idempotent unsubscribe.
func (h *pushHub) SubscribeProjectPublishes(_ string, onPublish func()) (uint64, func()) {
	h.mu.Lock()
	id := h.nextSub
	h.nextSub++
	h.subs[id] = onPublish
	cur := h.seq
	h.mu.Unlock()
	var once sync.Once
	return cur, func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			delete(h.subs, id)
		})
	}
}

// publish records one publish footprint and pokes subscribers
// asynchronously, after the registry hold is released: never a
// synchronous kernel write under the subscription lock.
func (h *pushHub) publish(paths []string) {
	h.mu.Lock()
	h.seq++
	h.recent = append(h.recent, pushEntry{seq: h.seq, paths: paths})
	cbs := make([]func(), 0, len(h.subs))
	for _, cb := range h.subs {
		cbs = append(cbs, cb)
	}
	h.mu.Unlock()
	go func() {
		for _, cb := range cbs {
			cb()
		}
	}()
}

// TestPushSubscriberDrivesDelivery pins the push contract: New registers
// the mount's subscription at construction, a publish fires it, delivery
// runs through pollInvalidationsOnce (the invalidation counter moves),
// and Close unregisters so later publishes deliver nothing.
func TestPushSubscriberDrivesDelivery(t *testing.T) {
	t.Parallel()
	hub := newPushHub(newPCHub())
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	hub.mu.Lock()
	nsubs := len(hub.subs)
	hub.mu.Unlock()
	if nsubs != 1 {
		t.Fatalf("construction must register exactly one push subscription, got %d", nsubs)
	}
	before := fsys.Invalidations()
	start := time.Now()
	hub.publish([]string{"push.txt"})
	deadline := time.Now().Add(5 * time.Second)
	for fsys.Invalidations() == before {
		if time.Now().After(deadline) {
			t.Fatal("push publish never delivered through the subscription")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("push wake latency: %v", time.Since(start))
	if err := fsys.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	hub.mu.Lock()
	nsubs = len(hub.subs)
	hub.mu.Unlock()
	if nsubs != 0 {
		t.Fatalf("close must unregister the push subscription, %d left", nsubs)
	}
	n := fsys.Invalidations()
	hub.publish([]string{"push-again.txt"})
	time.Sleep(300 * time.Millisecond)
	if fsys.Invalidations() != n {
		t.Fatal("close must end delivery")
	}
}

// TestFanoutPushNoPollingGoroutine proves the push fan-out core claim: mounts
// carry no polling goroutine. Eight mounts must add far fewer than eight
// goroutines (the deleted ticker added exactly one each), publish
// delivery must work on every one, and Close must return the census to
// baseline.
func TestFanoutPushNoPollingGoroutine(t *testing.T) {
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	g0 := runtime.NumGoroutine()
	const mounts = 8
	hubs := make([]*pushHub, 0, mounts)
	fss := make([]*Filesystem, 0, mounts)
	for i := 0; i < mounts; i++ {
		hub := newPushHub(newPCHub())
		fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
		if err != nil {
			t.Fatalf("new filesystem %d: %v", i, err)
		}
		defer func() { _ = fsys.Close() }()
		fss = append(fss, fsys)
		hubs = append(hubs, hub)
	}
	time.Sleep(500 * time.Millisecond)
	if g1 := runtime.NumGoroutine(); g1-g0 > 4 {
		t.Fatalf("mounts must not add a polling goroutine each: base=%d with-mounts=%d (want <= base+4 for %d mounts)", g0, g1, mounts)
	}
	for i, hub := range hubs {
		before := fss[i].Invalidations()
		hub.publish([]string{"push.txt"})
		deadline := time.Now().Add(5 * time.Second)
		for fss[i].Invalidations() == before {
			if time.Now().After(deadline) {
				t.Fatalf("mount %d: push publish never delivered", i)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	time.Sleep(500 * time.Millisecond)
	if g2 := runtime.NumGoroutine(); g2-g0 > 4 {
		t.Fatalf("publishes must not strand goroutines: base=%d after-publish=%d (want <= base+4)", g0, g2)
	}
	for _, fsys := range fss {
		if err := fsys.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	time.Sleep(500 * time.Millisecond)
	if g3 := runtime.NumGoroutine(); g3-g0 > 4 {
		t.Fatalf("Close must not leak subscriptions: base=%d after-close=%d (want <= base+4)", g0, g3)
	}
}

// TestPushFanoutMountFreshness is the kernel-visible push proof: with
// entry and attr timeouts far beyond the test duration, a stat that turns
// fresh after a hub-side publish plus push delivery proves the push, not
// expiry. Wake latency is asserted on the subscription callback channel,
// not with sleeps.
func TestPushFanoutMountFreshness(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("mount test needs /dev/fuse: %v", err)
	}
	inner := newPCHub()
	hub := newPushHub(inner)
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
	// push delivery below.
	if got, err := os.ReadFile(full); err != nil || string(got) != "v1" {
		t.Fatalf("prime read = %q, %v; want %q", got, err, "v1")
	}

	// A second subscriber pins the push wake itself on a channel: the
	// mount's own subscription (registered at New) drives the kernel
	// proof below, this one measures the poke latency.
	fired := make(chan time.Time, 4)
	_, unsub := hub.SubscribeProjectPublishes("demo", func() { fired <- time.Now() })
	defer unsub()

	// The kernel cache is stale-proofed by construction (10-minute
	// timeouts): this stat must serve the primed size, so a fresh stat
	// after the publish below cannot be mistaken for expiry or for the
	// kernel revalidating on its own. It runs before the publish, so no
	// delivery can race it.
	if fi, err := os.Stat(full); err != nil || fi.Size() != 2 {
		t.Fatalf("fixture: stat must serve the primed size 2, got %v, %v", fiSize(fi), err)
	}

	// Simulated REST-side publish: backend truth moves, then the hub
	// pokes every subscriber. No FUSE notify, no polling.
	inner.mu.Lock()
	f := inner.files["fanout.txt"]
	if f == nil {
		inner.mu.Unlock()
		t.Fatal("seeded file missing from backend")
	}
	f.data = append([]byte(nil), []byte("v2longer")...)
	inner.touchLocked(f)
	inner.mu.Unlock()
	start := time.Now()
	hub.publish([]string{"fanout.txt"})
	select {
	case ft := <-fired:
		t.Logf("push wake latency: %v", ft.Sub(start))
	case <-time.After(5 * time.Second):
		t.Fatal("push callback never fired after publish")
	}

	deadline = time.Now().Add(10 * time.Second)
	for {
		fi, err := os.Stat(full)
		if err == nil && fi.Size() == int64(len("v2longer")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stat never turned fresh after push delivery (size still %v, err %v)", fiSize(fi), err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		got, err := os.ReadFile(full)
		if err == nil && string(got) == "v2longer" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("read never turned fresh after push delivery (got %q, err %v)", got, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
