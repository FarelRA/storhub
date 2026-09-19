package fusefs

import (
	"log/slog"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/FarelRA/storhub/internal/logging"
)

// projectVersioner is the optional hub capability behind cross-surface
// invalidation fan-out: a per-project version counter that advances on
// every swap of shared truth. StorHub implements it
// (internal/storage ProjectVersion); hubs that do not (test doubles)
// report ok=false and the mount falls back to timeout expiry, changing
// nothing.
type projectVersioner interface {
	ProjectVersion(project string) (uint64, bool)
}

// HubProjectVersion reads the hub's per-project version counter for this
// mount's project. ok=false means the hub has no counter (or the project
// is not resident there): the caller keeps timeout-expiry behavior.
func (s *Filesystem) HubProjectVersion() (uint64, bool) {
	if s == nil || s.hub == nil {
		return 0, false
	}
	pv, ok := s.hub.(projectVersioner)
	if !ok {
		return 0, false
	}
	return pv.ProjectVersion(s.project)
}

// ProjectVersionWatcher subscribes one mount to its hub's version counter.
// It owns only its last-seen baseline (no filesystem or hub state is
// touched), so mounts without a poller pay nothing and dropping the
// watcher leaks nothing. All methods are safe for concurrent use; the
// poll callback runs on the poller's goroutine, where only async-safe
// notify calls (notifyEntryForPath, notifyKernelContentChanged) belong.
//
// Fan-out wiring note: the watcher detects THAT the project moved; the
// affected entry set comes from the caller, which notifies exactly those
// entries (O(affected), no sweeps, no global invalidate). Comparing the
// counter on the lookup/stat path itself and starting the background poll
// at mount construction are follow-ups outside this file; tests drive the
// loop explicitly and prove the kernel-visible result.
type ProjectVersionWatcher struct {
	fs   *Filesystem
	mu   sync.Mutex
	last uint64
	have bool
}

// NewProjectVersionWatcher starts a version subscription for this mount.
// The first Check establishes the baseline and never reports a change.
func (s *Filesystem) NewProjectVersionWatcher() *ProjectVersionWatcher {
	return &ProjectVersionWatcher{fs: s}
}

// Check compares the hub counter against the last-seen baseline, advances
// the baseline, and reports whether the project moved. A hub without the
// counter capability never reports a change.
func (w *ProjectVersionWatcher) Check() (cur uint64, changed bool) {
	if w == nil {
		return 0, false
	}
	cur, ok := w.fs.HubProjectVersion()
	if !ok {
		return 0, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.have {
		w.last, w.have = cur, true
		return cur, false
	}
	if cur != w.last {
		w.last = cur
		return cur, true
	}
	return cur, false
}

// StartPoll runs the subscriber loop: every interval it checks the counter
// and invokes onChange (if non-nil) exactly once per observed movement.
// The returned stop ends delivery; it is idempotent. The zero interval
// resolves to one second.
func (w *ProjectVersionWatcher) StartPoll(interval time.Duration, onChange func(cur uint64)) (stop func()) {
	if interval <= 0 {
		interval = time.Second
	}
	stopCh := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if cur, changed := w.Check(); changed && onChange != nil {
					onChange(cur)
				}
			}
		}
	}()
	return func() { once.Do(func() { close(stopCh) }) }
}

var (
	notifyEntryFunc  = func(node *storhubNode, name string) { _ = node.NotifyEntry(name) }
	notifyDeleteFunc = func(parent *storhubNode, name string, child *storhubNode) {
		_ = parent.NotifyDelete(name, child.EmbeddedInode())
	}
	notifyContentFunc = func(node *storhubNode) { _ = node.NotifyContent(0, 0) }
)

// notifyKey identifies one pending entry/delete/content notification.
type notifyKey struct {
	kind uint8 // notifyKindEntry or notifyKindDelete
	node *storhubNode
	name string
}

const (
	notifyKindEntry uint8 = iota
	notifyKindDelete
	notifyKindContent
)

func (s *Filesystem) notifyEntryForPath(dirPath, name string) {
	// Count first: the kernel notify below is skipped on unmounted
	// (test-driven) filesystems, but the invalidation was still issued.
	s.invalCount.Add(1)
	s.mu.RLock()
	node := s.nodeForPathLocked(dirPath)
	s.mu.RUnlock()
	safeNotifyEntry(node, name)
}

func (n *storhubNode) notifyEntry(name string) {
	n.fs.notifyEntryForPath(n.currentPath(), name)
}

func (n *storhubNode) notifyDelete(name string, childInode uint64) {
	n.fs.invalCount.Add(1)
	n.fs.mu.RLock()
	child := n.fs.nodes[childInode]
	n.fs.mu.RUnlock()
	safeNotifyDelete(n, name, child)
}

func (s *Filesystem) notifyKernelContentChanged(inode uint64) {
	// Async through the notify slots, never a synchronous kernel write on
	// the caller: commit paths must not hold the inode opMu across a
	// NotifyContent round trip (see commitNotifies), and the slot bound
	// keeps backpressure gentle instead of piling up goroutines.
	s.invalCount.Add(1)
	s.mu.RLock()
	node := s.nodes[inode]
	s.mu.RUnlock()
	safeNotifyContentAsync(node)
}

// commitNotifies collects kernel-cache invalidation intents DURING a
// commit while the inode opMu (and the state mu) is held, for emission
// AFTER both are released. The commit cycle this breaks: commit held opMu
// across synchronous NotifyContent, while the kernel reverse-invalidate
// waited on page writeback whose Write handler needs opMu, wedging mounts.
// Emitting after unlock through the async slots keeps the same set of
// invalidations (redundant ones are acceptable and coalesce in flight;
// missed ones are not, so every path that notified before must append
// here and the committer must emit on every success return).
type commitNotifies struct {
	contents []uint64
	entries  [][2]string
}

func (c *commitNotifies) addContent(inode uint64) {
	if c == nil {
		return
	}
	c.contents = append(c.contents, inode)
}

func (c *commitNotifies) addEntry(dir, name string) {
	if c == nil {
		return
	}
	c.entries = append(c.entries, [2]string{dir, name})
}

func (c *commitNotifies) emit(fs *Filesystem) {
	if c == nil || fs == nil {
		return
	}
	for _, inode := range c.contents {
		fs.notifyKernelContentChanged(inode)
	}
	for _, e := range c.entries {
		fs.notifyEntryForPath(e[0], e[1])
	}
}

// fsConnected reports whether the filesystem is currently served over a
// FUSE connection. Kernel cache notifications are meaningless without a
// mount, and calling them on a detached filesystem - as tests do when
// they drive nodes directly - panics inside go-fuse on the nil
// connection state. Callers skip notification entirely in that case.
var fsConnectedFunc = (*Filesystem).connected

func (s *Filesystem) connected() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.server != nil
}

func fsConnected(fs *Filesystem) bool {
	return fsConnectedFunc(fs)
}

// nodeNotifyReady reports whether a kernel upcall may touch the node:
// non-nil, and not mid-attach (see attaching). A node under attach is
// getting fresh state from its lookup/create reply synchronously, so
// skipping its invalidation loses nothing and avoids racing go-fuse's
// inode initialization (data race plus nil-pointer panic). Check both
// before enqueueing and at fire time: a node can enter attach between
// the two.
func (s *Filesystem) nodeNotifyReady(n *storhubNode) bool {
	if n == nil {
		return false
	}
	if s == nil {
		// Detached seam (no filesystem): no attach lifecycle exists,
		// so there is nothing to race. Preserve the historical
		// behavior of driving such nodes (test seams rely on it).
		return true
	}
	s.attachMu.Lock()
	_, attaching := s.attaching[n]
	s.attachMu.Unlock()
	if attaching {
		return false
	}
	// Incarnation check: OnForget evicts the node from s.nodes, but an
	// already-queued async notify still holds the stale pointer and
	// would panic inside go-fuse (nil bridge) on firing. Every notify
	// source resolves through s.nodes, so a node absent (or superseded)
	// here has no live kernel inode to invalidate; skipping is sound.
	// A concurrent forget in the check-then-call micro-window stays
	// covered by the notifyAsync recover backstop. Locks are taken
	// sequentially (never nested) to keep the lock order acyclic.
	s.mu.RLock()
	cur := s.nodes[n.inode]
	s.mu.RUnlock()
	return cur == n
}

func safeNotifyContent(node *storhubNode) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logger := slog.Default()
			if node != nil {
				logger = node.fs.log()
			}
			logging.Error(logger, "panic in NotifyContent", "panic", r, "stack", string(buf[:n]))
		}
	}()
	if node == nil || !fsConnected(node.fs) {
		return
	}
	if !node.fs.nodeNotifyReady(node) {
		return
	}
	notifyContentFunc(node)
}

func safeNotifyContentAsync(node *storhubNode) {
	if node == nil || !fsConnected(node.fs) {
		return
	}
	contentFn := notifyContentFunc
	fs := node.fs
	if !fs.nodeNotifyReady(node) {
		return
	}
	fs.notifyAsync(notifyKey{kind: notifyKindContent, node: node}, "NotifyContent", func() {
		if fs.nodeNotifyReady(node) {
			contentFn(node)
		}
	})
}

// notifySlotWarnThreshold is the slot-wait duration past which a warn log
// fires (operator action: remount). No timeout or drop: dropped
// invalidations would turn into up to 60s of stale reads via entry and
// attr timeouts, so the block stays by design and only gains visibility.
const notifySlotWarnThreshold = 5 * time.Second

// NotifyStats reports parked-notify observability: currently parked,
// total slot waits observed, and coalesced duplicates folded.
func (s *Filesystem) NotifyStats() (parked int64, total uint64, coalesced uint64) {
	if s == nil {
		return 0, 0, 0
	}
	return s.notifyParked.Load(), s.notifyParkedTotal.Load(), s.notifyCoalesced.Load()
}

// NotifyParked reports how many notifies are currently parked on kernel
// backpressure.
func (s *Filesystem) NotifyParked() int64 {
	if s == nil {
		return 0
	}
	return s.notifyParked.Load()
}

// beginNotify marks a notification pending and takes a concurrency slot.
// It reports false when an identical notification is already queued: the
// duplicate coalesces into the pending one (post-commit invalidation
// storms collapse). Slot ownership: the slot is taken synchronously here,
// on the mutation path, so the mutator feels kernel backpressure instead
// of piling up unbounded notify goroutines; ownership passes to the
// spawned goroutine, which releases the slot in a defer. A wedged
// /dev/fuse therefore parks at most maxConcurrentNotifies slot-holders and
// the next mutation blocks in beginNotify. That block is by-design
// backpressure (no orphaned slot, no timeout/drain: timing out would lose
// invalidations the 60s entry/attr timeouts would then serve stale, and
// Close/Unmount cannot safely drain goroutines blocked in a kernel write).
// A nil filesystem (test seam driving detached nodes) skips both
// bookkeeping and the bound.
func (s *Filesystem) beginNotify(key notifyKey) bool {
	if s == nil {
		return true
	}
	s.notifyMu.Lock()
	if _, pending := s.notifyQueued[key]; pending {
		s.notifyMu.Unlock()
		s.notifyCoalesced.Add(1)
		return false
	}
	s.notifyQueued[key] = struct{}{}
	s.notifyMu.Unlock()
	// Blocking enqueue is deadlock-free by construction, even when the
	// caller holds the inode mu (commit emission runs after opMu release
	// but may still hold mu): slot holders (notify goroutines) only take
	// notifyMu and issue the kernel upcall, never the inode mu, so no
	// wait cycle exists. Full slots under a healthy kernel mean a commit
	// burst, absorbed as gentle backpressure with every invalidation
	// preserved; only a wedged kernel (unusable mount regardless) stalls
	// here, never normal operation.
	// Observability only: time the slot wait, count parked notifies, and
	// warn past the threshold. No timeout, no drop.
	start := time.Now()
	s.notifyParked.Add(1)
	s.notifyParkedTotal.Add(1)
	s.notifySlots <- struct{}{}
	parked := time.Since(start)
	s.notifyParked.Add(-1)
	if parked >= notifySlotWarnThreshold {
		logging.Warn(s.log(), "notify slot wait past threshold; kernel backpressure suspected, remount if persistent", "wait", parked.Round(time.Millisecond), "parked", s.notifyParked.Load())
	}
	return true
}

// endNotify clears the pending mark. It runs before the notification is
// issued, so a mutation landing during the kernel write still enqueues
// its own invalidation instead of being swallowed by the in-flight one.
func (s *Filesystem) endNotify(key notifyKey) {
	if s == nil {
		return
	}
	s.notifyMu.Lock()
	delete(s.notifyQueued, key)
	s.notifyMu.Unlock()
}

// notifyAsync dispatches fn on a fresh goroutine under the concurrency
// bound: the single funnel behind safeNotifyEntry/safeNotifyDelete, which
// previously duplicated the recover+slot dance. The pending mark clears
// before fn runs (see endNotify) so a mutation landing during the kernel
// write still enqueues its own invalidation.
func (s *Filesystem) notifyAsync(key notifyKey, what string, fn func()) {
	if !s.beginNotify(key) {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				logger := slog.Default()
				if s != nil {
					logger = s.log()
				}
				logging.Error(logger, "panic in "+what, "panic", r, "stack", string(buf[:n]))
			}
			if s != nil {
				<-s.notifySlots
			}
		}()
		s.endNotify(key)
		fn()
	}()
}

func safeNotifyEntry(node *storhubNode, name string) {
	if node == nil || !fsConnected(node.fs) {
		return
	}
	entryFn := notifyEntryFunc
	fs := node.fs
	if !fs.nodeNotifyReady(node) {
		return
	}
	fs.notifyAsync(notifyKey{kind: notifyKindEntry, node: node, name: name}, "NotifyEntry", func() {
		if fs.nodeNotifyReady(node) {
			entryFn(node, name)
		}
	})
}

func safeNotifyDelete(parent *storhubNode, name string, child *storhubNode) {
	if parent == nil || !fsConnected(parent.fs) {
		return
	}
	entryFn := notifyEntryFunc
	deleteFn := notifyDeleteFunc
	fs := parent.fs
	fs.notifyAsync(notifyKey{kind: notifyKindDelete, node: parent, name: name}, "NotifyDelete", func() {
		if !fs.nodeNotifyReady(parent) {
			return
		}
		if child == nil {
			entryFn(parent, name)
			return
		}
		if !fs.nodeNotifyReady(child) {
			return
		}
		deleteFn(parent, name, child)
	})
}

// invalidationPollInterval paces the cross-surface fan-out poll: an idle
// tick costs one cheap version read, and a tick that observes moves
// notifies exactly the changed entries. One second bounds cross-surface
// staleness near zero without measurable idle cost.
const invalidationPollInterval = time.Second

// publishedPathsSource is the hub capability behind fan-out: the
// namespace paths published after a cursor, or unknown scope when the
// window was lost. StorHub implements it (PublishedPathsSince); hubs
// without it (test doubles) leave the poller idle and timeout expiry
// keeps working unchanged.
type publishedPathsSource interface {
	PublishedPathsSince(project string, since uint64) (paths []string, unknown bool, current uint64)
}

// pollInvalidationsOnce consumes one fan-out window: paths changed since
// last are entry-notified (positive and negative dentries alike) plus
// content-notified where the path resolves to a tracked inode; unknown
// scope notifies every tracked path. It returns the new cursor.
// Deterministic and lock-free of test doubles: tests drive it directly,
// the background loop calls the same method.
func (s *Filesystem) pollInvalidationsOnce(last uint64) uint64 {
	src, ok := s.hub.(publishedPathsSource)
	if !ok {
		return last
	}
	paths, unknown, current := src.PublishedPathsSince(s.project, last)
	if unknown {
		s.invalidateAllTracked()
		return current
	}
	for _, p := range paths {
		s.invalidatePath(p)
	}
	return current
}

// invalidatePath notifies the kernel caches for one changed namespace
// path: the entry (which also clears a cached negative) plus content
// where the path currently resolves to a tracked inode. The root entry
// has no parent/name form, so only its content cache is touched.
func (s *Filesystem) invalidatePath(p string) {
	if p == "" {
		s.notifyKernelContentChanged(1)
		return
	}
	dir, base := path.Split(p)
	s.notifyEntryForPath(strings.TrimSuffix(dir, "/"), base)
	s.mu.RLock()
	ino, ok := s.pathToInode[p]
	s.mu.RUnlock()
	if ok {
		s.notifyKernelContentChanged(ino)
	}
}

// invalidateAllTracked notifies every tracked path: the unknown-scope
// fallback when the fan-out window was lost (ring overflow, remote-truth
// swap, rebase adopt). Rare by construction; O(cached) each time. Kernel
// negatives for never-seen names are not tracked anywhere, so they stay
// bounded by the entry timeout instead.
func (s *Filesystem) invalidateAllTracked() {
	s.mu.RLock()
	all := make([]string, 0, len(s.pathToInode))
	for p := range s.pathToInode {
		all = append(all, p)
	}
	s.mu.RUnlock()
	for _, p := range all {
		s.invalidatePath(p)
	}
}

// startInvalidationPoll runs the fan-out loop with its own cursor until
// the returned stop closes. One parked goroutine per mount; each idle
// tick is a single cheap version read.
func (s *Filesystem) startInvalidationPoll() (stop func()) {
	stopCh := make(chan struct{})
	var once sync.Once
	var last uint64
	go func() {
		ticker := time.NewTicker(invalidationPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				last = s.pollInvalidationsOnce(last)
			}
		}
	}()
	return func() { once.Do(func() { close(stopCh) }) }
}
