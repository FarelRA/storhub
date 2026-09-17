package fusefs

import (
	"log/slog"
	"runtime"

	"github.com/FarelRA/storhub/internal/logging"
)

var (
	notifyEntryFunc  = func(node *storhubNode, name string) { _ = node.NotifyEntry(name) }
	notifyDeleteFunc = func(parent *storhubNode, name string, child *storhubNode) {
		_ = parent.NotifyDelete(name, child.EmbeddedInode())
	}
)

// notifyKey identifies one pending entry/delete notification.
type notifyKey struct {
	kind uint8 // notifyKindEntry or notifyKindDelete
	node *storhubNode
	name string
}

const (
	notifyKindEntry uint8 = iota
	notifyKindDelete
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
	s.invalCount.Add(1)
	s.mu.Lock()
	node := s.nodes[inode]
	s.mu.Unlock()
	safeNotifyContent(node)
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
	_ = node.NotifyContent(0, 0)
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
		return false
	}
	s.notifyQueued[key] = struct{}{}
	s.notifyMu.Unlock()
	s.notifySlots <- struct{}{}
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
	fs.notifyAsync(notifyKey{kind: notifyKindEntry, node: node, name: name}, "NotifyEntry", func() {
		entryFn(node, name)
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
		if child == nil {
			entryFn(parent, name)
			return
		}
		deleteFn(parent, name, child)
	})
}
