package fusefs

import (
	"context"
	"math"
	"syscall"
)

// fallocate mode flags from linux/falloc.h, kept here because the only
// consumers are the overlay handlers below.
const (
	fallocFlKeepSize      = 0x01
	fallocFlPunchHole     = 0x02
	fallocFlNoHideStale   = 0x04
	fallocFlCollapseRange = 0x08
	fallocFlZeroRange     = 0x10
	fallocFlInsertRange   = 0x20
	fallocFlUnshare       = 0x40
)

// Allocate implements fs.FileAllocater: posix_fallocate-style space
// reservation on the open handle. Only plain allocation and
// FALLOC_FL_KEEP_SIZE map cleanly onto the overlay temp file - blocks
// are reserved locally so disk exhaustion surfaces at allocation time
// instead of mid-write. Hole punching and range collapsing would
// rewrite chunk layout semantics the overlay cannot express, so they
// return EOPNOTSUPP rather than pretending. A read-only handle has
// nothing to allocate against and returns EBADF, matching POSIX.
func (h *storhubHandle) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	_ = ctx
	// Load-then-use: the pointer is nilled under h.mu by Release, so
	// snapshot it first and use only the local below.
	writeState := h.snapshotWriteState()
	if writeState == nil {
		return syscall.EBADF
	}
	if off > math.MaxInt64 || size > math.MaxInt64 {
		return syscall.EINVAL
	}
	start := int64(off)
	length := int64(size)
	if start+length < start {
		return syscall.EINVAL
	}
	switch {
	case mode&^(fallocFlKeepSize|fallocFlPunchHole|fallocFlNoHideStale|fallocFlCollapseRange|fallocFlZeroRange|fallocFlInsertRange|fallocFlUnshare) != 0:
		return syscall.EINVAL
	case mode&(fallocFlPunchHole|fallocFlCollapseRange|fallocFlInsertRange|fallocFlUnshare|fallocFlZeroRange) != 0:
		if mode&fallocFlPunchHole != 0 && mode&fallocFlKeepSize == 0 {
			return syscall.EINVAL
		}
		return syscall.EOPNOTSUPP
	}
	writeState.opMu.Lock()
	defer unlockOpMu(&writeState.opMu)
	writeState.mu.Lock()
	defer writeState.mu.Unlock()
	if writeState.poisoned {
		return syscall.EIO
	}
	if err := writeState.ensureTempLocked(); err != nil {
		return errnoFromError(err)
	}
	if err := reserveSpace(writeState.temp, mode, start, length); err != nil {
		return errnoFromError(err)
	}
	end := start + length
	if mode&fallocFlKeepSize == 0 && end > writeState.logicalSize {
		writeState.markDirtyLocked(writeState.logicalSize, end)
		writeState.logicalSize = end
	}
	if h.fs.debugEnabled() {
		h.fs.debugOp("fallocate", "path", h.handlePath(), "inode", h.inode, "off", start, "size", length, "mode", mode)
	}
	return 0
}

// retrieveKernelCache has been removed.
// The kernel guarantees it sends FUSE_WRITE for dirty pages (including mmap)
// before FUSE_RELEASE via filemap_write_and_wait_range in fuse_flush().
// The opMu lock in commit() ensures all FUSE_WRITE handlers complete before
// the commit runs, so dirtyRanges is always populated correctly.

// Flush pushes dirty overlay data before the kernel releases the fd.
// The mount enables writeback caching, so close(2) may be the only
// durability signal the kernel sends for buffered writes: a no-op Flush
// would acknowledge data the next crash loses. Commit is idempotent, so
// the later Release commit is a no-op when Flush already pushed. The
// drain after a successful commit waits for remote durability; a drain
// failure returns EIO without quarantining (see drainProject).
func (h *storhubHandle) Flush(ctx context.Context) syscall.Errno {
	return h.commitFlushDrain(ctx)
}

func (h *storhubHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	_ = flags
	if h.fs.debugEnabled() {
		h.fs.debugOp("fsync", "path", h.handlePath(), "inode", h.inode)
	}
	return h.commitFlushDrain(ctx)
}

// journalFlusher is the optional hub capability behind the explicit
// flush: a hub that journals acknowledged mutations exposes
// FlushJournals so the sync path fsyncs the journal between commit and
// drain. Hubs without it (test doubles) skip the flush; StorHub
// durability on those paths still flows through DrainProjectContext,
// which flushes internally.
type journalFlusher interface {
	FlushJournals()
}

// flushHubJournals fsyncs the hub's op journals when the hub exposes
// them (see journalFlusher). Ordering point, never an error path: the
// drain right after re-asserts remote durability.
func (h *storhubHandle) flushHubJournals() {
	if fj, ok := h.fs.hub.(journalFlusher); ok {
		fj.FlushJournals()
	}
}

// flushAndDrain fsyncs the op journal, then waits for remote durability
// (see drainProject). The flush sits between commit and drain on every
// durability path: a crash after the journal fsync but before the remote
// push replays from the journal.
func (h *storhubHandle) flushAndDrain(ctx context.Context) syscall.Errno {
	h.flushHubJournals()
	return h.drainProject(ctx)
}

// commitFlushDrain is commitAndDrain with the F5 explicit flush in the
// middle (commit, journal fsync, drain). Same contract: a commit failure
// leaves the overlay dirty for the caller's quarantine decision; a drain
// failure is EIO with the overlay already published, so the caller must
// not quarantine.
func (h *storhubHandle) commitFlushDrain(ctx context.Context) syscall.Errno {
	if errno := h.commit(ctx); errno != 0 {
		return errno
	}
	return h.flushAndDrain(ctx)
}
