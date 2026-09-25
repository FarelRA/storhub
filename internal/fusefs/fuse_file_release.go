package fusefs

import (
	"context"
	"github.com/FarelRA/storhub/internal/logging"
	"github.com/hanwen/go-fuse/v2/fuse"
	"os"
	"syscall"
	"time"
)

func (h *storhubHandle) Release(ctx context.Context) syscall.Errno {
	started := time.Now()
	releasePath := h.handlePath()
	if h.fs.debugEnabled() {
		h.fs.debugOp("release start", "path", releasePath, "inode", h.inode)
	}
	errno := h.commit(ctx)
	h.releaseTrackedLocks()
	if errno != 0 {
		// The commit failed; the overlay temps hold the only copy of data
		// the application already wrote (Flush commits and propagates the
		// errno, but a successful close(2) may still have been reported
		// for earlier fsync-less writes). Preserve the overlay for manual
		// recovery instead of deleting it.
		h.quarantineHandleTemp()
		// Load-then-use: commit above already snapshotted the same
		// pointer, and a concurrent op may hold it too; the state
		// outlives the handle via refs and the registry.
		if writeState := h.loadWriteState(); writeState != nil && h.fs.soleWriteStateRef(writeState) {
			writeState.quarantineWriteTemp()
		}
	} else if drainErrno := h.flushAndDrain(ctx); drainErrno != 0 {
		// The commit published but the drain did not confirm remote
		// durability: return EIO WITHOUT quarantining the overlay. The
		// bytes are uploaded and published and the journal retains the
		// dirty state for retry; quarantining here would double-replay
		// the same bytes via redrive plus the quarantined overlay.
		// Cleanup follows the success path (close, do not preserve).
		h.closeHandleTemp()
		errno = drainErrno
	} else {
		h.closeHandleTemp()
	}
	h.fs.mu.Lock()
	delete(h.fs.handles, h.id)
	noHandlesLeft := !h.fs.hasOpenHandleForLocked(h.inode)
	h.fs.mu.Unlock()
	if noHandlesLeft {
		// POSIX: all of a process's locks on a file are gone once it has
		// no open descriptor left. The kernel enforces this itself for the
		// owning process; mirroring it here prevents locks from other
		// (crashed or sloppy) owners from lingering on the inode forever.
		// go-fuse does not surface FUSE_RELEASE's LockOwner (fs API v2.11),
		// so per-fd close semantics cannot be mirrored exactly - last
		// close is the guarantee that matters in practice.
		h.fs.dropAllLocksForInode(h.inode)
	}
	// Nil the pointer under h.mu so in-flight Read/Write/Allocate/Lseek
	// snapshots either observe the state (safe: it outlives the handle)
	// or a clean nil, never a torn read.
	h.mu.Lock()
	writeState := h.writeState
	h.writeState = nil
	h.mu.Unlock()
	if writeState != nil {
		h.fs.releaseWriteState(writeState)
	}
	if errno != 0 {
		h.fs.errorOp("release failed", "path", releasePath, "inode", h.inode, "elapsed", time.Since(started), "err", errno)
		return errno
	}
	if h.fs.debugEnabled() {
		h.fs.debugOp("release complete", "path", releasePath, "inode", h.inode, "elapsed", time.Since(started))
	}
	return errno
}

// quarantineTemps moves this handle's temp snapshot into the recovery
// directory instead of deleting it. Used on commit failure.
func (h *storhubHandle) quarantineHandleTemp() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	temp := h.temp
	tempPath := h.tempPath
	targetPath := h.path
	h.temp = nil
	h.tempPath = ""
	h.mu.Unlock()
	if temp != nil {
		_ = temp.Close()
	}
	if tempPath != "" {
		// Handle temps have unknown provenance at this point (no dirty
		// span tracking like writeState): record zero intent so the entry
		// stays manual-recovery-only, never auto-redriven.
		h.fs.quarantineFile(tempPath, targetPath, quarantineReasonCommitFailure, quarantineIntent{})
	}
}

func (h *storhubHandle) closeHandleTemp() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	if h.temp != nil {
		if err := h.temp.Close(); err != nil {
			logging.Error(h.fs.log(), "failed to close handle temp file", "err", err)
		}
	}
	if h.tempPath != "" {
		if err := os.Remove(h.tempPath); err != nil {
			logging.Error(h.fs.log(), "failed to remove handle temp file", "path", h.tempPath, "err", err)
		}
	}
}

func (h *storhubHandle) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	_ = ctx
	_ = flags
	h.fs.mu.RLock()
	defer h.fs.mu.RUnlock()
	locks := h.fs.lockTable[h.inode]
	for _, existing := range locks {
		if lockConflicts(existing, owner, *lk) {
			*out = existing.lock
			return 0
		}
	}
	out.Typ = syscall.F_UNLCK
	return 0
}
