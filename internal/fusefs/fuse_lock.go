package fusefs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type lockRecord struct {
	owner uint64
	lock  fuse.FileLock
}

// dropAllLocksForInode removes every lock record for the inode and wakes
// blocked waiters. Called when the last open handle disappears.
func (s *Filesystem) dropAllLocksForInode(inode uint64) {
	s.lockCond.L.Lock()
	_, existed := s.lockTable[inode]
	delete(s.lockTable, inode)
	if existed {
		// A released lock may unblock F_SETLKW waiters.
		s.lockCond.Broadcast()
	}
	s.lockCond.L.Unlock()
}

func (h *storhubHandle) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	_ = ctx
	_ = flags
	errno := h.fs.setLock(h.inode, owner, *lk)
	if errno == 0 {
		h.trackLockOwner(owner, lk.Typ)
	}
	return errno
}

// Setlkw blocks until the lock can be granted. Waiters sleep on the
// filesystem's lock condition variable and are woken by any lock-table
// change (or context cancellation) instead of polling.
func (h *storhubHandle) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	// flags carries FUSE_LK intent knobs beyond shared/exclusive type.
	// Blocking semantics here are driven by lk.Type alone; honoring the
	// remaining bits stays deferred until go-fuse round-trips them (see
	// Getlk/Setlk, same discard).
	_ = flags
	s := h.fs
	if ctx != nil && ctx.Done() != nil {
		// The cancellation broadcast must take the cond's locker.
		// A bare Broadcast can fire in the window between our ctx.Err()
		// check and Wait() - the wake-up would be lost and the waiter
		// would sleep until an unrelated lock event. Holding s.mu in the
		// handler serializes it against the check-then-Wait sequence
		// (Wait atomically releases s.mu, so the handler only runs while
		// we are either checking or already queued).
		//
		// Capturing the stop-func is mandatory, not hygiene: go-fuse's
		// per-request context is closed only on INTERRUPT or mount death,
		// never on normal completion, so an AfterFunc registered against
		// it would otherwise park a goroutine - and pin this Filesystem
		// through the closure, even after unmount - for every successful
		// blocking lock. The wake-up is only needed while parked in the
		// lockCond.Wait() loop below; deregister on exit.
		stop := context.AfterFunc(ctx, func() {
			s.lockCond.L.Lock()
			s.lockCond.Broadcast()
			s.lockCond.L.Unlock()
		})
		defer stop()
	}
	s.lockCond.L.Lock()
	for {
		errno := s.setLockLocked(h.inode, owner, *lk)
		if errno == 0 {
			// Release s.mu BEFORE taking handle.mu: Release's
			// releaseTrackedLocks takes them in the opposite order
			// (handle.mu, then s.mu), so nesting them here is an
			// ABBA deadlock.
			s.lockCond.L.Unlock()
			h.trackLockOwner(owner, lk.Typ)
			return 0
		}
		if err := ctx.Err(); err != nil {
			s.lockCond.L.Unlock()
			return errnoFromError(err)
		}
		s.lockCond.Wait()
	}
}

func (s *Filesystem) setLock(inode, owner uint64, lk fuse.FileLock) syscall.Errno {
	s.lockCond.L.Lock()
	defer s.lockCond.L.Unlock()
	errno := s.setLockLocked(inode, owner, lk)
	if errno == 0 {
		// A release may unblock F_SETLKW waiters.
		s.lockCond.Broadcast()
	}
	return errno
}

// setLockLocked applies a lock operation; callers must hold lockCond's
// locker (s.mu).
func (s *Filesystem) setLockLocked(inode, owner uint64, lk fuse.FileLock) syscall.Errno {
	locks := s.lockTable[inode]
	if lk.Typ == syscall.F_UNLCK {
		filtered := locks[:0]
		for _, existing := range locks {
			if existing.owner != owner {
				filtered = append(filtered, existing)
				continue
			}
			for _, segment := range subtractLock(existing.lock, lk) {
				filtered = append(filtered, lockRecord{owner: existing.owner, lock: segment})
			}
		}
		s.lockTable[inode] = filtered
		return 0
	}
	for _, existing := range locks {
		if lockConflicts(existing, owner, lk) {
			return syscall.EAGAIN
		}
	}
	filtered := locks[:0]
	for _, existing := range locks {
		if existing.owner == owner {
			for _, segment := range subtractLock(existing.lock, lk) {
				filtered = append(filtered, lockRecord{owner: existing.owner, lock: segment})
			}
			continue
		}
		filtered = append(filtered, existing)
	}
	s.lockTable[inode] = append(filtered, lockRecord{owner: owner, lock: lk})
	return 0
}

func lockConflicts(existing lockRecord, owner uint64, requested fuse.FileLock) bool {
	if requested.Typ == syscall.F_UNLCK || existing.owner == owner || !locksOverlap(existing.lock, requested) {
		return false
	}
	if existing.lock.Typ == syscall.F_RDLCK && requested.Typ == syscall.F_RDLCK {
		return false
	}
	return true
}

func (h *storhubHandle) trackLockOwner(owner uint64, typ uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if typ == syscall.F_UNLCK {
		delete(h.owners, owner)
		return
	}
	h.owners[owner] = struct{}{}
}

func (h *storhubHandle) releaseTrackedLocks() {
	h.mu.Lock()
	owners := make([]uint64, 0, len(h.owners))
	for owner := range h.owners {
		owners = append(owners, owner)
	}
	h.mu.Unlock()
	for _, owner := range owners {
		// Best-effort unlock during handle teardown: POSIX locks vanish
		// with the process anyway, so there is nobody left to report to.
		_ = h.fs.setLock(h.inode, owner, fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_UNLCK})
	}
}

// locksOverlap treats FileLock.End as EXCLUSIVE, matching what the kernel
// sends over FUSE (end = start + len): adjacent ranges [0,10) and [10,20)
// do not overlap. An End of 0 is the unlock-whole-file convention
// and is treated as infinity.
func locksOverlap(a, b fuse.FileLock) bool {
	aEnd := a.End
	bEnd := b.End
	if aEnd == 0 {
		aEnd = ^uint64(0)
	}
	if bEnd == 0 {
		bEnd = ^uint64(0)
	}
	return a.Start < bEnd && b.Start < aEnd
}

func subtractLock(existing, cut fuse.FileLock) []fuse.FileLock {
	if !locksOverlap(existing, cut) {
		return []fuse.FileLock{existing}
	}
	existingEnd := existing.End
	cutEnd := cut.End
	if existingEnd == 0 {
		existingEnd = ^uint64(0)
	}
	if cutEnd == 0 {
		cutEnd = ^uint64(0)
	}
	segments := make([]fuse.FileLock, 0, 2)
	if cut.Start > existing.Start {
		left := existing
		// Exclusive end: the retained left segment stops at the cut's
		// first byte, not one before it (the old inclusive math
		// dropped a byte from the retained lock on partial unlock).
		left.End = cut.Start
		segments = append(segments, left)
	}
	if cutEnd < existingEnd {
		right := existing
		right.Start = cutEnd
		if existing.End == 0 {
			right.End = 0
		} else {
			right.End = existing.End
		}
		segments = append(segments, right)
	}
	return segments
}
