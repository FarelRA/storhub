package fusefs

import (
	"context"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func (n *storhubNode) Setattr(ctx context.Context, f gofusefs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		// Pathless node (unlinked while open): an fh-carried setattr
		// on this node's own detached inode stages into the overlay
		// (or is accepted-and-discarded for read-only handles) instead
		// of failing. The kernel pushes cached mtime here on close
		// under writeback caching and fails close with EIO when the
		// flush does not succeed. Pinned by the conformance
		// unlink-while-open scenario.
		return n.setattrDetached(ctx, f, in, out, stale)
	}
	state, errno := n.setattrOverlayState(ctx, f)
	if errno != 0 {
		return errno
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("setattr start", "path", targetPath, "inode", n.inode, "valid", in.Valid)
	}
	usedLocalSize, localSize, errno := n.setattrSize(ctx, targetPath, in, state)
	if errno != 0 {
		n.fs.errorOp("setattr failed", "path", targetPath, "inode", n.inode, "step", "size", "err", errno)
		return errno
	}
	if errno := n.setattrMode(ctx, targetPath, in, state); errno != 0 {
		n.fs.errorOp("setattr failed", "path", targetPath, "inode", n.inode, "step", "mode", "err", errno)
		return errno
	}
	if errno := n.setattrOwner(ctx, targetPath, in, state); errno != 0 {
		n.fs.errorOp("setattr failed", "path", targetPath, "inode", n.inode, "step", "owner", "err", errno)
		return errno
	}
	if errno := n.setattrTimes(ctx, targetPath, in, state); errno != 0 {
		n.fs.errorOp("setattr failed", "path", targetPath, "inode", n.inode, "step", "times", "err", errno)
		return errno
	}
	return n.finishSetattr(ctx, targetPath, state, usedLocalSize, localSize, out, in.Valid)
}

// handleBase returns one handle's stat base for a pathless inode: its
// write-state snapshot when present (plus the state to stage into),
// else its open-time pin (with no state to stage into). A nil state
// means accept-and-discard for setattr; the reply still serves.
func (n *storhubNode) handleBase(handle *storhubHandle) (shfs.EntryInfo, *inodeWriteState, bool) {
	// Load-then-use under h.mu: Release nils the pointer concurrently,
	// and the state outlives the handle via refs and the registry.
	if state := handle.snapshotWriteState(); state != nil && state.inode == n.inode {
		if base, ok := state.cachedBaseEntry(); ok {
			return base, state, true
		}
	}
	if pin := handle.pinned; pin != nil {
		return *shfs.EntryFromFile(&pin.file, "", 0), nil, true
	}
	return shfs.EntryInfo{}, nil, false
}

// detachedBase resolves the stat base for a pathless (unlinked) inode.
// It prefers the calling handle's own view, then falls back to any
// surviving handle of the inode: utimensat-family syscalls arrive
// handleless (notify_change carries no file), and the issuing fd is
// necessarily one of the survivors. A handle still mid-open (registered
// but without pin or write state yet) simply misses and lets a complete
// one hit: at least one complete handle exists whenever any fd
// references the inode, because a usable fd implies its Open returned.
func (n *storhubNode) detachedBase(f gofusefs.FileHandle) (shfs.EntryInfo, *inodeWriteState, bool) {
	if handle, ok := f.(*storhubHandle); ok && handle.inode == n.inode {
		if base, state, hit := n.handleBase(handle); hit {
			return base, state, true
		}
	}
	n.fs.mu.RLock()
	survivors := make([]*storhubHandle, 0, len(n.fs.handles))
	for _, handle := range n.fs.handles {
		if handle.inode == n.inode {
			survivors = append(survivors, handle)
		}
	}
	n.fs.mu.RUnlock()
	for _, handle := range survivors {
		if base, state, hit := n.handleBase(handle); hit {
			return base, state, true
		}
	}
	return shfs.EntryInfo{}, nil, false
}

// detachedReply fills the attr reply for a pathless inode from its base
// plus the live overlay. It carries no path and zero links, which is the
// truth for an unlinked inode.
func (n *storhubNode) detachedReply(base shfs.EntryInfo, state *inodeWriteState, out *fuse.AttrOut) {
	entry := base
	if state != nil {
		state.mu.Lock()
		state.overlayEntryLocked(&entry)
		state.mu.Unlock()
	} else if shared := n.fs.writeStateForInode(n.inode); shared != nil {
		shared.mu.Lock()
		shared.overlayEntryLocked(&entry)
		shared.mu.Unlock()
	}
	entry.Path = ""
	entry.NLink = 0
	fillAttr(&out.Attr, &entry)
	out.SetTimeout(n.fs.opts.AttrTimeout)
}

// setattrDetached applies a setattr to an unlinked-but-open inode. With a
// write state the change stages into the overlay exactly like the linked
// path (same verb DAC against the snapshot, same pending patch); the
// commit discards it at Release, which is the POSIX fate of writes to
// unlinked files. Without one (read-only survivor) mode/owner/times are
// accepted and discarded, unobservable past close by definition, while
// a size change is EINVAL (ftruncate needs a writable fd).
//
// Identity note: the opener-match check of the linked path is
// deliberately absent. The kernel routes an fh only to the process that
// opened it, and a handleless call can only originate from an open fd
// (no path exists to name), so fd ownership is the auth; the verb DAC
// below is still enforced, and any staged change dies at Release.
// Anything else (directories, no surviving view) keeps ESTALE.
func (n *storhubNode) setattrDetached(ctx context.Context, f gofusefs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut, stale syscall.Errno) syscall.Errno {
	if n.isDir {
		return stale
	}
	base, state, ok := n.detachedBase(f)
	if !ok {
		return stale
	}
	if size, ok := in.GetSize(); ok {
		if state == nil {
			return syscall.EINVAL
		}
		state.opMu.Lock()
		state.mu.Lock()
		if state.poisoned {
			state.mu.Unlock()
			n.fs.unlockOpMu(&state.opMu)
			return syscall.EIO
		}
		err := state.setSizeLocked(int64(size))
		if err == nil {
			// state.path is "" here; the detached branch of the
			// stager is a safe no-op instead of a wrong-entry stat.
			n.fs.stagePrivClearForDataWrite(ctx, state, state.path)
		}
		state.mu.Unlock()
		n.fs.unlockOpMu(&state.opMu)
		if err != nil {
			return errnoFromError(err)
		}
	}
	if mode, ok := in.GetMode(); ok {
		dacEntry := base
		if shfs.IdentityPresent(ctx) {
			if err := shfs.CanChmod(ctx, &dacEntry); err != nil {
				return errnoFromError(err)
			}
		}
		if state != nil {
			state.opMu.Lock()
			state.mu.Lock()
			if state.poisoned {
				state.mu.Unlock()
				n.fs.unlockOpMu(&state.opMu)
				return syscall.EIO
			}
			state.pending.HasMode = true
			state.pending.Mode = mode & 0o7777
			state.mu.Unlock()
			n.fs.unlockOpMu(&state.opMu)
		}
	}
	uid, uidOK := in.GetUID()
	gid, gidOK := in.GetGID()
	if uidOK || gidOK {
		dacEntry := base
		if shfs.IdentityPresent(ctx) {
			if err := shfs.CanChown(ctx, &dacEntry, uid, gid); err != nil {
				return errnoFromError(err)
			}
		}
		if state != nil {
			state.opMu.Lock()
			state.mu.Lock()
			if state.poisoned {
				state.mu.Unlock()
				n.fs.unlockOpMu(&state.opMu)
				return syscall.EIO
			}
			overlayBase := base
			state.overlayEntryLocked(&overlayBase)
			if !uidOK {
				uid = overlayBase.UID
			}
			if !gidOK {
				gid = overlayBase.GID
			}
			state.pending.HasOwner = true
			state.pending.UID = uid
			state.pending.GID = gid
			// Same chown privilege clearing as the linked path: a
			// non-admin chown stages the cleared mode at once so the
			// detached fstat reply below observes it. overlayBase
			// already carries the effective overlay mode.
			stagePrivClearLocked(ctx, state, overlayBase.Mode)
			state.mu.Unlock()
			n.fs.unlockOpMu(&state.opMu)
		}
	}
	atime, atimeOK := in.GetATime()
	mtime, mtimeOK := in.GetMTime()
	if atimeOK || mtimeOK {
		dacEntry := base
		if shfs.IdentityPresent(ctx) {
			if err := shfs.CanSetTimes(ctx, &dacEntry); err != nil {
				return errnoFromError(err)
			}
		}
		if state != nil {
			state.opMu.Lock()
			state.mu.Lock()
			if state.poisoned {
				state.mu.Unlock()
				n.fs.unlockOpMu(&state.opMu)
				return syscall.EIO
			}
			overlayBase := base
			state.overlayEntryLocked(&overlayBase)
			if !atimeOK {
				atime = time.Unix(0, overlayBase.AccessedAt)
			}
			if !mtimeOK {
				mtime = time.Unix(0, overlayBase.ModifiedAt)
			}
			state.pending.HasTimes = true
			state.pending.ATime = atime
			state.pending.MTime = mtime
			state.mu.Unlock()
			n.fs.unlockOpMu(&state.opMu)
		}
	}
	n.detachedReply(base, state, out)
	n.fs.notifyKernelContentChanged(n.inode)
	if n.fs.debugEnabled() {
		n.fs.debugOp("setattr", "inode", n.inode, "valid", in.Valid, "detached", true)
	}
	return 0
}

// The overlay is honored only for a caller who may drive it. A
// handle attached to the write state is the common case; a path-based
// setattr (no fh) still reaches the overlay when the caller is the
// single writer who owns that inode's active state (a legitimate
// ftruncate on an open file arrives handleless from some clients). A
// non-owner's handleless truncate falls through to the hub verbs, which
// enforce DAC against the *requesting* caller, so a stranger's
// truncate/chmod is never deferred into the owner's next commit.
// A nil state means "go to the hub verbs".
// setattrOverlayState resolves the write state this setattr may drive.
func (n *storhubNode) setattrOverlayState(ctx context.Context, f gofusefs.FileHandle) (*inodeWriteState, syscall.Errno) {
	// Load-then-use under h.mu: Release nils the pointer concurrently.
	if handle, ok := f.(*storhubHandle); ok {
		if ws := handle.snapshotWriteState(); ws != nil {
			if errno := handle.checkOverlayCaller(ctx); errno != 0 {
				return nil, errno
			}
			return ws, 0
		}
	}
	if st := n.fs.writeStateForInode(n.inode); st != nil && n.fs.callerMayDriveOverlay(ctx, st) {
		return st, 0
	}
	return nil, 0
}

// setattrSize applies a size change: into the overlay when one is drivable,
// else via the hub truncate verb. It reports whether the reply must use the
// overlay's local size instead of the post-commit hub stat.
func (n *storhubNode) setattrSize(ctx context.Context, targetPath string, in *fuse.SetAttrIn, state *inodeWriteState) (bool, int64, syscall.Errno) {
	size, ok := in.GetSize()
	if !ok || n.isDir {
		return false, 0, 0
	}
	if state != nil {
		state.opMu.Lock()
		state.mu.Lock()
		if state.poisoned {
			state.mu.Unlock()
			n.fs.unlockOpMu(&state.opMu)
			return false, 0, syscall.EIO
		}
		err := state.setSizeLocked(int64(size))
		var localSize int64
		if err == nil {
			localSize = state.logicalSize
			// An overlay ftruncate is a data write for privilege
			// purposes: stage the cleared mode immediately so
			// pre-commit stat and exec observe it. ctx already
			// carries the caller identity (bound in Setattr).
			n.fs.stagePrivClearForDataWrite(ctx, state, targetPath)
		}
		state.mu.Unlock()
		n.fs.unlockOpMu(&state.opMu)
		if err != nil {
			return false, 0, errnoFromError(err)
		}
		return true, localSize, 0
	}
	if _, err := n.fs.hub.TruncateFileContext(ctx, n.fs.project, targetPath, int64(size)); err != nil {
		return false, 0, errnoFromError(err)
	}
	return false, 0, 0
}

// setattrMode applies a mode change: staged into the overlay's pending
// patch (with the ownership DAC at call time) or via the hub chmod verb.
func (n *storhubNode) setattrMode(ctx context.Context, targetPath string, in *fuse.SetAttrIn, state *inodeWriteState) syscall.Errno {
	mode, ok := in.GetMode()
	if !ok {
		return 0
	}
	if state != nil && !n.isDir {
		entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
		if err != nil {
			return errnoFromError(err)
		}
		// fchmod still needs the ownership DAC at call
		// time, not just at open.
		if shfs.IdentityPresent(ctx) {
			if err := shfs.CanChmod(ctx, entry); err != nil {
				return errnoFromError(err)
			}
		}
		state.opMu.Lock()
		state.mu.Lock()
		if state.poisoned {
			state.mu.Unlock()
			n.fs.unlockOpMu(&state.opMu)
			return syscall.EIO
		}
		state.pending.HasMode = true
		state.pending.Mode = mode & 0o7777
		state.mu.Unlock()
		n.fs.unlockOpMu(&state.opMu)
		return 0
	}
	if err := n.fs.hub.ChmodContext(ctx, n.fs.project, targetPath, mode&0o7777); err != nil {
		return errnoFromError(err)
	}
	return 0
}

// setattrOwner applies a uid/gid change: staged into the overlay's pending
// patch (with the hub's chown DAC now, under the caller's identity) or via
// the hub chown verb.
func (n *storhubNode) setattrOwner(ctx context.Context, targetPath string, in *fuse.SetAttrIn, state *inodeWriteState) syscall.Errno {
	uid, uidOK := in.GetUID()
	gid, gidOK := in.GetGID()
	if !uidOK && !gidOK {
		return 0
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return errnoFromError(err)
	}
	if state != nil && !n.isDir {
		// Chown via the overlay must satisfy the hub's chown
		// DAC now, under the caller's identity - not silently at the
		// state owner's next flush.
		if shfs.IdentityPresent(ctx) {
			if err := shfs.CanChown(ctx, entry, uid, gid); err != nil {
				return errnoFromError(err)
			}
		}
		state.opMu.Lock()
		state.mu.Lock()
		if state.poisoned {
			state.mu.Unlock()
			n.fs.unlockOpMu(&state.opMu)
			return syscall.EIO
		}
		state.overlayEntryLocked(entry)
		if !uidOK {
			uid = entry.UID
		}
		if !gidOK {
			gid = entry.GID
		}
		state.pending.HasOwner = true
		state.pending.UID = uid
		state.pending.GID = gid
		// Chown clears setuid/setgid for non-admin callers (POSIX
		// file_remove_privs): stage the cleared mode immediately so
		// pre-commit fstat observes it instead of the stale bits,
		// mirroring the data-write path. entry already carries the
		// effective overlay mode via the overlay above.
		stagePrivClearLocked(ctx, state, entry.Mode)
		state.mu.Unlock()
		n.fs.unlockOpMu(&state.opMu)
		return 0
	}
	if !uidOK {
		uid = entry.UID
	}
	if !gidOK {
		gid = entry.GID
	}
	if err := n.fs.hub.ChownContext(ctx, n.fs.project, targetPath, uid, gid); err != nil {
		return errnoFromError(err)
	}
	return 0
}

// setattrTimes applies an atime/mtime change: staged into the overlay's
// pending patch or via the hub chtimes verb. go-fuse already resolves
// UTIME_NOW to time.Now(); ok=false is UTIME_OMIT. Passing pointers
// preserves explicit epoch timestamps that ChtimesContext's omit-on-zero
// would rewrite.
func (n *storhubNode) setattrTimes(ctx context.Context, targetPath string, in *fuse.SetAttrIn, state *inodeWriteState) syscall.Errno {
	atime, atimeOK := in.GetATime()
	mtime, mtimeOK := in.GetMTime()
	if !atimeOK && !mtimeOK {
		return 0
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return errnoFromError(err)
	}
	if !atimeOK {
		atime = time.Unix(0, entry.AccessedAt)
	}
	if !mtimeOK {
		mtime = time.Unix(0, entry.ModifiedAt)
	}
	if state != nil && !n.isDir {
		if shfs.IdentityPresent(ctx) {
			if err := shfs.CanSetTimes(ctx, entry); err != nil {
				return errnoFromError(err)
			}
		}
		state.opMu.Lock()
		state.mu.Lock()
		if state.poisoned {
			state.mu.Unlock()
			n.fs.unlockOpMu(&state.opMu)
			return syscall.EIO
		}
		state.overlayEntryLocked(entry)
		if !atimeOK {
			atime = time.Unix(0, entry.AccessedAt)
		}
		if !mtimeOK {
			mtime = time.Unix(0, entry.ModifiedAt)
		}
		state.pending.HasTimes = true
		state.pending.ATime = atime
		state.pending.MTime = mtime
		state.mu.Unlock()
		n.fs.unlockOpMu(&state.opMu)
		return 0
	}
	var atimePtr, mtimePtr *time.Time
	if atimeOK {
		t := atime
		atimePtr = &t
	}
	if mtimeOK {
		t := mtime
		mtimePtr = &t
	}
	if err := n.fs.hub.ChtimesExplicitContext(ctx, n.fs.project, targetPath, atimePtr, mtimePtr); err != nil {
		return errnoFromError(err)
	}
	return 0
}

// finishSetattr stats the result (overlaying the local size when the size
// came from the overlay), refreshes the overlay's cached entry, and
// invalidates the kernel copies. It is the single stat+overlay+notify tail
// for every setattr path above.
func (n *storhubNode) finishSetattr(ctx context.Context, targetPath string, state *inodeWriteState, usedLocalSize bool, localSize int64, out *fuse.AttrOut, valid uint32) syscall.Errno {
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		n.fs.errorOp("setattr failed", "path", targetPath, "inode", n.inode, "step", "finish", "err", errnoFromError(err))
		return errnoFromError(err)
	}
	if usedLocalSize {
		entry.Size = localSize
	} else {
		n.fs.applyPendingSize(entry)
	}
	if state != nil && !n.isDir {
		state.mu.Lock()
		state.overlayEntryLocked(entry)
		state.mu.Unlock()
	}
	fillAttr(&out.Attr, entry)
	out.SetTimeout(n.fs.opts.AttrTimeout)
	// The attr response updates this handle's cache line, but other cached
	// copies (other nodes, readdir-plus) expire only via invalidation.
	n.fs.notifyKernelContentChanged(n.inode)
	if n.fs.debugEnabled() {
		n.fs.debugOp("setattr complete", "path", targetPath, "inode", n.inode, "valid", valid)
	}
	return 0
}
