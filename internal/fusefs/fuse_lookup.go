package fusefs

import (
	"context"
	"path"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func (n *storhubNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	parentPath, stale := n.safePath()
	if stale != 0 {
		return nil, stale
	}
	childPath := path.Join(parentPath, name)
	n.fs.debugf("lookup path=%s child=%s", parentPath, name)
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if err != nil {
		// Only a genuine ENOENT deserves the negative-entry cache:
		// pinning EACCES/EIO as a cached "does not exist" would hide a
		// permission or backend problem for the whole timeout window.
		if out != nil && errnoFromError(err) == syscall.ENOENT {
			out.SetEntryTimeout(n.fs.opts.NegativeTimeout)
		}
		return nil, errnoFromError(err)
	}
	n.fs.applyPendingSize(entry)
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	return ino, 0
}

func (n *storhubNode) Getattr(ctx context.Context, f gofusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	n.fs.debugf("getattr path=%s inode=%d", targetPath, n.inode)
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return errnoFromError(err)
	}
	n.fs.applyPendingSize(entry)
	fillAttr(&out.Attr, entry)
	out.SetTimeout(n.fs.opts.AttrTimeout)
	_ = f
	return 0
}

func (n *storhubNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	stats, err := n.fs.hub.StatFSContext(ctx, n.fs.project)
	if err != nil {
		return errnoFromError(err)
	}
	out.Files = uint64(stats.Inodes)
	// Report a quota-style view where total = used + free, so df
	// never shows free space exceeding the filesystem size (the old
	// Bfree = 1<<30 with Blocks = used+1 produced negative usage).
	usedBlocks := uint64(maxInt64(stats.Bytes/4096, 0))
	freeBlocks := uint64(1 << 30)
	out.Bsize = 4096
	out.Blocks = usedBlocks + freeBlocks
	out.Bfree = freeBlocks
	out.Bavail = freeBlocks
	out.NameLen = 255
	out.Frsize = 4096
	return 0
}

func (n *storhubNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	n.fs.debugf("access path=%s inode=%d mask=%#x", targetPath, n.inode, mask)
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return errnoFromError(err)
	}
	need := 0
	if mask&uint32(shfs.AccessRead) != 0 {
		need |= shfs.AccessRead
	}
	if mask&uint32(shfs.AccessWrite) != 0 {
		need |= shfs.AccessWrite
	}
	if mask&uint32(shfs.AccessExec) != 0 {
		need |= shfs.AccessExec
	}
	if need == 0 {
		return 0
	}
	id := shfs.IdentityFromContext(ctx)
	if err := shfs.CanAccessEntry(id, entry, need); err != nil {
		return errnoFromError(err)
	}
	return 0
}

func (n *storhubNode) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	_ = dev
	switch mode & syscall.S_IFMT {
	case 0, syscall.S_IFREG:
		inode, _, _, errno := n.Create(ctx, name, syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY, mode&0o7777, out)
		return inode, errno
	case syscall.S_IFIFO, syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFSOCK:
		return nil, syscall.ENOTSUP
	default:
		return nil, syscall.ENOTSUP
	}
}

// checkOverlayCaller verifies that the current caller may drive overlay
// mutations through this handle: the kernel routes an fh only to the
// process that opened it, but the server is the only DAC gate under
// NullPermissions, so re-validate the opener identity.
func (h *storhubHandle) checkOverlayCaller(ctx context.Context) syscall.Errno {
	if !shfs.IdentityPresent(ctx) {
		return 0
	}
	id := shfs.IdentityFromContext(ctx)
	if id.Admin {
		return 0
	}
	if h.hasOpener && id.UID != h.openerUID {
		return syscall.EPERM
	}
	return 0
}

// callerMayDriveOverlay reports whether the current caller may mutate an
// existing write state through a handleless path-based operation. It is the
// state-level counterpart of checkOverlayCaller: the overlay is the single
// writer's buffer, so only that writer (or an admin, or a request with no
// server-side identity, i.e. the trusted local process) may drive it. A
// poisoned or deleted state is never driven here; the caller falls through to
// the hub verbs, which enforce DAC independently.
func (s *Filesystem) callerMayDriveOverlay(ctx context.Context, state *inodeWriteState) bool {
	state.mu.Lock()
	poisoned, deleted := state.poisoned, state.deleted
	hasOpener, openerUID := state.hasOpener, state.openerUID
	state.mu.Unlock()
	if poisoned || deleted {
		return false
	}
	if !shfs.IdentityPresent(ctx) {
		return true
	}
	id := shfs.IdentityFromContext(ctx)
	if id.Admin {
		return true
	}
	return !hasOpener || id.UID == openerUID
}

func (n *storhubNode) Setattr(ctx context.Context, f gofusefs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	state, errno := n.setattrOverlayState(ctx, f)
	if errno != 0 {
		return errno
	}
	usedLocalSize, localSize, errno := n.setattrSize(ctx, targetPath, in, state)
	if errno != 0 {
		return errno
	}
	if errno := n.setattrMode(ctx, targetPath, in, state); errno != 0 {
		return errno
	}
	if errno := n.setattrOwner(ctx, targetPath, in, state); errno != 0 {
		return errno
	}
	if errno := n.setattrTimes(ctx, targetPath, in, state); errno != 0 {
		return errno
	}
	return n.finishSetattr(ctx, targetPath, state, usedLocalSize, localSize, out, in.Valid)
}

// setattrOverlayState resolves the write state this setattr may drive.
// The overlay is honored only for a caller who may drive it. A
// handle attached to the write state is the common case; a path-based
// setattr (no fh) still reaches the overlay when the caller is the
// single writer who owns that inode's active state (a legitimate
// ftruncate on an open file arrives handleless from some clients). A
// non-owner's handleless truncate falls through to the hub verbs, which
// enforce DAC against the *requesting* caller, so a stranger's
// truncate/chmod is never deferred into the owner's next commit.
// A nil state means "go to the hub verbs".
func (n *storhubNode) setattrOverlayState(ctx context.Context, f gofusefs.FileHandle) (*inodeWriteState, syscall.Errno) {
	if handle, ok := f.(*storhubHandle); ok && handle.writeState != nil {
		if errno := handle.checkOverlayCaller(ctx); errno != 0 {
			return nil, errno
		}
		return handle.writeState, 0
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
			state.opMu.Unlock()
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
		state.opMu.Unlock()
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
			state.opMu.Unlock()
			return syscall.EIO
		}
		state.pending.HasMode = true
		state.pending.Mode = mode & 0o7777
		state.mu.Unlock()
		state.opMu.Unlock()
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
			state.opMu.Unlock()
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
		state.mu.Unlock()
		state.opMu.Unlock()
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
		atime = time.Unix(entry.AccessedAt, 0)
	}
	if !mtimeOK {
		mtime = time.Unix(entry.ModifiedAt, 0)
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
			state.opMu.Unlock()
			return syscall.EIO
		}
		state.overlayEntryLocked(entry)
		if !atimeOK {
			atime = time.Unix(entry.AccessedAt, 0)
		}
		if !mtimeOK {
			mtime = time.Unix(entry.ModifiedAt, 0)
		}
		state.pending.HasTimes = true
		state.pending.ATime = atime
		state.pending.MTime = mtime
		state.mu.Unlock()
		state.opMu.Unlock()
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
	n.fs.debugf("setattr path=%s valid=%#x", targetPath, valid)
	return 0
}

func (n *storhubNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	parentPath, stale := n.safePath()
	if stale != 0 {
		return nil, stale
	}
	childPath := path.Join(parentPath, name)
	file, err := n.fs.hub.SymlinkContext(ctx, n.fs.project, target, childPath)
	if err != nil {
		return nil, errnoFromError(err)
	}
	nlink := n.fs.nlinkForEntry(ctx, childPath)
	entry := shfs.EntryFromFile(file, childPath, nlink)
	ino := n.attachEntry(ctx, entry, out)
	n.fs.publishEntry(parentPath, name)
	return ino, 0
}

func (n *storhubNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return nil, stale
	}
	target, err := n.fs.hub.ReadlinkContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return nil, errnoFromError(err)
	}
	return []byte(target), 0
}

func (n *storhubNode) Link(ctx context.Context, target gofusefs.InodeEmbedder, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetNode, ok := target.(*storhubNode)
	if !ok {
		return nil, syscall.EINVAL
	}
	sourcePath, stale := targetNode.safePath()
	if stale != 0 {
		return nil, stale
	}
	parentPath, stale := n.safePath()
	if stale != 0 {
		return nil, stale
	}
	linkPath := path.Join(parentPath, name)
	linked, err := n.fs.hub.LinkContext(ctx, n.fs.project, sourcePath, linkPath)
	if err != nil {
		return nil, errnoFromError(err)
	}
	if linked == nil {
		// A hub that reports success without an entry (e.g. a
		// directory source) must not be dereferenced by the constructor.
		return nil, syscall.EPERM
	}
	nlink := n.fs.nlinkForEntry(ctx, linkPath)
	entry := shfs.EntryFromFile(linked, linkPath, nlink)
	ino := n.attachEntry(ctx, entry, out)
	n.fs.publishEntry(parentPath, name)
	return ino, 0
}

// nlinkForEntry reports the hard-link count for a freshly created entry.
// A metadata load failure is logged and reported as 1 rather than silently
// fabricated as 0; getattr refreshes the value on the next lookup anyway.
// The caller's context is propagated: a background context would
// fall back to the process identity on a surface that must carry the
// kernel caller.
func (s *Filesystem) nlinkForEntry(ctx context.Context, entryPath string) int {
	repo, _, err := s.hub.LoadRepoMetadataReadonlyContext(ctx, s.project)
	if err != nil {
		s.errorf("nlink lookup failed path=%s err=%v", entryPath, err)
		return 1
	}
	if repo == nil {
		return 1
	}
	if n := repo.FileNLink(entryPath); n > 0 {
		return n
	}
	return 1
}

func fillEntryOut(out *fuse.EntryOut, entry *shfs.EntryInfo, opts Options) {
	if out == nil || entry == nil {
		return
	}
	fillAttr(&out.Attr, entry)
	out.SetAttrTimeout(opts.AttrTimeout)
	out.SetEntryTimeout(opts.EntryTimeout)
}

func fillAttr(attr *fuse.Attr, entry *shfs.EntryInfo) {
	attr.Ino = entry.Inode
	attr.Size = uint64(maxInt64(entry.Size, 0))
	attr.Blocks = uint64(maxInt64((entry.Size+511)/512, 0))
	attr.Owner = fuse.Owner{Uid: entry.UID, Gid: entry.GID}
	attr.Nlink = entry.NLink
	attr.Blksize = 4096
	atime := time.Unix(entry.AccessedAt, 0)
	mtime := time.Unix(entry.ModifiedAt, 0)
	ctime := time.Unix(entry.ChangedAt, 0)
	attr.SetTimes(&atime, &mtime, &ctime)
	mode := entry.Mode & 0o7777
	if entry.IsDir {
		mode |= syscall.S_IFDIR
	} else if entry.IsSymlink {
		mode |= syscall.S_IFLNK
	} else {
		mode |= syscall.S_IFREG
	}
	attr.Mode = mode
}

// entryInfoFromFile is kept for existing callers (including tests); it
// delegates to the single shfs constructor.
func entryInfoFromFile(file *metadata.FileMeta, path string, nlink int) *shfs.EntryInfo {
	return shfs.EntryFromFile(file, path, nlink)
}
