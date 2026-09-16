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
	if mask&0x4 != 0 {
		need |= shfs.AccessRead
	}
	if mask&0x2 != 0 {
		need |= shfs.AccessWrite
	}
	if mask&0x1 != 0 {
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
	usedLocalSize := false
	localSize := int64(0)
	// The overlay is honored only for a caller who may drive it. A
	// handle attached to the write state is the common case; a path-based
	// setattr (no fh) still reaches the overlay when the caller is the
	// single writer who owns that inode's active state (a legitimate
	// ftruncate on an open file arrives handleless from some clients). A
	// non-owner's handleless truncate falls through to the hub verbs, which
	// enforce DAC against the *requesting* caller, so a stranger's
	// truncate/chmod is never deferred into the owner's next commit.
	var state *inodeWriteState
	if handle, ok := f.(*storhubHandle); ok && handle.writeState != nil {
		state = handle.writeState
		if errno := handle.checkOverlayCaller(ctx); errno != 0 {
			return errno
		}
	} else if st := n.fs.writeStateForInode(n.inode); st != nil && n.fs.callerMayDriveOverlay(ctx, st) {
		state = st
	}
	if size, ok := in.GetSize(); ok && !n.isDir {
		if state != nil {
			state.opMu.Lock()
			state.mu.Lock()
			if state.poisoned {
				state.mu.Unlock()
				state.opMu.Unlock()
				return syscall.EIO
			}
			err := state.setSizeLocked(int64(size))
			if err == nil {
				usedLocalSize = true
				localSize = state.logicalSize
			}
			state.mu.Unlock()
			state.opMu.Unlock()
			if err != nil {
				return errnoFromError(err)
			}
		} else {
			if _, err := n.fs.hub.TruncateFileContext(ctx, n.fs.project, targetPath, int64(size)); err != nil {
				return errnoFromError(err)
			}
		}
	}
	if mode, ok := in.GetMode(); ok {
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
		} else {
			if err := n.fs.hub.ChmodContext(ctx, n.fs.project, targetPath, mode&0o7777); err != nil {
				return errnoFromError(err)
			}
		}
	}
	uid, uidOK := in.GetUID()
	gid, gidOK := in.GetGID()
	if uidOK || gidOK {
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
		} else {
			if !uidOK {
				uid = entry.UID
			}
			if !gidOK {
				gid = entry.GID
			}
			if err := n.fs.hub.ChownContext(ctx, n.fs.project, targetPath, uid, gid); err != nil {
				return errnoFromError(err)
			}
		}
	}
	atime, atimeOK := in.GetATime()
	mtime, mtimeOK := in.GetMTime()
	if atimeOK || mtimeOK {
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
		} else {
			// go-fuse already resolves UTIME_NOW to time.Now(); ok=false
			// is UTIME_OMIT. Passing pointers preserves explicit epoch
			// timestamps that ChtimesContext's omit-on-zero would rewrite.
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
		}
	}
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
	n.fs.debugf("setattr path=%s valid=%#x", targetPath, in.Valid)
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
	entry := entryInfoFromFile(file, childPath, nlink)
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	n.fs.notifyEntryForPath(parentPath, name)
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
		// directory source) must not be dereferenced by entryInfoFromFile.
		return nil, syscall.EPERM
	}
	nlink := n.fs.nlinkForEntry(ctx, linkPath)
	entry := entryInfoFromFile(linked, linkPath, nlink)
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	n.fs.notifyEntryForPath(parentPath, name)
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

func entryInfoFromFile(file *metadata.FileMeta, path string, nlink int) *shfs.EntryInfo {
	kind := metadata.NodeKindFile
	if file.Symlink != "" {
		kind = metadata.NodeKindSymlink
	}
	return &shfs.EntryInfo{
		Path:          path,
		Kind:          kind,
		IsSymlink:     file.Symlink != "",
		Size:          file.Size,
		Inode:         file.Inode,
		Mode:          file.Mode,
		UID:           file.UID,
		GID:           file.GID,
		NLink:         uint32(nlink),
		CreatedAt:     file.UploadedAt,
		ModifiedAt:    file.ModifiedAt,
		AccessedAt:    file.AccessedAt,
		ChangedAt:     file.ChangedAt,
		SymlinkTarget: file.Symlink,
	}
}
