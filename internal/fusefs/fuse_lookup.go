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
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("lookup start", "path", parentPath, "child", name)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if err != nil {
		// Only a genuine ENOENT deserves the negative-entry cache:
		// pinning EACCES/EIO as a cached "does not exist" would hide a
		// permission or backend problem for the whole timeout window.
		if out != nil && errnoFromError(err) == syscall.ENOENT {
			out.SetEntryTimeout(n.fs.opts.NegativeTimeout)
		}
		if n.fs.debugEnabled() {
			n.fs.debugOp("lookup failed", "path", childPath, "err", err)
		}
		return nil, errnoFromError(err)
	}
	n.fs.applyPendingSize(entry)
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	if n.fs.debugEnabled() {
		n.fs.debugOp("lookup complete", "path", childPath, "inode", entry.Inode, "elapsed", time.Since(started))
	}
	return ino, 0
}

func (n *storhubNode) Getattr(ctx context.Context, f gofusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		// Pathless node (unlinked while open): fstat on a surviving
		// fd must still work. Serve it from the detach-time snapshot
		// plus the live overlay; only a getattr with no surviving
		// view of the inode keeps ESTALE.
		if !n.isDir {
			if base, state, ok := n.detachedBase(f); ok {
				n.detachedReply(base, state, out)
				return 0
			}
		}
		return stale
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("getattr", "path", targetPath, "inode", n.inode)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return errnoFromError(err)
	}
	// Overlay staged mode/owner/times/size exactly as finishSetattr
	// does, so fstat on an open fd observes pre-commit fchmod/fchown
	// instead of the last committed stat.
	n.fs.applyOverlayForGetattr(f, n.inode, entry)
	fillAttr(&out.Attr, entry)
	out.SetTimeout(n.fs.opts.AttrTimeout)
	return 0
}

// applyOverlayForGetattr overlays the live write state onto a linked-file
// stat result. The calling handle's own state wins: it may still be
// referenced after unregistering from the inode map (e.g. a quarantined
// state), where the shared lookup below would miss it. Otherwise the
// shared state for the inode covers another handle's staged patch, and a
// handleless getattr falls back to applyPendingSize.
func (s *Filesystem) applyOverlayForGetattr(f gofusefs.FileHandle, inode uint64, entry *shfs.EntryInfo) {
	if handle, ok := f.(*storhubHandle); ok {
		if ws := handle.snapshotWriteState(); ws != nil && ws.inode == inode {
			ws.mu.Lock()
			ws.overlayEntryLocked(entry)
			ws.mu.Unlock()
			return
		}
	}
	s.applyPendingSize(entry)
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
	if n.fs.debugEnabled() {
		n.fs.debugOp("access", "path", targetPath, "inode", n.inode, "mask", mask)
	}
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
		s.errorOp("nlink lookup failed", "path", entryPath, "err", err)
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
	atime := time.Unix(0, entry.AccessedAt)
	mtime := time.Unix(0, entry.ModifiedAt)
	ctime := time.Unix(0, entry.ChangedAt)
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
