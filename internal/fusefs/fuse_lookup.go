package fusefs

import (
	"context"
	"path"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
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
		n.fs.debugOp("lookup start", "path", childPath)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if err != nil {
		// Only a genuine ENOENT deserves the negative-entry cache:
		// pinning EACCES/EIO as a cached "does not exist" would hide a
		// permission or backend problem for the whole timeout window.
		if out != nil && errnoFromError(err) == syscall.ENOENT {
			out.SetEntryTimeout(n.fs.opts.NegativeTimeout)
		}
		n.fs.errorOp("lookup failed", "path", childPath, "err", err)
		return nil, errnoFromError(err)
	}
	n.fs.overlayEntry(nil, entry.Inode, entry)
	ino := n.attachEntry(ctx, entry, out)
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
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("getattr start", "path", targetPath, "inode", n.inode)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		n.fs.errorOp("getattr failed", "path", targetPath, "inode", n.inode, "err", err)
		return errnoFromError(err)
	}
	// Overlay staged mode/owner/times/size exactly as finishSetattr
	// does, so fstat on an open fd observes pre-commit fchmod/fchown
	// instead of the last committed stat.
	n.fs.overlayEntry(f, n.inode, entry)
	fillAttr(&out.Attr, entry)
	out.SetTimeout(n.fs.opts.AttrTimeout)
	if n.fs.debugEnabled() {
		n.fs.debugOp("getattr complete", "path", targetPath, "inode", n.inode, "elapsed", time.Since(started))
	}
	return 0
}

// overlayEntry overlays the live write state onto a stat result: the
// calling handle's own state wins (it may be unregistered from the inode
// map, where the shared lookup below would miss it), otherwise the shared
// state for the inode covers another handle's staged patch, and a
// handleless stat falls back to the shared state via applyPendingSize.
// Every stat route (lookup, getattr, readdir-plus) funnels here, so all
// of them observe the same staged mode/owner/times/size. The overlay
// mechanics themselves (applyPendingLocked, applyPendingSize) live with
// the write path; this is the read-side funnel over them.
func (s *Filesystem) overlayEntry(f gofusefs.FileHandle, inode uint64, entry *shfs.EntryInfo) {
	if handle, ok := f.(*storhubHandle); ok {
		if ws := handle.loadWriteState(); ws != nil && ws.inode == inode {
			ws.mu.Lock()
			ws.applyPendingLocked(entry)
			ws.mu.Unlock()
			return
		}
	}
	s.applyPendingSize(entry)
}

func (n *storhubNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// Usage figures carry no per-user content, but the call still runs
	// through the caller injector so context markers (suppressed atime,
	// identity where the kernel supplied one) travel uniformly.
	ctx = n.fs.callerContext(ctx)
	stats, err := n.fs.hub.StatFSContext(ctx, n.fs.project)
	if err != nil {
		return errnoFromError(err)
	}
	out.Files = uint64(stats.Inodes)
	// Report a quota-style view where total = used + free, so df
	// never shows free space exceeding the filesystem size (the old
	// Bfree = 1<<30 with Blocks = used+1 produced negative usage).
	usedBlocks := uint64(max(stats.Bytes/4096, 0))
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
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("access start", "path", targetPath, "inode", n.inode, "mask", mask)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		n.fs.errorOp("access failed", "path", targetPath, "inode", n.inode, "mask", mask, "err", err)
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
		if n.fs.debugEnabled() {
			n.fs.debugOp("access complete", "path", targetPath, "inode", n.inode, "mask", mask, "elapsed", time.Since(started))
		}
		return 0
	}
	id := shfs.IdentityFromContext(ctx)
	if err := shfs.CanAccessEntry(id, entry, need); err != nil {
		n.fs.errorOp("access failed", "path", targetPath, "inode", n.inode, "mask", mask, "err", err)
		return errnoFromError(err)
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("access complete", "path", targetPath, "inode", n.inode, "mask", mask, "elapsed", time.Since(started))
	}
	return 0
}

func (n *storhubNode) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	_ = dev
	switch mode & syscall.S_IFMT {
	case 0, syscall.S_IFREG:
		inode, _, _, errno := n.Create(ctx, name, syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY, permBits(mode), out)
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
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("symlink start", "path", childPath, "target", target)
	}
	file, err := n.fs.hub.SymlinkContext(ctx, n.fs.project, target, childPath)
	if err != nil {
		n.fs.errorOp("symlink failed", "path", childPath, "target", target, "err", err)
		return nil, errnoFromError(err)
	}
	nlink := n.fs.nlinkForEntry(ctx, childPath)
	entry := shfs.EntryFromFile(file, childPath, nlink)
	ino := n.attachEntry(ctx, entry, out)
	n.fs.publishEntry(parentPath, name)
	if n.fs.debugEnabled() {
		n.fs.debugOp("symlink complete", "path", childPath, "target", target, "inode", entry.Inode, "elapsed", time.Since(started))
	}
	return ino, 0
}

func (n *storhubNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return nil, stale
	}
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("readlink start", "path", targetPath, "inode", n.inode)
	}
	target, err := n.fs.hub.ReadlinkContext(ctx, n.fs.project, targetPath)
	if err != nil {
		n.fs.errorOp("readlink failed", "path", targetPath, "inode", n.inode, "err", err)
		return nil, errnoFromError(err)
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("readlink complete", "path", targetPath, "target", target, "inode", n.inode, "elapsed", time.Since(started))
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
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("link start", "path", linkPath, "src", sourcePath)
	}
	linked, err := n.fs.hub.LinkContext(ctx, n.fs.project, sourcePath, linkPath)
	if err != nil {
		n.fs.errorOp("link failed", "path", linkPath, "src", sourcePath, "err", err)
		return nil, errnoFromError(err)
	}
	if linked == nil {
		// A hub that reports success without an entry (e.g. a
		// directory source) must not be dereferenced by the constructor.
		n.fs.errorOp("link failed", "path", linkPath, "src", sourcePath, "err", syscall.EPERM)
		return nil, syscall.EPERM
	}
	nlink := n.fs.nlinkForEntry(ctx, linkPath)
	entry := shfs.EntryFromFile(linked, linkPath, nlink)
	ino := n.attachEntry(ctx, entry, out)
	n.fs.publishEntry(parentPath, name)
	if n.fs.debugEnabled() {
		n.fs.debugOp("link complete", "path", linkPath, "src", sourcePath, "inode", entry.Inode, "elapsed", time.Since(started))
	}
	return ino, 0
}

// nlinkForEntry reports the hard-link count for a freshly created entry.
// A metadata load failure is logged and reported as 1 rather than silently
// fabricated as 0; getattr refreshes the value on the next lookup anyway.
// The int-to-uint32 conversion happens once at the EntryFrom* boundary,
// so this stays int and every renderer keeps uint32.
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

// fillEntryOut renders one EntryInfo into the kernel entry cache line.
// EntryFrom* (storage to EntryInfo) and Fill* (EntryInfo to kernel wire)
// are the two converter layers: storage builds the view, this package
// renders it, and neither re-spells the other's mapping.
func fillEntryOut(out *fuse.EntryOut, entry *shfs.EntryInfo, opts Options) {
	if out == nil || entry == nil {
		return
	}
	fillAttr(&out.Attr, entry)
	out.SetAttrTimeout(opts.AttrTimeout)
	out.SetEntryTimeout(opts.EntryTimeout)
}

// fillAttr renders one EntryInfo into kernel attributes. Timestamps are
// stored as int64 nanos and cross the wire as time.Time: this is the one
// place that conversion happens, so every attr route agrees on it.
func fillAttr(attr *fuse.Attr, entry *shfs.EntryInfo) {
	attr.Ino = entry.Inode
	attr.Size = uint64(max(entry.Size, 0))
	attr.Blocks = uint64(max((entry.Size+511)/512, 0))
	attr.Owner = fuse.Owner{Uid: entry.UID, Gid: entry.GID}
	attr.Nlink = entry.NLink
	attr.Blksize = 4096
	atime := time.Unix(0, entry.AccessedAt)
	mtime := time.Unix(0, entry.ModifiedAt)
	ctime := time.Unix(0, entry.ChangedAt)
	attr.SetTimes(&atime, &mtime, &ctime)
	mode := permBits(entry.Mode)
	if entry.IsDir {
		mode |= syscall.S_IFDIR
	} else if entry.IsSymlink {
		mode |= syscall.S_IFLNK
	} else {
		mode |= syscall.S_IFREG
	}
	attr.Mode = mode
}
