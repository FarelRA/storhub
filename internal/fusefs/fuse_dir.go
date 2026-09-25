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

// loadDir lists this directory once: the flat fuse.DirEntry stream for
// READDIR plus a per-child EntryInfo snapshot that READDIRPLUS answers
// EntryOut fills from, so `ls -l` costs one hub listing instead of one
// stat per child.
func (n *storhubNode) loadDir(ctx context.Context) ([]fuse.DirEntry, map[string]*shfs.EntryInfo, syscall.Errno) {
	dirPath, stale := n.safePath()
	if stale != 0 {
		return nil, nil, stale
	}
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("readdir start", "path", dirPath)
	}
	entries, err := n.fs.hub.ReadDirContext(ctx, n.fs.project, dirPath)
	if err != nil {
		n.fs.errorOp("readdir failed", "path", dirPath, "err", err)
		return nil, nil, errnoFromError(err)
	}
	result := make([]fuse.DirEntry, 0, len(entries)+2)
	result = append(result, fuse.DirEntry{Name: ".", Ino: n.inode, Mode: syscall.S_IFDIR})
	parentIno := uint64(1)
	if _, parent := n.Parent(); parent != nil {
		if ino := parent.StableAttr().Ino; ino != 0 {
			parentIno = ino
		}
	}
	result = append(result, fuse.DirEntry{Name: "..", Ino: parentIno, Mode: syscall.S_IFDIR})
	infos := make(map[string]*shfs.EntryInfo, len(entries))
	for _, entry := range entries {
		mode := uint32(syscall.S_IFREG)
		switch {
		case entry.IsDir:
			mode = syscall.S_IFDIR
		case entry.IsSymlink:
			mode = syscall.S_IFLNK
		}
		result = append(result, fuse.DirEntry{Name: entry.Name, Ino: entry.Inode, Mode: mode})
		infos[entry.Name] = shfs.EntryFromDirEntry(entry, path.Join(dirPath, entry.Name))
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("readdir complete", "path", dirPath, "elapsed", time.Since(started))
	}
	return result, infos, 0
}

// attachEntry registers entry under n and fills the kernel's entry cache
// line. It is the single entry-to-inode tail shared by lookup and every
// creation route (Mkdir/Create/Symlink/Link): register, attach, fill.
// A nil attach (detached tree, tests driving nodes without a mount)
// returns nil with no error, matching the node lookup route.
func (n *storhubNode) attachEntry(ctx context.Context, entry *shfs.EntryInfo, out *fuse.EntryOut) *gofusefs.Inode {
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	return ino
}

// publishEntry invalidates the parent namespace after a successful child
// creation, so the new name is visible before EntryTimeout expires.
func (s *Filesystem) publishEntry(parentPath, name string) {
	s.notifyEntryForPath(parentPath, name)
}

// evictChild drops path bookkeeping for a removed child, rebinds open
// handles to POSIX detached semantics, and issues the delete notification.
func (n *storhubNode) evictChild(name, childPath string, inode uint64) {
	remaining := n.fs.dropPath(inode, childPath)
	n.fs.rebindHandlesAfterPathChange(inode, childPath, remaining)
	n.notifyDelete(name, inode)
}

// notifyNamespaceChange evicts both parents' cached namespace after a
// rename. Both the source and the destination parent cached the old
// namespace; without both invalidations lookups serve the pre-rename tree
// until EntryTimeout expires.
func (s *Filesystem) notifyNamespaceChange(oldPath, newPath string) {
	oldDir, oldBase := shfs.ParentPath(oldPath), path.Base(oldPath)
	newDir, newBase := shfs.ParentPath(newPath), path.Base(newPath)
	s.notifyEntryForPath(oldDir, oldBase)
	if newDir != oldDir || newBase != oldBase {
		s.notifyEntryForPath(newDir, newBase)
	}
}

func (n *storhubNode) Readdir(ctx context.Context) (gofusefs.DirStream, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	result, _, errno := n.loadDir(ctx)
	if errno != 0 {
		return nil, errno
	}
	return gofusefs.NewListDirStream(result), 0
}

// storhubDirHandle is the directory FileHandle returned by OpendirHandle.
// go-fuse routes READDIRPLUS through FileLookuper on the handle, so
// answering it from the listing snapshot fills the kernel's per-child
// entry cache in one round-trip instead of a Lookup storm.
type storhubDirHandle struct {
	n       *storhubNode
	entries []fuse.DirEntry
	infos   map[string]*shfs.EntryInfo
	idx     int
	loaded  bool
}

func (n *storhubNode) OpendirHandle(ctx context.Context, flags uint32) (gofusefs.FileHandle, uint32, syscall.Errno) {
	_ = ctx
	_ = flags
	// The listing is taken lazily on the first read (mirroring go-fuse's
	// own dirStreamAsFile): opendir stays cheap and each stream sees the
	// tree as of its first readdir.
	return &storhubDirHandle{n: n}, 0, 0
}

func (d *storhubDirHandle) ensureLoaded(ctx context.Context) syscall.Errno {
	if d.loaded {
		return 0
	}
	entries, infos, errno := d.n.loadDir(d.n.fs.callerContext(ctx))
	if errno != 0 {
		return errno
	}
	d.entries, d.infos, d.loaded = entries, infos, true
	return 0
}

// Readdirent implements gofusefs.FileReaddirenter.
func (d *storhubDirHandle) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	if errno := d.ensureLoaded(ctx); errno != 0 {
		return nil, errno
	}
	if d.idx >= len(d.entries) {
		return nil, 0
	}
	entry := d.entries[d.idx]
	d.idx++
	entry.Off = uint64(d.idx)
	return &entry, 0
}

// Seekdir implements gofusefs.FileSeekdirer: the kernel may replay from an
// opaque offset after an interrupted read.
func (d *storhubDirHandle) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	if errno := d.ensureLoaded(ctx); errno != 0 {
		return errno
	}
	if off > uint64(len(d.entries)) {
		return syscall.EINVAL
	}
	d.idx = int(off)
	return 0
}

// Releasedir implements gofusefs.FileReleasedirer.
func (d *storhubDirHandle) Releasedir(_ context.Context, _ uint32) {}

// Lookup implements gofusefs.FileLookuper: READDIRPLUS asks the directory
// handle, not the node, to fill each child's EntryOut. The snapshot
// already carries the attributes, so the fill is a map hit instead of a
// hub stat; a name newer than the snapshot falls back to the live node
// route, which is the authority when the two disagree.
func (d *storhubDirHandle) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	if errno := d.ensureLoaded(ctx); errno != 0 {
		return nil, errno
	}
	entry := d.infos[name]
	if entry == nil {
		return d.n.Lookup(ctx, name, out)
	}
	n := d.n
	n.fs.overlayEntry(nil, entry.Inode, entry)
	return n.attachEntry(ctx, entry, out), 0
}

func (n *storhubNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	ctx = shfs.WithCreateMode(n.fs.callerContext(ctx), mode)
	parentPath, stale := n.safePath()
	if stale != 0 {
		return nil, stale
	}
	childPath := path.Join(parentPath, name)
	if n.fs.debugEnabled() {
		n.fs.debugOp("mkdir start", "path", childPath, "mode", mode)
	}
	if err := n.fs.hub.MkdirContext(ctx, n.fs.project, childPath); err != nil {
		n.fs.errorOp("mkdir failed", "path", childPath, "err", err)
		return nil, errnoFromError(err)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if err != nil {
		n.fs.errorOp("mkdir failed", "path", childPath, "err", err)
		return nil, errnoFromError(err)
	}
	ino := n.attachEntry(ctx, entry, out)
	n.fs.publishEntry(parentPath, name)
	if n.fs.debugEnabled() {
		n.fs.debugOp("mkdir complete", "path", childPath, "inode", entry.Inode)
	}
	return ino, 0
}

func (n *storhubNode) Unlink(ctx context.Context, name string) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	parentPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	childPath := path.Join(parentPath, name)
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("unlink start", "path", childPath)
	}
	entry, _ := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if entry != nil {
		if err := n.fs.materializeHandlesForPath(ctx, entry.Inode, childPath); err != nil {
			n.fs.errorOp("unlink failed", "path", childPath, "err", err)
			return errnoFromError(err)
		}
	}
	if err := n.fs.hub.UnlinkContext(ctx, n.fs.project, childPath); err != nil {
		n.fs.errorOp("unlink failed", "path", childPath, "err", err)
		return errnoFromError(err)
	}
	n.fs.dropPinnedForPath(childPath)
	if entry != nil {
		n.evictChild(name, childPath, entry.Inode)
	} else {
		n.notifyEntry(name)
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("unlink complete", "path", childPath, "elapsed", time.Since(started))
	}
	return 0
}

func (n *storhubNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	parentPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	childPath := path.Join(parentPath, name)
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("rmdir start", "path", childPath)
	}
	entry, _ := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if err := n.fs.hub.RmdirContext(ctx, n.fs.project, childPath); err != nil {
		n.fs.errorOp("rmdir failed", "path", childPath, "err", err)
		return errnoFromError(err)
	}
	n.fs.dropPinnedForPath(childPath)
	if entry != nil {
		n.evictChild(name, childPath, entry.Inode)
	} else {
		n.notifyEntry(name)
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("rmdir complete", "path", childPath, "elapsed", time.Since(started))
	}
	return 0
}

func (n *storhubNode) Rename(ctx context.Context, name string, newParent gofusefs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	oldDirPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	parentNode, ok := newParent.(*storhubNode)
	if !ok {
		return syscall.EINVAL
	}
	newDirPath, stale := parentNode.safePath()
	if stale != 0 {
		return stale
	}
	oldPath := path.Join(oldDirPath, name)
	newPath := path.Join(newDirPath, newName)
	if flags&renameExchange != 0 || flags&renameWhiteout != 0 {
		return syscall.EINVAL
	}
	started := time.Now()
	if n.fs.debugEnabled() {
		n.fs.debugOp("rename start", "path", oldPath, "dst", newPath, "flags", flags)
	}
	oldEntry, _ := n.fs.hub.StatPathContext(ctx, n.fs.project, oldPath)
	newEntry, _ := n.fs.hub.StatPathContext(ctx, n.fs.project, newPath)
	// The pre-stat is only a fast path; the authoritative noreplace
	// decision is enforced inside RenameContext's transaction via
	// shfs.WithNoReplace, so a target created between this stat and the
	// transaction still fails with EEXIST instead of being clobbered.
	var renameOpts []shfs.MutateOption
	if flags&renameNoReplace != 0 {
		if newEntry != nil {
			return syscall.EEXIST
		}
		renameOpts = append(renameOpts, shfs.WithNoReplace())
	}
	// A handle open on the replaced target must keep serving its own
	// snapshot: materialize before the metadata swap removes the path.
	if newEntry != nil && (oldEntry == nil || newEntry.Inode != oldEntry.Inode) {
		if err := n.fs.materializeHandlesForPath(ctx, newEntry.Inode, newPath); err != nil {
			n.fs.errorOp("rename failed", "path", oldPath, "dst", newPath, "flags", flags, "err", err)
			return errnoFromError(err)
		}
	}
	if err := n.fs.hub.RenameContext(ctx, n.fs.project, oldPath, newPath, renameOpts...); err != nil {
		n.fs.errorOp("rename failed", "path", oldPath, "dst", newPath, "flags", flags, "err", err)
		return errnoFromError(err)
	}
	// The namespace moved: cached layouts keyed by either path are stale
	// (open handles keep their own pin pointers and are unaffected).
	n.fs.dropPinnedForPath(oldPath)
	n.fs.dropPinnedForPath(newPath)
	if oldEntry != nil {
		n.fs.remapPaths(oldPath, newPath)
	}
	if newEntry != nil && (oldEntry == nil || newEntry.Inode != oldEntry.Inode) {
		remaining := n.fs.dropPath(newEntry.Inode, newPath)
		n.fs.rebindHandlesAfterPathChange(newEntry.Inode, newPath, remaining)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, newPath)
	if err == nil {
		n.fs.rememberPath(entry.Inode, newPath)
	} else {
		// The mapping for newPath was just dropped above; if the
		// re-stat fails the renamed node has no path left. Swallowing the
		// error silently loses the mapping - report it,
		// and re-register from the entry we already know when possible.
		n.fs.errorOp("rename post-stat failed", "path", newPath, "err", err)
		if oldEntry != nil {
			n.fs.rememberPath(oldEntry.Inode, newPath)
		}
	}
	// Both parents cached the old namespace; evict both or lookups serve
	// the pre-rename tree until EntryTimeout expires.
	n.fs.notifyNamespaceChange(oldPath, newPath)
	if n.fs.debugEnabled() {
		n.fs.debugOp("rename complete", "path", oldPath, "dst", newPath, "flags", flags, "elapsed", time.Since(started))
	}
	return 0
}
