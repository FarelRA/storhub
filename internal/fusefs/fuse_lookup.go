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
	n.fs.debugf("getattr path=%s inode=%d", targetPath, n.inode)
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

// seekDataWhence and seekHoleWhence are the Linux SEEK_DATA/SEEK_HOLE
// values. Only these two whences reach FUSE_LSEEK (the kernel resolves
// SET/CUR/END itself); anything else is EINVAL.
const (
	seekDataWhence = 3
	seekHoleWhence = 4
)

// Compile-time check that the handle forwards lseek: the go-fuse bridge
// dispatches FUSE_LSEEK to FileLseeker on the open handle (falling back to
// a size-based default without it), so SEEK_DATA/SEEK_HOLE reach this
// method regardless of which lookup-layer file defines it.
var _ gofusefs.FileLseeker = (*storhubHandle)(nil)

// Lseek implements SEEK_DATA/SEEK_HOLE on the open handle. It lives in
// this lookup-layer file rather than fuse_file.go because the open/read/
// write paths there are owned by another agent; same package, so the
// method set is unaffected.
//
// Data extents are the union of the pinned chunk coverage (the open-time
// content layout) and the overlay dirty ranges (uncommitted writes,
// including fallocate extensions), clamped to the effective size. The size
// is the live overlay logical size when one exists for this inode
// (mirroring readLiveOverlay, so seeks agree with reads under contention)
// and the pinned size otherwise. Uncontended handles over dense metadata
// see exactly the stock default (data at off, hole at size); only real
// chunk gaps or dirty-range edges move the answers, so uncontended
// behavior is unchanged.
func (h *storhubHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	_ = ctx
	if off >= 1<<63 {
		return 0, syscall.EINVAL
	}
	size, extents := h.lseekView()
	if size < 0 {
		// No pinned layout and no live overlay: nothing to seek in.
		return 0, syscall.EIO
	}
	start := int64(off)
	switch whence {
	case seekDataWhence:
		if start >= size {
			return 0, syscall.ENXIO
		}
		for _, r := range extents {
			if start >= r.End {
				continue
			}
			if start < r.Start {
				return uint64(r.Start), 0
			}
			return uint64(start), 0
		}
		return 0, syscall.ENXIO
	case seekHoleWhence:
		if start > size {
			return 0, syscall.ENXIO
		}
		if start == size {
			return uint64(size), 0
		}
		pos := start
		for _, r := range extents {
			if pos < r.Start {
				return uint64(pos), 0
			}
			if pos < r.End {
				pos = r.End
			}
		}
		if pos > size {
			pos = size
		}
		return uint64(pos), 0
	default:
		return 0, syscall.EINVAL
	}
}

// lseekView snapshots the effective size and the sorted, merged data
// extents for Lseek. Own write state wins; otherwise a live overlay for
// the inode (another handle's uncommitted writes) counts, exactly like
// the read path; with neither, the pinned layout alone describes the
// file. Callers must not hold h.mu or any state mutex.
func (h *storhubHandle) lseekView() (int64, []ByteRange) {
	// Load-then-use under h.mu: Release nils the pointer concurrently.
	if ws := h.snapshotWriteState(); ws != nil {
		ws.mu.Lock()
		defer ws.mu.Unlock()
		size := ws.logicalSize
		return size, h.lseekExtentsLocked(ws, size)
	}
	if ws := h.fs.writeStateForInode(h.inode); ws != nil {
		ws.mu.Lock()
		defer ws.mu.Unlock()
		size := ws.logicalSize
		return size, h.lseekExtentsLocked(ws, size)
	}
	h.mu.Lock()
	pin := h.pinned
	h.mu.Unlock()
	if pin == nil {
		return -1, nil
	}
	return pin.file.Size, lseekChunkExtents(pin, pin.file.Size)
}

// lseekExtentsLocked merges the overlay dirty ranges over the pinned chunk
// coverage, clamped to size. A temp-authoritative overlay holds the full
// image locally, so the whole range is data regardless of chunk metadata.
func (h *storhubHandle) lseekExtentsLocked(ws *inodeWriteState, size int64) []ByteRange {
	if size <= 0 {
		return nil
	}
	if ws.tempAuthoritative {
		return []ByteRange{{Start: 0, End: size}}
	}
	h.mu.Lock()
	pin := h.pinned
	h.mu.Unlock()
	var extents []ByteRange
	if pin != nil {
		extents = lseekChunkExtents(pin, size)
	}
	for _, r := range ws.dirtyRanges {
		s, e := r.Start, r.End
		if s < 0 {
			s = 0
		}
		if e > size {
			e = size
		}
		if s < e {
			extents = append(extents, ByteRange{Start: s, End: e})
		}
	}
	return mergeByteRanges(extents)
}

// lseekChunkExtents converts the pinned chunk descriptors into byte spans
// clamped to [0, size). Descriptors missing from the shared map (corrupt
// metadata territory) contribute nothing, so unknown spans read as holes
// rather than claimed data that cannot be fetched.
func lseekChunkExtents(pin *pinnedContent, size int64) []ByteRange {
	if pin == nil || size <= 0 {
		return nil
	}
	limit := size
	if pin.file.Size < limit {
		limit = pin.file.Size
	}
	var extents []ByteRange
	for _, id := range pin.file.Chunks {
		c, ok := pin.chunks[id]
		if !ok {
			continue
		}
		s, e := c.Offset, c.Offset+c.Size
		if s < 0 {
			s = 0
		}
		if e > limit {
			e = limit
		}
		if s < e {
			extents = append(extents, ByteRange{Start: s, End: e})
		}
	}
	return mergeByteRanges(extents)
}

// mergeByteRanges sorts spans by start and coalesces overlapping or
// adjacent ones into disjoint data extents.
func mergeByteRanges(ranges []ByteRange) []ByteRange {
	if len(ranges) == 0 {
		return nil
	}
	sorted := append([]ByteRange(nil), ranges...)
	sortByteRanges(sorted)
	out := sorted[:1]
	for _, r := range sorted[1:] {
		last := &out[len(out)-1]
		if r.Start <= last.End {
			if r.End > last.End {
				last.End = r.End
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

// sortByteRanges orders spans by start offset, breaking ties by end.
func sortByteRanges(ranges []ByteRange) {
	for i := 1; i < len(ranges); i++ {
		for j := i; j > 0 && (ranges[j].Start < ranges[j-1].Start ||
			(ranges[j].Start == ranges[j-1].Start && ranges[j].End < ranges[j-1].End)); j-- {
			ranges[j], ranges[j-1] = ranges[j-1], ranges[j]
		}
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
// accepted and discarded — unobservable past close by definition — while
// a size change is EINVAL (ftruncate needs a writable fd).
//
// Identity note: the opener-match check of the linked path is
// deliberately absent. The kernel routes an fh only to the process that
// opened it, and a handleless call can only originate from an open fd
// (no path exists to name), so fd ownership is the auth; the verb DAC
// below is still enforced, and any staged change dies at Release.
// Anything else — directories, no surviving view — keeps ESTALE.
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
			unlockOpMu(&state.opMu)
			return syscall.EIO
		}
		err := state.setSizeLocked(int64(size))
		if err == nil {
			// state.path is "" here; the detached branch of the
			// stager is a safe no-op instead of a wrong-entry stat.
			n.fs.stagePrivClearForDataWrite(ctx, state, state.path)
		}
		state.mu.Unlock()
		unlockOpMu(&state.opMu)
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
				unlockOpMu(&state.opMu)
				return syscall.EIO
			}
			state.pending.HasMode = true
			state.pending.Mode = mode & 0o7777
			state.mu.Unlock()
			unlockOpMu(&state.opMu)
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
				unlockOpMu(&state.opMu)
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
			unlockOpMu(&state.opMu)
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
				unlockOpMu(&state.opMu)
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
			unlockOpMu(&state.opMu)
		}
	}
	n.detachedReply(base, state, out)
	n.fs.notifyKernelContentChanged(n.inode)
	n.fs.debugf("setattr detached inode=%d valid=%#x", n.inode, in.Valid)
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
			unlockOpMu(&state.opMu)
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
		unlockOpMu(&state.opMu)
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
			unlockOpMu(&state.opMu)
			return syscall.EIO
		}
		state.pending.HasMode = true
		state.pending.Mode = mode & 0o7777
		state.mu.Unlock()
		unlockOpMu(&state.opMu)
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
			unlockOpMu(&state.opMu)
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
		unlockOpMu(&state.opMu)
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
			unlockOpMu(&state.opMu)
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
		unlockOpMu(&state.opMu)
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
