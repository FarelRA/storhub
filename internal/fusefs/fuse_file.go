package fusefs

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type storhubHandle struct {
	fs    *Filesystem
	inode uint64
	id    uint64
	flags uint32

	// openerUID/openerGID record the kernel caller identity captured at
	// open time (only when the context carried one). Overlay metadata
	// operations re-validate it so a handle can never be driven by a
	// different uid than the one that opened it.
	openerUID uint32
	openerGID uint32
	hasOpener bool

	// pinned holds the content identity captured at open time: the file
	// entry plus the chunk descriptors it referenced. The lazy read
	// fallback resolves bytes against it, so renames or unlinks after
	// open can never change what this handle returns (POSIX open
	// semantics). Pure metadata - a few dozen bytes even for large files;
	// no network at pin time.
	pinned *pinnedContent

	mu         sync.Mutex
	temp       *os.File
	tempPath   string
	path       string
	closed     bool
	deleted    bool
	owners     map[uint64]struct{}
	writeState *inodeWriteState
}

// pinnedContent is an immutable, self-contained view of one file's data
// layout. Chunk assets are content-addressed, so the descriptors stay
// valid for the handle's lifetime regardless of later metadata changes.
// Pins are shared across handles with the same key (see pinnedKey): the
// struct and its chunk map are never mutated after publication, so
// concurrent readers need no lock.
type pinnedContent struct {
	file   metadata.FileMeta
	chunks map[int64]metadata.ChunkInfo
}

// pinnedKey identifies one shareable open-time content layout: the file's
// path plus its version (inode, size, mtime, ctime). Identical keys imply
// identical chunk layouts: any content change through the tracked mutators
// bumps size, mtime, or ctime (and replace/rename paths drop the key
// outright via dropPinnedForPath).
type pinnedKey struct {
	project  string
	path     string
	inode    uint64
	size     int64
	modified int64
	changed  int64
}

// maxPinnedEntries caps the shared pin cache: each entry is metadata only
// (a FileMeta plus one descriptor per chunk), but K concurrent opens of a
// many-chunk file must not grow it without bound. Inserts past the cap
// evict one arbitrary entry; the next open re-pins.
const maxPinnedEntries = 256

func pinnedKeyFor(project, targetPath string, file *metadata.FileMeta) pinnedKey {
	return pinnedKey{
		project:  project,
		path:     targetPath,
		inode:    file.Inode,
		size:     file.Size,
		modified: file.ModifiedAt,
		changed:  file.ChangedAt,
	}
}

func (s *Filesystem) pinnedFor(key pinnedKey) (*pinnedContent, bool) {
	s.pinnedMu.Lock()
	defer s.pinnedMu.Unlock()
	pin, ok := s.pinned[key]
	return pin, ok
}

func (s *Filesystem) storePinned(key pinnedKey, pin *pinnedContent) {
	s.pinnedMu.Lock()
	defer s.pinnedMu.Unlock()
	if s.pinned == nil {
		s.pinned = make(map[pinnedKey]*pinnedContent)
	}
	if _, ok := s.pinned[key]; !ok && len(s.pinned) >= maxPinnedEntries {
		for old := range s.pinned {
			delete(s.pinned, old)
			break
		}
	}
	s.pinned[key] = pin
}

// dropPinnedForPath forgets cached layouts at targetPath or below it after
// a rename, unlink, rmdir, or content commit. Version keys already
// self-heal (the next open misses and re-pins), so this is hygiene: it
// keeps churn from pinning dead layouts until cap eviction.
func (s *Filesystem) dropPinnedForPath(targetPath string) {
	s.pinnedMu.Lock()
	defer s.pinnedMu.Unlock()
	for key := range s.pinned {
		if key.path == targetPath || strings.HasPrefix(key.path, targetPath+"/") {
			delete(s.pinned, key)
		}
	}
}

func (n *storhubNode) Open(ctx context.Context, flags uint32) (gofusefs.FileHandle, uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return nil, 0, stale
	}
	n.fs.debugf("open start path=%s inode=%d flags=%#x", targetPath, n.inode, flags)
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return nil, 0, errnoFromError(err)
	}
	if entry.IsDir {
		return nil, 0, syscall.EISDIR
	}
	if entry.IsSymlink {
		return nil, 0, syscall.ELOOP
	}
	// Pin the content layout at open time, shared across handles that
	// open the same version (see pinnedKeyFor): a file entry clone plus
	// the chunk descriptors it references. Pure metadata copying - no
	// network beyond what the stat already did. If the file vanished
	// between stat and pin, the open fails with ENOENT rather than
	// degrading to path-live reads, which would reintroduce the
	// rename-over race this pin prevents.
	repoMeta, _, metaErr := n.fs.hub.LoadRepoMetadataReadonlyContext(ctx, n.fs.project)
	if metaErr != nil {
		return nil, 0, errnoFromError(metaErr)
	}
	file := repoMeta.FindFile(targetPath)
	if file == nil {
		return nil, 0, syscall.ENOENT
	}
	// The mount runs with NullPermissions, so the kernel enforces no
	// DAC at all - this server is the only gate. A write-open must carry
	// write permission on the file, or any user could overwrite any file
	// through the overlay commit path. Requests without a kernel caller
	// identity are direct library use (the local process on its own
	// repository) and are not multi-user surfaces.
	if shfs.IdentityPresent(ctx) {
		// Open-ONLY read gate: any open that is not write-only must carry
		// read permission on the file. Reads through the handle stay fast
		// (no per-Read re-check): the open-time pin below captures the
		// content layout, and the commit-time write DAC still guards the
		// overlay path independently.
		if flags&syscall.O_ACCMODE != syscall.O_WRONLY {
			if err := shfs.CheckReadAccess(ctx, repoMeta, targetPath); err != nil {
				n.fs.debugf("open denied path=%s step=read-dac err=%v", targetPath, err)
				return nil, 0, errnoFromError(err)
			}
		}
		if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND) != 0 {
			if err := shfs.CheckWriteAccess(ctx, repoMeta, targetPath); err != nil {
				n.fs.debugf("open denied path=%s step=dac err=%v", targetPath, err)
				return nil, 0, errnoFromError(err)
			}
		}
	}
	// Pin the content layout at open time, shared across handles that
	// open the same version: a file entry clone plus the chunk descriptors
	// it references. Pure metadata copying - no network beyond what the
	// stat already did. If the file vanished between stat and pin, the
	// open fails with ENOENT rather than degrading to path-live reads,
	// which would reintroduce the rename-over race this pin prevents.
	key := pinnedKeyFor(n.fs.project, targetPath, file)
	pin, ok := n.fs.pinnedFor(key)
	if !ok {
		fresh := &pinnedContent{
			file:   file.Clone(),
			chunks: make(map[int64]metadata.ChunkInfo, len(file.Chunks)),
		}
		for _, id := range file.Chunks {
			if chunk, ok := repoMeta.Chunks()[id]; ok {
				fresh.chunks[id] = chunk
			}
		}
		n.fs.storePinned(key, fresh)
		pin = fresh
	}
	h, err := n.fs.newHandle(ctx, n.inode, targetPath, flags, nil)
	if err != nil {
		return nil, 0, errnoFromError(err)
	}
	h.pinned = pin
	n.fs.debugf("open path=%s inode=%d flags=%#x", targetPath, n.inode, flags)
	return h, 0, 0
}

func (n *storhubNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofusefs.Inode, gofusefs.FileHandle, uint32, syscall.Errno) {
	ctx = shfs.WithCreateMode(n.fs.callerContext(ctx), mode)
	parentPath, stale := n.safePath()
	if stale != 0 {
		return nil, nil, 0, stale
	}
	childPath := path.Join(parentPath, name)
	n.fs.debugf("create start path=%s flags=%#x mode=%#o", childPath, flags, mode)
	file, err := n.fs.hub.CreateFileContext(ctx, n.fs.project, childPath)
	if err != nil {
		n.fs.debugf("create failed path=%s step=create err=%v", childPath, err)
		return nil, nil, 0, errnoFromError(err)
	}
	nlink := n.fs.nlinkForEntry(ctx, childPath)
	entry := shfs.EntryFromFile(file, childPath, nlink)
	ino := n.attachEntry(ctx, entry, out)
	h, err := n.fs.newHandle(ctx, entry.Inode, childPath, flags, &writeBootstrap{baseSize: entry.Size})
	if err != nil {
		n.fs.debugf("create failed path=%s step=open-handle err=%v", childPath, err)
		// The empty file is already committed remotely; leaving it
		// behind would orphan an entry the application was told was never
		// created. Roll it back before reporting the failure.
		if unlinkErr := n.fs.hub.UnlinkContext(ctx, n.fs.project, childPath); unlinkErr != nil {
			n.fs.errorf("create rollback failed path=%s err=%v (original: %v)", childPath, unlinkErr, err)
		}
		n.fs.notifyEntryForPath(parentPath, name)
		return nil, nil, 0, errnoFromError(err)
	}
	// The kernel may hold a negative entry for this name (NegativeTimeout);
	// the create must evict it or the file stays invisible until expiry.
	n.fs.notifyEntryForPath(parentPath, name)
	n.fs.debugf("create path=%s inode=%d flags=%#x mode=%#o", childPath, entry.Inode, flags, mode)
	return ino, h, 0, 0
}

func (s *Filesystem) newHandle(ctx context.Context, inode uint64, targetPath string, flags uint32, bootstrap *writeBootstrap) (*storhubHandle, error) {
	if targetPath == "" {
		targetPath = s.pathForInode(inode)
	}
	h := &storhubHandle{fs: s, inode: inode, flags: flags, id: s.nextHandle.Add(1), path: targetPath, owners: make(map[uint64]struct{})}
	if shfs.IdentityPresent(ctx) {
		id := shfs.IdentityFromContext(ctx)
		h.openerUID, h.openerGID, h.hasOpener = id.UID, id.GID, true
	}
	s.mu.Lock()
	s.handles[h.id] = h
	s.mu.Unlock()
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_TRUNC) != 0 {
		writeState, err := s.acquireWriteState(ctx, inode, h.path, bootstrap)
		if err != nil {
			h.closeTemp()
			s.mu.Lock()
			delete(s.handles, h.id)
			s.mu.Unlock()
			return nil, err
		}
		h.writeState = writeState
		if flags&syscall.O_TRUNC != 0 && bootstrap == nil {
			// Serialize with in-flight commits and writes on opMu: the
			// truncate must land wholly before or after them, never in
			// the middle of a commit that already captured its plan.
			// Lock order is always opMu before mu.
			writeState.opMu.Lock()
			writeState.mu.Lock()
			if err := writeState.setSizeLocked(0); err != nil {
				writeState.mu.Unlock()
				writeState.opMu.Unlock()
				s.releaseWriteState(writeState)
				s.mu.Lock()
				delete(s.handles, h.id)
				s.mu.Unlock()
				return nil, err
			}
			// O_TRUNC is a data write for privilege purposes: stage
			// the cleared mode immediately for non-admin openers.
			// ctx already carries the caller identity (bound in
			// Open/Create).
			s.stagePrivClearForDataWrite(ctx, writeState, writeState.path)
			writeState.mu.Unlock()
			writeState.opMu.Unlock()
		}
	}
	// A read-only open never attaches to another writer's writeState:
	// it serves the pinned snapshot instead. Sharing would let its
	// commit() push another handle's dirty bytes and let its Release
	// drop the writer's refcount.
	return h, nil
}

func (h *storhubHandle) materializePath(ctx context.Context, targetPath string) error {
	h.mu.Lock()
	if h.temp != nil {
		h.mu.Unlock()
		return nil
	}
	temp, err := h.fs.newOverlayTemp("handle-*")
	if err != nil {
		h.mu.Unlock()
		return err
	}
	h.temp = temp
	h.tempPath = temp.Name()
	h.path = targetPath
	h.mu.Unlock()

	// Network calls without lock
	entry, err := h.fs.hub.StatPathContext(ctx, h.fs.project, targetPath)
	if err != nil {
		// Propagate: silently treating stat failure as "empty file"
		// would serve EOF for a readable handle whose remote stat
		// merely hiccupped (same contract as the writeState twin).
		if rmErr := temp.Close(); rmErr != nil {
			h.fs.errorf("materialize cleanup failed path=%s temp=%s err=%v", targetPath, temp.Name(), rmErr)
		}
		if rmErr := os.Remove(temp.Name()); rmErr != nil {
			h.fs.errorf("materialize cleanup failed path=%s temp=%s err=%v", targetPath, temp.Name(), rmErr)
		}
		h.mu.Lock()
		h.temp = nil
		h.tempPath = ""
		h.mu.Unlock()
		return err
	}
	if entry.Size > 0 {
		if dlErr := h.fs.hub.DownloadFileContext(ctx, h.fs.project, targetPath, temp.Name()); dlErr != nil {
			if err := temp.Close(); err != nil {
				logging.Error(h.fs.log(), "failed to close temp file after download error", "path", temp.Name(), "err", err)
			}
			if err := os.Remove(temp.Name()); err != nil {
				logging.Error(h.fs.log(), "failed to remove temp file after download error", "path", temp.Name(), "err", err)
			}
			h.mu.Lock()
			h.temp = nil
			h.tempPath = ""
			h.mu.Unlock()
			return dlErr
		}
		h.mu.Lock()
		if _, seekErr := h.temp.Seek(0, 0); seekErr != nil {
			h.mu.Unlock()
			return seekErr
		}
		h.mu.Unlock()
	}
	return nil
}

func (h *storhubHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if writeState := h.writeState; writeState != nil {
		// Serialize with commits on opMu (always opMu before mu): commit
		// drops mu across its network window while mutating the plan,
		// and a read straddling that window would serve half-old,
		// half-new bytes.
		writeState.opMu.Lock()
		defer writeState.opMu.Unlock()
		writeState.mu.Lock()
		defer writeState.mu.Unlock()
		// Reading a poisoned overlay would serve zeros for ranges
		// whose bytes are in recovery/ - fail instead of lying.
		if writeState.poisoned {
			return nil, syscall.EIO
		}
		// Fill the kernel's own payload buffer directly: dest is the
		// request's outPayload, and go-fuse marshals ReadResultData from
		// the returned slice, so the old per-read copy was pure GC churn.
		n, err := writeState.readIntoLocked(ctx, dest, off)
		if err != nil {
			h.fs.errorf("read failed path=%s inode=%d off=%d len=%d err=%v", h.path, h.inode, off, len(dest), err)
			return nil, errnoFromError(err)
		}
		return fuse.ReadResultData(dest[:n]), 0
	}
	h.mu.Lock()
	temp := h.temp
	h.mu.Unlock()
	if temp != nil {
		n, err := temp.ReadAt(dest, off)
		if err != nil && !errors.Is(err, os.ErrClosed) && !errors.Is(err, io.EOF) {
			h.fs.errorf("read failed path=%s inode=%d off=%d len=%d err=%v", h.path, h.inode, off, len(dest), err)
			return nil, errnoFromError(err)
		}
		return fuse.ReadResultData(dest[:n]), 0
	}
	data, err := h.readFromPinned(ctx, off, int64(len(dest)))
	if err != nil {
		if errors.Is(err, io.EOF) {
			// A past-EOF read is an empty read, not an error;
			// errnoFromError would otherwise map io.EOF to EIO.
			return fuse.ReadResultData(nil), 0
		}
		// A failed read is an operational event users experience as EIO
		// with no other trace; without this line the backend cause was
		// invisible unless the mount ran at debug level.
		h.fs.errorf("read failed path=%s inode=%d off=%d len=%d err=%v", h.path, h.inode, off, len(dest), err)
		return nil, errnoFromError(err)
	}
	return fuse.ReadResultData(data), 0
}

// readFromPinned resolves bytes against the content layout captured at
// open time. The pinned file entry and chunk descriptors are immutable,
// and chunk assets are content-addressed, so every later read of this
// handle sees exactly the content that existed when it was opened -
// regardless of renames, replacements, or unlinks that happened in
// between. Zero network at pin time; asset ranges only on actual reads.
func (h *storhubHandle) readFromPinned(ctx context.Context, off, length int64) ([]byte, error) {
	h.mu.Lock()
	pin := h.pinned
	h.mu.Unlock()
	if pin == nil {
		return nil, syscall.EIO
	}
	return h.fs.hub.ReadPinnedFileContext(ctx, h.fs.project, &pin.file, pin.chunks, off, length)
}

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
	if h.writeState == nil {
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
	h.writeState.opMu.Lock()
	defer h.writeState.opMu.Unlock()
	h.writeState.mu.Lock()
	defer h.writeState.mu.Unlock()
	if h.writeState.poisoned {
		return syscall.EIO
	}
	if err := h.writeState.ensureTempLocked(); err != nil {
		return errnoFromError(err)
	}
	if err := reserveSpace(h.writeState.temp, mode, start, length); err != nil {
		return errnoFromError(err)
	}
	end := start + length
	if mode&fallocFlKeepSize == 0 && end > h.writeState.logicalSize {
		h.writeState.markDirtyLocked(h.writeState.logicalSize, end)
		h.writeState.logicalSize = end
	}
	h.fs.debugf("fallocate path=%s inode=%d off=%d size=%d mode=%#x", h.path, h.inode, start, length, mode)
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
// the later Release commit is a no-op when Flush already pushed.
func (h *storhubHandle) Flush(ctx context.Context) syscall.Errno {
	return h.commit(ctx)
}

func (h *storhubHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	_ = flags
	h.fs.debugf("fsync path=%s inode=%d", h.path, h.inode)
	return h.commit(ctx)
}

func (h *storhubHandle) Release(ctx context.Context) syscall.Errno {
	h.fs.debugf("release path=%s inode=%d", h.path, h.inode)
	errno := h.commit(ctx)
	h.releaseTrackedLocks()
	if errno != 0 {
		// The commit failed; the overlay temps hold the only copy of data
		// the application already wrote (Flush commits and propagates the
		// errno, but a successful close(2) may still have been reported
		// for earlier fsync-less writes). Preserve the overlay for manual
		// recovery instead of deleting it.
		h.quarantineTemps()
		if h.writeState != nil && h.fs.soleWriteStateRef(h.writeState) {
			h.writeState.quarantineTemps()
		}
	} else {
		h.closeTemp()
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
	if h.writeState != nil {
		h.fs.releaseWriteState(h.writeState)
		h.writeState = nil
	}
	_ = ctx
	return errno
}

// quarantineTemps moves this handle's temp snapshot into the recovery
// directory instead of deleting it. Used on commit failure.
func (h *storhubHandle) quarantineTemps() {
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

func (h *storhubHandle) closeTemp() {
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
