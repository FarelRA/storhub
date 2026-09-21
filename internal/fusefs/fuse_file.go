package fusefs

import (
	"context"
	"errors"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
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

// handlePath snapshots the handle's current path for logging. The path
// is rebased under h.mu by renames and unlinks racing any in-flight op
// (close-then-unlink hits Release against materializePath), so logging
// h.path directly is a data race. h.mu is always a leaf lock, safe to
// take on error and debug paths. h.inode never needs this: it is set
// once before the handle is published and never mutated.
func (h *storhubHandle) handlePath() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.path
}

// snapshotWriteState returns the handle's write state under h.mu. Release
// nils the pointer under the same lock, so every other reader must load it
// this way: a plain read races the Release write. h.mu is a leaf lock; the
// caller must release it before taking opMu or the state mutex.
func (h *storhubHandle) snapshotWriteState() *inodeWriteState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.writeState
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
	if n.fs.debugEnabled() {
		n.fs.debugOp("open start", "path", targetPath, "inode", n.inode, "flags", flags)
	}
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
	// Close the stat-then-pin window: a REST rename, replace, or unlink
	// landing between the stat and the pin changes which inode the path
	// names. When the pinned layout names a different inode than the
	// stat entry, retry the pair once; a persistent mismatch fails
	// ENOENT so the kernel retries the lookup instead of opening a
	// mixed identity (pre-race inode bookkeeping with post-race bytes).
	if file.Inode != entry.Inode {
		entryRetry, errRetry := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
		if errRetry != nil {
			return nil, 0, errnoFromError(errRetry)
		}
		repoRetry, _, metaRetryErr := n.fs.hub.LoadRepoMetadataReadonlyContext(ctx, n.fs.project)
		if metaRetryErr != nil {
			return nil, 0, errnoFromError(metaRetryErr)
		}
		fileRetry := repoRetry.FindFile(targetPath)
		if fileRetry == nil {
			return nil, 0, syscall.ENOENT
		}
		if fileRetry.Inode != entryRetry.Inode {
			return nil, 0, syscall.ENOENT
		}
		entry = entryRetry
		repoMeta = repoRetry
		file = fileRetry
	}
	// The mount runs with NullPermissions, so the kernel enforces no
	// DAC at all - this server is the only gate. A write-open must carry
	// write permission on the file, or any user could overwrite any file
	// through the overlay commit path. Requests without a kernel caller
	// identity are direct library use (the local process on its own
	// repository) and are not multi-user surfaces.
	if shfs.IdentityPresent(ctx) {
		// Open-ONLY read gate: any open that is not write-only must carry
		// read permission on the file. O_PATH carries no access mode
		// (Linux 0o10000000; no portable const, darwin lacks it) and is
		// exempt: the kernel never forwards O_PATH opens anyway, so this
		// is handler hygiene for a latent path, with no observable
		// change today. Reads through the handle stay fast
		// (no per-Read re-check): the open-time pin below captures the
		// content layout, and the commit-time write DAC still guards the
		// overlay path independently.
		// oPathFlag is Linux O_PATH. There is no portable const (darwin
		// lacks O_PATH entirely); the kernel never forwards such opens,
		// so the literal only feeds the exemption below.
		const oPathFlag = 0o10000000
		if flags&syscall.O_ACCMODE != syscall.O_WRONLY && flags&oPathFlag == 0 {
			if err := shfs.CheckReadAccess(ctx, repoMeta, targetPath); err != nil {
				if n.fs.debugEnabled() {
					n.fs.debugOp("open denied", "path", targetPath, "err", err)
				}
				return nil, 0, errnoFromError(err)
			}
		}
		if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND) != 0 {
			if err := shfs.CheckWriteAccess(ctx, repoMeta, targetPath); err != nil {
				if n.fs.debugEnabled() {
					n.fs.debugOp("open denied", "path", targetPath, "err", err)
				}
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
	if n.fs.debugEnabled() {
		n.fs.debugOp("open complete", "path", targetPath, "inode", n.inode, "flags", flags)
	}
	return h, 0, 0
}

func (n *storhubNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofusefs.Inode, gofusefs.FileHandle, uint32, syscall.Errno) {
	ctx = shfs.WithCreateMode(n.fs.callerContext(ctx), mode)
	parentPath, stale := n.safePath()
	if stale != 0 {
		return nil, nil, 0, stale
	}
	childPath := path.Join(parentPath, name)
	if n.fs.debugEnabled() {
		n.fs.debugOp("create start", "path", childPath, "flags", flags, "mode", mode)
	}
	file, err := n.fs.hub.CreateFileContext(ctx, n.fs.project, childPath)
	if err != nil {
		if n.fs.debugEnabled() {
			n.fs.debugOp("create failed", "path", childPath, "err", err)
		}
		return nil, nil, 0, errnoFromError(err)
	}
	nlink := n.fs.nlinkForEntry(ctx, childPath)
	entry := shfs.EntryFromFile(file, childPath, nlink)
	ino := n.attachEntry(ctx, entry, out)
	h, err := n.fs.newHandle(ctx, entry.Inode, childPath, flags, &writeBootstrap{baseSize: entry.Size})
	if err != nil {
		if n.fs.debugEnabled() {
			n.fs.debugOp("create failed", "path", childPath, "err", err)
		}
		// The empty file is already committed remotely; leaving it
		// behind would orphan an entry the application was told was never
		// created. Roll it back before reporting the failure.
		if unlinkErr := n.fs.hub.UnlinkContext(ctx, n.fs.project, childPath); unlinkErr != nil {
			n.fs.errorOp("create rollback failed", "path", childPath, "err", unlinkErr, "original", err)
		}
		n.fs.notifyEntryForPath(parentPath, name)
		return nil, nil, 0, errnoFromError(err)
	}
	// The kernel may hold a negative entry for this name (NegativeTimeout);
	// the create must evict it or the file stays invisible until expiry.
	n.fs.notifyEntryForPath(parentPath, name)
	if n.fs.debugEnabled() {
		n.fs.debugOp("create complete", "path", childPath, "inode", entry.Inode, "flags", flags, "mode", mode)
	}
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
		// The handle is already published in s.handles above, so
		// scanners (detachedBase, rebind) may load the pointer
		// concurrently: store it under h.mu.
		h.mu.Lock()
		h.writeState = writeState
		h.mu.Unlock()
		if flags&syscall.O_TRUNC != 0 && bootstrap == nil {
			// Serialize with in-flight commits and writes on opMu: the
			// truncate must land wholly before or after them, never in
			// the middle of a commit that already captured its plan.
			// Lock order is always opMu before mu.
			writeState.opMu.Lock()
			writeState.mu.Lock()
			if err := writeState.setSizeLocked(0); err != nil {
				writeState.mu.Unlock()
				unlockOpMu(&writeState.opMu)
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
			unlockOpMu(&writeState.opMu)
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
	temp, err := h.fs.newOverlayTemp("handle*")
	if err != nil {
		h.mu.Unlock()
		return err
	}
	h.temp = temp
	h.tempPath = temp.Name()
	h.path = targetPath
	h.mu.Unlock()

	// Network calls without lock. The fd may close under us while they
	// run: RELEASE is not synchronized with close, so a racing Release
	// can close (and remove) our temp before we Seek it — historically
	// surfacing as "file already closed" EIO on the concurrent unlink.
	// Every exit below re-checks under h.mu and abandons the snapshot
	// when the handle was released: no future read needs it, and the
	// unlink must proceed.
	entry, err := h.fs.hub.StatPathContext(ctx, h.fs.project, targetPath)
	if err != nil {
		// Propagate: silently treating stat failure as "empty file"
		// would serve EOF for a readable handle whose remote stat
		// merely hiccupped (same contract as the writeState twin).
		// A handle released anywhere along the way (including the
		// cleanup below) abandons instead: no future read needs the
		// snapshot, and the unlink must proceed.
		if h.abandonMaterialize(temp) {
			return nil
		}
		if rmErr := temp.Close(); rmErr != nil {
			h.fs.errorOp("materialize cleanup failed", "path", targetPath, "temp", temp.Name(), "err", rmErr)
		}
		if rmErr := os.Remove(temp.Name()); rmErr != nil {
			h.fs.errorOp("materialize cleanup failed", "path", targetPath, "temp", temp.Name(), "err", rmErr)
		}
		h.mu.Lock()
		if h.temp == temp {
			h.temp = nil
			h.tempPath = ""
		}
		released := h.closed
		h.mu.Unlock()
		if released {
			return nil
		}
		return err
	}
	if entry.Size > 0 {
		if dlErr := h.fs.hub.DownloadFileContext(ctx, h.fs.project, targetPath, temp.Name()); dlErr != nil {
			if h.abandonMaterialize(temp) {
				return nil
			}
			if err := temp.Close(); err != nil {
				logging.Error(h.fs.log(), "failed to close temp file after download error", "path", temp.Name(), "err", err)
			}
			if err := os.Remove(temp.Name()); err != nil {
				logging.Error(h.fs.log(), "failed to remove temp file after download error", "path", temp.Name(), "err", err)
			}
			h.mu.Lock()
			if h.temp == temp {
				h.temp = nil
				h.tempPath = ""
			}
			released := h.closed
			h.mu.Unlock()
			if released {
				return nil
			}
			return dlErr
		}
		h.mu.Lock()
		if h.abandonMaterializeLocked(temp) {
			h.mu.Unlock()
			return nil
		}
		if _, seekErr := h.temp.Seek(0, 0); seekErr != nil {
			h.mu.Unlock()
			return seekErr
		}
		h.mu.Unlock()
	}
	return nil
}

// abandonMaterialize drops a snapshot temp fetched for a handle that was
// released concurrently, reporting whether the caller must abandon its
// snapshot (true) or proceed (false). Callers must not hold h.mu.
func (h *storhubHandle) abandonMaterialize(temp *os.File) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.abandonMaterializeLocked(temp)
}

// abandonMaterializeLocked is abandonMaterialize with h.mu held. When the
// handle was released (or its temp replaced) out from under a snapshot
// fetch, it detaches our temp so the caller can close and remove it, and
// reports true. Close and remove run inline: h.mu is a leaf lock, the
// same discipline as closeTemp.
func (h *storhubHandle) abandonMaterializeLocked(temp *os.File) bool {
	if !h.closed && h.temp == temp {
		return false
	}
	name := temp.Name()
	if h.temp == temp {
		h.temp = nil
		h.tempPath = ""
	}
	_ = temp.Close()
	_ = os.Remove(name)
	return true
}

func (h *storhubHandle) Read(ctx context.Context, dest []byte, off int64) (result fuse.ReadResult, errno syscall.Errno) {
	started := time.Now()
	if h.fs.debugEnabled() {
		h.fs.debugOp("read start", "path", h.handlePath(), "inode", h.inode, "off", off, "size", len(dest))
	}
	// Success is logged once here; every failure path below already logs
	// at Error with path, inode, offset, and cause.
	defer func() {
		if errno == 0 {
			if h.fs.debugEnabled() {
				h.fs.debugOp("read complete", "path", h.handlePath(), "inode", h.inode, "off", off, "elapsed", time.Since(started))
			}
		}
	}()
	if writeState := h.snapshotWriteState(); writeState != nil {
		// Serialize with commits on opMu (always opMu before mu): commit
		// drops mu across its network window while mutating the plan,
		// and a read straddling that window would serve half-old,
		// half-new bytes.
		writeState.opMu.Lock()
		defer unlockOpMu(&writeState.opMu)
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
			h.fs.errorOp("read failed", "path", h.handlePath(), "inode", h.inode, "off", off, "len", len(dest), "err", err)
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
			h.fs.errorOp("read failed", "path", h.handlePath(), "inode", h.inode, "off", off, "len", len(dest), "err", err)
			return nil, errnoFromError(err)
		}
		return fuse.ReadResultData(dest[:n]), 0
	}
	// A read-only handle shares the inode's page cache with writers:
	// when another handle holds this inode's live writeState, ranges it
	// dirtied must come from the overlay temp and only clean spans from
	// the pinned snapshot below. Uncontended reads (no live state, or a
	// state with nothing uncommitted) fall through untouched.
	if result, errno, served := h.readLiveOverlay(ctx, dest, off); served {
		return result, errno
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
		h.fs.errorOp("read failed", "path", h.handlePath(), "inode", h.inode, "off", off, "len", len(dest), "err", err)
		return nil, errnoFromError(err)
	}
	return fuse.ReadResultData(data), 0
}

// readLiveOverlay serves a handle without its own writeState from another
// handle's live overlay for the same inode: dirty spans come from the
// overlay temp, clean spans from the pinned path exactly as before.
// Locking: takes only the short state mutex, never opMu. A commit holds
// opMu across its whole network window, so readers serializing on it
// would hang for minutes; writers and commit-plan changes both need the
// state mutex, so the dirty-range plus logical-size snapshot and the temp
// preads under it are atomic against concurrent writes and plan mutation.
// Commits only read the temp during their network phase, so this is safe.
// Returns served=false when no live state exists or it holds nothing
// uncommitted, in which case the caller falls back to the pinned path
// with zero behavior change. A poisoned state fails closed with EIO,
// matching the owned-handle read; a closed or temp-less state falls back
// to pinned.
func (h *storhubHandle) readLiveOverlay(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno, bool) {
	if off < 0 {
		return nil, 0, false
	}
	state := h.fs.writeStateForInode(h.inode)
	if state == nil {
		return nil, 0, false
	}
	state.mu.Lock()
	if state.poisoned {
		state.mu.Unlock()
		return nil, syscall.EIO, true
	}
	if state.closed || state.temp == nil {
		state.mu.Unlock()
		return nil, 0, false
	}
	logical := state.logicalSize
	if len(state.dirtyRanges) == 0 && logical == state.baseSize {
		state.mu.Unlock()
		return nil, 0, false
	}
	if off >= logical || len(dest) == 0 {
		state.mu.Unlock()
		return fuse.ReadResultData(nil), 0, true
	}
	limit := int64(len(dest))
	if capN := logical - off; limit > capN {
		limit = capN
	}
	dirty := append([]ByteRange(nil), state.dirtyRanges...)
	temp := state.temp
	type span struct{ start, end int64 }
	var clean []span
	pos := off
	for _, r := range dirty {
		if r.End <= off || r.Start >= off+limit {
			continue
		}
		s, e := r.Start, r.End
		if s < off {
			s = off
		}
		if e > off+limit {
			e = off + limit
		}
		if s > pos {
			clean = append(clean, span{pos, s})
		}
		// Local pread under the state mutex only: microsecond I/O,
		// fully serial against writers and plan mutation.
		n, err := temp.ReadAt(dest[pos-off:e-off], pos)
		if err != nil && !errors.Is(err, io.EOF) {
			state.mu.Unlock()
			h.fs.errorOp("read failed", "path", h.handlePath(), "inode", h.inode, "off", off, "len", len(dest), "err", err)
			return nil, errnoFromError(err), true
		}
		for i := int64(n); i < e-pos; i++ {
			dest[pos-off+i] = 0
		}
		pos = e
	}
	if pos < off+limit {
		clean = append(clean, span{pos, off + limit})
	}
	state.mu.Unlock()
	for _, g := range clean {
		data, err := h.readFromPinned(ctx, g.start, g.end-g.start)
		if err != nil {
			if errors.Is(err, io.EOF) {
				for i := g.start; i < g.end; i++ {
					dest[i-off] = 0
				}
				continue
			}
			h.fs.errorOp("read failed", "path", h.handlePath(), "inode", h.inode, "off", off, "len", len(dest), "err", err)
			return nil, errnoFromError(err), true
		}
		copy(dest[g.start-off:], data)
		for i := g.start + int64(len(data)); i < g.end; i++ {
			dest[i-off] = 0
		}
	}
	return fuse.ReadResultData(dest[:limit]), 0, true
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
