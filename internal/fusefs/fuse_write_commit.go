package fusefs

import (
	"context"
	"os"
	"path"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func (h *storhubHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	// Load-then-use: Release nils the pointer under h.mu, so snapshot
	// it once here and use only the local below.
	writeState := h.snapshotWriteState()
	if writeState == nil {
		// No write state exists for this handle: only read-oriented
		// materialized snapshots (e.g. displaced handles) land here. The
		// kernel never routes WRITE to such handles under enforced mode
		// flags, and historically this branch wrote into the snapshot temp
		// without dirty tracking - the data was acknowledged but silently
		// discarded at commit. Fail loudly instead of pretending the write
		// landed.
		h.fs.debugOp("write rejected", "path", h.handlePath(), "inode", h.inode)
		return 0, syscall.EIO
	}
	h.fs.debugOp("write start", "path", h.handlePath(), "inode", h.inode, "off", off, "size", len(data))
	writeState.opMu.Lock()
	writeState.mu.Lock()
	// A quarantined (poisoned) overlay no longer holds the bytes its
	// dirty ranges claimed. Accepting new writes would resurrect an empty
	// temp and commit zeros over remote data.
	if writeState.poisoned {
		writeState.mu.Unlock()
		unlockOpMu(&writeState.opMu)
		return 0, syscall.EIO
	}
	if h.flags&syscall.O_APPEND != 0 && off >= writeState.logicalSize {
		// Append at the overlay EOF. When off points inside the file the
		// kernel sent a merged writeback covering already-acknowledged
		// bytes plus the new tail (writeback dirties whole pages, so one
		// FUSE_WRITE can replay the prefix the server already acked):
		// honoring its offset rewrites the prefix identically and lands
		// the tail once, while forcing it to EOF duplicates the prefix
		// (observed kernel 7 vs server 11, reads truncated to garbage).
		off = writeState.logicalSize
	}
	if writeState.temp == nil {
		if err := writeState.ensureTempLocked(); err != nil {
			errno := errnoFromError(err)
			writeState.mu.Unlock()
			unlockOpMu(&writeState.opMu)
			return 0, errno
		}
	}
	if off > writeState.logicalSize {
		writeState.markDirtyLocked(writeState.logicalSize, off)
		if err := writeState.temp.Truncate(off); err != nil {
			errno := errnoFromError(err)
			writeState.mu.Unlock()
			unlockOpMu(&writeState.opMu)
			return 0, errno
		}
		writeState.logicalSize = off
	}
	n, err := writeState.temp.WriteAt(data, off)
	if err != nil {
		errno := errnoFromError(err)
		writeState.mu.Unlock()
		unlockOpMu(&writeState.opMu)
		return uint32(n), errno
	}
	end := off + int64(n)
	if end > writeState.logicalSize {
		writeState.logicalSize = end
		if err := writeState.temp.Truncate(end); err != nil {
			errno := errnoFromError(err)
			writeState.mu.Unlock()
			unlockOpMu(&writeState.opMu)
			return uint32(n), errno
		}
	}
	writeState.markDirtyLocked(off, end)
	// Bound the disjoint-range count: pathological scatter patterns
	// collapse into one authoritative span (with base backfill) instead
	// of growing the slice until commit.
	if err := writeState.ensureDirtyBounded(ctx); err != nil {
		errno := errnoFromError(err)
		writeState.mu.Unlock()
		unlockOpMu(&writeState.opMu)
		return uint32(n), errno
	}
	// POSIX privilege clearing is overlay-immediate, not commit-deferred:
	// a non-admin data write stages the cleared mode into pending now, so
	// pre-commit stat and exec already observe it. The commit-time hub
	// verbs sanitize again; this staging must never resurrect bits, only
	// clear them (see stagePrivClearLocked).
	h.fs.stagePrivClearForDataWrite(h.fs.callerContext(ctx), writeState, writeState.path)
	syncWrite := h.flags&syncWriteFlags != 0
	h.fs.debugOp("write complete", "path", writeState.path, "inode", h.inode, "off", off, "bytes", n)
	writeState.mu.Unlock()
	unlockOpMu(&writeState.opMu)
	if !syncWrite {
		// Buffered path: durability waits for Flush/Fsync/Release, so
		// this write pays zero added latency (no drain here).
		return uint32(n), 0
	}
	// O_SYNC (decision: HONOR): commit plus drain synchronously per write
	// before acknowledging, so the bytes are remote-durable on return.
	// Cost is the point: correct but slow. O_DSYNC is treated identically
	// (on linux; see syncWriteFlags for other platforms) because our
	// commit granularity cannot distinguish data from metadata durability:
	// every commit pushes data plus size plus the pending metadata patch
	// as one unit, so a data-only barrier is still a full commit plus
	// drain. On failure report 0/EIO (the staged bytes stay dirty for a
	// later fsync or close to retry); a drain-only failure publishes
	// without quarantining (see drainProject).
	if errno := h.commitAndDrain(ctx); errno != 0 {
		h.fs.errorOp("write failed", "path", h.handlePath(), "inode", h.inode, "off", off, "errno", errno)
		return 0, errno
	}
	return uint32(n), 0
}

func (h *storhubHandle) commit(ctx context.Context) syscall.Errno {
	// Load-then-use: Release nils the pointer under h.mu, so snapshot
	// it once here and thread ws through every helper below. h.mu is
	// released before opMu/mu are taken, preserving the leaf order.
	ws := h.snapshotWriteState()
	if ws == nil {
		// Read-only or detached handle: provably nothing to commit.
		return 0
	}
	// Flush/Fsync/Release arrive with the kernel caller's context; bind
	// it to an fs identity so the commit-time DAC re-check below sees the
	// real uid.
	ctx = h.fs.callerContext(ctx)
	h.mu.Lock()
	handlePath := h.path
	h.mu.Unlock()
	ws.opMu.Lock()
	ws.mu.Lock()
	if ws.poisoned {
		ws.mu.Unlock()
		unlockOpMu(&ws.opMu)
		// The overlay was quarantined; committing would upload zeros.
		return syscall.EIO
	}
	if len(ws.dirtyRanges) == 0 && ws.logicalSize == ws.baseSize && !ws.hasPendingMetadataLocked() {
		ws.mu.Unlock()
		unlockOpMu(&ws.opMu)
		return 0
	}
	if ws.deleted || handlePath == "" {
		ws.mu.Unlock()
		unlockOpMu(&ws.opMu)
		// POSIX unlinked-open-handle semantics: writes via an open fd
		// succeed and reads are served from the temp overlay; the data
		// is discarded at Release (link count zero). Pinned by
		// TestFUSEHandleRenameAndUnlinkSemantics — do NOT return an
		// error here. Note: the emptiness test is exact, not
		// TrimSpace-based: a file legitimately named " " must still
		// commit its writes.
		return 0
	}
	// Re-check write DAC at commit time under the *flushing*
	// caller's identity. The bytes only reach the remote at this point,
	// and the identity here may differ from the one that opened the
	// handle (Flush/Fsync/Release carry their own kernel caller). opMu
	// stays held across the metadata load; only mu is released.
	ws.mu.Unlock()
	errno := h.checkCommitWriteAccess(ctx, handlePath)
	ws.mu.Lock()
	if errno != 0 {
		ws.mu.Unlock()
		unlockOpMu(&ws.opMu)
		return errno
	}
	// Re-validate under the lock: the DAC check released it.
	if ws.deleted || ws.poisoned {
		ws.mu.Unlock()
		unlockOpMu(&ws.opMu)
		if ws.poisoned {
			return syscall.EIO
		}
		return 0
	}
	// Re-read the authoritative path under lock after the DAC window:
	// a concurrent rename rebind (now serialized on opMu, but possibly
	// landing before opMu acquisition or racing the pre-lock capture)
	// may have moved the handle. Fail closed on mismatch so bytes never
	// land at the pre-rename name and DAC is never evaluated for the
	// wrong path. The overlay stays dirty for retry at the new name.
	h.mu.Lock()
	curHandlePath := h.path
	curHandleDetached := h.deleted || curHandlePath == ""
	h.mu.Unlock()
	curStatePath := ws.path
	curStateDetached := ws.deleted || curStatePath == ""
	if curHandleDetached || curStateDetached {
		ws.mu.Unlock()
		unlockOpMu(&ws.opMu)
		return 0
	}
	if curHandlePath != handlePath || curStatePath != handlePath {
		ws.mu.Unlock()
		unlockOpMu(&ws.opMu)
		return syscall.ENOENT
	}
	targetPath := handlePath
	baseSize := ws.baseSize
	logicalSize := ws.logicalSize
	pending := ws.pending
	// Collect notification intents while opMu/mu are held; emit only
	// after both are released (see commitNotifies). Every success return
	// below records its intents before returning, so none are dropped;
	// failure returns record nothing and emit nothing.
	var notifies commitNotifies
	errno = h.commitTemp(ctx, targetPath, baseSize, logicalSize, pending, &notifies)
	unlockOpMu(&ws.opMu)
	if errno == 0 {
		notifies.emit(h.fs)
	}
	return errno
}

// drainProject blocks until everything published before the call lands in
// the remote commit. It runs only on the durability paths (Flush, Fsync,
// Release, O_SYNC writes); normal buffered writes never drain, so they
// pay zero added latency.
func (h *storhubHandle) drainProject(ctx context.Context) syscall.Errno {
	// On drain failure return EIO WITHOUT quarantining the overlay: the
	// bytes are already uploaded and published, and the journal retains
	// the dirty state for retry. Quarantining here would double-replay
	// the same bytes via redrive plus the quarantined overlay.
	if err := h.fs.hub.DrainProjectContext(ctx, h.fs.project); err != nil {
		h.fs.errorOp("drain failed", "path", h.handlePath(), "inode", h.inode, "err", err)
		return syscall.EIO
	}
	return 0
}

// commitAndDrain pushes the overlay (commit) and then waits for remote
// durability (drain). A commit failure returns its errno and leaves the
// overlay dirty for the caller's quarantine decision; a drain failure
// returns EIO with the overlay already published, so the caller must not
// quarantine (see drainProject).
func (h *storhubHandle) commitAndDrain(ctx context.Context) syscall.Errno {
	if errno := h.commit(ctx); errno != 0 {
		return errno
	}
	return h.drainProject(ctx)
}

// checkCommitWriteAccess enforces the file-level write DAC against the
// live readonly metadata view before any commit verb dispatches.
// Requests without a kernel caller identity are direct library use and
// are governed by the storage layer's own checks; a file that is no
// longer in the view is left to the backend's not-found handling.
func (h *storhubHandle) checkCommitWriteAccess(ctx context.Context, targetPath string) syscall.Errno {
	if !shfs.IdentityPresent(ctx) {
		return 0
	}
	repoMeta, _, err := h.fs.hub.LoadRepoMetadataReadonlyContext(ctx, h.fs.project)
	if err != nil {
		return errnoFromError(err)
	}
	if repoMeta.FindFile(targetPath) == nil {
		return 0
	}
	if err := shfs.CheckWriteAccess(ctx, repoMeta, targetPath); err != nil {
		h.fs.debugOp("commit denied", "path", targetPath, "inode", h.inode, "err", err)
		return errnoFromError(err)
	}
	return 0
}

// commitTemp handles all temp-based commit paths (truncate, chunk-rewrite, replace, patch).
// Caller must hold ws.mu. Releases and re-acquires ws.mu as needed.
// ws is snapshotted under h.mu at entry: helpers must use the snapshot,
// never reload h.writeState mid-frame.
//
// Crash-ordering contract (and its limit): within one commit, data lands
// before the size reconcile, and the size reconcile lands before the
// metadata patch, so a crash can never leave metadata pointing at data
// that never arrived. Ranges stay dirty until the whole pair succeeds, so
// a retry replays instead of resuming mid-step. What this is NOT is a
// single backend transaction: patch+truncate+metadata are separate backend
// operations, and GitHub offers no multi-op commit primitive to fuse them
// (full C4 atomicity is structurally impossible client-side, not merely
// unimplemented). The degradation envelope is verified, not assumed:
// a crash between steps leaves the prior valid state readable, orphaned
// assets are purge-reclaimable, retries are idempotent, failed commits
// quarantine with intent sidecars, and startup auto-redrive (C11: full
// images replace, recorded spans patch, bare size changes truncate, all
// under the target fingerprint CAS) recommits what the next mount can
// prove untouched. Anything the CAS cannot prove stays quarantined for
// manual recovery.
func (h *storhubHandle) commitTemp(ctx context.Context, targetPath string, baseSize, logicalSize int64, pending shfs.MetadataPatch, notifies *commitNotifies) syscall.Errno {
	// Load-then-use under h.mu: Release nils the pointer concurrently.
	// commit holds opMu across this whole frame and Release nils only
	// after its own commit, so the snapshot cannot go nil mid-frame.
	ws := h.snapshotWriteState()
	if ws == nil {
		return syscall.EIO
	}
	if len(ws.dirtyRanges) == 0 {
		ws.mu.Unlock()
		if logicalSize != baseSize {
			h.fs.debugOp("commit truncate", "path", targetPath, "inode", h.inode, "size", logicalSize)
			if _, err := h.fs.hub.TruncateFileContext(ctx, h.fs.project, targetPath, logicalSize); err != nil {
				h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
				return errnoFromError(err)
			}
		}
		if errno := h.applyMetadataPatch(ctx, targetPath, pending); errno != 0 {
			return errno
		}
		ws.mu.Lock()
		ws.commitCacheRefreshLocked(logicalSize)
		ws.mu.Unlock()
		// Deferred: opMu is held by commit(); the emission runs after
		// its release (see commitNotifies).
		notifies.addContent(h.inode)
		ws.mu.Lock()
		ws.pending = shfs.MetadataPatch{}
		ws.mu.Unlock()
		h.fs.dropPinnedForPath(targetPath)
		notifies.addEntry(shfs.ParentPath(targetPath), path.Base(targetPath))
		return 0
	}
	planned := ws.plannedRangesLocked()
	// Rung order: chunk-rewrite (rebuild the touched chunks wholesale) is
	// preferred when fragmentation is high but the affected byte volume is
	// modest; full replace when the ladder in shouldReplaceLocked says
	// ranged work cannot pay; otherwise patch each dirty range in place.
	if ws.shouldChunkRewriteLocked(planned) {
		return h.commitChunkRewrite(ctx, targetPath, logicalSize, planned, pending, notifies)
	}
	if ws.shouldReplaceLocked(planned) {
		return h.commitReplace(ctx, targetPath, logicalSize, planned, pending, notifies)
	}
	return h.commitPatch(ctx, targetPath, baseSize, logicalSize, ws.dirtyRanges, pending, notifies)
}

// commitChunkRewrite handles the chunk-rewrite path.
// Caller must hold ws.mu. Releases and re-acquires ws.mu.
func (h *storhubHandle) commitChunkRewrite(ctx context.Context, targetPath string, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch, notifies *commitNotifies) syscall.Errno {
	// Load-then-use under h.mu; see commitTemp for why this cannot go
	// nil mid-frame.
	ws := h.snapshotWriteState()
	if ws == nil {
		return syscall.EIO
	}
	snapshotPath, err := ws.createRangeSnapshotLocked(ctx, planned)
	baseSize := ws.baseSize
	ws.mu.Unlock()
	if err != nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
		return errnoFromError(err)
	}
	// The snapshot is event-scoped: this commit frame owns it and removes
	// it on every exit path.
	defer func() { _ = os.Remove(snapshotPath) }()
	h.fs.debugOp("commit chunk-rewrite", "path", targetPath, "inode", h.inode, "base", baseSize, "size", logicalSize, "ranges", len(planned))
	repoMeta, _, err := h.fs.hub.LoadRepoMetadataReadonlyContext(ctx, h.fs.project)
	if err != nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
		return errnoFromError(err)
	}
	fileMeta := repoMeta.FindFile(targetPath)
	if fileMeta == nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode)
		return syscall.ENOENT
	}
	if _, err := h.fs.hub.RewriteFileRangesWithMetadataContext(ctx, h.fs.project, targetPath, snapshotPath, repoMeta, fileMeta, logicalSize, planned); err != nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
		return errnoFromError(err)
	}
	ws.mu.Lock()
	ws.commitCacheRefreshLocked(logicalSize)
	ws.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending, notifies)
}

// commitReplace handles the full-file replace path.
// Caller must hold ws.mu. Releases and re-acquires ws.mu.
func (h *storhubHandle) commitReplace(ctx context.Context, targetPath string, logicalSize int64, _ []ByteRange, pending shfs.MetadataPatch, notifies *commitNotifies) syscall.Errno {
	// Load-then-use under h.mu; see commitTemp for why this cannot go
	// nil mid-frame.
	ws := h.snapshotWriteState()
	if ws == nil {
		return syscall.EIO
	}
	snapshotPath, cleanupSnapshot, err := ws.replaceInputPathLocked(ctx)
	baseSize := ws.baseSize
	dirtyCount := len(ws.dirtyRanges)
	ws.mu.Unlock()
	if err != nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
		return errnoFromError(err)
	}
	if cleanupSnapshot {
		// Event-scoped snapshot: this commit frame owns the removal.
		defer func() { _ = os.Remove(snapshotPath) }()
	}
	h.fs.debugOp("commit replace", "path", targetPath, "inode", h.inode, "base", baseSize, "size", logicalSize, "dirty_ranges", dirtyCount)
	if _, err := h.fs.hub.ReplaceFileContext(ctx, h.fs.project, targetPath, snapshotPath); err != nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
		return errnoFromError(err)
	}
	ws.mu.Lock()
	ws.commitCacheRefreshLocked(logicalSize)
	ws.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending, notifies)
}

// removeDirtyRangeLocked drops [start,end) from the dirty set after a range
// was successfully committed, so a retry only re-applies the remainder.
// Caller must hold w.mu.
func (w *inodeWriteState) removeDirtyRangeLocked(start, end int64) {
	kept := w.dirtyRanges[:0]
	for _, r := range w.dirtyRanges {
		if r.End <= start || r.Start >= end {
			kept = append(kept, r)
			continue
		}
		if r.Start < start {
			kept = append(kept, ByteRange{Start: r.Start, End: start})
		}
		if r.End > end {
			kept = append(kept, ByteRange{Start: end, End: r.End})
		}
	}
	w.dirtyRanges = kept
}

// commitPatch handles the partial patch path.
// Caller must hold ws.mu. Releases and re-acquires ws.mu.
func (h *storhubHandle) commitPatch(ctx context.Context, targetPath string, baseSize, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch, notifies *commitNotifies) syscall.Errno {
	// Load-then-use under h.mu; see commitTemp for why this cannot go
	// nil mid-frame.
	ws := h.snapshotWriteState()
	if ws == nil {
		return syscall.EIO
	}
	edits := make([]shfs.RangeEdit, 0, len(planned))
	for _, dirty := range planned {
		buf := make([]byte, dirty.End-dirty.Start)
		n, err := ws.readIntoLocked(ctx, buf, dirty.Start)
		if err != nil {
			ws.mu.Unlock()
			return errnoFromError(err)
		}
		// A short read is legal only when the missing tail lies past
		// end-of-file, where POSIX holes read as zeros. Anything else is
		// internal inconsistency: refuse to upload fabricated bytes.
		if n < len(buf) && dirty.Start+int64(n) < logicalSize {
			ws.mu.Unlock()
			h.fs.errorOp("commit aborted", "path", targetPath, "inode", h.inode, "step", "patch-read", "start", dirty.Start, "end", dirty.End, "got", n, "want", len(buf))
			return syscall.EIO
		}
		for j := n; j < len(buf); j++ {
			buf[j] = 0
		}
		deleteSize := dirty.End - dirty.Start
		if dirty.Start >= baseSize {
			deleteSize = 0
		} else if maxDelete := baseSize - dirty.Start; deleteSize > maxDelete {
			deleteSize = maxDelete
		}
		edits = append(edits, shfs.RangeEdit{Start: dirty.Start, DeleteSize: deleteSize, Data: buf})
	}
	ws.mu.Unlock()

	// One batched operation for the whole commit: a single release
	// resolution and playlist rebuild instead of one round-trip chain per
	// range. Ascending order needs no intermediate offset fixups. Either
	// the whole batch commits or none of it does, so failures leave every
	// range dirty and the retry replays the identical batch.
	h.fs.debugOp("commit patch", "path", targetPath, "inode", h.inode, "base", baseSize, "size", logicalSize, "ranges", len(planned))
	if _, err := h.fs.hub.PatchFileRangesContext(ctx, h.fs.project, targetPath, edits); err != nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
		return errnoFromError(err)
	}
	// Crash ordering: the commit is not done until the size is
	// reconciled. Consume the dirty ranges only after the post-patch
	// truncate succeeds, so a truncate failure leaves the full patch
	// replayable instead of half-applied. Replayed edits are idempotent
	// (each replaces the same span with the same bytes).
	if logicalSize != baseSize {
		appendOnly := len(planned) == 1 && planned[0].Start >= baseSize && logicalSize == planned[0].End
		if !appendOnly {
			if _, err := h.fs.hub.TruncateFileContext(ctx, h.fs.project, targetPath, logicalSize); err != nil {
				h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
				return errnoFromError(err)
			}
		}
	}
	// Mark all applied ranges consumed so a retry resumes instead of
	// re-applying committed edits (which would duplicate bytes).
	ws.mu.Lock()
	// The network window above released mu; a concurrent quarantine
	// (Close teardown) may have taken the temp with it. Fail the commit
	// keeping the ranges dirty instead of dereferencing a nil *os.File -
	// the remote patch already landed, and the replay is idempotent.
	if ws.temp == nil || ws.poisoned {
		ws.mu.Unlock()
		h.fs.errorOp("commit interrupted by quarantine", "path", targetPath, "inode", h.inode)
		return syscall.EIO
	}
	for _, dirty := range planned {
		ws.removeDirtyRangeLocked(dirty.Start, dirty.End)
	}
	ws.mu.Unlock()
	ws.mu.Lock()
	if err := ws.refreshBaseSnapshotLocked(); err != nil {
		h.fs.debugOp("commit cache refresh failed", "path", targetPath, "inode", h.inode, "err", err)
		ws.clearBaseSnapshotLocked()
	}
	ws.baseSize = logicalSize
	ws.dirtyRanges = nil
	ws.tempAuthoritative = false
	if err := ws.temp.Truncate(logicalSize); err != nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
		ws.mu.Unlock()
		return errnoFromError(err)
	}
	// Deferred: opMu is held by commit(); the emission runs after its
	// release (see commitNotifies). The post-update below appends its own
	// content+entry pair, so a patch commit emits two content intents;
	// that redundancy is acceptable and coalesces in flight.
	notifies.addContent(h.inode)
	ws.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending, notifies)
}

// commitCacheRefreshLocked updates the cached write state after a successful remote write.
// Caller must hold w.mu.
func (w *inodeWriteState) commitCacheRefreshLocked(logicalSize int64) {
	if err := w.refreshBaseSnapshotLocked(); err != nil {
		w.clearBaseSnapshotLocked()
	}
	w.baseSize = logicalSize
	w.dirtyRanges = nil
	w.tempAuthoritative = false
}

// commitPostUpdate applies pending metadata, clears it, evicts the shared
// pin for the path (the version changed, so open handles re-pin on next
// open), and records notifications.
// Caller must NOT hold ws.mu. Notifications are only recorded
// into notifies; the committer emits them after releasing opMu (see
// commitNotifies). Every success return records exactly one content plus
// one entry intent, so no invalidation is dropped on this path.
func (h *storhubHandle) commitPostUpdate(ctx context.Context, targetPath string, pending shfs.MetadataPatch, notifies *commitNotifies) syscall.Errno {
	if errno := h.applyMetadataPatch(ctx, targetPath, pending); errno != 0 {
		return errno
	}
	// Load-then-use under h.mu; see commitTemp for why this cannot go
	// nil mid-frame.
	ws := h.snapshotWriteState()
	if ws == nil {
		return syscall.EIO
	}
	ws.mu.Lock()
	ws.pending = shfs.MetadataPatch{}
	ws.mu.Unlock()
	h.fs.dropPinnedForPath(targetPath)
	notifies.addContent(h.inode)
	notifies.addEntry(shfs.ParentPath(targetPath), path.Base(targetPath))
	return 0
}

func (h *storhubHandle) applyMetadataPatch(ctx context.Context, targetPath string, patch shfs.MetadataPatch) syscall.Errno {
	if !patch.HasMode && !patch.HasOwner && !patch.HasTimes {
		return 0
	}
	if err := h.fs.hub.ApplyMetadataPatchContext(ctx, h.fs.project, targetPath, patch); err != nil {
		h.fs.debugOp("commit failed", "path", targetPath, "inode", h.inode, "err", err)
		return errnoFromError(err)
	}
	return 0
}

// copyPageSize returns the buffer size for overlay copy loops. It is the
// configured OverlayBufferSize (default applied at mount), capped by the normalized
// chunk size so it can never exceed a single chunk window.
func (s *Filesystem) copyPageSize() int64 {
	size := s.opts.OverlayBufferSize
	if size <= 0 {
		size = defaultOverlayBufferSize
	}
	if capSize := normalizedChunkSize(s.hub.ChunkSize()); size > capSize {
		return capSize
	}
	return size
}
