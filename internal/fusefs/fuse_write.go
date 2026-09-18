package fusefs

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
)

type inodeWriteState struct {
	fs    *Filesystem
	inode uint64

	opMu         sync.Mutex
	mu           sync.Mutex
	temp         *os.File
	tempPath     string
	baseTemp     *os.File
	baseTempPath string
	path         string
	closed       bool
	deleted      bool
	// openerUID/hasOpener record the identity that first created this
	// overlay, so a handleless Setattr can tell the single writer's own
	// truncate from a stranger's: the writer's path-based ftruncate
	// must still reach the overlay, a non-owner's must fall through to the
	// DAC-enforcing hub verbs.
	openerUID uint32
	hasOpener bool
	// poisoned marks a quarantined overlay whose bytes were moved to the
	// recovery directory: the temp no longer holds the data the dirty
	// ranges claim, so every further write/read/commit must fail EIO
	// instead of uploading zeros over remote content.
	poisoned          bool
	refs              int
	baseSize          int64
	logicalSize       int64
	dirtyRanges       []ByteRange
	tempAuthoritative bool
	pending           shfs.MetadataPatch
}

type ByteRange struct {
	Start int64
	End   int64
}

// maxDirtyRanges caps the disjoint dirty-range count per inode. Past the
// cap the ranges coalesce into one authoritative span (see
// ensureDirtyBounded): a sparse 1-byte-write storm must not grow the slice
// without bound until commit clears it.
const maxDirtyRanges = 1024

type writeBootstrap struct {
	baseSize int64
}

// soleWriteStateRef reports whether state is registered and held by exactly
// one open handle. Caller must not hold state.mu.
func (s *Filesystem) soleWriteStateRef(state *inodeWriteState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.writeStates[state.inode]
	return current == state && state.refs <= 1
}

func (s *Filesystem) writeStateForInode(inode uint64) *inodeWriteState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.writeStates[inode]
}

func (s *Filesystem) applyPendingSize(entry *shfs.EntryInfo) {
	if entry == nil {
		return
	}
	state := s.writeStateForInode(entry.Inode)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleted {
		return
	}
	state.overlayEntryLocked(entry)
}

func (s *Filesystem) acquireWriteState(ctx context.Context, inode uint64, targetPath string, bootstrap *writeBootstrap) (*inodeWriteState, error) {
	s.mu.Lock()
	if existing := s.writeStates[inode]; existing != nil {
		existing.refs++
		s.mu.Unlock()
		return existing, nil
	}
	state := &inodeWriteState{fs: s, inode: inode, path: targetPath, refs: 1}
	if shfs.IdentityPresent(ctx) {
		id := shfs.IdentityFromContext(ctx)
		state.openerUID, state.hasOpener = id.UID, true
	}
	s.writeStates[inode] = state
	s.mu.Unlock()
	var err error
	if bootstrap != nil {
		err = state.materializeBootstrap(bootstrap.baseSize)
	} else {
		err = state.materialize(ctx)
	}
	if err != nil {
		s.mu.Lock()
		if s.writeStates[inode] == state {
			delete(s.writeStates, inode)
		}
		s.mu.Unlock()
		state.closeTemp()
		return nil, err
	}
	return state, nil
}

func (w *inodeWriteState) materializeBootstrap(size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.temp != nil {
		return nil
	}
	temp, err := w.fs.newOverlayTemp("inode-*")
	if err != nil {
		return err
	}
	w.temp = temp
	w.tempPath = temp.Name()
	w.baseSize = size
	w.logicalSize = size
	w.tempAuthoritative = size == 0
	if err := w.temp.Truncate(size); err != nil {
		return err
	}
	return nil
}

func (s *Filesystem) releaseWriteState(state *inodeWriteState) {
	if state == nil {
		return
	}
	shouldClose := false
	s.mu.Lock()
	if current := s.writeStates[state.inode]; current == state {
		state.refs--
		if state.refs <= 0 {
			delete(s.writeStates, state.inode)
			shouldClose = true
		}
	}
	s.mu.Unlock()
	if shouldClose {
		state.closeTemp()
	}
}

func (w *inodeWriteState) materialize(ctx context.Context) error {
	w.mu.Lock()
	if w.temp != nil {
		w.mu.Unlock()
		return nil
	}
	path := w.path
	temp, err := w.fs.newOverlayTemp("inode-*")
	if err != nil {
		w.mu.Unlock()
		return err
	}
	tempPath := temp.Name()
	w.temp = temp
	w.tempPath = tempPath
	w.mu.Unlock()

	// Network call without lock
	entry, err := w.fs.hub.StatPathContext(ctx, w.fs.project, path)
	if err != nil {
		// Propagate: silently treating stat failure as "empty file"
		// would swap an open handle's content to EOF.
		if rmErr := os.Remove(tempPath); rmErr != nil {
			w.fs.errorf("materialize cleanup failed path=%s temp=%s err=%v", path, tempPath, rmErr)
		}
		w.mu.Lock()
		w.temp = nil
		w.tempPath = ""
		w.mu.Unlock()
		return err
	}
	w.mu.Lock()
	w.baseSize = entry.Size
	w.logicalSize = entry.Size
	truncErr := w.temp.Truncate(entry.Size)
	w.mu.Unlock()
	if truncErr != nil {
		return truncErr
	}
	return nil
}

func (w *inodeWriteState) snapshotBaseLocked(ctx context.Context, targetPath string) error {
	if w.baseTemp != nil {
		return nil
	}
	baseTemp, err := w.fs.newOverlayTemp("inode-base-*")
	if err != nil {
		return err
	}
	baseTempPath := baseTemp.Name()

	// Release lock before network calls
	w.mu.Unlock()
	entry, statErr := w.fs.hub.StatPathContext(ctx, w.fs.project, targetPath)
	if statErr == nil && entry.Size > 0 {
		if dlErr := w.fs.hub.DownloadFileContext(ctx, w.fs.project, targetPath, baseTempPath); dlErr != nil {
			_ = baseTemp.Close()
			_ = os.Remove(baseTempPath)
			w.mu.Lock()
			return dlErr
		}
	}
	w.mu.Lock()
	// Re-check: another goroutine may have already set this
	if w.baseTemp != nil {
		if err := baseTemp.Close(); err != nil {
			logging.Error(w.fs.log(), "failed to close base snapshot temp (duplicate)", "path", baseTempPath, "err", err)
		}
		if err := os.Remove(baseTempPath); err != nil {
			logging.Error(w.fs.log(), "failed to remove base snapshot temp (duplicate)", "path", baseTempPath, "err", err)
		}
		return nil
	}
	w.baseTemp = baseTemp
	w.baseTempPath = baseTempPath
	return nil
}

func (w *inodeWriteState) clearBaseSnapshotLocked() {
	if w.baseTemp != nil {
		if err := w.baseTemp.Close(); err != nil {
			logging.Error(w.fs.log(), "failed to close base snapshot temp", "err", err)
		}
		w.baseTemp = nil
	}
	if w.baseTempPath != "" {
		if err := os.Remove(w.baseTempPath); err != nil {
			logging.Error(w.fs.log(), "failed to remove base snapshot temp", "path", w.baseTempPath, "err", err)
		}
		w.baseTempPath = ""
	}
}

func (w *inodeWriteState) refreshBaseSnapshotLocked() error {
	if w.baseTemp == nil {
		return nil
	}
	if err := w.baseTemp.Truncate(w.logicalSize); err != nil {
		return err
	}
	buf := make([]byte, w.fs.copyPageSize())
	for _, dirty := range w.dirtyRanges {
		for offset := dirty.Start; offset < dirty.End; {
			want := int64(len(buf))
			if remaining := dirty.End - offset; want > remaining {
				want = remaining
			}
			n, err := w.temp.ReadAt(buf[:want], offset)
			if err != nil && err != io.EOF {
				return err
			}
			if n == 0 {
				return io.ErrNoProgress
			}
			if _, err := w.baseTemp.WriteAt(buf[:n], offset); err != nil {
				return err
			}
			offset += int64(n)
		}
	}
	return nil
}

// markDirtyLocked records [start,end) as dirty, merging with touching or
// overlapping ranges. The dirty set stays sorted and disjoint, reusing the
// existing slice instead of allocating a fresh one per write.
func (w *inodeWriteState) markDirtyLocked(start, end int64) {
	if start < 0 {
		start = 0
	}
	if end <= start {
		return
	}
	// i: first range that touches or overlaps [start,end); everything
	// before it is untouched. j: one past the last touching range.
	i := 0
	for i < len(w.dirtyRanges) && w.dirtyRanges[i].End < start {
		i++
	}
	j := len(w.dirtyRanges)
	for j > i && w.dirtyRanges[j-1].Start > end {
		j--
	}
	if i == j {
		w.dirtyRanges = append(w.dirtyRanges, ByteRange{})
		copy(w.dirtyRanges[i+1:], w.dirtyRanges[i:])
		w.dirtyRanges[i] = ByteRange{Start: start, End: end}
		return
	}
	if w.dirtyRanges[i].Start < start {
		start = w.dirtyRanges[i].Start
	}
	if w.dirtyRanges[j-1].End > end {
		end = w.dirtyRanges[j-1].End
	}
	w.dirtyRanges[i] = ByteRange{Start: start, End: end}
	w.dirtyRanges = append(w.dirtyRanges[:i+1], w.dirtyRanges[j:]...)
}

// ensureDirtyBounded enforces maxDirtyRanges after a mark. Past the cap the
// dirty set collapses to one span and the temp is made authoritative (see
// coalesceDirtyLocked), so the bound holds no matter how fragmented the
// write pattern gets. Caller must hold opMu and w.mu; the backfill may drop
// and re-acquire w.mu around base reads (opMu stays held, so no commit or
// competing read interleaves).
func (w *inodeWriteState) ensureDirtyBounded(ctx context.Context) error {
	if len(w.dirtyRanges) <= maxDirtyRanges {
		return nil
	}
	if w.tempAuthoritative {
		// The temp already holds the whole file: collapsing is honest
		// with no backfill.
		w.dirtyRanges = []ByteRange{{Start: 0, End: w.logicalSize}}
		if w.logicalSize == 0 {
			w.dirtyRanges = nil
		}
		return nil
	}
	return w.coalesceDirtyLocked(ctx)
}

// coalesceDirtyLocked materializes the full logical content into the temp
// (backfilling clean gaps from the base snapshot or the hub) and collapses
// the dirty set to a single [0, logicalSize) span with tempAuthoritative
// set. Collapsing without the backfill would corrupt: clean regions would
// serve temp holes (zeros) instead of base bytes on reads, and the commit
// would upload those zeros over real data. On backfill failure the honest
// range set is left intact and the error propagates to the writer.
func (w *inodeWriteState) coalesceDirtyLocked(ctx context.Context) error {
	if err := w.ensureTempLocked(); err != nil {
		return err
	}
	honest := append([]ByteRange(nil), w.dirtyRanges...)
	baseSize := w.baseSize
	logicalSize := w.logicalSize
	// Backfill every clean gap below the base size: above it the temp
	// holes already read as the logical zeros.
	pos := int64(0)
	for _, r := range honest {
		if err := w.backfillGapLocked(ctx, pos, r.Start, baseSize); err != nil {
			return err
		}
		pos = r.End
	}
	if err := w.backfillGapLocked(ctx, pos, logicalSize, baseSize); err != nil {
		return err
	}
	if err := w.temp.Truncate(logicalSize); err != nil {
		return err
	}
	w.dirtyRanges = []ByteRange{{Start: 0, End: logicalSize}}
	if logicalSize == 0 {
		w.dirtyRanges = nil
	}
	w.tempAuthoritative = true
	return nil
}

// backfillGapLocked copies the base bytes for the clean gap [from, to)
// into the temp. Ranges at or above the base size need nothing: temp holes
// already read as the logical zeros there. Reads prefer the local base
// snapshot when one exists and fall back to the hub.
func (w *inodeWriteState) backfillGapLocked(ctx context.Context, from, to, baseSize int64) error {
	if from < 0 {
		from = 0
	}
	if to > baseSize {
		to = baseSize
	}
	if from >= to {
		return nil
	}
	buf := make([]byte, w.fs.copyPageSize())
	for offset := from; offset < to; {
		want := int64(len(buf))
		if remaining := to - offset; want > remaining {
			want = remaining
		}
		var n int
		var err error
		if w.baseTemp != nil {
			n, err = w.baseTemp.ReadAt(buf[:want], offset)
		} else {
			var data []byte
			data, err = w.readBaseRangeLocked(ctx, offset, want)
			if err == nil {
				n = copy(buf, data)
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		if _, err := w.temp.WriteAt(buf[:n], offset); err != nil {
			return err
		}
		offset += int64(n)
	}
	return nil
}

func (w *inodeWriteState) truncateDirtyRangesLocked(size int64) {
	trimmed := make([]ByteRange, 0, len(w.dirtyRanges))
	for _, existing := range w.dirtyRanges {
		if existing.Start >= size {
			continue
		}
		if existing.End > size {
			existing.End = size
		}
		trimmed = append(trimmed, existing)
	}
	w.dirtyRanges = trimmed
}

func (w *inodeWriteState) coversRangeLocked(start, end int64) bool {
	if start >= end {
		return true
	}
	covered := start
	for _, existing := range w.dirtyRanges {
		if existing.End <= covered {
			continue
		}
		if existing.Start > covered {
			return false
		}
		covered = existing.End
		if covered >= end {
			return true
		}
	}
	return covered >= end
}

func (w *inodeWriteState) setSizeLocked(size int64) error {
	if size < 0 {
		return syscall.EINVAL
	}
	if w.poisoned {
		return syscall.EIO
	}
	if w.temp == nil {
		if err := w.ensureTempLocked(); err != nil {
			return err
		}
	}
	oldSize := w.logicalSize
	w.logicalSize = size
	if err := w.temp.Truncate(size); err != nil {
		return err
	}
	if size == 0 {
		w.tempAuthoritative = true
		w.clearBaseSnapshotLocked()
	} else if size > oldSize && !w.tempAuthoritative {
		// Regrown bytes must read as zeros (POSIX); the temp file may
		// hold stale data left over from before a shrink. Zero-fill the
		// regrown region and mark it dirty so the commit uploads it.
		buf := make([]byte, 32*1024)
		for offset := oldSize; offset < size; {
			n := int64(len(buf))
			if remaining := size - offset; remaining < n {
				n = remaining
			}
			if _, err := w.temp.WriteAt(buf[:n], offset); err != nil {
				return err
			}
			offset += n
		}
		w.markDirtyLocked(oldSize, size)
	}
	w.truncateDirtyRangesLocked(size)
	return nil
}

func (w *inodeWriteState) ensureTempLocked() error {
	if w.temp != nil {
		return nil
	}
	temp, err := w.fs.newOverlayTemp("inode-*")
	if err != nil {
		return err
	}
	w.temp = temp
	w.tempPath = temp.Name()
	if err := w.temp.Truncate(w.logicalSize); err != nil {
		return err
	}
	return nil
}

// readIntoLocked fills dest with the file content visible at off, serving
// dirty ranges from the overlay temp file and clean ranges from the base
// snapshot or the hub. Caller must hold w.mu.
func (w *inodeWriteState) readIntoLocked(ctx context.Context, dest []byte, off int64) (int, error) {
	if off >= w.logicalSize || len(dest) == 0 {
		return 0, nil
	}
	limit := int64(len(dest))
	if max := w.logicalSize - off; limit > max {
		limit = max
	}
	filled := int64(0)
	visibleBaseSize := minInt64(w.baseSize, w.logicalSize)
	for filled < limit {
		segmentStart := off + filled
		dirty, dirtyRange := w.nextDirtyRangeLocked(segmentStart)
		if dirty {
			chunkEnd := dirtyRange.End
			if chunkEnd > off+limit {
				chunkEnd = off + limit
			}
			if w.temp == nil {
				if err := w.ensureTempLocked(); err != nil {
					return 0, err
				}
			}
			n, err := w.temp.ReadAt(dest[filled:filled+(chunkEnd-segmentStart)], segmentStart)
			if err != nil && !errors.Is(err, io.EOF) {
				return int(filled), err
			}
			filled += int64(n)
			if int64(n) < chunkEnd-segmentStart {
				for i := filled; i < chunkEnd-off; i++ {
					dest[i] = 0
				}
				filled = chunkEnd - off
			}
			continue
		}
		cleanEnd := off + limit
		if dirtyRange.Start >= 0 && dirtyRange.Start < cleanEnd {
			cleanEnd = dirtyRange.Start
		}
		if segmentStart >= visibleBaseSize {
			for i := filled; i < cleanEnd-off; i++ {
				dest[i] = 0
			}
			filled = cleanEnd - off
			continue
		}
		readEnd := cleanEnd
		if readEnd > visibleBaseSize {
			readEnd = visibleBaseSize
		}
		if w.baseTemp != nil {
			n, err := w.baseTemp.ReadAt(dest[filled:filled+(readEnd-segmentStart)], segmentStart)
			if err != nil && !errors.Is(err, io.EOF) {
				return int(filled), err
			}
			if n == 0 {
				return int(filled), io.ErrNoProgress
			}
			filled += int64(n)
			continue
		}
		data, err := w.readBaseRangeLocked(ctx, segmentStart, readEnd-segmentStart)
		if err != nil {
			return int(filled), err
		}
		if len(data) == 0 {
			return int(filled), io.ErrNoProgress
		}
		copy(dest[filled:], data)
		filled += int64(len(data))
	}
	return int(filled), nil
}

func (w *inodeWriteState) readBaseRangeLocked(ctx context.Context, offset, length int64) ([]byte, error) {
	if length <= 0 {
		return []byte{}, nil
	}
	path := w.path
	w.mu.Unlock()
	data, err := w.fs.hub.ReadFileAtContext(shfs.WithSuppressedAtime(ctx), w.fs.project, path, offset, length)
	w.mu.Lock()
	return data, err
}

func (w *inodeWriteState) nextDirtyRangeLocked(offset int64) (bool, ByteRange) {
	for _, existing := range w.dirtyRanges {
		if offset >= existing.Start && offset < existing.End {
			return true, existing
		}
		if existing.Start > offset {
			return false, existing
		}
	}
	return false, ByteRange{Start: -1, End: -1}
}

func (w *inodeWriteState) dirtyBytesLocked() int64 {
	total := int64(0)
	for _, dirty := range w.dirtyRanges {
		total += dirty.End - dirty.Start
	}
	return total
}

func (w *inodeWriteState) hasPendingMetadataLocked() bool {
	return w.pending.HasMode || w.pending.HasOwner || w.pending.HasTimes
}

func (w *inodeWriteState) overlayEntryLocked(entry *shfs.EntryInfo) {
	if entry == nil {
		return
	}
	if w.pending.HasMode {
		entry.Mode = w.pending.Mode
	}
	if w.pending.HasOwner {
		entry.UID = w.pending.UID
		entry.GID = w.pending.GID
	}
	if w.pending.HasTimes {
		entry.AccessedAt = w.pending.ATime.Unix()
		entry.ModifiedAt = w.pending.MTime.Unix()
	}
	entry.Size = w.logicalSize
	entry.ChangedAt = max(entry.ChangedAt, w.fs.hub.Now())
}

// privClearBits is the setuid+setgid mask a data write clears for
// non-admin callers (POSIX CAP_FSETID semantics: only a privileged writer
// keeps the bits).
const privClearBits = 0o6000

// stagePrivClearLocked stages the cleared form of effective into the overlay
// pending patch when effective carries setuid/setgid and the caller is
// non-admin. Admin writers leave the bits untouched. An explicit fchmod
// afterwards overwrites pending wholesale, so staged clears compose: a
// later write re-clears from the staged mode, and a later chmod sets
// exactly what was requested, never ORing cleared bits back. Caller must
// hold state.opMu and state.mu.
func stagePrivClearLocked(ctx context.Context, state *inodeWriteState, effective uint32) {
	if shfs.IdentityPresent(ctx) && shfs.IdentityFromContext(ctx).Admin {
		return
	}
	if effective&privClearBits == 0 {
		return
	}
	state.pending.HasMode = true
	state.pending.Mode = effective &^ privClearBits
}

// stagePrivClearForDataWrite resolves the overlay-visible mode and stages
// its cleared form when a data write (handle Write or overlay ftruncate)
// lands on setuid/setgid bits for a non-admin caller. Staging into pending
// is what makes pre-commit stat (Getattr/Lookup overlay the pending patch)
// and exec (kernel mode from Getattr) observe cleared bits before the hub
// verbs sanitize at commit time. Caller must hold state.opMu and state.mu;
// the hub stat for the unstaged case runs with mu released while opMu stays
// held, so no commit or competing mutation interleaves (same pattern as the
// dirty-range backfill). ctx must already carry the caller identity.
func (s *Filesystem) stagePrivClearForDataWrite(ctx context.Context, state *inodeWriteState, targetPath string) {
	if shfs.IdentityPresent(ctx) && shfs.IdentityFromContext(ctx).Admin {
		return
	}
	if state.pending.HasMode {
		stagePrivClearLocked(ctx, state, state.pending.Mode)
		return
	}
	if targetPath == "" {
		// Detached (unlinked) handle: no path to stat, and the commit
		// discards anyway. Staging a mode derived from the wrong entry
		// (e.g. root) would corrupt the handle's own stat view.
		return
	}
	state.mu.Unlock()
	entry, err := s.hub.StatPathContext(ctx, s.project, targetPath)
	state.mu.Lock()
	if err != nil || entry == nil {
		s.debugf("priv-clear stat failed path=%s err=%v", targetPath, err)
		return
	}
	if state.poisoned || state.deleted {
		return
	}
	effective := entry.Mode
	if state.pending.HasMode {
		effective = state.pending.Mode
	}
	stagePrivClearLocked(ctx, state, effective)
}

func (w *inodeWriteState) plannedRangesLocked() []ByteRange {
	chunkSize := normalizedChunkSize(w.fs.hub.ChunkSize())
	planned := make([]ByteRange, 0, len(w.dirtyRanges))
	for _, dirty := range w.dirtyRanges {
		start := dirty.Start
		if start < w.baseSize {
			start = (start / chunkSize) * chunkSize
		}
		end := dirty.End
		if end < w.baseSize {
			end = ((end + chunkSize - 1) / chunkSize) * chunkSize
			if end > w.baseSize {
				end = w.baseSize
			}
		}
		if end < dirty.End {
			end = dirty.End
		}
		planned = mergeByteRange(planned, ByteRange{Start: start, End: end})
	}
	return planned
}

func mergeByteRange(existing []ByteRange, next ByteRange) []ByteRange {
	if next.End <= next.Start {
		return existing
	}
	merged := make([]ByteRange, 0, len(existing)+1)
	inserted := false
	for _, current := range existing {
		if current.End < next.Start {
			merged = append(merged, current)
			continue
		}
		if next.End < current.Start {
			if !inserted {
				merged = append(merged, next)
				inserted = true
			}
			merged = append(merged, current)
			continue
		}
		if current.Start < next.Start {
			next.Start = current.Start
		}
		if current.End > next.End {
			next.End = current.End
		}
	}
	if !inserted {
		merged = append(merged, next)
	}
	return merged
}

func totalByteRanges(ranges []ByteRange) int64 {
	total := int64(0)
	for _, dirty := range ranges {
		total += dirty.End - dirty.Start
	}
	return total
}

// Commit-strategy thresholds: when ranged patching costs more than a
// wholesale rewrite. shouldReplaceLocked checks them top to bottom; every
// arm is live policy for when per-range overhead (one metadata commit
// each) dominates.
const (
	// replaceDirtyFracNum/replaceDirtyFracDen: dirty >= 3/4 of the file:
	// patching would rewrite most of the file a range at a time.
	replaceDirtyFracNum = 3
	replaceDirtyFracDen = 4
	// replaceFragRanges/replaceFragCoverNum/replaceFragCoverDen: >= 12
	// dirty ranges covering >= 1/3 of the file: fragmentation dominates.
	replaceFragRanges   = 12
	replaceFragCoverNum = 1
	replaceFragCoverDen = 3
	// replaceHalfNum/replaceHalfDen: dirty >= 1/2 of the file: the
	// break-even point where replace wins unconditionally.
	replaceHalfNum = 1
	replaceHalfDen = 2
	// rewriteRangesMany: >= 4 planned ranges: rebuild the touched chunks
	// wholesale when fragmentation is high but byte volume is modest.
	rewriteRangesMany = 4
	// rewriteRangesSome: >= 2 planned ranges covering < 1/2 of the file.
	rewriteRangesSome = 2
)

// shouldReplaceLocked decides whether pending writes escalate to a
// full-file replace (one atomic remote rewrite) instead of ranged patching.
func (w *inodeWriteState) shouldReplaceLocked(planned []ByteRange) bool {
	fileSize := max(w.baseSize, w.logicalSize)
	if fileSize == 0 {
		return false
	}
	dirtyBytes := w.dirtyBytesLocked()
	if dirtyBytes*replaceDirtyFracDen >= fileSize*replaceDirtyFracNum {
		return true
	}
	if len(planned) >= replaceFragRanges && dirtyBytes*replaceFragCoverDen >= fileSize*replaceFragCoverNum {
		return true
	}
	if dirtyBytes*replaceHalfDen >= fileSize*replaceHalfNum {
		return true
	}
	return false
}

func (w *inodeWriteState) shouldChunkRewriteLocked(planned []ByteRange) bool {
	if len(planned) == 0 {
		return false
	}
	if len(planned) >= rewriteRangesMany {
		return true
	}
	if len(planned) >= rewriteRangesSome && totalByteRanges(planned) < max(w.baseSize, w.logicalSize)/2 {
		return true
	}
	return false
}

func (w *inodeWriteState) writeRangeToLocked(ctx context.Context, out *os.File, start, end int64) error {
	buf := make([]byte, w.fs.copyPageSize())
	for offset := start; offset < end; {
		want := int64(len(buf))
		if remaining := end - offset; want > remaining {
			want = remaining
		}
		n, err := w.readIntoLocked(ctx, buf[:want], offset)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		if _, err := out.WriteAt(buf[:n], offset); err != nil {
			return err
		}
		offset += int64(n)
	}
	return nil
}

func (w *inodeWriteState) writeWorkingRangeToLocked(out *os.File, start, end int64) error {
	buf := make([]byte, w.fs.copyPageSize())
	for offset := start; offset < end; {
		want := int64(len(buf))
		if remaining := end - offset; want > remaining {
			want = remaining
		}
		n, err := w.temp.ReadAt(buf[:want], offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			for i := range buf[:want] {
				buf[i] = 0
			}
			n = int(want)
		}
		if int64(n) < want {
			for i := n; i < int(want); i++ {
				buf[i] = 0
			}
			n = int(want)
		}
		if _, err := out.WriteAt(buf[:n], offset); err != nil {
			return err
		}
		offset += int64(n)
	}
	return nil
}

// newOverlayTemp creates one overlay temp file in the mount cache dir:
// the single funnel behind write-state materialization, base snapshots,
// handle snapshots, and commit snapshots.
func (s *Filesystem) newOverlayTemp(prefix string) (*os.File, error) {
	return os.CreateTemp(s.cacheDir, prefix)
}

// newSnapshotTemp creates a sized commit snapshot temp, fills it via
// fill, then syncs and closes it, returning its path. Any failure closes
// and removes the temp and returns the error: the snapshot is owned by the
// calling commit frame either way.
func (s *Filesystem) newSnapshotTemp(prefix string, size int64, fill func(*os.File) error) (string, error) {
	temp, err := s.newOverlayTemp(prefix)
	if err != nil {
		return "", err
	}
	name := temp.Name()
	cleanup := true
	defer func() {
		if !cleanup {
			return
		}
		if err := temp.Close(); err != nil {
			logging.Error(s.log(), "failed to close snapshot temp", "path", name, "err", err)
		}
		if err := os.Remove(name); err != nil {
			logging.Error(s.log(), "failed to remove snapshot temp", "path", name, "err", err)
		}
	}()
	if err := temp.Truncate(size); err != nil {
		return "", err
	}
	if err := fill(temp); err != nil {
		return "", err
	}
	// Crash ordering: the snapshot is the commit's input. Sync it
	// after the content lands and before any remote mutation reads it,
	// so a local crash in between cannot rewrite the backend from a
	// torn local file.
	if err := temp.Sync(); err != nil {
		return "", err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	cleanup = false
	return name, nil
}

func (w *inodeWriteState) createCommittedSnapshotLocked(ctx context.Context) (string, error) {
	return w.fs.newSnapshotTemp("inode-commit-*", w.logicalSize, func(temp *os.File) error {
		if w.tempAuthoritative || w.coversRangeLocked(0, w.logicalSize) {
			return w.writeWorkingRangeToLocked(temp, 0, w.logicalSize)
		}
		return w.writeRangeToLocked(ctx, temp, 0, w.logicalSize)
	})
}

func (w *inodeWriteState) replaceInputPathLocked(ctx context.Context) (string, bool, error) {
	if w.temp != nil && w.tempPath != "" && (w.tempAuthoritative || w.coversRangeLocked(0, w.logicalSize)) {
		if err := w.temp.Truncate(w.logicalSize); err != nil {
			return "", false, err
		}
		return w.tempPath, false, nil
	}
	snapshotPath, err := w.createCommittedSnapshotLocked(ctx)
	if err != nil {
		return "", false, err
	}
	return snapshotPath, true, nil
}

func (w *inodeWriteState) createRangeSnapshotLocked(ctx context.Context, ranges []ByteRange) (string, error) {
	return w.fs.newSnapshotTemp("inode-ranges-*", w.logicalSize, func(temp *os.File) error {
		buf := make([]byte, w.fs.copyPageSize())
		for _, r := range ranges {
			for offset := r.Start; offset < r.End; {
				want := int64(len(buf))
				if remaining := r.End - offset; want > remaining {
					want = remaining
				}
				n, err := w.readIntoLocked(ctx, buf[:want], offset)
				if err != nil {
					return err
				}
				if n == 0 {
					break
				}
				if _, err := temp.WriteAt(buf[:n], offset); err != nil {
					return err
				}
				offset += int64(n)
			}
		}
		return nil
	})
}

func (w *inodeWriteState) closeTemp() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	if w.temp != nil {
		if err := w.temp.Close(); err != nil {
			logging.Error(w.fs.log(), "failed to close write state temp file", "err", err)
		}
	}
	if w.tempPath != "" {
		if err := os.Remove(w.tempPath); err != nil {
			logging.Error(w.fs.log(), "failed to remove write state temp file", "path", w.tempPath, "err", err)
		}
	}
	if w.baseTemp != nil {
		if err := w.baseTemp.Close(); err != nil {
			logging.Error(w.fs.log(), "failed to close write state base temp file", "err", err)
		}
	}
	if w.baseTempPath != "" {
		if err := os.Remove(w.baseTempPath); err != nil {
			logging.Error(w.fs.log(), "failed to remove write state base temp file", "path", w.baseTempPath, "err", err)
		}
	}
	w.mu.Unlock()
}

// quarantineTemps moves the overlay temp (the only copy of uncommitted
// written data) into the recovery directory; the re-downloadable base snapshot
// is discarded as usual. Marks the state closed so a later closeTemp is a no-op.
func (w *inodeWriteState) quarantineTemps() {
	w.quarantineTempsReason(quarantineReasonCommitFailure)
}

// quarantineTempsReason is quarantineTemps with an explicit manifest
// reason (commit failure vs dirty-at-close).
func (w *inodeWriteState) quarantineTempsReason(reason string) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	// Capture the redrive intent BEFORE clearing: whether this temp is a
	// proven full image (authoritative, or dirty spans covering the whole
	// logical file), the spans/sizes for manual context, and the target's
	// current identity for the startup compare-and-swap.
	intent := quarantineIntent{
		fullImage:   w.tempAuthoritative || w.coversRangeLocked(0, w.logicalSize),
		baseSize:    w.baseSize,
		logicalSize: w.logicalSize,
		pending:     w.pending,
		hasPending:  w.pending.HasMode || w.pending.HasOwner || w.pending.HasTimes,
	}
	for _, r := range w.dirtyRanges {
		intent.ranges = append(intent.ranges, [2]int64{r.Start, r.End})
	}
	targetPath := w.path
	w.mu.Unlock()
	// Fingerprint outside the state lock: the Stat is a network call and
	// must never run under it. Best-effort (the commit just failed; the
	// backend may be unreachable): without a fingerprint the entry stays
	// manual-recovery-only.
	if targetPath != "" {
		if entry, err := w.fs.hub.StatPathContext(context.Background(), w.fs.project, targetPath); err == nil && entry != nil {
			intent.fingerprint = &targetFingerprint{
				Size:       entry.Size,
				Inode:      entry.Inode,
				ModifiedAt: entry.ModifiedAt,
				ChangedAt:  entry.ChangedAt,
			}
		}
	}
	w.mu.Lock()
	// Re-verify full-image under the relock: a write landing in the Stat
	// window above changed the state the candidate was captured from. Any
	// intervening mutation fails closed to manual recovery (a concurrent
	// write during quarantine means the temp's provenance is no longer
	// something auto-redrive may assert).
	intent.fullImage = intent.fullImage &&
		(w.tempAuthoritative || w.coversRangeLocked(0, w.logicalSize))
	intent.ranges = nil
	for _, r := range w.dirtyRanges {
		intent.ranges = append(intent.ranges, [2]int64{r.Start, r.End})
	}
	intent.baseSize = w.baseSize
	intent.logicalSize = w.logicalSize
	intent.pending = w.pending
	intent.hasPending = w.pending.HasMode || w.pending.HasOwner || w.pending.HasTimes
	// The overlay bytes just moved to recovery/, so the temp no
	// longer exists. Clearing the dirty set and poisoning the state
	// guarantees a late Write/commit fails EIO instead of re-materializing
	// an empty temp and uploading zeros over the remote file.
	w.dirtyRanges = nil
	w.baseSize = w.logicalSize
	w.poisoned = true
	temp := w.temp
	tempPath := w.tempPath
	baseTemp := w.baseTemp
	baseTempPath := w.baseTempPath
	targetPath = w.path
	w.temp = nil
	w.tempPath = ""
	w.baseTemp = nil
	w.baseTempPath = ""
	w.mu.Unlock()
	// Unregister so no later lookup can route new operations here; the
	// handles' own refs keep the struct alive until Release.
	w.fs.mu.Lock()
	if current := w.fs.writeStates[w.inode]; current == w {
		delete(w.fs.writeStates, w.inode)
	}
	w.fs.mu.Unlock()
	if baseTemp != nil {
		_ = baseTemp.Close()
	}
	if baseTempPath != "" {
		_ = os.Remove(baseTempPath)
	}
	if temp != nil {
		_ = temp.Close()
	}
	if tempPath != "" {
		w.fs.quarantineFile(tempPath, targetPath, reason, intent)
	}
}

// hasUncommittedChanges reports whether the overlay holds data that has never
// been successfully committed.
func (w *inodeWriteState) hasUncommittedChanges() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.dirtyRanges) > 0 || w.logicalSize != w.baseSize || w.hasPendingMetadataLocked()
}

func (h *storhubHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if h.writeState == nil {
		// No write state exists for this handle: only read-oriented
		// materialized snapshots (e.g. displaced handles) land here. The
		// kernel never routes WRITE to such handles under enforced mode
		// flags, and historically this branch wrote into the snapshot temp
		// without dirty tracking - the data was acknowledged but silently
		// discarded at commit. Fail loudly instead of pretending the write
		// landed.
		h.fs.debugf("write rejected path=%s inode=%d reason=no-write-state", h.path, h.inode)
		return 0, syscall.EIO
	}
	h.writeState.opMu.Lock()
	defer h.writeState.opMu.Unlock()
	h.writeState.mu.Lock()
	defer h.writeState.mu.Unlock()
	// A quarantined (poisoned) overlay no longer holds the bytes its
	// dirty ranges claimed. Accepting new writes would resurrect an empty
	// temp and commit zeros over remote data.
	if h.writeState.poisoned {
		return 0, syscall.EIO
	}
	if h.flags&syscall.O_APPEND != 0 {
		off = h.writeState.logicalSize
	}
	if h.writeState.temp == nil {
		if err := h.writeState.ensureTempLocked(); err != nil {
			return 0, errnoFromError(err)
		}
	}
	if off > h.writeState.logicalSize {
		h.writeState.markDirtyLocked(h.writeState.logicalSize, off)
		if err := h.writeState.temp.Truncate(off); err != nil {
			return 0, errnoFromError(err)
		}
		h.writeState.logicalSize = off
	}
	n, err := h.writeState.temp.WriteAt(data, off)
	if err != nil {
		return uint32(n), errnoFromError(err)
	}
	end := off + int64(n)
	if end > h.writeState.logicalSize {
		h.writeState.logicalSize = end
		if err := h.writeState.temp.Truncate(end); err != nil {
			return uint32(n), errnoFromError(err)
		}
	}
	h.writeState.markDirtyLocked(off, end)
	// Bound the disjoint-range count: pathological scatter patterns
	// collapse into one authoritative span (with base backfill) instead
	// of growing the slice until commit.
	if err := h.writeState.ensureDirtyBounded(ctx); err != nil {
		return uint32(n), errnoFromError(err)
	}
	// POSIX privilege clearing is overlay-immediate, not commit-deferred:
	// a non-admin data write stages the cleared mode into pending now, so
	// pre-commit stat and exec already observe it. The commit-time hub
	// verbs sanitize again; this staging must never resurrect bits, only
	// clear them (see stagePrivClearLocked).
	h.fs.stagePrivClearForDataWrite(h.fs.callerContext(ctx), h.writeState, h.writeState.path)
	h.fs.debugf("write path=%s inode=%d off=%d bytes=%d", h.writeState.path, h.inode, off, n)
	return uint32(n), 0
}

func (h *storhubHandle) commit(ctx context.Context) syscall.Errno {
	if h.writeState == nil {
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
	h.writeState.opMu.Lock()
	defer h.writeState.opMu.Unlock()
	h.writeState.mu.Lock()
	if h.writeState.poisoned {
		h.writeState.mu.Unlock()
		// The overlay was quarantined; committing would upload zeros.
		return syscall.EIO
	}
	if len(h.writeState.dirtyRanges) == 0 && h.writeState.logicalSize == h.writeState.baseSize && !h.writeState.hasPendingMetadataLocked() {
		h.writeState.mu.Unlock()
		return 0
	}
	if h.writeState.deleted || handlePath == "" {
		h.writeState.mu.Unlock()
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
	h.writeState.mu.Unlock()
	errno := h.checkCommitWriteAccess(ctx, handlePath)
	h.writeState.mu.Lock()
	if errno != 0 {
		h.writeState.mu.Unlock()
		return errno
	}
	// Re-validate under the lock: the DAC check released it.
	if h.writeState.deleted || h.writeState.poisoned {
		h.writeState.mu.Unlock()
		if h.writeState.poisoned {
			return syscall.EIO
		}
		return 0
	}
	targetPath := handlePath
	baseSize := h.writeState.baseSize
	logicalSize := h.writeState.logicalSize
	pending := h.writeState.pending
	return h.commitTemp(ctx, targetPath, baseSize, logicalSize, pending)
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
		h.fs.debugf("commit denied path=%s inode=%d step=dac err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	return 0
}

// commitTemp handles all temp-based commit paths (truncate, chunk-rewrite, replace, patch).
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu as needed.
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
func (h *storhubHandle) commitTemp(ctx context.Context, targetPath string, baseSize, logicalSize int64, pending shfs.MetadataPatch) syscall.Errno {
	if len(h.writeState.dirtyRanges) == 0 {
		h.writeState.mu.Unlock()
		if logicalSize != baseSize {
			h.fs.debugf("commit truncate path=%s inode=%d size=%d", targetPath, h.inode, logicalSize)
			if _, err := h.fs.hub.TruncateFileContext(ctx, h.fs.project, targetPath, logicalSize); err != nil {
				h.fs.debugf("commit failed path=%s inode=%d step=truncate err=%v", targetPath, h.inode, err)
				return errnoFromError(err)
			}
		}
		if errno := h.applyMetadataPatch(ctx, targetPath, pending); errno != 0 {
			return errno
		}
		h.writeState.mu.Lock()
		h.writeState.commitCacheRefreshLocked(logicalSize)
		h.writeState.mu.Unlock()
		h.fs.notifyKernelContentChanged(h.inode)
		h.writeState.mu.Lock()
		h.writeState.pending = shfs.MetadataPatch{}
		h.writeState.mu.Unlock()
		h.fs.dropPinnedForPath(targetPath)
		h.fs.notifyEntryForPath(shfs.ParentPath(targetPath), path.Base(targetPath))
		return 0
	}
	planned := h.writeState.plannedRangesLocked()
	// Rung order: chunk-rewrite (rebuild the touched chunks wholesale) is
	// preferred when fragmentation is high but the affected byte volume is
	// modest; full replace when the ladder in shouldReplaceLocked says
	// ranged work cannot pay; otherwise patch each dirty range in place.
	if h.writeState.shouldChunkRewriteLocked(planned) {
		return h.commitChunkRewrite(ctx, targetPath, logicalSize, planned, pending)
	}
	if h.writeState.shouldReplaceLocked(planned) {
		return h.commitReplace(ctx, targetPath, logicalSize, planned, pending)
	}
	return h.commitPatch(ctx, targetPath, baseSize, logicalSize, h.writeState.dirtyRanges, pending)
}

// commitChunkRewrite handles the chunk-rewrite path.
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu.
func (h *storhubHandle) commitChunkRewrite(ctx context.Context, targetPath string, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch) syscall.Errno {
	snapshotPath, err := h.writeState.createRangeSnapshotLocked(ctx, planned)
	baseSize := h.writeState.baseSize
	h.writeState.mu.Unlock()
	if err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=range-snapshot err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	// The snapshot is event-scoped: this commit frame owns it and removes
	// it on every exit path.
	defer func() { _ = os.Remove(snapshotPath) }()
	h.fs.debugf("commit chunk-rewrite path=%s inode=%d base=%d size=%d ranges=%d", targetPath, h.inode, baseSize, logicalSize, len(planned))
	repoMeta, _, err := h.fs.hub.LoadRepoMetadataReadonlyContext(ctx, h.fs.project)
	if err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=load-metadata err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	fileMeta := repoMeta.FindFile(targetPath)
	if fileMeta == nil {
		h.fs.debugf("commit failed path=%s inode=%d step=find-file err=not found", targetPath, h.inode)
		return syscall.ENOENT
	}
	if _, err := h.fs.hub.RewriteFileRangesWithMetadataContext(ctx, h.fs.project, targetPath, snapshotPath, repoMeta, fileMeta, logicalSize, planned); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=chunk-rewrite err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	h.writeState.mu.Lock()
	h.writeState.commitCacheRefreshLocked(logicalSize)
	h.writeState.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending)
}

// commitReplace handles the full-file replace path.
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu.
func (h *storhubHandle) commitReplace(ctx context.Context, targetPath string, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch) syscall.Errno {
	snapshotPath, cleanupSnapshot, err := h.writeState.replaceInputPathLocked(ctx)
	baseSize := h.writeState.baseSize
	dirtyCount := len(h.writeState.dirtyRanges)
	h.writeState.mu.Unlock()
	if err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=full-snapshot err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	if cleanupSnapshot {
		// Event-scoped snapshot: this commit frame owns the removal.
		defer func() { _ = os.Remove(snapshotPath) }()
	}
	h.fs.debugf("commit replace path=%s inode=%d base=%d size=%d dirty_ranges=%d", targetPath, h.inode, baseSize, logicalSize, dirtyCount)
	if _, err := h.fs.hub.ReplaceFileContext(ctx, h.fs.project, targetPath, snapshotPath); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=replace err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	h.writeState.mu.Lock()
	h.writeState.commitCacheRefreshLocked(logicalSize)
	h.writeState.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending)
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
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu.
func (h *storhubHandle) commitPatch(ctx context.Context, targetPath string, baseSize, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch) syscall.Errno {
	edits := make([]shfs.RangeEdit, 0, len(planned))
	for _, dirty := range planned {
		buf := make([]byte, dirty.End-dirty.Start)
		n, err := h.writeState.readIntoLocked(ctx, buf, dirty.Start)
		if err != nil {
			h.writeState.mu.Unlock()
			return errnoFromError(err)
		}
		// A short read is legal only when the missing tail lies past
		// end-of-file, where POSIX holes read as zeros. Anything else is
		// internal inconsistency: refuse to upload fabricated bytes.
		if n < len(buf) && dirty.Start+int64(n) < logicalSize {
			h.writeState.mu.Unlock()
			h.fs.errorf("commit aborted path=%s inode=%d step=patch-read short read at [%d,%d): got %d of %d bytes before EOF", targetPath, h.inode, dirty.Start, dirty.End, n, len(buf))
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
	h.writeState.mu.Unlock()

	// One batched operation for the whole commit: a single release
	// resolution and playlist rebuild instead of one round-trip chain per
	// range. Ascending order needs no intermediate offset fixups. Either
	// the whole batch commits or none of it does, so failures leave every
	// range dirty and the retry replays the identical batch.
	h.fs.debugf("commit patch path=%s inode=%d base=%d size=%d ranges=%d", targetPath, h.inode, baseSize, logicalSize, len(planned))
	if _, err := h.fs.hub.PatchFileRangesContext(ctx, h.fs.project, targetPath, edits); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=patch-batch err=%v", targetPath, h.inode, err)
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
				h.fs.debugf("commit failed path=%s inode=%d step=post-patch-truncate err=%v", targetPath, h.inode, err)
				return errnoFromError(err)
			}
		}
	}
	// Mark all applied ranges consumed so a retry resumes instead of
	// re-applying committed edits (which would duplicate bytes).
	h.writeState.mu.Lock()
	// The network window above released mu; a concurrent quarantine
	// (Close teardown) may have taken the temp with it. Fail the commit
	// keeping the ranges dirty instead of dereferencing a nil *os.File -
	// the remote patch already landed, and the replay is idempotent.
	if h.writeState.temp == nil || h.writeState.poisoned {
		h.writeState.mu.Unlock()
		h.fs.errorf("commit interrupted by quarantine path=%s inode=%d step=post-patch-local", targetPath, h.inode)
		return syscall.EIO
	}
	for _, dirty := range planned {
		h.writeState.removeDirtyRangeLocked(dirty.Start, dirty.End)
	}
	h.writeState.mu.Unlock()
	h.writeState.mu.Lock()
	if err := h.writeState.refreshBaseSnapshotLocked(); err != nil {
		h.fs.debugf("commit cache refresh failed path=%s inode=%d step=patch-cache err=%v", targetPath, h.inode, err)
		h.writeState.clearBaseSnapshotLocked()
	}
	h.writeState.baseSize = logicalSize
	h.writeState.dirtyRanges = nil
	h.writeState.tempAuthoritative = false
	if err := h.writeState.temp.Truncate(logicalSize); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=local-truncate err=%v", targetPath, h.inode, err)
		h.writeState.mu.Unlock()
		return errnoFromError(err)
	}
	h.fs.notifyKernelContentChanged(h.inode)
	h.writeState.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending)
}

// commitCacheRefreshLocked updates the cached write state after a successful remote write.
// Caller must hold h.writeState.mu.
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
// open), and notifies.
// Caller must NOT hold h.writeState.mu.
func (h *storhubHandle) commitPostUpdate(ctx context.Context, targetPath string, pending shfs.MetadataPatch) syscall.Errno {
	if errno := h.applyMetadataPatch(ctx, targetPath, pending); errno != 0 {
		return errno
	}
	h.writeState.mu.Lock()
	h.writeState.pending = shfs.MetadataPatch{}
	h.writeState.mu.Unlock()
	h.fs.dropPinnedForPath(targetPath)
	h.fs.notifyKernelContentChanged(h.inode)
	h.fs.notifyEntryForPath(shfs.ParentPath(targetPath), path.Base(targetPath))
	return 0
}

func (h *storhubHandle) applyMetadataPatch(ctx context.Context, targetPath string, patch shfs.MetadataPatch) syscall.Errno {
	if !patch.HasMode && !patch.HasOwner && !patch.HasTimes {
		return 0
	}
	if err := h.fs.hub.ApplyMetadataPatchContext(ctx, h.fs.project, targetPath, patch); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=metadata-patch err=%v", targetPath, h.inode, err)
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
