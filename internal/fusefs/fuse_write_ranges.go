package fusefs

import (
	"context"
	"errors"
	"io"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

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
	if capN := w.logicalSize - off; limit > capN {
		limit = capN
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
		entry.AccessedAt = w.pending.ATime.UnixNano()
		entry.ModifiedAt = w.pending.MTime.UnixNano()
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
