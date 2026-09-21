package fusefs

import (
	"context"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
)

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
