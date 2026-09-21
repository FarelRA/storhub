package fusefs

import (
	"context"
	"io"
	"os"
	"sync"

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
	// baseEntry caches the hub's entry at detach-snapshot time (see
	// snapshotBaseLocked). An unlinked-but-open inode has no path to
	// stat, so fh-carried ops on it (close-time mtime flush, fstat)
	// build their replies from this snapshot plus the live overlay
	// instead of failing ESTALE. First snapshot wins: the entry at
	// detach time is the handle's truth for the rest of its life.
	baseEntry    shfs.EntryInfo
	hasBaseEntry bool
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

// ByteRange is one dirty byte span within a write handle.
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
	temp, err := w.fs.newOverlayTemp("inode*")
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
	temp, err := w.fs.newOverlayTemp("inode*")
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
	baseTemp, err := w.fs.newOverlayTemp("inodebase*")
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
	if statErr == nil && entry != nil && !w.hasBaseEntry {
		w.baseEntry = *entry
		w.hasBaseEntry = true
	}
	return nil
}

// cachedBaseEntry returns the detach-time entry snapshot for fh-carried
// ops on an unlinked inode. Callers must not hold w.mu.
func (w *inodeWriteState) cachedBaseEntry() (shfs.EntryInfo, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.baseEntry, w.hasBaseEntry
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
