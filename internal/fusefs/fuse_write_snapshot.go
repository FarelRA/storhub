package fusefs

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/FarelRA/storhub/internal/logging"
)

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
			zeroSpan(buf[:want])
			n = int(want)
		}
		if int64(n) < want {
			zeroSpan(buf[n:want])
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
	return w.fs.newSnapshotTemp("inodecommit*", w.logicalSize, func(temp *os.File) error {
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
	return w.fs.newSnapshotTemp("inoderanges*", w.logicalSize, func(temp *os.File) error {
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

func (w *inodeWriteState) closeWriteTemp() {
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
func (w *inodeWriteState) quarantineWriteTemp() {
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
