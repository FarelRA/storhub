package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// windowReader is a seekable view over one upload chunk whose bytes exist in
// two places: a spool file mirroring everything already pulled from the live
// stream ([0, mirrored)), and the live stream itself for the remainder.
//
// It exists so GitHub's upload client - which rewinds via Seek(0, Start)
// before every transport retry - can replay a failed window without
// re-reading the network. First attempt streams live and tees into the
// spool; any later attempt replays the mirrored prefix for free and tees
// only the unconsumed suffix.
//
// Invariant: `mirrored` is the high-water mark of live bytes durably written
// to the spool, and reads below it always come from disk.
type windowReader struct {
	// mu serializes Read against Seek for the same reason as
	// chunking.ChunkReader: the uploader rewinds between transport retries
	// while the previous attempt's body may still be draining on the
	// http transport's writeLoop.
	mu       sync.Mutex
	spool    *os.File  // flat spool file: full-window mirror (sparse until written)
	live     io.Reader // upstream cursor, shared across sequential windows
	size     int64     // window length
	mirrored int64     // high-water mark of spooled bytes
	pos      int64     // current position within the window
}

var errWindowOverrun = errors.New("window reader: live stream overran window")

// errWindowSeekAhead rejects a Seek past the mirrored high-water mark: the
// bytes there were never pulled from the live stream, so a forward seek
// then Read would WriteAt the next live byte at the wrong absolute offset,
// leaving an unwritten hole that later replays as zeros (range-geometry enforcement). The
// uploader's only real use is Seek(0, Start) between transport retries,
// which always satisfies target <= mirrored.
var errWindowSeekAhead = errors.New("window reader: seek ahead of mirrored bytes")

// spoolBase returns the upload-spool directory: <CacheBase>/rest (flat
// upload-* files, no per-upload dirs).
//
// History: this was once <CacheBase>/storhub/rest. The old path migrates
// its entries into place on first use and the empty old dir is removed;
// no symlink is left behind, so the new path is the single truth. Files
// stranded in the old dir by a crash mid-migration are still swept by the
// orphan reaper, which reads both dirs.
//
// Config.CacheDir precedence (explicit > env > XDG > temp) and the
// Config.SpoolBase/ObjectCacheDir/CacheBase accessors are the config
// owner's slice (internal/config); this file consumes storcfg.CacheBase()
// and must switch to cfg.SpoolBase() once it lands.

// spoolMigrateOnce runs the legacy-dir migration at most once per process:
// migration is a one-time event, while spoolBase runs per upload window.
var spoolMigrateOnce sync.Once

func spoolBase() (string, error) {
	base := storcfg.CacheBase()
	rest := filepath.Join(base, "rest")
	if err := os.MkdirAll(rest, 0o755); err != nil {
		return "", fmt.Errorf("create spool base dir: %w", err)
	}
	spoolMigrateOnce.Do(func() { migrateLegacySpoolDir(base, rest) })
	return rest, nil
}

// migrateLegacySpoolDir moves entries from the pre-XDG <base>/storhub/rest
// dir into place and removes the emptied old dir, leaving no symlink: the
// new path is the single truth. Best-effort: a failed entry move keeps
// the legacy dir with its remaining files.
func migrateLegacySpoolDir(base, rest string) {
	legacy := filepath.Join(base, "storhub", "rest")
	if info, err := os.Lstat(legacy); err == nil && info.IsDir() && !isSymlink(info) {
		// Legacy dir from a previous version: migrate contents one entry
		// at a time (best-effort), then drop the emptied dir.
		entries, _ := os.ReadDir(legacy)
		moved := true
		for _, e := range entries {
			if err := os.Rename(filepath.Join(legacy, e.Name()), filepath.Join(rest, e.Name())); err != nil {
				moved = false
				break
			}
		}
		if moved {
			_ = os.Remove(legacy)
		}
	}
}

func isSymlink(info os.FileInfo) bool { return info.Mode()&os.ModeSymlink != 0 }

func newWindowReader(live io.Reader, size int64) (*windowReader, func(), error) {
	base, err := spoolBase()
	if err != nil {
		return nil, nil, err
	}
	// Flat layout: the spool IS a file named upload-<id>. No per-upload dirs.
	file, err := os.CreateTemp(base, "upload-*")
	if err != nil {
		return nil, nil, fmt.Errorf("create spool file: %w", err)
	}
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}
	// NOTE: deliberately NOT implementing io.Close on windowReader -
	// http.Client closes request bodies after every response, which would
	// kill the spool file between rename attempts. Lifecycle is owned
	// exclusively by the returned cleanup func.
	return &windowReader{spool: file, live: live, size: size}, cleanup, nil
}

// Read serves [pos, size): bytes below `mirrored` come from the spool,
// everything else is pulled from the live stream and simultaneously written
// through to the spool at its absolute offset.
func (w *windowReader) Read(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	total := 0
	for total < len(p) {
		switch {
		case w.pos >= w.size:
			if total == 0 {
				return 0, io.EOF
			}
			return total, nil

		case w.pos < w.mirrored: // replay from disk
			n64 := min(int64(len(p)-total), w.mirrored-w.pos)
			if _, err := w.spool.ReadAt(p[total:total+int(n64)], w.pos); err != nil && !errors.Is(err, io.EOF) {
				return total, err
			}
			w.pos += n64
			total += int(n64)

		default: // pull from live, tee to spool at absolute offset
			buf := p[total:]
			n, err := w.live.Read(buf[:min(len(buf), int(w.size-w.pos))])
			if n > 0 {
				if _, werr := w.spool.WriteAt(buf[:n], w.pos); werr != nil {
					return total, werr
				}
				w.mirrored = w.pos + int64(n)
				w.pos += int64(n)
				total += n
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					if w.pos != w.size {
						return total, fmt.Errorf("live stream ended at %d/%d bytes: %w", w.pos, w.size, io.ErrUnexpectedEOF)
					}
					if total == 0 {
						return 0, io.EOF
					}
					return total, nil
				}
				return total, err
			}
			if n == 0 {
				return total, io.ErrNoProgress
			}
		}
	}
	return total, nil
}

// Seek supports the rewind GitHub's uploader performs between attempts
// (Seek(0, Start)). Targets past `mirrored` are hard-rejected
// (errWindowSeekAhead): seeking ahead then Reading would tee the next live
// byte at the wrong absolute offset and corrupt the window with a zero
// hole. Targets within [0, mirrored] replay from spool; [mirrored, size]
// resumes the live stream exactly where it left off.
func (w *windowReader) Seek(offset int64, whence int) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = w.pos + offset
	case io.SeekEnd:
		target = w.size + offset
	default:
		return 0, fmt.Errorf("window seek: invalid whence %d", whence)
	}
	if target < 0 || target > w.size {
		return 0, errWindowOverrun
	}
	if target > w.mirrored {
		return 0, fmt.Errorf("%w: seek to %d with only %d bytes mirrored", errWindowSeekAhead, target, w.mirrored)
	}
	w.pos = target
	return w.pos, nil
}

// NOTE: windowReader deliberately does NOT implement io.Closer -
// http.Client closes request bodies after every response, which would kill
// the spool file between rename attempts. Lifecycle is owned exclusively by
// the cleanup func returned by newWindowReader.
