package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
	spool    *os.File  // full-window mirror (sparse until written)
	live     io.Reader // upstream cursor, shared across sequential windows
	size     int64     // window length
	mirrored int64     // high-water mark of spooled bytes
	pos      int64     // current position within the window
}

var errWindowOverrun = errors.New("window reader: live stream overran window")

func newWindowReader(live io.Reader, size int64) (*windowReader, func(), error) {
	base := filepath.Join(storcfg.CacheBase(), "rest")
	if err := mkdirAll(base); err != nil {
		return nil, nil, fmt.Errorf("create spool base dir: %w", err)
	}
	dir, err := os.MkdirTemp(base, "upload-*")
	if err != nil {
		return nil, nil, fmt.Errorf("create spool dir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }

	file, err := os.Create(filepath.Join(dir, "window"))
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("create spool file: %w", err)
	}
	return &windowReader{spool: file, live: live, size: size}, cleanup, nil
}

// Read serves [pos, size): bytes below `mirrored` come from the spool,
// everything else is pulled from the live stream and simultaneously written
// through to the spool at its absolute offset.
func (w *windowReader) Read(p []byte) (int, error) {
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
			n, err := w.live.Read(buf[:minInt(len(buf), int(w.size-w.pos))])
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
// (Seek(0, Start)); arbitrary offsets are provided for completeness.
func (w *windowReader) Seek(offset int64, whence int) (int64, error) {
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
	w.pos = target
	return w.pos, nil
}

// Close releases the spool file handle; callers additionally remove the
// spool directory via the cleanup func handed out by newWindowReader.
func (w *windowReader) Close() error { return w.spool.Close() }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func mkdirAll(dir string) error { return os.MkdirAll(dir, 0o755) }
