package fusefs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/FarelRA/storhub/internal/test"
)

// ---------------------------------------------------------------------------
// Handle: one open file description with an independent cursor.
// ---------------------------------------------------------------------------

// pcHandle wraps an *os.File. Positioned operations use pread/pwrite
// equivalents so they never move the cursor; Read and Write are
// cursor-based. Mode violations are rejected locally with ErrAccess so a
// kernel EBADF can never be confused with use-after-close.
type pcHandle struct {
	mu     sync.Mutex
	f      *os.File
	mode   test.OpenMode
	cursor int64
	closed bool
}

func (h *pcHandle) readable() bool {
	return h.mode == test.OpenReadOnly || h.mode == test.OpenReadWrite
}

func (h *pcHandle) writable() bool {
	return h.mode == test.OpenWriteOnly || h.mode == test.OpenReadWrite ||
		h.mode == test.OpenAppend || h.mode == test.OpenTruncate
}

func (h *pcHandle) PRead(offset int64, length int) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, test.ErrClosed
	}
	if !h.readable() {
		return nil, test.ErrAccess
	}
	if offset < 0 || length < 0 {
		return nil, test.ErrInvalid
	}
	if length == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, length)
	total := 0
	for total < length {
		n, err := h.f.ReadAt(buf[total:], offset+int64(total))
		total += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, pcTranslate(err)
		}
		if n == 0 {
			break
		}
	}
	return buf[:total], nil
}

func (h *pcHandle) PWrite(offset int64, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, test.ErrClosed
	}
	if !h.writable() {
		return 0, test.ErrAccess
	}
	if offset < 0 {
		return 0, test.ErrInvalid
	}
	written := 0
	for written < len(data) {
		n, err := h.f.WriteAt(data[written:], offset+int64(written))
		written += n
		if err != nil {
			return written, pcTranslate(err)
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (h *pcHandle) Read(length int) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, test.ErrClosed
	}
	if !h.readable() {
		return nil, test.ErrAccess
	}
	if length < 0 {
		return nil, test.ErrInvalid
	}
	if length == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, length)
	total := 0
	for total < length {
		n, err := h.f.ReadAt(buf[total:], h.cursor+int64(total))
		total += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, pcTranslate(err)
		}
		if n == 0 {
			break
		}
	}
	h.cursor += int64(total)
	return buf[:total], nil
}

func (h *pcHandle) Write(data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, test.ErrClosed
	}
	if !h.writable() {
		return 0, test.ErrAccess
	}
	if h.mode == test.OpenAppend {
		// The fd was opened O_APPEND, so the kernel forces every write
		// to the end of file regardless of the cursor.
		written := 0
		for written < len(data) {
			n, err := h.f.Write(data[written:])
			written += n
			if err != nil {
				h.cursor += int64(written)
				return written, pcTranslate(err)
			}
			if n == 0 {
				h.cursor += int64(written)
				return written, io.ErrShortWrite
			}
		}
		h.cursor += int64(written)
		return written, nil
	}
	written := 0
	for written < len(data) {
		n, err := h.f.WriteAt(data[written:], h.cursor+int64(written))
		written += n
		if err != nil {
			h.cursor += int64(written)
			return written, pcTranslate(err)
		}
		if n == 0 {
			h.cursor += int64(written)
			return written, io.ErrShortWrite
		}
	}
	h.cursor += int64(written)
	return written, nil
}

func (h *pcHandle) Truncate(size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	if !h.writable() {
		return test.ErrAccess
	}
	if size < 0 {
		return test.ErrInvalid
	}
	return pcTranslate(h.f.Truncate(size))
}

func (h *pcHandle) Sync() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	return pcTranslate(h.f.Sync())
}

func (h *pcHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	h.closed = true
	return pcTranslate(h.f.Close())
}

// pcSeekWhence values are the Linux lseek whences for data/hole queries.
// os.File.Seek passes whence straight to the kernel, which forwards
// SEEK_DATA/SEEK_HOLE through FUSE_LSEEK to the server's Lseek handler;
// numeric literals keep this file compiling on non-Linux hosts.
const (
	pcSeekData = 3
	pcSeekHole = 4
)

// SeekData implements test.SeekHandle over a real mount fd.
func (h *pcHandle) SeekData(off int64) (int64, error) {
	return h.pcSeek(off, pcSeekData)
}

// SeekHole implements test.SeekHandle over a real mount fd.
func (h *pcHandle) SeekHole(off int64) (int64, error) {
	return h.pcSeek(off, pcSeekHole)
}

func (h *pcHandle) pcSeek(off int64, whence int) (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, test.ErrClosed
	}
	if off < 0 {
		return 0, test.ErrInvalid
	}
	n, err := h.f.Seek(off, whence)
	if err != nil {
		// The server reports ENXIO for data past EOF (and hole past
		// EOF); surface it as the harness unsatisfiable-range sentinel
		// so scenarios match with errors.Is.
		if errors.Is(err, syscall.ENXIO) {
			return 0, fmt.Errorf("seek: %w", test.ErrUnsatisfiableRange)
		}
		return 0, pcTranslate(err)
	}
	return n, nil
}

// pcPathHandle is an O_PATH-style handle: permission-free to open, unable
// to do I/O. Every data operation fails with ErrAccess locally, mirroring
// the kernel's EBADF-on-I/O for O_PATH fds; use after close fails with
// ErrClosed.
type pcPathHandle struct {
	mu     sync.Mutex
	fd     int
	closed bool
}

func (h *pcPathHandle) deny() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	return test.ErrAccess
}

func (h *pcPathHandle) PRead(_ int64, _ int) ([]byte, error) {
	return nil, h.deny()
}

func (h *pcPathHandle) PWrite(_ int64, _ []byte) (int, error) {
	return 0, h.deny()
}

func (h *pcPathHandle) Read(_ int) ([]byte, error) {
	return nil, h.deny()
}

func (h *pcPathHandle) Write(_ []byte) (int, error) {
	return 0, h.deny()
}

func (h *pcPathHandle) Truncate(_ int64) error {
	return h.deny()
}

func (h *pcPathHandle) Sync() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	return nil
}

func (h *pcPathHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	h.closed = true
	return pcTranslate(syscall.Close(h.fd))
}
