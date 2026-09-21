package fusefs

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/FarelRA/storhub/internal/test"
)

// pcSurface maps Surface paths (absolute, slash separated) onto a FUSE
// mount point using only os and syscall operations.
type pcSurface struct {
	mount string
	// umask masks CreateFile modes (see test.UmaskSurface; zero
	// disables masking). Guarded by mu alongside casMu users: scenarios
	// run sequentially, but concurrent workers inside one scenario read
	// it.
	mu sync.Mutex
	// umask is read under mu.
	umask uint32
	// casMu serializes the check-then-write in CompareAndWrite so two
	// adapter-side CAS attempts cannot interleave between the revision
	// read and the write. It narrows (never closes) the
	// non-atomicity documented on pcRevision: the server still applies
	// no atomic CAS, which only production can fix.
	casMu sync.Mutex
}

var _ test.UmaskSurface = (*pcSurface)(nil)

// SetUmask implements test.UmaskSurface.SetUmask.
func (s *pcSurface) SetUmask(mask uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.umask = mask & 0o777
}

// join validates a Surface path and maps it under the mount point.
func (s *pcSurface) join(p string) (string, error) {
	if p == "" || p[0] != '/' {
		return "", test.ErrInvalid
	}
	return filepath.Join(s.mount, strings.TrimPrefix(p, "/")), nil
}

// pcTranslate maps syscall failures onto the posixconform sentinels. The
// shared errno-to-sentinel knowledge lives in test.Translate (nil wrap
// keeps the bare sentinels this adapter always returned); only the
// permission-failure note below stays local. A permission failure that
// is not about handle modes (for example chown by a non-privileged
// caller) has no sentinel and is returned raw so the scenario fails
// loudly instead of being misclassified.
func pcTranslate(err error) error {
	return test.Translate(err, nil)
}

func (s *pcSurface) CreateFile(p string, perm uint32, exclusive bool) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	fi, statErr := os.Lstat(full)
	if statErr == nil {
		// Mirror the oracle: repeat non-exclusive create over a file is
		// a no-op, but any create over a directory or symlink collides,
		// and exclusive create over anything collides.
		if exclusive || fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return test.ErrExists
		}
		return nil
	}
	if !errors.Is(statErr, syscall.ENOENT) {
		return pcTranslate(statErr)
	}
	flags := os.O_RDWR | os.O_CREATE
	if exclusive {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(full, flags, os.FileMode(perm&0o7777))
	if err != nil {
		return pcTranslate(err)
	}
	if err := pcTranslate(f.Close()); err != nil {
		return err
	}
	// Apply the configured umask with an explicit chmod, but only when
	// a mask is set, keeping the unmasked path byte-identical.
	s.mu.Lock()
	umask := s.umask
	s.mu.Unlock()
	if umask != 0 {
		target := (perm & 0o7777) &^ (umask & 0o777)
		if st, err := s.Stat(p); err == nil && st.Mode&0o7777 != target {
			return s.Chmod(p, target)
		}
	}
	return nil
}

func (s *pcSurface) Open(p string, mode test.OpenMode, disp test.CreateDisposition) (test.Handle, error) {
	full, err := s.join(p)
	if err != nil {
		return nil, err
	}
	// pcOPath is Linux O_PATH: a permission-free, I/O-free open. Kept as a
	// numeric literal (rather than syscall.O_PATH) so this test file still
	// compiles on non-Linux hosts; the scenario fails loudly off-Linux
	// instead, like the renameat2 helper below.
	const pcOPath = 0o10000000
	if mode == test.OpenPath {
		fd, err := syscall.Open(full, pcOPath, 0)
		if err != nil {
			return nil, pcTranslate(err)
		}
		return &pcPathHandle{fd: fd}, nil
	}
	var flags int
	switch mode {
	case test.OpenReadOnly:
		flags = os.O_RDONLY
	case test.OpenWriteOnly:
		flags = os.O_WRONLY
	case test.OpenReadWrite:
		flags = os.O_RDWR
	case test.OpenAppend:
		flags = os.O_WRONLY | os.O_APPEND
	case test.OpenTruncate:
		flags = os.O_WRONLY | os.O_TRUNC
	default:
		return nil, test.ErrInvalid
	}
	// Creation intent travels in disp, like O_CREAT: write modes no
	// longer imply creation. OpenReadOnly never creates (O_CREAT
	// without write access is meaningless here); every other mode
	// creates only under CreateIfMissing.
	switch disp {
	case test.CreateNever, test.CreateIfMissing:
	default:
		return nil, test.ErrInvalid
	}
	if mode != test.OpenReadOnly && disp == test.CreateIfMissing {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(full, flags, 0o644)
	if err != nil {
		return nil, pcTranslate(err)
	}
	h := &pcHandle{f: f, mode: mode}
	if mode == test.OpenAppend {
		if fi, err := f.Stat(); err == nil {
			h.cursor = fi.Size()
		}
	}
	return h, nil
}

func (s *pcSurface) Stat(p string) (test.Stat, error) {
	full, err := s.join(p)
	if err != nil {
		return test.Stat{}, err
	}
	fi, err := os.Stat(full)
	if err != nil {
		return test.Stat{}, pcTranslate(err)
	}
	if fi.IsDir() {
		return test.Stat{}, test.ErrIsDir
	}
	var st test.Stat
	st.Size = fi.Size()
	mode := uint32(fi.Mode().Perm())
	if fi.Mode()&os.ModeSetuid != 0 {
		mode |= test.SetUIDBit
	}
	if fi.Mode()&os.ModeSetgid != 0 {
		mode |= test.SetGIDBit
	}
	st.Mode = mode
	st.MTime = fi.ModTime().UnixNano()
	if sys, ok := fi.Sys().(*syscall.Stat_t); ok {
		st.UID = sys.Uid
		st.GID = sys.Gid
	}
	return st, nil
}

func (s *pcSurface) Truncate(p string, size int64) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	if size < 0 {
		return test.ErrInvalid
	}
	return pcTranslate(os.Truncate(full, size))
}

// pcFileMode converts conformance permission bits (including setuid and
// setgid) to an os.FileMode.
func pcFileMode(mode uint32) os.FileMode {
	fm := os.FileMode(mode & 0o777)
	if mode&test.SetUIDBit != 0 {
		fm |= os.ModeSetuid
	}
	if mode&test.SetGIDBit != 0 {
		fm |= os.ModeSetgid
	}
	return fm
}

func (s *pcSurface) Chmod(p string, mode uint32) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	return pcTranslate(os.Chmod(full, pcFileMode(mode)))
}

func (s *pcSurface) Chown(p string, uid, gid uint32) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	if err := os.Chown(full, int(uid), int(gid)); err != nil {
		return pcTranslate(err)
	}
	// The contract requires chown to clear setuid/setgid. When the
	// backend already did, this stat finds nothing to do; when it did
	// not, the explicit chmod closes the gap with a real syscall.
	fi, err := os.Lstat(full)
	if err != nil {
		return pcTranslate(err)
	}
	if fi.Mode()&(os.ModeSetuid|os.ModeSetgid) == 0 {
		return nil
	}
	clean := fi.Mode().Perm()
	return pcTranslate(os.Chmod(full, clean))
}

func (s *pcSurface) Utimens(p string, mtime int64) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	ts := time.Unix(0, mtime)
	return pcTranslate(os.Chtimes(full, ts, ts))
}

func (s *pcSurface) Unlink(p string) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	fi, err := os.Lstat(full)
	if err != nil {
		return pcTranslate(err)
	}
	if fi.IsDir() {
		return test.ErrIsDir
	}
	return pcTranslate(os.Remove(full))
}

func (s *pcSurface) Rename(oldPath, newPath string, noReplace bool) error {
	oldFull, err := s.join(oldPath)
	if err != nil {
		return err
	}
	newFull, err := s.join(newPath)
	if err != nil {
		return err
	}
	if oldPath == newPath {
		if _, err := os.Lstat(oldFull); err != nil {
			return pcTranslate(err)
		}
		return nil
	}
	if !noReplace {
		return pcTranslate(os.Rename(oldFull, newFull))
	}
	// Atomic noreplace rename via renameat2 (Linux-only syscall; the
	// build-tagged pcRenameNoReplace helper reports ENOSYS elsewhere so
	// darwin vet still compiles and the scenario fails loudly off-Linux).
	if err := pcRenameNoReplace(oldFull, newFull); err != nil {
		return pcTranslate(err)
	}
	return nil
}

func (s *pcSurface) Mkdir(p string, perm uint32) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	return pcTranslate(os.Mkdir(full, os.FileMode(perm&0o777)))
}

func (s *pcSurface) Rmdir(p string) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	fi, err := os.Lstat(full)
	if err != nil {
		return pcTranslate(err)
	}
	if !fi.IsDir() {
		return test.ErrNotDir
	}
	return pcTranslate(os.Remove(full))
}

func (s *pcSurface) Symlink(target, linkPath string) error {
	full, err := s.join(linkPath)
	if err != nil {
		return err
	}
	if target == "" {
		return test.ErrInvalid
	}
	return pcTranslate(os.Symlink(target, full))
}

func (s *pcSurface) Readlink(linkPath string) (string, error) {
	full, err := s.join(linkPath)
	if err != nil {
		return "", err
	}
	target, err := os.Readlink(full)
	if err != nil {
		return "", pcTranslate(err)
	}
	return target, nil
}

func (s *pcSurface) ReadRange(p string, offset, length int64) ([]byte, error) {
	full, err := s.join(p)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, test.ErrInvalid
	}
	f, err := os.Open(full)
	if err != nil {
		return nil, pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return nil, pcTranslate(err)
	}
	if fi.IsDir() {
		return nil, test.ErrIsDir
	}
	if offset >= fi.Size() {
		return nil, test.ErrUnsatisfiableRange
	}
	if length == 0 {
		return []byte{}, nil
	}
	end := offset + length
	if end > fi.Size() {
		end = fi.Size()
	}
	buf := make([]byte, end-offset)
	if _, err := io.ReadFull(io.NewSectionReader(f, offset, end-offset), buf); err != nil {
		return nil, pcTranslate(err)
	}
	return buf, nil
}

func (s *pcSurface) Append(p string, data []byte) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	// No O_CREATE on purpose: Append never creates a missing file.
	f, err := os.OpenFile(full, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	for len(data) > 0 {
		n, err := f.Write(data)
		if err != nil {
			_ = f.Close()
			return pcTranslate(err)
		}
		if n == 0 {
			_ = f.Close()
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return pcTranslate(f.Close())
}

func (s *pcSurface) Sync(p string) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	f, err := os.Open(full)
	if err != nil {
		return pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return pcTranslate(err)
	}
	return pcTranslate(f.Close())
}

// pcRevision hashes file content. The FUSE path exposes no server version
// or etag, so this is a best-effort CAS token: it advances on every data
// change and never on metadata-only changes, but the check-then-write in
// CompareAndWrite is not atomic.
func pcRevision(full string) (uint64, error) {
	data, err := os.ReadFile(full)
	if err != nil {
		return 0, pcTranslate(err)
	}
	sum := fnv.New64a()
	_, _ = sum.Write(data)
	return sum.Sum64(), nil
}

func (s *pcSurface) Revision(p string) (uint64, error) {
	full, err := s.join(p)
	if err != nil {
		return 0, err
	}
	return pcRevision(full)
}

func (s *pcSurface) CompareAndWrite(p string, offset int64, data []byte, token uint64) error {
	// Adapter-side serialization only (see casMu): the server still
	// applies no atomic CAS.
	s.casMu.Lock()
	defer s.casMu.Unlock()
	full, err := s.join(p)
	if err != nil {
		return err
	}
	current, err := pcRevision(full)
	if err != nil {
		return err
	}
	if current != token {
		return test.ErrPrecondition{Expected: token, Actual: current}
	}
	if offset < 0 {
		return test.ErrInvalid
	}
	f, err := os.OpenFile(full, os.O_WRONLY, 0)
	if err != nil {
		return pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	for len(data) > 0 {
		n, err := f.WriteAt(data, offset)
		offset += int64(n)
		data = data[n:]
		if err != nil {
			_ = f.Close()
			return pcTranslate(err)
		}
		if n == 0 {
			_ = f.Close()
			return io.ErrShortWrite
		}
	}
	return pcTranslate(f.Close())
}

// PunchHole issues a real fallocate PUNCH_HOLE|KEEP_SIZE so the server's
// Allocate handler decides honestly: EOPNOTSUPP surfaces as ErrUnsupported
// (the scenario then requires unchanged content), anything else is a loud
// failure. Linux-only by nature (fallocate(2)); x/sys is already a module
// dependency (see the renameat2 helper below).
func (s *pcSurface) PunchHole(p string, off, length int64) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	if off < 0 || length < 0 {
		return test.ErrInvalid
	}
	if length == 0 {
		return nil
	}
	f, err := os.OpenFile(full, os.O_RDWR, 0)
	if err != nil {
		return pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	if err := pcFallocatePunchHole(int64(f.Fd()), off, length); err != nil {
		if errors.Is(err, syscall.EOPNOTSUPP) {
			return fmt.Errorf("punch hole %s: %w", p, test.ErrUnsupported)
		}
		return pcTranslate(err)
	}
	return pcTranslate(f.Close())
}

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
