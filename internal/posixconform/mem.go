package posixconform

import (
	"strings"
	"sync"
)

// MemSurface is an in-memory Surface with textbook POSIX semantics. It is the
// oracle that proves the harness can go green. One mutex guards all state;
// deletion detaches names while open handles keep working.
type MemSurface struct {
	mu    sync.Mutex
	files map[string]*memFile
	dirs  map[string]uint32
	links map[string]string
	clock int64 // logical mtime clock, advanced on every mutation
}

// Compile-time conformance checks.
var (
	_ Surface    = (*MemSurface)(nil)
	_ PunchHoler = (*MemSurface)(nil)
	_ Handle     = (*memHandle)(nil)
	_ SeekHandle = (*memHandle)(nil)
)

type memFile struct {
	data    []byte
	mode    uint32
	uid     uint32
	gid     uint32
	mtime   int64
	version uint64 // CAS token, advanced by every data mutation
}

// NewMemSurface returns an empty MemSurface with only the root directory present.
func NewMemSurface() *MemSurface {
	return &MemSurface{
		files: make(map[string]*memFile),
		dirs:  map[string]uint32{"/": 0o755},
		links: make(map[string]string),
		clock: 1000000000,
	}
}

func validPath(path string) bool {
	return len(path) > 1 && path[0] == '/'
}

func parentOf(path string) string {
	for i := len(path) - 1; i > 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "/"
}

func (m *MemSurface) tick() int64 {
	m.clock++
	return m.clock
}

func (m *MemSurface) bumpLocked(f *memFile) {
	f.version++
	m.clock++
	f.mtime = m.clock
}

// resolveLocked follows symlinks up to 40 hops; caller must hold m.mu.
func (m *MemSurface) resolveLocked(path string) (*memFile, error) {
	cur := path
	for i := 0; i < 40; i++ {
		if f, ok := m.files[cur]; ok {
			return f, nil
		}
		if tgt, ok := m.links[cur]; ok {
			cur = tgt
			continue
		}
		if _, ok := m.dirs[cur]; ok {
			return nil, ErrIsDir
		}
		return nil, ErrNotFound
	}
	return nil, ErrLoop
}

func sliceClamp(data []byte, offset, length int64) []byte {
	if offset >= int64(len(data)) || length <= 0 {
		return []byte{}
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	out := make([]byte, end-offset)
	copy(out, data[offset:end])
	return out
}

func growForWrite(data []byte, offset int64, chunk []byte) []byte {
	end := offset + int64(len(chunk))
	if end > int64(len(data)) {
		nb := make([]byte, end)
		copy(nb, data)
		data = nb
	}
	copy(data[offset:], chunk)
	return data
}

// CreateFile implements Surface.CreateFile.
func (m *MemSurface) CreateFile(path string, perm uint32, exclusive bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	if _, ok := m.files[path]; ok {
		if exclusive {
			return ErrExists
		}
		return nil
	}
	if _, ok := m.links[path]; ok {
		return ErrExists
	}
	if _, ok := m.dirs[path]; ok {
		return ErrExists
	}
	if _, ok := m.dirs[parentOf(path)]; !ok {
		return ErrNotFound
	}
	m.files[path] = &memFile{mode: perm & 0o7777, mtime: m.tick(), version: 1}
	return nil
}

// Open implements Surface.Open. OpenPath is the perm-free open: like the
// oracle having no permission checks at all, it succeeds on any existing
// path (and reports ErrNotFound on a missing one without creating it),
// while the handle itself carries no I/O rights.
func (m *MemSurface) Open(path string, mode OpenMode) (Handle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch mode {
	case OpenReadOnly, OpenWriteOnly, OpenReadWrite, OpenAppend, OpenTruncate, OpenPath:
	default:
		return nil, ErrInvalid
	}
	if !validPath(path) {
		return nil, ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		if err != ErrNotFound || mode == OpenReadOnly || mode == OpenPath {
			return nil, err
		}
		if _, ok := m.dirs[parentOf(path)]; !ok {
			return nil, ErrNotFound
		}
		f = &memFile{mode: 0o644, mtime: m.tick(), version: 1}
		m.files[path] = f
	} else if mode == OpenTruncate {
		f.data = nil
		m.bumpLocked(f)
	}
	var cursor int64
	if mode == OpenAppend {
		cursor = int64(len(f.data))
	}
	return &memHandle{mem: m, file: f, mode: mode, cursor: cursor}, nil
}

// Stat implements Surface.Stat.
func (m *MemSurface) Stat(path string) (Stat, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return Stat{}, ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return Stat{}, err
	}
	return Stat{Size: int64(len(f.data)), Mode: f.mode, UID: f.uid, GID: f.gid, MTime: f.mtime}, nil
}

// Truncate implements Surface.Truncate.
func (m *MemSurface) Truncate(path string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) || size < 0 {
		return ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return err
	}
	nb := make([]byte, size)
	copy(nb, f.data)
	f.data = nb
	m.bumpLocked(f)
	return nil
}

// Chmod implements Surface.Chmod.
func (m *MemSurface) Chmod(path string, mode uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return err
	}
	f.mode = mode & 0o7777
	return nil
}

// Chown implements Surface.Chown.
func (m *MemSurface) Chown(path string, uid, gid uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return err
	}
	f.uid = uid
	f.gid = gid
	f.mode &^= SetUIDBit | SetGIDBit
	return nil
}

// Utimens implements Surface.Utimens.
func (m *MemSurface) Utimens(path string, mtime int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return err
	}
	f.mtime = mtime
	return nil
}

// Unlink implements Surface.Unlink.
func (m *MemSurface) Unlink(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	if _, ok := m.files[path]; ok {
		delete(m.files, path)
		return nil
	}
	if _, ok := m.links[path]; ok {
		delete(m.links, path)
		return nil
	}
	if _, ok := m.dirs[path]; ok {
		return ErrIsDir
	}
	return ErrNotFound
}

// Rename implements Surface.Rename for files and symlinks.
func (m *MemSurface) Rename(oldPath, newPath string, noReplace bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(oldPath) || !validPath(newPath) {
		return ErrInvalid
	}
	if oldPath == newPath {
		if _, ok := m.files[oldPath]; ok {
			return nil
		}
		if _, ok := m.links[oldPath]; ok {
			return nil
		}
		if _, ok := m.dirs[oldPath]; ok {
			return ErrIsDir
		}
		return ErrNotFound
	}
	isFile := false
	if _, ok := m.files[oldPath]; ok {
		isFile = true
	} else if _, ok := m.links[oldPath]; !ok {
		if _, ok := m.dirs[oldPath]; ok {
			return ErrIsDir
		}
		return ErrNotFound
	}
	if _, ok := m.dirs[parentOf(newPath)]; !ok {
		return ErrNotFound
	}
	if _, ok := m.files[newPath]; ok {
		if noReplace {
			return ErrExists
		}
		delete(m.files, newPath)
	} else if _, ok := m.links[newPath]; ok {
		if noReplace {
			return ErrExists
		}
		delete(m.links, newPath)
	} else if _, ok := m.dirs[newPath]; ok {
		return ErrIsDir
	}
	if isFile {
		m.files[newPath] = m.files[oldPath]
		delete(m.files, oldPath)
	} else {
		m.links[newPath] = m.links[oldPath]
		delete(m.links, oldPath)
	}
	return nil
}

// Mkdir implements Surface.Mkdir.
func (m *MemSurface) Mkdir(path string, perm uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	if _, ok := m.files[path]; ok {
		return ErrExists
	}
	if _, ok := m.links[path]; ok {
		return ErrExists
	}
	if _, ok := m.dirs[path]; ok {
		return ErrExists
	}
	if _, ok := m.dirs[parentOf(path)]; !ok {
		return ErrNotFound
	}
	m.dirs[path] = perm & 0o7777
	return nil
}

// Rmdir implements Surface.Rmdir.
func (m *MemSurface) Rmdir(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	if _, ok := m.files[path]; ok {
		return ErrNotDir
	}
	if _, ok := m.links[path]; ok {
		return ErrNotDir
	}
	if _, ok := m.dirs[path]; !ok {
		return ErrNotFound
	}
	prefix := path + "/"
	for name := range m.files {
		if strings.HasPrefix(name, prefix) {
			return ErrNotEmpty
		}
	}
	for name := range m.links {
		if strings.HasPrefix(name, prefix) {
			return ErrNotEmpty
		}
	}
	for name := range m.dirs {
		if strings.HasPrefix(name, prefix) {
			return ErrNotEmpty
		}
	}
	delete(m.dirs, path)
	return nil
}

// Symlink implements Surface.Symlink.
func (m *MemSurface) Symlink(target, linkPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(linkPath) || target == "" {
		return ErrInvalid
	}
	if _, ok := m.files[linkPath]; ok {
		return ErrExists
	}
	if _, ok := m.links[linkPath]; ok {
		return ErrExists
	}
	if _, ok := m.dirs[linkPath]; ok {
		return ErrExists
	}
	if _, ok := m.dirs[parentOf(linkPath)]; !ok {
		return ErrNotFound
	}
	m.links[linkPath] = target
	return nil
}

// Readlink implements Surface.Readlink.
func (m *MemSurface) Readlink(linkPath string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(linkPath) {
		return "", ErrInvalid
	}
	if tgt, ok := m.links[linkPath]; ok {
		return tgt, nil
	}
	if _, ok := m.files[linkPath]; ok {
		return "", ErrInvalid
	}
	if _, ok := m.dirs[linkPath]; ok {
		return "", ErrInvalid
	}
	return "", ErrNotFound
}

// ReadRange implements Surface.ReadRange.
func (m *MemSurface) ReadRange(path string, offset, length int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return nil, ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, ErrInvalid
	}
	if offset >= int64(len(f.data)) {
		return nil, ErrUnsatisfiableRange
	}
	end := offset + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	out := make([]byte, end-offset)
	copy(out, f.data[offset:end])
	return out, nil
}

// Append implements Surface.Append on an existing file.
func (m *MemSurface) Append(path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return err
	}
	f.data = append(f.data, data...)
	f.mode &^= SetUIDBit | SetGIDBit
	m.bumpLocked(f)
	return nil
}

// Sync implements Surface.Sync.
func (m *MemSurface) Sync(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	if _, err := m.resolveLocked(path); err != nil {
		return err
	}
	return nil
}

// CompareAndWrite implements Surface.CompareAndWrite.
func (m *MemSurface) CompareAndWrite(path string, offset int64, data []byte, token uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return err
	}
	if f.version != token {
		return ErrPrecondition{Expected: token, Actual: f.version}
	}
	if offset < 0 {
		return ErrInvalid
	}
	f.data = growForWrite(f.data, offset, data)
	f.mode &^= SetUIDBit | SetGIDBit
	m.bumpLocked(f)
	return nil
}

// Revision implements Surface.Revision.
func (m *MemSurface) Revision(path string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return 0, ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return 0, err
	}
	return f.version, nil
}

type memHandle struct {
	mem    *MemSurface
	file   *memFile
	mode   OpenMode
	cursor int64
	closed bool
}

func (h *memHandle) readable() bool {
	return h.mode == OpenReadOnly || h.mode == OpenReadWrite
}

func (h *memHandle) writable() bool {
	return h.mode == OpenWriteOnly || h.mode == OpenReadWrite || h.mode == OpenAppend || h.mode == OpenTruncate
}

// PRead implements Handle.PRead.
func (h *memHandle) PRead(offset int64, length int) ([]byte, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if !h.readable() {
		return nil, ErrAccess
	}
	if offset < 0 || length < 0 {
		return nil, ErrInvalid
	}
	return sliceClamp(h.file.data, offset, int64(length)), nil
}

// PWrite implements Handle.PWrite.
func (h *memHandle) PWrite(offset int64, data []byte) (int, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return 0, ErrClosed
	}
	if !h.writable() {
		return 0, ErrAccess
	}
	if offset < 0 {
		return 0, ErrInvalid
	}
	h.file.data = growForWrite(h.file.data, offset, data)
	h.file.mode &^= SetUIDBit | SetGIDBit
	h.mem.bumpLocked(h.file)
	return len(data), nil
}

// Read implements Handle.Read.
func (h *memHandle) Read(length int) ([]byte, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if !h.readable() {
		return nil, ErrAccess
	}
	if length < 0 {
		return nil, ErrInvalid
	}
	out := sliceClamp(h.file.data, h.cursor, int64(length))
	h.cursor += int64(len(out))
	return out, nil
}

// Write implements Handle.Write.
func (h *memHandle) Write(data []byte) (int, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return 0, ErrClosed
	}
	if !h.writable() {
		return 0, ErrAccess
	}
	if h.mode == OpenAppend {
		h.cursor = int64(len(h.file.data))
	}
	h.file.data = growForWrite(h.file.data, h.cursor, data)
	h.cursor += int64(len(data))
	h.file.mode &^= SetUIDBit | SetGIDBit
	h.mem.bumpLocked(h.file)
	return len(data), nil
}

// Truncate implements Handle.Truncate.
func (h *memHandle) Truncate(size int64) error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	if !h.writable() {
		return ErrAccess
	}
	if size < 0 {
		return ErrInvalid
	}
	nb := make([]byte, size)
	copy(nb, h.file.data)
	h.file.data = nb
	h.mem.bumpLocked(h.file)
	return nil
}

// Sync implements Handle.Sync.
func (h *memHandle) Sync() error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	return nil
}

// Close implements Handle.Close and is idempotent.
func (h *memHandle) Close() error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	h.closed = true
	return nil
}

// SeekData implements SeekHandle.SeekData over dense bytes: every offset
// below the size is data, so in-range offsets return themselves and
// anything at or past EOF fails with ErrUnsatisfiableRange (ENXIO).
func (h *memHandle) SeekData(off int64) (int64, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return 0, ErrClosed
	}
	if off < 0 {
		return 0, ErrInvalid
	}
	if off >= int64(len(h.file.data)) {
		return 0, ErrUnsatisfiableRange
	}
	return off, nil
}

// SeekHole implements SeekHandle.SeekHole over dense bytes: the only hole
// is at the size itself, so every offset at or below the size reports the
// size and anything past it fails with ErrUnsatisfiableRange (ENXIO).
func (h *memHandle) SeekHole(off int64) (int64, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return 0, ErrClosed
	}
	if off < 0 {
		return 0, ErrInvalid
	}
	size := int64(len(h.file.data))
	if off > size {
		return 0, ErrUnsatisfiableRange
	}
	return size, nil
}

// PunchHole implements PunchHoler by zero-filling: on dense bytes a
// deallocated span reads back as zeros, observably equal to a real hole
// punch, so the oracle emulates the bytes rather than failing. Like every
// other data mutation it clears setuid/setgid and advances the revision.
func (m *MemSurface) PunchHole(path string, off, length int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validPath(path) {
		return ErrInvalid
	}
	if off < 0 || length < 0 {
		return ErrInvalid
	}
	f, err := m.resolveLocked(path)
	if err != nil {
		return err
	}
	if length == 0 || off >= int64(len(f.data)) {
		return nil
	}
	end := off + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	for i := off; i < end; i++ {
		f.data[i] = 0
	}
	f.mode &^= SetUIDBit | SetGIDBit
	m.bumpLocked(f)
	return nil
}
