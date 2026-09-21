package test

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

// Truncate implements Handle.Truncate. Like the path form it clears
// setuid/setgid and advances the revision.
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
	h.file.mode &^= SetUIDBit | SetGIDBit
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

// Close implements Handle.Close: the first call releases the handle,
// later calls fail with ErrClosed like close(2) on a closed fd.
func (h *memHandle) Close() error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
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
