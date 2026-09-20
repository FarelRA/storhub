package test

import (
	"time"
)

// memScratch is a pathless open file description over private bytes,
// the oracle half of ScratchSession. Writes stage privately; Link binds
// a pending name (validating absence like create); Close publishes the
// staged image at the pending name with create semantics (a name taken
// since link fails instead of overwriting) or discards when unlinked.
// Expiry is checked lazily on every operation like the product's
// action-triggered sweep: no goroutines, no timers.
type memScratch struct {
	mem      *MemSurface
	data     []byte
	name     string
	cursor   int64
	closed   bool
	deadline time.Time
}

var _ ScratchSession = (*memScratch)(nil)

// OpenScratch implements SessionSurface.OpenScratch on the oracle.
func (m *MemSurface) OpenScratch(ttl time.Duration) (ScratchSession, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &memScratch{mem: m, deadline: time.Now().Add(ttl)}, nil
}

func (h *memScratch) expired() bool {
	// Single wall-clock comparison shared with the fakes (see Expired).
	return Expired(h.deadline, time.Now())
}

func (h *memScratch) checkUsable() error {
	if h.closed {
		return ErrClosed
	}
	if h.expired() {
		return ErrStale
	}
	return nil
}

func (h *memScratch) linkTarget(path string) (*MemSurface, error) {
	if !validPath(path) {
		return nil, ErrInvalid
	}
	return h.mem, nil
}

// PRead implements Handle.PRead over staged bytes.
func (h *memScratch) PRead(offset int64, length int) ([]byte, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if err := h.checkUsable(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, ErrInvalid
	}
	return sliceClamp(h.data, offset, int64(length)), nil
}

// PWrite implements Handle.PWrite over staged bytes.
func (h *memScratch) PWrite(offset int64, data []byte) (int, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if err := h.checkUsable(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, ErrInvalid
	}
	h.data = growForWrite(h.data, offset, data)
	return len(data), nil
}

// Read implements Handle.Read over staged bytes.
func (h *memScratch) Read(length int) ([]byte, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if err := h.checkUsable(); err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, ErrInvalid
	}
	out := sliceClamp(h.data, h.cursor, int64(length))
	h.cursor += int64(len(out))
	return out, nil
}

// Write implements Handle.Write over staged bytes.
func (h *memScratch) Write(data []byte) (int, error) {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if err := h.checkUsable(); err != nil {
		return 0, err
	}
	h.data = growForWrite(h.data, h.cursor, data)
	h.cursor += int64(len(data))
	return len(data), nil
}

// Truncate implements Handle.Truncate over staged bytes.
func (h *memScratch) Truncate(size int64) error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if err := h.checkUsable(); err != nil {
		return err
	}
	if size < 0 {
		return ErrInvalid
	}
	nb := make([]byte, size)
	copy(nb, h.data)
	h.data = nb
	return nil
}

// Sync implements Handle.Sync: staged bytes are already visible to the
// description itself, so sync is a no-op success like fsync on an
// unlinked fd.
func (h *memScratch) Sync() error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	return h.checkUsable()
}

// Link implements ScratchSession.Link: validates absence now (like
// create) and stages the name for close. Linking an occupied path, a
// second link, or a bad path fails without touching the staged bytes.
func (h *memScratch) Link(path string) error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if err := h.checkUsable(); err != nil {
		return err
	}
	m, err := h.linkTarget(path)
	if err != nil {
		return err
	}
	if h.name != "" {
		return ErrExists
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
	h.name = path
	return nil
}

// Relink implements ScratchSession.Relink: same validation as Link,
// rebinding the pending name so a close wedged on a taken target can
// be rescued.
func (h *memScratch) Relink(path string) error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if err := h.checkUsable(); err != nil {
		return err
	}
	if _, err := h.linkTarget(path); err != nil {
		return err
	}
	if _, ok := h.mem.files[path]; ok {
		return ErrExists
	}
	if _, ok := h.mem.links[path]; ok {
		return ErrExists
	}
	if _, ok := h.mem.dirs[path]; ok {
		return ErrExists
	}
	if _, ok := h.mem.dirs[parentOf(path)]; !ok {
		return ErrNotFound
	}
	h.name = path
	return nil
}

// Close implements Handle.Close: a linked name taken since link fails
// with ErrExists WITHOUT consuming the description (relink can still
// rescue it); a free name publishes the staged image; no name
// discards. A second close fails with ErrClosed.
func (h *memScratch) Close() error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	if err := h.checkUsable(); err != nil {
		return err
	}
	if h.name == "" {
		h.closed = true
		return nil
	}
	if _, ok := h.mem.files[h.name]; ok {
		return ErrExists
	}
	if _, ok := h.mem.links[h.name]; ok {
		return ErrExists
	}
	if _, ok := h.mem.dirs[h.name]; ok {
		return ErrExists
	}
	h.mem.files[h.name] = &memFile{mode: 0o644, mtime: h.mem.tick(), version: 1, data: append([]byte(nil), h.data...)}
	h.closed = true
	return nil
}
