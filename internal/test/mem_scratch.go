package test

import (
	"time"
)

// memScratch is a pathless open file description over private bytes,
// the oracle half of ScratchSession. Writes stage privately; Link
// appends a pending name (validating absence like create, and rejecting
// already-pending names); Close publishes the staged image to ALL
// pending names, each with create semantics (every name pre-validated
// first, so a name taken since link fails the whole close instead of
// overwriting, leaving the handle open) or discards when unlinked.
// Relink replaces the whole pending set with one path. Expiry is
// checked lazily on every operation like the product's
// action-triggered sweep: no goroutines, no timers.
type memScratch struct {
	mem      *MemSurface
	data     []byte
	pending  PendingNames
	cursor   int64
	closed   bool
	deadline time.Time
}

var _ ScratchSession = (*memScratch)(nil)

// OpenScratch implements SessionSurface.OpenScratch on the oracle. The
// TTL folds through the shared clamp (non-positive takes the default,
// over-max caps at the limit), the same rule the fakes apply.
func (m *MemSurface) OpenScratch(ttl time.Duration) (ScratchSession, error) {
	return &memScratch{mem: m, deadline: SessionExpiryAt(time.Now(), ttl, DefaultTTL, MaxTTL)}, nil
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

// nameTakenLocked reports whether path is occupied by a file, link, or
// directory. Caller holds h.mem.mu.
func (h *memScratch) nameTakenLocked(path string) bool {
	if _, ok := h.mem.files[path]; ok {
		return true
	}
	if _, ok := h.mem.links[path]; ok {
		return true
	}
	if _, ok := h.mem.dirs[path]; ok {
		return true
	}
	return false
}

// Link implements ScratchSession.Link: validates absence now (like
// create) and appends the name to the pending set for close. Linking an
// occupied path, an already-pending name, or a bad path fails without
// touching the staged bytes.
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
	if h.nameTakenLocked(path) {
		return ErrExists
	}
	if _, ok := m.dirs[parentOf(path)]; !ok {
		return ErrNotFound
	}
	return h.pending.Add(path)
}

// Relink implements ScratchSession.Relink: same validation as Link,
// replacing the whole pending set with one path so a close wedged on a
// taken target can be rescued.
func (h *memScratch) Relink(path string) error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if err := h.checkUsable(); err != nil {
		return err
	}
	if _, err := h.linkTarget(path); err != nil {
		return err
	}
	if h.nameTakenLocked(path) {
		return ErrExists
	}
	if _, ok := h.mem.dirs[parentOf(path)]; !ok {
		return ErrNotFound
	}
	h.pending.Replace(path)
	return nil
}

// Close implements Handle.Close: every pending name is pre-validated
// first, so a name taken since link fails the whole close with
// ErrExists WITHOUT consuming the description (relink can still rescue
// it) and publishes nothing; free names all publish the staged image,
// each with create semantics; no name discards. A second close fails
// with ErrClosed.
func (h *memScratch) Close() error {
	h.mem.mu.Lock()
	defer h.mem.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	if err := h.checkUsable(); err != nil {
		return err
	}
	if h.pending.Empty() {
		h.closed = true
		return nil
	}
	for _, name := range h.pending.List() {
		if h.nameTakenLocked(name) {
			return ErrExists
		}
	}
	for _, name := range h.pending.List() {
		h.mem.files[name] = &memFile{mode: uint32(0o644) &^ (h.mem.umask & 0o777), mtime: h.mem.tick(), version: 1, data: append([]byte(nil), h.data...)}
	}
	h.closed = true
	return nil
}
