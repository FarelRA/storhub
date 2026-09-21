package cli

import (
	"strconv"
	"sync"
	"time"

	"github.com/FarelRA/storhub/internal/test"
)

// cliScratchHandle is a pathless open-file description over one
// server-side session: writes stage (no sync — an unlinked scratch has
// nothing to commit to), Link names the staged image, Close publishes
// or discards. A close that fails (taken target) leaves the description
// open for Relink, mirroring close(2) never consuming a failed close.
type cliScratchHandle struct {
	surface *cliPOSIXSurface
	session string
	mu      sync.Mutex
	cursor  int64
	closed  bool
}

func (s *cliPOSIXSurface) OpenScratch(ttl time.Duration) (test.ScratchSession, error) {
	ttlRaw := ""
	if ttl > 0 {
		ttlRaw = ttl.String()
	}
	id, err := s.sessionOpen("", "r+", ttlRaw)
	if err != nil {
		return nil, err
	}
	return &cliScratchHandle{surface: s, session: id}, nil
}

func (h *cliScratchHandle) checkClosed() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	return nil
}

func (h *cliScratchHandle) PRead(offset int64, length int) ([]byte, error) {
	if err := h.checkClosed(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, test.ErrInvalid
	}
	if length == 0 {
		return []byte{}, nil
	}
	out, err := h.surface.runCLI([]string{"session", "read", "--token", "x", "--handle", h.session,
		"--offset", strconv.FormatInt(offset, 10), "--length", strconv.Itoa(length)})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	return out, nil
}

func (h *cliScratchHandle) PWrite(offset int64, data []byte) (int, error) {
	if err := h.checkClosed(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, test.ErrInvalid
	}
	if len(data) == 0 {
		return 0, nil
	}
	if _, err := h.surface.runCLI([]string{"session", "write", "--token", "x", "--handle", h.session,
		strconv.FormatInt(offset, 10), string(data)}); err != nil {
		return 0, pcTranslateErr(err)
	}
	return len(data), nil
}

func (h *cliScratchHandle) Read(length int) ([]byte, error) {
	if err := h.checkClosed(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	cursor := h.cursor
	h.mu.Unlock()
	if length < 0 {
		return nil, test.ErrInvalid
	}
	if length == 0 {
		return []byte{}, nil
	}
	got, err := h.PRead(cursor, length)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.cursor = cursor + int64(len(got))
	h.mu.Unlock()
	return got, nil
}

func (h *cliScratchHandle) Write(data []byte) (int, error) {
	if err := h.checkClosed(); err != nil {
		return 0, err
	}
	h.mu.Lock()
	cursor := h.cursor
	h.mu.Unlock()
	if len(data) == 0 {
		return 0, nil
	}
	wrote, err := h.PWrite(cursor, data)
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	h.cursor = cursor + int64(wrote)
	h.mu.Unlock()
	return wrote, nil
}

func (h *cliScratchHandle) Truncate(size int64) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	if size < 0 {
		return test.ErrInvalid
	}
	if _, err := h.surface.runCLI([]string{"session", "truncate", "--token", "x", "--handle", h.session, strconv.FormatInt(size, 10)}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliScratchHandle) Sync() error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	// Unlinked scratch has nothing to commit to; the staged bytes are
	// already visible to the description itself, so sync succeeds as a
	// no-op like fsync on an unwritten fd. Publishing is Close's job.
	return nil
}

func (h *cliScratchHandle) Link(path string) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := h.surface.runCLI([]string{"session", "link", "--token", "x", "--handle", h.session, rel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliScratchHandle) Relink(path string) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := h.surface.runCLI([]string{"session", "relink", "--token", "x", "--handle", h.session, rel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliScratchHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return test.ErrClosed
	}
	h.mu.Unlock()
	if _, err := h.surface.runCLI([]string{"session", "close", "--token", "x", "--handle", h.session, "--sync"}); err != nil {
		return pcTranslateErr(err)
	}
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// Entrypoint.
// ---------------------------------------------------------------------------
