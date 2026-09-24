package rest

// rest_conform_scratch_test.go: conformance scratch sessions over REST.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/FarelRA/storhub/internal/test"
	"net/http"
	"sync"
	"time"
)

func (a *restConformAdapter) OpenScratch(ttl time.Duration) (test.ScratchSession, error) {
	ttlRaw := ""
	if ttl > 0 {
		ttlRaw = ttl.String()
	}
	id, err := a.openSession("", "r+", ttlRaw)
	if err != nil {
		return nil, err
	}
	return &restScratchHandle{adapter: a, session: id}, nil
}

// restScratchHandle is a pathless open-file description over one
// server-side session: writes stage (no sync: an unlinked scratch has
// nothing to commit to), Link names the staged image, Close publishes
// or discards. A close that fails (taken target) leaves the description
// open for Relink, mirroring close(2) never consuming a failed close.

type restScratchHandle struct {
	adapter *restConformAdapter
	session string
	mu      sync.Mutex
	cursor  int64
	closed  bool
}

func (h *restScratchHandle) checkClosed() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("scratch: %w", test.ErrClosed)
	}
	return nil
}

func (h *restScratchHandle) PRead(offset int64, length int) ([]byte, error) {
	if err := h.checkClosed(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("scratch pread: %w: negative offset or length", test.ErrInvalid)
	}
	if length == 0 {
		return []byte{}, nil
	}
	target := "/api/v1/handles/" + h.session + "?offset=" + fmt.Sprintf("%d", offset) + "&length=" + fmt.Sprintf("%d", length)
	status, _, data := h.adapter.do(http.MethodGet, target, nil, nil)
	switch status {
	case http.StatusOK, http.StatusPartialContent:
		return data, nil
	default:
		return nil, mapPCStatus(status, data, "scratch pread")
	}
}

func (h *restScratchHandle) PWrite(offset int64, data []byte) (int, error) {
	if err := h.checkClosed(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, fmt.Errorf("scratch pwrite: %w: negative offset", test.ErrInvalid)
	}
	if len(data) == 0 {
		return 0, nil
	}
	// Staged only: an unlinked scratch has no publish target, so no
	// sync follows (unlike pathed handles, where every write commits).
	target := "/api/v1/handles/" + h.session + "/write"
	status, _, resp := h.adapter.doJSON(http.MethodPost, target, sessionWriteRequest{Offset: offset, Data: base64.StdEncoding.EncodeToString(data)}, nil)
	if status != http.StatusOK {
		return 0, mapPCStatus(status, resp, "scratch pwrite")
	}
	var out sessionWriteResponse
	if err := json.Unmarshal(resp, &out); err != nil {
		return 0, fmt.Errorf("scratch pwrite: decode written: %v", err)
	}
	return out.Written, nil
}

func (h *restScratchHandle) Read(length int) ([]byte, error) {
	if err := h.checkClosed(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	cursor := h.cursor
	h.mu.Unlock()
	if length < 0 {
		return nil, fmt.Errorf("scratch read: %w: negative length", test.ErrInvalid)
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

func (h *restScratchHandle) Write(data []byte) (int, error) {
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

func (h *restScratchHandle) Truncate(size int64) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	if size < 0 {
		return fmt.Errorf("scratch truncate: %w: negative size", test.ErrInvalid)
	}
	target := "/api/v1/handles/" + h.session + "/truncate"
	status, _, data := h.adapter.doJSON(http.MethodPost, target, sessionTruncateRequest{Size: size}, nil)
	if status != http.StatusOK && status != http.StatusNoContent {
		return mapPCStatus(status, data, "scratch truncate")
	}
	return nil
}

func (h *restScratchHandle) Sync() error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	// Unlinked scratch has nothing to commit to; the staged bytes are
	// already visible to the description itself, so sync succeeds as a
	// no-op like fsync on an unwritten fd. Linked-but-uncommitted
	// handles also succeed here without publishing: publishing is
	// Close's job.
	return nil
}

func (h *restScratchHandle) Link(path string) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	target := "/api/v1/handles/" + h.session + "/link"
	status, _, data := h.adapter.doJSON(http.MethodPost, target, sessionLinkRequest{Path: trimPCPath(path)}, nil)
	if status != http.StatusOK {
		return mapPCStatus(status, data, "scratch link "+path)
	}
	return nil
}

func (h *restScratchHandle) Relink(path string) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	target := "/api/v1/handles/" + h.session + "/relink"
	status, _, data := h.adapter.doJSON(http.MethodPost, target, sessionLinkRequest{Path: trimPCPath(path)}, nil)
	if status != http.StatusOK {
		return mapPCStatus(status, data, "scratch relink "+path)
	}
	return nil
}

func (h *restScratchHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return fmt.Errorf("scratch close: %w", test.ErrClosed)
	}
	h.mu.Unlock()
	// A failed close (taken target) leaves the description open for
	// Relink: only success consumes it.
	target := "/api/v1/handles/" + h.session + "/close?sync=1"
	status, _, data := h.adapter.doJSON(http.MethodPost, target, struct{}{}, nil)
	if status != http.StatusOK {
		return mapPCStatus(status, data, "scratch close")
	}
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}
