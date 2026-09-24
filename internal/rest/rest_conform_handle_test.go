package rest

// rest_conform_handle_test.go: conformance handle IO over REST sessions.

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/FarelRA/storhub/internal/test"
	"net/http"
	"net/url"
	"sync"
)

type restConformHandle struct {
	adapter *restConformAdapter
	rest    string
	mode    test.OpenMode
	mu      sync.Mutex
	cursor  int64
	closed  bool
	// ino pins the open-time inode. Stateless verbs address the path,
	// but POSIX fds address the inode: after unlink+recreate the path
	// names a new file, and only the pin tells them apart. Backends
	// preserve inode identity across commits (updates carry the
	// existing inode; renames move it), so the pin never needs
	// refreshing: adopting a new inode here would rebind the handle
	// to a stranger's file, which is exactly the bug this prevents.
	ino uint64
	// session is the server-side open-file description (/handles).
	// Stateless verbs carry IO while the path is linked (immediate
	// publish, which is what cross-handle visibility and uncommitted
	// stat size observe); the session carries the pin plus close
	// commit/discard/follow, and serves reads/writes staged after an
	// unlink or rename. This mirrors the surface as designed: stateless
	// endpoints plus stateful handles are one REST surface, and open
	// file descriptions live in the stateful half.
	session string
}

func (h *restConformHandle) sessionSync() error {
	target := "/api/v1/handles/" + h.session + "/sync"
	status, _, data := h.adapter.doJSON(http.MethodPost, target, struct{}{}, nil)
	if status != http.StatusOK && status != http.StatusNoContent {
		return mapPCStatus(status, data, "session sync")
	}
	return nil
}

func (h *restConformHandle) sessionRead(offset int64, length int) ([]byte, error) {
	target := "/api/v1/handles/" + h.session + "?offset=" + fmt.Sprintf("%d", offset) + "&length=" + fmt.Sprintf("%d", length)
	status, _, data := h.adapter.do(http.MethodGet, target, nil, nil)
	switch status {
	case http.StatusOK, http.StatusPartialContent:
		return data, nil
	default:
		return nil, mapPCStatus(status, data, "session pread")
	}
}

func (h *restConformHandle) sessionWrite(offset int64, data []byte) (int, error) {
	target := "/api/v1/handles/" + h.session + "/write"
	status, _, resp := h.adapter.doJSON(http.MethodPost, target, sessionWriteRequest{Offset: offset, Data: base64.StdEncoding.EncodeToString(data)}, nil)
	if status != http.StatusOK {
		return 0, mapPCStatus(status, resp, "session pwrite")
	}
	var out sessionWriteResponse
	if err := json.Unmarshal(resp, &out); err != nil {
		return 0, fmt.Errorf("session pwrite: decode written: %v", err)
	}
	return out.Written, nil
}

func (h *restConformHandle) sessionStat() (sessionStatResponse, error) {
	target := "/api/v1/handles/" + h.session
	status, _, data := h.adapter.do(http.MethodGet, target, nil, nil)
	if status != http.StatusOK {
		return sessionStatResponse{}, mapPCStatus(status, data, "session stat")
	}
	var out sessionStatResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return sessionStatResponse{}, fmt.Errorf("session stat: decode: %v", err)
	}
	return out, nil
}

func (h *restConformHandle) sessionTruncate(size int64) error {
	target := "/api/v1/handles/" + h.session + "/truncate"
	status, _, data := h.adapter.doJSON(http.MethodPost, target, sessionTruncateRequest{Size: size}, nil)
	if status != http.StatusOK && status != http.StatusNoContent {
		return mapPCStatus(status, data, "session truncate")
	}
	return nil
}

func canPCRead(m test.OpenMode) bool {
	return m == test.OpenReadOnly || m == test.OpenReadWrite
}

func canPCWrite(m test.OpenMode) bool {
	return m != test.OpenReadOnly
}

func (h *restConformHandle) checkClosed() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("handle: %w", test.ErrClosed)
	}
	return nil
}

// statLinked stats h.rest and reports whether it still names the
// open-time inode, returning the entry when it does. A recycled name
// (same path, new inode after unlink+recreate) must serve the pinned
// session, never the new file: POSIX fds address the inode, not the
// name. A gone path reports detached with no error; other stat
// failures propagate.

func (h *restConformHandle) statLinked() (entry *EntryInfo, linked bool, err error) {
	h.mu.Lock()
	ino := h.ino
	h.mu.Unlock()
	entry, _, serr := h.adapter.statEntry("/" + h.rest)
	if serr != nil {
		if errors.Is(serr, test.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, serr
	}
	return entry, entry.Inode == ino, nil
}

func (h *restConformHandle) getRange(offset int64, length int) ([]byte, error) {
	target := pcBase(h.adapter.project) + "/content?path=" + url.QueryEscape(h.rest)
	end := offset + int64(length) - 1
	headers := map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", offset, end)}
	status, _, data := h.adapter.do(http.MethodGet, target, nil, headers)
	switch status {
	case http.StatusOK, http.StatusPartialContent:
		return data, nil
	case http.StatusRequestedRangeNotSatisfiable:
		// 416 means the offset sits at or past EOF: the product fails
		// loud with Content-Range */size (never silent truncation), and
		// the harness maps that to an empty read, matching pread-at-EOF.
		return []byte{}, nil
	default:
		return nil, mapPCStatus(status, data, "pread")
	}
}

func (h *restConformHandle) PRead(offset int64, length int) ([]byte, error) {
	if err := h.checkClosed(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	mode := h.mode
	h.mu.Unlock()
	if !canPCRead(mode) {
		return nil, fmt.Errorf("pread: %w", test.ErrAccess)
	}
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("pread: %w: negative offset or length", test.ErrInvalid)
	}
	if length == 0 {
		return []byte{}, nil
	}
	if _, linked, err := h.statLinked(); err != nil {
		return nil, err
	} else if !linked {
		// Detached (unlinked, renamed away, or recycled): serve the
		// session pin plus staged writes.
		return h.sessionRead(offset, length)
	}
	got, err := h.getRange(offset, length)
	if err == nil {
		return got, nil
	}
	if !errors.Is(err, test.ErrNotFound) {
		return nil, err
	}
	// Racedetach between the stat and the fetch: same session fallback.
	return h.sessionRead(offset, length)
}

func (h *restConformHandle) PWrite(offset int64, data []byte) (int, error) {
	if err := h.checkClosed(); err != nil {
		return 0, err
	}
	h.mu.Lock()
	mode := h.mode
	h.mu.Unlock()
	if !canPCWrite(mode) {
		return 0, fmt.Errorf("pwrite: %w", test.ErrAccess)
	}
	if offset < 0 {
		return 0, fmt.Errorf("pwrite: %w: negative offset", test.ErrInvalid)
	}
	if len(data) == 0 {
		return 0, nil
	}
	// Staged through the session, then committed: the bytes publish
	// immediately (cross-handle visibility, stat size) and the pin
	// refreshes, so a later unlink still serves them from the session.
	wrote, err := h.sessionWrite(offset, data)
	if err != nil {
		return 0, err
	}
	if err := h.sessionSync(); err != nil {
		return 0, err
	}
	return wrote, nil
}

func (h *restConformHandle) Read(length int) ([]byte, error) {
	if err := h.checkClosed(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	mode := h.mode
	cursor := h.cursor
	h.mu.Unlock()
	if !canPCRead(mode) {
		return nil, fmt.Errorf("read: %w", test.ErrAccess)
	}
	if length < 0 {
		return nil, fmt.Errorf("read: %w: negative length", test.ErrInvalid)
	}
	if length == 0 {
		return []byte{}, nil
	}
	if _, linked, err := h.statLinked(); err != nil {
		return nil, err
	} else if !linked {
		sess, serr := h.sessionRead(cursor, length)
		if serr != nil {
			return nil, serr
		}
		h.mu.Lock()
		h.cursor = cursor + int64(len(sess))
		h.mu.Unlock()
		return sess, nil
	}
	got, err := h.getRange(cursor, length)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.cursor = cursor + int64(len(got))
	h.mu.Unlock()
	return got, nil
}

func (h *restConformHandle) Write(data []byte) (int, error) {
	if err := h.checkClosed(); err != nil {
		return 0, err
	}
	h.mu.Lock()
	mode := h.mode
	cursor := h.cursor
	h.mu.Unlock()
	if !canPCWrite(mode) {
		return 0, fmt.Errorf("write: %w", test.ErrAccess)
	}
	if len(data) == 0 {
		return 0, nil
	}
	off := cursor
	if mode == test.OpenAppend {
		entry, linked, err := h.statLinked()
		if err != nil {
			return 0, err
		}
		if !linked {
			// Detached mid-append (unlinked, renamed, or recycled):
			// the session size is the end.
			st, serr := h.sessionStat()
			if serr != nil {
				return 0, serr
			}
			off = st.Size
		} else {
			off = entry.Size
		}
	}
	wrote, err := h.PWrite(off, data)
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	h.cursor = off + int64(wrote)
	h.mu.Unlock()
	return wrote, nil
}

func (h *restConformHandle) Truncate(size int64) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	h.mu.Lock()
	mode := h.mode
	h.mu.Unlock()
	if !canPCWrite(mode) {
		return fmt.Errorf("truncate: %w", test.ErrAccess)
	}
	if size < 0 {
		return fmt.Errorf("truncate: %w: negative size", test.ErrInvalid)
	}
	// Staged through the session, then committed like writes: the pin
	// refreshes and a later unlink still serves the truncated view.
	if err := h.sessionTruncate(size); err != nil {
		return err
	}
	return h.sessionSync()
}

func (h *restConformHandle) Sync() error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	// Session sync commits, repins, and honors ?sync=1 with a project
	// drain: the fsync equivalent with the same durability as
	// Surface.Sync.
	target := "/api/v1/handles/" + h.session + "/sync?sync=1"
	status, _, data := h.adapter.doJSON(http.MethodPost, target, struct{}{}, nil)
	if status != http.StatusOK && status != http.StatusNoContent {
		return mapPCStatus(status, data, "handle sync")
	}
	return nil
}

func (h *restConformHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return fmt.Errorf("close: %w", test.ErrClosed)
	}
	h.mu.Unlock()
	// Session close commits staged state (discarding when the path went
	// away, following a rename) and drains with ?sync=1, so close means
	// durable exactly like Surface.Sync.
	target := "/api/v1/handles/" + h.session + "/close?sync=1"
	status, _, data := h.adapter.doJSON(http.MethodPost, target, struct{}{}, nil)
	if status != http.StatusOK {
		return mapPCStatus(status, data, "close")
	}
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}
