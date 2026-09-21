package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/FarelRA/storhub/internal/test"
	"github.com/FarelRA/storhub/storhub"
)

// ---------------------------------------------------------------------------
// Handle with adapter-owned cursor.
// ---------------------------------------------------------------------------

type cliHandle struct {
	mu      sync.Mutex
	surface *cliPOSIXSurface
	path    string
	mode    test.OpenMode
	cursor  int64
	closed  bool
	// ino pins the open-time inode: after unlink+recreate the path
	// names a new file, and only the pin tells them apart. Backends
	// preserve inode identity across commits, so the pin never needs
	// refreshing — adopting a new inode would rebind the handle to a
	// stranger's file.
	ino uint64
	// session is the server-side open-file description (session open).
	// One-shot verbs carry IO while the path is linked (immediate
	// publish, which is what cross-handle visibility and uncommitted
	// stat size observe); the session carries the pin plus close
	// commit/discard/follow, and serves IO staged after an unlink or
	// rename. Stateless commands plus stateful sessions are one CLI
	// surface, and open file descriptions live in the stateful half.
	session string
}

// cliSessionMode maps open modes to session fopen strings. Only "r" and
// "r+" are used: the adapter enforces fd legality locally, create and
// truncate happen through one-shot verbs first, and append cursors stay
// adapter-side.
func cliSessionMode(m test.OpenMode) string {
	if m == test.OpenReadOnly {
		return "r"
	}
	return "r+"
}

func (s *cliPOSIXSurface) sessionOpen(rel, mode, ttl string) (string, error) {
	args := []string{"session", "open", "--token", "x", s.project}
	if rel != "" {
		args = append(args, rel)
	}
	args = append(args, "--mode", mode)
	if ttl != "" {
		args = append(args, "--ttl", ttl)
	}
	out, err := s.runCLI(args)
	if err != nil {
		return "", pcTranslateErr(err)
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		return "", fmt.Errorf("session open %s: empty handle", rel)
	}
	return id, nil
}

func (h *cliHandle) sessionRead(offset int64, length int) ([]byte, error) {
	out, err := h.surface.runCLI([]string{"session", "read", "--token", "x", "--handle", h.session,
		"--offset", strconv.FormatInt(offset, 10), "--length", strconv.Itoa(length)})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	return out, nil
}

func (h *cliHandle) sessionWrite(offset int64, data []byte) (int, error) {
	// Text travels via argv, which is exact for the ASCII payloads the
	// table uses; binary callers use session write with "-" (stdin).
	if _, err := h.surface.runCLI([]string{"session", "write", "--token", "x", "--handle", h.session,
		strconv.FormatInt(offset, 10), string(data)}); err != nil {
		return 0, pcTranslateErr(err)
	}
	return len(data), nil
}

func (h *cliHandle) sessionSync() error {
	if _, err := h.surface.runCLI([]string{"session", "sync", "--token", "x", "--handle", h.session}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliHandle) sessionTruncate(size int64) error {
	if _, err := h.surface.runCLI([]string{"session", "truncate", "--token", "x", "--handle", h.session, strconv.FormatInt(size, 10)}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliHandle) sessionSize() (int64, error) {
	out, err := h.surface.runCLI([]string{"session", "stat", "--token", "x", "--handle", h.session, "--json"})
	if err != nil {
		return 0, pcTranslateErr(err)
	}
	var doc struct {
		Size int64 `json:"size"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		return 0, fmt.Errorf("session stat: decode size: %v", err)
	}
	return doc.Size, nil
}

func (h *cliHandle) readable() bool {
	return h.mode == test.OpenReadOnly || h.mode == test.OpenReadWrite
}

func (h *cliHandle) writable() bool {
	return h.mode == test.OpenWriteOnly || h.mode == test.OpenReadWrite ||
		h.mode == test.OpenAppend || h.mode == test.OpenTruncate
}

// statLinkedLocked stats h.path and reports whether it still names the
// open-time inode, returning the entry when it does. A recycled name
// (same path, new inode after unlink+recreate) must serve the pinned
// session, never the new file. Caller holds h.mu, matching catLocked.
func (h *cliHandle) statLinkedLocked() (entry *storhub.EntryInfo, linked bool, err error) {
	ino := h.ino
	entry, err = h.surface.statViaCLI(h.path)
	if err != nil {
		if errors.Is(err, test.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return entry, entry.Inode == ino, nil
}

// catLocked streams the whole file through `cat`; caller holds h.mu.
func (h *cliHandle) catLocked() ([]byte, error) {
	rel, err := cliPath(h.path)
	if err != nil {
		return nil, err
	}
	data, err := h.surface.runCLI([]string{"cat", "--token", "x", h.surface.project, rel})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	return data, nil
}

func (h *cliHandle) PRead(offset int64, length int) ([]byte, error) {
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
	if _, linked, err := h.statLinkedLocked(); err != nil {
		return nil, err
	} else if !linked {
		// Detached (unlinked, renamed away, or recycled): serve the
		// session pin plus staged writes.
		sess, serr := h.sessionRead(offset, length)
		if serr != nil {
			return nil, serr
		}
		return sess, nil
	}
	data, err := h.catLocked()
	if err != nil {
		// The path is gone (unlinked or renamed away) but the open
		// description survives: serve the session pin plus staged
		// writes. Only NotFound falls back; anything else propagates.
		if !errors.Is(pcTranslateErr(err), test.ErrNotFound) {
			return nil, err
		}
		sess, serr := h.sessionRead(offset, length)
		if serr != nil {
			return nil, serr
		}
		return sess, nil
	}
	if offset >= int64(len(data)) || length == 0 {
		return []byte{}, nil
	}
	end := offset + int64(length)
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return append([]byte(nil), data[offset:end]...), nil
}

func (h *cliHandle) PWrite(offset int64, data []byte) (int, error) {
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

func (h *cliHandle) Read(length int) ([]byte, error) {
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
	if _, linked, err := h.statLinkedLocked(); err != nil {
		return nil, err
	} else if !linked {
		sess, serr := h.sessionRead(h.cursor, length)
		if serr != nil {
			return nil, serr
		}
		h.cursor += int64(len(sess))
		return sess, nil
	}
	data, err := h.catLocked()
	if err != nil {
		if !errors.Is(pcTranslateErr(err), test.ErrNotFound) {
			return nil, err
		}
		sess, serr := h.sessionRead(h.cursor, length)
		if serr != nil {
			return nil, serr
		}
		h.cursor += int64(len(sess))
		return sess, nil
	}
	if h.cursor >= int64(len(data)) || length == 0 {
		return []byte{}, nil
	}
	end := h.cursor + int64(length)
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	out := append([]byte(nil), data[h.cursor:end]...)
	h.cursor = end
	return out, nil
}

func (h *cliHandle) Write(data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, test.ErrClosed
	}
	if !h.writable() {
		return 0, test.ErrAccess
	}
	off := h.cursor
	if h.mode == test.OpenAppend {
		// O_APPEND forces the cursor to the end on every cursor write.
		// Detached mid-append (unlinked, renamed, or recycled), the
		// session size is the end.
		_, linked, err := h.statLinkedLocked()
		if err != nil {
			return 0, err
		}
		if !linked {
			sz, serr := h.sessionSize()
			if serr != nil {
				return 0, serr
			}
			off = sz
		} else {
			st, err := h.surface.statViaCLI(h.path)
			if err != nil {
				return 0, err
			}
			off = st.Size
		}
	}
	wrote, err := h.sessionWrite(off, data)
	if err != nil {
		return 0, err
	}
	if err := h.sessionSync(); err != nil {
		return 0, err
	}
	h.cursor = off + int64(wrote)
	return wrote, nil
}

func (h *cliHandle) Truncate(size int64) error {
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
	// Staged through the session, then committed like writes: the pin
	// refreshes and a later unlink still serves the truncated view.
	if err := h.sessionTruncate(size); err != nil {
		return err
	}
	return h.sessionSync()
}

func (h *cliHandle) Sync() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	// Session sync commits, repins, and the close flag drains the
	// project; plain session sync is the fsync equivalent here because
	// every staged write already committed through write+sync.
	if err := h.sessionSync(); err != nil {
		return err
	}
	if _, err := h.surface.runCLI([]string{"project", "sync", "--token", "x", h.surface.project}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return test.ErrClosed
	}
	h.mu.Unlock()
	// Session close commits staged state (discarding when the path went
	// away, following a rename) with --sync draining, so close means
	// durable exactly like Sync.
	if _, err := h.surface.runCLI([]string{"session", "close", "--token", "x", "--handle", h.session, "--sync"}); err != nil {
		return pcTranslateErr(err)
	}
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}
