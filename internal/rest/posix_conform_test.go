package rest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FarelRA/storhub/internal/posixconform"
)

const pcProject = "demo"

type restConformAdapter struct {
	handler http.Handler
	project string
	mu      sync.Mutex
	etagBy  map[uint64]string
}

type restConformHandle struct {
	adapter *restConformAdapter
	rest    string
	mode    posixconform.OpenMode
	mu      sync.Mutex
	cursor  int64
	closed  bool
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

// pcSessionMode maps open modes to session fopen strings. Only "r" and
// "r+" are used: the adapter enforces fd read/write legality locally
// (canPCRead/canPCWrite, allowed by the harness contract), create and
// truncate happen through stateless verbs before the session opens, and
// append cursors stay adapter-side, so the session never needs
// create/truncate/append bits.
func pcSessionMode(m posixconform.OpenMode) string {
	if m == posixconform.OpenReadOnly {
		return "r"
	}
	return "r+"
}

func (a *restConformAdapter) openSession(path, mode string) (string, error) {
	target := "/api/v1/handles"
	status, _, data := a.doJSON(http.MethodPost, target, sessionOpenRequest{Project: a.project, Path: path, Mode: mode}, nil)
	if status != http.StatusCreated {
		return "", mapPCStatus(status, data, "open session "+path)
	}
	var resp sessionOpenResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", fmt.Errorf("open session %s: decode handle: %v", path, err)
	}
	if resp.Handle == "" {
		return "", fmt.Errorf("open session %s: empty handle", path)
	}
	return resp.Handle, nil
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

func trimPCPath(p string) string {
	return strings.TrimPrefix(p, "/")
}

func pcBase(project string) string {
	return "/api/v1/projects/" + project
}

func etagToken(etag string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(etag))
	return h.Sum64()
}

func (a *restConformAdapter) rememberToken(etag string) uint64 {
	tok := etagToken(etag)
	a.mu.Lock()
	if a.etagBy == nil {
		a.etagBy = map[uint64]string{}
	}
	a.etagBy[tok] = etag
	a.mu.Unlock()
	return tok
}

func (a *restConformAdapter) lookupETag(tok uint64) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.etagBy[tok]
	return v, ok
}

func (a *restConformAdapter) do(method, target string, body io.Reader, headers map[string]string) (int, http.Header, []byte) {
	req := httptest.NewRequest(method, target, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, req)
	resp := rec.Result()
	data, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, resp.Header, data
}

func (a *restConformAdapter) doJSON(method, target string, payload any, headers map[string]string) (int, http.Header, []byte) {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, []byte(err.Error())
		}
		body = bytes.NewReader(raw)
		if headers == nil {
			headers = map[string]string{}
		}
		headers["Content-Type"] = "application/json"
	}
	return a.do(method, target, body, headers)
}

func pcErrCode(data []byte) (string, string) {
	var env restError
	if err := json.Unmarshal(data, &env); err != nil {
		return "", strings.TrimSpace(string(data))
	}
	return env.Error.Code, env.Error.Message
}

func mapPCStatus(status int, data []byte, what string) error {
	code, msg := pcErrCode(data)
	detail := strings.TrimSpace(string(data))
	if msg != "" {
		detail = msg
	}
	lower := strings.ToLower(detail + " " + code)
	switch status {
	case http.StatusNotFound:
		return fmt.Errorf("%s: %w: %s", what, posixconform.ErrNotFound, detail)
	case http.StatusConflict:
		switch {
		case strings.Contains(lower, "not empty"):
			return fmt.Errorf("%s: %w: %s", what, posixconform.ErrNotEmpty, detail)
		case strings.Contains(lower, "is a directory"):
			return fmt.Errorf("%s: %w: %s", what, posixconform.ErrIsDir, detail)
		case strings.Contains(lower, "not a directory"):
			return fmt.Errorf("%s: %w: %s", what, posixconform.ErrNotDir, detail)
		default:
			return fmt.Errorf("%s: %w: %s", what, posixconform.ErrExists, detail)
		}
	case http.StatusBadRequest:
		return fmt.Errorf("%s: %w: %s", what, posixconform.ErrInvalid, detail)
	case http.StatusRequestedRangeNotSatisfiable:
		return fmt.Errorf("%s: %w: %s", what, posixconform.ErrUnsatisfiableRange, detail)
	case http.StatusPreconditionFailed:
		return fmt.Errorf("%s: precondition failed: %s", what, detail)
	default:
		if status >= 200 && status < 300 {
			return nil
		}
		return fmt.Errorf("%s: %w: status %d: %s", what, posixconform.ErrInvalid, status, detail)
	}
}

func (a *restConformAdapter) statEntry(pcPath string) (*EntryInfo, string, error) {
	rp := trimPCPath(pcPath)
	target := pcBase(a.project) + "/nodes?path=" + url.QueryEscape(rp)
	status, header, data := a.do(http.MethodGet, target, nil, nil)
	if status != http.StatusOK {
		return nil, "", mapPCStatus(status, data, "stat "+pcPath)
	}
	var nr nodeResponse
	if err := json.Unmarshal(data, &nr); err != nil {
		return nil, "", fmt.Errorf("stat %s: %w: decode: %v", pcPath, posixconform.ErrInvalid, err)
	}
	if nr.Entry == nil {
		return nil, "", fmt.Errorf("stat %s: %w: empty entry", pcPath, posixconform.ErrNotFound)
	}
	etag := nr.ETag
	if etag == "" {
		etag = header.Get("ETag")
	}
	return nr.Entry, etag, nil
}

// restMaxFollowHops caps adapter-side symlink resolution, matching the
// backend's own loop bound: past it the path reports ErrLoop, never a hang.
const restMaxFollowHops = 40

// resolve follows a final symlink chain like open(2), because nodes stat
// with lstat semantics (a terminal link reports itself). It returns the
// resolved absolute path with its entry and ETag. Dangling targets report
// ErrNotFound; chains past restMaxFollowHops report ErrLoop.
func (a *restConformAdapter) resolve(pcPath string) (string, *EntryInfo, string, error) {
	current := pcPath
	for i := 0; i < restMaxFollowHops; i++ {
		entry, etag, err := a.statEntry(current)
		if err != nil {
			return current, nil, "", err
		}
		if !entry.IsSymlink {
			return current, entry, etag, nil
		}
		target := entry.SymlinkTarget
		if !strings.HasPrefix(target, "/") {
			dir := current[:strings.LastIndex(current, "/")]
			target = dir + "/" + target
		}
		current = target
	}
	return current, nil, "", fmt.Errorf("too many levels resolving %s: %w", pcPath, posixconform.ErrLoop)
}

func (a *restConformAdapter) CreateFile(path string, perm uint32, exclusive bool) error {
	_ = perm
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/ops/create-file"
	status, _, data := a.doJSON(http.MethodPost, target, pathRequest{Path: rp}, nil)
	if status == http.StatusCreated || status == http.StatusOK {
		return nil
	}
	if status == http.StatusConflict {
		// Idempotence emulation lives HERE in the harness, not in the
		// product: POST /ops/create-file is create-only by design (409
		// when the path exists), so a non-exclusive create re-stats and
		// treats "already there" as success. The server never fakes it.
		if !exclusive {
			if _, _, err := a.statEntry(path); err == nil {
				return nil
			}
		}
		return fmt.Errorf("create %s: %w: %s", path, posixconform.ErrExists, strings.TrimSpace(string(data)))
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("create %s: %w: %s", path, posixconform.ErrNotFound, strings.TrimSpace(string(data)))
	}
	return mapPCStatus(status, data, "create "+path)
}

func (a *restConformAdapter) Open(path string, mode posixconform.OpenMode) (posixconform.Handle, error) {
	resolved, entry, _, statErr := a.resolve(path)
	if statErr != nil {
		if !errors.Is(statErr, posixconform.ErrNotFound) {
			return nil, statErr
		}
		if mode == posixconform.OpenReadOnly {
			return nil, fmt.Errorf("open %s: %w", path, posixconform.ErrNotFound)
		}
		if err := a.CreateFile(path, 0o644, false); err != nil {
			if !errors.Is(err, posixconform.ErrExists) {
				return nil, err
			}
		}
		if resolved, _, _, statErr = a.resolve(path); statErr != nil {
			return nil, statErr
		}
	} else {
		if entry.IsDir {
			return nil, fmt.Errorf("open %s: %w", path, posixconform.ErrIsDir)
		}
	}
	if mode == posixconform.OpenTruncate {
		rp := trimPCPath(resolved)
		target := pcBase(a.project) + "/content?path=" + url.QueryEscape(rp) + "&op=truncate&size=0"
		status, _, data := a.do(http.MethodPatch, target, nil, nil)
		if status != http.StatusOK {
			return nil, mapPCStatus(status, data, "open truncate "+path)
		}
	}
	// Every handle also opens a server-side session: the open-file
	// description behind close commit/discard/follow and detached IO.
	// Failing here fails the open loudly instead of silently dropping
	// POSIX close semantics.
	id, err := a.openSession(trimPCPath(resolved), pcSessionMode(mode))
	if err != nil {
		return nil, err
	}
	return &restConformHandle{adapter: a, rest: trimPCPath(resolved), mode: mode, session: id}, nil
}

func (a *restConformAdapter) Stat(path string) (posixconform.Stat, error) {
	_, entry, _, err := a.resolve(path)
	if err != nil {
		return posixconform.Stat{}, err
	}
	return posixconform.Stat{
		Size:  entry.Size,
		Mode:  entry.Mode,
		UID:   entry.UID,
		GID:   entry.GID,
		MTime: entry.ModifiedAt,
	}, nil
}

func (a *restConformAdapter) Truncate(path string, size int64) error {
	if size < 0 {
		return fmt.Errorf("truncate %s: %w: negative size", path, posixconform.ErrInvalid)
	}
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/content?path=" + url.QueryEscape(rp) + "&op=truncate&size=" + fmt.Sprintf("%d", size)
	status, _, data := a.do(http.MethodPatch, target, nil, nil)
	if status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, data, "truncate "+path)
}

func (a *restConformAdapter) Chmod(path string, mode uint32) error {
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/ops/chmod"
	status, _, data := a.doJSON(http.MethodPost, target, chmodRequest{Path: rp, Mode: mode}, nil)
	if status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, data, "chmod "+path)
}

func (a *restConformAdapter) Chown(path string, uid, gid uint32) error {
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/ops/chown"
	status, _, data := a.doJSON(http.MethodPost, target, chownRequest{Path: rp, UID: uid, GID: gid}, nil)
	if status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, data, "chown "+path)
}

func (a *restConformAdapter) Utimens(path string, mtime int64) error {
	rp := trimPCPath(path)
	stamp := time.Unix(0, mtime).UTC()
	target := pcBase(a.project) + "/ops/utimes"
	status, _, data := a.doJSON(http.MethodPost, target, utimesRequest{Path: rp, Atime: stamp, Mtime: stamp}, nil)
	if status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, data, "utimens "+path)
}

func (a *restConformAdapter) Unlink(path string) error {
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/ops/unlink"
	status, _, data := a.doJSON(http.MethodPost, target, pathRequest{Path: rp}, nil)
	if status == http.StatusNoContent || status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, data, "unlink "+path)
}

func (a *restConformAdapter) Rename(oldPath, newPath string, noReplace bool) error {
	target := pcBase(a.project) + "/ops/rename"
	status, _, data := a.doJSON(http.MethodPost, target, renameRequest{OldPath: trimPCPath(oldPath), NewPath: trimPCPath(newPath), NoReplace: noReplace}, nil)
	if status == http.StatusOK || status == http.StatusCreated {
		return nil
	}
	return mapPCStatus(status, data, "rename "+oldPath)
}

func (a *restConformAdapter) Mkdir(path string, perm uint32) error {
	_ = perm
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/ops/mkdir"
	status, _, data := a.doJSON(http.MethodPost, target, pathRequest{Path: rp}, nil)
	if status == http.StatusCreated || status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, data, "mkdir "+path)
}

func (a *restConformAdapter) Rmdir(path string) error {
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/ops/rmdir"
	status, _, data := a.doJSON(http.MethodPost, target, pathRequest{Path: rp}, nil)
	if status == http.StatusNoContent || status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, data, "rmdir "+path)
}

func (a *restConformAdapter) Symlink(target, linkPath string) error {
	route := pcBase(a.project) + "/ops/symlink"
	status, _, data := a.doJSON(http.MethodPost, route, symlinkRequest{Target: target, LinkPath: trimPCPath(linkPath)}, nil)
	if status == http.StatusCreated || status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, data, "symlink "+linkPath)
}

func (a *restConformAdapter) Readlink(linkPath string) (string, error) {
	entry, _, err := a.statEntry(linkPath)
	if err != nil {
		return "", err
	}
	if !entry.IsSymlink {
		return "", fmt.Errorf("readlink %s: %w: not a symlink", linkPath, posixconform.ErrInvalid)
	}
	return entry.SymlinkTarget, nil
}

func (a *restConformAdapter) ReadRange(path string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("readrange %s: %w: negative offset or length", path, posixconform.ErrInvalid)
	}
	if length == 0 {
		return []byte{}, nil
	}
	// Reads follow a final link like read(2).
	resolved, _, _, err := a.resolve(path)
	if err != nil {
		return nil, err
	}
	rp := trimPCPath(resolved)
	target := pcBase(a.project) + "/content?path=" + url.QueryEscape(rp)
	end := offset + length - 1
	headers := map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", offset, end)}
	status, _, data := a.do(http.MethodGet, target, nil, headers)
	switch status {
	case http.StatusOK, http.StatusPartialContent:
		return data, nil
	case http.StatusRequestedRangeNotSatisfiable:
		return nil, fmt.Errorf("readrange %s: %w", path, posixconform.ErrUnsatisfiableRange)
	default:
		return nil, mapPCStatus(status, data, "readrange "+path)
	}
}

func (a *restConformAdapter) Append(path string, data []byte) error {
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/content?path=" + url.QueryEscape(rp) + "&op=append"
	var body io.Reader
	if len(data) > 0 {
		body = bytes.NewReader(data)
	}
	status, _, resp := a.do(http.MethodPatch, target, body, nil)
	if status == http.StatusOK {
		return nil
	}
	return mapPCStatus(status, resp, "append "+path)
}

func (a *restConformAdapter) Sync(path string) error {
	// No standalone flush endpoint exists; a dry-run prune with ?sync=1
	// drains the project journal without mutating anything, which is
	// exactly the durability the scenario pins.
	target := pcBase(a.project) + "/ops/prune?sync=1"
	status, _, data := a.doJSON(http.MethodPost, target, pruneRequest{Scope: "all", DryRun: true}, nil)
	if status != http.StatusOK {
		return mapPCStatus(status, data, "sync "+path)
	}
	return nil
}

func (a *restConformAdapter) Revision(path string) (uint64, error) {
	_, etag, err := a.statEntry(path)
	if err != nil {
		return 0, err
	}
	if etag == "" {
		return 0, fmt.Errorf("revision %s: %w: missing etag", path, posixconform.ErrInvalid)
	}
	return a.rememberToken(etag), nil
}

func (a *restConformAdapter) CompareAndWrite(path string, offset int64, data []byte, token uint64) error {
	if offset < 0 {
		return fmt.Errorf("cas %s: %w: negative offset", path, posixconform.ErrInvalid)
	}
	etag, ok := a.lookupETag(token)
	if !ok {
		etag = fmt.Sprintf("\"%016x\"", token)
	}
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/content?path=" + url.QueryEscape(rp) + "&op=write&offset=" + fmt.Sprintf("%d", offset)
	var body io.Reader
	if len(data) > 0 {
		body = bytes.NewReader(data)
	}
	status, _, resp := a.do(http.MethodPatch, target, body, map[string]string{"If-Match": etag})
	if status == http.StatusOK {
		return nil
	}
	if status == http.StatusPreconditionFailed || status == http.StatusConflict {
		_, currentETag, statErr := a.statEntry(path)
		var actual uint64
		if statErr == nil && currentETag != "" {
			actual = a.rememberToken(currentETag)
		}
		return posixconform.ErrPrecondition{Expected: token, Actual: actual}
	}
	_ = resp
	return mapPCStatus(status, resp, "cas "+path)
}

func canPCRead(m posixconform.OpenMode) bool {
	return m == posixconform.OpenReadOnly || m == posixconform.OpenReadWrite
}

func canPCWrite(m posixconform.OpenMode) bool {
	return m != posixconform.OpenReadOnly
}

func (h *restConformHandle) checkClosed() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("handle: %w", posixconform.ErrClosed)
	}
	return nil
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
		return nil, fmt.Errorf("pread: %w", posixconform.ErrAccess)
	}
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("pread: %w: negative offset or length", posixconform.ErrInvalid)
	}
	if length == 0 {
		return []byte{}, nil
	}
	got, err := h.getRange(offset, length)
	if err == nil {
		return got, nil
	}
	if !errors.Is(err, posixconform.ErrNotFound) {
		return nil, err
	}
	// The path is gone (unlinked or renamed away) but the open
	// description survives: serve the session pin plus staged writes.
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
		return 0, fmt.Errorf("pwrite: %w", posixconform.ErrAccess)
	}
	if offset < 0 {
		return 0, fmt.Errorf("pwrite: %w: negative offset", posixconform.ErrInvalid)
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
		return nil, fmt.Errorf("read: %w", posixconform.ErrAccess)
	}
	if length < 0 {
		return nil, fmt.Errorf("read: %w: negative length", posixconform.ErrInvalid)
	}
	if length == 0 {
		return []byte{}, nil
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
		return 0, fmt.Errorf("write: %w", posixconform.ErrAccess)
	}
	if len(data) == 0 {
		return 0, nil
	}
	off := cursor
	if mode == posixconform.OpenAppend {
		entry, _, err := h.adapter.statEntry("/" + h.rest)
		if err != nil {
			if !errors.Is(err, posixconform.ErrNotFound) {
				return 0, err
			}
			// Unlinked mid-append: the session size is the end.
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
		return fmt.Errorf("truncate: %w", posixconform.ErrAccess)
	}
	if size < 0 {
		return fmt.Errorf("truncate: %w: negative size", posixconform.ErrInvalid)
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
		return fmt.Errorf("close: %w", posixconform.ErrClosed)
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

func TestPosixConformREST(t *testing.T) {
	if os.Getenv("STORHUB_CONFORMANCE") == "" {
		t.Skip("conformance suite runs only with STORHUB_CONFORMANCE=1 (Phase 0 RED: known deviations open)")
	}
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	adapter := &restConformAdapter{handler: handler, project: pcProject, etagBy: map[uint64]string{}}
	results := posixconform.Run(adapter, posixconform.Filter(posixconform.Table, posixconform.SurfaceREST))
	passed, failed := posixconform.Summary(results)
	for _, r := range results {
		if r.Pass {
			t.Logf("PASS %s", r.Name)
		} else {
			t.Logf("FAIL %s: %s", r.Name, r.Error)
		}
	}
	t.Logf("summary: %d passed, %d failed", passed, failed)
	if failed > 0 {
		t.Fatalf("%d scenarios failed", failed)
	}
}
