package rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/FarelRA/storhub/internal/test"
)

type restConformAdapter struct {
	handler http.Handler
	project string
	mu      sync.Mutex
	etagBy  map[uint64]string
	// umask masks CreateFile modes (see test.UmaskSurface; zero
	// disables masking). Guarded by mu alongside etagBy.
	umask uint32
}

var _ test.UmaskSurface = (*restConformAdapter)(nil)

// SetUmask implements test.UmaskSurface.SetUmask.
func (a *restConformAdapter) SetUmask(mask uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.umask = mask & 0o777
}

// pcSessionMode maps open modes to session fopen strings. Only "r" and
// "r+" are used: the adapter enforces fd read/write legality locally
// (canPCRead/canPCWrite, allowed by the harness contract), create and
// truncate happen through stateless verbs before the session opens, and
// append cursors stay adapter-side, so the session never needs
// create/truncate/append bits.
func pcSessionMode(m test.OpenMode) string {
	if m == test.OpenReadOnly {
		return "r"
	}
	return "r+"
}

func (a *restConformAdapter) openSession(path, mode, ttl string) (string, error) {
	target := "/api/v1/handles"
	status, _, data := a.doJSON(http.MethodPost, target, sessionOpenRequest{Project: a.project, Path: path, Mode: mode, TTL: ttl}, nil)
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
	// The status-to-sentinel knowledge lives in test.SentinelForStatus;
	// this delegate only adds the operation context. Precondition
	// failures stay local (they carry no test sentinel), as does the
	// loud unknown-status report with the code attached.
	if status == http.StatusPreconditionFailed {
		return fmt.Errorf("%s: precondition failed: %s", what, detail)
	}
	if status >= 200 && status < 300 {
		return nil
	}
	if sentinel := test.SentinelForStatus(status, detail+" "+code); sentinel != nil {
		return fmt.Errorf("%s: %w: %s", what, sentinel, detail)
	}
	return fmt.Errorf("%s: %w: status %d: %s", what, test.ErrInvalid, status, detail)
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
		return nil, "", fmt.Errorf("stat %s: %w: decode: %v", pcPath, test.ErrInvalid, err)
	}
	if nr.Entry == nil {
		return nil, "", fmt.Errorf("stat %s: %w: empty entry", pcPath, test.ErrNotFound)
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
	return current, nil, "", fmt.Errorf("too many levels resolving %s: %w", pcPath, test.ErrLoop)
}

func (a *restConformAdapter) CreateFile(path string, perm uint32, exclusive bool) error {
	rp := trimPCPath(path)
	target := pcBase(a.project) + "/ops/create"
	status, _, data := a.doJSON(http.MethodPost, target, pathRequest{Path: rp}, nil)
	if status == http.StatusCreated || status == http.StatusOK {
		return a.applyCreateMask(path, perm)
	}
	if status == http.StatusConflict {
		// Idempotence emulation lives HERE in the harness, not in the
		// product: POST /ops/create is create-only by design (409
		// when the path exists), so a non-exclusive create re-stats and
		// treats "already there" as success. The server never fakes it.
		if !exclusive {
			if _, _, err := a.statEntry(path); err == nil {
				return nil
			}
		}
		return fmt.Errorf("create %s: %w: %s", path, test.ErrExists, strings.TrimSpace(string(data)))
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("create %s: %w: %s", path, test.ErrNotFound, strings.TrimSpace(string(data)))
	}
	return mapPCStatus(status, data, "create "+path)
}

// applyCreateMask enforces the configured umask after a create: the
// product create path carries no mode, so the fake always records
// 0o644, and the adapter chmods to the masked mode when (and only
// when) a mask is configured, leaving the unmasked path byte-identical.
func (a *restConformAdapter) applyCreateMask(path string, perm uint32) error {
	a.mu.Lock()
	umask := a.umask
	a.mu.Unlock()
	if umask == 0 {
		return nil
	}
	target := (perm & 0o7777) &^ (umask & 0o777)
	if target == 0o644 {
		return nil
	}
	entry, _, err := a.statEntry(path)
	if err != nil {
		return err
	}
	if entry.Mode&0o7777 == target {
		return nil
	}
	return a.Chmod(path, target)
}

func (a *restConformAdapter) Open(path string, mode test.OpenMode, disp test.CreateDisposition) (test.Handle, error) {
	switch mode {
	case test.OpenReadOnly, test.OpenWriteOnly, test.OpenReadWrite, test.OpenAppend, test.OpenTruncate:
	default:
		return nil, fmt.Errorf("open %s: %w: unknown mode", path, test.ErrInvalid)
	}
	switch disp {
	case test.CreateNever, test.CreateIfMissing:
	default:
		return nil, fmt.Errorf("open %s: %w: unknown create disposition", path, test.ErrInvalid)
	}
	resolved, entry, _, statErr := a.resolve(path)
	if statErr != nil {
		if !errors.Is(statErr, test.ErrNotFound) {
			return nil, statErr
		}
		// Creation intent travels in disp, like O_CREAT: CreateNever
		// reports the missing path, while CreateIfMissing creates it
		// before opening. OpenReadOnly never creates.
		if disp == test.CreateNever || mode == test.OpenReadOnly || mode == test.OpenPath {
			return nil, fmt.Errorf("open %s: %w", path, test.ErrNotFound)
		}
		if err := a.CreateFile(path, 0o644, false); err != nil {
			if !errors.Is(err, test.ErrExists) {
				return nil, err
			}
		}
		if resolved, _, _, statErr = a.resolve(path); statErr != nil {
			return nil, statErr
		}
	} else {
		if entry.IsDir {
			return nil, fmt.Errorf("open %s: %w", path, test.ErrIsDir)
		}
	}
	if mode == test.OpenTruncate {
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
	id, err := a.openSession(trimPCPath(resolved), pcSessionMode(mode), "")
	if err != nil {
		return nil, err
	}
	if entry == nil {
		if _, entry, _, statErr = a.resolve(resolved); statErr != nil {
			return nil, statErr
		}
	}
	return &restConformHandle{adapter: a, rest: trimPCPath(resolved), mode: mode, session: id, ino: entry.Inode}, nil
}

func (a *restConformAdapter) Stat(path string) (test.Stat, error) {
	_, entry, _, err := a.resolve(path)
	if err != nil {
		return test.Stat{}, err
	}
	return test.Stat{
		Size:  entry.Size,
		Mode:  entry.Mode,
		UID:   entry.UID,
		GID:   entry.GID,
		MTime: entry.ModifiedAt,
	}, nil
}

func (a *restConformAdapter) Truncate(path string, size int64) error {
	if size < 0 {
		return fmt.Errorf("truncate %s: %w: negative size", path, test.ErrInvalid)
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
		return "", fmt.Errorf("readlink %s: %w: not a symlink", linkPath, test.ErrInvalid)
	}
	return entry.SymlinkTarget, nil
}

func (a *restConformAdapter) ReadRange(path string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("readrange %s: %w: negative offset or length", path, test.ErrInvalid)
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
		return nil, fmt.Errorf("readrange %s: %w", path, test.ErrUnsatisfiableRange)
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
	// No standalone flush endpoint exists; a dryrun prune with ?sync=1
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
		return 0, fmt.Errorf("revision %s: %w: missing etag", path, test.ErrInvalid)
	}
	return a.rememberToken(etag), nil
}

func (a *restConformAdapter) CompareAndWrite(path string, offset int64, data []byte, token uint64) error {
	if offset < 0 {
		return fmt.Errorf("cas %s: %w: negative offset", path, test.ErrInvalid)
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
		return test.ErrPrecondition{Expected: token, Actual: actual}
	}
	_ = resp
	return mapPCStatus(status, resp, "cas "+path)
}
