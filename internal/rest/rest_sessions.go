package rest

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/go-chi/chi/v5"
)

// rest_sessions.go: Phase 2B session manager over REST.
//
// Routes (all under the same auth middleware as the project routes, so the
// request context already carries the authenticated identity):
//
//	POST   /handles                  open: {project, path?, mode, ttl?} -> 201 {handle}
//	GET    /handles/{h}              stat when no range query: 200 {handle,project,path,size,dirty,mode}
//	GET    /handles/{h}?offset=&length=  ranged read: 200 (206 when partial) raw bytes
//	POST   /handles/{h}/write        {offset, data-base64} -> 200 {handle, written}
//	POST   /handles/{h}/truncate     {size} -> 200 stat document
//	POST   /handles/{h}/sync         commit without closing -> 200 stat document
//	POST   /handles/{h}/link         {path} -> 200 stat document
//	POST   /handles/{h}/close        commit and destroy -> 200 {handle, status}
//	DELETE /handles/{h}              alias of close -> 204
//
// Read choice: offset-only positional reads (pread style). No cursor state
// is kept server side: every GET read names its range explicitly, so two
// clients sharing nothing but a handle id cannot skew each other. Append
// callers pass the end offset in write (ignored in append mode, where the
// manager always stages at the current end); document, do not work around.
//
// Identity: handlers call h.clientFor(r) and forward r.Context() unchanged.
// The manager reads caller identity from ctx on every call (DAC once at
// open, ownership on every later op), so no identity is ever synthesized
// here. The share lane (restrictedClient via readOnlyShare) denies every
// session verb: share visitors are read-only by construction.
//
// Error mapping (session-first, then the package conventions):
//
//	stale (expired or unknown handle) -> 410 Gone, code "gone", reason kept
//	owner mismatch                    -> 403 Forbidden, code "forbidden"
//	project or user busy (caps)       -> 429 Too Many Requests, code "rate_limited"
//	unlinked scratch / already linked -> 409 Conflict, code "conflict"
//	mode violations (EBADF)           -> 400 Bad Request, code "bad_request"

// registerSessionRoutes owns dispatch for the handles tree: the router maps
// methods, handlers never switch on r.Method.
func (h *restHandler) registerSessionRoutes(r chi.Router) {
	r.Post("/", h.handleSessionOpen)
	r.Get("/{handle}", h.handleSessionGet)
	r.Post("/{handle}/write", h.handleSessionWrite)
	r.Post("/{handle}/truncate", h.handleSessionTruncate)
	r.Post("/{handle}/sync", h.handleSessionSync)
	r.Post("/{handle}/link", h.handleSessionLink)
	r.Post("/{handle}/relink", h.handleSessionRelink)
	r.Post("/{handle}/close", h.handleSessionClosePost)
	r.Delete("/{handle}", h.handleSessionCloseDelete)
}

type sessionOpenRequest struct {
	Project string `json:"project"`
	Path    string `json:"path,omitempty"`
	Mode    string `json:"mode"`
	TTL     string `json:"ttl,omitempty"`
}

type sessionOpenResponse struct {
	Handle string `json:"handle"`
}

type sessionStatResponse struct {
	Handle  string `json:"handle"`
	Project string `json:"project"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	Dirty   bool   `json:"dirty"`
	Stale   bool   `json:"stale"`
	Mode    string `json:"mode"`
}

type sessionWriteRequest struct {
	Offset int64  `json:"offset"`
	Data   string `json:"data"`
}

type sessionWriteResponse struct {
	Handle  string `json:"handle"`
	Written int    `json:"written"`
}

type sessionTruncateRequest struct {
	Size int64 `json:"size"`
}

type sessionLinkRequest struct {
	Path string `json:"path"`
}

type sessionCloseResponse struct {
	Handle string `json:"handle"`
	Status string `json:"status"`
}

func statSessionResponse(handle string, stat storage.SessionStat) sessionStatResponse {
	return sessionStatResponse{
		Handle:  handle,
		Project: stat.Project,
		Path:    stat.Path,
		Size:    stat.Size,
		Dirty:   stat.Dirty,
		Stale:   stat.Stale,
		Mode:    stat.Mode.String(),
	}
}

// writeSessionError maps session failures to status codes before falling
// back to the package conventions for everything else.
func (h *restHandler) writeSessionError(w http.ResponseWriter, err error) {
	var stale *storage.StaleSessionError
	switch {
	case errors.As(err, &stale):
		h.writeError(w, http.StatusGone, "gone", err.Error())
	case errors.Is(err, storage.ErrStaleSession):
		h.writeError(w, http.StatusGone, "gone", err.Error())
	case errors.Is(err, storage.ErrSessionOwnerMismatch):
		h.writeMappedError(w, errForbidden(err.Error()))
	case errors.Is(err, storage.ErrSessionProjectBusy),
		errors.Is(err, storage.ErrSessionUserBusy):
		h.writeError(w, http.StatusTooManyRequests, "rate_limited", err.Error())
	case errors.Is(err, storage.ErrSessionUnlinked),
		errors.Is(err, storage.ErrSessionLinked):
		h.writeMappedError(w, &restStatusError{status: http.StatusConflict, message: err.Error()})
	case errors.Is(err, syscall.EBADF):
		h.writeMappedError(w, errBadRequest(err.Error()))
	default:
		h.writeMappedError(w, err)
	}
}

func (h *restHandler) handleSessionOpen(w http.ResponseWriter, r *http.Request) {
	var req sessionOpenRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("project", req.Project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if strings.TrimSpace(req.Mode) == "" {
		h.writeMappedError(w, errBadRequest("mode is required"))
		return
	}
	mode, err := storage.ParseOpenMode(strings.TrimSpace(req.Mode))
	if err != nil {
		h.writeMappedError(w, errBadRequest(err.Error()))
		return
	}
	var opts []storage.SessionOption
	if strings.TrimSpace(req.TTL) != "" {
		ttl, err := time.ParseDuration(strings.TrimSpace(req.TTL))
		if err != nil {
			h.writeMappedError(w, errBadRequest("ttl must be a Go duration string like \"5m\""))
			return
		}
		opts = append(opts, storage.WithSessionTTL(ttl))
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	defer h.traceOp(r, "session-open", req.Project, req.Path, "mode", req.Mode)(&err)
	id, err := client.OpenSession(r.Context(), req.Project, req.Path, mode, opts...)
	if err != nil {
		h.writeSessionError(w, err)
		return
	}
	h.writeJSON(w, http.StatusCreated, sessionOpenResponse{Handle: id})
}

// handleSessionGet serves both stat and ranged read on one route: a request
// carrying offset or length reads bytes (pread style, no cursor), a bare
// request stats the handle.
func (h *restHandler) handleSessionGet(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	if strings.TrimSpace(handle) == "" {
		h.writeMappedError(w, errBadRequest("handle is required"))
		return
	}
	query := r.URL.Query()
	if hasQueryKey(r, "offset") || hasQueryKey(r, "length") {
		h.serveSessionRead(w, r, handle, query.Get("offset"), query.Get("length"))
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	stat, err := client.StatSession(r.Context(), handle)
	if err != nil {
		h.writeSessionError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, statSessionResponse(handle, stat))
}

func hasQueryKey(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}

// serveSessionRead streams [offset, offset+length) as raw bytes. Length
// defaults to EOF (resolved via stat); offset defaults to 0. Partial
// answers carry Content-Range with a 206, like /content.
func (h *restHandler) serveSessionRead(w http.ResponseWriter, r *http.Request, handle, rawOffset, rawLength string) {
	var offset int64
	if strings.TrimSpace(rawOffset) != "" {
		parsed, err := parseNonNegativeInt(rawOffset, "offset")
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		offset = parsed
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	stat, err := client.StatSession(r.Context(), handle)
	if err != nil {
		h.writeSessionError(w, err)
		return
	}
	length := stat.Size - offset
	if length < 0 {
		length = 0
	}
	if strings.TrimSpace(rawLength) != "" {
		parsed, err := parseNonNegativeInt(rawLength, "length")
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		length = parsed
	}
	data, err := client.ReadSession(r.Context(), handle, offset, length)
	if err != nil {
		h.writeSessionError(w, err)
		return
	}
	// An empty read never carries a range: offset-at-EOF or an empty file
	// would otherwise emit 206 with an invalid "bytes N-(N-1)/M"
	// Content-Range. Answer plain 200 with no Content-Range instead (416
	// stays reserved for the byte-range endpoint, which fails loud there).
	if len(data) == 0 {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	end := offset + int64(len(data))
	partial := offset != 0 || end < stat.Size
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(end-1, 10)+"/"+strconv.FormatInt(stat.Size, 10))
	}
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func (h *restHandler) handleSessionWrite(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	var req sessionWriteRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if req.Offset < 0 {
		h.writeMappedError(w, errBadRequest("offset must be non-negative"))
		return
	}
	data, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil {
		h.writeMappedError(w, errBadRequest("data must be base64-encoded"))
		return
	}
	if int64(len(data)) > h.opts.MaxPatchBodySize {
		h.writeMappedError(w, errPayloadTooLarge("write payload exceeds the configured limit"))
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	defer h.traceOp(r, "session-write", "", handle)(&err)
	wrote, err := client.WriteSession(r.Context(), handle, req.Offset, data)
	if err != nil {
		h.writeSessionError(w, err)
		return
	}
	// Staged writes are not yet published, but ?sync=1 still drains the
	// project (no-op when nothing is published) so callers get one
	// durability spelling across all session verbs, mirroring closeSession.
	if !h.drainSessionProject(w, r, handle) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.writeJSON(w, http.StatusOK, sessionWriteResponse{Handle: handle, Written: wrote})
}

func (h *restHandler) handleSessionTruncate(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	var req sessionTruncateRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if req.Size < 0 {
		h.writeMappedError(w, errBadRequest("size must be non-negative"))
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	defer h.traceOp(r, "session-truncate", "", handle)(&err)
	if err = client.TruncateSession(r.Context(), handle, req.Size); err != nil {
		h.writeSessionError(w, err)
		return
	}
	if !h.drainSessionProject(w, r, handle) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.respondWithSessionStat(w, r, handle)
}

func (h *restHandler) handleSessionSync(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	// SyncSession commits staged state without closing (commit-then-drain
	// on ?sync=1: the project drain below lands the commit remotely, the
	// same durability closeSession offers).
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	defer h.traceOp(r, "session-sync", "", handle)(&err)
	if err = client.SyncSession(r.Context(), handle); err != nil {
		h.writeSessionError(w, err)
		return
	}
	if !h.drainSessionProject(w, r, handle) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.respondWithSessionStat(w, r, handle)
}

func (h *restHandler) handleSessionLink(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	var req sessionLinkRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	defer h.traceOp(r, "session-link", "", handle)(&err)
	if err = client.LinkSession(r.Context(), handle, req.Path); err != nil {
		h.writeSessionError(w, err)
		return
	}
	if !h.drainSessionProject(w, r, handle) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.respondWithSessionStat(w, r, handle)
}

// handleSessionRelink rescues a handle whose linked target was taken by a
// concurrent writer: it retargets the staged bytes instead of stranding
// the handle until TTL expiry. Same validation and error mapping as link.
func (h *restHandler) handleSessionRelink(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	var req sessionLinkRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	defer h.traceOp(r, "session-relink", "", handle)(&err)
	if err = client.RelinkSession(r.Context(), handle, req.Path); err != nil {
		h.writeSessionError(w, err)
		return
	}
	if !h.drainSessionProject(w, r, handle) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.respondWithSessionStat(w, r, handle)
}

// handleSessionClosePost answers POST .../close with a JSON ack; the DELETE
// alias answers 204 with no body, like every other successful delete.
func (h *restHandler) handleSessionClosePost(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	if !h.closeSession(w, r, handle) {
		return
	}
	h.writeJSON(w, http.StatusOK, sessionCloseResponse{Handle: handle, Status: "closed"})
}

func (h *restHandler) handleSessionCloseDelete(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	if !h.closeSession(w, r, handle) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// closeSession stats the handle for its project, closes (committing), then
// honors ?sync=1 by draining the project, reusing the maybeDrain pattern.
// False means the handler already answered.
func (h *restHandler) closeSession(w http.ResponseWriter, r *http.Request, handle string) bool {
	if strings.TrimSpace(handle) == "" {
		h.writeMappedError(w, errBadRequest("handle is required"))
		return false
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return false
	}
	defer h.traceOp(r, "session-close", "", handle)(&err)
	stat, err := client.StatSession(r.Context(), handle)
	if err != nil {
		h.writeSessionError(w, err)
		return false
	}
	if err = client.CloseSession(r.Context(), handle); err != nil {
		h.writeSessionError(w, err)
		return false
	}
	if !h.maybeDrain(w, r, stat.Project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return false
	}
	return true
}

// drainSessionProject stats the handle for its project, then honors
// ?sync=1 by draining the project, reusing the maybeDrain pattern from
// closeSession. Staged-only verbs (write, truncate, link, relink) share it
// so every mutating session verb answers one durability spelling. False
// means the handler already answered.
func (h *restHandler) drainSessionProject(w http.ResponseWriter, r *http.Request, handle string) bool {
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return false
	}
	stat, err := client.StatSession(r.Context(), handle)
	if err != nil {
		h.writeSessionError(w, err)
		return false
	}
	return h.maybeDrain(w, r, stat.Project)
}

func (h *restHandler) respondWithSessionStat(w http.ResponseWriter, r *http.Request, handle string) {
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	stat, err := client.StatSession(r.Context(), handle)
	if err != nil {
		h.writeSessionError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, statSessionResponse(handle, stat))
}

// Session verbs on the auth wrapper forward the request context unchanged:
// the manager reads caller identity from ctx on every call, so any extra
// REST-layer DAC here would either duplicate or bypass the open-time check.
// No identity is synthesized; the ctx passes through untouched.

func (c *authorizedClient) OpenSession(ctx context.Context, project, path string, mode storage.OpenMode, opts ...storage.SessionOption) (string, error) {
	return c.base.OpenSession(ctx, project, path, mode, opts...)
}

func (c *authorizedClient) ReadSession(ctx context.Context, handleID string, offset, length int64) ([]byte, error) {
	return c.base.ReadSession(ctx, handleID, offset, length)
}

func (c *authorizedClient) WriteSession(ctx context.Context, handleID string, offset int64, data []byte) (int, error) {
	return c.base.WriteSession(ctx, handleID, offset, data)
}

func (c *authorizedClient) TruncateSession(ctx context.Context, handleID string, size int64) error {
	return c.base.TruncateSession(ctx, handleID, size)
}

func (c *authorizedClient) StatSession(ctx context.Context, handleID string) (storage.SessionStat, error) {
	return c.base.StatSession(ctx, handleID)
}

func (c *authorizedClient) SyncSession(ctx context.Context, handleID string) error {
	return c.base.SyncSession(ctx, handleID)
}

func (c *authorizedClient) LinkSession(ctx context.Context, handleID, path string) error {
	return c.base.LinkSession(ctx, handleID, path)
}

func (c *authorizedClient) RelinkSession(ctx context.Context, handleID, path string) error {
	return c.base.RelinkSession(ctx, handleID, path)
}

func (c *authorizedClient) CloseSession(ctx context.Context, handleID string) error {
	return c.base.CloseSession(ctx, handleID)
}

// Session verbs on the share lane are denied like every other mutation:
// share visitors are read-only, and the chokepoint test requires each Client
// method to answer a zero-argument call with an access-denied error.

func (readOnlyShare) OpenSession(_ context.Context, _, _ string, _ storage.OpenMode, _ ...storage.SessionOption) (string, error) {
	return "", errReadOnly()
}

func (readOnlyShare) ReadSession(_ context.Context, _ string, _, _ int64) ([]byte, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) WriteSession(_ context.Context, _ string, _ int64, _ []byte) (int, error) {
	return 0, errReadOnly()
}

func (readOnlyShare) TruncateSession(_ context.Context, _ string, _ int64) error {
	return errReadOnly()
}

func (readOnlyShare) StatSession(_ context.Context, _ string) (storage.SessionStat, error) {
	return storage.SessionStat{}, errReadOnly()
}

func (readOnlyShare) SyncSession(_ context.Context, _ string) error {
	return errReadOnly()
}

func (readOnlyShare) LinkSession(_ context.Context, _, _ string) error {
	return errReadOnly()
}

func (readOnlyShare) RelinkSession(_ context.Context, _, _ string) error {
	return errReadOnly()
}

func (readOnlyShare) CloseSession(_ context.Context, _ string) error {
	return errReadOnly()
}
