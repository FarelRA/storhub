package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/go-chi/chi/v5"
)

const (
	defaultRESTBasePath      = "/api/v1"
	defaultRESTStreamChunk   = 1 << 20
	defaultRESTPatchBodySize = 8 << 20
	defaultRESTShareTTL      = 7 * 24 * 720 * storcfg.PatienceUnit // 7 days
	maxRequestBodyMemory     = 32 << 10
	// panicStackSize bounds the captured goroutine stack on panic recovery.
	panicStackSize = 8192
	// nobodyUID/nobodyGID is the POSIX "nobody" account: share-link visitors
	// get no identity beyond what path permissions grant them.
	nobodyUID = uint32(65534)
	nobodyGID = uint32(65534)
)

// Options tunes the HTTP surface: routing base, streaming sizes, share
// lifetimes, signing keys, and auth mode.
type Options struct {
	BasePath string
	// DefaultProject pins the console to a single project when the server
	// was started as `storhub serve <project> ...`.
	DefaultProject   string
	StreamChunkSize  int64
	MaxPatchBodySize int64
	ShareTTL         time.Duration
	// MaxShareTTL bounds client-requested share lifetimes; zero uses the
	// default of one week.
	MaxShareTTL     time.Duration
	ShareSigningKey []byte
	Auth            *AuthOptions
	// AllowAnonymous explicitly opts into serving every route without any
	// authentication. It never happens by accident.
	AllowAnonymous bool
}

// Re-exported model and filesystem-view types for handler signatures.
type (
	// FileMetadata is a stored file entry with chunks and POSIX metadata.
	FileMetadata = metadata.FileMeta
	// MetadataRevision identifies one committed metadata state.
	MetadataRevision = metadata.MetadataRevision
	// EntryInfo is the stat-style view of a path.
	EntryInfo = shfs.EntryInfo
	// DirEntry is one child name plus kind in a directory listing.
	DirEntry = shfs.DirEntry
	// FSStats aggregates project-wide counts.
	FSStats = shfs.FSStats
	// NodeKind discriminates file system node types.
	NodeKind = metadata.NodeKind
)

// Re-exported node-kind constants for response shaping.
const (
	// NodeKindFile is the regular-file node kind.
	NodeKindFile = metadata.NodeKindFile
	// NodeKindSymlink is the symlink node kind.
	NodeKindSymlink = metadata.NodeKindSymlink
)

// Shared sentinel: no such project path.
var (
	// ErrNotFound reports a missing project path.
	ErrNotFound = shfs.ErrNotFound
)

// Client is context-first: every operation carries the request context so
// cancellation and deadlines propagate from the HTTP request all the way
// down into storage. Method names mirror the storage layer's *Context
// variants, which *storage.StorHub implements directly.
type Client interface {
	CreateFileContext(ctx context.Context, project, filePath string) (*metadata.FileMeta, error)
	MkdirContext(ctx context.Context, project, dirPath string) error
	DeleteFileContext(ctx context.Context, project, filePath string, opts ...shfs.MutateOption) error
	RmdirContext(ctx context.Context, project, dirPath string, opts ...shfs.MutateOption) error
	RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...shfs.MutateOption) error
	CopyContext(ctx context.Context, project, srcPath, dstPath string) error
	CloneRange(ctx context.Context, project, src string, srcOff int64, dst string, dstOff int64, length int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error)
	TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error)
	AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error)
	WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error)
	PatchFileContext(ctx context.Context, project, filePath string, offset, deleteSize int64, edit []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error)
	ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error)
	StatPathContext(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error)
	ReadDirContext(ctx context.Context, project, dirPath string) ([]shfs.DirEntry, error)
	StatFSContext(ctx context.Context, project string) (*shfs.FSStats, error)
	SymlinkContext(ctx context.Context, project, target, linkPath string) (*metadata.FileMeta, error)
	ReadlinkContext(ctx context.Context, project, linkPath string) (string, error)
	LinkContext(ctx context.Context, project, existingPath, newPath string) (*metadata.FileMeta, error)
	ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error
	ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error
	ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error
	SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte, mode ...shfs.XAttrMode) error
	GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error)
	ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error)
	RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error
	// RevisionContext reports the project's current metadata revision for
	// use with fs.WithExpectedRevision preconditions.
	RevisionContext(ctx context.Context, project string) (string, error)
	ListMetadataRevisionsContext(ctx context.Context, project string) ([]metadata.MetadataRevision, error)
	RollbackMetadataContext(ctx context.Context, project, commitSHA string) error
	RevertPathContext(ctx context.Context, project, path, commitSHA string) error
	PruneContext(ctx context.Context, project, scope string, keep int, dryRun bool) (*storage.PruneResult, error)
	// Degraded-mode operations: DegradedProjects lists latched projects,
	// ReEnableProject clears one latch (the only path back to healthy),
	// PressureSnapshot exposes the operator pressure ledger.
	DegradedProjects() ([]string, error)
	ReEnableProject(project string) error
	PressureSnapshot() (storage.PressureSnapshot, error)
	PressureFailureStreak(project string) (uint64, error)
	PressurePendingDepth(project string) (int, error)
	DeleteProjectContext(ctx context.Context, project string) error
	ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (*metadata.FileMeta, error)
	// DrainProjectContext blocks until everything published before the call
	// lands in the remote commit (the storage fsync primitive). It backs
	// the ?sync=1 opt-in on every mutating endpoint via maybeDrain.
	DrainProjectContext(ctx context.Context, project string) error
	// OpenSession opens a stateful file handle (server-held handles).
	// signatures mirror *storage.StorHub directly so the real hub satisfies
	// this interface with no adapter; handlers must forward the request
	// context unchanged so the manager sees the authenticated identity.
	OpenSession(ctx context.Context, project, path string, mode storage.OpenMode, opts ...storage.SessionOption) (string, error)
	ReadSession(ctx context.Context, handleID string, offset, length int64) ([]byte, error)
	WriteSession(ctx context.Context, handleID string, offset int64, data []byte) (int, error)
	TruncateSession(ctx context.Context, handleID string, size int64) error
	StatSession(ctx context.Context, handleID string) (storage.SessionStat, error)
	SyncSession(ctx context.Context, handleID string) error
	LinkSession(ctx context.Context, handleID, path string) error
	RelinkSession(ctx context.Context, handleID, path string) error
	CloseSession(ctx context.Context, handleID string) error
}

type restHandler struct {
	client       Client
	opts         Options
	shares       *shareRegistry
	logger       *slog.Logger
	shareSignKey ed25519.PrivateKey
}

type contextKey string

const clientCtxKey contextKey = "restclient"

// clientFor resolves the per-request Client placed in the context by the
// auth middleware. A foreign value under clientCtxKey is a middleware bug:
// fail CLOSED with a 500-class error (logged, answering generic
// internal_error through writeMappedError) rather than silently falling
// back to the raw unrestricted client, which would turn every project
// route into an unauthenticated pass-through.
func (h *restHandler) clientFor(r *http.Request) (Client, error) {
	if value := r.Context().Value(clientCtxKey); value != nil {
		client, ok := value.(Client)
		if !ok {
			typeName := ""
			if t := reflect.TypeOf(value); t != nil {
				typeName = t.String()
			}
			logging.Error(h.logger, "rest: context value under clientCtxKey does not implement Client; failing closed", "type", typeName)
			return nil, &restStatusError{status: http.StatusInternalServerError, message: "rest: context client does not implement Client"}
		}
		return client, nil
	}
	return h.client, nil
}

// HTTP status/byte capture lives in internal/logging (HTTPRecorder): one
// package owns one job. See logging/http_recorder.go.

type shareRegistry struct {
	mu    sync.RWMutex
	items map[string]*shareRecord
	// revoked maps deleted share IDs to the deleted record's expiry so
	// stateless redemption stops honoring them immediately. Entries are
	// dropped once the underlying JWT would have expired on its own, so
	// the map cannot grow without bound. Per-registry (per handler) by
	// design: no package-level state shared across hubs or tests.
	revoked map[string]time.Time
}

type shareRecord struct {
	// ID is the short opaque registry identifier. It is never a credential;
	// the redemption routes (GET /shares/{token}...) take the signed JWT
	// itself as the path segment, while the management routes
	// (/projects/{p}/shares/{id}) use this ID. Token holds the signed JWT
	// for bearer authentication of scoped reads and is returned ONLY on
	// creation.
	ID           string
	Token        string
	Project      string
	Path         string
	IsDir        bool
	CreatedAt    time.Time
	ExpiresAt    time.Time
	CreatorUID   uint32
	CreatorAdmin bool
}

type restError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type ackResponse struct {
	Project string `json:"project"`
	Status  string `json:"status"`
}

// revokeShare records a deleted share ID (with the record's expiry) so
// stateless redemption stops honoring it immediately. Revocation lives in
// the handler's own registry - never in package-level state - and entries
// self-expire with the token they shadow (see isRevoked/sweepExpiredShares).
func (h *restHandler) revokeShare(id string, expiresAt time.Time) {
	h.shares.mu.Lock()
	if h.shares.revoked == nil {
		h.shares.revoked = map[string]time.Time{}
	}
	h.shares.revoked[id] = expiresAt
	h.shares.mu.Unlock()
}

func (h *restHandler) isRevoked(id string) bool {
	now := time.Now()
	h.shares.mu.Lock()
	defer h.shares.mu.Unlock()
	expiresAt, revoked := h.shares.revoked[id]
	if !revoked {
		return false
	}
	if !expiresAt.After(now) {
		// The JWT would fail its own exp check by now: drop the entry and
		// let verification reject it on its merits.
		delete(h.shares.revoked, id)
		return false
	}
	return true
}

func newHandlerForClient(client Client, opts Options) (http.Handler, error) {
	h, auth, err := newRestHandler(client, opts)
	if err != nil {
		return nil, err
	}
	r := chi.NewRouter()

	// Outermost middleware: a panic in any handler becomes a clean 500
	// instead of a dropped connection, with the stack in the server log.
	r.Use(h.recoverPanics)
	r.Use(h.requestLogging)

	r.Get("/", h.serveUIRoot)
	r.Get("/config.js", h.serveConfigJS)
	r.Get("/_nuxt/*", h.serveUIAssets)
	r.Get("/favicon.svg", h.serveUIPublic)

	basePath := strings.TrimRight(h.opts.BasePath, "/")

	r.Get(basePath, h.handleAPIInfo)
	r.Get(basePath+"/shares/{token}", h.handleShareInfo)
	r.Get(basePath+"/shares/{token}/download", h.serveShareDownload)
	r.Head(basePath+"/shares/{token}/download", h.serveShareDownload)
	r.Post(basePath+"/shares/{token}/derive", h.handleShareDerive)

	if auth != nil {
		r.Post(basePath+"/auth/login", h.handleLogin(auth))

		r.Group(func(r chi.Router) {
			r.Use(h.authMiddleware(auth, basePath))
			r.Route(basePath+"/projects/{project}", func(r chi.Router) {
				h.registerProjectRoutes(r)
			})
			r.Route(basePath+"/handles", func(r chi.Router) {
				h.registerSessionRoutes(r)
			})
		})
	} else {
		r.Route(basePath+"/projects/{project}", func(r chi.Router) {
			h.registerProjectRoutes(r)
		})
		r.Route(basePath+"/handles", func(r chi.Router) {
			h.registerSessionRoutes(r)
		})
	}

	return r, nil
}

func (h *restHandler) registerProjectRoutes(r chi.Router) {
	// One handler per method: the router owns dispatch, so handlers never
	// switch on r.Method again and methodNotAllowed boilerplate is gone.
	r.Get("/", h.handleProjectGet)
	r.Delete("/", h.handleProjectDelete)
	r.Get("/nodes", h.handleNodeGet)
	r.Head("/nodes", h.handleNodeGet)
	r.Delete("/nodes", h.handleNodeDelete)
	r.Get("/children", h.handleChildren)
	r.Get("/content", h.serveContent)
	r.Head("/content", h.serveContent)
	r.Put("/content", h.handleContentReplace)
	r.Patch("/content", h.handleContentPatch)
	r.Get("/xattrs", h.handleXAttrs)
	r.Get("/xattrs/value", h.handleXAttrGet)
	r.Put("/xattrs/value", h.handleXAttrPut)
	r.Delete("/xattrs/value", h.handleXAttrDelete)
	r.Get("/revisions", h.handleRevisions)
	r.Post("/ops/create", h.handleCreateFile)
	r.Post("/ops/mkdir", h.handleMkdir)
	r.Post("/ops/rmdir", h.handleRmdir)
	r.Post("/ops/unlink", h.handleUnlink)
	r.Post("/ops/rename", h.handleRename)
	r.Post("/ops/copy", h.handleCopy)
	r.Post("/ops/link", h.handleLink)
	r.Post("/ops/symlink", h.handleSymlink)
	r.Post("/ops/chmod", h.handleChmod)
	r.Post("/ops/chown", h.handleChown)
	r.Post("/ops/utimes", h.handleUtimes)
	r.Post("/ops/rollback", h.handleRollback)
	r.Post("/ops/revert", h.handleRevertPath)
	r.Post("/ops/prune", h.handlePrune)
	r.Post("/ops/enable", h.handleEnable)
	r.Get("/ops/status", h.handleStatus)
	r.Get("/shares", h.handleProjectSharesGet)
	r.Post("/shares", h.handleProjectSharesPost)
	r.Get("/shares/{shareID}", h.handleProjectShareGet)
	r.Delete("/shares/{shareID}", h.handleProjectShareDelete)
}

func (h *restHandler) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				stack := make([]byte, panicStackSize)
				n := runtime.Stack(stack, false)
				logging.Error(h.logger, "panic serving request",
					"method", r.Method,
					"path", logging.RedactSensitivePath(r.URL.Path),
					"panic", rec,
					"stack", string(stack[:n]))
				h.writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (h *restHandler) requestLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Gate BEFORE redacting: RedactSensitivePath+RedactQueryValues run
		// per request even when Debug is dropped at the warn default.
		if h.logger == nil || !h.logger.Enabled(r.Context(), slog.LevelDebug) {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now().UTC()
		sw := logging.NewHTTPRecorder(w)
		logging.Debug(h.logger, "http request start", "method", r.Method, "path", logging.RedactSensitivePath(r.URL.Path), "query", logging.RedactQueryValues(r.URL.RawQuery), "remote", r.RemoteAddr)
		next.ServeHTTP(sw, r)
		logging.Debug(h.logger, "http request complete", "method", r.Method, "path", logging.RedactSensitivePath(r.URL.Path), "status", sw.Status(), "bytes", sw.Bytes(), "elapsed", time.Since(started))
	})
}

// defaultRESTUmask is the server default umask carried by REST
// identities. Creations through REST use fixed modes (0644 files, 0755
// dirs, 0777 links) that already reflect a 022 mask, matching the FUSE
// default; attaching it here (instead of leaving Umask 0) means any
// future explicit-mode REST input flows through ApplyCreateMode masked
// rather than bypassing the mask entirely.
const defaultRESTUmask = 0o022

// decodeJSON reads one JSON object from the request body.
// allowEmpty treats a missing or whitespace-only body as "no fields
// supplied" (dst left at zero): endpoints whose whole body is optional
// (prune) pass true so a bodyless POST is not a 400 EOF, matching purge.
func (h *restHandler) decodeJSON(r *http.Request, dst any, allowEmpty bool) error {
	if r.Body == nil {
		if allowEmpty {
			return nil
		}
		return errBadRequest("request body is required")
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyMemory+1))
	if err != nil {
		return errBadRequest("unable to read request body")
	}
	if allowEmpty && len(bytes.TrimSpace(payload)) == 0 {
		return nil
	}
	if int64(len(payload)) > maxRequestBodyMemory {
		return errPayloadTooLarge(fmt.Sprintf("request body exceeds %d bytes", maxRequestBodyMemory))
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errBadRequest(fmt.Sprintf("invalid JSON body: %v", err))
	}
	if err := dec.Decode(new(struct{})); err != io.EOF {
		return errBadRequest("request body must contain a single JSON object")
	}
	return nil
}

// decodePathRequest decodes a {"path": ...} body for the simple file-ops.
// One decode+validate site so handlers stay three lines.
func (h *restHandler) decodePathRequest(r *http.Request, field string) (string, error) {
	var req pathRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		return "", err
	}
	if err := requireNonEmptyPath(field, req.Path); err != nil {
		return "", err
	}
	return req.Path, nil
}

func (h *restHandler) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// After WriteHeader nothing useful can follow on the wire, but a
	// Debug line keeps encode failures diagnosable server-side.
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		logging.Debug(h.logger, "response encode failed", "status", status, "err", err)
	}
}

func (h *restHandler) writeError(w http.ResponseWriter, status int, code, message string) {
	resp := restError{}
	resp.Error.Code = code
	resp.Error.Message = message
	h.writeJSON(w, status, resp)
}

func (h *restHandler) writeMappedError(w http.ResponseWriter, err error) {
	status := mappedStatus(err)
	code := mappedCode(status)
	message := err.Error()
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) {
		// Upstream (GitHub) failures can echo request URLs, tokens, and
		// infrastructure details in their bodies and Error() text; the
		// client learns the upstream class through the mapped status, so
		// no raw upstream message is ever forwarded.
		if apiErr.RateLimited {
			// Preserve retry timing for clients so "purge" can surface
			// "rate limited, retry after X" instead of generic 502.
			if !apiErr.RateLimitReset.IsZero() {
				if delay := time.Until(apiErr.RateLimitReset); delay > 0 {
					w.Header().Set("Retry-After", strconv.Itoa(int(delay.Seconds())+1))
				}
			} else if apiErr.RetryAfter > 0 {
				w.Header().Set("Retry-After", strconv.Itoa(int(apiErr.RetryAfter.Seconds())+1))
			}
			message = "rate limit exceeded: retry after the advertised window"
		} else {
			message = "upstream GitHub request failed"
		}
	} else if status >= http.StatusInternalServerError {
		// 5xx-class failures are this server's (or its backends') fault and
		// their raw text can carry paths, hostnames, or internal wording,
		// which must never be echoed to clients. Log the detail, answer
		// generically - the same pattern purge/prune already use.
		logging.Error(h.logger, "internal failure mapped to client error", "status", status, "code", code, "err", err)
		message = "internal server error"
	}
	h.writeError(w, status, code, message)
}

// traceStart logs the Debug "<op> start" half of one handler span through
// the canonical logging.Start core and returns the clock read the deferred
// traceFinish needs for elapsed. The component attr carries the rest
// namespace, so the op name stays bare. targetPath is a project-relative
// path from the request (never a token or secret); extra carries
// endpoint-specific attrs such as scope.
func (h *restHandler) traceStart(r *http.Request, op, project, targetPath string, extra ...any) time.Time {
	// Skip everything but the clock read when Debug is off: slice builds,
	// redaction, and logging are all disabled-path waste the alloc-parity
	// benchmark budgets would charge per request.
	if h.logger == nil || !h.logger.Enabled(r.Context(), slog.LevelDebug) {
		return time.Now().UTC()
	}
	started := time.Now().UTC()
	startArgs := append([]any{"project", project, "path", targetPath, "method", r.Method, "route", logging.RedactSensitivePath(r.URL.Path)}, extra...)
	logging.Start(h.logger, op, startArgs...)
	return started
}

// traceFinish logs the closing half of one handler span through the
// canonical logging.Finish core: Error "<op> failed" with err or Debug
// "<op> complete". Handlers call traceStart, then defer traceFinish with a
// pointer to their err variable plus the returned clock read, so every
// post-validation invocation completes its pair. Split into two direct
// calls (instead of one closure-returning helper) so the disabled path
// allocates nothing: no closure value, no slice builds, no redaction on
// success. Handlers pass no extra attrs on the hot paths, so those finish
// calls carry a nil extras slice.
//
// Failure pass-through: a non-nil finish error is always reported, even
// when Debug is off, so production failures stay observable while success
// spans stay gated. Spans open after request validation, so malformed-body
// 400s answered before traceStart carry no span: the pair holds for
// post-validation invocations only. Static-asset, info, and login routes
// (serveUIRoot, serveConfigJS, serveUIAssets, serveUIPublic, handleAPIInfo,
// handleLogin) never open spans by design: they carry no project/op
// identity worth tracing at Debug.
func (h *restHandler) traceFinish(r *http.Request, op, project, targetPath string, started time.Time, perr *error, extra ...any) {
	var finishErr error
	if perr != nil {
		finishErr = *perr
	}
	if finishErr == nil && (h.logger == nil || !h.logger.Enabled(r.Context(), slog.LevelDebug)) {
		return
	}
	finishArgs := append([]any{"project", project, "path", targetPath, "method", r.Method, "route", logging.RedactSensitivePath(r.URL.Path)}, extra...)
	logging.Finish(h.logger, op, started, finishErr, finishArgs...)
}

// maybeDrain honors the ?sync=1 opt-in (mirroring the POSIX write/fsync
// split: async by default, durable on request). Call it after a mutation
// completes and before responding. It returns false when the handler
// already answered and the caller must return without writing more.
//
// A failed drain answers 500 carrying the storage error, which names the
// project per the DrainProjectContext contract. The mutation itself is
// already published and journaled at that point, so 500-after-publish
// means retry-or-verify, never silent loss. writeMappedError is
// deliberately bypassed here: it redacts 5xx wording, which would strip
// the project name the caller needs for the retry.
func (h *restHandler) maybeDrain(w http.ResponseWriter, r *http.Request, project string) bool {
	want, err := parseBoolStrict(queryFirstParam(r.URL.RawQuery, "sync"), "sync")
	if err != nil {
		h.writeMappedError(w, err)
		return false
	}
	if !want {
		return true
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return false
	}
	if err := client.DrainProjectContext(r.Context(), project); err != nil {
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return false
	}
	return true
}

func mappedStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if errors.Is(err, shfs.ErrPreconditionFailed) {
		return http.StatusPreconditionFailed
	}
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.NotFound():
			return http.StatusNotFound
		case apiErr.RateLimited:
			return http.StatusTooManyRequests
		default:
			// Every other upstream answer is GitHub's failure, not this
			// server's: surface it as a gateway-class error.
			return http.StatusBadGateway
		}
	}
	var rerr *restStatusError
	if errors.As(err, &rerr) {
		return rerr.status
	}
	if errors.Is(err, shfs.ErrNotFound) {
		return http.StatusNotFound
	}
	// DAC refusals raised below the REST pre-checks (in-transaction
	// re-authorize, session commit recheck, clone inner checks, or a
	// lexical traverse check that passed while physical resolution
	// failed) arrive as raw errno: like the wrapper path they are
	// denials (403), and like FUSE they stay EACCES/EPERM, never a 500
	// that invites retries of a deterministic denial. Malformed
	// arguments that reach storage as EINVAL are client errors (400).
	if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
		return http.StatusForbidden
	}
	if errors.Is(err, syscall.EINVAL) {
		return http.StatusBadRequest
	}
	if errors.Is(err, syscall.EFBIG) {
		return http.StatusRequestEntityTooLarge
	}
	// Zero-extend cap: a single grow beyond 16 MiB fails in storage with
	// EFBIG (fs/io.go), a deterministic per-request ceiling, so it maps
	// to 413 instead of the retry-inviting 500. FUSE surfaces the errno
	// natively; only the REST translation needs the arm.
	switch {
	case errors.Is(err, shfs.ErrAlreadyExists),
		errors.Is(err, shfs.ErrNotEmpty),
		errors.Is(err, shfs.ErrIsDirectory),
		errors.Is(err, shfs.ErrNotDirectory):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// mappedCode is the single owner of wire error codes, with three narrow
// bespoke exceptions beside it: session stale handles answer 410 "gone"
// (HTTP has no mappedCode entry for 410, and the expired-vs-never-existed
// signal is load-bearing for session clients), login challenges answer
// 401 "invalid_credentials" (auth challenge shape, not an API error), and
// the UI fallback answers 404 "ui_not_built" (non-API asset fallback).
// Every other status maps to exactly one code below. Handlers must use
// errBadRequest/errPayloadTooLarge (+writeMappedError) instead of
// inventing ad-hoc codes (xattr_too_large,
// invalid_request, invalid_patch_op, recursive_delete_unsupported); bespoke
// codes survive only where the status alone cannot distinguish the case
// (conflict sub-cases, not_implemented, range). writeError is reserved for
// truly bespoke cases (auth challenges, UI fallbacks).
func mappedCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusPreconditionFailed:
		return "precondition_failed"
	case http.StatusRequestEntityTooLarge:
		return "payload_too_large"
	case http.StatusRequestedRangeNotSatisfiable:
		return "range_not_satisfiable"
	case http.StatusMethodNotAllowed:
		return "method_not_allowed"
	case http.StatusTooManyRequests:
		return "rate_limited"
	case http.StatusBadGateway:
		return "bad_gateway"
	case http.StatusNotImplemented:
		return "not_implemented"
	default:
		return "internal_error"
	}
}

// REST-layer input validation: reject malformed client input
// with 400 HERE instead of forwarding it to storage, whose generic errors
// would surface as 500s echoing internal wording.
//
// Canonical query/body parsers: exactly three: parseNonNegativeInt,
// parseBoolStrict, parsePruneScope. Do not add a fourth idiom; CLI-side
// parsing mirrors parseNonNegativeInt via parseNonNegativeArg (usageError).

func requireNonEmptyPath(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return errBadRequest(field + " is required")
	}
	return nil
}

// maxQueryParams mirrors net/url's default parameter cap: parseQuery
// rejects the whole query past it, so a first-match lookup must too. The
// GODEBUG urlmaxqueryparams override is operator-only and intentionally
// not mirrored here.
const maxQueryParams = 10000

// queryFirstParam returns the first value for key in raw exactly as
// url.Values.Get would after ParseQuery: pairs split on "&", segments
// containing a literal ";" skipped, empty segments skipped, split on the
// first "=", key and value QueryUnescaped (pairs with decoding errors
// skipped), first match wins. Unlike r.URL.Query().Get it builds no map
// or slices, so it costs zero heap on escape-free input. The benchmarked
// read paths (serve-content, replace, node-get) use it, and every other
// handler query lookup does too, so per-request query parsing adds no
// heap on the alloc-parity benchmark paths.
func queryFirstParam(raw, key string) string {
	if strings.Count(raw, "&")+1 > maxQueryParams {
		return ""
	}
	for raw != "" {
		var pair string
		pair, raw, _ = strings.Cut(raw, "&")
		if strings.Contains(pair, ";") {
			continue
		}
		if pair == "" {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		kk, err := url.QueryUnescape(k)
		if err != nil {
			continue
		}
		if kk != key {
			continue
		}
		vv, err := url.QueryUnescape(v)
		if err != nil {
			continue
		}
		return vv
	}
	return ""
}

// parseNonNegativeInt parses a required non-negative int64 query/body value.
// Missing, malformed, or negative inputs all answer 400 here so storage
// never sees them.
func parseNonNegativeInt(raw, field string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, errBadRequest(field + " is required")
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, errBadRequest(field + " must be a valid integer")
	}
	if value < 0 {
		return 0, errBadRequest(field + " must be non-negative")
	}
	return value, nil
}

// parseBoolStrict interprets a query-string boolean. Absent means false;
// recognized truthy/falsey spellings resolve; anything else is a 400 so
// callers never silently take the other branch.
func parseBoolStrict(raw, field string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "false", "0", "no":
		return false, nil
	case "true", "1", "yes":
		return true, nil
	default:
		return false, errBadRequest(field + " must be a boolean (true/false)")
	}
}

// parsePruneScope validates a purge scope against the storage constants
// (the single source of the objects|assets|history|all set). Empty means
// "all". Unknown scopes answer 400 with the known set as guidance.
func parsePruneScope(raw string) (storage.PruneScope, error) {
	scope := strings.TrimSpace(raw)
	if scope == "" {
		return storage.PruneAll, nil
	}
	switch storage.PruneScope(scope) {
	case storage.PruneObjects, storage.PruneAssets, storage.PruneHistory, storage.PruneChunks, storage.PruneAll:
		return storage.PruneScope(scope), nil
	default:
		return "", errBadRequest(`prune scope must be one of "objects", "assets", "history", "chunks", "all"`)
	}
}

// commitSHAPattern matches git object ids: lowercase hex, 7 (shortest
// unambiguous abbreviation) through 64 characters.
var commitSHAPattern = regexp.MustCompile(`^[0-9a-f]{7,64}$`)

func requireCommitSHA(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errBadRequest("commit_sha is required")
	}
	if !commitSHAPattern.MatchString(value) {
		return errBadRequest("commit_sha must be a 7-64 character hexadecimal commit SHA")
	}
	return nil
}

type restStatusError struct {
	status  int
	message string
}

func (e *restStatusError) Error() string { return e.message }

var errEmptyPrecondition = &restStatusError{status: 0, message: "precondition header missing"}

func isPreconditionHeaderEmpty(err error) bool {
	return errors.Is(err, errEmptyPrecondition)
}

func errBadRequest(message string) error {
	return &restStatusError{status: http.StatusBadRequest, message: message}
}

func errPreconditionFailed(message string) error {
	return &restStatusError{status: http.StatusPreconditionFailed, message: message}
}

func errPayloadTooLarge(message string) error {
	return &restStatusError{status: http.StatusRequestEntityTooLarge, message: message}
}

func errForbidden(message string) error {
	return &restStatusError{status: http.StatusForbidden, message: message}
}

var weakShareKeys = [][]byte{
	[]byte("replaceme"),
	[]byte("0123456789abcdef0123456789abcdef"),
}

func isWeakShareKey(key []byte) bool {
	return slices.ContainsFunc(weakShareKeys, func(w []byte) bool { return bytes.Equal(key, w) })
}
