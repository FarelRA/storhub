package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

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
	defaultRESTShareTTL      = 7 * 24 * time.Hour
	maxRequestBodyMemory     = 32 << 10
	// panicStackSize bounds the captured goroutine stack on panic recovery.
	panicStackSize = 8192
	// nobodyUID/nobodyGID is the POSIX "nobody" account: share-link visitors
	// get no identity beyond what path permissions grant them.
	nobodyUID = uint32(65534)
	nobodyGID = uint32(65534)
)

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

type (
	FileMetadata     = metadata.FileMeta
	MetadataRevision = metadata.MetadataRevision
	EntryInfo        = shfs.EntryInfo
	DirEntry         = shfs.DirEntry
	FSStats          = shfs.FSStats
	NodeKind         = metadata.NodeKind
)

const (
	NodeKindFile    = metadata.NodeKindFile
	NodeKindSymlink = metadata.NodeKindSymlink
)

var (
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
	PurgeUntrackedContext(ctx context.Context, project string) (*storage.PurgeResult, error)
	PruneContext(ctx context.Context, project, scope string, keep int, dryRun bool) (*storage.PruneResult, error)
	DeleteProjectContext(ctx context.Context, project string) error
	ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (*metadata.FileMeta, error)
	// DrainProjectContext blocks until everything published before the call
	// lands in the remote commit (the storage fsync primitive). It backs
	// the ?sync=1 opt-in on every mutating endpoint via maybeDrain.
	DrainProjectContext(ctx context.Context, project string) error
	// OpenSession opens a stateful file handle (Phase 2B sessions). The
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

const clientCtxKey contextKey = "rest-client"

// clientFor resolves the per-request Client placed in the context by the
// auth middleware. A foreign value under clientCtxKey is a middleware bug:
// fail CLOSED (panic -> recoverPanics -> logged 500) rather than silently
// falling back to the raw unrestricted client, which would turn every
// project route into an unauthenticated pass-through.
func (h *restHandler) clientFor(r *http.Request) Client {
	if value := r.Context().Value(clientCtxKey); value != nil {
		client, ok := value.(Client)
		if !ok {
			logging.Error(h.logger, "rest: context value under clientCtxKey does not implement Client; failing closed", "type", fmt.Sprintf("%T", value))
			panic("rest: context client does not implement Client")
		}
		return client
	}
	return h.client
}

// HTTP status/byte capture lives in internal/logging (HTTPRecorder): the
// former statusWriter duplicate was removed so one package owns one job.
// See logging/http_recorder.go.

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

func DefaultOptions() Options {
	return Options{
		BasePath:         defaultRESTBasePath,
		StreamChunkSize:  defaultRESTStreamChunk,
		MaxPatchBodySize: defaultRESTPatchBodySize,
		ShareTTL:         defaultRESTShareTTL,
	}
}

func NewHandler(hub *storage.StorHub, opts Options) (http.Handler, error) {
	if hub == nil {
		return nil, errors.New("storhub: REST handler requires a non-nil hub")
	}
	return newHandlerForClient(hub, opts)
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

// newRestHandler validates keys, applies defaults, derives the EdDSA share
// key, and builds the handler plus its authenticator (nil for anonymous).
// Split out of newHandlerForClient so route-table construction
// (registerRoutes inline above) and login handling read as separate steps.
func newRestHandler(client Client, opts Options) (*restHandler, *restAuthenticator, error) {
	if opts.Auth == nil && !opts.AllowAnonymous {
		return nil, nil, errors.New("security constraint: no Auth configured; set AllowAnonymous:true to serve unauthenticated traffic deliberately")
	}
	opts = opts.withDefaults()
	logger := logging.WithComponent(nil, "rest")
	if provider, ok := client.(interface{ Logger() *slog.Logger }); ok && provider.Logger() != nil {
		logger = logging.WithComponent(provider.Logger(), "rest")
	}
	// Unified key: all cards (auth and share) derive from TokenSigningKey.
	// This gives one EdDSA key for the whole API, share and login are just
	// different capabilities (kind) on the same JWT.
	if opts.Auth != nil {
		if len(opts.Auth.TokenSigningKey) < 32 {
			return nil, nil, errors.New("security constraint: token signing key must be at least 32 bytes")
		}
		if isWeakShareKey(opts.Auth.TokenSigningKey) {
			return nil, nil, errors.New("security constraint: token signing key is a known weak/default key")
		}
	}
	if len(opts.ShareSigningKey) > 0 {
		if len(opts.ShareSigningKey) < 32 {
			return nil, nil, errors.New("security constraint: share signing key must be at least 32 bytes")
		}
		if isWeakShareKey(opts.ShareSigningKey) {
			return nil, nil, errors.New("security constraint: share signing key is a known weak/default key")
		}
	}
	h := &restHandler{client: client, opts: opts, shares: newShareRegistry(), logger: logger}
	if opts.Auth != nil && len(opts.Auth.TokenSigningKey) > 0 {
		seed := sha256.Sum256(opts.Auth.TokenSigningKey)
		h.shareSignKey = ed25519.NewKeyFromSeed(seed[:32])
		h.opts.ShareSigningKey = h.shareSignKey.Seed()
	} else if len(opts.ShareSigningKey) > 0 {
		seed := opts.ShareSigningKey
		if len(seed) > 32 {
			hash := sha256.Sum256(seed)
			seed = hash[:]
		}
		h.shareSignKey = ed25519.NewKeyFromSeed(seed[:32])
	}
	if opts.Auth == nil {
		return h, nil, nil
	}
	auth, err := newAuthenticator(*opts.Auth)
	if err != nil {
		return nil, nil, err
	}
	return h, auth, nil
}

// handleLogin serves POST /auth/login: decode credentials, mint an auth JWT.
func (h *restHandler) handleLogin(auth *restAuthenticator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req restLoginRequest
		if err := h.decodeJSON(r, &req, false); err != nil {
			h.writeMappedError(w, err)
			return
		}
		principal, token, ttl, err := auth.login(req.Username, req.Password)
		if err != nil {
			h.writeError(w, http.StatusUnauthorized, "invalid_credentials", "invalid username or password")
			return
		}
		h.writeJSON(w, http.StatusOK, restLoginResponse{Token: token, TokenType: "Bearer", ExpiresIn: int64(ttl.Seconds()), Principal: principal})
	}
}

func (o Options) withDefaults() Options {
	if strings.TrimSpace(o.BasePath) == "" {
		o.BasePath = defaultRESTBasePath
	}
	o.BasePath = "/" + strings.Trim(strings.TrimSpace(o.BasePath), "/")
	// "/" normalizes to an empty route pattern, which panics chi at
	// construction; treat it as "not set" and fall back to the default.
	if o.BasePath == "/" {
		o.BasePath = defaultRESTBasePath
	}
	if o.StreamChunkSize <= 0 {
		o.StreamChunkSize = defaultRESTStreamChunk
	}
	if o.MaxPatchBodySize <= 0 {
		o.MaxPatchBodySize = defaultRESTPatchBodySize
	}
	if o.ShareTTL <= 0 {
		o.ShareTTL = defaultRESTShareTTL
	}
	// A zero MaxShareTTL must not mean "unbounded": default it so the
	// client-requested-lifetime clamp is always armed.
	if o.MaxShareTTL <= 0 {
		o.MaxShareTTL = defaultRESTShareTTL
	}
	return o
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
	r.Post("/ops/create-file", h.handleCreateFile)
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
	r.Post("/ops/revert-path", h.handleRevertPath)
	r.Post("/ops/purge", h.handlePurge)
	r.Post("/ops/prune", h.handlePrune)
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
		// per request even when Info is dropped at the warn default.
		if h.logger == nil || !h.logger.Enabled(r.Context(), slog.LevelInfo) {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now().UTC()
		sw := logging.NewHTTPRecorder(w)
		logging.Info(h.logger, "http request start", "method", r.Method, "path", logging.RedactSensitivePath(r.URL.Path), "query", logging.RedactQueryValues(r.URL.RawQuery), "remote", r.RemoteAddr)
		next.ServeHTTP(sw, r)
		logging.Info(h.logger, "http request complete", "method", r.Method, "path", logging.RedactSensitivePath(r.URL.Path), "status", sw.Status(), "bytes", sw.Bytes(), "elapsed", time.Since(started))
	})
}

// requestBearerToken extracts the Authorization: Bearer *** token from a
// request, tolerating inline whitespace. Empty unless the scheme is Bearer.
// Callers keep their own header-vs-query precedence on top of it.
func requestBearerToken(r *http.Request) string {
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
}

func (h *restHandler) authMiddleware(auth *restAuthenticator, basePath string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, fromQuery := bearerOrQueryToken(r)
			if token == "" {
				h.writeUnauthorized(w, auth, "missing bearer token")
				return
			}
			// authPrincipal rejects auth-JWT-via-query explicitly so the
			// token falls through to the share lane below before failing.
			if ctx, ok := h.authPrincipal(r, auth, token, fromQuery); ok {
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			if ctx, ok, fatal := h.sharePrincipal(r, auth, basePath, token, w); fatal {
				return
			} else if ok {
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			h.writeUnauthorized(w, auth, "invalid bearer token")
		})
	}
}

// bearerOrQueryToken extracts the bearer credential, reporting whether it
// came from the query string (share-capability lane) or the header.
func bearerOrQueryToken(r *http.Request) (token string, fromQuery bool) {
	if token = requestBearerToken(r); token != "" {
		return token, false
	}
	if token = strings.TrimSpace(r.URL.Query().Get("token")); token != "" {
		return token, true
	}
	return "", false
}

// authPrincipal verifies an auth JWT and returns the request context
// carrying the authorized client. ok=false means "not a usable auth token,
// try the share lane"; a nil context with ok=false after a query-token
// rejection still falls through to shares.
func (h *restHandler) authPrincipal(r *http.Request, auth *restAuthenticator, token string, fromQuery bool) (context.Context, bool) {
	principal, err := auth.parseToken(token)
	if err == nil && fromQuery {
		// Auth JWTs must travel in the Authorization header: query
		// strings land in intermediaries, browser history and
		// Referer headers. Query-token acceptance is reserved for
		// share capabilities (handled below), not the whole
		// authenticated surface.
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	// Re-read the user record behind the token: auth JWTs are
	// otherwise irrevocable, so a disabled, demoted, or removed
	// account must not keep full access until expiry.
	fresh, live := auth.currentPrincipal(principal)
	if !live {
		return nil, false
	}
	// Attach the caller's identity for the storage layers below:
	// downstream permission checks must see the authenticated
	// principal, never the server process's own credentials.
	identity := shfs.WithIdentity(r.Context(), shfs.Identity{
		UID:    fresh.UID,
		GID:    fresh.PrimaryGID,
		Groups: fresh.Groups,
		Admin:  fresh.Admin,
	})
	return context.WithValue(identity, clientCtxKey, &authorizedClient{base: h.client, principal: fresh}), true
}

// sharePrincipal verifies a share JWT and returns the nobody-visitor
// context scoped to the shared path. fatal=true means the handler already
// answered (share-management forbidden); ok=false means "not a share token".
func (h *restHandler) sharePrincipal(r *http.Request, auth *restAuthenticator, basePath, token string, w http.ResponseWriter) (context.Context, bool, bool) {
	claims, err := h.parseShareToken(token)
	if err != nil {
		return nil, false, false
	}
	if h.isRevoked(claims.ID) {
		h.writeUnauthorized(w, auth, "invalid bearer token")
		return nil, false, true
	}
	project := chi.URLParam(r, "project")
	if project == claims.Project && strings.HasPrefix(r.URL.Path, basePath+"/projects/"+project+"/shares") {
		h.writeMappedError(w, errForbidden("share links cannot manage shares"))
		return nil, false, true
	}
	// Share links act as an unauthenticated read-only visitor.
	identity := shfs.WithIdentity(r.Context(), shfs.Identity{UID: nobodyUID, GID: nobodyGID})
	return context.WithValue(identity, clientCtxKey, newRestrictedClient(h.client, claims.Project, claims.Path)), true, false
}

func (h *restHandler) writeUnauthorized(w http.ResponseWriter, auth *restAuthenticator, message string) {
	w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q`, auth.realm))
	h.writeError(w, http.StatusUnauthorized, "unauthorized", message)
}

func (h *restHandler) serveConfigJS(w http.ResponseWriter, r *http.Request) {
	payload, err := json.Marshal(map[string]any{
		"basePath":    h.opts.BasePath,
		"authEnabled": h.opts.Auth != nil,
		"project":     h.opts.DefaultProject,
	})
	if err != nil {
		logging.Error(h.logger, "config serialization failed", "err", err)
		h.writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	_, _ = fmt.Fprintf(w, "window.STORHUB_UI_CONFIG = %s;", payload)
}

// handleAPIInfo answers GET <basePath>: service identity for the console.
// A handle* route (JSON document), not a serve* byte stream.
func (h *restHandler) handleAPIInfo(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]any{
		"service":   "storhub-rest",
		"version":   "v1",
		"base_path": h.opts.BasePath,
		// Set by `storhub serve <project>`: the console auto-loads this and
		// hides the free-form project selector - one server, one project.
		"project": h.opts.DefaultProject,
	})
}

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
	_ = json.NewEncoder(w).Encode(payload)
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
	want, err := parseBoolStrict(r.URL.Query().Get("sync"), "sync")
	if err != nil {
		h.writeMappedError(w, err)
		return false
	}
	if !want {
		return true
	}
	if err := h.clientFor(r).DrainProjectContext(r.Context(), project); err != nil {
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

// mappedCode is the single owner of wire error codes: every status maps
// to exactly one code. Handlers must use errBadRequest/errPayloadTooLarge
// (+writeMappedError) instead of inventing ad-hoc codes (xattr_too_large,
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
// Canonical query/body parsers: exactly three — parseNonNegativeInt,
// parseBoolStrict, parsePruneScope. Do not add a fourth idiom; CLI-side
// parsing mirrors parseNonNegativeInt via parseNonNegativeArg (usageError).

func requireNonEmptyPath(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return errBadRequest(field + " is required")
	}
	return nil
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

// parsePruneScope validates a prune scope against the storage constants
// (the single source of the objects|assets|history|all set). Empty means
// "all". Unknown scopes answer 400 with the known set as guidance.
func parsePruneScope(raw string) (storage.PruneScope, error) {
	scope := strings.TrimSpace(raw)
	if scope == "" {
		return storage.PruneAll, nil
	}
	switch storage.PruneScope(scope) {
	case storage.PruneObjects, storage.PruneAssets, storage.PruneHistory, storage.PruneAll:
		return storage.PruneScope(scope), nil
	default:
		return "", errBadRequest(`prune scope must be one of "objects", "assets", "history", "all"`)
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
	[]byte("replace-me"),
	[]byte("0123456789abcdef0123456789abcdef"),
}

func isWeakShareKey(key []byte) bool {
	return slices.ContainsFunc(weakShareKeys, func(w []byte) bool { return bytes.Equal(key, w) })
}
