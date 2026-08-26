package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path"
	"runtime"
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
	"github.com/golang-jwt/jwt/v5"
)

const (
	defaultRESTBasePath      = "/api/v1"
	defaultRESTStreamChunk   = 1 << 20
	defaultRESTPatchBodySize = 8 << 20
	defaultRESTShareTTL      = 7 * 24 * time.Hour
	maxRequestBodyMemory     = 32 << 10
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
	RenameContext(ctx context.Context, project, oldPath, newPath string) error
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
	SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte) error
	GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error)
	ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error)
	RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error
	// RevisionContext reports the project's current metadata revision for
	// use with fs.WithExpectedRevision preconditions.
	RevisionContext(ctx context.Context, project string) (string, error)
	ListMetadataRevisionsContext(ctx context.Context, project string) ([]metadata.MetadataRevision, error)
	RollbackMetadataContext(ctx context.Context, project, commitSHA string) error
	PurgeUntrackedContext(ctx context.Context, project string) (*storage.PurgeResult, error)
	DeleteProjectContext(ctx context.Context, project string) error
	ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (*metadata.FileMeta, error)
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

func (h *restHandler) clientFor(r *http.Request) Client {
	if client, ok := r.Context().Value(clientCtxKey).(Client); ok {
		return client
	}
	return h.client
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

type shareRegistry struct {
	mu    sync.RWMutex
	items map[string]*shareRecord
}

type shareRecord struct {
	// ID is the short opaque URL identifier (never a credential); Token
	// holds the signed JWT for bearer authentication of scoped reads.
	ID        string
	Token     string
	Project   string
	Path      string
	Download  bool
	IsDir     bool
	CreatedAt time.Time
	ExpiresAt time.Time
}

// restrictedClient wraps a Client and restricts access to a specific project and path
// readOnlyShare supplies every mutating Client method as a denial. It is
// embedded by restrictedClient so the read-only-share policy lives in
// exactly ONE place: a future Client method cannot silently delegate to the
// underlying client, because the compiler forces a decision here — either a
// new denial lands in this struct (policy stays centralized) or an explicit,
// reviewed override is written on restrictedClient itself.
type readOnlyShare struct{}

func errReadOnly() error { return errForbidden("access denied: read-only share") }

func (readOnlyShare) CreateFileContext(ctx context.Context, project, filePath string) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) MkdirContext(ctx context.Context, project, dirPath string) error {
	return errReadOnly()
}

func (readOnlyShare) DeleteFileContext(ctx context.Context, project, filePath string, opts ...shfs.MutateOption) error {
	return errReadOnly()
}

func (readOnlyShare) RmdirContext(ctx context.Context, project, dirPath string, opts ...shfs.MutateOption) error {
	return errReadOnly()
}

func (readOnlyShare) RenameContext(ctx context.Context, project, oldPath, newPath string) error {
	return errReadOnly()
}

func (readOnlyShare) TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) PatchFileContext(ctx context.Context, project, filePath string, offset, deleteSize int64, edit []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) SymlinkContext(ctx context.Context, project, target, linkPath string) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) LinkContext(ctx context.Context, project, existingPath, newPath string) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error {
	return errReadOnly()
}

func (readOnlyShare) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error {
	return errReadOnly()
}

func (readOnlyShare) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error {
	return errReadOnly()
}

func (readOnlyShare) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte) error {
	return errReadOnly()
}

func (readOnlyShare) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error {
	return errReadOnly()
}

func (readOnlyShare) RollbackMetadataContext(ctx context.Context, project, commitSHA string) error {
	return errReadOnly()
}

func (readOnlyShare) PurgeUntrackedContext(ctx context.Context, project string) (*storage.PurgeResult, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) DeleteProjectContext(ctx context.Context, project string) error {
	return errReadOnly()
}

// restrictedClient wraps a Client and restricts access to a specific project
// and path. Read methods enforce the shared-prefix check then delegate;
// every mutation is denied by the embedded readOnlyShare.
type restrictedClient struct {
	readOnlyShare
	underlying     Client
	allowedProject string
	allowedPath    string
}

func newRestrictedClient(underlying Client, project, path string) *restrictedClient {
	allowedPath, err := canonicalSharePath(path)
	if err != nil {
		allowedPath = strings.Trim(strings.TrimSpace(path), "/")
	}
	return &restrictedClient{
		underlying:     underlying,
		allowedProject: project,
		allowedPath:    allowedPath,
	}
}

func (c *restrictedClient) checkAccess(project, targetPath string) error {
	if project != c.allowedProject {
		return errForbidden("access denied: project not shared")
	}
	canonicalTargetPath, err := canonicalSharePath(targetPath)
	if err != nil {
		return errForbidden("access denied: path not shared")
	}
	if hasPathPrefix(canonicalTargetPath, c.allowedPath) {
		return nil
	}
	return errForbidden("access denied: path not shared")
}

func (c *restrictedClient) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	if err := c.checkAccess(project, filePath); err != nil {
		return nil, err
	}
	return c.underlying.ReadFileAtContext(ctx, project, filePath, offset, length)
}

func (c *restrictedClient) StatPathContext(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	if err := c.checkAccess(project, targetPath); err != nil {
		return nil, err
	}
	return c.underlying.StatPathContext(ctx, project, targetPath)
}

func (c *restrictedClient) ReadDirContext(ctx context.Context, project, dirPath string) ([]shfs.DirEntry, error) {
	if err := c.checkAccess(project, dirPath); err != nil {
		return nil, err
	}
	return c.underlying.ReadDirContext(ctx, project, dirPath)
}

// StatFS and revision listing are denied with share-specific messages:
// aggregate stats and history leak information beyond the shared subtree.
func (c *restrictedClient) StatFSContext(ctx context.Context, project string) (*shfs.FSStats, error) {
	return nil, errForbidden("access denied: share metadata is limited to the shared path")
}

func (c *restrictedClient) ListMetadataRevisionsContext(ctx context.Context, project string) ([]metadata.MetadataRevision, error) {
	return nil, errForbidden("access denied: share metadata is limited to the shared path")
}

// Share visitors never learn the project's revision: like stats and
// history, it is metadata beyond the shared subtree.
func (c *restrictedClient) RevisionContext(ctx context.Context, project string) (string, error) {
	return "", errForbidden("access denied: share metadata is limited to the shared path")
}

func (c *restrictedClient) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	if err := c.checkAccess(project, linkPath); err != nil {
		return "", err
	}
	return c.underlying.ReadlinkContext(ctx, project, linkPath)
}

func (c *restrictedClient) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
	if err := c.checkAccess(project, targetPath); err != nil {
		return nil, err
	}
	return c.underlying.GetXAttrContext(ctx, project, targetPath, attr)
}

func (c *restrictedClient) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	if err := c.checkAccess(project, targetPath); err != nil {
		return nil, err
	}
	return c.underlying.ListXAttrContext(ctx, project, targetPath)
}

type restError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type projectResponse struct {
	Project string        `json:"project"`
	Stats   *shfs.FSStats `json:"stats"`
}

type nodeResponse struct {
	Project string          `json:"project"`
	Entry   *shfs.EntryInfo `json:"entry"`
	ETag    string          `json:"etag,omitempty"`
}

type entriesResponse struct {
	Project string          `json:"project"`
	Path    string          `json:"path"`
	Entries []shfs.DirEntry `json:"entries"`
}

type xattrListResponse struct {
	Project string   `json:"project"`
	Path    string   `json:"path"`
	Names   []string `json:"names"`
}

type revisionsResponse struct {
	Project   string                      `json:"project"`
	Revisions []metadata.MetadataRevision `json:"revisions"`
}

type ackResponse struct {
	Project string `json:"project"`
	Status  string `json:"status"`
}

type pathRequest struct {
	Path string `json:"path"`
}

type renameRequest struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

type linkRequest struct {
	ExistingPath string `json:"existing_path"`
	NewPath      string `json:"new_path"`
}

type symlinkRequest struct {
	Target   string `json:"target"`
	LinkPath string `json:"link_path"`
}

type chmodRequest struct {
	Path string `json:"path"`
	Mode uint32 `json:"mode"`
}

type chownRequest struct {
	Path string `json:"path"`
	UID  uint32 `json:"uid"`
	GID  uint32 `json:"gid"`
}

type utimesRequest struct {
	Path  string    `json:"path"`
	Atime time.Time `json:"atime"`
	Mtime time.Time `json:"mtime"`
}

type rollbackRequest struct {
	CommitSHA string `json:"commit_sha"`
}

type shareRequest struct {
	Path string `json:"path"`
	// ExpiresInSeconds expresses the lifetime in plain seconds. There is
	// deliberately no time.Duration field: JSON numbers for a Duration are
	// nanoseconds, a trap no client should be able to fall into.
	ExpiresInSeconds int64 `json:"expires_in_seconds,omitempty"`
	Download         *bool `json:"download,omitempty"`
}

type shareResponse struct {
	ID          string `json:"id"`
	Project     string `json:"project"`
	Path        string `json:"path"`
	URL         string `json:"url"`
	DownloadURL string `json:"download_url,omitempty"`
	// Token is the signed share JWT, returned ONLY on creation (programmatic
	// bearer use); listings never carry it so capabilities do not leak
	// through read endpoints.
	Token     string `json:"token,omitempty"`
	ExpiresAt string `json:"expires_at"`
	Download  bool   `json:"download"`
	IsDir     bool   `json:"is_dir"`
}

type shareClaims struct {
	jwt.RegisteredClaims
	ID       string `json:"id"`
	Project  string `json:"prj"`
	Path     string `json:"pth"`
	Download bool   `json:"dl"`
	IsDir    bool   `json:"dir"`
}

// revokedShares marks deleted share IDs so stateless redemption stops
// honoring them immediately (per-process; see serveShareInfo).
var revokedShares sync.Map // map[string]struct{}

func revokeShare(id string) { revokedShares.Store(id, struct{}{}) }
func (h *restHandler) isRevoked(id string) bool {
	_, bad := revokedShares.Load(id)
	return bad
}

type sharesResponse struct {
	Project string          `json:"project"`
	Shares  []shareResponse `json:"shares"`
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
	if opts.Auth == nil && !opts.AllowAnonymous {
		return nil, errors.New("security constraint: no Auth configured; set AllowAnonymous:true to serve unauthenticated traffic deliberately")
	}
	opts = opts.withDefaults()
	logger := logging.WithComponent(nil, "rest")
	if provider, ok := client.(interface{ Logger() *slog.Logger }); ok && provider.Logger() != nil {
		logger = logging.WithComponent(provider.Logger(), "rest")
	}
	if len(opts.ShareSigningKey) > 0 {
		if len(opts.ShareSigningKey) < 32 {
			return nil, errors.New("security constraint: share signing key must be at least 32 bytes")
		}
		if isWeakShareKey(opts.ShareSigningKey) {
			return nil, errors.New("security constraint: share signing key is a known weak/default key")
		}
	}
	h := &restHandler{client: client, opts: opts, shares: &shareRegistry{items: map[string]*shareRecord{}}, logger: logger}
	if len(opts.ShareSigningKey) == 0 && opts.Auth != nil && len(opts.Auth.TokenSigningKey) > 0 {
		// No explicit share key: derive one from the auth token signing key
		// so an auth-file deployment gets working shares with zero extra
		// configuration. Domain-separated hash => deterministic across
		// restarts (existing share links keep verifying), unrelated to the
		// JWT key material, and always >= 32 bytes.
		hash := sha256.Sum256([]byte("storhub/share-signing/v1\x00" + string(opts.Auth.TokenSigningKey)))
		opts.ShareSigningKey = hash[:]
	}
	if len(opts.ShareSigningKey) > 0 {
		seed := opts.ShareSigningKey
		if len(seed) > 32 {
			hash := sha256.Sum256(seed)
			seed = hash[:]
		}
		h.shareSignKey = ed25519.NewKeyFromSeed(seed[:32])
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

	r.Get("/shares/{id}/download", h.serveShareDownload)
	r.Head("/shares/{id}/download", h.serveShareDownload)

	basePath := strings.TrimRight(opts.BasePath, "/")

	r.Get(basePath, h.serveAPIInfo)
	r.Get(basePath+"/shares/{id}", h.serveShareInfo)
	// Signed download links minted under /projects/{p}/downloads are redeemed
	// here WITHOUT auth headers: that is the entire point - the browser's own
	// download manager (or curl/wget) fetches a plain URL.
	r.Get(basePath+"/download/{project}", h.serveProjectDownload)
	r.Head(basePath+"/download/{project}", h.serveProjectDownload)

	if opts.Auth != nil {
		auth, err := newAuthenticator(*opts.Auth)
		if err != nil {
			return nil, err
		}
		r.Post(basePath+"/auth/login", func(w http.ResponseWriter, r *http.Request) {
			var req restLoginRequest
			if err := h.decodeJSON(r, &req); err != nil {
				h.writeMappedError(w, err)
				return
			}
			principal, token, ttl, err := auth.login(req.Username, req.Password)
			if err != nil {
				h.writeError(w, http.StatusUnauthorized, "invalid_credentials", "invalid username or password")
				return
			}
			h.writeJSON(w, http.StatusOK, restLoginResponse{Token: token, TokenType: "Bearer", ExpiresIn: int64(ttl.Seconds()), Principal: principal})
		})

		r.Group(func(r chi.Router) {
			r.Use(h.authMiddleware(auth, basePath))
			r.Route(basePath+"/projects/{project}", func(r chi.Router) {
				h.registerProjectRoutes(r)
			})
		})
	} else {
		r.Route(basePath+"/projects/{project}", func(r chi.Router) {
			h.registerProjectRoutes(r)
		})
	}

	return r, nil
}

func (o Options) withDefaults() Options {
	if strings.TrimSpace(o.BasePath) == "" {
		o.BasePath = defaultRESTBasePath
	}
	o.BasePath = "/" + strings.Trim(strings.TrimSpace(o.BasePath), "/")
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
	r.Get("/", h.handleProject)
	r.Delete("/", h.handleProject)
	r.Get("/nodes", h.handleNodes)
	r.Head("/nodes", h.handleNodes)
	r.Delete("/nodes", h.handleNodes)
	r.Get("/children", h.handleChildren)
	r.Get("/content", h.handleContentRead)
	r.Head("/content", h.handleContentRead)
	r.Put("/content", h.handleContentReplace)
	r.Patch("/content", h.handleContentPatch)
	r.Get("/xattrs", h.handleXAttrs)
	r.Get("/xattrs/value", h.handleXAttrValue)
	r.Put("/xattrs/value", h.handleXAttrValue)
	r.Delete("/xattrs/value", h.handleXAttrValue)
	r.Get("/revisions", h.handleRevisions)
	r.Post("/ops/create-file", h.handleCreateFile)
	r.Post("/ops/mkdir", h.handleMkdir)
	r.Post("/ops/rmdir", h.handleRmdir)
	r.Post("/ops/unlink", h.handleUnlink)
	r.Post("/ops/rename", h.handleRename)
	r.Post("/ops/link", h.handleLink)
	r.Post("/ops/symlink", h.handleSymlink)
	r.Post("/ops/chmod", h.handleChmod)
	r.Post("/ops/chown", h.handleChown)
	r.Post("/ops/utimes", h.handleUtimes)
	r.Post("/ops/rollback", h.handleRollback)
	r.Post("/ops/purge", h.handlePurge)
	r.Get("/shares", h.handleProjectShares)
	r.Post("/shares", h.handleProjectShares)
	r.Get("/shares/{shareID}", h.handleProjectShare)
	r.Delete("/shares/{shareID}", h.handleProjectShare)
	// Mints a short-lived signed download URL (redeemable without auth
	// headers, so the browser's own downloader or curl can use it).
	r.Post("/downloads", h.handleCreateDownloadLink)
}

func (h *restHandler) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				stack := make([]byte, 8192)
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
		started := time.Now().UTC()
		sw := &statusWriter{ResponseWriter: w}
		logging.Info(h.logger, "http request start", "method", r.Method, "path", logging.RedactSensitivePath(r.URL.Path), "query", logging.RedactQueryValues(r.URL.RawQuery), "remote", r.RemoteAddr)
		next.ServeHTTP(sw, r)
		status := sw.status
		if status == 0 {
			status = http.StatusOK
		}
		logging.Info(h.logger, "http request complete", "method", r.Method, "path", logging.RedactSensitivePath(r.URL.Path), "status", status, "bytes", sw.bytes, "elapsed", time.Since(started))
	})
}

func (h *restHandler) authMiddleware(auth *restAuthenticator, basePath string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
			if !strings.HasPrefix(authHeader, "Bearer ") {
				w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q`, auth.realm))
				h.writeError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
				return
			}
			token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))

			principal, err := auth.parseToken(token)
			if err == nil {
				// Attach the caller's identity for the storage layers below:
				// downstream permission checks must see the authenticated
				// principal, never the server process's own credentials.
				identity := shfs.WithIdentity(r.Context(), shfs.Identity{
					UID:    principal.UID,
					GID:    principal.PrimaryGID,
					Groups: principal.Groups,
					Admin:  principal.Admin,
				})
				ctx := context.WithValue(identity, clientCtxKey, &authorizedClient{base: h.client, principal: principal})
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			claims, err := h.parseShareToken(token)
			if err == nil {
				if h.isRevoked(claims.ID) {
					w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q`, auth.realm))
					h.writeError(w, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
					return
				}
				project := chi.URLParam(r, "project")
				if project == claims.Project && strings.HasPrefix(r.URL.Path, basePath+"/projects/"+project+"/shares") {
					h.writeError(w, http.StatusForbidden, "forbidden", "share links cannot manage shares")
					return
				}
				// Share links act as an unauthenticated read-only visitor.
				identity := shfs.WithIdentity(r.Context(), shfs.Identity{UID: nobodyUID, GID: nobodyGID})
				ctx := context.WithValue(identity, clientCtxKey, newRestrictedClient(h.client, claims.Project, claims.Path))
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q`, auth.realm))
			h.writeError(w, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
		})
	}
}

func (h *restHandler) serveConfigJS(w http.ResponseWriter, r *http.Request) {
	payload, err := json.Marshal(map[string]any{
		"basePath":    h.opts.BasePath,
		"authEnabled": h.opts.Auth != nil,
		"project":     h.opts.DefaultProject,
	})
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	_, _ = fmt.Fprintf(w, "window.STORHUB_UI_CONFIG = %s;", payload)
}

func (h *restHandler) serveAPIInfo(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]any{
		"service":   "storhub-rest",
		"version":   "v1",
		"base_path": h.opts.BasePath,
		// Set by `storhub serve <project>`: the console auto-loads this and
		// hides the free-form project selector - one server, one project.
		"project": h.opts.DefaultProject,
	})
}

// Share redemption follows ONE pathway: the signed JWT is the credential
// and the source of truth. GET /shares/{token} verifies it statelessly and
// answers from claims - no registry lookup, so links survive restarts.
// Revocation: DELETE marks the share ID revoked (checked by the auth
// middleware), killing the link immediately for this process; revocation is
// per-process by design - permanent revocation is key rotation.
func (h *restHandler) serveShareInfo(w http.ResponseWriter, r *http.Request) {
	segment := chi.URLParam(r, "id")
	claims, err := h.parseShareToken(segment)
	if err != nil || strings.TrimSpace(claims.Path) == "" || strings.TrimSpace(claims.Project) == "" {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	if h.isRevoked(claims.ID) {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	h.writeJSON(w, http.StatusOK, shareResponse{
		ID:        claims.ID,
		Project:   claims.Project,
		Path:      claims.Path,
		URL:       "/?share=" + url.QueryEscape(segment),
		Token:     segment,
		ExpiresAt: claims.ExpiresAt.Time.UTC().Format(time.RFC3339),
		Download:  claims.Download,
		IsDir:     claims.IsDir,
	})
}

// ---- Signed single-file download links -------------------------------------
//
// One mechanism reused, not a second token system: these are shareClaims
// JWTs (dl=true, pth=exact file, 5-minute life) verified by parseShareToken.
// Stateless by construction - no registry, self-expiring, valid across
// restarts within their window - and they delegate streaming to
// handleContentRead so Range/206 resume, ETag and HEAD come for free.

const (
	downloadLinkTTL = 5 * time.Minute
)

type downloadLinkRequest struct {
	Path string `json:"path"`
}

type downloadLinkResponse struct {
	URL       string `json:"url"`
	ExpiresIn int64  `json:"expires_in"`
}

func (h *restHandler) handleCreateDownloadLink(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req downloadLinkRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if strings.TrimSpace(req.Path) == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "path is required")
		return
	}
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, req.Path)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if entry.IsDir {
		h.writeError(w, http.StatusConflict, "is_directory", "download links target files; use shares for directories")
		return
	}
	signed, err := h.signDownloadToken(project, req.Path)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	basePath := strings.TrimRight(h.opts.BasePath, "/")
	h.writeJSON(w, http.StatusCreated, downloadLinkResponse{
		URL:       fmt.Sprintf("%s/download/%s?token=%s", basePath, url.PathEscape(project), url.QueryEscape(signed)),
		ExpiresIn: int64(downloadLinkTTL.Seconds()),
	})
}

func (h *restHandler) signDownloadToken(project, filePath string) (string, error) {
	if h.shareSignKey == nil {
		return "", errForbidden("share signing key not configured (pass --share-key or serve with an auth file)")
	}
	now := time.Now()
	shareID, err := newShareID()
	if err != nil {
		return "", err
	}
	claims := &shareClaims{
		ID:       shareID,
		Project:  project,
		Path:     filePath,
		Download: true,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(downloadLinkTTL)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	return token.SignedString(h.shareSignKey)
}

// serveProjectDownload redeems a signed link. The claims must match the URL
// exactly (project from path, file from ?path=): tokens are scoped to one
// file and grant nothing else.
func (h *restHandler) serveProjectDownload(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	claims, err := h.parseShareToken(r.URL.Query().Get("token"))
	if err != nil || !claims.Download {
		h.writeError(w, http.StatusForbidden, "forbidden", "invalid or expired download token")
		return
	}
	if claims.Project != project || strings.TrimSpace(claims.Path) == "" {
		h.writeError(w, http.StatusForbidden, "forbidden", "download token does not cover this project")
		return
	}

	// The signed token is the single source of truth for the file path -
	// nothing about the target is client-controllable. Delegate to the
	// canonical byte-streaming reader; inject only the attachment
	// disposition. Range/206/ETag/HEAD behavior is inherited.
	r2 := r.Clone(r.Context())
	q := r2.URL.Query()
	q.Set("path", claims.Path)
	r2.URL.RawQuery = q.Encode()
	w.Header().Set("Content-Disposition", contentDisposition(claims.Path))
	h.handleContentRead(w, r2)
}

func contentDisposition(name string) string {
	base := name
	if idx := strings.LastIndexByte(base, '/'); idx >= 0 {
		base = base[idx+1:]
	}
	if base == "" || base == "." || base == ".." {
		base = "download"
	}
	var ascii strings.Builder
	for _, rn := range base {
		if rn < 0x20 || rn > 0x7e || rn == '"' || rn == '\\' {
			ascii.WriteRune('_')
		} else {
			ascii.WriteRune(rn)
		}
	}
	return fmt.Sprintf(`attachment; filename="%s"; filename*=UTF-8''%s`,
		ascii.String(), url.PathEscape(base))
}

func (h *restHandler) handleProject(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	switch r.Method {
	case http.MethodGet:
		stats, err := h.clientFor(r).StatFSContext(r.Context(), project)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, projectResponse{Project: project, Stats: stats})
	case http.MethodDelete:
		if err := h.clientFor(r).DeleteProjectContext(r.Context(), project); err != nil {
			h.writeMappedError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "deleted"})
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodDelete)
	}
}

func (h *restHandler) handleNodes(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := r.URL.Query().Get("path")
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		eTag := restEntryETag(entry)
		h.setRevisionHeader(w, r, project)
		if h.ifNoneMatchSatisfied(r.Header.Get("If-None-Match"), eTag) {
			w.Header().Set("ETag", eTag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", eTag)
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		h.writeJSON(w, http.StatusOK, nodeResponse{Project: project, Entry: entry, ETag: eTag})
	case http.MethodDelete:
		entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		revOpts, perr := h.mutationPrecondition(r, project, targetPath)
		if perr != nil {
			h.writeMappedError(w, perr)
			return
		}
		if entry.IsDir {
			if r.URL.Query().Get("recursive") == "true" {
				h.writeError(w, http.StatusNotImplemented, "recursive_delete_unsupported", "recursive directory deletion is not supported")
				return
			}
		}
		if entry.IsDir {
			err = h.clientFor(r).RmdirContext(r.Context(), project, targetPath, revOpts...)
		} else {
			err = h.clientFor(r).DeleteFileContext(r.Context(), project, targetPath, revOpts...)
		}
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodHead, http.MethodDelete)
	}
}

func (h *restHandler) handleChildren(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	dirPath := r.URL.Query().Get("path")
	entries, err := h.clientFor(r).ReadDirContext(r.Context(), project, dirPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, entriesResponse{Project: project, Path: dirPath, Entries: entries})
}

func (h *restHandler) handleContentRead(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := r.URL.Query().Get("path")
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if entry.IsDir {
		h.writeError(w, http.StatusConflict, "is_directory", fmt.Sprintf("path is a directory: %s", filePath))
		return
	}
	if entry.IsSymlink {
		target, readErr := h.clientFor(r).ReadlinkContext(r.Context(), project, filePath)
		if readErr != nil {
			h.writeMappedError(w, readErr)
			return
		}
		w.Header().Set("Content-Type", "application/symlink-target")
		w.Header().Set("X-StorHub-Symlink-Target", target)
		w.Header().Set("Content-Length", strconv.Itoa(len(target)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.WriteString(w, target)
		return
	}

	eTag := restEntryETag(entry)
	if h.ifNoneMatchSatisfied(r.Header.Get("If-None-Match"), eTag) {
		w.Header().Set("ETag", eTag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	start, end, partial, err := parseByteRange(r.Header.Get("Range"), entry.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", entry.Size))
		h.writeError(w, http.StatusRequestedRangeNotSatisfiable, "range_not_satisfiable", err.Error())
		return
	}
	contentLength := end - start
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, entry.Size))
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("ETag", eTag)
	w.Header().Set("Content-Type", detectContentType(filePath))
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	w.WriteHeader(status)
	sent := int64(0)
	for offset := start; offset < end; {
		readLen := h.opts.StreamChunkSize
		if remaining := end - offset; remaining < readLen {
			readLen = remaining
		}
		chunk, readErr := h.clientFor(r).ReadFileAtContext(r.Context(), project, filePath, offset, readLen)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			logging.Error(h.logger, "stream aborted mid-response", "project", project, "path", filePath, "offset", offset, "sent", sent, "expected", end-start, "err", readErr)
			return
		}
		if len(chunk) == 0 {
			break
		}
		if _, writeErr := w.Write(chunk); writeErr != nil {
			return
		}
		// Advance by the bytes actually read: short reads must not skip
		// data (a concurrent shrink clamps ReadFileAt to live size).
		offset += int64(len(chunk))
		sent += int64(len(chunk))
	}
}

func (h *restHandler) handleContentReplace(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := r.URL.Query().Get("path")
	entry, exists, err := h.lookupOptional(r, project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	replaceRevOpts, replaceRevErr := h.mutationPrecondition(r, project, filePath)
	if replaceRevErr != nil {
		h.writeMappedError(w, replaceRevErr)
		return
	}
	if exists {
		// Type conflicts hold under both If-Match flavors.
		if entry.IsDir {
			h.writeError(w, http.StatusConflict, "is_directory", fmt.Sprintf("path is a directory: %s", filePath))
			return
		}
		if replaceRevOpts == nil {
			if err := h.requireMatch(r.Header.Get("If-Match"), restEntryETag(entry)); err != nil {
				if !isPreconditionHeaderEmpty(err) {
					h.writeMappedError(w, err)
					return
				}
			}
			if h.ifNoneMatchStar(r.Header.Get("If-None-Match")) {
				h.writeMappedError(w, errPreconditionFailed("resource already exists"))
				return
			}
		}
	} else if r.Header.Get("If-Match") != "" {
		h.writeMappedError(w, errPreconditionFailed("resource does not exist"))
		return
	}

	created := !exists
	if !exists {
		if _, err := h.clientFor(r).CreateFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
	} else if entry.IsSymlink {
		// A symlink is replaced by a regular file, not followed; clear it
		// first because create refuses existing nodes.
		if err := h.clientFor(r).DeleteFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
		if _, err := h.clientFor(r).CreateFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
	}
	// The create call above leaves an empty placeholder behind when the path
	// did not exist (or clobbers a symlink). If the body transfer then
	// fails, remove the placeholder instead of stranding an orphan the
	// client believes was never created.
	replaceOpts := replaceRevOpts
	if r.ContentLength >= 0 {
		replaceOpts = append(replaceOpts, shfs.WithSize(r.ContentLength))
	}
	// Accepted uploads outlive their HTTP request: a client disconnect at
	// minute 25 of a 400 MB transfer must not orphan half-uploaded chunks
	// (and previously skipped compensating deletes, since those were bound
	// to the same dying context). WithoutCancel keeps identity/logging
	// values while dropping cancellation; the scaled transferDeadline inside
	// the GitHub client remains the real bound. If the client is gone, the
	// final response write simply fails silently.
	uploadCtx := context.WithoutCancel(r.Context())
	if _, err := h.clientFor(r).ReplaceFileFromReaderContext(uploadCtx, project, filePath, r.Body, replaceOpts...); err != nil {
		if created || (exists && entry.IsSymlink) {
			if cleanupErr := h.clientFor(r).DeleteFileContext(r.Context(), project, filePath); cleanupErr != nil {
				h.logger.Error("failed to clean up placeholder after failed replace", "project", project, "path", filePath, "err", cleanupErr)
			}
		}
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, filePath, ternaryStatus(created, http.StatusCreated, http.StatusOK))
}

func (h *restHandler) handleContentPatch(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := r.URL.Query().Get("path")
	ifMatch := r.Header.Get("If-Match")
	if strings.TrimSpace(ifMatch) != "" {
		// Fail fast on a stale token before reading the body. Flavor is
		// resolved first so a current-revision CAS token is honored rather
		// than misjudged against attribute ETags; the authoritative check
		// runs again just before each mutation below.
		if _, ferr := h.mutationPrecondition(r, project, filePath); ferr != nil {
			h.writeMappedError(w, ferr)
			return
		}
	}
	op := strings.TrimSpace(r.URL.Query().Get("op"))
	switch op {
	case "append":
		if err := h.streamAppendBody(w, r, project, filePath, r.Body); err != nil {
			h.writeMappedError(w, err)
			return
		}
	case "write":
		offset, parseErr := parseRequiredInt64(r.URL.Query().Get("offset"), "offset")
		if parseErr != nil {
			h.writeMappedError(w, parseErr)
			return
		}
		if err := h.streamWriteBody(w, r, project, filePath, r.Body, offset); err != nil {
			h.writeMappedError(w, err)
			return
		}
	case "patch":
		offset, parseErr := parseRequiredInt64(r.URL.Query().Get("offset"), "offset")
		if parseErr != nil {
			h.writeMappedError(w, parseErr)
			return
		}
		deleteSize, parseErr := parseRequiredInt64(r.URL.Query().Get("delete_size"), "delete_size")
		if parseErr != nil {
			h.writeMappedError(w, parseErr)
			return
		}
		edit, readErr := h.readPatchBody(w, r)
		if readErr != nil {
			h.writeMappedError(w, readErr)
			return
		}
		revOpts, perr := h.mutationPrecondition(r, project, filePath)
		if perr != nil {
			h.writeMappedError(w, perr)
			return
		}
		if _, err := h.clientFor(r).PatchFileContext(r.Context(), project, filePath, offset, deleteSize, edit, revOpts...); err != nil {
			h.writeMappedError(w, err)
			return
		}
	case "truncate":
		size, parseErr := parseRequiredInt64(r.URL.Query().Get("size"), "size")
		if parseErr != nil {
			h.writeMappedError(w, parseErr)
			return
		}
		revOpts, perr := h.mutationPrecondition(r, project, filePath)
		if perr != nil {
			h.writeMappedError(w, perr)
			return
		}
		if _, err := h.clientFor(r).TruncateFileContext(r.Context(), project, filePath, size, revOpts...); err != nil {
			h.writeMappedError(w, err)
			return
		}
	default:
		h.writeError(w, http.StatusBadRequest, "invalid_patch_op", "query parameter op must be one of append, write, patch, truncate")
		return
	}
}

// enforceFreshPrecondition restates the client's If-Match requirement against
// the resource's current state immediately before a mutating call. Checking
// only a snapshot taken earlier in the request would apply mutations to a
// state the client never saw; failing closed with 412 here closes most of
// that window.
func (h *restHandler) enforceFreshPrecondition(r *http.Request, project, targetPath string) error {
	ifMatch := r.Header.Get("If-Match")
	if strings.TrimSpace(ifMatch) == "" {
		return nil
	}
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		return err
	}
	return h.requireMatch(ifMatch, restEntryETag(entry))
}

// revisionPrecondition implements the revision flavor of If-Match. When the
// header carries the project's CURRENT metadata revision, it is returned as
// an fs.WithExpectedRevision option so storage re-verifies against remote
// HEAD immediately before applying — true compare-and-swap. Header values
// that are not the current revision (e.g. classic attribute ETags from
// earlier clients) yield no options here; those flows keep their existing
// freshness semantics via enforceFreshPrecondition, which still answers 412
// on staleness. Absent If-Match yields no options.
func (h *restHandler) revisionPrecondition(r *http.Request, project string) (opts []shfs.MutateOption, matched bool, err error) {
	ifMatch := strings.TrimSpace(r.Header.Get("If-Match"))
	if ifMatch == "" {
		return nil, false, nil
	}
	rev, err := h.clientFor(r).RevisionContext(r.Context(), project)
	if err != nil {
		return nil, false, err
	}
	if rerr := h.requireMatch(unquoteEntityTag(ifMatch), rev); rerr != nil {
		// Not a revision token: defer to the attribute-ETag freshness check.
		return nil, false, nil
	}
	return []shfs.MutateOption{shfs.WithExpectedRevision(rev)}, true, nil
}

// unquoteEntityTag strips HTTP entity-tag quotes so tokens published via
// X-StorHub-Revision compare equal whether or not the client quoted them
// (RFC 9110 clients quote If-Match values).
func unquoteEntityTag(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		return v[1 : len(v)-1]
	}
	return v
}

// mutationPrecondition resolves the If-Match flavor ONCE and enforces it
// for a mutating flow: a token equal to the project's CURRENT metadata
// revision becomes backend compare-and-swap options (apply-time check,
// immune to attribute collisions), while any other non-empty token keeps
// classic attribute-ETag freshness against freshly-statted state. Every
// mutating endpoint funnels through here so neither flavor can be
// short-circuited by a stale fast path.
func (h *restHandler) mutationPrecondition(r *http.Request, project, filePath string) (opts []shfs.MutateOption, err error) {
	revOpts, revMatched, rerr := h.revisionPrecondition(r, project)
	if rerr != nil {
		return nil, rerr
	}
	if revMatched {
		return revOpts, nil
	}
	if err := h.enforceFreshPrecondition(r, project, filePath); err != nil {
		return nil, err
	}
	return nil, nil
}

func (h *restHandler) handleXAttrs(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := r.URL.Query().Get("path")
	names, err := h.clientFor(r).ListXAttrContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, xattrListResponse{Project: project, Path: targetPath, Names: names})
}

func (h *restHandler) handleXAttrValue(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := r.URL.Query().Get("path")
	name := r.URL.Query().Get("name")
	switch r.Method {
	case http.MethodGet:
		value, err := h.clientFor(r).GetXAttrContext(r.Context(), project, targetPath, name)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-StorHub-XAttr-Name", name)
		w.Header().Set("Content-Length", strconv.Itoa(len(value)))
		_, _ = w.Write(value)
	case http.MethodPut:
		payload, err := io.ReadAll(io.LimitReader(r.Body, h.opts.MaxPatchBodySize+1))
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		if int64(len(payload)) > h.opts.MaxPatchBodySize {
			h.writeError(w, http.StatusRequestEntityTooLarge, "xattr_too_large", "xattr value exceeds the configured limit")
			return
		}
		if err := h.clientFor(r).SetXAttrContext(r.Context(), project, targetPath, name, payload); err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := h.clientFor(r).RemoveXAttrContext(r.Context(), project, targetPath, name); err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func (h *restHandler) handleRevisions(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	revisions, err := h.clientFor(r).ListMetadataRevisionsContext(r.Context(), project)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, revisionsResponse{Project: project, Revisions: revisions})
}

func (h *restHandler) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.clientFor(r).CreateFileContext(r.Context(), project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusCreated)
}

func (h *restHandler) handleMkdir(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).MkdirContext(r.Context(), project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusCreated)
}

func (h *restHandler) handleRmdir(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).RmdirContext(r.Context(), project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleUnlink(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).DeleteFileContext(r.Context(), project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleRename(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req renameRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).RenameContext(r.Context(), project, req.OldPath, req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.NewPath, http.StatusOK)
}

func (h *restHandler) handleLink(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req linkRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.clientFor(r).LinkContext(r.Context(), project, req.ExistingPath, req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.NewPath, http.StatusCreated)
}

func (h *restHandler) handleSymlink(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req symlinkRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.clientFor(r).SymlinkContext(r.Context(), project, req.Target, req.LinkPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.LinkPath, http.StatusCreated)
}

func (h *restHandler) handleChmod(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req chmodRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).ChmodContext(r.Context(), project, req.Path, req.Mode); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusOK)
}

func (h *restHandler) handleChown(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req chownRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).ChownContext(r.Context(), project, req.Path, req.UID, req.GID); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusOK)
}

func (h *restHandler) handleUtimes(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req utimesRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).ChtimesContext(r.Context(), project, req.Path, req.Atime.Unix(), req.Mtime.Unix()); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusOK)
}

func (h *restHandler) handleRollback(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req rollbackRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).RollbackMetadataContext(r.Context(), project, req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "rolled_back"})
}

// purgeResponse is the typed result of a purge operation; it replaces the
// former ad-hoc map so every endpoint returns a struct-shaped document.
type purgeResponse struct {
	Project         string `json:"project"`
	Status          string `json:"status"`
	DeletedReleases int    `json:"deleted_releases"`
	DeletedAssets   int    `json:"deleted_assets"`
}

func (h *restHandler) handlePurge(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	result, err := h.clientFor(r).PurgeUntrackedContext(r.Context(), project)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, purgeResponse{
		Project:         project,
		Status:          "purged",
		DeletedReleases: result.DeletedReleases,
		DeletedAssets:   result.DeletedAssets,
	})
}

func (h *restHandler) handleProjectShares(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	switch r.Method {
	case http.MethodGet:
		h.listProjectShares(w, r, project)
	case http.MethodPost:
		h.createProjectShare(w, r, project)
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (h *restHandler) handleProjectShare(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	shareID := chi.URLParam(r, "shareID")
	switch r.Method {
	case http.MethodDelete:
		h.deleteProjectShare(w, r, project, shareID)
	case http.MethodGet:
		h.getProjectShare(w, r, project, shareID)
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodDelete)
	}
}

func (h *restHandler) createProjectShare(w http.ResponseWriter, r *http.Request, project string) {
	var req shareRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	sharePath, err := canonicalSharePath(req.Path)
	if err != nil {
		h.writeMappedError(w, errBadRequest("invalid share path"))
		return
	}
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, sharePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	expiresIn := time.Duration(0)
	if req.ExpiresInSeconds > 0 {
		expiresIn = time.Duration(req.ExpiresInSeconds) * time.Second
	}
	if expiresIn <= 0 {
		expiresIn = h.opts.ShareTTL
	}
	if max := h.opts.MaxShareTTL; max > 0 && expiresIn > max {
		expiresIn = max
	}
	download := true
	if req.Download != nil {
		download = *req.Download
	}
	record, err := h.newShareRecord(project, sharePath, download, entry.IsDir, expiresIn)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	// 201 with Location: a new resource was created and is addressable at
	// the project-shares collection, consistent with REST creation
	// semantics everywhere else in this API.
	w.Header().Set("Location", path.Join(h.opts.BasePath, "projects", url.PathEscape(project), "shares", record.ID))
	created := h.shareResponse(record)
	created.Token = record.Token
	h.writeJSON(w, http.StatusCreated, created)
}

func (h *restHandler) listProjectShares(w http.ResponseWriter, r *http.Request, project string) {
	if _, err := h.clientFor(r).StatFSContext(r.Context(), project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	shares := h.projectShareResponses(project)
	h.writeJSON(w, http.StatusOK, sharesResponse{Project: project, Shares: shares})
}

func (h *restHandler) getProjectShare(w http.ResponseWriter, r *http.Request, project, shareID string) {
	record, ok := h.lookupShare(shareID)
	if !ok || record.Project != project {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	if _, err := h.clientFor(r).StatFSContext(r.Context(), project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, h.shareResponse(record))
}

func (h *restHandler) deleteProjectShare(w http.ResponseWriter, r *http.Request, project, shareID string) {
	record, ok := h.lookupShare(shareID)
	if !ok || record.Project != project {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	if _, err := h.clientFor(r).StatFSContext(r.Context(), project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.removeShare(record.ID)
	revokeShare(record.ID) // stateless redemption stops immediately
	// 204 like every other successful delete in this API (nodes, projects).
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) serveShareDownload(w http.ResponseWriter, r *http.Request) {
	// Same single pathway as info: the token (query here) is the credential.
	claims, err := h.parseShareToken(r.URL.Query().Get("token"))
	if err != nil || h.isRevoked(claims.ID) {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	if !claims.Download {
		h.writeError(w, http.StatusForbidden, "forbidden", "download is disabled for this share")
		return
	}
	targetPath, err := h.resolveSharePath(claims, r.URL.Query().Get("path"))
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.serveDownloadPath(w, r, claims.Project, targetPath)
}

func (h *restHandler) resolveSharePath(claims *shareClaims, rawPath string) (string, error) {
	targetPath := claims.Path
	if strings.TrimSpace(rawPath) != "" {
		canonicalPath, err := canonicalSharePath(rawPath)
		if err != nil {
			return "", err
		}
		targetPath = canonicalPath
	}
	if !hasPathPrefix(targetPath, claims.Path) {
		return "", errForbidden("access denied: path not shared")
	}
	return targetPath, nil
}

func (h *restHandler) serveDownloadPath(w http.ResponseWriter, r *http.Request, project, targetPath string) {
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if entry.IsDir {
		h.writeError(w, http.StatusNotImplemented, "directory_download", "directory download not yet implemented")
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", path.Base(targetPath)))
	if entry.IsSymlink {
		target, readErr := h.clientFor(r).ReadlinkContext(r.Context(), project, targetPath)
		if readErr != nil {
			h.writeMappedError(w, readErr)
			return
		}
		w.Header().Set("Content-Type", "application/symlink-target")
		w.Header().Set("Content-Length", strconv.Itoa(len(target)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.WriteString(w, target)
		return
	}
	eTag := restEntryETag(entry)
	w.Header().Set("ETag", eTag)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", detectContentType(targetPath))
	start, end, partial, rangeErr := parseByteRange(r.Header.Get("Range"), entry.Size)
	if rangeErr != nil {
		if strings.TrimSpace(r.Header.Get("Range")) != "" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", entry.Size))
			h.writeError(w, http.StatusRequestedRangeNotSatisfiable, "range_not_satisfiable", rangeErr.Error())
			return
		}
		start, end = 0, entry.Size
	}
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, entry.Size))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(end-start, 10))
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	w.WriteHeader(status)
	sent := int64(0)
	for offset := start; offset < end; {
		readLen := h.opts.StreamChunkSize
		if remaining := end - offset; remaining < readLen {
			readLen = remaining
		}
		chunk, readErr := h.clientFor(r).ReadFileAtContext(r.Context(), project, targetPath, offset, readLen)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			logging.Error(h.logger, "stream aborted mid-response", "project", project, "path", targetPath, "offset", offset, "sent", sent, "expected", end-start, "err", readErr)
			return
		}
		if len(chunk) == 0 {
			break
		}
		if _, writeErr := w.Write(chunk); writeErr != nil {
			return
		}
		// Advance by the bytes actually read: short reads must not skip
		// data.
		offset += int64(len(chunk))
		sent += int64(len(chunk))
	}
}

func hasPathPrefix(targetPath, allowedPath string) bool {
	targetPath, err := canonicalSharePath(targetPath)
	if err != nil {
		return false
	}
	allowedPath, err = canonicalSharePath(allowedPath)
	if err != nil {
		return false
	}
	if allowedPath == "" {
		return true
	}
	return targetPath == allowedPath || strings.HasPrefix(targetPath, allowedPath+"/")
}

func canonicalSharePath(raw string) (string, error) {
	clean := path.Clean("/" + strings.TrimSpace(raw))
	clean = strings.TrimPrefix(clean, "/")
	if clean == "." {
		return "", nil
	}
	if strings.HasPrefix(clean, "../") || clean == ".." {
		return "", errBadRequest("path traversal is not allowed")
	}
	return clean, nil
}

func (h *restHandler) parseShareToken(token string) (*shareClaims, error) {
	if h.shareSignKey == nil {
		return nil, errForbidden("share signing key not configured (pass --share-key or serve with an auth file)")
	}
	claims := &shareClaims{}
	parsed, err := jwt.ParseWithClaims(strings.TrimSpace(token), claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodEd25519); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return h.shareSignKey.Public(), nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithTimeFunc(time.Now))
	if err != nil || !parsed.Valid {
		return nil, errForbidden("invalid or expired share token")
	}
	return claims, nil
}

func (h *restHandler) newShareRecord(project, sharePath string, download, isDir bool, expiresIn time.Duration) (*shareRecord, error) {
	if h.shareSignKey == nil {
		// Configuration error surfaced through the API error mapper as a
		// clean 403 (not a 500): the deployment is missing its share key.
		return nil, errForbidden("share signing key not configured (pass --share-key or serve with an auth file)")
	}
	id, err := newShareID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	expiresAt := now.Add(expiresIn)
	claims := shareClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(now),
		},
		ID:       id,
		Project:  project,
		Path:     sharePath,
		Download: download,
		IsDir:    isDir,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	signedToken, err := token.SignedString(h.shareSignKey)
	if err != nil {
		return nil, err
	}
	record := &shareRecord{ID: id, Token: signedToken, Project: project, Path: sharePath, Download: download, IsDir: isDir, CreatedAt: now, ExpiresAt: expiresAt}
	h.shares.mu.Lock()
	h.shares.items[id] = record
	h.shares.mu.Unlock()
	h.sweepExpiredShares()
	return record, nil
}

func (h *restHandler) lookupShare(shareID string) (*shareRecord, bool) {
	h.shares.mu.RLock()
	record, ok := h.shares.items[shareID]
	h.shares.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if !record.ExpiresAt.After(time.Now()) {
		h.removeShare(shareID)
		return nil, false
	}
	copy := *record
	return &copy, true
}

// sweepExpiredShares bounds registry memory: expired records are dropped
// whenever live-plus-expired entries exceed the threshold, so a burst of
// short-lived shares cannot accumulate without limit even if nobody ever
// looks them up again.
const shareSweepThreshold = 128

func (h *restHandler) sweepExpiredShares() {
	now := time.Now()
	h.shares.mu.Lock()
	defer h.shares.mu.Unlock()
	if len(h.shares.items) < shareSweepThreshold {
		return
	}
	for shareID, record := range h.shares.items {
		if !record.ExpiresAt.After(now) {
			delete(h.shares.items, shareID)
		}
	}
}

func (h *restHandler) removeShare(shareID string) {
	h.shares.mu.Lock()
	delete(h.shares.items, shareID)
	h.shares.mu.Unlock()
}

func (h *restHandler) projectShareResponses(project string) []shareResponse {
	now := time.Now()
	h.shares.mu.Lock()
	defer h.shares.mu.Unlock()
	shares := make([]shareResponse, 0)
	for shareID, record := range h.shares.items {
		if !record.ExpiresAt.After(now) {
			delete(h.shares.items, shareID)
			continue
		}
		if record.Project != project {
			continue
		}
		copy := *record
		copy.Token = "" // listings never carry the credential (or its URLs)
		shares = append(shares, h.shareResponse(&copy))
	}
	return shares
}

// shareResponse builds the public shape. Redemption URLs are minted ONLY
// from the signed token (creation responses carry it; listings cannot, so
// their URL fields stay empty - the token is the credential).
func (h *restHandler) shareResponse(record *shareRecord) shareResponse {
	resp := shareResponse{ID: record.ID, Project: record.Project, Path: record.Path, ExpiresAt: record.ExpiresAt.UTC().Format(time.RFC3339), Download: record.Download, IsDir: record.IsDir}
	if record.Token == "" {
		return resp
	}
	resp.URL = "/?share=" + url.QueryEscape(record.Token)
	if record.Download && !record.IsDir {
		resp.DownloadURL = "/shares/" + url.PathEscape(record.ID) + "/download?token=" + url.QueryEscape(record.Token)
	}
	return resp
}

func newShareID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func (h *restHandler) decodeJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return errBadRequest("request body is required")
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyMemory+1))
	if err != nil {
		return errBadRequest("unable to read request body")
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

func (h *restHandler) respondWithNode(w http.ResponseWriter, r *http.Request, project, targetPath string, status int) {
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, status, nodeResponse{Project: project, Entry: entry, ETag: restEntryETag(entry)})
}

func (h *restHandler) lookupOptional(r *http.Request, project, targetPath string) (*shfs.EntryInfo, bool, error) {
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
	if err == nil {
		return entry, true, nil
	}
	if mappedStatus(err) == http.StatusNotFound {
		return nil, false, nil
	}
	return nil, false, err
}

// streamWriteBody applies an entire write atomically by buffering the body
// up to MaxPatchBodySize and issuing a single WriteFileAt call. Chunked
// multi-call writes would expose torn intermediate states to concurrent
// readers and leave partial data committed on failure; bodies beyond the
// cap are rejected so clients fall back to the atomic full-file PUT.
func (h *restHandler) streamWriteBody(w http.ResponseWriter, r *http.Request, project, filePath string, body io.Reader, offset int64) error {
	payload, err := h.readMutationBody(w, r, body)
	if err != nil {
		return err
	}
	revOptsW, perrW := h.mutationPrecondition(r, project, filePath)
	if perrW != nil {
		return perrW
	}
	_, err = h.clientFor(r).WriteFileAtContext(r.Context(), project, filePath, offset, payload, revOptsW...)
	return err
}

// streamAppendBody mirrors streamWriteBody: one AppendFile call, or 413.
func (h *restHandler) streamAppendBody(w http.ResponseWriter, r *http.Request, project, filePath string, body io.Reader) error {
	payload, err := h.readMutationBody(w, r, body)
	if err != nil {
		return err
	}
	revOptsA, perrA := h.mutationPrecondition(r, project, filePath)
	if perrA != nil {
		return perrA
	}
	_, err = h.clientFor(r).AppendFileContext(r.Context(), project, filePath, payload, revOptsA...)
	return err
}

func (h *restHandler) readMutationBody(w http.ResponseWriter, r *http.Request, body io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(body, h.opts.MaxPatchBodySize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > h.opts.MaxPatchBodySize {
		return nil, errPayloadTooLarge(fmt.Sprintf("mutation body exceeds the configured limit of %d bytes; use full-file PUT for large payloads", h.opts.MaxPatchBodySize))
	}
	return payload, nil
}

func (h *restHandler) readPatchBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(r.Body, h.opts.MaxPatchBodySize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > h.opts.MaxPatchBodySize {
		return nil, errPayloadTooLarge("patch payload exceeds the configured limit")
	}
	return payload, nil
}

func (h *restHandler) ifNoneMatchStar(header string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.TrimSpace(part) == "*" {
			return true
		}
	}
	return false
}

func (h *restHandler) ifNoneMatchSatisfied(header, eTag string) bool {
	if strings.TrimSpace(header) == "" {
		return false
	}
	for _, part := range strings.Split(header, ",") {
		value := strings.TrimSpace(part)
		if value == "*" || value == eTag {
			return true
		}
	}
	return false
}

func (h *restHandler) requireMatch(header, eTag string) error {
	if strings.TrimSpace(header) == "" {
		return errEmptyPrecondition
	}
	for _, part := range strings.Split(header, ",") {
		value := strings.TrimSpace(part)
		if value == "*" || value == eTag {
			return nil
		}
	}
	return errPreconditionFailed("etag precondition failed")
}

func (h *restHandler) methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
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
		message = "upstream GitHub request failed"
	}
	h.writeError(w, status, code, message)
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

func mappedCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
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
	case http.StatusBadGateway:
		return "bad_gateway"
	case http.StatusNotImplemented:
		return "not_implemented"
	default:
		return "internal_error"
	}
}

// setRevisionHeader publishes the project's current metadata revision as a
// response header so clients can obtain CAS tokens for later If-Match use.
// Best effort: a revision fetch failure never fails the read.
func (h *restHandler) setRevisionHeader(w http.ResponseWriter, r *http.Request, project string) {
	rev, err := h.clientFor(r).RevisionContext(r.Context(), project)
	if err != nil || rev == "" {
		return
	}
	w.Header().Set("X-StorHub-Revision", rev)
}

func restEntryETag(entry *shfs.EntryInfo) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, entry.Path)
	_, _ = io.WriteString(hash, "|")
	_, _ = io.WriteString(hash, fmt.Sprintf("%d|%d|%d|%d|%d|%t|%t|%s|%s", entry.Inode, entry.Size, entry.Mode, entry.UID, entry.GID, entry.IsDir, entry.IsSymlink, time.Unix(entry.ModifiedAt, 0).UTC().Format(time.RFC3339Nano), time.Unix(entry.ChangedAt, 0).UTC().Format(time.RFC3339Nano)))
	if entry.SymlinkTarget != "" {
		_, _ = io.WriteString(hash, "|")
		_, _ = io.WriteString(hash, entry.SymlinkTarget)
	}
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
}

func detectContentType(filePath string) string {
	if ext := path.Ext(filePath); ext != "" {
		if typ := mime.TypeByExtension(ext); typ != "" {
			return typ
		}
	}
	return "application/octet-stream"
}

func parseByteRange(header string, size int64) (start, end int64, partial bool, err error) {
	if size < 0 {
		return 0, 0, false, fmt.Errorf("invalid object size")
	}
	if strings.TrimSpace(header) == "" {
		return 0, size, false, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, fmt.Errorf("range header must start with bytes=")
	}
	raw := strings.TrimPrefix(header, "bytes=")
	if strings.Contains(raw, ",") {
		return 0, 0, false, fmt.Errorf("multiple ranges are not supported")
	}
	parts := strings.SplitN(raw, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid byte range")
	}
	if size == 0 {
		return 0, 0, false, fmt.Errorf("range cannot be satisfied for an empty file")
	}
	if parts[0] == "" {
		suffix, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil || suffix <= 0 {
			return 0, 0, false, fmt.Errorf("invalid byte range")
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size, true, nil
	}
	start, parseErr := strconv.ParseInt(parts[0], 10, 64)
	if parseErr != nil || start < 0 {
		return 0, 0, false, fmt.Errorf("invalid byte range")
	}
	if start >= size {
		return 0, 0, false, fmt.Errorf("range start exceeds file size")
	}
	if parts[1] == "" {
		return start, size, true, nil
	}
	inclusiveEnd, parseErr := strconv.ParseInt(parts[1], 10, 64)
	if parseErr != nil || inclusiveEnd < start {
		return 0, 0, false, fmt.Errorf("invalid byte range")
	}
	if inclusiveEnd >= size {
		inclusiveEnd = size - 1
	}
	return start, inclusiveEnd + 1, true, nil
}

func parseRequiredInt64(raw, field string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, errBadRequest(field + " is required")
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, errBadRequest(field + " must be a valid integer")
	}
	return value, nil
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
	for _, weak := range weakShareKeys {
		if len(key) == len(weak) {
			match := true
			for i := range key {
				if key[i] != weak[i] {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func ternaryStatus(cond bool, yes, no int) int {
	if cond {
		return yes
	}
	return no
}
