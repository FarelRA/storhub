package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

const maxMetadataBytes = 8 << 20

var githubRepoNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type (
	Config           = storcfg.Config
	ChunkInfo        = metadata.ChunkInfo
	FileMeta         = metadata.FileMeta
	RepoMetadata     = metadata.RepoMetadata
	MetadataRevision = metadata.MetadataRevision
	DirMeta          = metadata.DirMeta
	NodeKind         = metadata.NodeKind
	ReleaseRef       = metadata.ReleaseRef
)

const (
	NodeKindFile    = metadata.NodeKindFile
	NodeKindSymlink = metadata.NodeKindSymlink
)

func DefaultConfig() Config {
	return storcfg.Default()
}

func NewRepoMetadata(project string) *RepoMetadata {
	return metadata.NewRepoMetadata(project)
}

type StorHub struct {
	token      string
	owner      string
	gh         *ghapi.Client
	config     storcfg.Config
	bufferPool sync.Pool
	ownerMu    sync.Mutex
	repoMu     sync.Mutex
	repoState  map[string]bool
	logger     *slog.Logger

	// Metadata management
	metaMu    sync.RWMutex
	metaCache map[string]*projectMetadata

	// Release list cache to avoid per-upload ListReleases (secondary rate limit)
	releaseMu    sync.RWMutex
	releaseCache map[string]releaseCacheEntry

	// Git repository cache for metadata operations. RLock serves lookups
	// (the common case); only insertion and teardown take the write lock.
	gitMu    sync.RWMutex
	gitRepos map[string]*gitRepo

	// Content-addressed index object caches (split layout), one per project.
	// Same RLock-for-reads discipline as gitMu.
	objCacheMu sync.RWMutex
	objCaches  map[string]*objectCache

	// Cached per-project loggers: projectLogger is on the hot path of every
	// operation and logger.With allocates a new slog.Logger per call.
	loggers sync.Map // project -> *slog.Logger

	// Hoisted fs/posix services: they are stateless wrappers over the hub,
	// so one instance per hub suffices (allocating per call was pure GC load).
	fsSvc    *shfs.Service
	posixSvc *implposix.Service

	// Op-journal group-commit state (journal.go): append handles stay open,
	// fsyncs coalesce behind a short window instead of one per appended op.
	journalMu    sync.Mutex
	journalFiles map[string]*os.File
	journalDirty map[string]bool
	journalTimer *time.Timer

	// Shutdown coordination
	shutdownOnce sync.Once
	// gitCleanupOnce guards the per-project git mirror removal so
	// repeated Shutdown calls still drain without releasing twice.
	gitCleanupOnce sync.Once
	shutdownCh     chan struct{}
	shutdownWg     sync.WaitGroup
	// baseCtx is the hub-level context every background commit derives from.
	// Shutdown cancels it, so an in-flight push unwinds promptly instead of
	// parking the shutdown wait on the HTTP client's own timeout.
	baseCtx    context.Context
	baseCancel context.CancelFunc
	// shutdownMu guards shutdownStarted. Revival/creation paths take it
	// around shutdownWg.Add, and Shutdown sets the flag before its Wait:
	// an Add that wins the mutex happens-before the Wait, one that loses
	// is skipped (the drain commits the state instead). This closes the
	// WaitGroup-misuse window where Add raced Wait with a zero counter.
	shutdownMu      sync.Mutex
	shutdownStarted bool
	// capWarned records that the MaxTrackedProjects overflow warning has
	// fired for the current threshold crossing; guarded by metaMu.
	capWarned bool
}

// isAlreadyExists reports GitHub's duplicate-resource 422. The structural
// errors[] array (code "already_exists") is authoritative; the flat-body
// substring remains as a fallback for responses that predate structured
// parsing (proxies, older mocks).
func isAlreadyExists(err error) bool {
	var apiErr *ghapi.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnprocessableEntity {
		return false
	}
	if apiErr.IsValidationIssue("already_exists", "") {
		return true
	}
	bodyLower := strings.ToLower(apiErr.Body + " " + apiErr.Message)
	return strings.Contains(bodyLower, "already_exists")
}

// isReleaseFull reports the release-asset ceiling 422. The live prod body
// (v18 probe) carries errors[].field "file_count" - matched structurally;
// the legacy substrings cover older body shapes.
func isReleaseFull(err error) bool {
	var apiErr *ghapi.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnprocessableEntity {
		return false
	}
	if apiErr.IsValidationIssue("", "file_count") {
		return true
	}
	bodyLower := strings.ToLower(apiErr.Body + " " + apiErr.Message)
	return strings.Contains(bodyLower, "file_count") || strings.Contains(bodyLower, "1000") || strings.Contains(bodyLower, "too many")
}

func (h *StorHub) debugf(format string, args ...any) {
	// Guard the level before Sprintf: at Info level the format work was
	// pure discarded allocation on every call.
	if !h.logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	logging.Debug(h.logger, fmt.Sprintf(format, args...))
}

func (h *StorHub) logOpStart(project, op string, args ...any) time.Time {
	logging.Debug(h.projectLogger(project), op+" start", args...)
	return time.Now().UTC()
}

func (h *StorHub) logOpFinish(project, op string, started time.Time, err error, args ...any) {
	if err != nil {
		args = append(args, "elapsed", time.Since(started), "err", err)
		logging.Error(h.projectLogger(project), op+" failed", args...)
		return
	}
	args = append(args, "elapsed", time.Since(started))
	logging.Debug(h.projectLogger(project), op+" complete", args...)
}

func NewStorHub(token string) (*StorHub, error) {
	return NewStorHubWithContext(context.Background(), token, DefaultConfig())
}

func NewStorHubWithConfig(token string, cfg Config) (*StorHub, error) {
	return NewStorHubWithContext(context.Background(), token, cfg)
}

func NewStorHubWithContext(ctx context.Context, token string, cfg Config) (*StorHub, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("token is required")
	}

	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	// Unset git cache dir takes the shared XDG default
	// (~/.cache/storhub/git); project directories beneath it are claimed
	// by lockfile and removed on Shutdown.
	if strings.TrimSpace(cfg.GitCacheDir) == "" {
		cfg.GitCacheDir = storcfg.DefaultGitCacheBase()
	}
	baseCtx, baseCancel := context.WithCancel(ctx)
	hub := &StorHub{
		token:        token,
		gh:           ghapi.NewClient(token, cfg),
		config:       cfg,
		repoState:    make(map[string]bool),
		metaCache:    make(map[string]*projectMetadata),
		releaseCache: make(map[string]releaseCacheEntry),
		gitRepos:     make(map[string]*gitRepo),
		objCaches:    make(map[string]*objectCache),
		logger:       logging.WithComponent(cfg.Logger, "storage"),
		shutdownCh:   make(chan struct{}),
		baseCtx:      baseCtx,
		baseCancel:   baseCancel,
		journalFiles: make(map[string]*os.File),
		journalDirty: make(map[string]bool),
		bufferPool: sync.Pool{New: func() any {
			buf := make([]byte, cfg.BufferSize)
			return &buf
		}},
	}
	hub.fsSvc = shfs.NewService(hub)
	hub.posixSvc = implposix.NewService(hub)
	// The in-memory sweeper TTL-evicts idle clean caches and force-retries
	// oversized pending stacks; it is ctx-bound and joined on Shutdown.
	hub.shutdownWg.Add(1)
	go hub.sweeperLoop()
	return hub, nil
}

func (h *StorHub) Owner() string { return h.owner }

func (h *StorHub) ensureOwner(ctx context.Context) error {
	h.ownerMu.Lock()
	if strings.TrimSpace(h.owner) != "" {
		h.ownerMu.Unlock()
		return nil
	}
	h.ownerMu.Unlock()

	owner, err := h.getAuthenticatedUser(ctx)
	if err != nil {
		return fmt.Errorf("resolve authenticated user: %w", err)
	}

	h.ownerMu.Lock()
	if strings.TrimSpace(h.owner) == "" {
		h.owner = owner
	}
	h.ownerMu.Unlock()
	return nil
}

// Shutdown gracefully shuts down the StorHub, committing any dirty metadata.
// It is safe to call on a client that was never fully started (or twice);
// uninitialized machinery is simply skipped. Only the stop broadcast is
// once-guarded: every call waits for loops and sweeps stranded dirty state,
// so a mutation that landed after its loop exited still converges.
func (h *StorHub) Shutdown(ctx context.Context) error {
	h.shutdownOnce.Do(func() {
		logging.Info(h.logger, "shutdown initiated")

		// Mark the hub shutting down BEFORE the Wait below: revival and
		// creation paths check the flag under shutdownMu around their
		// shutdownWg.Add, so no Add can race the Wait with a zero counter.
		h.shutdownMu.Lock()
		h.shutdownStarted = true
		h.shutdownMu.Unlock()

		// Cancel the hub-level ctx so commits in flight unwind now instead
		// of parking the wait on the HTTP client's own timeout.
		if h.baseCancel != nil {
			h.baseCancel()
		}
		if h.shutdownCh != nil {
			// Signal all commit loops (and the sweeper) to stop
			close(h.shutdownCh)
		}
	})

	// Wait for all commit loops to finish with timeout
	done := make(chan struct{})
	go func() {
		h.shutdownWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		shutdownErr := fmt.Errorf("shutdown timeout: %w", ctx.Err())
		logging.Error(h.logger, "shutdown timeout", "err", ctx.Err())
		return shutdownErr
	}

	// Every loop has exited; flush and release any journal append handles
	// still buffered behind the group-commit window before the drain
	// rewrites journals.
	h.closeJournals()

	// Contract: the per-project git cache is a pure mirror of
	// remote state, so Shutdown removes the directories this
	// hub claimed. Re-clone on next use. Once-guarded: a second
	// Shutdown must still drain below without releasing twice.
	h.gitCleanupOnce.Do(func() {
		h.gitMu.Lock()
		for name, r := range h.gitRepos {
			if err := r.release(true); err != nil {
				logging.Warn(h.logger, "shutdown git cache cleanup failed", "project", name, "err", err)
			}
		}
		h.gitMu.Unlock()
	})

	// Sweep: a trigger poke to a loop that already exited wakes
	// nobody, and a post-shutdown mutation never had a live loop at
	// all. Commit any still-dirty projects synchronously so Shutdown
	// converges instead of dropping them.
	if err := h.drainDirtyMetadata(ctx); err != nil {
		return err
	}
	logging.Info(h.logger, "shutdown complete")
	return nil
}

func validateProject(project string) error {
	project = strings.TrimSpace(project)
	if project == "" {
		return errors.New("project is required")
	}
	if len(project) > 100 {
		return fmt.Errorf("project name too long: %d", len(project))
	}
	if project == "." || project == ".." {
		return fmt.Errorf("invalid project name: %s", project)
	}
	if !githubRepoNamePattern.MatchString(project) {
		return fmt.Errorf("invalid project name: %s", project)
	}
	if strings.HasPrefix(project, ".") || strings.HasSuffix(project, ".") {
		return fmt.Errorf("invalid project name: %s", project)
	}
	return nil
}

func shortSHA(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

func extractAPIError(err error) *ghapi.APIError {
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return nil
}

func isRetryableDownloadError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}
	// Transient CDN trouble (throttle, 5xx) must retry like any other
	// network hiccup; permanent statuses stay terminal.
	var cdnErr *ghapi.CDNError
	if errors.As(err, &cdnErr) {
		return cdnErr.Transient()
	}
	return isRetryableNetworkError(err)
}

func defaultFileMode(kind NodeKind) uint32 {
	switch kind {
	case NodeKindSymlink:
		return 0o777
	default:
		return 0o644
	}
}

func defaultDirMode() uint32 {
	return 0o755
}

func defaultOwnerIDs() (uint32, uint32) {
	return implposix.DefaultOwnerIDs()
}

func (h *StorHub) NewFUSE(project string, opts fusefs.Options) (*fusefs.Filesystem, error) {
	if opts.Logger == nil {
		opts.Logger = logging.WithComponent(h.logger, "fuse")
	}
	return fusefs.New(h, project, opts)
}

func (h *StorHub) Now() int64 {
	return h.config.Now().Unix()
}

func (h *StorHub) ChunkSize() int64 {
	return h.config.ChunkSize
}

// fsService/posixService return the hub's single cached services. They are
// stateless wrappers over the hub, so allocating a fresh one per call was
// pure GC churn on every fs/posix verb.
func (h *StorHub) fsService() *shfs.Service {
	return h.fsSvc
}

func (h *StorHub) posixService() *implposix.Service {
	return h.posixSvc
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
