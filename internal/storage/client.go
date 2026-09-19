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

// Central tunables for the storage pipeline. All caps and thresholds that
// were previously scattered as magic literals live here with their
// rationale, so a limit change has one edit site.
//
//	releaseAssetCap: GitHub's per-release asset ceiling (1000). The upload
//	  picker (workflows.go), isReleaseFull, and the mock's file_count 422
//	  must agree on this value.
//	maxReleaseRotations: bound on release-full rotations per chunk upload
//	  (workflows.go chunkSink.put). Persistent-full releases (concurrent
//	  writers) previously re-listed + re-uploaded forever; exceeding the
//	  cap fails loudly instead.
//	maxNameRetries: bound on asset-name collision retries per chunk.
//	maxPendingOpsPerProject, releaseCacheTTL, metaCacheIdleTTL,
//	sweeperInterval live in caches.go (cache residency policy); they are
//	referenced here for discoverability but defined there to keep the
//	cache policy in one place.
const (
	releaseAssetCap     = 1000
	maxReleaseRotations = 5
	maxNameRetries      = 5
)

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
	ownerMu    sync.RWMutex
	repoMu     sync.RWMutex
	repoState  map[string]bool
	logger     *slog.Logger

	// Metadata management
	metaMu    sync.RWMutex
	metaCache map[string]*projectMetadata

	// In-flight fresh metadata loads, one per project. Concurrent cold-
	// cache misses join the owner's flight instead of each paying a full
	// remote reload (see loadRepoMetadataFresh).
	flightMu sync.Mutex
	flights  map[string]*loadFlight

	// Open-session table (Phase 2B). Lives on the hub so sessions die
	// with it: restarts drop everything, and no global registry can pin
	// dead hubs. Lazily created under sessionMu.
	sessionMu sync.Mutex
	sessions  *sessionHubState

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
	// shutdownDone closes when every commit loop and the sweeper have
	// exited (i.e. shutdownWg drains). It is created once and shared by
	// all Shutdown callers so a timed-out Shutdown does not strand a
	// per-call waiter goroutine parked on Wg.Wait: the waiter below is
	// the only one, and late callers observe the same channel.
	// Guarded by shutdownMu for creation; read-only afterwards.
	shutdownDone chan struct{}
	// capWarned records that the MaxTrackedProjects overflow warning has
	// fired for the current threshold crossing; guarded by metaMu.
	capWarned bool
}

// isValidation422 matches a 422 whose structured errors[] array carries the
// given code/field (empty means "don't care"), falling back to a
// lowercased substring scan for responses that predate structured parsing
// (proxies, older mocks). It is the single home of the
// errors.As+422+IsValidationIssue+substring pattern; isAlreadyExists and
// isReleaseFull are thin specializations.
func isValidation422(err error, code, field string, substrings ...string) bool {
	var apiErr *ghapi.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnprocessableEntity {
		return false
	}
	if apiErr.IsValidationIssue(code, field) {
		return true
	}
	bodyLower := strings.ToLower(apiErr.Body + " " + apiErr.Message)
	for _, sub := range substrings {
		if sub != "" && strings.Contains(bodyLower, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

// isAlreadyExists reports GitHub's duplicate-resource 422. The structural
// errors[] array (code "already_exists") is authoritative; the flat-body
// substring remains as a fallback for responses that predate structured
// parsing (proxies, older mocks).
func isAlreadyExists(err error) bool {
	return isValidation422(err, "already_exists", "", "already_exists")
}

// isReleaseFull reports the release-asset ceiling 422. The live prod body
// (v18 probe) carries errors[].field "file_count" - matched structurally;
// the legacy substrings cover older body shapes.
func isReleaseFull(err error) bool {
	return isValidation422(err, "", "file_count", "file_count", "1000", "too many")
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
	// Hot read paths (list/download/stat) stay silent at Debug: logging
	// every list-files/list-releases/read at Debug turned an idle mount's
	// thousands-of-ops-per-minute into formatted stderr traffic. Mutating
	// and commit paths keep their Debug start; read failures still log
	// via logOpFinish's error branch.
	if isReadOnlyOp(op) {
		return time.Time{}
	}
	logging.Debug(h.projectLogger(project), op+" start", args...)
	return time.Now().UTC()
}

func (h *StorHub) logOpFinish(project, op string, started time.Time, err error, args ...any) {
	// Demoted read ops pass a zero start (logOpStart skipped them);
	// report a zero elapsed instead of time.Since(zero).
	elapsed := time.Duration(0)
	if !started.IsZero() {
		elapsed = time.Since(started)
	}
	if err != nil {
		args = append(args, "elapsed", elapsed, "err", err)
		logging.Error(h.projectLogger(project), op+" failed", args...)
		return
	}
	if isReadOnlyOp(op) {
		return
	}
	args = append(args, "elapsed", elapsed)
	logging.Debug(h.projectLogger(project), op+" complete", args...)
}

// isReadOnlyOp reports the ops whose per-call Debug start/complete logs are
// demoted away. The set is the read-only verbs (list/stat/download paths);
// every mutating verb keeps its Debug span. Errors always log regardless.
func isReadOnlyOp(op string) bool {
	switch op {
	case "list-files", "list-releases", "list-metadata-revisions",
		"download-file":
		return true
	default:
		return false
	}
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
		flights:      make(map[string]*loadFlight),
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
	h.ownerMu.RLock()
	if strings.TrimSpace(h.owner) != "" {
		h.ownerMu.RUnlock()
		return nil
	}
	h.ownerMu.RUnlock()

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

	// Wait for all commit loops to finish with timeout. The waiter is
	// shared: a single goroutine closes shutdownDone when shutdownWg
	// drains, so N concurrent/timed-out Shutdown calls observe one
	// channel instead of stranding N waiters parked on Wg.Wait.
	h.shutdownMu.Lock()
	if h.shutdownDone == nil {
		h.shutdownDone = make(chan struct{})
		go func(done chan struct{}) {
			h.shutdownWg.Wait()
			close(done)
		}(h.shutdownDone)
	}
	done := h.shutdownDone
	h.shutdownMu.Unlock()

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
	return h.config.Now().UnixNano()
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
