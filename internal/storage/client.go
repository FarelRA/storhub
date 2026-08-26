package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
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

	// Git repository cache for metadata operations
	gitMu    sync.Mutex
	gitRepos map[string]*gitRepo

	// Shutdown coordination
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	shutdownWg   sync.WaitGroup
	// capWarned records that the MaxTrackedProjects overflow warning has
	// fired for the current threshold crossing; guarded by metaMu.
	capWarned bool
}

// projectMetadata holds metadata for a single project with batched commit support
type projectMetadata struct {
	mu         sync.RWMutex
	commitMu   sync.Mutex
	meta       *metadata.RepoMetadata
	sha        string
	dirty      bool
	version    uint64
	lastCommit time.Time
	lastAccess time.Time
	stopCh     chan struct{}
	stoppedCh  chan struct{}
	triggerCh  chan struct{}
	// hydrated records that this instance's meta reflects a remote load
	// (or a confirmed-new project). Cold caches must hydrate before any
	// mutation, or the mutation commits an empty tree over remote state.
	hydrated bool
	// reviving guards the eviction-revival critical section under metaMu:
	// without it two concurrent mutators can both pass the stopped check
	// and swap channels under a freshly started commit loop.
	reviving bool
	// stopped is set when this instance was evicted (capacity pressure or
	// explicit invalidation). A long operation that captured the pointer
	// before eviction must not strand its acknowledged mutations on a dead
	// commit loop; markProjectDirtyLive revives the instance instead.
	stopped bool
}

type releaseCacheEntry struct {
	releases []ghapi.Release
}

func (h *StorHub) getCachedReleases(project string) ([]ghapi.Release, bool) {
	h.releaseMu.RLock()
	defer h.releaseMu.RUnlock()
	entry, ok := h.releaseCache[project]
	if !ok {
		return nil, false
	}
	out := make([]ghapi.Release, len(entry.releases))
	copy(out, entry.releases)
	return out, true
}

func (h *StorHub) setCachedReleases(project string, releases []ghapi.Release) {
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	cp := make([]ghapi.Release, len(releases))
	copy(cp, releases)
	h.releaseCache[project] = releaseCacheEntry{releases: cp}
}

func (h *StorHub) invalidateReleaseCache(project string) {
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	delete(h.releaseCache, project)
}

func (h *StorHub) addReleaseToCache(project string, release *ghapi.Release) {
	if release == nil {
		return
	}
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	entry, ok := h.releaseCache[project]
	if !ok {
		h.releaseCache[project] = releaseCacheEntry{releases: []ghapi.Release{*release}}
		return
	}
	entry.releases = append(append([]ghapi.Release(nil), entry.releases...), *release)
	h.releaseCache[project] = entry
}

func (h *StorHub) bumpCachedReleaseAssetCount(project, tag string) {
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	entry, ok := h.releaseCache[project]
	if !ok {
		return
	}
	for i := range entry.releases {
		if entry.releases[i].TagName == tag {
			entry.releases[i].Assets = append(entry.releases[i].Assets, ghapi.Asset{ID: -1})
			h.releaseCache[project] = entry
			return
		}
	}
}

func isAlreadyExists(err error) bool {
	var apiErr *ghapi.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnprocessableEntity {
		return false
	}
	bodyLower := strings.ToLower(apiErr.Body + " " + apiErr.Message)
	return strings.Contains(bodyLower, "already_exists")
}

func isReleaseFull(err error) bool {
	var apiErr *ghapi.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnprocessableEntity {
		return false
	}
	bodyLower := strings.ToLower(apiErr.Body + " " + apiErr.Message)
	return strings.Contains(bodyLower, "file_count") || strings.Contains(bodyLower, "1000") || strings.Contains(bodyLower, "too many")
}

// ensureHydratedLocked applies the cold-cache guard to direct metadata
// writers that bypass UpdateRepoMetadataContext (advisory atime queueing).
// Caller holds pm.mu; the lock is dropped and re-acquired around the
// remote load, mirroring the transaction path. A load failure fails
// closed: the caller must not touch an unhydrated tree.
func (h *StorHub) ensureHydratedLocked(ctx context.Context, project string, pm *projectMetadata) error {
	if pm.hydrated {
		return nil
	}
	pm.mu.Unlock()
	loaded, loadedSHA, loadErr := h.loadRepoMetadataFresh(ctx, project)
	pm.mu.Lock()
	switch {
	case loadErr == nil:
		if !pm.hydrated && !pm.dirty {
			pm.meta = loaded
			pm.sha = loadedSHA
		}
		pm.hydrated = true
	case errors.Is(loadErr, shfs.ErrNotFound):
		pm.hydrated = true
	default:
		return fmt.Errorf("hydrate metadata before atime update: %w", loadErr)
	}
	return nil
}

func markProjectDirtyLocked(pm *projectMetadata) {
	pm.dirty = true
	pm.version++
}

// markProjectDirtyLiveLocked marks pm dirty, reviving it first if it was
// evicted while an operation was still using the pointer.
// Revival puts the same instance back into the cache with fresh loop
// channels, so a mutation acknowledged to the caller can never silently
// strand on a commit loop that already stopped. When a fresher incarnation
// owns the cache slot, the stale snapshot's changes cannot be applied and
// this fails loudly instead of losing them silently.
//
// It returns the trigger channel to poke after releasing pm.mu. Reading
// the field directly post-unlock would race a revival's channel swap;
// the returned value was read under the final pm.mu critical section,
// giving callers a synchronized handle. A revival failure returns the
// stale channel: poking it is a harmless no-op.
//
// Caller must hold pm.mu; the lock is dropped and re-acquired around cache
// bookkeeping so metaMu→pm.mu ordering stays consistent with
// getOrCreateProjectMeta.
func (h *StorHub) markProjectDirtyLiveLocked(project string, pm *projectMetadata) chan struct{} {
	if !pm.stopped {
		markProjectDirtyLocked(pm)
		return pm.triggerCh
	}
	pm.mu.Unlock()
	// Wait for the evicted commit loop to fully exit before replacing its
	// channels: the loop reads stopCh/triggerCh unsynchronized, and the
	// eviction close(stopCh) only requests exit — stoppedCh closes when it
	// has actually returned.
	select {
	case <-pm.stoppedCh:
	case <-time.After(5 * time.Second):
		logging.Error(h.projectLogger(project), "evicted commit loop did not stop; skipping metadata revival", "project", project)
		pm.mu.Lock()
		return pm.triggerCh
	}
	h.metaMu.Lock()
	current, exists := h.metaCache[project]
	revived := false
	live := false
	switch {
	case exists && current != pm:
		logging.Error(h.projectLogger(project),
			"evicted metadata snapshot diverged from a newer reload; mutations in this window are lost",
			"project", project)
	case exists && current == pm && !current.stopped:
		// Another goroutine completed the revival while we waited on
		// stoppedCh/metaMu; the instance is live again — just mark dirty.
		live = true
	case pm.reviving:
		// A concurrent revival is between channel swap and loop start;
		// touching channels here would orphan its fresh commit loop.
		live = true
	default:
		// Revive: fresh channels for a new commit loop, then re-insert.
		pm.reviving = true
		pm.stopCh = make(chan struct{})
		pm.stoppedCh = make(chan struct{})
		pm.triggerCh = make(chan struct{}, 1)
		pm.stopped = false
		if !exists {
			h.metaCache[project] = pm
		}
		revived = true
	}
	h.metaMu.Unlock()
	if revived {
		logging.Info(h.projectLogger(project), "reviving evicted project metadata after concurrent operation", "project", project)
		h.shutdownWg.Add(1)
		go h.commitLoop(project, pm)
		h.metaMu.Lock()
		pm.reviving = false
		h.metaMu.Unlock()
	}
	pm.mu.Lock()
	if revived || live {
		markProjectDirtyLocked(pm)
	}
	return pm.triggerCh
}

func (h *StorHub) debugf(format string, args ...any) {
	logging.Debug(h.logger, fmt.Sprintf(format, args...))
}

func (h *StorHub) logOpStart(project, op string, args ...any) time.Time {
	logging.Info(h.projectLogger(project), op+" start", args...)
	return time.Now().UTC()
}

func (h *StorHub) logOpFinish(project, op string, started time.Time, err error, args ...any) {
	args = append(args, "elapsed", time.Since(started))
	if err != nil {
		args = append(args, "err", err)
		logging.Error(h.projectLogger(project), op+" failed", args...)
		return
	}
	logging.Info(h.projectLogger(project), op+" complete", args...)
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
	hub := &StorHub{
		token:        token,
		gh:           ghapi.NewClient(token, cfg),
		config:       cfg,
		repoState:    make(map[string]bool),
		metaCache:    make(map[string]*projectMetadata),
		releaseCache: make(map[string]releaseCacheEntry),
		gitRepos:     make(map[string]*gitRepo),
		logger:       logging.WithComponent(cfg.Logger, "storage"),
		shutdownCh:   make(chan struct{}),
		bufferPool: sync.Pool{New: func() any {
			buf := make([]byte, cfg.BufferSize)
			return &buf
		}},
	}
	return hub, nil
}

func (h *StorHub) getGitRepo(project string) *gitRepo {
	if h.config.DisableGitBackend {
		return nil
	}
	h.gitMu.Lock()
	defer h.gitMu.Unlock()
	if r, ok := h.gitRepos[project]; ok {
		return r
	}
	r := newGitRepo(h.config.GitCacheDir, h.owner, project, h.token)
	h.gitRepos[project] = r
	return r
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

// getOrCreateProjectMeta returns the projectMetadata for a project, creating it if needed
func (h *StorHub) getOrCreateProjectMeta(project string) *projectMetadata {
	// Fast path: read lock to check if exists
	h.metaMu.RLock()
	pm, exists := h.metaCache[project]
	h.metaMu.RUnlock()

	if exists {
		pm.mu.Lock()
		pm.lastAccess = h.config.Now()
		pm.mu.Unlock()
		return pm
	}

	// Slow path: write lock to create
	h.metaMu.Lock()
	defer h.metaMu.Unlock()

	// Double-check after acquiring write lock
	pm, exists = h.metaCache[project]
	if exists {
		pm.mu.Lock()
		pm.lastAccess = h.config.Now()
		pm.mu.Unlock()
		return pm
	}

	now := h.config.Now()
	// Enforce the residency cap before adding another entry: growth is an
	// event, so the cap is applied exactly when a new project joins.
	h.evictForCapacityLocked()
	// Create new projectMetadata
	pm = &projectMetadata{
		meta:       metadata.NewRepoMetadata(project),
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
		triggerCh:  make(chan struct{}, 1),
		lastAccess: now,
	}
	h.metaCache[project] = pm

	// Start commit loop goroutine
	h.shutdownWg.Add(1)
	go h.commitLoop(project, pm)

	return pm
}

// commitLoop commits dirty metadata when events demand it: a mutation
// trigger or the final drain at shutdown. It never polls; idle cost is
// one parked goroutine per tracked project. A failed push retains dirty
// state and is retried by the next trigger on that project, an explicit
// FlushMetadata/FlushProjectContext, or Shutdown.
func (h *StorHub) commitLoop(project string, pm *projectMetadata) {
	defer h.shutdownWg.Done()
	defer close(pm.stoppedCh)

	logger := h.projectLogger(project)

	for {
		select {
		case <-pm.stopCh:
			// Project evicted, stop the commit loop
			return
		case <-pm.triggerCh:
			// Wake up and commit if dirty, then continue loop.
			if err := h.commitProjectMetadata(context.Background(), project, pm); err != nil {
				h.recoverMetadataCommitFailure(project, err)
			}

		case <-h.shutdownCh:
			// Shutdown requested
			if err := h.commitProjectMetadata(context.Background(), project, pm); err != nil {
				logging.Error(logger, "shutdown metadata commit failed", "err", err)
				return
			}
			return
		}
	}
}

// commitError carries the metadata version a failed commit attempted, so
// recovery can tell whether newer mutations arrived after the snapshot.
type commitError struct {
	err     error
	version uint64
}

func (e *commitError) Error() string { return e.err.Error() }
func (e *commitError) Unwrap() error { return e.err }

func (h *StorHub) recoverMetadataCommitFailure(project string, err error) {
	logger := h.projectLogger(project)
	// Conflict (stale previous_sha against remote HEAD) means another
	// writer advanced the metadata: reload and discard local uncommitted
	// state, matching what actually happened remotely — but only when no
	// newer mutation landed after the failed snapshot. Any other failure
	// is transient: retain dirty state so the loop retries with it.
	var apiErr *ghapi.APIError
	var cerr *commitError
	isConflict := errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict
	if !isConflict {
		logging.Error(logger, "metadata commit failed; retaining dirty metadata (heals on next mutation, FlushMetadata, or shutdown)", "err", err)
		return
	}
	if errors.As(err, &cerr) {
		pm := h.getOrCreateProjectMeta(project)
		pm.mu.Lock()
		mutatedSince := pm.version != cerr.version
		pm.mu.Unlock()
		if mutatedSince {
			logging.Error(logger, "metadata commit conflicted; newer local mutations pending, retaining them for retry", "err", err)
			return
		}
	}
	logging.Error(logger, "metadata commit conflicted, reloading from github", "err", err)
	if _, _, loadErr := h.loadRepoMetadataFresh(context.Background(), project); loadErr != nil {
		logging.Error(logger, "failed to reload metadata after conflict; preserving dirty metadata for retry", "err", loadErr)
		return
	}
	logging.Info(logger, "metadata reloaded from github, in-memory changes discarded")
}

// commitProjectMetadata commits dirty metadata without holding pm.mu during GitHub I/O.
func (h *StorHub) commitProjectMetadata(ctx context.Context, project string, pm *projectMetadata) error {
	pm.commitMu.Lock()
	defer pm.commitMu.Unlock()

	started := h.config.Now().UTC()
	pm.mu.Lock()
	if !pm.dirty {
		pm.mu.Unlock()
		return nil
	}
	pm.meta.Normalize(project, h.config.Now().Unix())
	pm.meta.LastMod = h.config.Now().Unix()
	pm.meta.RecomputeStats()
	meta := pm.meta.Clone()
	previousSHA := pm.sha
	version := pm.version
	pm.mu.Unlock()

	logging.Info(h.projectLogger(project), "commit metadata start", "previous_sha", shortSHA(previousSHA))

	if err := h.ensureOwner(ctx); err != nil {
		return err
	}

	if err := meta.Validate(); err != nil {
		logging.Error(h.projectLogger(project), "commit metadata failed", "step", "validate", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return &commitError{err: fmt.Errorf("invalid metadata: %w", err), version: version}
	}

	metaBytes, err := meta.ToJSON()
	if err != nil {
		logging.Error(h.projectLogger(project), "commit metadata failed", "step", "serialize", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return &commitError{err: fmt.Errorf("marshal metadata: %w", err), version: version}
	}

	if len(metaBytes) > maxMetadataBytes {
		err := fmt.Errorf("metadata too large: %d bytes (max %d)", len(metaBytes), maxMetadataBytes)
		logging.Error(h.projectLogger(project), "commit metadata failed", "step", "size_check", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return &commitError{err: err, version: version}
	}

	// Commit metadata
	message := "storhub: update metadata"
	var commitSHA, contentSHA string
	if repo := h.getGitRepo(project); repo != nil {
		commitSHA, contentSHA, err = repo.writeCommitPush(ctx, metadataFilePath, metaBytes, message)
	} else {
		commitSHA, contentSHA, err = h.gh.PutFileContent(ctx, h.owner, project, metadataFilePath, metaBytes, previousSHA, message)
	}
	if err != nil {
		err = wrapNoSpace(h.config.GitCacheDir, err)
		logging.Error(h.projectLogger(project), "commit metadata failed", "step", "git_commit", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return &commitError{err: fmt.Errorf("commit metadata: %w", err), version: version}
	}

	pm.mu.Lock()
	pm.sha = contentSHA
	if pm.version == version {
		pm.dirty = false
		pm.lastCommit = h.config.Now()
	}
	pm.mu.Unlock()

	logging.Info(h.projectLogger(project), "commit metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "commit_sha", shortSHA(commitSHA), "content_sha", shortSHA(contentSHA), "bytes", len(metaBytes))

	return nil
}

// evictForCapacityLocked keeps the tracked-project set at
// MaxTrackedProjects by evicting the least-recently-used clean entry at
// insert time. Victims must be clean (nothing unpushed) and not mid-
// revival; a dirty entry survives arbitrarily long. When no victim
// qualifies, insertion proceeds unbounded and warns once per threshold
// crossing - failing an unrelated operation would be worse than honest
// degradation. Alternating access to more projects than the cap thrashes
// remote reloads per miss; that costs bandwidth, never data. Revival
// re-insertions (markProjectDirtyLiveLocked) deliberately bypass this
// cap - they serve an operation already in flight; overshoot is bounded
// by concurrently stranded stale pointers.
// Caller holds metaMu for writing.
func (h *StorHub) evictForCapacityLocked() {
	if len(h.metaCache) < h.config.MaxTrackedProjects {
		h.capWarned = false
		return
	}
	type candidate struct {
		name       string
		lastAccess time.Time
	}
	var cands []candidate
	for name, pm := range h.metaCache {
		pm.mu.RLock()
		eligible := !pm.dirty && !pm.reviving && !pm.stopped
		last := pm.lastAccess
		pm.mu.RUnlock()
		if eligible {
			cands = append(cands, candidate{name: name, lastAccess: last})
		}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].lastAccess.Before(cands[j].lastAccess) })
	// Walk candidates oldest-first. Re-check under pm.mu so a mutation
	// racing this eviction either lands before (entry stays dirty, the
	// next candidate gets its turn) or sees stopped=true and revives via
	// markProjectDirtyLiveLocked. metaMu is held throughout, so the map
	// itself is stable while we walk.
	for _, cand := range cands {
		pm := h.metaCache[cand.name]
		pm.mu.Lock()
		evictable := !pm.dirty && !pm.reviving && !pm.stopped
		if evictable {
			pm.stopped = true
			close(pm.stopCh)
			delete(h.metaCache, cand.name)
		}
		pm.mu.Unlock()
		if evictable {
			// Successful enforcement re-arms the overflow warning: at
			// steady state the cache pins len == cap forever, so without
			// this the first crossing would silence all later ones.
			h.capWarned = false
			return
		}
	}
	if !h.capWarned {
		logging.Warn(h.logger, "tracked projects exceed cap with no evictable entry; inserting unbounded", "cap", h.config.MaxTrackedProjects, "resident", len(h.metaCache))
		h.capWarned = true
	}
}

// Shutdown gracefully shuts down the StorHub, committing any dirty metadata.
// It is safe to call on a client that was never fully started (or twice);
// uninitialized machinery is simply skipped.
func (h *StorHub) Shutdown(ctx context.Context) error {
	var shutdownErr error
	h.shutdownOnce.Do(func() {
		logging.Info(h.logger, "shutdown initiated")

		if h.shutdownCh != nil {
			// Signal all commit loops to stop
			close(h.shutdownCh)
		}

		// Wait for all commit loops to finish with timeout
		done := make(chan struct{})
		go func() {
			h.shutdownWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Contract: the per-project git cache is a pure mirror of
			// remote state, so Shutdown removes the directories this
			// hub claimed. Re-clone on next use.
			h.gitMu.Lock()
			for name, r := range h.gitRepos {
				if err := r.release(true); err != nil {
					logging.Warn(h.logger, "shutdown git cache cleanup failed", "project", name, "err", err)
				}
			}
			h.gitMu.Unlock()
			logging.Info(h.logger, "shutdown complete")
		case <-ctx.Done():
			shutdownErr = fmt.Errorf("shutdown timeout: %w", ctx.Err())
			logging.Error(h.logger, "shutdown timeout", "err", ctx.Err())
		}
	})

	return shutdownErr
}

// FlushMetadata forces an immediate commit of all dirty metadata for all projects
// This is useful for testing or when you need to ensure metadata is persisted immediately
func (h *StorHub) FlushMetadata(ctx context.Context) error {
	h.metaMu.RLock()
	type projectWithName struct {
		name string
		meta *projectMetadata
	}
	projects := make([]projectWithName, 0, len(h.metaCache))
	for name, pm := range h.metaCache {
		projects = append(projects, projectWithName{name: name, meta: pm})
	}
	h.metaMu.RUnlock()

	// Collect every failure: stopping at the first error would leave other
	// projects' dirty metadata unflushed with no diagnostic.
	var errs []error
	for _, p := range projects {
		if err := h.commitProjectMetadata(ctx, p.name, p.meta); err != nil {
			errs = append(errs, fmt.Errorf("flush %s: %w", p.name, err))
		}
	}
	return errors.Join(errs...)
}

// FlushProjectContext commits dirty metadata for one project, creating
// the tracking entry if absent (an unknown project name therefore starts
// residency with an empty tree and reports success without network
// traffic). It is the per-project counterpart of FlushMetadata and the
// remedy after a failed push: healing requires a later operation on that
// project, this call, or Shutdown.
func (h *StorHub) FlushProjectContext(ctx context.Context, project string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	return h.commitProjectMetadata(ctx, project, h.getOrCreateProjectMeta(project))
}

func (h *StorHub) UploadFile(project, fileName, inputPath string) (*FileMeta, error) {
	return h.UploadFileContext(context.Background(), project, fileName, inputPath)
}

func (h *StorHub) UploadFileContext(ctx context.Context, project, fileName, inputPath string) (*FileMeta, error) {
	return h.putFileContext(ctx, project, fileName, inputPath, false)
}

func (h *StorHub) ReplaceFile(project, fileName, inputPath string) (*FileMeta, error) {
	return h.ReplaceFileContext(context.Background(), project, fileName, inputPath)
}

func (h *StorHub) ReplaceFileContext(ctx context.Context, project, fileName, inputPath string, opts ...shfs.MutateOption) (*FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.putFileContext(ctx, project, fileName, inputPath, true)
}

func (h *StorHub) PrepareReplaceContext(ctx context.Context, project, fileName string, requiredSlots int) (releaseTag string, uploadURL string, err error) {
	started := h.logOpStart(project, "prepare-replace", "path", fileName, "required_slots", requiredSlots)
	defer func() {
		h.logOpFinish(project, "prepare-replace", started, err, "path", fileName, "required_slots", requiredSlots, "release", releaseTag)
	}()
	if err := validateProject(project); err != nil {
		return "", "", err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return "", "", err
	}
	if cleanName == "" {
		return "", "", errors.New("file name is required")
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return "", "", err
	}
	if err := shfs.RequireParentDirectory(repoMeta, cleanName); err != nil {
		return "", "", err
	}
	existing := repoMeta.FindFile(cleanName)
	if existing == nil {
		return "", "", fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(cleanName)
	releaseTag, uploadURL, err = h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots, "")
	return releaseTag, uploadURL, err
}

// trimChunks drops chunks at or beyond size and re-sorts/re-indexes them.
func trimChunks(chunks []ChunkInfo, size int64) []ChunkInfo {
	if len(chunks) == 0 {
		return chunks
	}
	filtered := make([]ChunkInfo, 0, len(chunks))
	for _, c := range chunks {
		if c.Offset < size {
			if c.Offset+c.Size > size {
				c.Size = size - c.Offset
			}
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		return filtered
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Offset < filtered[j].Offset
	})
	return filtered
}

func (h *StorHub) FinalizeReplaceChunksContext(ctx context.Context, project, fileName, releaseTag string, size int64, chunks []ChunkInfo) (result *FileMeta, err error) {
	started := h.logOpStart(project, "finalize-replace", "path", fileName, "release", releaseTag, "size", size, "chunks", len(chunks))
	defer func() {
		h.logOpFinish(project, "finalize-replace", started, err, "path", fileName, "release", releaseTag, "size", size, "chunks", len(chunks))
	}()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	current := repoMeta.FindFile(cleanName)
	if current == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// Trim chunks beyond the logical file size.
	// Kernel writeback cache may flush stale dirty pages from the previous
	// file content (before truncation), producing chunks past the new EOF.
	chunks = trimChunks(chunks, size)

	now := h.config.Now().Unix()
	fileMeta := current.Clone()

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. PrepareReplaceContext EnsureReleases only on a local
	// clone that is discarded before this call.
	pm.meta.EnsureRelease(releaseTag, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(chunks))
	for i := range chunks {
		id := pm.meta.AllocateChunkID()
		pm.meta.Chunks[id] = chunks[i]
		chunkIDs[i] = id
	}
	fileMeta.Chunks = chunkIDs
	fileMeta.Size = size
	latest := pm.meta.FindFile(cleanName)
	if latest == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, latest, now)
	implposix.ReplaceInodeFamily(pm.meta, cleanName, latest, fileMeta, now)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	result = &fileMeta
	return result, nil
}

func (h *StorHub) ReplaceFileFromReader(project, filePath string, body io.Reader) (*metadata.FileMeta, error) {
	return h.ReplaceFileFromReaderContext(context.Background(), project, filePath, body)
}

func (h *StorHub) ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (result *metadata.FileMeta, err error) {
	if body == nil {
		return nil, fmt.Errorf("request body is nil")
	}
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}

	size, hasSize := shfs.ApplyMutateOptions(opts).ExpectedSize()
	if !hasSize {
		return nil, fmt.Errorf("upload size unknown: pass fs.WithSize(n) (REST callers: Content-Length)")
	}
	chunkSize := chunking.NormalizedSize(h.ChunkSize())
	requiredSlots := 0
	if size > 0 {
		requiredSlots = int((size + chunkSize - 1) / chunkSize)
	}
	releaseTag, uploadURL, err := h.PrepareReplaceContext(ctx, project, filePath, requiredSlots)
	if err != nil {
		return nil, err
	}

	// Stream the body straight into per-chunk GitHub uploads. Each window is
	// tee-mirrored to a spool file so transport retries rewind from disk
	// instead of re-reading the network; a failed window compensates by
	// deleting earlier windows of this call, keeping metadata-atomicity.
	var chunks []ChunkInfo
	var uploaded int64
	namer := newAssetNamer()
	for uploaded < size {
		windowSize := min64(h.ChunkSize(), size-uploaded)

		win, cleanup, werr := newWindowReader(body, windowSize)
		if werr != nil {
			h.compensateDeleteAssets(ctx, project, chunks)
			return nil, werr
		}

		const maxNameRetries = 5
		var chunk ChunkInfo
		applied := false
		for renameAttempt := 0; renameAttempt < maxNameRetries; renameAttempt++ {
			assetName, nameErr := namer.Next()
			if nameErr != nil {
				cleanup()
				h.compensateDeleteAssets(ctx, project, chunks)
				return nil, nameErr
			}
			if _, seekErr := win.Seek(0, io.SeekStart); seekErr != nil {
				cleanup()
				h.compensateDeleteAssets(ctx, project, chunks)
				return nil, seekErr
			}
			assetID, uploadErr := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, win, windowSize)
			if uploadErr == nil {
				chunk = ChunkInfo{Size: windowSize, Offset: uploaded, Release: releaseTag, AssetID: assetID, AssetOffset: 0}
				applied = true
				break
			}
			if isAlreadyExists(uploadErr) {
				h.debugf("upload chunk asset name collision, retry=%d asset=%s", renameAttempt+1, assetName)
				continue
			}
			if isReleaseFull(uploadErr) {
				h.debugf("upload release full (422 file_count), creating new release, retry=%d asset=%s release=%s", renameAttempt+1, assetName, releaseTag)
				h.invalidateReleaseCache(project)
				remainingSlots := 1
				if size > uploaded {
					remainingSlots = int((size - uploaded + chunkSize - 1) / chunkSize)
				}
				newTag, newURL, err := h.PrepareReplaceContext(ctx, project, filePath, remainingSlots)
				if err != nil {
					cleanup()
					h.compensateDeleteAssets(ctx, project, chunks)
					return nil, err
				}
				releaseTag = newTag
				uploadURL = newURL
				continue
			}
			cleanup()
			h.compensateDeleteAssets(ctx, project, chunks)
			return nil, uploadErr
		}
		cleanup()
		if !applied {
			h.compensateDeleteAssets(ctx, project, chunks)
			return nil, fmt.Errorf("upload chunk failed after %d name retries", maxNameRetries)
		}
		chunks = append(chunks, chunk)
		uploaded += windowSize
	}

	return h.FinalizeReplaceChunksContext(ctx, project, filePath, releaseTag, uploaded, chunks)
}

func (h *StorHub) FillChunkRangeContext(ctx context.Context, project string, chunk metadata.ChunkInfo, dst []byte) error {
	return h.fillAssetRange(ctx, project, chunk, dst)
}

func (h *StorHub) PatchFile(project, fileName string, offset, deleteSize int64, edit []byte) (*FileMeta, error) {
	return h.PatchFileContext(context.Background(), project, fileName, offset, deleteSize, edit)
}

func (h *StorHub) PatchFileContext(ctx context.Context, project, fileName string, offset, deleteSize int64, edit []byte, opts ...shfs.MutateOption) (result *FileMeta, err error) {
	started := h.logOpStart(project, "patch-file", "path", fileName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	defer func() {
		h.logOpFinish(project, "patch-file", started, err, "path", fileName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	}()
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if offset < 0 {
		return nil, errors.New("patch offset must be non-negative")
	}
	if deleteSize < 0 {
		return nil, errors.New("patch delete size must be non-negative")
	}
	if deleteSize == 0 && len(edit) == 0 {
		return nil, errors.New("patch edit or delete size is required")
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	if fileMeta.Symlink != "" {
		return nil, fmt.Errorf("cannot patch symlink: %s", cleanName)
	}
	patchEnd := offset + deleteSize
	if offset > fileMeta.Size || patchEnd > fileMeta.Size {
		return nil, fmt.Errorf("patch range [%d,%d) exceeds file size %d", offset, patchEnd, fileMeta.Size)
	}

	result, err = h.patchFileWithMetadataContext(ctx, project, cleanName, repoMeta, fileMeta, offset, deleteSize, edit)
	return result, err
}

// PatchFileRangesWithMetadataContext applies a batch of ascending,
// disjoint edits as ONE operation: one release resolution, one asset per
// chunk of edited bytes, one playlist rebuild, one metadata mutation.
// Compared to looping PatchFileContext per range it removes the per-range
// release listing round-trip and the N-1 intermediate playlist states -
// on a slow link that turns N+2 latency chains into one. The caller owns
// the pre-network validation (range bounds, sort order); this layer
// re-validates defensively and re-checks concurrent size changes once
// before committing.
// PatchFileRangesContext applies a batch of ascending, disjoint edits as
// ONE operation: one release resolution, one asset per chunk of edited
// bytes, one playlist rebuild, one metadata mutation. Compared to looping
// PatchFileContext per range it removes the per-range release listing
// round-trip and the N-1 intermediate playlist states - on a slow link
// that turns N+2 latency chains into one. Either the whole batch commits
// or none of it does.
func (h *StorHub) PatchFileRangesContext(ctx context.Context, project, fileName string, edits []shfs.RangeEdit) (result *FileMeta, err error) {
	started := h.logOpStart(project, "patch-file-ranges", "path", fileName, "edits", len(edits))
	defer func() {
		h.logOpFinish(project, "patch-file-ranges", started, err, "path", fileName, "edits", len(edits))
	}()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if len(edits) == 0 {
		return nil, errors.New("patch batch is empty")
	}
	for i, edit := range edits {
		if edit.Start < 0 || edit.DeleteSize < 0 {
			return nil, fmt.Errorf("patch edit %d has negative range", i)
		}
		if edit.DeleteSize == 0 && edit.Len() == 0 {
			return nil, fmt.Errorf("patch edit %d is empty", i)
		}
		if i > 0 && edit.Start < edits[i-1].End() {
			return nil, fmt.Errorf("patch edit %d overlaps its predecessor", i)
		}
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	if fileMeta.Symlink != "" {
		return nil, fmt.Errorf("cannot patch symlink: %s", cleanName)
	}
	for i, edit := range edits {
		if edit.End() > fileMeta.Size {
			return nil, fmt.Errorf("patch edit %d range [%d,%d) exceeds file size %d", i, edit.Start, edit.End(), fileMeta.Size)
		}
	}

	newChunks, releaseTag, err := h.buildPatchedRangeChunks(ctx, project, repoMeta, *fileMeta, cleanName, edits)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().Unix()
	patched := fileMeta.Clone()

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	pm.meta.EnsureRelease(releaseTag, now)
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := pm.meta.AllocateChunkID()
		pm.meta.Chunks[id] = newChunks[i]
		chunkIDs[i] = id
	}
	patched.Chunks = chunkIDs
	totalDelete, totalInsert := int64(0), int64(0)
	for _, edit := range edits {
		totalDelete += edit.DeleteSize
		totalInsert += edit.Len()
	}
	patched.Size = fileMeta.Size - totalDelete + totalInsert
	patched.Mode = shfs.SanitizeWrittenFileMode(patched.Mode)
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := pm.meta.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// Same concurrency guard as the single-edit path: the snapshot may be
	// stale by the time uploads finish; one size check covers the batch.
	if current.Size != fileMeta.Size || edits[len(edits)-1].End() > current.Size {
		pm.mu.Unlock()
		return nil, fmt.Errorf("file %s changed concurrently (size %d, expected %d); patch batch rejected", cleanName, current.Size, fileMeta.Size)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &patched, current, now)
	implposix.ReplaceInodeFamily(pm.meta, cleanName, current, patched, now)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	return &patched, nil
}

func (h *StorHub) patchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *RepoMetadata, fileMeta *FileMeta, offset, deleteSize int64, edit []byte) (*FileMeta, error) {
	newChunks, releaseTag, err := h.buildPatchedChunks(ctx, project, repoMeta, *fileMeta, cleanName, offset, deleteSize, edit)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().Unix()
	patched := fileMeta.Clone()

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. buildPatchedChunks EnsureReleases only on a local
	// clone that is discarded here.
	pm.meta.EnsureRelease(releaseTag, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := pm.meta.AllocateChunkID()
		pm.meta.Chunks[id] = newChunks[i]
		chunkIDs[i] = id
	}
	patched.Chunks = chunkIDs
	patched.Size = fileMeta.Size - deleteSize + int64(len(edit))
	patched.Mode = shfs.SanitizeWrittenFileMode(patched.Mode)
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := pm.meta.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// The pre-network validation ran against a snapshot; the file may have
	// changed since. Re-check the edited range against current state before
	// committing, so a concurrent truncate/replace cannot be clobbered.
	if current.Size != fileMeta.Size || offset+deleteSize > current.Size {
		pm.mu.Unlock()
		return nil, fmt.Errorf("file %s changed concurrently (size %d, expected %d); patch rejected", cleanName, current.Size, fileMeta.Size)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &patched, current, now)
	implposix.ReplaceInodeFamily(pm.meta, cleanName, current, patched, now)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	return &patched, nil
}

func (h *StorHub) rewriteFileRangesWithMetadataContext(ctx context.Context, project, cleanName, snapshotPath string, repoMeta *RepoMetadata, fileMeta *FileMeta, finalSize int64, dirtyRanges []byteRange) (*FileMeta, error) {
	newChunks, releaseTag, err := h.buildRewrittenChunks(ctx, project, repoMeta, *fileMeta, cleanName, snapshotPath, finalSize, dirtyRanges)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().Unix()
	rewritten := fileMeta.Clone()

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. buildRewrittenChunks EnsureReleases only on a local
	// clone that is discarded here.
	pm.meta.EnsureRelease(releaseTag, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := pm.meta.AllocateChunkID()
		pm.meta.Chunks[id] = newChunks[i]
		chunkIDs[i] = id
	}
	rewritten.Chunks = chunkIDs
	rewritten.Size = finalSize
	rewritten.Mode = shfs.SanitizeWrittenFileMode(rewritten.Mode)
	rewritten.ModifiedAt = now
	rewritten.ChangedAt = now
	rewritten.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := pm.meta.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &rewritten, current, now)
	implposix.ReplaceInodeFamily(pm.meta, cleanName, current, rewritten, now)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	return &rewritten, nil
}

func (h *StorHub) putFileContext(ctx context.Context, project, fileName, inputPath string, replace bool) (result *FileMeta, err error) {
	op := "upload-file"
	if replace {
		op = "replace-file"
	}
	started := h.logOpStart(project, op, "path", fileName, "input", inputPath)
	defer func() { h.logOpFinish(project, op, started, err, "path", fileName, "input", inputPath) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}

	fileInfo, err := os.Stat(inputPath)
	if err != nil {
		return nil, fmt.Errorf("stat input file: %w", err)
	}
	if fileInfo.IsDir() {
		return nil, shfs.IsDirectory(inputPath)
	}

	if err := h.ensureRepo(ctx, project); err != nil {
		return nil, err
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	if err := shfs.RequireParentDirectory(repoMeta, cleanName); err != nil {
		return nil, err
	}
	if err := shfs.CheckParentWrite(ctx, repoMeta, cleanName); err != nil {
		return nil, err
	}
	existing := repoMeta.FindFile(cleanName)
	var preferredRelease string
	if !replace && existing != nil {
		return nil, shfs.AlreadyExists(cleanName)
	}

	planner, err := chunking.NewStreamingChunker(inputPath, cleanName, h.config.ChunkSize)
	if err != nil {
		return nil, err
	}
	defer func() { _ = planner.Close() }()

	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(cleanName)
	requiredSlots := planner.NumChunks()
	if fileInfo.Size() == 0 {
		requiredSlots = 0
	}
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots, preferredRelease)
	if err != nil {
		return nil, err
	}

	results := []ChunkInfo{}
	if fileInfo.Size() > 0 {
		results, err = h.uploadChunks(ctx, project, releaseTag, uploadURL, planner)
		if err != nil {
			return nil, err
		}
	}
	fileMeta := FileMeta{
		Size:   fileInfo.Size(),
		Chunks: nil,
	}
	implposix.ApplyUploadIdentity(repoMeta, cleanName, existing, &fileMeta, h.config.Now().Unix())
	if existing == nil {
		defaultUID, defaultGID := h.DefaultOwnerIDs()
		fileMeta.UID, fileMeta.GID = shfs.OwnerIDsForCreate(ctx, defaultUID, defaultGID)
	}
	if existing != nil {
		fileMeta.Mode = shfs.SanitizeWrittenFileMode(fileMeta.Mode)
	}
	fileMeta.Mode, fileMeta.UID, fileMeta.GID = shfs.ApplyParentInheritance(repoMeta, cleanName, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	if err := shfs.CheckParentWrite(ctx, pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		return nil, err
	}
	if err := shfs.RequireParentDirectory(pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		return nil, err
	}
	if !replace && pm.meta.FindFile(cleanName) != nil {
		pm.mu.Unlock()
		return nil, shfs.AlreadyExists(cleanName)
	}
	pm.meta.EnsureRelease(releaseTag, h.config.Now().Unix())
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(results))
	for i := range results {
		id := pm.meta.AllocateChunkID()
		pm.meta.Chunks[id] = results[i]
		chunkIDs[i] = id
	}
	fileMeta.Chunks = chunkIDs
	current := pm.meta.FindFile(cleanName)
	if current != nil {
		implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, current, h.config.Now().Unix())
		implposix.ReplaceInodeFamily(pm.meta, cleanName, current, fileMeta, h.config.Now().Unix())
	} else {
		fileMeta.Mode, fileMeta.UID, fileMeta.GID = shfs.ApplyParentInheritance(pm.meta, cleanName, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
		metadata.InitializeNewFileIdentity(pm.meta, &fileMeta, h.config.Now().Unix())
		pm.meta.UpsertFile(cleanName, fileMeta, h.config.Now().Unix())
	}
	shfs.TouchParentDirectory(pm.meta, cleanName, h.config.Now().Unix())
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	result = &fileMeta
	return result, nil
}

func (h *StorHub) DownloadFile(project, fileName, outputPath string) error {
	return h.DownloadFileContext(context.Background(), project, fileName, outputPath)
}

func (h *StorHub) DownloadFileContext(ctx context.Context, project, fileName, outputPath string) (err error) {
	started := h.logOpStart(project, "download-file", "path", fileName, "output", outputPath)
	defer func() { h.logOpFinish(project, "download-file", started, err, "path", fileName, "output", outputPath) }()
	if err := validateProject(project); err != nil {
		return err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return err
	}
	if cleanName == "" {
		return errors.New("file name is required")
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer func() {
		cerr := outFile.Close()
		if err == nil && cerr != nil {
			err = fmt.Errorf("close output file: %w", cerr)
		}
		if err != nil {
			_ = os.Remove(outputPath)
		}
	}()

	if err := outFile.Truncate(fileMeta.Size); err != nil {
		return fmt.Errorf("preallocate output file: %w", err)
	}

	for _, chunkName := range fileMeta.Chunks {
		chunk, ok := repoMeta.Chunks[chunkName]
		if !ok {
			return fmt.Errorf("chunk %d not found", chunkName)
		}
		if err := h.downloadChunkWithRetry(ctx, project, outFile, chunk); err != nil {
			return err
		}
	}

	if err := outFile.Sync(); err != nil {
		return fmt.Errorf("sync output file: %w", err)
	}
	// The deferred closer owns Close so it fires exactly once, whether the
	// success path completes here or an error unwinds through the defers.
	return nil
}

func (h *StorHub) ListFiles(project string) ([]FileMeta, error) {
	return h.ListFilesContext(context.Background(), project)
}

func (h *StorHub) ListFilesContext(ctx context.Context, project string) (result []FileMeta, err error) {
	started := h.logOpStart(project, "list-files")
	defer func() { h.logOpFinish(project, "list-files", started, err, "count", len(result)) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	files := repoMeta.AllFiles()
	result = files
	return result, nil
}

func (h *StorHub) ListReleases(project string) ([]metadata.ReleaseRef, error) {
	return h.ListReleasesContext(context.Background(), project)
}

func (h *StorHub) ListReleasesContext(ctx context.Context, project string) (result []metadata.ReleaseRef, err error) {
	started := h.logOpStart(project, "list-releases")
	defer func() { h.logOpFinish(project, "list-releases", started, err, "count", len(result)) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	result = make([]metadata.ReleaseRef, 0, len(repoMeta.Releases))
	for _, ref := range repoMeta.Releases {
		result = append(result, ref.Clone())
	}
	return result, nil
}

func (h *StorHub) ListMetadataRevisions(project string) ([]MetadataRevision, error) {
	return h.ListMetadataRevisionsContext(context.Background(), project)
}

func (h *StorHub) ListMetadataRevisionsContext(ctx context.Context, project string) (result []MetadataRevision, err error) {
	started := h.logOpStart(project, "list-metadata-revisions")
	defer func() { h.logOpFinish(project, "list-metadata-revisions", started, err, "count", len(result)) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	result, err = h.listMetadataRevisions(ctx, project)
	return result, err
}

func (h *StorHub) RollbackMetadata(project, commitSHA string) error {
	return h.RollbackMetadataContext(context.Background(), project, commitSHA)
}

func (h *StorHub) RollbackMetadataContext(ctx context.Context, project, commitSHA string) (err error) {
	started := h.logOpStart(project, "rollback-metadata", "commit_sha", commitSHA)
	defer func() { h.logOpFinish(project, "rollback-metadata", started, err, "commit_sha", commitSHA) }()
	if err := validateProject(project); err != nil {
		return err
	}
	if strings.TrimSpace(commitSHA) == "" {
		return errors.New("commit sha is required")
	}
	// Flush any dirty metadata first so cached SHA matches GitHub
	pm := h.getOrCreateProjectMeta(project)
	if err := h.commitProjectMetadata(ctx, project, pm); err != nil {
		return err
	}

	currentMeta, currentSHA, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return err
	}
	if err := currentMeta.Validate(); err != nil {
		return err
	}
	rollbackMeta, err := h.getMetadataRevision(ctx, project, commitSHA)
	if err != nil {
		return err
	}
	if err := h.validateMetadataSnapshot(ctx, project, rollbackMeta); err != nil {
		return err
	}
	_, _, err = h.commitRepoMetadata(ctx, project, *rollbackMeta, currentSHA, fmt.Sprintf("storhub: rollback metadata to %s", shortSHA(commitSHA)))
	return err
}

func (h *StorHub) getBuffer() *[]byte { return h.bufferPool.Get().(*[]byte) }

func (h *StorHub) putBuffer(buf *[]byte) { h.bufferPool.Put(buf) }

func (h *StorHub) downloadChunkWithRetry(ctx context.Context, project string, outFile *os.File, chunk ChunkInfo) error {
	if chunk.Size == 0 {
		return nil
	}
	buf := h.getBuffer()
	defer h.putBuffer(buf)

	for attempt := 0; attempt <= h.config.MaxRetries; attempt++ {
		reader, _, err := h.downloadAssetStream(ctx, project, chunk.AssetID, chunk.AssetOffset, chunk.AssetOffset+chunk.Size-1)
		if err != nil {
			if !isRetryableDownloadError(err) || attempt == h.config.MaxRetries {
				return fmt.Errorf("download chunk %d: %w", chunk.AssetID, err)
			}
			if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, extractAPIError(err))); sleepErr != nil {
				return sleepErr
			}
			continue
		}

		written, copyErr := h.writeChunk(outFile, reader, *buf, chunk)
		closeErr := reader.Close()
		if copyErr == nil && closeErr != nil {
			copyErr = closeErr
		}
		if copyErr == nil && written != chunk.Size {
			copyErr = fmt.Errorf("chunk %d size mismatch: expected %d, got %d", chunk.AssetID, chunk.Size, written)
		}
		if copyErr == nil {
			return nil
		}
		if !isRetryableDownloadError(copyErr) || attempt == h.config.MaxRetries {
			return fmt.Errorf("download chunk %d: %w", chunk.AssetID, copyErr)
		}
		if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, extractAPIError(copyErr))); sleepErr != nil {
			return sleepErr
		}
	}
	return fmt.Errorf("download chunk %d: exhausted retries", chunk.AssetID)
}

func (h *StorHub) writeChunk(outFile *os.File, reader io.Reader, buf []byte, chunk ChunkInfo) (int64, error) {
	written := int64(0)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if _, writeErr := outFile.WriteAt(buf[:n], chunk.Offset+written); writeErr != nil {
				return written, fmt.Errorf("write chunk %d: %w", chunk.AssetID, writeErr)
			}
			written += int64(n)
		}
		if readErr == io.EOF {
			return written, nil
		}
		if readErr != nil {
			return written, fmt.Errorf("read chunk %d: %w", chunk.AssetID, readErr)
		}
	}
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

func (h *StorHub) LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*metadata.RepoMetadata, string, error) {
	return h.loadRepoMetadataReadonly(ctx, project)
}

// UpdateRepoMetadataContext updates metadata using the new batching system
// The mutation is applied to in-memory metadata and marked dirty for event-driven commit
func (h *StorHub) UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*metadata.RepoMetadata) error, message string) (*metadata.RepoMetadata, error) {
	lockStarted := h.config.Now().UTC()
	pm := h.getOrCreateProjectMeta(project)

	logging.Debug(h.projectLogger(project), "metadata writer wait", "message", message)
	pm.mu.Lock()

	logging.Debug(h.projectLogger(project), "metadata writer acquired", "message", message, "wait", h.config.Now().UTC().Sub(lockStarted))

	started := h.config.Now().UTC()
	h.debugf("metadata update start project=%s message=%q", project, message)

	// Hydration guard: a freshly created projectMetadata starts EMPTY. If
	// the project exists remotely, applying a mutation to that empty tree
	// and committing would replace the entire remote state (files, dirs,
	// chunk catalog) with just this one change. Load remote truth first;
	// only a confirmed-new project may proceed on an empty tree.
	if !pm.hydrated {
		pm.mu.Unlock()
		loaded, loadedSHA, loadErr := h.loadRepoMetadataFresh(ctx, project)
		pm.mu.Lock()
		switch {
		case loadErr == nil:
			if !pm.hydrated && !pm.dirty {
				pm.meta = loaded
				pm.sha = loadedSHA
			}
			pm.hydrated = true
		case errors.Is(loadErr, shfs.ErrNotFound):
			// Confirmed-new project: empty tree is the truth.
			pm.hydrated = true
		default:
			pm.mu.Unlock()
			return nil, fmt.Errorf("hydrate metadata before mutation: %w", loadErr)
		}
	}

	// Apply mutation to in-memory metadata
	if err := fn(pm.meta); err != nil {
		pm.mu.Unlock()
		h.debugf("metadata update failed project=%s step=apply elapsed=%s err=%v", project, h.config.Now().UTC().Sub(started), err)
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, err
	}

	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	// Trigger the commit loop to wake up immediately
	select {
	case trigger <- struct{}{}:
	default:
	}

	h.debugf("metadata update complete project=%s elapsed=%s", project, h.config.Now().UTC().Sub(started))
	logging.Debug(h.projectLogger(project), "metadata update complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started))

	return pm.meta, nil
}

func (h *StorHub) RewriteFileRangesWithMetadataContext(ctx context.Context, project, cleanName, snapshotPath string, repoMeta *metadata.RepoMetadata, fileMeta *metadata.FileMeta, finalSize int64, dirtyRanges []fusefs.ByteRange) (*metadata.FileMeta, error) {
	ranges := make([]byteRange, len(dirtyRanges))
	for i, dirty := range dirtyRanges {
		ranges[i] = byteRange{start: dirty.Start, end: dirty.End}
	}
	return h.rewriteFileRangesWithMetadataContext(ctx, project, cleanName, snapshotPath, repoMeta, fileMeta, finalSize, ranges)
}

func (h *StorHub) ValidateProjectName(project string) error {
	return validateProject(project)
}

func (h *StorHub) EnsureRepoContext(ctx context.Context, project string) error {
	return h.ensureRepo(ctx, project)
}

func (h *StorHub) LoadRepoMetadataContext(ctx context.Context, project string) (*metadata.RepoMetadata, string, error) {
	return h.loadRepoMetadata(ctx, project)
}

func (h *StorHub) GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *metadata.RepoMetadata, requiredSize int, preferredTag string) (string, string, error) {
	return h.getOrCreateUploadRelease(ctx, project, repoMeta, requiredSize, preferredTag)
}

func (h *StorHub) PatchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *metadata.RepoMetadata, fileMeta *metadata.FileMeta, offset, deleteSize int64, edit []byte) (*metadata.FileMeta, error) {
	return h.patchFileWithMetadataContext(ctx, project, cleanName, repoMeta, fileMeta, offset, deleteSize, edit)
}

func (h *StorHub) FillAssetRangeContext(ctx context.Context, project string, segment metadata.ChunkInfo, dst []byte) error {
	return h.fillAssetRange(ctx, project, segment, dst)
}

func (h *StorHub) FileNotFound(path string) error {
	return shfs.NotFound(path)
}

func (h *StorHub) DefaultFileMode(kind metadata.NodeKind) uint32 {
	return defaultFileMode(kind)
}

func (h *StorHub) DefaultOwnerIDs() (uint32, uint32) {
	return defaultOwnerIDs()
}

func (h *StorHub) AtimePolicy() storcfg.AtimePolicy {
	return h.config.AtimePolicy
}

func (h *StorHub) fsService() *shfs.Service {
	return shfs.NewService(h)
}

func (h *StorHub) posixService() *implposix.Service {
	return implposix.NewService(h)
}

func (h *StorHub) CreateFile(project, filePath string) (*metadata.FileMeta, error) {
	return h.CreateFileContext(context.Background(), project, filePath)
}

func (h *StorHub) CreateFileContext(ctx context.Context, project, filePath string) (*metadata.FileMeta, error) {
	return h.fsService().CreateFileContext(ctx, project, filePath)
}

func (h *StorHub) Mkdir(project, dirPath string) error {
	return h.MkdirContext(context.Background(), project, dirPath)
}

func (h *StorHub) MkdirContext(ctx context.Context, project, dirPath string) error {
	return h.fsService().MkdirContext(ctx, project, dirPath)
}

func (h *StorHub) Unlink(project, filePath string) error {
	return h.DeleteFile(project, filePath)
}

func (h *StorHub) UnlinkContext(ctx context.Context, project, filePath string) error {
	return h.DeleteFileContext(ctx, project, filePath)
}

func (h *StorHub) Rmdir(project, dirPath string) error {
	return h.RmdirContext(context.Background(), project, dirPath)
}

func (h *StorHub) RmdirContext(ctx context.Context, project, dirPath string, opts ...shfs.MutateOption) error {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return err
	}
	return h.fsService().RmdirContext(ctx, project, dirPath)
}

func (h *StorHub) Rename(project, oldPath, newPath string) error {
	return h.RenameContext(context.Background(), project, oldPath, newPath)
}

func (h *StorHub) RenameContext(ctx context.Context, project, oldPath, newPath string) error {
	return h.fsService().RenameContext(ctx, project, oldPath, newPath)
}

func (h *StorHub) TruncateFile(project, filePath string, size int64) (*metadata.FileMeta, error) {
	return h.TruncateFileContext(context.Background(), project, filePath, size)
}

func (h *StorHub) TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.fsService().TruncateFileContext(ctx, project, filePath, size)
}

func (h *StorHub) AppendFile(project, filePath string, data []byte) (*metadata.FileMeta, error) {
	return h.AppendFileContext(context.Background(), project, filePath, data)
}

func (h *StorHub) AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.fsService().AppendFileContext(ctx, project, filePath, data)
}

func (h *StorHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*metadata.FileMeta, error) {
	return h.WriteFileAtContext(context.Background(), project, filePath, offset, data)
}

func (h *StorHub) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.fsService().WriteFileAtContext(ctx, project, filePath, offset, data)
}

func (h *StorHub) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	return h.ReadFileAtContext(context.Background(), project, filePath, offset, length)
}

func (h *StorHub) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	result := make([]byte, length)
	n, err := h.ReadFileAtBufferContext(ctx, project, filePath, offset, result)
	if err != nil {
		return nil, err
	}
	return result[:n], nil
}

func (h *StorHub) ReadFileAtBufferContext(ctx context.Context, project, filePath string, offset int64, result []byte) (int, error) {
	if err := validateProject(project); err != nil {
		return 0, err
	}
	cleanPath, err := shfs.NormalizePath(filePath)
	if err != nil {
		return 0, err
	}
	if cleanPath == "" {
		return 0, errors.New("file name is required")
	}
	if offset < 0 {
		return 0, errors.New("read offset and length must be non-negative")
	}
	repo, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return 0, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return 0, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanPath)
	}
	if err := shfs.CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return 0, err
	}
	if file.Symlink != "" {
		return 0, shfs.InvalidSymlink(cleanPath)
	}
	if offset > file.Size {
		return 0, io.EOF
	}
	if len(result) == 0 {
		return 0, nil
	}
	end := offset + int64(len(result))
	if end > file.Size {
		end = file.Size
	}
	segments := overlappingFileSegments(file, repo.Chunks, offset, end)
	for _, segment := range segments {
		if err := h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end]); err != nil {
			return 0, err
		}
	}
	shfs.TouchFileAccessTime(ctx, h, project, cleanPath, h.config.Now().Unix())
	return int(end - offset), nil
}

// ReadPinnedFileContext reads bytes for a caller-held metadata snapshot -
// a file entry plus the chunk descriptors it referenced when captured,
// typically at FUSE open time. Later renames or unlinks of the source
// path cannot change what this returns: resolution never touches live
// metadata, and chunk assets are content-addressed. Access was already
// authorized when the snapshot was taken, so no permission re-check
// runs here, and atime is left untouched because a historical read must
// not refresh the live entry.
func (h *StorHub) ReadPinnedFileContext(ctx context.Context, project string, file *metadata.FileMeta, chunks map[int64]metadata.ChunkInfo, offset, length int64) ([]byte, error) {
	if file == nil {
		return nil, shfs.NotFound("pinned file")
	}
	if length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	if length == 0 || offset >= file.Size {
		return []byte{}, nil
	}
	result := make([]byte, length)
	end := offset + length
	if end > file.Size {
		end = file.Size
	}
	for _, segment := range overlappingFileSegments(file, chunks, offset, end) {
		if err := h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end]); err != nil {
			return nil, err
		}
	}
	return result[:end-offset], nil
}

type fileReadSegment struct {
	chunk metadata.ChunkInfo
	start int
	end   int
}

func overlappingFileSegments(file *metadata.FileMeta, repoChunks map[int64]metadata.ChunkInfo, offset, end int64) []fileReadSegment {
	if file == nil || end <= offset {
		return nil
	}
	segments := make([]fileReadSegment, 0, len(file.Chunks))
	chunks := make([]metadata.ChunkInfo, 0, len(file.Chunks))
	for _, id := range file.Chunks {
		if chunk, ok := repoChunks[id]; ok {
			chunks = append(chunks, chunk)
		}
	}
	startIndex := sort.Search(len(chunks), func(i int) bool {
		return chunks[i].Offset+chunks[i].Size > offset
	})
	for _, chunk := range chunks[startIndex:] {
		chunkEnd := chunk.Offset + chunk.Size
		if chunk.Offset >= end {
			break
		}
		if chunkEnd <= offset || chunk.Size == 0 {
			continue
		}
		segmentStart := max(offset, chunk.Offset)
		segmentEnd := min(end, chunkEnd)
		segment := chunk
		segment.Offset = segmentStart
		segment.AssetOffset = chunk.AssetOffset + (segmentStart - chunk.Offset)
		segment.Size = segmentEnd - segmentStart
		segments = append(segments, fileReadSegment{
			chunk: segment,
			start: int(segmentStart - offset),
			end:   int(segmentEnd - offset),
		})
	}
	return segments
}

func (h *StorHub) StatPath(project, targetPath string) (*shfs.EntryInfo, error) {
	return h.StatPathContext(context.Background(), project, targetPath)
}

func (h *StorHub) StatPathContext(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	return h.fsService().StatPathContext(ctx, project, targetPath)
}

func (h *StorHub) ReadDir(project, dirPath string) ([]shfs.DirEntry, error) {
	return h.ReadDirContext(context.Background(), project, dirPath)
}

func (h *StorHub) ReadDirContext(ctx context.Context, project, dirPath string) ([]shfs.DirEntry, error) {
	return h.fsService().ReadDirContext(ctx, project, dirPath)
}

func (h *StorHub) StatFS(project string) (*shfs.FSStats, error) {
	return h.StatFSContext(context.Background(), project)
}

func (h *StorHub) StatFSContext(ctx context.Context, project string) (*shfs.FSStats, error) {
	return h.fsService().StatFSContext(ctx, project)
}

func (h *StorHub) Symlink(project, target, linkPath string) (*metadata.FileMeta, error) {
	return h.SymlinkContext(context.Background(), project, target, linkPath)
}

func (h *StorHub) SymlinkContext(ctx context.Context, project, target, linkPath string) (*metadata.FileMeta, error) {
	return h.posixService().SymlinkContext(ctx, project, target, linkPath)
}

func (h *StorHub) Readlink(project, linkPath string) (string, error) {
	return h.ReadlinkContext(context.Background(), project, linkPath)
}

func (h *StorHub) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	return h.posixService().ReadlinkContext(ctx, project, linkPath)
}

func (h *StorHub) Link(project, existingPath, newPath string) (*metadata.FileMeta, error) {
	return h.LinkContext(context.Background(), project, existingPath, newPath)
}

func (h *StorHub) LinkContext(ctx context.Context, project, existingPath, newPath string) (*metadata.FileMeta, error) {
	return h.posixService().LinkContext(ctx, project, existingPath, newPath)
}

func (h *StorHub) Chmod(project, targetPath string, mode uint32) error {
	return h.ChmodContext(context.Background(), project, targetPath, mode)
}

func (h *StorHub) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error {
	return h.posixService().ChmodContext(ctx, project, targetPath, mode)
}

func (h *StorHub) Chown(project, targetPath string, uid, gid uint32) error {
	return h.ChownContext(context.Background(), project, targetPath, uid, gid)
}

func (h *StorHub) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error {
	return h.posixService().ChownContext(ctx, project, targetPath, uid, gid)
}

func (h *StorHub) Chtimes(project, targetPath string, atime, mtime int64) error {
	return h.ChtimesContext(context.Background(), project, targetPath, atime, mtime)
}

func (h *StorHub) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error {
	return h.posixService().ChtimesContext(ctx, project, targetPath, atime, mtime)
}

// ChtimesExplicitContext forwards utimensat-style trinary semantics:
// nil omits a timestamp, non-nil sets it exactly (epoch included).
func (h *StorHub) ChtimesExplicitContext(ctx context.Context, project, targetPath string, atime, mtime *time.Time) error {
	return h.posixService().ChtimesExplicitContext(ctx, project, targetPath, atime, mtime)
}

func (h *StorHub) SetXAttr(project, targetPath, attr string, data []byte) error {
	return h.SetXAttrContext(context.Background(), project, targetPath, attr, data)
}

func (h *StorHub) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte) error {
	return h.posixService().SetXAttrContext(ctx, project, targetPath, attr, data)
}

func (h *StorHub) GetXAttr(project, targetPath, attr string) ([]byte, error) {
	return h.GetXAttrContext(context.Background(), project, targetPath, attr)
}

func (h *StorHub) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
	return h.posixService().GetXAttrContext(ctx, project, targetPath, attr)
}

func (h *StorHub) ListXAttr(project, targetPath string) ([]string, error) {
	return h.ListXAttrContext(context.Background(), project, targetPath)
}

func (h *StorHub) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	return h.posixService().ListXAttrContext(ctx, project, targetPath)
}

func (h *StorHub) RemoveXAttr(project, targetPath, attr string) error {
	return h.RemoveXAttrContext(context.Background(), project, targetPath, attr)
}

func (h *StorHub) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error {
	return h.posixService().RemoveXAttrContext(ctx, project, targetPath, attr)
}

func (h *StorHub) ApplyMetadataPatchContext(ctx context.Context, project, targetPath string, patch shfs.MetadataPatch) error {
	return h.posixService().ApplyMetadataPatchContext(ctx, project, targetPath, patch)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// compensateDeleteAssets best-effort removes windows uploaded by this call
// after a later failure; metadata was never committed, so these are pure
// orphans. Individual failures are logged, not fatal - the original error
// is what matters.
func (h *StorHub) compensateDeleteAssets(ctx context.Context, project string, chunks []ChunkInfo) {
	for _, c := range chunks {
		if err := h.deleteAssetByID(ctx, project, c.AssetID); err != nil {
			h.debugf("compensating delete failed project=%s asset=%d err=%v", project, c.AssetID, err)
		}
	}
}
