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

// projectMetadata holds metadata for a single project with batched commit support.
//
// Sharing discipline (load-bearing, enforced by cowTree/publishTreeLocked):
// the tree reachable through pm.meta is IMMUTABLE once published. Readers
// (cachedRepoMetadata*) hand out the live pointer under pm.mu.RLock without
// cloning, so every mutation must go through copy-on-write: take a private
// copy with cowTree, mutate it, publish it back with publishTreeLocked (or a
// wholesale swap under pm.mu.Lock). Mutating a published tree in place is a
// data race against lock-free readers, not a style violation. Entry values
// (FileMeta/DirMeta) are likewise immutable: replace map entries, never edit
// a stored entry's Chunks/XAttrs in place.
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
	// sizeCapped is set when a commit observes the serialized metadata
	// above maxMetadataBytes. Growth mutations are then rejected fast
	// at admission instead of accepted into a tree that can never commit;
	// shrink paths (delete, UpdateRepoMetadataContext folding under the
	// ceiling, purge) stay open, and the next fitting commit clears it.
	sizeCapped bool
	// opStack holds the pending, self-contained metadata operations this
	// project has accumulated since the last successful commit. It drives
	// the rich commit message, the crash-recovery journal, and
	// rebase-on-conflict. Guarded by mu like every other mutable field.
	opStack opStack
	// baseTree points at the immutable tree of the last committed (or
	// freshly loaded) state the pending ops were built on. The rebase
	// fingerprints it on demand (hashPaths) and diffs upstream against it
	// to detect real conflicts. Holding the frozen committed tree costs a
	// pointer instead of a per-path SHA map (~100 B/path resident), and
	// the fingerprint is only paid on the rare conflict path. Guarded by mu.
	baseTree *metadata.RepoMetadata
	// objectCount is the running total of index objects written for this
	// project (from the manifest), used for the history-accumulation
	// threshold warning. Guarded by mu.
	objectCount uint64
	// historyWarned records that the object-count threshold warning has
	// fired for the current crossing, so it logs once per window rather
	// than on every commit. Guarded by mu.
	historyWarned bool
}

// releaseCacheTTL bounds how stale a cached release list may be before the
// upload picker refetches. CAS/rebase safety makes brief staleness harmless;
// the cache exists to dodge per-upload ListReleases secondary rate limits.
const releaseCacheTTL = 60 * time.Second

type releaseCacheEntry struct {
	releases []ghapi.Release
	// fetchedAt anchors the TTL; the sweeper and readers treat an entry
	// older than releaseCacheTTL as a miss. Guarded by releaseMu.
	fetchedAt time.Time
}

// releaseCacheCap bounds the release-cache map so create/delete churn in a
// long-lived server cannot grow it past the tracked-project set.
func (h *StorHub) releaseCacheCap() int {
	if h.config.MaxTrackedProjects <= 0 {
		return 128
	}
	return 2 * h.config.MaxTrackedProjects
}

// getCachedReleases returns a deep copy of the project's cached release list
// for callers that may mutate the result. The hot upload path uses
// cachedReleasesView instead: a shared read-only view costs O(releases)
// aliasing-free reads instead of O(assets) copies per lookup.
func (h *StorHub) getCachedReleases(project string) ([]ghapi.Release, bool) {
	releases, ok := h.cachedReleasesView(project)
	if !ok {
		return nil, false
	}
	return cloneReleases(releases), true
}

// cachedReleasesView returns the cached list under a read lock. The slice is
// the cache's own: callers must treat it and its asset arrays as read-only
// (the picker only reads TagName/UploadURL/ID and asset IDs). A TTL-stale
// entry reports a miss so the caller refetches; the sweeper drops it.
func (h *StorHub) cachedReleasesView(project string) ([]ghapi.Release, bool) {
	h.releaseMu.RLock()
	defer h.releaseMu.RUnlock()
	entry, ok := h.releaseCache[project]
	if !ok {
		return nil, false
	}
	if h.config.Now().Sub(entry.fetchedAt) > releaseCacheTTL {
		return nil, false
	}
	return entry.releases, true
}

func (h *StorHub) setCachedReleases(project string, releases []ghapi.Release) {
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	h.releaseCache[project] = releaseCacheEntry{
		releases:  slimReleases(releases),
		fetchedAt: h.config.Now(),
	}
	h.trimReleaseCacheLocked()
}

// slimReleases copies a release list for cache residency, keeping only the
// fields the upload picker reads (tag, upload URL, release ID, and each
// asset's ID) and dropping the per-asset Name/Size strings. A project with
// 100k cached assets no longer pins 100k name strings; the picker's capacity
// math (len + ID + placeholder detection) is unaffected.
func slimReleases(in []ghapi.Release) []ghapi.Release {
	if in == nil {
		return nil
	}
	out := make([]ghapi.Release, len(in))
	for i, r := range in {
		out[i] = r
		if r.Assets != nil {
			assets := make([]ghapi.Asset, len(r.Assets))
			for j, a := range r.Assets {
				assets[j] = ghapi.Asset{ID: a.ID}
			}
			out[i].Assets = assets
		}
	}
	return out
}

// trimReleaseCacheLocked evicts the stalest entries past the cap. Caller
// holds releaseMu for writing.
func (h *StorHub) trimReleaseCacheLocked() {
	for len(h.releaseCache) > h.releaseCacheCap() {
		victim, victimAt := "", h.config.Now()
		for name, entry := range h.releaseCache {
			if victim == "" || entry.fetchedAt.Before(victimAt) {
				victim, victimAt = name, entry.fetchedAt
			}
		}
		if victim == "" {
			return
		}
		delete(h.releaseCache, victim)
	}
}

// cloneReleases deep-copies releases including their embedded asset slices.
// A shallow struct copy would alias the Assets backing arrays, letting any
// caller that mutates a fetched Release corrupt the cache.
func cloneReleases(in []ghapi.Release) []ghapi.Release {
	if in == nil {
		return nil
	}
	out := make([]ghapi.Release, len(in))
	for i, r := range in {
		out[i] = r
		if r.Assets != nil {
			cp := make([]ghapi.Asset, len(r.Assets))
			copy(cp, r.Assets)
			out[i].Assets = cp
		}
	}
	return out
}

// clearSizeCapped lifts the breach marker after a fitting commit or an
// admitted shrink; growth past the ceiling re-arms it.
func (h *StorHub) clearSizeCapped(project string) {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return
	}
	pm.mu.Lock()
	pm.sizeCapped = false
	pm.mu.Unlock()
}

// lookupProjectMeta returns the cached projectMetadata if present, without
// creating one. Read-only callers (rollback layout detection) must not spawn
// a cache entry as a side effect.
func (h *StorHub) lookupProjectMeta(project string) *projectMetadata {
	h.metaMu.RLock()
	defer h.metaMu.RUnlock()
	return h.metaCache[project]
}

func (h *StorHub) invalidateReleaseCache(project string) {
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	delete(h.releaseCache, project)
}

// addReleaseToCache appends a freshly created release to the project's
// cached list. It rebuilds the slice (new backing array) rather than
// appending in place, so a concurrent cachedReleasesView reader holding the
// previous slice never observes a torn update.
func (h *StorHub) addReleaseToCache(project string, release *ghapi.Release) {
	if release == nil {
		return
	}
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	entry, ok := h.releaseCache[project]
	if !ok {
		h.releaseCache[project] = releaseCacheEntry{
			releases:  []ghapi.Release{*cloneReleaseShallow(release)},
			fetchedAt: h.config.Now(),
		}
		h.trimReleaseCacheLocked()
		return
	}
	updated := make([]ghapi.Release, len(entry.releases), len(entry.releases)+1)
	copy(updated, entry.releases)
	updated = append(updated, *cloneReleaseShallow(release))
	entry.releases = updated
	h.releaseCache[project] = entry
	h.trimReleaseCacheLocked()
}

// cloneReleaseShallow copies a release for cache residency with its own asset
// backing array, keeping only asset IDs (the picker never reads Name/Size).
func cloneReleaseShallow(r *ghapi.Release) *ghapi.Release {
	out := *r
	if r.Assets != nil {
		assets := make([]ghapi.Asset, len(r.Assets))
		for j, a := range r.Assets {
			assets[j] = ghapi.Asset{ID: a.ID}
		}
		out.Assets = assets
	}
	return &out
}

// bumpCachedReleaseAssetCount records one uploaded asset against its release.
// It copy-on-writes the affected release's asset slice (and the releases
// slice) instead of appending through the shared array, so readers holding a
// cachedReleasesView of the prior state are not raced.
func (h *StorHub) bumpCachedReleaseAssetCount(project, tag string, assetID int64) {
	h.releaseMu.Lock()
	defer h.releaseMu.Unlock()
	entry, ok := h.releaseCache[project]
	if !ok {
		return
	}
	for i := range entry.releases {
		if entry.releases[i].TagName != tag {
			continue
		}
		updated := make([]ghapi.Release, len(entry.releases))
		copy(updated, entry.releases)
		// Record the real asset ID, not a -1 placeholder: the picker counts
		// embedded assets for capacity, and a fake ID both skews that math
		// and can never match server truth.
		assets := make([]ghapi.Asset, len(entry.releases[i].Assets), len(entry.releases[i].Assets)+1)
		copy(assets, entry.releases[i].Assets)
		updated[i].Assets = append(assets, ghapi.Asset{ID: assetID})
		entry.releases = updated
		h.releaseCache[project] = entry
		return
	}
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
		return fmt.Errorf("hydrate metadata before mutation: %w", loadErr)
	}
	return nil
}

// ensureMutableLocked is the shared pre-flight for direct mutation sites
// (the paths that bypass UpdateRepoMetadataContext): the cold-cache
// hydration guard (mutating an unhydrated empty tree commits it over
// remote state) and the size-capped growth gate (a tree already
// over the per-object ceiling can never commit growth; rejecting fast
// avoids the uncommittable-dirty livelock). Caller holds pm.mu; on error
// the caller unlocks and aborts.
func (h *StorHub) ensureMutableLocked(ctx context.Context, project string, pm *projectMetadata) error {
	if err := h.ensureHydratedLocked(ctx, project, pm); err != nil {
		return err
	}
	if pm.sizeCapped {
		return fmt.Errorf("metadata over size ceiling: growth is rejected until the tree fits again; delete entries or run `storhub prune`")
	}
	return nil
}

func markProjectDirtyLocked(pm *projectMetadata) {
	pm.dirty = true
	pm.version++
}

// maxPendingOpsPerProject bounds one project's pending op stack. The stack
// coalesces per path, so exceeding this means a project accumulated thousands
// of distinct changed paths while commits kept failing: the sweeper then
// force-retries the commit (ops can never be dropped - they are the
// acknowledged mutations), and the residency cap's backpressure stops new
// projects from piling on.
const maxPendingOpsPerProject = 4096

// appendOpLocked records one metadata mutation in the project's op stack
// and mirrors it to the crash-recovery journal. Caller holds pm.mu. The
// journal receives the post-coalescing op (the stack's latest state for its
// path), so replay folds to exactly the in-memory stack.
func (h *StorHub) appendOpLocked(project string, pm *projectMetadata, op Op) {
	pm.opStack.append(op)
	if len(pm.opStack.ops) > 0 {
		h.journalAppend(project, pm.opStack.ops[len(pm.opStack.ops)-1])
	}
	if len(pm.opStack.ops) == maxPendingOpsPerProject {
		logging.Warn(h.projectLogger(project), "pending op stack hit residency cap; commit will be force-retried until it drains", "ops", maxPendingOpsPerProject)
	}
}

// emitFamilySiblingsLocked records full-state ops for every hardlink
// sibling an inode-family mutation touched (ReplaceInodeFamily propagates
// identity across the family, TouchInodeFamilyChangedAt bumps them). The
// primary path's own op is the caller's business. Without sibling capture a
// mid-commit conflict would replay only the primary path and silently
// revert the propagation.
//
// `siblings` must be captured from the private COW tree BEFORE it is
// published: FindFilesByInode unconditionally rebuilds the index maps, which
// would race lock-free readers of the published tree. `tree` is that (soon
// to be published) private copy; reading entries from it here is safe.
// Caller holds pm.mu.
func (h *StorHub) emitFamilySiblingsLocked(project string, pm *projectMetadata, tree *RepoMetadata, siblings []string, inode uint64, primaryPath, cause string, now int64) {
	if inode == 0 {
		return
	}
	for _, path := range siblings {
		if path == primaryPath {
			continue
		}
		entry := tree.FindFile(path)
		if entry == nil {
			continue
		}
		e := entry.Clone()
		h.appendOpLocked(project, pm, Op{
			Type: OpSetattr, Paths: []string{path}, Cause: cause,
			Timestamp: now, File: &e,
			Chunks: chunkRecordsFor(tree, e.Chunks),
		})
	}
}

// emitParentDirOpLocked records the parent directory's current state after
// a create/delete touched its mtime. Caller holds pm.mu.
func (h *StorHub) emitParentDirOpLocked(project string, pm *projectMetadata, path, cause string, now int64) {
	parent := shfs.ParentPath(path)
	if parent == "" {
		root := pm.meta.Root.Clone()
		h.appendOpLocked(project, pm, Op{Type: OpSetattr, Paths: []string{""}, Cause: cause, Timestamp: now, Dir: &root})
		return
	}
	dir := pm.meta.GetDirectory(parent)
	if dir == nil {
		return
	}
	d := dir.Clone()
	h.appendOpLocked(project, pm, Op{Type: OpSetattr, Paths: []string{parent}, Cause: cause, Timestamp: now, Dir: &d})
}

// startCommitLoopLocked starts pm's commit loop unless Shutdown has begun.
// shutdownMu pairs the shutdownStarted check with shutdownWg.Add so an Add
// can never race Shutdown's Wait with a zeroed counter (the classic
// WaitGroup-misuse panic): Shutdown sets the flag under the same mutex
// before it waits, so a loser of the race skips the Add and the entry's
// dirty state is committed by the shutdown drain instead.
// Caller holds pm.mu (and metaMu where the cache slot is also managed).
func (h *StorHub) startCommitLoopLocked(project string, pm *projectMetadata) bool {
	h.shutdownMu.Lock()
	if h.shutdownStarted {
		h.shutdownMu.Unlock()
		return false
	}
	h.shutdownWg.Add(1)
	h.shutdownMu.Unlock()
	go h.commitLoop(project, pm)
	return true
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
// revivalTimeout returns the configured bound on waiting for an evicted
// commit loop to exit during revival, defaulting to 5s when unset (a
// literally-constructed Config that never ran WithDefaults).
func (h *StorHub) revivalTimeout() time.Duration {
	if d := h.config.RevivalTimeout; d > 0 {
		return d
	}
	return 5 * time.Second
}

func (h *StorHub) markProjectDirtyLiveLocked(project string, pm *projectMetadata) chan struct{} {
	if !pm.stopped {
		markProjectDirtyLocked(pm)
		return pm.triggerCh
	}
	// Capture the old loop's completion channel under pm.mu: the revival
	// below swaps pm.stoppedCh, and reading the field after releasing the
	// lock would race that swap.
	stoppedCh := pm.stoppedCh
	pm.mu.Unlock()
	// Wait for the evicted commit loop to fully exit before replacing its
	// channels: the loop reads stopCh/triggerCh unsynchronized, and the
	// eviction close(stopCh) only requests exit - stoppedCh closes when it
	// has actually returned.
	select {
	case <-stoppedCh:
	case <-time.After(h.revivalTimeout()):
		logging.Error(h.projectLogger(project), "evicted commit loop did not stop; reviving without channel swap", "project", project)
		// The mutation is already acknowledged. Re-insert the entry
		// (the old loop is still alive and will exit on its closed
		// stopCh; the shutdown drain and later revivals cover the
		// rest) and mark dirty, so the work is never silently stranded.
		h.metaMu.Lock()
		if current, exists := h.metaCache[project]; !exists || current == pm {
			h.metaCache[project] = pm
		}
		h.metaMu.Unlock()
		pm.mu.Lock()
		markProjectDirtyLocked(pm)
		return pm.triggerCh
	}
	h.metaMu.Lock()
	pm.mu.Lock()
	current, exists := h.metaCache[project]
	revived := false
	live := false
	drainOnly := false
	switch {
	case exists && current != pm:
		logging.Error(h.projectLogger(project),
			"evicted metadata snapshot diverged from a newer reload; mutations in this window are lost",
			"project", project)
	case exists && current == pm && !current.stopped:
		// Another goroutine completed the revival while we waited on
		// stoppedCh/metaMu; the instance is live again - just mark dirty.
		live = true
	case pm.reviving:
		// A concurrent revival is between channel swap and loop start;
		// touching channels here would orphan its fresh commit loop.
		live = true
	default:
		// Revive: fresh channels for a new commit loop, then re-insert.
		// The swap runs under pm.mu (metaMu is held too, preserving the
		// metaMu→pm.mu order): mutators read stopped/triggerCh under
		// pm.mu only, so an unsynchronized swap is a data race.
		//
		// If Shutdown has begun, do NOT swap channels or start a loop:
		// re-insert the entry as-is and mark it dirty so the shutdown
		// drain commits the acknowledged mutation instead of stranding it.
		h.shutdownMu.Lock()
		if h.shutdownStarted {
			h.shutdownMu.Unlock()
			if !exists {
				h.metaCache[project] = pm
			}
			drainOnly = true
		} else {
			h.shutdownMu.Unlock()
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
	}
	pm.mu.Unlock()
	h.metaMu.Unlock()
	if drainOnly {
		// Shutdown began: the entry is back in the cache (or was never
		// removed); mark it dirty and return with pm.mu held like every
		// other path. The shutdown drain commits it.
		pm.mu.Lock()
		markProjectDirtyLocked(pm)
		return pm.triggerCh
	}
	if revived {
		logging.Info(h.projectLogger(project), "reviving evicted project metadata after concurrent operation", "project", project)
		// startCommitLoopLocked returns false if Shutdown began between the
		// flag check in the switch above and the Add; the loop is not
		// started, but the entry is live in the cache and the tail below
		// marks it dirty for the drain. Either way clear the revival guard.
		h.startCommitLoopLocked(project, pm)
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

func (h *StorHub) getGitRepo(project string) *gitRepo {
	if h.config.DisableGitBackend {
		return nil
	}
	// Reads take RLock: the map lookup is the common case and must not
	// convoy behind a single writer. Insert keeps the exclusive lock.
	h.gitMu.RLock()
	r, ok := h.gitRepos[project]
	h.gitMu.RUnlock()
	if ok {
		return r
	}
	h.gitMu.Lock()
	defer h.gitMu.Unlock()
	if r, ok := h.gitRepos[project]; ok {
		return r
	}
	r = newGitRepo(h.config.GitCacheDir, h.owner, project, h.token)
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

	// Double-check after acquiring write lock
	pm, exists = h.metaCache[project]
	if exists {
		h.metaMu.Unlock()
		pm.mu.Lock()
		pm.lastAccess = h.config.Now()
		pm.mu.Unlock()
		return pm
	}

	now := h.config.Now()
	// Enforce the residency cap before adding another entry: growth is an
	// event, so the cap is applied exactly when a new project joins. Read
	// paths admit unbounded (a freshly loaded entry is clean and therefore
	// evictable); mutation entry points use getOrCreateProjectMetaAdmitted.
	admitted, evicted := h.evictForCapacityLocked()
	_ = admitted // read path inserts regardless; fresh entries are evictable
	pm = h.newProjectMetaLocked(project, now)
	h.metaMu.Unlock()
	h.releaseEvicted(evicted)
	return pm
}

// releaseEvicted cascades residue teardown for projects evicted while the
// caller held metaMu; it must run after that lock is dropped (git teardown
// does directory I/O).
func (h *StorHub) releaseEvicted(evicted []string) {
	for _, name := range evicted {
		h.releaseProjectResidue(name)
	}
}

// getOrCreateProjectMetaAdmitted is the mutation-facing variant: a brand-new
// project is refused when the cache is at cap and nothing is evictable
// (every resident entry holds unpushed changes). Accepting it anyway would
// make the cap bypassable by persistently-dirty projects - backpressure
// instead asks the caller to retry once a flush frees a slot.
func (h *StorHub) getOrCreateProjectMetaAdmitted(project string) (*projectMetadata, error) {
	h.metaMu.RLock()
	pm, exists := h.metaCache[project]
	h.metaMu.RUnlock()
	if exists {
		pm.mu.Lock()
		pm.lastAccess = h.config.Now()
		pm.mu.Unlock()
		return pm, nil
	}

	h.metaMu.Lock()
	if pm, exists = h.metaCache[project]; exists {
		h.metaMu.Unlock()
		pm.mu.Lock()
		pm.lastAccess = h.config.Now()
		pm.mu.Unlock()
		return pm, nil
	}
	admitted, evicted := h.evictForCapacityLocked()
	if !admitted {
		h.metaMu.Unlock()
		h.releaseEvicted(evicted)
		return nil, fmt.Errorf("too many tracked projects with unpushed metadata (cap %d): flush or retry before mutating a new project", h.config.MaxTrackedProjects)
	}
	pm = h.newProjectMetaLocked(project, h.config.Now())
	h.metaMu.Unlock()
	h.releaseEvicted(evicted)
	return pm, nil
}

// newProjectMetaLocked creates the entry, inserts it, and starts its commit
// loop (skipped once Shutdown began - the drain covers dirty state).
// Caller holds metaMu for writing.
func (h *StorHub) newProjectMetaLocked(project string, now time.Time) *projectMetadata {
	pm := &projectMetadata{
		meta:       metadata.NewRepoMetadata(project),
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
		triggerCh:  make(chan struct{}, 1),
		lastAccess: now,
	}
	h.metaCache[project] = pm
	h.startCommitLoopLocked(project, pm)
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

	for {
		select {
		case <-pm.stopCh:
			// Project evicted, stop the commit loop
			return
		case <-pm.triggerCh:
			// Wake up and commit if dirty, then continue loop. The commit
			// derives from the hub-level ctx (canceled by Shutdown), so a
			// push in flight unwinds promptly when shutdown begins instead
			// of parking the wait on the HTTP client's own timeout.
			if err := h.commitProjectMetadata(h.baseCtx, project, pm); err != nil {
				h.recoverMetadataCommitFailure(project, err)
			}

		case <-h.shutdownCh:
			// Shutdown requested. The final drain belongs to Shutdown's
			// sweep (drainDirtyMetadata), which runs after every loop has
			// exited and also covers projects whose loop died earlier.
			// Committing here as well would double-attempt every dirty
			// project and race the sweep.
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
	// Rebase exhaustion retains the pending ops for a later retry: the
	// commit already tried replaying them onto upstream and kept losing
	// the race - discarding here would destroy exactly that work.
	var rex *rebaseExhaustedError
	if errors.As(err, &rex) {
		logging.Error(logger, "metadata commit conflicted past rebase budget; retaining pending ops for retry", "err", rex.err, "attempts", rex.attempts)
		return
	}
	// Conflict (stale previous_sha against remote HEAD) means another
	// writer advanced the metadata. Rebase-capable commits resolve their
	// own conflicts internally (exhaustion returns above), so a conflict
	// reaching here still has its ops pending: reloading remote truth and
	// discarding them (the old behavior) destroyed acknowledged work on a
	// path that could not even be reached from commitProjectMetadata.
	// Retain instead - the next trigger rebases the stack onto upstream.
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
	logging.Error(logger, "metadata commit conflicted; retaining pending ops for the next rebase", "err", err)
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
	// Normalize a private copy, never the shared tree. A failed commit
	// (size ceiling, push error) must leave pm.meta exactly as the
	// mutations left it; the normalized working copy is applied back only
	// on success below. cowTree is a shallow copy over immutable entries,
	// so the commit no longer deep-copies every Chunks/XAttrs.
	working := *cowTree(pm.meta)
	previousSHA := pm.sha
	version := pm.version
	// Snapshot the op stack with the working copy: the commit message
	// describes exactly these ops, and only they may be dropped on
	// success (mutations landing mid-commit carry higher seqs and stay).
	// Recording the snapshot seq lets coalescing refuse to rewrite ops
	// that are in flight inside this commit.
	ops := pm.opStack.snapshot()
	opSeq := pm.opStack.maxSeq()
	pm.opStack.noteSnapshot(opSeq)
	baseTree := pm.baseTree
	objectCount := pm.objectCount
	pm.mu.Unlock()

	// The split index (metadata version 5) is the default and only write
	// layout: every commit produces a manifest plus content-addressed objects.
	// A project still on a legacy single-blob document (version <= 4) migrates
	// on this write; its loaded tree carries that version, so the layout is
	// read straight from the metadata version, not a separate flag.
	headSplit := working.IsSplit()
	working.MarkSplit()

	now := h.config.Now().Unix()
	working.Normalize(project, now)
	working.LastMod = now
	working.RecomputeStats()

	logging.Info(h.projectLogger(project), "commit metadata start", "previous_sha", shortSHA(previousSHA), "migrating", !headSplit)

	if err := h.ensureOwner(ctx); err != nil {
		return err
	}

	// No full-tree Validate here: it is a hot-path O(N) pass over a tree
	// that was validated when it was loaded (loadIndexTree), and every
	// mutation since then went through op application or the transaction
	// admission path. Validation runs on load and on the rebase result.

	// A legacy->split migration CASes the manifest, not the legacy blob:
	// resolve the manifest's own token. Two clobber windows exist:
	//   - the manifest is absent: the PUT carries no token, so the
	//     contents API does no conflict detection; a concurrent migrator
	//     landing before our read-back is caught by verifying HEAD after
	//     the publish.
	//   - the manifest exists but our tree was built on the legacy blob:
	//     a rival migrated after our load. CASing with the rival's token
	//     would "succeed" while publishing a tree that lacks the rival's
	//     changes, so this is treated as a conflict up front and rebased.
	migrationUnconditional := false
	migratedUnderneath := false
	if !headSplit && h.config.DisableGitBackend {
		if data, s, found, lerr := h.readIndexHead(ctx, project); lerr == nil {
			if found && metadata.IsManifest(data) {
				previousSHA = s
				migratedUnderneath = true
			} else {
				previousSHA = ""
				migrationUnconditional = true
			}
		}
	}

	// Commit with a bounded commit/rebase cycle: a CAS conflict (another
	// writer advanced the index) rebases the pending ops onto upstream
	// state instead of discarding them, then retries.
	message := buildCommitMessage(ops, previousSHA)
	var commitSHA, contentSHA string
	var newObjectCount uint64
	didRebase := false
	// The rebase baseline fingerprint is computed on demand (only when a
	// conflict actually forces a rebase) and memoized across attempts, so
	// the common clean commit never pays the O(N) hash pass the old
	// resident basePaths map precomputed at store time.
	var baseFP map[string][16]byte
	for attempt := 1; ; attempt++ {
		var err error
		if attempt == 1 && migratedUnderneath {
			// The manifest appeared after our legacy base was loaded.
			// Publishing with the rival's token would pass CAS while
			// dropping the rival's changes - enter the conflict path
			// directly so the first attempt rebases.
			err = &ghapi.APIError{StatusCode: http.StatusConflict, Message: "project migrated to the split layout after our base was loaded"}
		} else {
			commitSHA, contentSHA, newObjectCount, err = h.publishIndex(ctx, project, &working, previousSHA, message, objectCount)
			if err == nil && migrationUnconditional && previousSHA == "" {
				// The manifest PUT carried no CAS token, so success does
				// not prove our bytes are HEAD. Read back and compare the
				// content SHA; a mismatch means a concurrent migrator won.
				if _, s, found, lerr := h.readIndexHead(ctx, project); lerr == nil && found && s != "" && s != contentSHA {
					err = &ghapi.APIError{
						StatusCode: http.StatusConflict,
						Message:    fmt.Sprintf("migration publish clobbered by a concurrent migrator (HEAD %s, ours %s)", shortSHA(s), shortSHA(contentSHA)),
					}
				}
			}
		}
		if err == nil {
			break
		}
		var over *oversizeError
		if errors.As(err, &over) {
			logging.Error(h.projectLogger(project), "commit metadata failed", "step", "size_check", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
			// Fail-fast admission from here on: growth mutations are
			// rejected until a shrink folds the tree back under the ceiling.
			pm.mu.Lock()
			pm.sizeCapped = true
			pm.mu.Unlock()
			return &commitError{err: err, version: version}
		}
		var apiErr *ghapi.APIError
		isConflict := errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict
		if !isConflict || attempt >= maxCommitAttempts {
			if isConflict {
				err = &rebaseExhaustedError{err: err, attempts: attempt}
			}
			err = wrapNoSpace(h.config.GitCacheDir, err)
			logging.Error(h.projectLogger(project), "commit metadata failed", "step", "git_commit", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
			return &commitError{err: fmt.Errorf("commit metadata: %w", err), version: version}
		}
		logging.Warn(h.projectLogger(project), "metadata commit conflicted; rebasing op stack", "attempt", attempt, "ops", len(ops))
		if baseFP == nil && baseTree != nil {
			baseFP = hashPaths(baseTree)
		}
		rebased, upstreamSHA, resolutions, rerr := h.rebaseOntoUpstream(ctx, project, ops, baseFP)
		if rerr != nil {
			logging.Error(h.projectLogger(project), "commit metadata failed", "step", "rebase", "elapsed", h.config.Now().UTC().Sub(started), "err", rerr)
			return &commitError{err: fmt.Errorf("rebase pending ops: %w", rerr), version: version}
		}
		didRebase = true
		working = *rebased
		working.LastMod = now
		previousSHA = upstreamSHA
		message = buildCommitMessage(ops, previousSHA) + "\n" + rebaseMessageNote(resolutions, upstreamSHA)
	}

	pm.mu.Lock()
	pm.sha = contentSHA
	pm.objectCount = newObjectCount
	if pm.version == version {
		// Apply-back: the normalized working copy becomes the shared
		// truth on success. Without this the cache keeps the raw mutation
		// state (stale stats, unrepaired inode counter) and every later
		// Validate of cached state - rollback, drain, explicit checks -
		// trips over it. Version-guarded: a mutation that landed while the
		// commit ran owns the newer state; its own commit normalizes.
		pm.meta = &working
		pm.dirty = false
		pm.lastCommit = h.config.Now()
		pm.opStack.clearUpTo(opSeq)
	} else if didRebase {
		// A mutation landed mid-commit AND the commit rebased: the
		// committed tree carries upstream changes the live tree lacks, so
		// adopting the live tree wholesale would make the next commit
		// overwrite them. Replay the surviving ops onto the committed tree.
		pm.opStack.clearUpTo(opSeq)
		surviving := pm.opStack.snapshot()
		if len(surviving) > 0 {
			rebased := working
			if err := applyOps(&rebased, surviving); err != nil {
				// Defensive only: well-formed ops cannot fail replay.
				// Converge to the committed truth rather than keep a
				// stale tree that could clobber it.
				logging.Error(h.projectLogger(project), "mid-commit mutation replay failed; committed state retained", "err", err)
				pm.opStack.clear()
				pm.meta = &working
			} else {
				rebased.Normalize(project, h.config.Now().Unix())
				rebased.RecomputeStats()
				pm.meta = &rebased
			}
		} else {
			pm.meta = &working
		}
		pm.lastCommit = h.config.Now()
	} else {
		// Mid-commit mutation, no rebase: the live tree already contains
		// the committed ops' effects plus the newer mutation. Drop the
		// committed ops (their state is in the live tree) and keep the
		// mutation's ops + dirty flag for the next commit.
		pm.opStack.clearUpTo(opSeq)
	}
	// The journal is rewritten to the surviving stack.
	h.journalRewrite(project, pm.opStack.ops)
	// The rebase baseline moves to the just-committed state (a frozen tree
	// we hand a pointer to; the fingerprint is recomputed only on conflict).
	pm.baseTree = &working
	// A fitting commit lifts the size-ceiling breach marker (re-armed on breach).
	pm.sizeCapped = false
	pm.mu.Unlock()

	h.warnHistoryThreshold(project, pm)
	logging.Info(h.projectLogger(project), "commit metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "commit_sha", shortSHA(commitSHA), "content_sha", shortSHA(contentSHA), "objects", newObjectCount)

	return nil
}

// evictForCapacityLocked keeps the tracked-project set at
// MaxTrackedProjects by evicting the least-recently-used clean entry at
// insert time. Victims must be clean (nothing unpushed) and not mid-
// revival; a dirty entry survives arbitrarily long. It returns admitted=false
// when the cache is at cap and no victim qualifies: read paths still insert
// (a fresh entry is clean, hence evictable, so growth self-limits), while
// mutation admission (getOrCreateProjectMetaAdmitted) turns the false into
// backpressure instead of the old "insert unbounded". Alternating access to
// more projects than the cap thrashes remote reloads per miss; that costs
// bandwidth, never data. Revival re-insertions (markProjectDirtyLiveLocked)
// deliberately bypass this cap - they serve an operation already in flight;
// overshoot is bounded by concurrently stranded stale pointers.
//
// Evicted names are returned for the caller to cascade (releaseProjectResidue)
// AFTER dropping metaMu: the git mirror teardown does directory I/O and must
// not hold the cache lock. Caller holds metaMu for writing.
func (h *StorHub) evictForCapacityLocked() (admitted bool, evicted []string) {
	if len(h.metaCache) < h.config.MaxTrackedProjects {
		h.capWarned = false
		return true, nil
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
			return true, []string{cand.name}
		}
	}
	if !h.capWarned {
		logging.Warn(h.logger, "tracked projects exceed cap with no evictable entry; new-project mutations face backpressure", "cap", h.config.MaxTrackedProjects, "resident", len(h.metaCache))
		h.capWarned = true
	}
	return false, nil
}

// Cache residency policy: a clean project's metadata is dropped after
// metaCacheIdleTTL without access (the lastAccess stamp was already
// recorded; nothing read it until now), a cached release list after
// releaseCacheTTL (enforced lazily on read and here for untouched
// projects), and a dirty project whose pending stack passed the op cap gets
// its commit force-retried. The sweeper is the missing periodic bound: the
// insert-time eviction alone never ran on idle timeout.
const (
	metaCacheIdleTTL = 30 * time.Minute
	sweeperInterval  = 30 * time.Second
)

// sweeperLoop is the low-frequency in-memory cache janitor. It is bound to
// the hub context (canceled by Shutdown) and joined on shutdownWg, so it
// never outlives the hub.
func (h *StorHub) sweeperLoop() {
	defer h.shutdownWg.Done()
	ticker := time.NewTicker(sweeperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.shutdownCh:
			return
		case <-h.baseCtx.Done():
			return
		case <-ticker.C:
			h.sweepCachesOnce()
		}
	}
}

// sweepCachesOnce TTL-evicts idle clean metadata entries (cascading their
// residue), drops stale release lists, and pokes a commit retry for dirty
// projects whose op stack outgrew the residency cap.
func (h *StorHub) sweepCachesOnce() {
	now := h.config.Now()

	var evicted []string
	h.metaMu.Lock()
	for name, pm := range h.metaCache {
		pm.mu.Lock()
		idleClean := !pm.dirty && !pm.reviving && !pm.stopped && !pm.lastAccess.IsZero() && now.Sub(pm.lastAccess) > metaCacheIdleTTL
		forceFlush := pm.dirty && len(pm.opStack.ops) >= maxPendingOpsPerProject
		trigger := pm.triggerCh
		pm.mu.Unlock()
		if idleClean {
			pm.mu.Lock()
			if !pm.dirty && !pm.reviving && !pm.stopped {
				pm.stopped = true
				close(pm.stopCh)
				delete(h.metaCache, name)
				pm.mu.Unlock()
				evicted = append(evicted, name)
				continue
			}
			pm.mu.Unlock()
		}
		if forceFlush {
			// Retry the failing commit; the stack can never be dropped
			// (acknowledged mutations), only pushed.
			select {
			case trigger <- struct{}{}:
			default:
			}
		}
	}
	h.metaMu.Unlock()
	h.releaseEvicted(evicted)

	h.releaseMu.Lock()
	for name, entry := range h.releaseCache {
		if now.Sub(entry.fetchedAt) > releaseCacheTTL {
			delete(h.releaseCache, name)
		}
	}
	h.releaseMu.Unlock()
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

// drainDirtyMetadata synchronously commits every project left dirty -
// mutations stranded by an exited loop or a prior Shutdown. Best effort
// across projects: every failure is reported, none skips the rest.
func (h *StorHub) drainDirtyMetadata(ctx context.Context) error {
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

	var errs []error
	for _, p := range projects {
		if err := h.commitProjectMetadata(ctx, p.name, p.meta); err != nil {
			errs = append(errs, fmt.Errorf("shutdown drain %s: %w", p.name, err))
		}
	}
	return errors.Join(errs...)
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
			// FlushMetadata gets the same conflict recovery as the
			// commit loop: a 409 reloads remote HEAD into the cache so a
			// stale flush converges instead of staying stale. The error
			// is still reported to the caller.
			h.recoverMetadataCommitFailure(p.name, err)
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
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return "", "", err
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return "", "", err
	}
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return "", "", err
	}
	if cleanName == "" {
		return "", "", errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
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
	releaseTag, uploadURL, err = h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots)
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
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
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
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, chunks)
		return nil, err
	}

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. PrepareReplaceContext EnsureReleases only on a local
	// clone that is discarded before this call. All mutations apply to a
	// private COW copy; the shared tree is swapped in only on success.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, now)
	ensureChunkReleases(tree, chunks, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(chunks))
	for i := range chunks {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, chunks[i])
		chunkIDs[i] = id
	}
	fileMeta.Chunks = chunkIDs
	fileMeta.Size = size
	latest := tree.FindFile(cleanName)
	if latest == nil {
		// The file vanished between the readonly pre-check and this
		// locked re-check (concurrent delete). The chunks are already
		// uploaded and their IDs already allocated into the catalog:
		// roll both back or the assets leak until PurgeUntracked and the
		// catalog carries chunks no file references. The COW copy is
		// discarded, so the shared tree never saw the allocation.
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, chunks)
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, latest, now)
	implposix.ReplaceInodeFamily(tree, cleanName, latest, fileMeta, now)
	siblings := tree.FindFilesByInode(fileMeta.Inode)
	publishTreeLocked(pm, tree)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	opFile := fileMeta.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPutFile, Paths: []string{cleanName}, Cause: "replace-chunks",
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, fileMeta.Inode, cleanName, "replace-chunks-family", now)
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
	// Name-collision retries and release-full rotation live in the sink.
	totalChunks := 0
	if size > 0 {
		totalChunks = int((size + chunkSize - 1) / chunkSize)
	}
	prepare := func(remaining int) (string, string, error) {
		return h.PrepareReplaceContext(ctx, project, filePath, remaining)
	}
	sink := h.newChunkSink(ctx, project, releaseTag, uploadURL, totalChunks, prepare)
	var uploaded int64
	for uploaded < size {
		windowSize := min64(chunkSize, size-uploaded)

		win, cleanup, werr := newWindowReader(body, windowSize)
		if werr != nil {
			h.compensateDeleteAssets(ctx, project, sink.results)
			return nil, werr
		}

		if err := sink.put(win, windowSize, uploaded); err != nil {
			cleanup()
			h.compensateDeleteAssets(ctx, project, sink.results)
			return nil, err
		}
		cleanup()
		uploaded += windowSize
	}

	return h.FinalizeReplaceChunksContext(ctx, project, filePath, sink.releaseTag, uploaded, sink.results)
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
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
		return nil, err
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
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
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
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return nil, err
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
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
		return nil, err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
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
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, newChunks)
		return nil, err
	}
	// Mutations apply to a private COW copy; publish only on success.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, now)
	ensureChunkReleases(tree, newChunks, now)
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, newChunks[i])
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
	current := tree.FindFile(cleanName)
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
	implposix.ReplaceInodeFamily(tree, cleanName, current, patched, now)
	siblings := tree.FindFilesByInode(patched.Inode)
	publishTreeLocked(pm, tree)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	opFile := patched.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPatch, Paths: []string{cleanName}, Cause: "patch-ranges",
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, patched.Inode, cleanName, "patch-ranges-family", now)
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
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, newChunks)
		return nil, err
	}

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. buildPatchedChunks EnsureReleases only on a local
	// clone that is discarded here. Mutations apply to a private COW copy;
	// publish only on success.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, now)
	ensureChunkReleases(tree, newChunks, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, newChunks[i])
		chunkIDs[i] = id
	}
	patched.Chunks = chunkIDs
	patched.Size = fileMeta.Size - deleteSize + int64(len(edit))
	patched.Mode = shfs.SanitizeWrittenFileMode(patched.Mode)
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := tree.FindFile(cleanName)
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
	implposix.ReplaceInodeFamily(tree, cleanName, current, patched, now)
	siblings := tree.FindFilesByInode(patched.Inode)
	publishTreeLocked(pm, tree)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	opFile := patched.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPatch, Paths: []string{cleanName}, Cause: "patch",
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, patched.Inode, cleanName, "patch-family", now)
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
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, newChunks)
		return nil, err
	}

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. buildRewrittenChunks EnsureReleases only on a local
	// clone that is discarded here. Mutations apply to a private COW copy;
	// publish only on success.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, now)
	ensureChunkReleases(tree, newChunks, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, newChunks[i])
		chunkIDs[i] = id
	}
	rewritten.Chunks = chunkIDs
	rewritten.Size = finalSize
	rewritten.Mode = shfs.SanitizeWrittenFileMode(rewritten.Mode)
	rewritten.ModifiedAt = now
	rewritten.ChangedAt = now
	rewritten.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := tree.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &rewritten, current, now)
	implposix.ReplaceInodeFamily(tree, cleanName, current, rewritten, now)
	siblings := tree.FindFilesByInode(rewritten.Inode)
	publishTreeLocked(pm, tree)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	opFile := rewritten.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPutFile, Paths: []string{cleanName}, Cause: "rewrite-ranges",
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, rewritten.Inode, cleanName, "rewrite-ranges-family", now)
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
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return nil, err
	}

	// Backpressure gate: reserve the project's metadata slot BEFORE any
	// upload work. A brand-new project is refused here (cheaply, with no
	// orphaned assets) when the cache is at cap and every resident project
	// holds unpushed changes; an already-resident project passes through.
	if _, err := h.getOrCreateProjectMetaAdmitted(project); err != nil {
		return nil, err
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
	// put has open(O_CREAT|O_TRUNC) semantics: a final symlink is followed
	// to its target, so followFinal is true.
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
		return nil, err
	}
	if err := shfs.RequireParentDirectory(repoMeta, cleanName); err != nil {
		return nil, err
	}
	if err := shfs.CheckParentWrite(ctx, repoMeta, cleanName); err != nil {
		return nil, err
	}
	if repoMeta.HasDirectory(cleanName) {
		return nil, shfs.IsDirectory(cleanName)
	}
	existing := repoMeta.FindFile(cleanName)
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
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots)
	if err != nil {
		return nil, err
	}

	results := []ChunkInfo{}
	if fileInfo.Size() > 0 {
		prepare := func(remaining int) (string, string, error) {
			return h.getOrCreateUploadRelease(ctx, project, &workingMeta, remaining)
		}
		results, err = h.uploadChunks(ctx, project, releaseTag, uploadURL, planner, prepare)
		if err != nil {
			// The file never commits: delete this call's orphaned assets so
			// a mid-upload failure cannot leak storage. Rotation inside the
			// sink already moved later chunks; only compensate what landed.
			h.compensateDeleteAssets(ctx, project, results)
			return nil, err
		}
	}
	fileMeta := FileMeta{
		Size:   fileInfo.Size(),
		Chunks: nil,
	}
	implposix.ApplyUploadIdentity(cleanName, existing, &fileMeta, h.config.Now().Unix())
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
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}

	if err := shfs.CheckTraversal(ctx, pm.meta, traversed); err != nil {
		pm.mu.Unlock()
		// The chunks are already uploaded by now; a late permission
		// failure must not leak them as orphans.
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}
	if err := shfs.CheckParentWrite(ctx, pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		// The chunks are already uploaded by now; a late permission
		// failure must not leak them as orphans.
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}
	if err := shfs.RequireParentDirectory(pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}
	if pm.meta.HasDirectory(cleanName) {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, shfs.IsDirectory(cleanName)
	}
	if !replace && pm.meta.FindFile(cleanName) != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, shfs.AlreadyExists(cleanName)
	}
	// All mutations apply to a private COW copy; the shared tree is swapped
	// in only once every fallible step has succeeded.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, h.config.Now().Unix())
	// Rotation may have spread this file's chunks across releases;
	// ensuring only the initial tag would strand rotated chunks outside
	// the catalog where PurgeUntracked deletes live data.
	ensureChunkReleases(tree, results, h.config.Now().Unix())
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(results))
	for i := range results {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, results[i])
		chunkIDs[i] = id
	}
	fileMeta.Chunks = chunkIDs
	current := tree.FindFile(cleanName)
	if current != nil {
		implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, current, h.config.Now().Unix())
		implposix.ReplaceInodeFamily(tree, cleanName, current, fileMeta, h.config.Now().Unix())
	} else {
		fileMeta.Mode, fileMeta.UID, fileMeta.GID = shfs.ApplyParentInheritance(tree, cleanName, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
		metadata.InitializeNewFileIdentity(tree, &fileMeta, h.config.Now().Unix())
		tree.UpsertFile(cleanName, fileMeta, h.config.Now().Unix())
	}
	shfs.TouchParentDirectory(tree, cleanName, h.config.Now().Unix())
	siblings := tree.FindFilesByInode(fileMeta.Inode)
	publishTreeLocked(pm, tree)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	cause := "upload"
	if replace {
		cause = "replace"
	}
	now := h.config.Now().Unix()
	opFile := fileMeta.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPutFile, Paths: []string{cleanName}, Cause: cause,
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, fileMeta.Inode, cleanName, cause+"-family", now)
	h.emitParentDirOpLocked(project, pm, cleanName, cause+"-parent", now)
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
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return err
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return err
	}
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return err
	}
	if cleanName == "" {
		return errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
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
		chunk, ok := repoMeta.Chunks()[chunkName]
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
	result = make([]metadata.ReleaseRef, 0, len(repoMeta.Releases()))
	for _, ref := range repoMeta.Releases() {
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
	// A branch name is not a revision. The contents API resolves
	// unknown refs to HEAD content, so passing 'main' would silently
	// roll back to HEAD (a no-op that reports success). Only a commit
	// SHA from this file's own revision history is accepted.
	if strings.ContainsAny(commitSHA, "/ 	\n") {
		return fmt.Errorf("invalid metadata revision %q: not a commit SHA", commitSHA)
	}
	if err := h.validateMetadataRevision(ctx, project, commitSHA); err != nil {
		return err
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
	// Git-path CAS pin: cached/fresh loads carry "" as the version token,
	// which would make the write-time compare vacuous and let this rollback
	// silently overwrite a concurrent writer. Re-sync and pin the real HEAD
	// commit, so commitRepoMetadata aborts with 409 when HEAD moved. The
	// fresh load returns the HEAD token paired atomically with the index
	// content it read; re-reading headCommitSHA separately could pair
	// content at commit N with token N+1.
	if !h.config.DisableGitBackend {
		if fresh, freshSHA, freshErr := h.loadRepoMetadataFresh(ctx, project); freshErr == nil && freshSHA != "" {
			currentMeta = fresh
			if err := currentMeta.Validate(); err != nil {
				return err
			}
			currentSHA = freshSHA
		}
	}
	rollbackMeta, err := h.getMetadataRevision(ctx, project, commitSHA)
	if err != nil {
		return err
	}
	if err := h.validateMetadataSnapshot(ctx, project, rollbackMeta); err != nil {
		return err
	}
	// The snapshot was validated against a listing taken moments ago;
	// assets can be deleted between that check and this commit. Re-check
	// immediately before committing to narrow the race window.
	if err := h.validateMetadataSnapshot(ctx, project, rollbackMeta); err != nil {
		return fmt.Errorf("rollback snapshot changed before commit: %w", err)
	}
	_, _, err = h.commitRepoMetadata(ctx, project, *rollbackMeta, currentSHA, fmt.Sprintf("storhub: rollback metadata to %s", shortSHA(commitSHA)))
	if err != nil {
		return err
	}
	// The delete can also land mid-commit (after the re-check above).
	// Verify the committed snapshot against fresh server state and fail
	// loudly instead of blessing bytes that can no longer be downloaded.
	if err := h.validateMetadataSnapshot(ctx, project, rollbackMeta); err != nil {
		return fmt.Errorf("rollback committed but snapshot no longer validates: %w", err)
	}
	return nil
}

// RevertPath restores a single path (a file or an entire directory subtree) to
// its state at commitSHA, leaving every other path untouched, as a NEW commit.
func (h *StorHub) RevertPath(project, path, commitSHA string) error {
	return h.RevertPathContext(context.Background(), project, path, commitSHA)
}

// RevertPathContext is the per-path counterpart of RollbackMetadataContext:
// instead of repointing the whole index at an old revision, it replays just
// `path`'s historical state onto the current tree. It is a revert, not a
// force-push: history is preserved and the result flows through the normal
// transaction path (op synthesis, journal, rebase, commit). The reverted
// subtree's assets are validated against live releases before and after the
// commit, so restoring a path whose bytes were purged fails loudly rather
// than committing a dangling reference.
func (h *StorHub) RevertPathContext(ctx context.Context, project, path, commitSHA string) (err error) {
	started := h.logOpStart(project, "revert-path", "path", path, "commit_sha", commitSHA)
	defer func() { h.logOpFinish(project, "revert-path", started, err, "path", path, "commit_sha", commitSHA) }()
	if err := validateProject(project); err != nil {
		return err
	}
	if err := shfs.ValidateAccessPathShape(path); err != nil {
		return err
	}
	if strings.TrimSpace(commitSHA) == "" {
		return errors.New("commit sha is required")
	}
	// A branch name is not a revision (the contents API resolves unknown
	// refs to HEAD, which would silently "revert" to current).
	if strings.ContainsAny(commitSHA, "/ \t\n") {
		return fmt.Errorf("invalid metadata revision %q: not a commit SHA", commitSHA)
	}
	if err := h.validateMetadataRevision(ctx, project, commitSHA); err != nil {
		return err
	}
	// Flush pending mutations so the revert is built on committed truth.
	pm := h.getOrCreateProjectMeta(project)
	if err := h.commitProjectMetadata(ctx, project, pm); err != nil {
		return err
	}
	historical, err := h.getMetadataRevision(ctx, project, commitSHA)
	if err != nil {
		return err
	}
	// Validate the would-be result before committing: the reverted subtree's
	// chunks must resolve to releases/assets that still exist.
	current, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return err
	}
	// A revert addresses the node the path names, so the final symlink is
	// followed; the historical tree is keyed by concrete paths, hence the
	// resolved key (not the raw spelling) is what RevertSubtree replays.
	cleanPath, _, err := shfs.ResolveAccessPath(current, path, true)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		return errors.New("revert requires a non-root path")
	}
	preview := current.Clone()
	if err := metadata.RevertSubtree(&preview, historical, cleanPath, h.config.Now().Unix()); err != nil {
		return err
	}
	preview.Normalize(project, h.config.Now().Unix())
	if err := h.validateMetadataSnapshot(ctx, project, &preview); err != nil {
		return fmt.Errorf("revert %s: %w", cleanPath, err)
	}
	message := fmt.Sprintf("storhub: revert %s to %s", cleanPath, shortSHA(commitSHA))
	if _, err := h.UpdateRepoMetadataContext(ctx, project, func(m *metadata.RepoMetadata) error {
		return metadata.RevertSubtree(m, historical, cleanPath, h.config.Now().Unix())
	}, message); err != nil {
		return err
	}
	// Commit synchronously: a revert is a discrete operation the caller
	// expects to be durable on return, not left to the async flush loop.
	if err := h.commitProjectMetadata(ctx, project, h.getOrCreateProjectMeta(project)); err != nil {
		return err
	}
	// Assets can be deleted between the pre-check and the commit; re-check
	// the committed state against fresh server truth.
	committed, _, err := h.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		return err
	}
	if err := h.validateMetadataSnapshot(ctx, project, committed); err != nil {
		return fmt.Errorf("revert committed but snapshot no longer validates: %w", err)
	}
	return nil
}

// validateMetadataRevision ensures revision is a known commit SHA of the
// project's metadata history, never a branch or tag name.
func (h *StorHub) validateMetadataRevision(ctx context.Context, project, revision string) error {
	revisions, err := h.listMetadataRevisions(ctx, project)
	if err != nil {
		return err
	}
	for _, rev := range revisions {
		if rev.CommitSHA == revision {
			return nil
		}
	}
	return fmt.Errorf("invalid metadata revision %q: not a known commit SHA for project %s", revision, project)
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

// UpdateRepoMetadataContext applies fn as a transaction against the project's
// metadata. The mutation is applied to a private copy-on-write tree under the
// exclusive lock and marked dirty for event-driven commit; the shared tree is
// swapped in only after every fallible step (apply, normalize, admission) has
// succeeded, so a rejected mutation leaves shared state, the dirty flag, and
// the op stack exactly as they were (rollback is free: the copy is discarded).
func (h *StorHub) UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*metadata.RepoMetadata) error, message string) (*metadata.RepoMetadata, error) {
	pm, err := h.getOrCreateProjectMetaAdmitted(project)
	if err != nil {
		return nil, err
	}
	lockStarted := h.config.Now().UTC()

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

	// 8MB ceiling — fail fast, never accept-then-never-commit. Apply the
	// mutation to a throwaway COW copy and measure the result before
	// touching shared state. An oversize growth is rejected at admission
	// with a remediation pointer; shared state, dirty, and usability stay
	// exactly as they were. Shrinks stay open: a mutation that reduces the
	// tree is admitted even while oversize, so the project can always fold
	// back under the ceiling.
	//
	// The blob ceiling is a legacy constraint: a split project has no
	// single-blob size limit (admission is per-object at commit), so
	// measuring the blob serialization must not reject growth on it.
	candidate := cowTree(pm.meta)
	// beforeSize is the pre-transaction serialized size, measured on the
	// private copy (the engine's incremental SerializedSize, not a whole-tree
	// ToJSON marshal). It is captured before fn so the shrink test below has
	// a baseline without ever touching the shared tree's derived state.
	beforeSize, err := candidate.SerializedSize()
	if err != nil {
		pm.mu.Unlock()
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, fmt.Errorf("size metadata: %w", err)
	}
	if err := fn(candidate); err != nil {
		pm.mu.Unlock()
		h.debugf("metadata update failed project=%s step=apply elapsed=%s err=%v", project, h.config.Now().UTC().Sub(started), err)
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, err
	}
	// Op synthesis is deferred until after admission passes: appending
	// ops before a rejection would leave the rejected mutation's ops in the
	// shared stack and the journal, where a later rebase or crash replay
	// resurrects work that was never acknowledged.
	cause := causeFromMessage(message)
	admitNow := h.config.Now().Unix()
	candidate.Normalize(project, admitNow)
	candidate.RecomputeStats()
	// Admission, expressed for the split layout (version 5). The whole-tree
	// serialized size is a cheap upper bound (incremental counter, no
	// allocation-heavy encode): if the entire tree serializes under the
	// contents-API limit, every object (a strict subset) does too, so the
	// mutation is admitted without building the tree. Only when the size
	// breaches the limit do we pay for a BuildTree to find whether a SINGLE
	// object (one enormous directory) is the culprit; a tree that merely
	// exceeds the old blob ceiling but splits into small objects is admitted,
	// because the split removed that ceiling. Shrinks always stay open.
	afterSize, err := candidate.SerializedSize()
	if err != nil {
		pm.mu.Unlock()
		h.debugf("metadata update failed project=%s step=size elapsed=%s err=%v", project, h.config.Now().UTC().Sub(started), err)
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, fmt.Errorf("size metadata: %w", err)
	}
	if afterSize > maxMetadataBytes {
		shrinking := afterSize < beforeSize
		// Fail-fast: once the ceiling is armed, a growth mutation
		// can never commit - reject it here instead of paying the full
		// BuildTree + publish cycle on every trigger. Shrinks stay open so
		// the project can always fold back under the ceiling.
		if pm.sizeCapped && !shrinking {
			pm.mu.Unlock()
			h.debugf("metadata update rejected project=%s step=admission-capped bytes=%d elapsed=%s", project, afterSize, h.config.Now().UTC().Sub(started))
			logging.Error(h.projectLogger(project), "metadata update rejected: project is over the size ceiling; growth mutations fail fast", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "bytes", afterSize, "max", maxMetadataBytes)
			return nil, fmt.Errorf("metadata over size ceiling (%d bytes, max %d): growth is rejected until the tree fits again; delete entries or run `storhub prune`", afterSize, maxMetadataBytes)
		}
		oversizeObject := false
		if !shrinking {
			if res, berr := metadata.BuildTree(candidate); berr == nil {
				for _, obj := range res.Objects {
					if len(obj) > maxMetadataBytes {
						oversizeObject = true
						break
					}
				}
			}
		}
		if oversizeObject {
			pm.sizeCapped = true
			pm.mu.Unlock()
			h.debugf("metadata update rejected project=%s step=admission bytes=%d elapsed=%s", project, afterSize, h.config.Now().UTC().Sub(started))
			logging.Error(h.projectLogger(project), "metadata update rejected: single index object over ceiling", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "bytes", afterSize, "max", maxMetadataBytes)
			return nil, fmt.Errorf("metadata too large: one directory serializes past %d bytes; distribute entries across subdirectories or run PurgeUntracked to shrink", maxMetadataBytes)
		}
		// The tree exceeds the old blob ceiling but splits into small
		// objects: admitted under the split layout.
		pm.sizeCapped = false
	} else {
		pm.sizeCapped = false
	}
	// Op synthesis: diff the pre-transaction tree against the (normalized,
	// admitted) candidate so every transaction-level mutation (fs/posix
	// ops, prune, release catalog changes) lands in the op stack - rich
	// commit messages, the crash-recovery journal, and rebase all read
	// from it. Runs only after admission: a rejected mutation leaves the
	// shared stack and journal untouched.
	//
	// NEEDS-INTEGRATION(11): intent-based op synthesis (the caller declaring
	// what it changed) would replace this full before/after diff; until then
	// the diff is the only way a generic fn's effect is captured.
	for _, op := range synthesizeOpsFromDiff(pm.meta, candidate, cause, h.config.Now().Unix()) {
		h.appendOpLocked(project, pm, op)
	}
	// Publish: candidate was normalized (indexes rebuilt) and is private, so
	// the swap is the atomic publish point. No second rebuild needed.
	pm.meta = candidate

	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	// Trigger the commit loop to wake up immediately
	select {
	case trigger <- struct{}{}:
	default:
	}

	h.debugf("metadata update complete project=%s elapsed=%s", project, h.config.Now().UTC().Sub(started))
	logging.Debug(h.projectLogger(project), "metadata update complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started))

	// Return a snapshot Clone, never the live pointer: callers mutating the
	// result must not corrupt the hub's in-memory truth (or the pending
	// batch) behind pm.mu's back. Pinned by TestUpdateRepoMetadataReturnsClone.
	pm.mu.RLock()
	out := pm.meta.Clone()
	pm.mu.RUnlock()
	out.RebuildIndexes()
	return &out, nil
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

func (h *StorHub) GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *metadata.RepoMetadata, requiredSize int) (string, string, error) {
	return h.getOrCreateUploadRelease(ctx, project, repoMeta, requiredSize)
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

// fsService/posixService return the hub's single cached services. They are
// stateless wrappers over the hub, so allocating a fresh one per call was
// pure GC churn on every fs/posix verb.
func (h *StorHub) fsService() *shfs.Service {
	return h.fsSvc
}

func (h *StorHub) posixService() *implposix.Service {
	return h.posixSvc
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

func (h *StorHub) RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...shfs.MutateOption) error {
	return h.fsService().RenameContext(ctx, project, oldPath, newPath, opts...)
}

func (h *StorHub) Copy(project, srcPath, dstPath string) error {
	return h.CopyContext(context.Background(), project, srcPath, dstPath)
}

func (h *StorHub) CopyContext(ctx context.Context, project, srcPath, dstPath string) error {
	return h.fsService().CopyContext(ctx, project, srcPath, dstPath)
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
	if err := shfs.ValidateAccessPathShape(filePath); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, errors.New("read offset and length must be non-negative")
	}
	repo, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return 0, err
	}
	cleanPath, traversed, err := shfs.ResolveAccessPath(repo, filePath, true)
	if err != nil {
		return 0, err
	}
	if cleanPath == "" {
		return 0, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repo, traversed); err != nil {
		return 0, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return 0, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanPath)
	}
	if err := shfs.CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return 0, err
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
	segments := overlappingFileSegments(file, repo.Chunks(), offset, end)
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

func (h *StorHub) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte, mode ...shfs.XAttrMode) error {
	return h.posixService().SetXAttrContext(ctx, project, targetPath, attr, data, mode...)
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
