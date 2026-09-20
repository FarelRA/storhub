package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	ghapi "github.com/FarelRA/storhub/internal/github"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

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
	// lastAccess is the idle-evict clock. It is an atomic UnixNano
	// so every cache hit can bump it without taking pm.mu exclusive:
	// the old per-op pm.mu.Lock just to touch a timestamp serialized
	// all same-project readers behind writers. Loads/stores use
	// h.config.Now(); zero means "never accessed".
	lastAccess atomic.Int64
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
	// recent is the bounded ring of namespace paths each local publish
	// touched, newest last, for cross-surface invalidation fan-out (see
	// notePublishedPathsLocked). A nil paths entry means unknown scope
	// (remote-truth swap, rebase adopt): consumers invalidate broadly.
	// fanoutSeq is the ring's dedicated cursor, bumped once per recorded
	// publish. Guarded by mu.
	recent    []pathVersion
	fanoutSeq uint64
	// objectCount is the running total of index objects written for this
	// project (from the manifest), used for the history-accumulation
	// threshold warning. Guarded by mu.
	objectCount uint64
	// historyWarned records that the object-count threshold warning has
	// fired for the current crossing, so it logs once per window rather
	// than on every commit. Guarded by mu.
	historyWarned bool
	// treeCache carries per-object Merkle build state across commits so
	// an unchanged subtree is neither re-marshalled nor re-emitted
	// (BuildTreeStream). It is valid only for the tree it was built from
	// plus the caller's retained object store; guarded by mu, never
	// shared across projects or goroutines. Bounded by entry count
	// (shas, not bytes): when the node+bucket census exceeds
	// treeCacheMaxEntries the cache resets instead of growing with the
	// tree. See treeCacheFor.
	treeCache *metadata.TreeCache
}

// releaseCacheTTL bounds how stale a cached release list may be before the
// upload picker refetches. CAS/rebase safety makes brief staleness harmless;
// the cache exists to dodge per-upload ListReleases secondary rate limits.
const releaseCacheTTL = 12 * storcfg.PatienceUnit

type releaseCacheEntry struct {
	releases []ghapi.Release
	// fetchedAt anchors the TTL; readers treat an entry
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
// entry reports a miss so the caller refetches; stale entries for
// untouched projects simply sit until refetch overwrites them.
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

// copyReleases is the single home of release-list copying. slim=true keeps
// only the fields the upload picker reads (tag, upload URL, release ID,
// each asset's ID) and drops per-asset Name/Size strings; slim=false
// deep-copies everything. A shallow struct copy would alias the Assets
// backing arrays, letting any caller that mutates a fetched Release
// corrupt the cache.
func copyReleases(in []ghapi.Release, slim bool) []ghapi.Release {
	if in == nil {
		return nil
	}
	out := make([]ghapi.Release, len(in))
	for i, r := range in {
		out[i] = r
		if r.Assets != nil {
			assets := make([]ghapi.Asset, len(r.Assets))
			if slim {
				for j, a := range r.Assets {
					assets[j] = ghapi.Asset{ID: a.ID}
				}
			} else {
				copy(assets, r.Assets)
			}
			out[i].Assets = assets
		}
	}
	return out
}

// slimReleases copies a release list for cache residency, keeping only the
// fields the upload picker reads (tag, upload URL, release ID, and each
// asset's ID) and dropping the per-asset Name/Size strings. A project with
// 100k cached assets no longer pins 100k name strings; the picker's capacity
// math (len + ID + placeholder detection) is unaffected.
func slimReleases(in []ghapi.Release) []ghapi.Release {
	return copyReleases(in, true)
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
	return copyReleases(in, false)
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
	cp := copyReleases([]ghapi.Release{*r}, true)
	out := cp[0]
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

// maxPendingOpsPerProject bounds one project's pending op stack. The stack
// coalesces per path, so exceeding this means a project accumulated thousands
// of distinct changed paths while commits kept failing: the crossing
// mutation force-retries the commit via the live trigger (ops can never be
// dropped - they are the acknowledged mutations), and the residency cap's
// backpressure stops new projects from piling on.
const maxPendingOpsPerProject = 4096

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
	// Thread the hub's injectable clock into the git backend so tests
	// can freeze time (audit 34): wall time.Now() made commit ordering
	// diverge from the mock's logical clock. Nil means time.Now.
	r.now = h.config.Now
	h.gitRepos[project] = r
	return r
}

// touchLastAccess bumps the idle-evict clock without taking pm.mu.
// The atomic keeps per-op hits off the exclusive lock (audit 33).
func touchLastAccess(pm *projectMetadata, now time.Time) {
	if pm == nil || now.IsZero() {
		return
	}
	pm.lastAccess.Store(now.UnixNano())
}

// lastAccessTime loads the idle stamp (zero when never accessed).
func lastAccessTime(pm *projectMetadata) time.Time {
	if pm == nil {
		return time.Time{}
	}
	nano := pm.lastAccess.Load()
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano).UTC()
}

// lookupOrInsert is the single double-checked-lock core behind
// getOrCreateProjectMeta (read paths, admit unbounded) and
// getOrCreateProjectMetaAdmitted (mutation paths, backpressure when at
// cap with nothing evictable). admit=false inserts regardless (fresh
// entries are clean, hence evictable, so growth self-limits);
// admit=true refuses a brand-new project when the residency cap cannot
// admit it.
//
// Idle expiry (Phase E2, replacing the sweeper tick): a hit whose idle
// clock passed metaCacheIdleTTL falls through to the slow path, which
// evicts the entry while it is still clean and idle and inserts a fresh
// one. Touched, dirty, reviving, or concurrently replaced entries are
// served live; eviction never drops unpushed work.
func (h *StorHub) lookupOrInsert(project string, admit bool) (*projectMetadata, error) {
	h.metaMu.RLock()
	pm, exists := h.metaCache[project]
	h.metaMu.RUnlock()
	if exists {
		now := h.config.Now()
		if !h.idleExpired(pm, now) {
			touchLastAccess(pm, now)
			return pm, nil
		}
		// Possibly idle-expired: the slow path revalidates under the
		// write lock and evicts only while the entry is still clean.
	}
	h.metaMu.Lock()
	var evicted []string
	if pm, exists = h.metaCache[project]; exists {
		if h.idleExpired(pm, h.config.Now()) {
			if name, ok := h.evictIdleEntryLocked(project); ok {
				evicted = append(evicted, name)
				exists = false
			}
		}
		if exists {
			h.metaMu.Unlock()
			touchLastAccess(pm, h.config.Now())
			return pm, nil
		}
		h.metaMu.Unlock()
		h.releaseEvicted(evicted)
		h.metaMu.Lock()
		// Re-check: the residue release above ran without the lock, so
		// a concurrent insert may have recreated the entry meanwhile.
		if pm, exists = h.metaCache[project]; exists {
			h.metaMu.Unlock()
			touchLastAccess(pm, h.config.Now())
			return pm, nil
		}
	}
	now := h.config.Now()
	// Enforce the residency cap before adding another entry: growth is an
	// event, so the cap is applied exactly when a new project joins. Read
	// paths admit unbounded (a freshly loaded entry is clean and therefore
	// evictable); mutation entry points use admit=true for backpressure.
	// DELIBERATE CONTRACT (audit 31, test-pinned): an all-dirty cache
	// inserts unbounded on the read path; hard-capping reads too was
	// EXCLUDED — change nothing here without revisiting
	// eventdriven_test.go:347.
	admitted, capEvicted := h.evictForCapacityLocked()
	evicted = append(evicted, capEvicted...)
	if admit && !admitted {
		h.metaMu.Unlock()
		h.releaseEvicted(evicted)
		return nil, fmt.Errorf("too many tracked projects with unpushed metadata (cap %d): flush or retry before mutating a new project", h.config.MaxTrackedProjects)
	}
	_ = admitted // read path inserts regardless; fresh entries are evictable
	pm = h.newProjectMetaLocked(project, now)
	h.metaMu.Unlock()
	h.releaseEvicted(evicted)
	return pm, nil
}

// idleExpired reports whether pm's idle clock passed metaCacheIdleTTL: a
// clean entry nobody touched for the TTL is evictable on the get path
// (Phase E2 replaces the sweeper tick). A zero stamp means never accessed
// and never expires. now is the caller's clock read, reused for the touch
// so the fast path pays a single Now per hit.
func (h *StorHub) idleExpired(pm *projectMetadata, now time.Time) bool {
	last := lastAccessTime(pm)
	if last.IsZero() {
		return false
	}
	return now.Sub(last) > metaCacheIdleTTL
}

// evictIdleEntryLocked drops the resident entry for project while it is
// still clean and idle-past-the-TTL. Caller holds metaMu for writing.
// The TTL is re-read under the write lock and evictEntryLocked rechecks
// clean under pm.mu, so a use that raced the fast-path stamp read is
// served live instead. Returns the evicted name for cascade via
// releaseEvicted after metaMu is dropped.
func (h *StorHub) evictIdleEntryLocked(project string) (string, bool) {
	pm, ok := h.metaCache[project]
	if !ok {
		return "", false
	}
	if !h.idleExpired(pm, h.config.Now()) {
		return "", false
	}
	if !evictEntryLocked(h, project, pm) {
		return "", false
	}
	return project, true
}

// getOrCreateProjectMeta returns the projectMetadata for a project, creating it if needed
func (h *StorHub) getOrCreateProjectMeta(project string) *projectMetadata {
	pm, _ := h.lookupOrInsert(project, false)
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
	return h.lookupOrInsert(project, true)
}

// newProjectMetaLocked creates the entry, inserts it, and starts its commit
// loop (skipped once Shutdown began - the drain covers dirty state).
// Caller holds metaMu for writing.
func (h *StorHub) newProjectMetaLocked(project string, now time.Time) *projectMetadata {
	pm := &projectMetadata{
		meta:      metadata.NewRepoMetadata(project),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
		triggerCh: make(chan struct{}, 1),
	}
	pm.lastAccess.Store(now.UnixNano())
	h.metaCache[project] = pm
	h.startCommitLoopLocked(project, pm)
	return pm
}

// evictEntryLocked stops a project's loop and removes it from the cache.
// Caller holds metaMu for writing and pm.mu is taken here; the shared
// close/stop/delete block was duplicated in evictForCapacityLocked and
// sweepCachesOnce. Returns true when the entry was evictable and removed.
func evictEntryLocked(h *StorHub, name string, pm *projectMetadata) bool {
	pm.mu.Lock()
	evictable := !pm.dirty && !pm.reviving && !pm.stopped
	if evictable {
		pm.stopped = true
		close(pm.stopCh)
		delete(h.metaCache, name)
	}
	pm.mu.Unlock()
	return evictable
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
		pm.mu.RUnlock()
		if eligible {
			cands = append(cands, candidate{name: name, lastAccess: lastAccessTime(pm)})
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
		if evictEntryLocked(h, cand.name, pm) {
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

// Cache residency policy (Phase E2: no periodic goroutine; every leg is
// event-driven): a clean project's metadata is dropped after
// metaCacheIdleTTL without access, enforced on the lookupOrInsert get
// path (the lastAccess stamp was already recorded; the get path reads it
// instead of a tick). A cached release list older than releaseCacheTTL
// reports a miss on read (cachedReleasesView); projects nobody touches
// need no eviction, so no drop pass exists. A dirty project whose pending
// stack crosses a residency bound gets its commit force-retried by the
// crossing mutation itself (appendOpLocked pokes the live triggerCh under
// the pm.mu hold every caller already takes). Memory stays capped by
// MaxTrackedProjects either way, so worst case without any timer is
// bounded residency, not growth.
const metaCacheIdleTTL = 360 * storcfg.PatienceUnit

// sweepCachesOnce is the manual cache-drain backstop, kept as a callable
// for tests and shutdown paths: it TTL-evicts idle clean metadata entries
// (cascading their residue) and pokes a commit retry for dirty projects
// whose op stack outgrew the residency cap. No timer calls it; the live
// legs moved to lookupOrInsert (idle evict) and appendOpLocked
// (cap-cross poke). The release-list drop leg is deleted outright: the
// read path already treats older-than-TTL as a miss, and projects nobody
// touches need no eviction.
//
// Lock discipline (audit 33): the candidate snapshot is taken under
// metaMu.RLock; per-project decisions run after the global lock is
// dropped so a contended pm.mu never stalls all cache misses/inserts.
// Eviction re-takes metaMu for writing and re-validates under pm.mu.
// Force-flush pokes re-read the CURRENT triggerCh under pm.mu: the
// channel captured before unlock may be stale after a
// markProjectDirtyLiveLocked revival swap (caches.go vs commit.go), and
// a stale send wakes nobody.
func (h *StorHub) sweepCachesOnce() {
	now := h.config.Now()

	type snapshot struct {
		name string
		pm   *projectMetadata
	}
	h.metaMu.RLock()
	snap := make([]snapshot, 0, len(h.metaCache))
	for name, pm := range h.metaCache {
		snap = append(snap, snapshot{name: name, pm: pm})
	}
	h.metaMu.RUnlock()

	var evicted []string
	for _, s := range snap {
		s.pm.mu.RLock()
		idle := !s.pm.dirty && !s.pm.reviving && !s.pm.stopped
		last := lastAccessTime(s.pm)
		needFlush := s.pm.dirty && s.pm.opStack.needsForceFlush()
		s.pm.mu.RUnlock()
		idleClean := idle && !last.IsZero() && now.Sub(last) > metaCacheIdleTTL
		if idleClean {
			h.metaMu.Lock()
			// Re-validate under both locks: the entry may have been
			// mutated, revived, or evicted since the snapshot.
			if cur, ok := h.metaCache[s.name]; ok && cur == s.pm {
				if evictEntryLocked(h, s.name, s.pm) {
					evicted = append(evicted, s.name)
					h.metaMu.Unlock()
					continue
				}
			}
			h.metaMu.Unlock()
		}
		if needFlush {
			// Re-read the live trigger under pm.mu; a revival may have
			// swapped it since the snapshot. The non-blocking send
			// holds pm.mu, which is safe (never blocks).
			s.pm.mu.Lock()
			stale := s.pm.stopped || s.pm.reviving
			trigger := s.pm.triggerCh
			stillDirty := s.pm.dirty && len(s.pm.opStack.ops) >= maxPendingOpsPerProject
			if !stale && stillDirty {
				h.pressure.noteForceRetry()
				// Retry the failing commit; the stack can never be dropped
				// (acknowledged mutations), only pushed.
				select {
				case trigger <- struct{}{}:
				default:
				}
			}
			s.pm.mu.Unlock()
		}
	}
	h.releaseEvicted(evicted)
}

// treeCacheMaxEntries bounds the per-project Merkle build cache by entry
// count (shas, not bytes): one entry per directory node + one per chunk
// bucket + one releases object. A 16k-entry tree is already far past any
// realistic single-commit working set; exceeding it resets the cache
// (costing re-marshal, never correctness).
const treeCacheMaxEntries = 16384
