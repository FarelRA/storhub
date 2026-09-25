package storage

import (
	"context"
	"errors"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	"github.com/go-git/go-git/v6/plumbing"
	gogitobject "github.com/go-git/go-git/v6/plumbing/object"
	"log/slog"
	"os"
	"strings"
)

const metadataFilePath = ".storhub/metadata.json"

func (h *StorHub) ensureRepo(ctx context.Context, project string) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	exists, err := h.repoExists(ctx, project)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := h.gh.CreateRepo(ctx, project, h.config.RepoDescription, !h.config.CreatePublicRepo, true); err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == 422 && isRepoAlreadyExistsError(apiErr) {
			exists, existsErr := h.repoExists(ctx, project)
			if existsErr == nil && exists {
				h.setRepoState(project, true)
				return nil
			}
		}
		return fmt.Errorf("ensure repository: %w", err)
	}
	h.setRepoState(project, true)
	return nil
}

func (h *StorHub) repoExists(ctx context.Context, project string) (bool, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return false, err
	}
	h.repoMu.RLock()
	exists, ok := h.repoState[project]
	h.repoMu.RUnlock()
	if ok {
		return exists, nil
	}
	exists, err := h.gh.RepoExists(ctx, h.owner, project)
	if err != nil {
		return false, err
	}
	h.setRepoState(project, exists)
	return exists, nil
}

// deleteRepo deletes the remote repository and drops all hub-local state
// for the project: the commit loop stops, the metadata cache entry goes
// (cascading residue if resident), then the cascade runs unconditionally
// so no gitRepos/objCaches/repoState/releaseCache entry lingers even when
// the project was never resident in metaCache. releaseProjectResidue is
// idempotent, so the double call is safe.
func (h *StorHub) deleteRepo(ctx context.Context, project string) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	if err := h.gh.DeleteRepo(ctx, h.owner, project); err != nil {
		return err
	}
	h.invalidateRepoMetadata(project)
	h.releaseProjectResidue(project)
	return nil
}

func (h *StorHub) getAuthenticatedUser(ctx context.Context) (string, error) {
	return h.gh.GetAuthenticatedUser(ctx)
}

func (h *StorHub) loadRepoMetadata(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if meta, sha, ok := h.cachedRepoMetadata(project); ok {
		return meta, sha, nil
	}
	return h.loadRepoMetadataFresh(ctx, project)
}

func (h *StorHub) loadRepoMetadataReadonly(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if meta, sha, ok := h.cachedRepoMetadataReadonly(project); ok {
		return meta, sha, nil
	}
	return h.loadRepoMetadataFresh(ctx, project)
}

func (h *StorHub) loadRepoMetadataFresh(ctx context.Context, project string) (*RepoMetadata, string, error) {
	// Single-flight: concurrent cold-cache misses for one project join
	// the owner's remote load instead of each paying it. Fresh loads are
	// the only uncached remote reads left on hot paths (validation
	// probes, hydration, rollback pins), and without coalescing a burst
	// of N arrivals costs N full reloads. A canceled waiter stops
	// waiting; the flight continues for the rest.
	h.flightMu.Lock()
	if f, ok := h.flights[project]; ok {
		h.flightMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-f.done:
			return f.meta, f.sha, f.err
		}
	}
	f := &loadFlight{done: make(chan struct{})}
	h.flights[project] = f
	h.flightMu.Unlock()

	f.meta, f.sha, f.err = h.loadRepoMetadataFreshUnshared(ctx, project)

	h.flightMu.Lock()
	delete(h.flights, project)
	h.flightMu.Unlock()
	close(f.done)
	return f.meta, f.sha, f.err
}

// loadFlight is one in-flight fresh metadata load shared by every
// goroutine that missed the cache for the project while it ran.
type loadFlight struct {
	done chan struct{}
	meta *RepoMetadata
	sha  string
	err  error
}

func (h *StorHub) loadRepoMetadataFreshUnshared(ctx context.Context, project string) (*RepoMetadata, string, error) {
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "load metadata start")
	data, sha, found, err := h.readIndexHead(ctx, project)
	if err != nil {
		logging.Error(h.projectLogger(project), "load metadata failed", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, "", err
	}
	if !found {
		exists, existsErr := h.repoExists(ctx, project)
		if existsErr != nil {
			return nil, "", existsErr
		}
		if !exists {
			return nil, "", shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		// Brand-new (or wiped) project: start on the split layout (version 5).
		m := NewRepoMetadata(project)
		pendingOps := h.journalReplayForLoad(project, m)
		h.storeRepoMetadata(project, m, "", pendingOps, 0)
		// The returned tree may be published directly (hydration swaps it
		// into pm.meta); journal replay invalidates the indexes, so rebuild
		// before handing it out.
		m.RebuildIndexes()
		logging.Info(h.projectLogger(project), "load metadata initialized empty repository metadata", "elapsed", h.config.Now().UTC().Sub(started))
		return m, "", nil
	}
	m, objectCount, err := h.loadIndexTree(ctx, project, data)
	if err != nil {
		logging.Error(h.projectLogger(project), "load metadata failed", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, "", err
	}
	pendingOps := h.journalReplayForLoad(project, m)
	// NOTE: sha is the CAS token for the document that was found (manifest
	// blob sha for a split project, metadata blob sha for a legacy one). It
	// must never be consumed as a git ref: pins capture chunk layouts instead.
	h.storeRepoMetadata(project, m, sha, pendingOps, objectCount)
	m.RebuildIndexes()
	logging.Debug(h.projectLogger(project), "load metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "sha", shortSHA(sha), "bytes", len(data), "split", m.IsSplit())
	return m, sha, nil
}

func (h *StorHub) commitRepoMetadata(ctx context.Context, project string, metadata *RepoMetadata, previousSHA, message string) (string, string, error) {
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "commit metadata start", "message", message, "previous_sha", shortSHA(previousSHA))
	if err := h.ensureOwner(ctx); err != nil {
		return "", "", err
	}
	metadata.Normalize(project, h.config.Now().UnixNano())
	metadata.LastMod = h.config.Now().UnixNano()
	metadata.RecomputeStats()
	if err := metadata.Validate(); err != nil {
		return "", "", fmt.Errorf("validate metadata: %w", err)
	}
	// The split index (version 5) is the only write layout: rollback and
	// cleanup republish the manifest, re-pointing the index at this tree's
	// objects (rollback-as-revert, no force-push). A legacy project's first
	// such commit also migrates it to the split layout.
	metadata.MarkSplit()
	var objectCount uint64
	if pm := h.lookupProjectMeta(project); pm != nil {
		pm.mu.RLock()
		objectCount = pm.objectCount
		pm.mu.RUnlock()
	}
	commitSHA, contentSHA, newCount, err := h.publishIndex(ctx, project, metadata, previousSHA, message, objectCount)
	if err != nil {
		logging.Error(h.projectLogger(project), "commit metadata failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return "", "", err
	}
	h.storeRepoMetadata(project, metadata, contentSHA, nil, newCount)
	h.clearSizeCapped(project)
	logging.Debug(h.projectLogger(project), "commit metadata complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "commit_sha", shortSHA(commitSHA), "content_sha", shortSHA(contentSHA), "objects", newCount)
	return commitSHA, contentSHA, nil
}

func (h *StorHub) setRepoState(project string, exists bool) {
	h.repoMu.Lock()
	defer h.repoMu.Unlock()
	h.repoState[project] = exists
}

// forgetRepoState drops the cached existence bool for a project that no
// longer exists, so a deleted project's entry cannot linger forever.
func (h *StorHub) forgetRepoState(project string) {
	h.repoMu.Lock()
	defer h.repoMu.Unlock()
	delete(h.repoState, project)
}

// releaseProjectResidue tears down every per-project map entry besides the
// metadata cache: the git mirror handle (closing the *git.Repository and
// removing its claimed cache dir), the object cache handle, the repo-state
// bool, and the release list. Eviction and project deletion must cascade
// here or a long-lived server leaks one heavy entry per create/delete churn.
//
// It takes each map's own lock and never metaMu, so callers may hold metaMu
// (eviction) or no lock at all (deleteRepo) without inverting lock order.
func (h *StorHub) releaseProjectResidue(project string) {
	h.gitMu.Lock()
	repo := h.gitRepos[project]
	delete(h.gitRepos, project)
	h.gitMu.Unlock()
	if repo != nil {
		if err := repo.release(true); err != nil {
			logging.Warn(h.projectLogger(project), "release git mirror on eviction failed", "err", err)
		}
	}
	// Free the in-memory handle first, then remove its disk dir best-effort
	// (size-ceiling plus object-cache residency): evicted/deleted projects previously accumulated
	// objects/<owner__proj>/ dirs until the next process-start
	// ReapOrphanedCaches, unbounded across churn despite the per-project
	// count+byte caps. I/O runs after the map lock, matching the git
	// mirror above. A concurrent objectCacheFor may recreate the dir;
	// RemoveAll on a live path is still safe (cache misses refetch).
	h.objCacheMu.Lock()
	objCache := h.objCaches[project]
	delete(h.objCaches, project)
	h.objCacheMu.Unlock()
	if objCache != nil {
		// Best-effort: eviction must not fail on a wedged disk.
		_ = os.RemoveAll(objCache.dir)
	}
	h.forgetRepoState(project)
	h.invalidateReleaseCache(project)
	// Drop the cached per-project logger too, so project churn cannot grow
	// the logger cache without bound.
	h.loggers.Delete(project)
	// Close the op-journal handle/file: eviction must not leak one open FD
	// plus map entries per churned project until Shutdown.
	h.closeProjectJournal(project)
}

// Load-path map (two cores, thin adapters): every load funnels through
// cachedMeta (shared-pointer cache read) or loadRepoMetadataFreshUnshared
// (remote load + store). The four named entries each carry a distinct
// contract worth keeping: loadRepoMetadata serves any resident entry,
// loadRepoMetadataReadonly misses on unhydrated entries whose empty tree
// is not remote truth, loadRepoMetadataFresh coalesces concurrent cold
// misses onto one flight, and loadRepoMetadataFreshUnshared is the
// uncoalesced remote read. Collapse them only with a flags argument that
// preserves all four contracts; the names stay until every caller agrees
// on the flags.

// cachedMeta is the single home of the shared-pointer cache read:
// requireHydrated=false serves any resident entry (loadRepoMetadata path),
// requireHydrated=true misses on unhydrated entries whose EMPTY tree is not
// remote truth (loadRepoMetadataReadonly path). cachedRepoMetadata and
// cachedRepoMetadataReadonly are thin wrappers (verbs.go calls both).
func (h *StorHub) cachedMeta(project string, requireHydrated bool) (*RepoMetadata, string, bool) {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return nil, "", false
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if requireHydrated && !pm.hydrated {
		// An unhydrated entry carries an EMPTY tree that is not remote
		// truth; serving it lets a cold-cache mutation commit over real
		// remote state. Miss instead: the caller falls through to a
		// fresh load.
		return nil, "", false
	}
	// Share the immutable current pointer: published trees are never
	// mutated in place (cloneForWrite/publishTreeLocked discipline), so a
	// reader holding the pointer after releasing the lock sees a frozen
	// snapshot. Cloning here was a full deep copy per read (O(tree)
	// allocations); the indexes were built at store/publish time and
	// stay valid.
	return pm.meta, pm.sha, true
}

func (h *StorHub) cachedRepoMetadata(project string) (*RepoMetadata, string, bool) {
	return h.cachedMeta(project, false)
}

func (h *StorHub) cachedRepoMetadataReadonly(project string) (*RepoMetadata, string, bool) {
	return h.cachedMeta(project, true)
}

// cloneForWrite returns a private, mutable copy of a published metadata tree.
//
// Published trees (pm.meta) are shared with lock-free readers, so no code
// may mutate one in place. Mutation sites take a copy here, apply their
// changes, and publish with publishTreeLocked. The copy is the metadata
// engine's Clone: the four stored maps are copied while their immutable
// entry VALUES are shared, and a clean derived index is shared read-only, so
// the copy is cheap. The first tracked mutation drops the copy to a private
// dirty derived state (the engine's owner/mapsShared guard), never writing
// into maps the published tree still reads.
func cloneForWrite(m *RepoMetadata) *RepoMetadata {
	return m.Clone()
}

// ProjectVersion reports the per-project metadata version counter, the
// cross-surface invalidation source: every swap of shared truth
// (publishTreeLocked callers via markProjectDirtyLocked, the
// storeRepoMetadata apply-back branches) advances it, while paths that
// replace nothing leave it alone. A subscriber (FUSE) baselines the value
// and treats any movement as "kernel-cached entries for this project may
// be stale", then revalidates and invalidates exactly the affected
// entries. ok=false means the project is not resident: no counter exists,
// so the caller falls back to timeout expiry.
//
// Lock discipline: metaMu for the map lookup, then pm.mu for reading,
// the same order as every other reader. The swap side always advances the
// counter under pm.mu for writing, so this read never races a publish.
func (h *StorHub) ProjectVersion(project string) (uint64, bool) {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return 0, false
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.version, true
}

// maxRecentPaths bounds the per-project publish ring for invalidation
// fan-out: with 64 entries a 1s poller survives a 64-publish burst
// without losing scope; older entries evict, and a consumer whose
// baseline predates the oldest retained entry gets unknown-scope (safe,
// invalidate broadly) instead of a silent miss.

// notePublishedPathsLocked records one publish's footprint in the ring.
// Caller holds pm.mu for writing (it runs inside publishTreeLocked's
// contract, or another spot holding pm.mu across the swap). Nil paths
// marks unknown scope.
func notePublishedPathsLocked(pm *projectMetadata, paths []string) {
	pm.fanoutSeq++
	pm.recent = append(pm.recent, pathVersion{seq: pm.fanoutSeq, paths: paths})
	if len(pm.recent) > maxRecentPaths {
		pm.recent = append([]pathVersion(nil), pm.recent[len(pm.recent)-maxRecentPaths:]...)
	}
	// Push harvest: snapshot this publish's subscriber pokes under the
	// same mu hold that guards the ring, then poke without holding pm.mu.
	// The hop is a fresh goroutine per publish that has subscribers, so a
	// slow or backpressured subscriber never stalls the publisher and no
	// synchronous kernel write ever runs under storage locks; the fuse
	// side funnels delivery through its async notify slots.
	if cbs := fanoutSnapshotLocked(pm); len(cbs) > 0 {
		go func() {
			for _, cb := range cbs {
				cb()
			}
		}()
	}
}

// PublishedPathsSince returns the namespace paths published after the
// since cursor, plus the current cursor for the next call. unknown
// reports that scope was lost (unknown-scope entry in range, ring
// overflow past the baseline, or project not resident): the caller must
// invalidate broadly instead of trusting the path list. Empty paths with
// unknown=false means nothing changed.

// storeRepoMetadata caches remote truth for a project. The tree's own version
// records its layout (split vs legacy) and objectCount carries the running
// hint. When pendingOps is non-nil (a crash-recovery journal replayed onto the
// loaded state), the ops become the project's pending stack: dirty stays set
// and the journal is kept until the next commit lands them.
//
// The apply-back is version-guarded: when the entry still carries local
// work (dirty, or a non-empty op stack), remote truth must NOT clobber it -
// discarding acknowledged mutations, the pending stack, and the crash journal
// here is data loss. The loader still receives the freshly read values; the
// local tree keeps its stale CAS token, so the next commit conflicts and
// rebases onto the remote state instead of silently overwriting it. Shared
// state that is replaced bumps pm.version so an in-flight transaction's
// version guard observes the swap.
func (h *StorHub) storeRepoMetadata(project string, meta *RepoMetadata, sha string, pendingOps []Op, objectCount uint64) {
	clone := meta.Clone()
	clone.RebuildIndexes()

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if len(pendingOps) > 0 {
		pm.meta = clone
		pm.sha = sha
		pm.hydrated = true
		pm.objectCount = objectCount
		// The rebase baseline moves to the freshly loaded state.
		pm.baseTree = clone
		pm.opStack.clear()
		for _, op := range pendingOps {
			pm.opStack.append(op)
		}
		pm.dirty = true
		pm.version++
		// Remote-truth swap with local pending ops: whole-tree content
		// arrives with unknown namespace scope for fan-out purposes.
		notePublishedPathsLocked(pm, nil)
		pm.mu.Unlock()
		return
	}
	if pm.dirty || len(pm.opStack.ops) > 0 {
		// Pending local work outranks the remote snapshot: keep the tree,
		// the stack, the dirty flag, and the journal exactly as they are.
		pm.hydrated = true
		pm.mu.Unlock()
		return
	}
	pm.meta = clone
	pm.sha = sha
	pm.hydrated = true
	pm.objectCount = objectCount
	// The rebase baseline moves to the freshly loaded state.
	pm.baseTree = clone
	pm.dirty = false // Just stored, so not dirty
	pm.opStack.clear()
	pm.version++
	// Clean remote-truth swap: same unknown-scope treatment (a fresh
	// load can move any entry).
	notePublishedPathsLocked(pm, nil)
	pm.mu.Unlock()
	h.journalRewrite(project, nil)
}

// journalReplayForLoad replays the crash-recovery journal onto a freshly
// loaded remote state, but only for a cache entry that has never been
// hydrated (cold start). A hydrated project with pending ops is in its
// normal commit cycle; replaying there would resurrect discarded state.
// Returns the replayed ops for the pending stack, or nil.
func (h *StorHub) journalReplayForLoad(project string, meta *RepoMetadata) []Op {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if ok {
		pm.mu.RLock()
		cold := !pm.hydrated
		pm.mu.RUnlock()
		if !cold {
			return nil
		}
	}
	ops := h.journalRead(project)
	if len(ops) == 0 {
		return nil
	}
	ops = dropSupersededOps(project, h.projectLogger(project), meta, ops)
	if len(ops) == 0 {
		return nil
	}
	if err := applyOps(meta, ops); err != nil {
		logging.Error(h.projectLogger(project), "op journal replay failed; pending ops discarded", "err", err)
		h.journalRewrite(project, nil)
		return nil
	}
	meta.Normalize(project, h.config.Now().UnixNano())
	meta.RecomputeStats()
	logging.Info(h.projectLogger(project), "op journal replayed onto remote state", "ops", len(ops))
	return ops
}

// dropSupersededOps removes journal ops that a full-state assertion must not
// re-apply over newer remote state: a journal rewrite that failed after
// a commit leaves committed ops in the file, and a cold replay would then
// assert them over entries the world has since moved past. An op whose
// timestamp predates the change time of the entry it targets is stale and is
// dropped; renames consult renameSupersededByUpstream (rebase.go: a stale
// rename would clobber a newer target unconditionally at apply time);
// catalog ops without a resolvable target are kept - replay applies them
// defensively.
func dropSupersededOps(project string, logger *slog.Logger, meta *RepoMetadata, ops []Op) []Op {
	kept := make([]Op, 0, len(ops))
	for _, op := range ops {
		path := opPath(op)
		var changedAt int64
		var exists bool
		switch {
		case op.Type == OpMkdir || (op.Type == OpSetattr && op.File == nil && op.Dir != nil):
			if d, ok := meta.Dirs()[path]; ok {
				changedAt, exists = max(d.ChangedAt, d.ModifiedAt), true
			}
		case isStateClass(op.Type) || isDeleteClass(op.Type):
			if f, ok := meta.Files()[path]; ok {
				changedAt, exists = f.ChangedAt, true
			}
		default:
			// Renames overwrite their target unconditionally at apply
			// time, so a stale one is destructive (unlike single-target
			// state ops, which the timestamp guard already drops).
			if op.Type == OpRename && renameSupersededByUpstream(meta, op) {
				to := ""
				if len(op.Paths) == 2 {
					to = op.Paths[1]
				}
				logging.Debug(logger, "op journal rename superseded by newer remote state; skipped",
					"project", project, "op", op.Type, "to", to, "op_ts", op.Timestamp)
				continue
			}
			kept = append(kept, op)
			continue
		}
		if exists && changedAt > op.Timestamp {
			logging.Debug(logger, "op journal entry superseded by newer remote state; skipped",
				"project", project, "op", op.Type, "path", path, "op_ts", op.Timestamp, "remote_ts", changedAt)
			continue
		}
		kept = append(kept, op)
	}
	return kept
}

func (h *StorHub) invalidateRepoMetadata(project string) {
	h.metaMu.Lock()
	pm, ok := h.metaCache[project]
	if ok {
		delete(h.metaCache, project)
		// Teardown mirrors eviction: without stopping the loop, deleting
		// the cache entry would leak a live commit goroutine per deleted
		// repo (and a duplicate loop if the project returns). metaMu→pm.mu
		// ordering matches getOrCreateProjectMeta and eviction.
		pm.mu.Lock()
		pm.stopped = true
		close(pm.stopCh)
		pm.mu.Unlock()
	}
	h.metaMu.Unlock()
	if ok {
		// Cascade outside metaMu: the git mirror release does directory
		// I/O and must not stall cache readers.
		h.releaseProjectResidue(project)
	}
}

func isMetadataNotFound(err error) bool {
	if err == nil {
		return false
	}
	// The git backend surfaces a missing metadata file as an fs-not-exist
	// error chain (readFileHead) or a go-git file-not-found (readFileRef);
	// match sentinels, never message text.
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, shfs.ErrNotFound) {
		return true
	}
	if errors.Is(err, gogitobject.ErrFileNotFound) || errors.Is(err, plumbing.ErrObjectNotFound) {
		return true
	}
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) && apiErr.NotFound() {
		return true
	}
	return false
}

func isRepoAlreadyExistsError(apiErr *ghapi.APIError) bool {
	if apiErr == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(apiErr.Message))
	return strings.Contains(message, "already exists") || strings.Contains(message, "name already exists")
}
