package storage

import (
	"sync"
)

// maxRecentPaths bounds the per-project publish ring for invalidation
// fan-out: with 64 entries a 1s poller survives a 64-publish burst
// without losing scope; older entries evict, and a consumer whose
// baseline predates the oldest retained entry gets unknown-scope (safe,
// invalidate broadly) instead of a silent miss.
const maxRecentPaths = 64

// pathVersion is one publish's namespace footprint: a strictly increasing
// fan-out sequence number plus the touched paths, or nil paths for
// unknown scope (remote-truth swap, conflict-rebase adopt). The sequence
// is dedicated (not pm.version) because the version bump happens after
// the note in publish flows; sharing it would let two concurrent
// publishes stamp the same version and lose entries at the cursor.

// pathVersion is one publish's namespace footprint: a strictly increasing
// fan-out sequence number plus the touched paths, or nil paths for
// unknown scope (remote-truth swap, conflict-rebase adopt). The sequence
// is dedicated (not pm.version) because the version bump happens after
// the note in publish flows; sharing it would let two concurrent
// publishes stamp the same version and lose entries at the cursor.
type pathVersion struct {
	seq   uint64
	paths []string
}

// Push fan-out subscriber registry (Phase E1: the poller replacement).
//
// A FUSE mount subscribes at construction and unsubscribes at Close; every
// publish then pokes its project's subscribers instead of each mount
// polling PublishedPathsSince once a second. The registry lives in this
// file (not on projectMetadata or StorHub) so the set and its harvest stay
// together: notePublishedPathsLocked resolves subscribers by the
// *projectMetadata pointer it already receives, so no call-site signature
// changes and no publish semantics change.
//
// Records are keyed by pm pointer. When eviction drops a pm from the hub
// cache, a recreated pm starts with no record: a subscribed mount misses
// pushes for publishes that land on the new pm until it resubscribes, and
// falls back to kernel entry/attr timeout expiry for those. That corner
// needs an idle mount (eviction only takes clean, untouched projects) plus
// a cross-surface write in the same window; it self-heals on the next
// revalidation, which recreates nothing but refetches fresh truth.
// Steady-state cost with no subscribers is one map lookup per publish;
// with subscribers it is one snapshot plus one short-lived dispatch
// goroutine per publish. There is no idle cost: no timers, no parked
// per-mount goroutines.

// Push fan-out subscriber registry (Phase E1: the poller replacement).
//
// A FUSE mount subscribes at construction and unsubscribes at Close; every
// publish then pokes its project's subscribers instead of each mount
// polling PublishedPathsSince once a second. The registry lives in this
// file (not on projectMetadata or StorHub) so the set and its harvest stay
// together: notePublishedPathsLocked resolves subscribers by the
// *projectMetadata pointer it already receives, so no call-site signature
// changes and no publish semantics change.
//
// Records are keyed by pm pointer. When eviction drops a pm from the hub
// cache, a recreated pm starts with no record: a subscribed mount misses
// pushes for publishes that land on the new pm until it resubscribes, and
// falls back to kernel entry/attr timeout expiry for those. That corner
// needs an idle mount (eviction only takes clean, untouched projects) plus
// a cross-surface write in the same window; it self-heals on the next
// revalidation, which recreates nothing but refetches fresh truth.
// Steady-state cost with no subscribers is one map lookup per publish;
// with subscribers it is one snapshot plus one short-lived dispatch
// goroutine per publish. There is no idle cost: no timers, no parked
// per-mount goroutines.
type fanoutProjectSubs struct {
	hub     *StorHub
	project string
	subs    map[uint64]func()
	nextID  uint64
}

var (
	fanoutSubsMu sync.Mutex
	fanoutSubs   = make(map[*projectMetadata]*fanoutProjectSubs)
)

// SubscribeProjectPublishes registers onPublish for pushes on project and
// returns the current fan-out cursor plus an idempotent unsubscribe. The
// cursor is read BEFORE the insert (not under one hold: two locks are
// involved, so order is the guarantee): a publish landing between the read
// and the insert is poked to the old set only, but the returned cursor
// predates it, so the subscriber's first pull still covers it. Nil
// callbacks are refused.

// SubscribeProjectPublishes registers onPublish for pushes on project and
// returns the current fan-out cursor plus an idempotent unsubscribe. The
// cursor is read BEFORE the insert (not under one hold: two locks are
// involved, so order is the guarantee): a publish landing between the read
// and the insert is poked to the old set only, but the returned cursor
// predates it, so the subscriber's first pull still covers it. Nil
// callbacks are refused.
func (h *StorHub) SubscribeProjectPublishes(project string, onPublish func()) (cursor uint64, unsubscribe func()) {
	never := func() {}
	if onPublish == nil {
		return 0, never
	}
	// A zero-value hub (uninitialized cache, as in CLI seam tests that
	// stub a bare &StorHub{}) cannot serve subscriptions: degrade to
	// unsubscribed exactly like a hub without the capability, so mount
	// construction never panics and timeout expiry keeps working.
	h.metaMu.RLock()
	initialized := h.metaCache != nil
	h.metaMu.RUnlock()
	if !initialized {
		return 0, never
	}
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.RLock()
	cursor = pm.fanoutSeq
	pm.mu.RUnlock()

	fanoutSubsMu.Lock()
	rec := fanoutSubs[pm]
	if rec == nil {
		rec = &fanoutProjectSubs{hub: h, project: project, subs: make(map[uint64]func())}
		fanoutSubs[pm] = rec
	}
	id := rec.nextID
	rec.nextID++
	rec.subs[id] = onPublish
	fanoutSubsMu.Unlock()

	var once sync.Once
	return cursor, func() {
		once.Do(func() {
			fanoutSubsMu.Lock()
			defer fanoutSubsMu.Unlock()
			rec := fanoutSubs[pm]
			if rec == nil {
				return
			}
			delete(rec.subs, id)
			if len(rec.subs) == 0 {
				delete(fanoutSubs, pm)
			}
		})
	}
}

// fanoutSnapshotLocked copies one publish's subscriber pokes. Callers hold
// no registry-external lock except pm.mu for writing (it runs inside
// notePublishedPathsLocked); the pokes themselves run later, without pm.mu.

// fanoutSnapshotLocked copies one publish's subscriber pokes. Callers hold
// no registry-external lock except pm.mu for writing (it runs inside
// notePublishedPathsLocked); the pokes themselves run later, without pm.mu.
func fanoutSnapshotLocked(pm *projectMetadata) []func() {
	fanoutSubsMu.Lock()
	defer fanoutSubsMu.Unlock()
	rec := fanoutSubs[pm]
	if rec == nil || len(rec.subs) == 0 {
		return nil
	}
	cbs := make([]func(), 0, len(rec.subs))
	for _, cb := range rec.subs {
		cbs = append(cbs, cb)
	}
	return cbs
}

// notePublishedPathsLocked records one publish's footprint in the ring.
// Caller holds pm.mu for writing (it runs inside publishTreeLocked's
// contract, or another spot holding pm.mu across the swap). Nil paths
// marks unknown scope.

// PublishedPathsSince returns the namespace paths published after the
// since cursor, plus the current cursor for the next call. unknown
// reports that scope was lost (unknown-scope entry in range, ring
// overflow past the baseline, or project not resident): the caller must
// invalidate broadly instead of trusting the path list. Empty paths with
// unknown=false means nothing changed.
func (h *StorHub) PublishedPathsSince(project string, since uint64) (paths []string, unknown bool, current uint64) {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return nil, true, 0
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	current = pm.fanoutSeq
	if len(pm.recent) == 0 {
		return nil, false, current
	}
	// The ring retains a contiguous suffix of sequences, so the window
	// is complete when the baseline touches it: at or just before the
	// oldest retained sequence (a zero baseline against a ring starting
	// at 1 is complete; nothing could have evicted yet). Anything older
	// may have missed evicted entries.
	if oldest := pm.recent[0].seq; since+1 < oldest {
		return nil, true, current
	}
	seen := make(map[string]struct{})
	for _, entry := range pm.recent {
		if entry.seq <= since {
			continue
		}
		if entry.paths == nil {
			return nil, true, current
		}
		for _, p := range entry.paths {
			seen[p] = struct{}{}
		}
	}
	for p := range seen {
		paths = append(paths, p)
	}
	return paths, false, current
}

// publishTreeLocked swaps a mutated COW copy in as the new shared truth.
// It rebuilds the derived indexes so the published tree is clean and
// exclusively owned: a lock-free reader's index read (NLink/DirNLink/
// FindFilesByInode) then hits a fresh index and never triggers a rebuild
// write that would race other readers. Caller holds pm.mu for writing.
// paths records this publish's namespace footprint in the fan-out ring
// (nil = unknown scope); the ring append runs under the same mu hold, so
// every swap carries exactly one entry and consumers cannot miss one.

// publishTreeLocked swaps a mutated COW copy in as the new shared truth.
// It rebuilds the derived indexes so the published tree is clean and
// exclusively owned: a lock-free reader's index read (NLink/DirNLink/
// FindFilesByInode) then hits a fresh index and never triggers a rebuild
// write that would race other readers. Caller holds pm.mu for writing.
// paths records this publish's namespace footprint in the fan-out ring
// (nil = unknown scope); the ring append runs under the same mu hold, so
// every swap carries exactly one entry and consumers cannot miss one.
func publishTreeLocked(pm *projectMetadata, tree *RepoMetadata, paths []string) {
	tree.RebuildIndexes()
	pm.meta = tree
	notePublishedPathsLocked(pm, paths)
}

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
