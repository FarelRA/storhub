package storage

import (
	"errors"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	"net/http"
	"time"
)

func markProjectDirtyLocked(pm *projectMetadata) {
	pm.dirty = true
	pm.version++
}

// healBaseTreeLocked repairs a nil rebase baseline left by a cold
// hydrate: the live tree is the truth the pending ops were built on.
// Published trees are immutable under copy-on-write discipline, so sharing
// the pointer matches store practice with no clone. Caller holds pm.mu.
func healBaseTreeLocked(pm *projectMetadata) {
	if pm.baseTree == nil && pm.meta != nil {
		pm.baseTree = pm.meta
	}
}

// appendOpLocked records one metadata mutation in the project's op stack
// and mirrors it to the crash-recovery journal. Caller holds pm.mu. The
// journal receives the PRE-COALESCING DELTA (the op exactly as appended,
// Times=1), so a later fold replays the identical append sequence and
// converges to exactly the in-memory stack: including cross-transaction
// rename chains and rename-then-delete, which post-coalescing tails cannot
// reproduce.
//
// Mutation contract: the intent funnel (synthesizeOpsFromIntents) is the
// canonical synthesizer for transaction-level mutations. Ops appended here
// directly are pre-synthesized singletons that bypass classification,
// rename pairing, and admission: each such site calls
// appendSynthesizedOpLocked and carries a comment naming why it cannot go
// through the funnel. New mutation paths default to the funnel.
//
// Cap behavior is drop-never: crossing the op-count or either 64MiB byte
// bound compacts the journal to the folded survivors (even though no commit
// succeeded) and warns; the append site pokes the commit trigger directly
// so the retry is immediate, not delayed. Growth pressure beyond that only
// warns: never fail-loud backpressure, never dropped acknowledged ops.
func (h *StorHub) appendOpLocked(project string, pm *projectMetadata, op Op) {
	beforeBytes := pm.opStack.bytes
	healBaseTreeLocked(pm)
	delta := pm.opStack.appendWithDelta(op)
	lineBytes := h.journalAppend(project, delta)
	if pm.opStack.bytes >= opStackMaxBytes || h.journalOverCap(project, pm.opStack.maxSeq(), lineBytes) {
		h.journalRewrite(project, pm.opStack.ops)
	}
	switch {
	case len(pm.opStack.ops) == maxPendingOpsPerProject:
		h.pressure.noteCapCross()
		logging.Warn(h.projectLogger(project), "pending op stack hit residency cap; commit will be force-retried until it drains", "ops", maxPendingOpsPerProject)
	case beforeBytes < opStackMaxBytes && pm.opStack.bytes >= opStackMaxBytes:
		h.pressure.noteCapCross()
		logging.Warn(h.projectLogger(project), "pending op stack hit byte cap; commit will be force-retried until it drains", "bytes", pm.opStack.bytes)
	}
	if pm.opStack.needsForceFlush() {
		h.pressure.noteForceRetry()
		// Force-retry the commit the moment the stack crosses a
		// residency bound: the sweeper tick is gone, so the crossing
		// mutation itself must wake the loop. pm.triggerCh is read
		// live under the pm.mu hold every caller already takes, so no
		// revival can swap the channel mid-send; the non-blocking
		// send never blocks under lock.
		select {
		case pm.triggerCh <- struct{}{}:
		default:
		}
	}
}

// appendSynthesizedOpLocked is the marked escape hatch for ops that reach
// the stack without passing the intent funnel: the op is already a complete
// full-state assertion (sibling propagation, parent mtime fixups, atime
// touches, upload and transfer writes), so classification and rename
// pairing have nothing to fold. Every call site names its reason in a
// comment. Caller holds pm.mu.
func (h *StorHub) appendSynthesizedOpLocked(project string, pm *projectMetadata, op Op) {
	h.appendOpLocked(project, pm, op)
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
		// Pre-synthesized singleton: the sibling entry is already final
		// state read from the candidate tree, with no intent recorded for
		// it, so the funnel has nothing to fold.
		h.appendSynthesizedOpLocked(project, pm, Op{
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
		// Pre-synthesized singleton: a parent mtime fixup carries final
		// state only, with no intent recorded for it.
		h.appendSynthesizedOpLocked(project, pm, Op{Type: OpSetattr, Paths: []string{""}, Cause: cause, Timestamp: now, Dir: &root})
		return
	}
	dir := pm.meta.GetDirectory(parent)
	if dir == nil {
		return
	}
	d := dir.Clone()
	// Pre-synthesized singleton: see the root branch above.
	h.appendSynthesizedOpLocked(project, pm, Op{Type: OpSetattr, Paths: []string{parent}, Cause: cause, Timestamp: now, Dir: &d})
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
	return 1 * storcfg.PatienceUnit
}

func (h *StorHub) markProjectDirtyLiveLocked(project string, pm *projectMetadata) chan struct{} {
	if ch, ok := markProjectDirtyFastLocked(pm); ok {
		return ch
	}
	// Capture the old loop's completion channel under pm.mu: the revival
	// below swaps pm.stoppedCh, and reading the field after releasing the
	// lock would race that swap.
	stoppedCh := pm.stoppedCh
	pm.mu.Unlock()
	return h.reviveEvictedProject(project, pm, stoppedCh)
}

// markProjectDirtyFastLocked marks pm dirty when its commit loop is live.
// Caller must hold pm.mu. Returns ok=false when the instance was evicted
// and needs the revival path.
func markProjectDirtyFastLocked(pm *projectMetadata) (chan struct{}, bool) {
	if pm.stopped {
		return nil, false
	}
	markProjectDirtyLocked(pm)
	return pm.triggerCh, true
}

// waitCommitLoopExit blocks until the evicted commit loop fully exits. The
// loop reads stopCh/triggerCh unsynchronized, and the eviction close(stopCh)
// only requests exit - stoppedCh closes when it has actually returned. A
// false return means the revival timeout fired; the caller then revives
// without the channel swap.
func (h *StorHub) waitCommitLoopExit(project string, stoppedCh <-chan struct{}) bool {
	// Stopped timer, not time.After: After would leak until it fires.
	timer := time.NewTimer(h.revivalTimeout())
	defer timer.Stop()
	select {
	case <-stoppedCh:
		return true
	case <-timer.C:
		logging.Error(h.projectLogger(project), "evicted commit loop did not stop; reviving without channel swap")
		return false
	}
}

// reviveEvictedProject re-inserts an evicted instance into the cache with
// fresh loop channels (or marks it for the shutdown drain), so a mutation
// acknowledged to the caller can never silently strand on a commit loop that
// already stopped. When a fresher incarnation owns the cache slot, the stale
// snapshot's changes cannot be applied and this fails loudly instead of
// losing them silently.
//
// Entry: pm.mu NOT held (the fast path released it around the exit wait).
// Exit: pm.mu HELD on every path, returning the trigger channel to poke
// after releasing pm.mu (the returned value was read under the final pm.mu
// critical section, giving callers a synchronized handle; a revival failure
// returns the stale channel, whose poke is a harmless no-op).
//
// Caller must hold NO locks on entry; the lock dance below drops and
// re-acquires around cache bookkeeping so metaMu→pm.mu ordering stays
// consistent with getOrCreateProjectMeta.
func (h *StorHub) reviveEvictedProject(project string, pm *projectMetadata, stoppedCh <-chan struct{}) chan struct{} {
	// Wait for the evicted commit loop to fully exit before replacing its
	// channels: the loop reads stopCh/triggerCh unsynchronized, and the
	// eviction close(stopCh) only requests exit - stoppedCh closes when it
	// has actually returned.
	if !h.waitCommitLoopExit(project, stoppedCh) {
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
	revived, live, drainOnly := h.tryReviveLocked(project, pm)
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
		logging.Info(h.projectLogger(project), "reviving evicted project metadata after concurrent operation")
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

// tryReviveLocked runs the revival state machine for an evicted instance:
// a fresher cache incarnation (diverged or concurrently revived) wins and
// the stale snapshot only marks dirty when it is still the live entry;
// otherwise fresh channels are swapped in for a new commit loop (unless
// Shutdown began, in which case the entry drains). Caller holds metaMu for
// writing AND pm.mu; returns with both still held.
func (h *StorHub) tryReviveLocked(project string, pm *projectMetadata) (revived, live, drainOnly bool) {
	current, exists := h.metaCache[project]
	switch {
	case exists && current != pm:
		logging.Error(h.projectLogger(project),
			"evicted metadata snapshot diverged from a newer reload; mutations in this window are lost")
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
	return revived, live, drainOnly
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

// isSealedClean reports whether a working tree carries the SealTransaction
// bookkeeping, meaning it was built only through tracked mutators from a
// normalized base: touched-file chunk order is canonical and stats were
// maintained incrementally, so the commit's full Normalize + RecomputeStats
// walk would be a no-op and is skipped (SEAL-SKIP). A tree failing the check
// takes the wholesale-construction path.
//
// Seal fields consumed (owned by the metadata engine, which guarantees
// SealTransaction marks the seal: coordinate: the metadata engine must keep
// stamping Project, Version, Root.Inode/Mode, and LastMod there, and every
// mutation path must stay on tracked mutators with incremental stats):
// Project (stamped non-empty), Version (stamped when zero), Root.Inode and
// Root.Mode (materialized when zero). LastMod is deliberately excluded: the
// commit stamps it unconditionally after the check.
//
// NOTE (residual risk, accepted per commit-admission review): an unsealed-but-normalized
// tree also passes and skips the walk. That is safe exactly while every
// mutation path goes through tracked mutators with incremental stats; a
// future direct-write path that bypasses them must either seal or force the
// full walk.
func isSealedClean(working *RepoMetadata, project string) bool {
	if working == nil {
		return false
	}
	if working.Project != project {
		return false
	}
	if working.Version == 0 {
		return false
	}
	if working.Root.Inode == 0 || working.Root.Mode == 0 {
		return false
	}
	return true
}
