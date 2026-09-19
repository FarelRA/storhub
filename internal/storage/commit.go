package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

func markProjectDirtyLocked(pm *projectMetadata) {
	pm.dirty = true
	pm.version++
}

// appendOpLocked records one metadata mutation in the project's op stack
// and mirrors it to the crash-recovery journal. Caller holds pm.mu. The
// journal receives the PRE-COALESCING DELTA (the op exactly as appended,
// Times=1), so a later fold replays the identical append sequence and
// converges to exactly the in-memory stack — including cross-transaction
// rename chains and rename-then-delete, which post-coalescing tails cannot
// reproduce.
//
// Cap behavior is drop-never: crossing the op-count or either 64MiB byte
// bound compacts the journal to the folded survivors (even though no commit
// succeeded) and warns; the sweeper force-retries the commit until it
// drains. Growth pressure beyond that only warns — never fail-loud
// backpressure, never dropped acknowledged ops.
func (h *StorHub) appendOpLocked(project string, pm *projectMetadata, op Op) {
	beforeBytes := pm.opStack.bytes
	delta := pm.opStack.appendWithDelta(op)
	h.journalAppend(project, delta)
	if pm.opStack.bytes >= opStackMaxBytes || h.journalOverCap(project, pm.opStack.maxSeq()) {
		h.journalRewrite(project, pm.opStack.ops)
	}
	switch {
	case len(pm.opStack.ops) == maxPendingOpsPerProject:
		logging.Warn(h.projectLogger(project), "pending op stack hit residency cap; commit will be force-retried until it drains", "ops", maxPendingOpsPerProject)
	case beforeBytes < opStackMaxBytes && pm.opStack.bytes >= opStackMaxBytes:
		logging.Warn(h.projectLogger(project), "pending op stack hit byte cap; commit will be force-retried until it drains", "bytes", pm.opStack.bytes)
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
	if ch, ok := markDirtyFastPath(pm); ok {
		return ch
	}
	// Capture the old loop's completion channel under pm.mu: the revival
	// below swaps pm.stoppedCh, and reading the field after releasing the
	// lock would race that swap.
	stoppedCh := pm.stoppedCh
	pm.mu.Unlock()
	return h.reviveEvictedProject(project, pm, stoppedCh)
}

// markDirtyFastPath marks pm dirty when its commit loop is live. Caller must
// hold pm.mu. Returns ok=false when the instance was evicted and needs the
// revival path.
func markDirtyFastPath(pm *projectMetadata) (chan struct{}, bool) {
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
	select {
	case <-stoppedCh:
		return true
	case <-time.After(h.revivalTimeout()):
		logging.Error(h.projectLogger(project), "evicted commit loop did not stop; reviving without channel swap", "project", project)
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
// Seal fields consumed (owned by the metadata agent, which guarantees
// SealTransaction marks the seal — coordinate: the metadata agent must keep
// stamping Project, Version, Root.Inode/Mode, and LastMod there, and every
// mutation path must stay on tracked mutators with incremental stats):
// Project (stamped non-empty), Version (stamped when zero), Root.Inode and
// Root.Mode (materialized when zero). LastMod is deliberately excluded: the
// commit stamps it unconditionally after the check.
//
// NOTE (residual risk, accepted per audit-33): an unsealed-but-normalized
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

// commitSnapshot captures the immutable inputs one commit-attempt series
// works from: a private working copy, the op batch the message describes,
// the CAS token, the admission version, and the rebase baseline. Taken under
// pm.mu; the commit then runs without holding it.
type commitSnapshot struct {
	working     *RepoMetadata
	previousSHA string
	version     uint64
	ops         []Op
	opSeq       uint64
	baseTree    *RepoMetadata
	objectCount uint64
	headSplit   bool
	now         int64
}

// snapshotCommitState snapshots the dirty state for one commit: a private
// working copy plus the op batch the message describes. Only ops at or below
// the snapshot seq may be dropped on success (mutations landing mid-commit
// carry higher seqs and stay); recording the snapshot seq lets coalescing
// refuse to rewrite ops in flight inside this commit. Returns nil when clean
// (nothing to do). Exits with pm.mu released on every path.
func (h *StorHub) snapshotCommitState(project string, pm *projectMetadata) *commitSnapshot {
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
	working := cowTree(pm.meta)
	snap := &commitSnapshot{
		working:     working,
		previousSHA: pm.sha,
		version:     pm.version,
		ops:         pm.opStack.snapshot(),
		opSeq:       pm.opStack.maxSeq(),
		baseTree:    pm.baseTree,
		objectCount: pm.objectCount,
		headSplit:   working.IsSplit(),
		now:         h.config.Now().Unix(),
	}
	pm.opStack.noteSnapshot(snap.opSeq)
	pm.mu.Unlock()
	return snap
}

// publishWithRebase stores the snapshot's working tree with a bounded
// commit/rebase cycle: a CAS conflict (another writer advanced the index)
// rebases the pending ops onto upstream state instead of discarding them,
// then retries. Returns the commit/content SHAs, the new object count, and
// whether a rebase landed; snap.working is replaced with the rebased tree
// when one does, and snap.previousSHA tracks the retargeted CAS token.
// Failures are wrapped as *commitError carrying the attempted version, so
// recovery can tell whether newer mutations arrived after the snapshot.
func (h *StorHub) publishWithRebase(ctx context.Context, project string, pm *projectMetadata, snap *commitSnapshot, started time.Time) (commitSHA, contentSHA string, newObjectCount uint64, didRebase bool, err error) {
	working := snap.working
	previousSHA := snap.previousSHA
	now := snap.now

	// The split index (metadata version 5) is the default and only write
	// layout: every commit produces a manifest plus content-addressed objects.
	// A project still on a legacy single-blob document (version <= 4) migrates
	// on this write; its loaded tree carries that version, so the layout is
	// read straight from the metadata version, not a separate flag.
	headSplit := snap.headSplit
	working.MarkSplit()

	// SEAL-SKIP: working trees built only from tracked mutators are already
	// normalized (SealTransaction + incremental stats) — skip the full
	// Normalize + RecomputeStats O(tree) walk when the seal is clean. A
	// single-op commit (touch) otherwise pays per-op O(tree) async CPU.
	sealed := isSealedClean(working, project)
	if !sealed {
		working.Normalize(project, now)
	}
	working.LastMod = now
	if !sealed {
		working.RecomputeStats()
	}

	logging.Info(h.projectLogger(project), "commit metadata start", "previous_sha", shortSHA(previousSHA), "migrating", !headSplit)

	if err := h.ensureOwner(ctx); err != nil {
		return "", "", 0, false, err
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
	message := buildCommitMessage(snap.ops, previousSHA)
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
			commitSHA, contentSHA, newObjectCount, err = h.publishIndex(ctx, project, working, previousSHA, message, snap.objectCount)
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
			return "", "", 0, false, &commitError{err: err, version: snap.version}
		}
		var apiErr *ghapi.APIError
		isConflict := errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict
		if !isConflict || attempt >= maxCommitAttempts {
			if isConflict {
				err = &rebaseExhaustedError{err: err, attempts: attempt}
			}
			err = wrapNoSpace(h.config.GitCacheDir, err)
			logging.Error(h.projectLogger(project), "commit metadata failed", "step", "git_commit", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
			return "", "", 0, false, &commitError{err: fmt.Errorf("commit metadata: %w", err), version: snap.version}
		}
		logging.Warn(h.projectLogger(project), "metadata commit conflicted; rebasing op stack", "attempt", attempt, "ops", len(snap.ops))
		if baseFP == nil && snap.baseTree != nil {
			baseFP = hashPaths(snap.baseTree)
		}
		rebased, upstreamSHA, resolutions, rerr := h.rebaseOntoUpstream(ctx, project, snap.ops, baseFP)
		if rerr != nil {
			logging.Error(h.projectLogger(project), "commit metadata failed", "step", "rebase", "elapsed", h.config.Now().UTC().Sub(started), "err", rerr)
			return "", "", 0, false, &commitError{err: fmt.Errorf("rebase pending ops: %w", rerr), version: snap.version}
		}
		didRebase = true
		working = rebased
		working.LastMod = now
		previousSHA = upstreamSHA
		message = buildCommitMessage(snap.ops, previousSHA) + "\n" + rebaseMessageNote(resolutions, upstreamSHA)
	}
	snap.working = working
	snap.previousSHA = previousSHA
	return commitSHA, contentSHA, newObjectCount, didRebase, nil
}

// applyCommittedTree publishes a successful commit back into the cache: the
// CAS token advances, the rebase baseline moves to the just-committed state
// (a frozen tree handed a pointer to; the fingerprint is recomputed only on
// conflict), and the pending stack folds to the survivors. The apply-back is
// three-way:
//
//   - no mid-commit mutation (version unchanged): the normalized working
//     copy becomes the shared truth (without this the cache keeps raw
//     mutation state — stale stats, unrepaired inode counter — and every
//     later Validate trips over it) and dirty clears;
//   - mid-commit mutation WITH rebase: the committed tree carries upstream
//     changes the live tree lacks, so the surviving ops replay onto the
//     committed tree instead of adopting the live tree wholesale (which
//     would make the next commit overwrite them);
//   - mid-commit mutation WITHOUT rebase: the live tree already contains the
//     committed ops' effects plus the newer mutation; drop the committed ops
//     and keep the mutation's ops + dirty flag for the next commit.
//
// The journal is rewritten to the surviving stack, and a fitting commit
// lifts the size-ceiling breach marker (re-armed on breach).
func (h *StorHub) applyCommittedTree(project string, pm *projectMetadata, snap *commitSnapshot, commitSHA, contentSHA string, newObjectCount uint64, didRebase bool) {
	working := snap.working
	pm.mu.Lock()
	pm.sha = contentSHA
	pm.objectCount = newObjectCount
	if pm.version == snap.version {
		// Apply-back: the normalized working copy becomes the shared
		// truth on success. Without this the cache keeps the raw mutation
		// state (stale stats, unrepaired inode counter) and every later
		// Validate of cached state - rollback, drain, explicit checks -
		// trips over it. Version-guarded: a mutation that landed while the
		// commit ran owns the newer state; its own commit normalizes.
		pm.meta = working
		pm.dirty = false
		pm.lastCommit = h.config.Now()
		pm.opStack.clearUpTo(snap.opSeq)
		// The adopted tree is normalized (repaired stats/counters), not
		// byte-identical to the published one: bump so version watchers
		// (cross-surface invalidation) observe the swap. Guards are
		// equality-based, so extra bumps only cause extra invalidations,
		// never missed ones.
		pm.version++
	} else if didRebase {
		// A mutation landed mid-commit AND the commit rebased: the
		// committed tree carries upstream changes the live tree lacks, so
		// adopting the live tree wholesale would make the next commit
		// overwrite them. Replay the surviving ops onto the committed tree.
		pm.opStack.clearUpTo(snap.opSeq)
		surviving := pm.opStack.snapshot()
		if len(surviving) > 0 {
			rebased := working.Clone()
			if err := applyOps(rebased, surviving); err != nil {
				// Defensive only: well-formed ops cannot fail replay.
				// Converge to the committed truth rather than keep a
				// stale tree that could clobber it.
				logging.Error(h.projectLogger(project), "mid-commit mutation replay failed; committed state retained", "err", err)
				pm.opStack.clear()
				pm.meta = working
				// Same bump: falling back to the committed tree swaps in
				// upstream content the live tree lacks.
				pm.version++
			} else {
				rebased.Normalize(project, h.config.Now().Unix())
				rebased.RecomputeStats()
				pm.meta = rebased
				// Rebased tree carries upstream changes the live tree
				// lacks: bump so watchers observe the arrival (see the
				// version-match branch above for the invariant), and mark
				// unknown fan-out scope (upstream paths are not tracked
				// here).
				pm.version++
				notePublishedPathsLocked(pm, nil)
			}
		} else {
			pm.meta = working
			// Same bump: the adopted tree differs from the previously
			// published one (upstream content landed).
			pm.version++
		}
		pm.lastCommit = h.config.Now()
	} else {
		// Mid-commit mutation, no rebase: the live tree already contains
		// the committed ops' effects plus the newer mutation. Drop the
		// committed ops (their state is in the live tree) and keep the
		// mutation's ops + dirty flag for the next commit.
		pm.opStack.clearUpTo(snap.opSeq)
	}
	// The journal is rewritten to the surviving stack.
	h.journalRewrite(project, pm.opStack.ops)
	// The rebase baseline moves to the just-committed state (a frozen tree
	// we hand a pointer to; the fingerprint is recomputed only on conflict).
	pm.baseTree = working
	// A fitting commit lifts the size-ceiling breach marker (re-armed on breach).
	pm.sizeCapped = false
	pm.mu.Unlock()
}

// commitProjectMetadata commits dirty metadata without holding pm.mu during GitHub I/O.
func (h *StorHub) commitProjectMetadata(ctx context.Context, project string, pm *projectMetadata) error {
	pm.commitMu.Lock()
	defer pm.commitMu.Unlock()

	started := h.config.Now().UTC()
	snap := h.snapshotCommitState(project, pm)
	if snap == nil {
		return nil
	}

	commitSHA, contentSHA, newObjectCount, didRebase, err := h.publishWithRebase(ctx, project, pm, snap, started)
	if err != nil {
		return err
	}

	h.applyCommittedTree(project, pm, snap, commitSHA, contentSHA, newObjectCount, didRebase)

	h.warnHistoryThreshold(project, pm)
	logging.Info(h.projectLogger(project), "commit metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "commit_sha", shortSHA(commitSHA), "content_sha", shortSHA(contentSHA), "objects", newObjectCount)

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

// drainMaxRounds bounds how many commit rounds one DrainProjectContext
// chases a concurrent writer's flood. Rounds past the first only happen
// when a mutation landed mid-drain; each round lands everything published
// before it started, so the loop converges unless writers outpace commits
// indefinitely, and then it fails loud instead of parking the caller.
const drainMaxRounds = 3

// DrainProjectContext blocks until everything published before the call
// has landed in the remote commit, or fails loudly. It is the shared
// durability primitive behind fsync, O_SYNC, and the REST/CLI sync
// opt-in. Semantics are fsync-class: pre-call data is durable on
// success; a concurrent writer's later mutations may ride along in the
// same commit or wait for a later one, but they can never strand the
// caller past drainMaxRounds. A clean project costs no network (the
// commit is a cheap no-op); a failed push retains dirty state for retry
// exactly like the commit loop and reports the error naming the project.
func (h *StorHub) DrainProjectContext(ctx context.Context, project string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	pm := h.getOrCreateProjectMeta(project)
	// Snapshot the frontier under mu: every op at or below target was
	// published before this call. A successful commit clears through its
	// own snapshot (taken later, so a superset), so anything left dirty
	// afterwards is strictly newer.
	pm.mu.Lock()
	target := pm.opStack.maxSeq()
	pm.mu.Unlock()
	for round := 0; round < drainMaxRounds; round++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("drain %s: %w", project, err)
		}
		if err := h.commitProjectMetadata(ctx, project, pm); err != nil {
			h.recoverMetadataCommitFailure(project, err)
			return fmt.Errorf("drain %s: %w", project, err)
		}
		pm.mu.Lock()
		dirty := pm.dirty
		stale := false
		for _, op := range pm.opStack.ops {
			if op.Seq <= target {
				stale = true
				break
			}
		}
		pm.mu.Unlock()
		if !dirty || !stale {
			return nil
		}
	}
	return fmt.Errorf("drain %s: still dirty after %d rounds", project, drainMaxRounds)
}
