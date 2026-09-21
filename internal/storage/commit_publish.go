package storage

import (
	"context"
	"errors"
	"fmt"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	"net/http"
	"time"
)

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
		h.pressure.noteRebase()
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
func (h *StorHub) applyCommittedTree(project string, pm *projectMetadata, snap *commitSnapshot, _, contentSHA string, newObjectCount uint64, didRebase bool) {
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
		//
		// No path-ring entry here: this swap carries no new namespace
		// content beyond the mutation's own publish (already ringed with
		// exact paths via publishTreeLocked). A nil entry would force
		// unknown scope on a window the test suite pins exact
		// (TestPublishedPathsSinceExactScopes); re-recording the op paths
		// would represent one mutation twice.
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
				// upstream content the live tree lacks. Unknown scope.
				pm.version++
				notePublishedPathsLocked(pm, nil)
			} else {
				rebased.Normalize(project, h.config.Now().UnixNano())
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
			// published one (upstream content landed). Unknown scope.
			pm.version++
			notePublishedPathsLocked(pm, nil)
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

	// Ordered-commit data-first step: the journal is fsynced after the
	// snapshot closes the batch and before anything publishes, so a
	// closed batch is disk-durable before its manifest CAS. This is what
	// the old group-commit timer did on a clock; the commit trigger does
	// it now on the event that actually closes the batch.
	h.flushJournals()

	commitSHA, contentSHA, newObjectCount, didRebase, err := h.publishWithRebase(ctx, project, pm, snap, started)
	if err != nil {
		h.pressure.noteCommitFailure(project)
		// The snapshot published nothing, and there is no mark to roll
		// back: the frozen batch keeps its generation, post-freeze
		// appends live in a newer one, and the merge rule never spans
		// generations — so later appends still coalesce exactly as the
		// journal fold replays them, with no restore step that could be
		// forgotten or raced. The next snapshot seals every pending
		// generation at once. (This deletes the old rollbackSnapshot:
		// a stale snapshot mark used to poison later merges, leaving
		// the live stack split while the fold merged; the structure now
		// excludes that interleaving, so the failure path is empty.)
		return err
	}

	h.applyCommittedTree(project, pm, snap, commitSHA, contentSHA, newObjectCount, didRebase)

	h.pressure.noteCommitSuccess(project)
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
	// Ordered-commit durability point: fsync every journal with pending
	// appends before attempting the remote push, so the sync path (FUSE
	// fsync/Flush/Release drain, O_SYNC, REST/CLI sync) is durable by
	// construction, and a failed push still leaves the journal durable
	// for retry.
	h.flushJournals()
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
