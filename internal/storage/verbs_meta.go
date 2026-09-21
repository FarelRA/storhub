package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// verbs_meta.go: metadata verbs: flush, list, revisions, rollback, revert, repo helpers.

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
			// commit loop: recoverMetadataCommitFailure RETAINS the dirty
			// state (pending ops included) for the next trigger instead of
			// discarding it — there is no 409-reload here. The error is
			// still reported to the caller.
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

// ListFilesContext returns every stored file in project.
func (h *StorHub) ListFilesContext(ctx context.Context, project string) ([]FileMeta, error) {
	var result []FileMeta
	var err error
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

// ListReleasesContext returns the chunk-holding releases of project.
func (h *StorHub) ListReleasesContext(ctx context.Context, project string) ([]metadata.ReleaseRef, error) {
	var result []metadata.ReleaseRef
	var err error
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
		result = append(result, ref)
	}
	return result, nil
}

// ListMetadataRevisionsContext returns the metadata history of project.
func (h *StorHub) ListMetadataRevisionsContext(ctx context.Context, project string) ([]MetadataRevision, error) {
	var result []MetadataRevision
	var err error
	started := h.logOpStart(project, "list-metadata-revisions")
	defer func() { h.logOpFinish(project, "list-metadata-revisions", started, err, "count", len(result)) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	result, err = h.listMetadataRevisions(ctx, project)
	return result, err
}

// RollbackMetadataContext resets project metadata to commitSHA.
func (h *StorHub) RollbackMetadataContext(ctx context.Context, project, commitSHA string) error {
	var err error
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
	_, _, err = h.commitRepoMetadata(ctx, project, rollbackMeta, currentSHA, fmt.Sprintf("storhub: rollback metadata to %s", shortSHA(commitSHA)))
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

// RevertPathContext restores a single path (a file or an entire directory subtree) to
// its state at commitSHA, leaving every other path untouched, as a NEW commit.
// its state at commitSHA, leaving every other path untouched, as a NEW commit.
// RevertPathContext is the per-path counterpart of RollbackMetadataContext:
// instead of repointing the whole index at an old revision, it replays just
// `path`'s historical state onto the current tree. It is a revert, not a
// force-push: history is preserved and the result flows through the normal
// transaction path (op synthesis, journal, rebase, commit). The reverted
// subtree's assets are validated against live releases before and after the
// commit, so restoring a path whose bytes were purged fails loudly rather
// than committing a dangling reference.
func (h *StorHub) RevertPathContext(ctx context.Context, project, path, commitSHA string) error {
	var err error
	started := h.logOpStart(project, "revert", "path", path, "commit_sha", commitSHA)
	defer func() { h.logOpFinish(project, "revert", started, err, "path", path, "commit_sha", commitSHA) }()
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
	if err := metadata.RevertSubtree(preview, historical, cleanPath, h.config.Now().UnixNano()); err != nil {
		return err
	}
	preview.Normalize(project, h.config.Now().UnixNano())
	if err := h.validateMetadataSnapshot(ctx, project, preview); err != nil {
		return fmt.Errorf("revert %s: %w", cleanPath, err)
	}
	message := fmt.Sprintf("storhub: revert %s to %s", cleanPath, shortSHA(commitSHA))
	if _, err := h.UpdateRepoMetadataContext(ctx, project, func(m *metadata.RepoMetadata) error {
		return metadata.RevertSubtree(m, historical, cleanPath, h.config.Now().UnixNano())
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

// LoadRepoMetadataReadonlyContext loads project metadata plus its revision without tracking.
func (h *StorHub) LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*metadata.RepoMetadata, string, error) {
	return h.loadRepoMetadataReadonly(ctx, project)
}

// UpdateRepoMetadataContext applies fn as a transaction against the project's
// metadata. The mutation is applied to a private copy-on-write tree under the
// exclusive lock and marked dirty for event-driven commit; the shared tree is
// swapped in only after every fallible step (apply, seal, admission) has
// succeeded, so a rejected mutation leaves shared state, the dirty flag, and
// the op stack exactly as they were (rollback is free: the copy is discarded).
//
// The transaction runs in three stages: hydrateProjectForTx (cold-cache
// guard), fn plus seal plus admitCandidateSplit (size admission, with the
// expensive BuildTree probe run OFF the lock), and publishTxLocked (op
// synthesis plus the atomic swap via publishTreeLocked).
//
// It returns the LIVE shared pointer, not a Clone: published trees are
// immutable under the COW discipline (every mutation goes through cowTree +
// publishTreeLocked, and lock-free readers already rely on it), so handing
// out the pointer is safe and avoids a full O(tree) Clone + RebuildIndexes
// per transaction. Callers MUST treat the result as read-only: mutating it
// corrupts the hub's in-memory truth and races lock-free readers. (Wave-2
// test flip: TestUpdateRepoMetadataReturnsClone in repo_safety_test.go
// asserts the old Clone return — it must be updated to pin read-only
// sharing instead of copying.)
func (h *StorHub) UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*metadata.RepoMetadata) error, message string) (*metadata.RepoMetadata, error) {
	// Degraded-mode admission: every fs/posix mutation funnels through
	// here, so one gate covers them all. Recovery verbs bypass
	// structurally (they commit directly, never through this funnel).
	if err := h.admitMutation(project); err != nil {
		return nil, err
	}
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

	if err := h.hydrateProjectForTx(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		return nil, err
	}

	// In-transaction CAS gate: the token the caller pre-checked against
	// remote HEAD is re-verified against the committed token and consumed
	// here, before fn runs, in the same pm.mu critical section as the
	// publish below. A mismatch fails with ErrPreconditionFailed with the
	// shared tree untouched (never partial application), and two racing
	// CAS on the same token admit exactly one winner.
	if err := h.checkRevisionGateLocked(pm, revisionGateFromContext(ctx)); err != nil {
		pm.mu.Unlock()
		h.debugf("metadata update rejected project=%s step=revision-gate err=%v", project, err)
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, err
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
	// Intent recording: the tracked mutators record what fn changes while
	// it runs, so op synthesis after admission folds the recorded intents
	// (O(changes)) instead of diffing the whole pre-transaction tree
	// (O(tree)). The recorder is transaction-scoped: attached here, dropped
	// by Clone, detached before the candidate is published.
	rec := metadata.NewIntentRecorder()
	candidate.AttachIntentRecorder(rec)
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
	admitNow := h.config.Now().UnixNano()
	// Canonicalize the files the mutation touched (chunk-id order is part of
	// the entry's serialized bytes and the read order), then stamp the
	// per-transaction bookkeeping. O(changes): the candidate's entries are
	// already normalized and its stats were maintained incrementally by the
	// mutators - the full Normalize/RecomputeStats walk is wholesale-
	// construction work (load, migrate, rebase replay), not mutation work.
	for path := range rec.FileIntents() {
		candidate.SortFileChunks(path)
	}
	candidate.SealTransaction(project, admitNow)
	afterSize, err := candidate.SerializedSize()
	if err != nil {
		pm.mu.Unlock()
		h.debugf("metadata update failed project=%s step=size elapsed=%s err=%v", project, h.config.Now().UTC().Sub(started), err)
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, fmt.Errorf("size metadata: %w", err)
	}
	admitVersion := pm.version
	if err := h.admitCandidateSplit(project, pm, candidate, int64(beforeSize), int64(afterSize), admitVersion, message, started); err != nil {
		pm.mu.Unlock()
		return nil, err
	}
	h.publishTxLocked(project, pm, candidate, rec, cause, admitNow)

	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	// Trigger the commit loop to wake up immediately
	select {
	case trigger <- struct{}{}:
	default:
	}

	h.debugf("metadata update complete project=%s elapsed=%s", project, h.config.Now().UTC().Sub(started))
	logging.Debug(h.projectLogger(project), "metadata update complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started))

	// Shared read-only pointer under the COW discipline (see the doc
	// comment above): no Clone, no RebuildIndexes — publishTreeLocked
	// already rebuilt the candidate's indexes before the swap.
	pm.mu.RLock()
	out := pm.meta
	pm.mu.RUnlock()
	return out, nil
}

// hydrateProjectForTx applies the cold-cache guard: a freshly created
// projectMetadata starts EMPTY, and applying a mutation to that empty tree
// would commit it over real remote state. Load remote truth first; only a
// confirmed-new project may proceed on an empty tree. Caller holds pm.mu;
// the lock is dropped and re-acquired around the remote load. Returns with
// pm.mu HELD on every path (the caller unlocks on error).
func (h *StorHub) hydrateProjectForTx(ctx context.Context, project string, pm *projectMetadata) error {
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
		// Confirmed-new project: empty tree is the truth.
		pm.hydrated = true
	default:
		return fmt.Errorf("hydrate metadata before mutation: %w", loadErr)
	}
	return nil
}

// admitCandidateSplit enforces the size ceiling on a sealed candidate.
// Admission is expressed for the split layout (version 5): the whole-tree
// serialized size is a cheap upper bound (incremental counter, no
// allocation-heavy encode) — if the entire tree serializes under the
// contents-API limit, every object (a strict subset) does too, so the
// mutation is admitted without building the tree. Only when the size
// breaches the limit do we pay for a BuildTree to find whether a SINGLE
// object (one enormous directory) is the culprit; a tree that merely exceeds
// the old blob ceiling but splits into small objects is admitted, because
// the split removed that ceiling. Shrinks always stay open. Fail-fast: once
// the ceiling is armed, a growth mutation can never commit — reject it here
// instead of paying the full BuildTree + publish cycle on every trigger.
//
// The O(tree) BuildTree probe runs OFF pm.mu (it reads only the private
// candidate): holding the exclusive lock across it stalls same-project
// readers. admitVersion is pm.version captured before the probe; when the
// re-acquired version differs, a concurrent mutation landed mid-probe. The
// probe result is still valid (it measures only the private candidate), but
// the sizeCapped flag is re-read fresh below so a concurrent breach (or
// relief) is honored. Caller holds pm.mu on entry; returns with pm.mu HELD
// on every path (the caller unlocks on error).
func (h *StorHub) admitCandidateSplit(project string, pm *projectMetadata, candidate *RepoMetadata, beforeSize, afterSize int64, admitVersion uint64, message string, started time.Time) error {
	if afterSize <= maxMetadataBytes {
		pm.sizeCapped = false
		return nil
	}
	shrinking := afterSize < beforeSize
	// Fail-fast: once the ceiling is armed, a growth mutation
	// can never commit - reject it here instead of paying the full
	// BuildTree + publish cycle on every trigger. Shrinks stay open so
	// the project can always fold back under the ceiling.
	if pm.sizeCapped && !shrinking {
		h.debugf("metadata update rejected project=%s step=admission-capped bytes=%d elapsed=%s", project, afterSize, h.config.Now().UTC().Sub(started))
		logging.Error(h.projectLogger(project), "metadata update rejected: project is over the size ceiling; growth mutations fail fast", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "bytes", afterSize, "max", maxMetadataBytes)
		return fmt.Errorf("metadata over size ceiling (%d bytes, max %d): growth is rejected until the tree fits again; delete entries or run `storhub prune`", afterSize, maxMetadataBytes)
	}
	if shrinking {
		pm.sizeCapped = false
		return nil
	}
	pm.mu.Unlock()
	res, berr := metadata.BuildTree(candidate)
	pm.mu.Lock()
	if pm.version != admitVersion {
		// A concurrent transaction published while the probe ran. The
		// probe still measures only our private candidate, so the
		// oversize verdict below stands; the flags it feeds into are
		// re-read fresh (sizeCapped below), never the pre-probe copy.
		h.debugf("metadata admission raced a concurrent publish project=%s", project)
	}
	oversizeObject := false
	if berr == nil {
		for _, obj := range res.Objects {
			if len(obj) > maxMetadataBytes {
				oversizeObject = true
				break
			}
		}
	}
	if oversizeObject {
		pm.sizeCapped = true
		h.debugf("metadata update rejected project=%s step=admission bytes=%d elapsed=%s", project, afterSize, h.config.Now().UTC().Sub(started))
		logging.Error(h.projectLogger(project), "metadata update rejected: single index object over ceiling", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "bytes", afterSize, "max", maxMetadataBytes)
		return fmt.Errorf("metadata too large: one directory serializes past %d bytes; distribute entries across subdirectories or run purge to shrink", maxMetadataBytes)
	}
	// The tree exceeds the old blob ceiling but splits into small
	// objects: admitted under the split layout.
	pm.sizeCapped = false
	return nil
}

// publishTxLocked folds the recorded intents into the shared op stack and
// swaps the private candidate in as the shared truth via publishTreeLocked
// (which RebuildIndexes the candidate before the swap, so the published
// tree is clean and exclusively owned — no second rebuild needed). Runs only
// after admission: a rejected mutation leaves the shared stack and journal
// untouched (the recorder dies with the discarded candidate). Caller holds
// pm.mu.
func (h *StorHub) publishTxLocked(project string, pm *projectMetadata, candidate *RepoMetadata, rec *metadata.IntentRecorder, cause string, now int64) {
	// Op synthesis: fold the intents the tracked mutators recorded while fn
	// ran, so every transaction-level mutation (fs/posix ops, prune, release
	// catalog changes) lands in the op stack - rich commit messages, the
	// crash-recovery journal, and rebase all read from it.
	candidate.DetachIntentRecorder()
	for _, op := range synthesizeOpsFromIntents(pm.meta, candidate, rec, cause, now) {
		h.appendOpLocked(project, pm, op)
	}
	// Fan-out footprint comes straight from the transaction recorder:
	// every file/dir the tracked mutators touched (puts and removes),
	// harvested here where pm.mu is held, so no verb can forget it.
	var txPaths []string
	if rec != nil {
		for path := range rec.FileIntents() {
			txPaths = append(txPaths, path)
		}
		for path := range rec.DirIntents() {
			txPaths = append(txPaths, path)
		}
	}
	publishTreeLocked(pm, candidate, txPaths)
}

// ValidateProjectName rejects project names outside the allowed shape.
func (h *StorHub) ValidateProjectName(project string) error {
	return validateProject(project)
}

// EnsureRepoContext creates the project repo when absent.
func (h *StorHub) EnsureRepoContext(ctx context.Context, project string) error {
	return h.ensureRepo(ctx, project)
}

// LoadRepoMetadataContext loads tracked project metadata plus its revision.
func (h *StorHub) LoadRepoMetadataContext(ctx context.Context, project string) (*metadata.RepoMetadata, string, error) {
	return h.loadRepoMetadata(ctx, project)
}

// FileNotFound returns the not-found error for path.
func (h *StorHub) FileNotFound(path string) error {
	return shfs.NotFound(path)
}

// DefaultFileMode returns the creation mode for kind.
func (h *StorHub) DefaultFileMode(kind metadata.NodeKind) uint32 {
	return defaultFileMode(kind)
}

// DefaultOwnerIDs returns the default uid and gid for new entries.
func (h *StorHub) DefaultOwnerIDs() (uint32, uint32) {
	return defaultOwnerIDs()
}

// AtimePolicy returns the effective atime update policy.
func (h *StorHub) AtimePolicy() storcfg.AtimePolicy {
	return h.config.AtimePolicy
}
