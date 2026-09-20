package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

func (h *StorHub) DeleteFile(project, fileName string) error {
	return h.DeleteFileContext(context.Background(), project, fileName)
}

func (h *StorHub) DeleteFileContext(ctx context.Context, project, fileName string, opts ...shfs.MutateOption) error {
	if err := h.admitMutation(project); err != nil {
		return err
	}
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return err
	}
	if err := validateProject(project); err != nil {
		return err
	}
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return err
	}
	// Load remote metadata first: on a cold cache the in-memory view is
	// empty and deleting an existing file would wrongly report NotFound.
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return err
	}
	// unlink(2) removes the final component itself: a symlink is unlinked,
	// never followed to its target, so followFinal is false.
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, false)
	if err != nil {
		return err
	}

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	if err := shfs.CheckTraversal(ctx, pm.meta, traversed); err != nil {
		pm.mu.Unlock()
		return err
	}
	if err := shfs.CheckParentWrite(ctx, pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		return err
	}
	if err := shfs.CheckStickyDelete(ctx, pm.meta, shfs.ParentPath(cleanName), cleanName); err != nil {
		pm.mu.Unlock()
		return err
	}
	if pm.meta.HasDirectory(cleanName) {
		pm.mu.Unlock()
		return shfs.IsDirectory(cleanName)
	}
	existing := pm.meta.FindFile(cleanName)
	if existing == nil {
		pm.mu.Unlock()
		return shfs.NotFound(cleanName)
	}
	// Run every fallible operation before the irreversible removal so an
	// error can never leave the file deleted while the caller believes the
	// delete failed. All mutations apply to a private COW copy; the shared
	// tree is swapped in only once they have all succeeded.
	now := h.config.Now().UnixNano()
	tree := cowTree(pm.meta)
	shfs.TouchParentDirectory(tree, cleanName, now)
	if len(tree.FindFilesByInode(existing.Inode)) > 0 {
		if err := implposix.TouchInodeFamilyChangedAt(tree, existing.Inode, now); err != nil {
			pm.mu.Unlock()
			return err
		}
	}
	if !tree.RemoveFile(cleanName) {
		pm.mu.Unlock()
		return shfs.NotFound(cleanName)
	}
	// Capture the surviving family members before publishing (FindFilesByInode
	// rebuilds indexes, which must not happen on the shared tree).
	siblings := tree.FindFilesByInode(existing.Inode)
	publishTreeLocked(pm, tree, []string{cleanName})
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	h.appendOpLocked(project, pm, Op{
		Type: OpDeleteFile, Paths: []string{cleanName}, Cause: "unlink",
		Timestamp: now, FreedChunks: len(existing.Chunks),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, existing.Inode, cleanName, "unlink-family", now)
	h.emitParentDirOpLocked(project, pm, cleanName, "unlink-parent", now)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	return nil
}

func (h *StorHub) DeleteRelease(project, tag string) error {
	return h.DeleteReleaseContext(context.Background(), project, tag)
}

func (h *StorHub) DeleteReleaseContext(ctx context.Context, project, tag string) error {
	if err := h.admitMutation(project); err != nil {
		return err
	}
	if err := validateProject(project); err != nil {
		return err
	}
	if strings.TrimSpace(tag) == "" {
		return errors.New("release tag is required")
	}
	// Load remote metadata first so a cold cache cannot report NotFound for
	// a release that exists remotely.
	if _, _, err := h.loadRepoMetadataReadonly(ctx, project); err != nil {
		return err
	}

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	if _, ok := pm.meta.Releases()[tag]; !ok {
		pm.mu.Unlock()
		return shfs.NotFound(fmt.Sprintf("release %s", tag))
	}
	tree := cowTree(pm.meta)
	if !tree.RemoveRelease(tag) {
		pm.mu.Unlock()
		return shfs.NotFound(fmt.Sprintf("release %s", tag))
	}
	// Release catalog changes have no namespace footprint: nothing for
	// cross-surface caches to invalidate.
	publishTreeLocked(pm, tree, []string{})
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	h.appendOpLocked(project, pm, Op{
		Type: OpRelease, Paths: []string{tag}, Tag: tag, Cause: "release-delete",
		Timestamp: h.config.Now().UnixNano(),
	})
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	return nil
}

func (h *StorHub) CleanupProject(project string) error {
	return h.CleanupProjectContext(context.Background(), project)
}

func (h *StorHub) CleanupProjectContext(ctx context.Context, project string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	repoMeta, repoMetaSHA, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return err
	}
	// Cheap no-op check first (audit 33): the old code paid 2 clones +
	// Normalize/RecomputeStats + 2 full ToJSON marshals just to test
	// no-op-ness. SerializedSize is the engine's incremental counter
	// (no encode); only when the sizes match do we pay for the marshal
	// pair to rule out a same-size-but-different tree.
	before := repoMeta.Clone()
	before.Normalize(project, h.config.Now().UnixNano())
	working := repoMeta.Clone()
	working.RecomputeStats()
	working.Normalize(project, h.config.Now().UnixNano())
	beforeSize, beforeErr := before.SerializedSize()
	afterSize, afterErr := working.SerializedSize()
	if beforeErr == nil && afterErr == nil && beforeSize != afterSize {
		_, _, err = h.commitRepoMetadata(ctx, project, working, repoMetaSHA, "storhub: cleanup metadata")
		return err
	}
	// The loaded tree may be the hub's shared snapshot: normalize a private
	// copy and commit that, never the shared one.
	beforePayload, beforeErr := before.ToJSON()
	afterPayload, afterErr := working.ToJSON()
	if beforeErr == nil && afterErr == nil && bytes.Equal(beforePayload, afterPayload) {
		return nil
	}
	_, _, err = h.commitRepoMetadata(ctx, project, working, repoMetaSHA, "storhub: cleanup metadata")
	return err
}

func (h *StorHub) DeleteProject(project string) error {
	return h.DeleteProjectContext(context.Background(), project)
}

func (h *StorHub) DeleteProjectContext(ctx context.Context, project string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	return h.deleteRepo(ctx, project)
}

// projectHasUncommittedState reports whether the project has local
// metadata mutations that have not been committed yet. It is a cheap,
// lock-only check (no I/O) suitable as a pre-flight gate.
func (h *StorHub) projectHasUncommittedState(project string) bool {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return false
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.dirty
}

// NOTE (residual race, accepted): the dirty gate above is check-then-act,
// so a mutation can land after the check passes, and a concurrent upload
// can reference a release between purge's classification and its delete.
// Closing that window needs a purge-wide write fence (or a server-side
// lease), which this codebase has no primitive for yet. The gate removes
// the cheap, common case — a visibly dirty tree — while true
// concurrent-write-during-purge remains callers-must-quiesce territory.

// purge task types, hoisted so the purge tail and the prune-assets dry-run
// share one classification (audit 18: dry-run must report would-delete
// counts, not zeros).
type purgeReleaseTask struct {
	id  int64
	tag string
}

type purgeAssetTask struct {
	id int64
}

// purgeIsRetryable gates purge sub-ops: only APIError-marked retryables
// (429/5xx per IsRetryable) wait out the advertised window and retry.
func purgeIsRetryable(err error) bool {
	var apiErr *ghapi.APIError
	return errors.As(err, &apiErr) && apiErr.IsRetryable()
}

func pruneAssetsLive(h *StorHub, ctx context.Context, project string, res *PruneResult) error {
	if err := validateProject(project); err != nil {
		return err
	}
	// Fail closed on in-flight state: a dirty tree means a mutation is
	// still converging, and classifying against it risks deleting
	// releases a pending commit is about to reference. Flush first,
	// then purge.
	if h.projectHasUncommittedState(project) {
		return fmt.Errorf("purge refused for project %s: uncommitted metadata changes pending; flush before purging", project)
	}
	releaseTasks, assetTasks, err := h.classifyUntracked(ctx, project)
	if err != nil {
		return err
	}
	// Optimistic fence against concurrent writers landing between
	// classification and deletion (audit: purge check-then-act race).
	releaseTasks, assetTasks, err = h.reverifyPurgePlan(ctx, project, releaseTasks, assetTasks)
	if err != nil {
		return err
	}
	if err := h.deletePurgePlan(ctx, project, releaseTasks, assetTasks, res); err != nil {
		return err
	}
	// Drop chunk records and squash only when something actually changed
	// (audit 18): the old tail ran UpdateRepoMetadataContext +
	// commitProjectMetadata unconditionally, so a no-op purge still
	// dirtied the tree and landed a manifest commit per run.
	if err := h.purgeAndSquashUntracked(ctx, project, len(releaseTasks)+len(assetTasks) > 0); err != nil {
		return err
	}
	return nil
}

// classifyUntracked loads fresh truth (never the cached snapshot: files
// committed after the local cache was populated must be visible, or purge
// deletes releases that are live remotely) and sorts every release/asset
// into tracked vs orphaned. No deletes happen here, so prune-assets
// dry-run reuses it to report would-delete counts.
func (h *StorHub) classifyUntracked(ctx context.Context, project string) (releaseTasks []purgeReleaseTask, assetTasks []purgeAssetTask, err error) {
	var repoMeta *metadata.RepoMetadata
	var releases []ghapi.Release
	if err := h.withRetry(ctx, "purge-load_metadata", 5, purgeIsRetryable, func() error {
		var err error
		// This also refreshes the cache, so the prune/commit tail below
		// operates on truth.
		repoMeta, _, err = h.loadRepoMetadataFresh(ctx, project)
		return err
	}); err != nil {
		return nil, nil, err
	}
	if err := h.withRetry(ctx, "purge-list_releases", 5, purgeIsRetryable, func() error {
		var err error
		releases, err = h.listReleases(ctx, project)
		return err
	}); err != nil {
		return nil, nil, err
	}
	trackedReleases, trackedAssets := trackedPurgeSets(repoMeta)
	for _, release := range releases {
		if _, ok := trackedReleases[release.TagName]; !ok {
			// An empty release holds no orphaned storage, so there is
			// nothing to reclaim and no reason to drop it. Fresh rotation
			// targets awaiting their first upload are exactly such
			// empties; deleting them destroys curated headroom.
			//
			// Fail closed on a count error, consistent with the upload
			// picker: a release whose size cannot be determined must be
			// skipped, never deleted. Deleting on a read error turns a
			// transient GitHub outage into permanent data loss.
			count, countErr := h.releaseAssetCount(ctx, project, release)
			if countErr != nil {
				logging.Warn(h.projectLogger("purge"), "purge skipping release with unknown asset count", "tag", release.TagName, "err", countErr)
				continue
			}
			if count == 0 {
				continue
			}
			releaseTasks = append(releaseTasks, purgeReleaseTask{id: release.ID, tag: release.TagName})
			continue
		}
		for _, asset := range release.Assets {
			if _, ok := trackedAssets[asset.ID]; ok {
				continue
			}
			assetTasks = append(assetTasks, purgeAssetTask{id: asset.ID})
		}
	}
	return releaseTasks, assetTasks, nil
}

// trackedPurgeSets computes the tracked release tags and asset IDs from
// fresh metadata truth: the single definition of "live" shared by
// classification and re-verification, so the two can never disagree on
// what counts as referenced.
func trackedPurgeSets(repoMeta *RepoMetadata) (trackedReleases map[string]struct{}, trackedAssets map[int64]struct{}) {
	trackedReleases = make(map[string]struct{}, len(repoMeta.Releases()))
	trackedAssets = make(map[int64]struct{})
	for tag := range repoMeta.Releases() {
		trackedReleases[tag] = struct{}{}
	}
	for _, file := range repoMeta.Files() {
		for _, chunkName := range file.Chunks {
			if chunk, ok := repoMeta.Chunks()[chunkName]; ok {
				trackedAssets[chunk.AssetID] = struct{}{}
				// A release referenced by any live chunk is tracked even if
				// the release catalog drifted (e.g. a crash between upload
				// and metadata commit); deleting it would cascade-delete
				// assets the file still needs.
				if chunk.Release != "" {
					trackedReleases[chunk.Release] = struct{}{}
				}
			}
		}
	}
	return trackedReleases, trackedAssets
}

// reverifyPurgePlan closes the classify-to-delete race: a commit landing
// between classification and deletion can reference a task's release or
// asset (crash-recovery uploads finishing late, concurrent writers),
// turning a correct classification into data loss. Reload fresh truth and
// drop tasks that became tracked; fail closed (abort the purge) when the
// reload itself fails rather than deleting on stale classification. The
// residual window (re-verify to DELETE call) is milliseconds of network,
// not seconds of classification — GitHub offers no CAS on releases or
// assets to close it fully, which is documented here, not solved.
func (h *StorHub) reverifyPurgePlan(ctx context.Context, project string, releaseTasks []purgeReleaseTask, assetTasks []purgeAssetTask) ([]purgeReleaseTask, []purgeAssetTask, error) {
	if len(releaseTasks) == 0 && len(assetTasks) == 0 {
		return nil, nil, nil
	}
	fresh, _, err := h.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		return nil, nil, fmt.Errorf("purge re-verify: %w", err)
	}
	trackedReleases, trackedAssets := trackedPurgeSets(fresh)
	// Fresh slices, not in-place filters: the caller's task lists stay
	// intact for logging/retry, and no aliasing subtlety survives.
	var keptReleases []purgeReleaseTask
	for _, task := range releaseTasks {
		if _, ok := trackedReleases[task.tag]; ok {
			continue
		}
		keptReleases = append(keptReleases, task)
	}
	var keptAssets []purgeAssetTask
	for _, task := range assetTasks {
		if _, ok := trackedAssets[task.id]; ok {
			continue
		}
		keptAssets = append(keptAssets, task)
	}
	if dropped := len(releaseTasks) - len(keptReleases) + len(assetTasks) - len(keptAssets); dropped > 0 {
		logging.Warn(h.projectLogger(project), "purge re-verify dropped newly-tracked tasks",
			"dropped_releases", len(releaseTasks)-len(keptReleases), "dropped_assets", len(assetTasks)-len(keptAssets))
	}
	return keptReleases, keptAssets, nil
}

// deletePurgePlan executes the classified deletes and records the counts.
// Every delete is fenced individually: before touching remote state it
// checks the truth version, and on any movement since the last check it
// rebuilds the tracked sets once from fresh truth and drops newly-tracked
// remainders loudly. A concurrent writer landing mid-loop therefore spares
// its data instead of racing the delete — the old whole-plan reverify
// could not see writes that landed after it ran. A delete failure aborts
// with the original task context (no partial counts reported on error,
// matching the old behavior); skipped tasks are reported in Notes, never
// silently dropped.
func (h *StorHub) deletePurgePlan(ctx context.Context, project string, releaseTasks []purgeReleaseTask, assetTasks []purgeAssetTask, result *PruneResult) error {
	fence := h.purgeFence(project)
	skippedReleases, skippedAssets := 0, 0
	for _, task := range releaseTasks {
		keep, err := fence.checkRelease(ctx, project, task)
		if err != nil {
			return err
		}
		if !keep {
			skippedReleases++
			continue
		}
		if err := h.withRetry(ctx, "purge-delete_release", 5, purgeIsRetryable, func() error {
			return h.deleteReleaseByID(ctx, project, task.id)
		}); err != nil {
			return fmt.Errorf("delete untracked release %s: %w", task.tag, err)
		}
		result.DeletedReleases++
	}
	for _, task := range assetTasks {
		keep, err := fence.checkAsset(ctx, project, task)
		if err != nil {
			return err
		}
		if !keep {
			skippedAssets++
			continue
		}
		if err := h.withRetry(ctx, "purge-delete_asset", 5, purgeIsRetryable, func() error {
			return h.deleteAssetByID(ctx, project, task.id)
		}); err != nil {
			return fmt.Errorf("delete untracked asset %d: %w", task.id, err)
		}
		result.DeletedAssets++
	}
	if skippedReleases+skippedAssets > 0 {
		result.Notes = append(result.Notes, fmt.Sprintf("spared %d releases and %d assets re-tracked by concurrent writes during the purge", skippedReleases, skippedAssets))
	}
	return nil
}

// purgeFence is the per-delete optimistic fence for one deletePurgePlan
// run. It watches the project's truth version (bumped on every shared
// truth swap): while nothing moves, deletes proceed with zero extra
// reads; the first movement rebuilds the tracked sets once from fresh
// truth and every later check consults them. Lookups never create cache
// entries (lookupProjectMeta, not getOrCreate): an evicted project
// simply revalidates from scratch.
type purgeFence struct {
	hub      *StorHub
	version  uint64
	haveBase bool
	releases map[string]struct{}
	assets   map[int64]struct{}
	loaded   bool
}

func (h *StorHub) purgeFence(project string) *purgeFence {
	f := &purgeFence{hub: h}
	if pm := h.lookupProjectMeta(project); pm != nil {
		pm.mu.RLock()
		f.version, f.haveBase = pm.version, true
		pm.mu.RUnlock()
	}
	return f
}

// revalidate rebuilds the tracked sets from fresh truth. It runs at most
// when the version moved (or no baseline existed); callers consult the
// loaded maps after.
func (f *purgeFence) revalidate(ctx context.Context, project string) error {
	fresh, _, err := f.hub.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		return fmt.Errorf("purge fence revalidate: %w", err)
	}
	f.releases, f.assets = trackedPurgeSets(fresh)
	f.loaded = true
	if pm := f.hub.lookupProjectMeta(project); pm != nil {
		pm.mu.RLock()
		f.version, f.haveBase = pm.version, true
		pm.mu.RUnlock()
	}
	return nil
}

// current reports whether shared truth moved since the baseline. An
// absent baseline (evicted project) always counts as moved: safety
// defaults to revalidating, never to assuming stillness.
func (f *purgeFence) current(project string) bool {
	if !f.haveBase {
		return false
	}
	pm := f.hub.lookupProjectMeta(project)
	if pm == nil {
		return false
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.version == f.version
}

func (f *purgeFence) checkRelease(ctx context.Context, project string, task purgeReleaseTask) (bool, error) {
	if !f.loaded || !f.current(project) {
		if err := f.revalidate(ctx, project); err != nil {
			return false, err
		}
	}
	_, ok := f.releases[task.tag]
	return !ok, nil
}

func (f *purgeFence) checkAsset(ctx context.Context, project string, task purgeAssetTask) (bool, error) {
	if !f.loaded || !f.current(project) {
		if err := f.revalidate(ctx, project); err != nil {
			return false, err
		}
	}
	_, ok := f.assets[task.id]
	return !ok, nil
}

// purgeAndSquashUntracked drops chunk records nothing references anymore
// and squashes legacy history. This is only safe at this exact point: the
// deletes above reclaimed the remote assets of unreferenced chunks, so no
// retained revision can still download them (rollback across a purge is
// already destructive by design). Without pruning, the catalog grows
// monotonically until metadata hits the size ceiling and every subsequent
// commit fails permanently.
//
// hadDeletes reports whether the delete phase removed anything. When it
// did not, a read-only probe decides: UpdateRepoMetadataContext marks the
// tree dirty unconditionally, so calling it for a zero-prune run would
// still land a manifest commit. Skipping the Update/commit/squash tail on
// a true no-op keeps purge side-effect free.
func (h *StorHub) purgeAndSquashUntracked(ctx context.Context, project string, hadDeletes bool) error {
	if !hadDeletes {
		// Read-only no-op probe on fresh truth: a clone the Update never
		// sees, so the shared tree stays clean when there is nothing to
		// reclaim. (A concurrent mutation racing the probe only means the
		// later Update finds work — never a missed delete.)
		probeMeta, _, err := h.loadRepoMetadataFresh(ctx, project)
		if err != nil {
			return err
		}
		if probe := probeMeta.Clone(); probe.PruneUnreferencedChunks() == 0 {
			return nil
		}
	}
	var pruned int
	if err := h.withRetry(ctx, "purge-prune_chunks", 5, purgeIsRetryable, func() error {
		_, perr := h.UpdateRepoMetadataContext(ctx, project, func(meta *metadata.RepoMetadata) error {
			pruned = meta.PruneUnreferencedChunks()
			return nil
		}, "storhub: prune unreferenced chunks")
		return perr
	}); err != nil {
		return fmt.Errorf("prune unreferenced chunks: %w", err)
	}
	if pruned > 0 {
		logging.Info(h.projectLogger(project), "pruned unreferenced chunks", "count", pruned)
	}
	// Commit the prune synchronously so the squash below cannot race it and
	// preserve a stale catalog in HEAD.
	if err := h.withRetry(ctx, "purge-commit_prune", 5, purgeIsRetryable, func() error {
		return h.commitProjectMetadata(ctx, project, h.getOrCreateProjectMeta(project))
	}); err != nil {
		return fmt.Errorf("commit pruned metadata: %w", err)
	}

	// Squash the entire metadata git history into a single orphan commit.
	// Since we cannot roll back individual files (content-addressed storage),
	// the commit history serves no purpose other than consuming space.
	//
	// Split (version-5) projects keep their history: rollback-as-revert depends on old
	// manifests pointing at live objects, and collapsing history here would
	// also drop the objects a single-path squash does not carry. History
	// compaction for the split layout is an explicit `storhub prune history`.
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.RLock()
	legacy := !pm.meta.IsSplit()
	pm.mu.RUnlock()
	if repo := h.getGitRepo(project); repo != nil && legacy {
		if err := h.ensureOwner(ctx); err != nil {
			return err
		}
		if err := h.withRetry(ctx, "purge-squash_history", 5, purgeIsRetryable, func() error {
			return repo.squashHistory(ctx, metadataFilePath, "storhub: squash metadata history")
		}); err != nil {
			return fmt.Errorf("squash metadata history: %w", err)
		}
	}
	return nil
}

// Item A10 chunk GC: orphan-rate/sprawl metrics plus conservative
// catalog compaction.
//
// WHAT IT RECLAIMS: chunk catalog records (ChunkInfo entries keyed by
// chunk ID) that no root references. It never touches release assets:
// the remote bytes stay until the existing purge reclaims
// them, so a catalog prune can never destroy bytes a stale reader
// still needs. Existing cleanup/purge semantics are untouched; this
// is a separate entry point sharing only the dirty-gate vocabulary.
//
// REFERENCE MARKING (roots, all must hold for collection):
//   - live files: every chunk ID in every file entry of the candidate
//     tree (the manifest-referenced set).
//   - pending ops: every File.Chunks reference plus every Chunks-map
//     key in the live opStack. The on-disk journal is the durable
//     mirror of the opStack (appendWithDelta deltas folded by foldOps),
//     so covering the live stack covers the journal: a chunk the
//     journal will replay is always referenced by the op that carries
//     it. The prune itself commits through UpdateRepoMetadataContext,
//     whose intent recorder synthesizes an OpChunkPrune whose replay
//     (applyChunkPruneOp) defensively skips still-referenced IDs, so a
//     crash between delete and commit replays safe.
//   - in-flight commit snapshots: snapshots are copies of opStack ops
//     still present in the live stack (cleared only by clearUpTo on
//     success), hence covered by the pending-ops union above.
//   - baseTree (last committed tree): NOT a root. The live tree already
//     is baseTree plus applied ops; nothing restores baseTree wholesale
//     (a rollback to an older revision restores that revision's whole
//     catalog with it, so collecting a live-orphan an old revision
//     names is safe).
//   - pinned sessions: NOT scanned as roots; instead any live session
//     handle for the project refuses the compaction outright (fail
//     loud, typed). Sessions pin only file-referenced IDs copied at
//     open and read through their own copy, so the refusal is strictly
//     conservative. WHY refusal over union: the session table lock and
//     pm.mu have no defined order, and a handle can publish between a
//     pre-scan and the delete; refusal plus the txn discipline below
//     closes that window without inventing a lock order.
//
// COMMIT-RACE DISCIPLINE: classification and deletion happen inside ONE
// UpdateRepoMetadataContext transaction holding pm.mu exclusively. A
// concurrent writer either lands before the txn (its IDs are visible
// in the candidate tree or the opStack union) or blocks until after
// (its fresh AllocateChunkID values postdate the delete set). A chunk
// referenced mid-compaction therefore always survives. Session opens
// pin from a tree read that serializes against the same exclusive
// lock, so a racing open pins post-GC state.
//
// DEFAULT POSTURE: OFF. There is no background trigger, no threshold,
// no auto-run: compaction is an explicit operator call, dry-run first.
// WHY OFF: this codebase has no purge-wide write fence (see the purge
// residual-race note), and silent automatic deletion of catalog
// records risks exactly the unforgivable failure. The operator runs
// ScanChunkGC (read-only), then CompactOrphanChunks dry-run, reads the
// per-object log, then compacts for real.

// ChunkGCResult reports what a scan saw or a compaction did (or would
// do under DryRun). DeletedIDs is sorted ascending for stable logs.
type ChunkGCResult struct {
	DryRun           bool    `json:"dry_run"`
	ScannedChunks    int     `json:"scanned_chunks"`
	OrphanChunks     int     `json:"orphan_chunks"`
	OrphanBytes      int64   `json:"orphan_bytes"`
	CollectedChunks  int     `json:"collected_chunks"`
	CollectedBytes   int64   `json:"collected_bytes"`
	DeletedIDs       []int64 `json:"deleted_ids,omitempty"`
	RefusedBySession bool    `json:"refused_by_session,omitempty"`
}

// ChunkGCRefusedError is the loud typed refusal: compaction never
// deletes silently under doubt (live session, bad project).
type ChunkGCRefusedError struct {
	Project string
	Reason  string
}

func (e *ChunkGCRefusedError) Error() string {
	return fmt.Sprintf("chunk GC refused for project %s: %s", e.Project, e.Reason)
}

// errChunkGCNoop aborts a compaction transaction with no publish when
// classification finds zero orphans. Internal sentinel, never surfaced:
// the caller translates it into a zero-result success so a no-op run
// stays side-effect free (no dirty mark, no empty commit).
var errChunkGCNoop = errors.New("chunk GC: nothing to collect")

// chunkGCHasLiveSession reports whether any non-destroyed session
// handle names the project. Conservative: expiry is ignored, a handle
// present in the table blocks until it is closed or reaped.
func (h *StorHub) chunkGCHasLiveSession(project string) bool {
	sh := h.sessionHub()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for _, s := range sh.byID {
		if s == nil || s.destroyed {
			continue
		}
		if s.project == project {
			return true
		}
	}
	return false
}

// chunkGCRoots unions the live-tree file references with the pending-op
// references (file payloads plus catalog keys). ops is the live stack
// copy; callers holding pm.mu may pass the stack directly.
func chunkGCRoots(tree *RepoMetadata, ops []Op) map[int64]struct{} {
	roots := make(map[int64]struct{})
	for _, file := range tree.Files() {
		for _, id := range file.Chunks {
			roots[id] = struct{}{}
		}
	}
	for _, op := range ops {
		if op.File != nil {
			for _, id := range op.File.Chunks {
				roots[id] = struct{}{}
			}
		}
		for id := range op.Chunks {
			roots[id] = struct{}{}
		}
	}
	return roots
}

// chunkGCClassify splits the catalog into sorted orphan IDs plus their
// unreachable byte total. Pure function, no I/O, so tests can pin the
// keep-vs-collect decision without a hub.
func chunkGCClassify(catalog map[int64]ChunkInfo, roots map[int64]struct{}) (orphans []int64, unreachable int64) {
	for id, info := range catalog {
		if _, ok := roots[id]; ok {
			continue
		}
		orphans = append(orphans, id)
		unreachable += info.Size
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i] < orphans[j] })
	return orphans, unreachable
}

// ScanChunkGC is the phase-1 read-only probe: it classifies orphans,
// publishes the scan counter plus the last-scan gauges to the pressure
// registry, logs loudly, and mutates nothing. Safe under live
// sessions (it deletes nothing, so the session gate does not apply).
func (h *StorHub) ScanChunkGC(ctx context.Context, project string) (*ChunkGCResult, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	pm := h.lookupProjectMeta(project)
	if pm == nil {
		// Untracked project: nothing cached, nothing to classify.
		// No entry is created as a side effect (read-only means
		// read-only).
		h.pressure.noteChunkGCScan()
		h.pressure.noteOrphanSnapshot(0, 0)
		return &ChunkGCResult{DryRun: true}, nil
	}
	pm.mu.RLock()
	roots := chunkGCRoots(pm.meta, append([]Op(nil), pm.opStack.ops...))
	scanned := len(pm.meta.Chunks())
	orphans, unreachable := chunkGCClassify(pm.meta.Chunks(), roots)
	pm.mu.RUnlock()

	_ = ctx
	h.pressure.noteChunkGCScan()
	h.pressure.noteOrphanSnapshot(uint64(len(orphans)), uint64(unreachable))
	logging.Info(h.projectLogger(project), "chunk GC scan",
		"scanned", scanned, "orphans", len(orphans), "unreachable_bytes", unreachable)
	return &ChunkGCResult{
		DryRun:        true,
		ScannedChunks: scanned,
		OrphanChunks:  len(orphans),
		OrphanBytes:   unreachable,
		DeletedIDs:    append([]int64(nil), orphans...),
	}, nil
}

// CompactOrphanChunks compacts unreachable chunk catalog records.
// dryRun=true classifies and logs without deleting (same numbers the
// real run would act on, barring concurrent writers). dryRun=false
// deletes inside one metadata transaction (see the race discipline
// above) and records the collection counters. A live session for the
// project refuses loudly with *ChunkGCRefusedError and deletes
// nothing. Release assets are never touched.
func (h *StorHub) CompactOrphanChunks(ctx context.Context, project string, dryRun bool) (*ChunkGCResult, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	if h.chunkGCHasLiveSession(project) {
		logging.Warn(h.projectLogger(project), "chunk GC refused: live session holds pins")
		return nil, &ChunkGCRefusedError{Project: project, Reason: "live session pins chunks; close sessions and retry"}
	}
	if dryRun {
		res, err := h.ScanChunkGC(ctx, project)
		if err != nil {
			return nil, err
		}
		res.RefusedBySession = false
		logging.Info(h.projectLogger(project), "chunk GC dry-run would collect",
			"orphans", res.OrphanChunks, "unreachable_bytes", res.OrphanBytes, "ids", res.DeletedIDs)
		return res, nil
	}
	pm := h.lookupProjectMeta(project)
	if pm == nil {
		h.pressure.noteChunkGCScan()
		h.pressure.noteOrphanSnapshot(0, 0)
		return &ChunkGCResult{}, nil
	}
	// Snapshot the pending-op roots BEFORE the transaction, under a
	// read lock. WHY not read pm.opStack inside fn: the transaction
	// resolves its own pm (an idle eviction could swap the entry
	// between our lookup and the txn), and dereferencing our possibly
	// stale pm under the txn's lock would be an unlocked read of a
	// revivable stack. The snapshot pairs with the candidate inside
	// fn; appends racing between snapshot and commit are still safe:
	// every verb mints FRESH chunk IDs (never reuses an orphan ID),
	// a racing verb serialized behind this txn looks its chunks up
	// in the post-GC candidate and fails loud ("chunk not found")
	// instead of corrupting, and the synthesized OpChunkPrune replay
	// defensively skips still-referenced IDs on crash replay.
	pm.mu.RLock()
	pendingOps := append([]Op(nil), pm.opStack.ops...)
	pm.mu.RUnlock()
	// The transaction holds pm.mu exclusively across classify+delete,
	// so concurrent writers serialize around us: a writer either
	// landed before (visible in the candidate or the snapshot above)
	// or blocks until after (fresh IDs postdate the delete set).
	var deleted []int64
	var collectedBytes int64
	var scanned int
	_, err := h.UpdateRepoMetadataContext(ctx, project, func(candidate *RepoMetadata) error {
		// No session re-check in here: the table mutex and pm.mu
		// have no defined lock order (session close/sync takes the
		// table lock then enters a metadata txn), so nesting the
		// table lock under the txn-owned pm.mu risks deadlock. The
		// single pre-check plus the serialization argument in the
		// package doc is the whole gate: a handle racing the gate
		// pins from a tree read serialized against this txn, so it
		// either blocked the run up front or pins post-GC state.
		roots := chunkGCRoots(candidate, pendingOps)
		scanned = len(candidate.Chunks())
		orphans, _ := chunkGCClassify(candidate.Chunks(), roots)
		if len(orphans) == 0 {
			// Abort the transaction with no publish: a no-op
			// compaction must stay side-effect free (no dirty
			// mark, no empty commit, no journal line).
			return errChunkGCNoop
		}
		for _, id := range orphans {
			// Defensive re-check: the opStack union above was built
			// from the same critical section, but a paranoid second
			// membership test costs nothing and turns any future
			// refactor that splits classify from delete into a
			// keep-instead-of-collect mistake, never the reverse.
			if _, ok := roots[id]; ok {
				continue
			}
			info, ok := candidate.Chunks()[id]
			if !ok {
				continue
			}
			candidate.DeleteChunk(id)
			deleted = append(deleted, id)
			collectedBytes += info.Size
		}
		return nil
	}, "storhub: chunk GC compact orphans")
	if err != nil {
		if errors.Is(err, errChunkGCNoop) {
			h.pressure.noteChunkGCScan()
			h.pressure.noteOrphanSnapshot(0, 0)
			logging.Info(h.projectLogger(project), "chunk GC compaction complete",
				"scanned", scanned, "collected", 0, "collected_bytes", 0)
			return &ChunkGCResult{ScannedChunks: scanned}, nil
		}
		return nil, err
	}
	h.pressure.noteChunkGCScan()
	h.pressure.noteOrphanSnapshot(0, 0)
	if len(deleted) > 0 {
		h.pressure.noteChunkGCCollected(uint64(len(deleted)), uint64(collectedBytes))
	}
	// Loud per-object logging: the operator reconstructs exactly what
	// one compaction removed from this project's catalog.
	for _, id := range deleted {
		logging.Info(h.projectLogger(project), "chunk GC collected orphan chunk", "chunk_id", id)
	}
	logging.Info(h.projectLogger(project), "chunk GC compaction complete",
		"scanned", scanned, "collected", len(deleted), "collected_bytes", collectedBytes)
	return &ChunkGCResult{
		ScannedChunks:   scanned,
		OrphanChunks:    len(deleted),
		OrphanBytes:     collectedBytes,
		CollectedChunks: len(deleted),
		CollectedBytes:  collectedBytes,
		DeletedIDs:      append([]int64(nil), deleted...),
	}, nil
}
