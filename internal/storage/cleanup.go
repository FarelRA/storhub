package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

type PurgeResult struct {
	DeletedReleases int `json:"deleted_releases"`
	DeletedAssets   int `json:"deleted_assets"`
}

func (h *StorHub) DeleteFile(project, fileName string) error {
	return h.DeleteFileContext(context.Background(), project, fileName)
}

func (h *StorHub) DeleteFileContext(ctx context.Context, project, fileName string, opts ...shfs.MutateOption) error {
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
	now := h.config.Now().Unix()
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
	publishTreeLocked(pm, tree)
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
	publishTreeLocked(pm, tree)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	h.appendOpLocked(project, pm, Op{
		Type: OpRelease, Paths: []string{tag}, Tag: tag, Cause: "release-delete",
		Timestamp: h.config.Now().Unix(),
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
	before.Normalize(project, h.config.Now().Unix())
	working := repoMeta.Clone()
	working.RecomputeStats()
	working.Normalize(project, h.config.Now().Unix())
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
func (h *StorHub) PurgeUntracked(project string) (*PurgeResult, error) {
	return h.PurgeUntrackedContext(context.Background(), project)
}

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

func (h *StorHub) PurgeUntrackedContext(ctx context.Context, project string) (*PurgeResult, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	// Fail closed on in-flight state: a dirty tree means a mutation is
	// still converging, and classifying (or prune-committing) against it
	// risks deleting releases a pending commit is about to reference.
	// Flush first, then purge.
	if h.projectHasUncommittedState(project) {
		return nil, fmt.Errorf("purge refused for project %s: uncommitted metadata changes pending; flush before purging", project)
	}
	releaseTasks, assetTasks, err := h.classifyUntracked(ctx, project)
	if err != nil {
		return nil, err
	}
	// Optimistic fence against concurrent writers landing between
	// classification and deletion (audit: purge check-then-act race).
	releaseTasks, assetTasks, err = h.reverifyPurgePlan(ctx, project, releaseTasks, assetTasks)
	if err != nil {
		return nil, err
	}
	result := &PurgeResult{}
	if err := h.deletePurgePlan(ctx, project, releaseTasks, assetTasks, result); err != nil {
		return nil, err
	}
	// Drop chunk records and squash only when something actually changed
	// (audit 18): the old tail ran UpdateRepoMetadataContext +
	// commitProjectMetadata unconditionally, so a no-op purge still
	// dirtied the tree and landed a manifest commit per run.
	if err := h.pruneAndSquashUntracked(ctx, project, len(releaseTasks)+len(assetTasks) > 0); err != nil {
		return nil, err
	}
	return result, nil
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
	keptReleases := releaseTasks[:0]
	for _, task := range releaseTasks {
		if _, ok := trackedReleases[task.tag]; ok {
			continue
		}
		keptReleases = append(keptReleases, task)
	}
	keptAssets := assetTasks[:0]
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
// A delete failure aborts with the original task context (no partial
// counts reported on error, matching the old behavior).
func (h *StorHub) deletePurgePlan(ctx context.Context, project string, releaseTasks []purgeReleaseTask, assetTasks []purgeAssetTask, result *PurgeResult) error {
	for _, task := range releaseTasks {
		if err := h.withRetry(ctx, "purge-delete_release", 5, purgeIsRetryable, func() error {
			return h.deleteReleaseByID(ctx, project, task.id)
		}); err != nil {
			return fmt.Errorf("delete untracked release %s: %w", task.tag, err)
		}
	}
	result.DeletedReleases = len(releaseTasks)
	for _, task := range assetTasks {
		if err := h.withRetry(ctx, "purge-delete_asset", 5, purgeIsRetryable, func() error {
			return h.deleteAssetByID(ctx, project, task.id)
		}); err != nil {
			return fmt.Errorf("delete untracked asset %d: %w", task.id, err)
		}
	}
	result.DeletedAssets = len(assetTasks)
	return nil
}

// pruneAndSquashUntracked drops chunk records nothing references anymore
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
func (h *StorHub) pruneAndSquashUntracked(ctx context.Context, project string, hadDeletes bool) error {
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
