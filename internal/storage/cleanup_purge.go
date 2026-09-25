package storage

import (
	"context"
	"errors"
	"fmt"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// purge task types, hoisted so the purge tail and the prune-assets dry-run
// share one classification (dry-run must report would-delete
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
				logging.Warn(h.projectLogger(project), "purge skipping release with unknown asset count", "tag", release.TagName, "err", countErr)
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

// liveFileChunkIDs unions every chunk ID named by the live file set: the
// one file to chunk traversal shared by the chunk collector and the asset
// classifier, so the two orphan definitions can never disagree on what a
// file references.
func liveFileChunkIDs(files map[string]FileMeta) map[int64]struct{} {
	ids := make(map[int64]struct{})
	for _, file := range files {
		for _, id := range file.Chunks {
			ids[id] = struct{}{}
		}
	}
	return ids
}

// trackedPurgeSets computes the tracked release tags and asset IDs from
// fresh metadata truth: the single definition of "live" shared by
// classification and the per-delete fence, so the two can never disagree
// on what counts as referenced. File liveness funnels through
// liveFileChunkIDs, the same traversal the chunk collector uses.
func trackedPurgeSets(repoMeta *RepoMetadata) (trackedReleases map[string]struct{}, trackedAssets map[int64]struct{}) {
	trackedReleases = make(map[string]struct{}, len(repoMeta.Releases()))
	trackedAssets = make(map[int64]struct{})
	for tag := range repoMeta.Releases() {
		trackedReleases[tag] = struct{}{}
	}
	live := liveFileChunkIDs(repoMeta.Files())
	for id := range live {
		if chunk, ok := repoMeta.Chunks()[id]; ok {
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
	return trackedReleases, trackedAssets
}

// deletePurgePlan executes the classified deletes and records the counts.
// The per-delete fence below is the single classify-to-delete gate: the
// first check revalidates from fresh truth unconditionally, so a commit
// landing between classification and deletion that re-tracks a task's
// release or asset spares it. There is no separate whole-plan reverify;
// one gate means one truth version to reason about.
// Every delete is fenced individually: before touching remote state it
// checks the truth version, and on any movement since the last check it
// rebuilds the tracked sets once from fresh truth and drops newly-tracked
// remainders loudly. A concurrent writer landing mid-loop therefore spares
// its data instead of racing the delete: every delete re-checks the truth
// version first, so writes that land after classification stay visible. A delete failure aborts
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
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "purge prune start", "scope", "assets")
	if !hadDeletes {
		// Read-only no-op probe on fresh truth: a clone the Update never
		// sees, so the shared tree stays clean when there is nothing to
		// reclaim. (A concurrent mutation racing the probe only means the
		// later Update finds work: never a missed delete.)
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
		logging.Error(h.projectLogger(project), "purge prune failed", "scope", "assets", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return fmt.Errorf("prune unreferenced chunks: %w", err)
	}
	logging.Debug(h.projectLogger(project), "purge prune complete", "scope", "assets", "count", pruned, "reclaimed", pruned, "elapsed", h.config.Now().UTC().Sub(started))
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
			return repo.squashHistoryCAS(ctx, metadataFilePath, "storhub: squash metadata history", "")
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
