package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	implposix "github.com/FarelRA/storhub/internal/posix"
	"strings"
)

// DeleteFile deletes the named file using background context.

// DeleteFileContext deletes the named file honoring mutate options.
func (h *StorHub) DeleteFileContext(ctx context.Context, project, fileName string, opts ...shfs.MutateOption) error {
	if err := h.admitMutation(project); err != nil {
		return err
	}
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return err
	}
	ctx = gateRevisionFromOpts(ctx, opts)
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
	cleanName, traversed, err := shfs.LstatResolveTracked(repoMeta, fileName)
	if err != nil {
		return err
	}

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	// Hydration pre-flight on the shared tree: without it this path relied
	// on the readonly preload above having populated the cache as a side
	// effect. Checks below run against hydrated truth, matching every
	// other direct mutation site. Deliberately not the full
	// ensureMutableLocked: its size-ceiling gate would refuse deletes on a
	// capped project, and deleting is the documented escape hatch from
	// capped state (the shared admission spells it admitCandidateSplit
	// with deleteEscape).
	if err := h.ensureHydratedLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		return err
	}

	if err := shfs.CheckWalkResolved(ctx, pm.meta, traversed); err != nil {
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
	// In-transaction CAS gate: the token check runs here, in the same
	// critical section as the removal below. Placed after the existence
	// checks so a missing file still reports NotFound, and before the COW
	// copy so a rejected CAS publishes nothing (never partial
	// application).
	if err := h.checkRevisionGateLocked(pm, revisionGateFromContext(ctx)); err != nil {
		pm.mu.Unlock()
		return err
	}
	// Run every fallible operation before the irreversible removal so an
	// error can never leave the file deleted while the caller believes the
	// delete failed. All mutations apply to a private COW copy; the shared
	// tree is swapped in only once they have all succeeded.
	now := h.config.Now().UnixNano()
	tree := cloneForWrite(pm.meta)
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

// DeleteRelease deletes the named release using background context.
func (h *StorHub) DeleteRelease(project, tag string) error {
	return h.DeleteReleaseContext(context.Background(), project, tag)
}

// DeleteReleaseContext deletes the named release.
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
	tree := cloneForWrite(pm.meta)
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

// CleanupProject purges unreferenced data using background context.
func (h *StorHub) CleanupProject(project string) error {
	return h.CleanupProjectContext(context.Background(), project)
}

// CleanupProjectContext purges unreferenced data for the project.
func (h *StorHub) CleanupProjectContext(ctx context.Context, project string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	repoMeta, repoMetaSHA, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return err
	}
	// Cheap no-op check first: SerializedSize is the engine's incremental
	// counter (no encode), so a size mismatch proves work is needed
	// without paying for marshals; only when the sizes match do we pay
	// for the marshal pair to rule out a same-size-but-different tree.
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

// DeleteProject deletes the whole project using background context.
func (h *StorHub) DeleteProject(project string) error {
	return h.DeleteProjectContext(context.Background(), project)
}

// DeleteProjectContext deletes the whole project.
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
// the cheap, common case, a visibly dirty tree, while true
// concurrent-write-during-purge remains callers-must-quiesce territory.
