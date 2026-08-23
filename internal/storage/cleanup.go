package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	shfs "github.com/FarelRA/storhub/internal/fs"
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

func (h *StorHub) DeleteFileContext(ctx context.Context, project, fileName string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	if strings.TrimSpace(fileName) == "" {
		return errors.New("file name is required")
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return err
	}
	// Load remote metadata first: on a cold cache the in-memory view is
	// empty and deleting an existing file would wrongly report NotFound.
	if _, _, err := h.loadRepoMetadataReadonly(ctx, project); err != nil {
		return err
	}

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

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
	// delete failed.
	now := h.config.Now().Unix()
	shfs.TouchParentDirectory(pm.meta, cleanName, now)
	if len(pm.meta.FindFilesByInode(existing.Inode)) > 0 {
		if err := implposix.TouchInodeFamilyChangedAt(pm.meta, existing.Inode, now); err != nil {
			pm.mu.Unlock()
			return err
		}
	}
	if !pm.meta.RemoveFile(cleanName) {
		pm.mu.Unlock()
		return shfs.NotFound(cleanName)
	}
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	select {
	case pm.triggerCh <- struct{}{}:
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

	if !pm.meta.RemoveRelease(tag) {
		pm.mu.Unlock()
		return shfs.NotFound(fmt.Sprintf("release %s", tag))
	}
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	select {
	case pm.triggerCh <- struct{}{}:
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
	before := repoMeta.Clone()
	before.Normalize(project, h.config.Now().Unix())
	repoMeta.RecomputeStats()
	repoMeta.Normalize(project, h.config.Now().Unix())
	beforePayload, beforeErr := before.ToJSON()
	afterPayload, afterErr := repoMeta.ToJSON()
	if beforeErr == nil && afterErr == nil && bytes.Equal(beforePayload, afterPayload) {
		return nil
	}
	_, _, err = h.commitRepoMetadata(ctx, project, *repoMeta, repoMetaSHA, "storhub: cleanup metadata")
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

func (h *StorHub) PurgeUntracked(project string) (*PurgeResult, error) {
	return h.PurgeUntrackedContext(context.Background(), project)
}

func (h *StorHub) PurgeUntrackedContext(ctx context.Context, project string) (*PurgeResult, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	releases, err := h.listReleases(ctx, project)
	if err != nil {
		return nil, err
	}
	trackedReleases := make(map[string]struct{}, len(repoMeta.Releases))
	trackedAssets := make(map[int64]struct{})
	for tag := range repoMeta.Releases {
		trackedReleases[tag] = struct{}{}
	}
	for _, file := range repoMeta.Files {
		for _, chunkName := range file.Chunks {
			if chunk, ok := repoMeta.Chunks[chunkName]; ok {
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
	type deleteRelease struct {
		id  int64
		tag string
	}
	type deleteAsset struct {
		id int64
	}
	var releaseTasks []deleteRelease
	var assetTasks []deleteAsset
	for _, release := range releases {
		if _, ok := trackedReleases[release.TagName]; !ok {
			releaseTasks = append(releaseTasks, deleteRelease{id: release.ID, tag: release.TagName})
			continue
		}
		for _, asset := range release.Assets {
			if _, ok := trackedAssets[asset.ID]; ok {
				continue
			}
			assetTasks = append(assetTasks, deleteAsset{id: asset.ID})
		}
	}
	result := &PurgeResult{}
	for _, task := range releaseTasks {
		if err := h.deleteReleaseByID(ctx, project, task.id); err != nil {
			return nil, fmt.Errorf("delete untracked release %s: %w", task.tag, err)
		}
	}
	result.DeletedReleases = len(releaseTasks)
	for _, task := range assetTasks {
		if err := h.deleteAssetByID(ctx, project, task.id); err != nil {
			return nil, fmt.Errorf("delete untracked asset %d: %w", task.id, err)
		}
	}
	result.DeletedAssets = len(assetTasks)

	// Drop chunk records nothing references anymore. This is only safe at
	// this exact point: the purge above has reclaimed the remote assets of
	// unreferenced chunks, so no retained revision can still download them
	// (rollback across a purge is already destructive by design). Without
	// pruning, the catalog grows monotonically until metadata hits the
	// size ceiling and every subsequent commit fails permanently.
	var pruned int
	if _, err := h.UpdateRepoMetadataContext(ctx, project, func(meta *metadata.RepoMetadata) error {
		pruned = meta.PruneUnreferencedChunks()
		return nil
	}, "storhub: prune unreferenced chunks"); err != nil {
		return nil, fmt.Errorf("prune unreferenced chunks: %w", err)
	}
	if pruned > 0 {
		logging.Info(h.projectLogger(project), "pruned unreferenced chunks", "count", pruned)
	}
	// Commit the prune synchronously so the squash below cannot race it and
	// preserve a stale catalog in HEAD.
	if err := h.commitProjectMetadata(ctx, project, h.getOrCreateProjectMeta(project)); err != nil {
		return nil, fmt.Errorf("commit pruned metadata: %w", err)
	}

	// Squash the entire metadata git history into a single orphan commit.
	// Since we cannot roll back individual files (content-addressed storage),
	// the commit history serves no purpose other than consuming space.
	if repo := h.getGitRepo(project); repo != nil {
		if err := h.ensureOwner(ctx); err != nil {
			return nil, err
		}
		if err := repo.squashHistory(ctx, metadataFilePath, "storhub: squash metadata history"); err != nil {
			return nil, fmt.Errorf("squash metadata history: %w", err)
		}
	}

	return result, nil
}
