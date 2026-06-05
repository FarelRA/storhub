package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	shfs "github.com/FarelRA/storhub/internal/fs"
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

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	defer pm.mu.Unlock()
	rollback := pm.meta.Clone()

	if err := shfs.CheckParentWrite(ctx, pm.meta, cleanName); err != nil {
		return err
	}
	if err := shfs.CheckStickyDelete(ctx, pm.meta, shfs.ParentPath(cleanName), cleanName); err != nil {
		return err
	}
	if pm.meta.HasDirectory(cleanName) {
		return shfs.IsDirectory(cleanName)
	}
	existing := pm.meta.FindFile(cleanName)
	if existing == nil {
		return shfs.NotFound(cleanName)
	}
	if !pm.meta.RemoveFile(cleanName) {
		return shfs.NotFound(cleanName)
	}
	if len(pm.meta.FindFilesByInode(existing.Inode)) > 0 {
		shfs.TouchParentDirectory(pm.meta, cleanName, h.config.Now().UTC())
		if err := implposix.TouchInodeFamilyChangedAt(pm.meta, existing.Inode, h.config.Now().UTC()); err != nil {
			return err
		}
	} else {
		shfs.TouchParentDirectory(pm.meta, cleanName, h.config.Now().UTC())
	}
	return h.commitDirtyProjectMetadataLocked(ctx, project, pm, &rollback)
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

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	defer pm.mu.Unlock()
	rollback := pm.meta.Clone()

	for _, release := range pm.meta.Releases {
		for _, file := range release.Files {
			if file.Release == tag {
				continue
			}
			for _, chunk := range file.Chunks {
				if chunk.Release == tag {
					return fmt.Errorf("release %s is still referenced by active file %s", tag, file.Name)
				}
			}
		}
	}
	if !pm.meta.RemoveRelease(tag) {
		return shfs.NotFound(fmt.Sprintf("release %s", tag))
	}
	return h.commitDirtyProjectMetadataLocked(ctx, project, pm, &rollback)
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
	before.Normalize(project, h.config.Now().UTC())
	repoMeta.RecomputeStats()
	repoMeta.Normalize(project, h.config.Now().UTC())
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
	for _, release := range repoMeta.Releases {
		trackedReleases[release.Tag] = struct{}{}
		for _, file := range release.Files {
			for _, chunk := range file.Chunks {
				trackedAssets[chunk.AssetID] = struct{}{}
			}
		}
	}
	result := &PurgeResult{}
	for _, release := range releases {
		if _, ok := trackedReleases[release.TagName]; !ok {
			if err := h.deleteReleaseByID(ctx, project, release.ID); err != nil {
				return nil, fmt.Errorf("delete untracked release %s: %w", release.TagName, err)
			}
			result.DeletedReleases++
			continue
		}
		for _, asset := range release.Assets {
			if _, ok := trackedAssets[asset.ID]; ok {
				continue
			}
			if err := h.deleteAssetByID(ctx, project, asset.ID); err != nil {
				return nil, fmt.Errorf("delete untracked asset %d: %w", asset.ID, err)
			}
			result.DeletedAssets++
		}
	}
	return result, nil
}
