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
	_, err = h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if err := shfs.CheckParentWrite(ctx, meta, cleanName); err != nil {
			return err
		}
		if meta.HasDirectory(cleanName) {
			return fmt.Errorf("is a directory: %s", cleanName)
		}
		existing := meta.FindFile(cleanName)
		if existing == nil {
			return fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
		}
		if !meta.RemoveFile(cleanName) {
			return fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
		}
		if len(meta.FindFilesByInode(existing.Inode)) > 0 {
			return implposix.TouchInodeFamilyChangedAt(meta, existing.Inode, h.config.Now().UTC())
		}
		return nil
	}, fmt.Sprintf("storhub: delete %s", cleanName))
	return err
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
	_, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		for _, release := range meta.Releases {
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
		if !meta.RemoveRelease(tag) {
			return fmt.Errorf("release not found: %s", tag)
		}
		return nil
	}, fmt.Sprintf("storhub: hide release %s", tag))
	return err
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
