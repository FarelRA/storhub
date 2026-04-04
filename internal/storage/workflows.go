package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

const metadataFilePath = ".storhub/metadata.json"

func (h *StorHub) uploadChunks(ctx context.Context, project, releaseTag, uploadURL string, planner *chunking.StreamingChunker) ([]ChunkInfo, error) {
	results := make([]ChunkInfo, planner.NumChunks())
	namer := newAssetNamer()
	err := runConcurrent(ctx, h.config.MaxConcurrentTransfers, planner.NumChunks(), func(i int) error {
		chunk, err := planner.GetChunk(i)
		if err != nil {
			return err
		}
		assetName, err := namer.Next()
		if err != nil {
			return err
		}
		assetID, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, chunk, chunk.Size())
		if err != nil {
			return fmt.Errorf("upload chunk %d: %w", i, err)
		}
		results[i] = ChunkInfo{Name: assetName, Size: chunk.Size(), Index: i, Offset: chunk.Offset(), Release: releaseTag, AssetOffset: 0, AssetID: assetID}
		return nil
	})
	if err != nil {
		return results, err
	}
	return results, nil
}

func (h *StorHub) ensureRepo(ctx context.Context, project string) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	exists, err := h.repoExists(ctx, project)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := h.gh.CreateRepo(ctx, project, h.config.RepoDescription, !h.config.CreatePublicRepo, true); err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == 422 && isRepoAlreadyExistsError(apiErr) {
			exists, existsErr := h.repoExists(ctx, project)
			if existsErr == nil && exists {
				h.setRepoState(project, true)
				return nil
			}
		}
		return fmt.Errorf("ensure repository: %w", err)
	}
	h.setRepoState(project, true)
	return nil
}

func (h *StorHub) repoExists(ctx context.Context, project string) (bool, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return false, err
	}
	h.repoMu.Lock()
	exists, ok := h.repoState[project]
	h.repoMu.Unlock()
	if ok {
		return exists, nil
	}
	exists, err := h.gh.RepoExists(ctx, h.owner, project)
	if err != nil {
		return false, err
	}
	h.setRepoState(project, exists)
	return exists, nil
}

func (h *StorHub) getAuthenticatedUser(ctx context.Context) (string, error) {
	return h.gh.GetAuthenticatedUser(ctx)
}

func (h *StorHub) loadRepoMetadata(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if meta, sha, ok := h.cachedRepoMetadata(project); ok {
		return meta, sha, nil
	}
	return h.loadRepoMetadataFresh(ctx, project)
}

func (h *StorHub) loadRepoMetadataReadonly(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if meta, sha, ok := h.cachedRepoMetadataReadonly(project); ok {
		return meta, sha, nil
	}
	return h.loadRepoMetadataFresh(ctx, project)
}

func (h *StorHub) loadRepoMetadataFresh(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, "", err
	}
	data, sha, err := h.gh.GetFileContent(ctx, h.owner, project, metadataFilePath, "")
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			exists, existsErr := h.repoExists(ctx, project)
			if existsErr != nil {
				return nil, "", existsErr
			}
			if !exists {
				return nil, "", fmt.Errorf("%w: %s", ErrProjectNotFound, project)
			}
			meta := NewRepoMetadata(project)
			h.storeRepoMetadata(project, *meta, "")
			return meta, "", nil
		}
		return nil, "", err
	}
	meta := NewRepoMetadata(project)
	if err := meta.FromJSON(data); err != nil {
		return nil, "", fmt.Errorf("parse metadata: %w", err)
	}
	meta.Normalize(project, h.config.Now())
	if err := meta.Validate(); err != nil {
		return nil, "", fmt.Errorf("validate metadata: %w", err)
	}
	h.storeRepoMetadata(project, *meta, sha)
	return meta, sha, nil
}

func (h *StorHub) commitRepoMetadata(ctx context.Context, project string, metadata RepoMetadata, previousSHA, message string) (string, string, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return "", "", err
	}
	metadata.Normalize(project, h.config.Now())
	metadata.LastModified = h.config.Now().UTC()
	metadata.RecomputeStats()
	if err := metadata.Validate(); err != nil {
		return "", "", fmt.Errorf("validate metadata: %w", err)
	}
	payload, err := metadata.ToJSON()
	if err != nil {
		return "", "", err
	}
	if len(payload) > maxMetadataBytes {
		return "", "", fmt.Errorf("metadata too large: %d bytes exceeds %d", len(payload), maxMetadataBytes)
	}
	commitSHA, contentSHA, err := h.gh.PutFileContent(ctx, h.owner, project, metadataFilePath, payload, previousSHA, message)
	if err != nil {
		return "", "", err
	}
	h.storeRepoMetadata(project, metadata, contentSHA)
	return commitSHA, contentSHA, nil
}

func (h *StorHub) listMetadataRevisions(ctx context.Context, project string) ([]MetadataRevision, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	commits, err := h.gh.ListFileCommits(ctx, h.owner, project, metadataFilePath)
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil, fmt.Errorf("%w: %s", ErrProjectNotFound, project)
		}
		return nil, err
	}
	revisions := make([]MetadataRevision, 0, len(commits))
	for _, commit := range commits {
		revisions = append(revisions, MetadataRevision{CommitSHA: commit.SHA, Message: commit.Message, CommittedAt: commit.CommittedAt})
	}
	return revisions, nil
}

func (h *StorHub) getMetadataRevision(ctx context.Context, project, commitSHA string) (*RepoMetadata, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	data, _, err := h.gh.GetFileContent(ctx, h.owner, project, metadataFilePath, commitSHA)
	if err != nil {
		return nil, err
	}
	meta := NewRepoMetadata(project)
	if err := meta.FromJSON(data); err != nil {
		return nil, fmt.Errorf("parse metadata revision: %w", err)
	}
	meta.Normalize(project, h.config.Now())
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("validate metadata revision: %w", err)
	}
	return meta, nil
}

func (h *StorHub) validateMetadataSnapshot(ctx context.Context, project string, metadata *RepoMetadata) error {
	releases, err := h.listReleases(ctx, project)
	if err != nil {
		return err
	}
	releaseIndex := make(map[string]ghapi.Release, len(releases))
	assetIndex := make(map[string]map[int64]struct{}, len(releases))
	for _, release := range releases {
		releaseIndex[release.TagName] = release
		assets := make(map[int64]struct{}, len(release.Assets))
		for _, asset := range release.Assets {
			assets[asset.ID] = struct{}{}
		}
		assetIndex[release.TagName] = assets
	}
	for _, releaseMeta := range metadata.Releases {
		for _, file := range releaseMeta.Files {
			for _, chunk := range file.Chunks {
				release, ok := releaseIndex[chunk.Release]
				if !ok {
					return fmt.Errorf("rollback metadata references missing release: %s", chunk.Release)
				}
				if _, ok := assetIndex[release.TagName][chunk.AssetID]; !ok {
					return fmt.Errorf("rollback metadata references missing asset %d in release %s", chunk.AssetID, chunk.Release)
				}
			}
		}
	}
	return nil
}

func (h *StorHub) getOrCreateUploadRelease(ctx context.Context, project string, metadata *RepoMetadata, requiredSlots int, preferredTag string) (string, string, error) {
	releases, err := h.listReleases(ctx, project)
	if err != nil {
		return "", "", err
	}
	releaseIndex := make(map[string]ghapi.Release, len(releases))
	for _, release := range releases {
		releaseIndex[release.TagName] = release
	}
	if strings.TrimSpace(preferredTag) != "" {
		if release, ok := releaseIndex[preferredTag]; ok && len(release.Assets)+requiredSlots <= 1000 {
			metadata.EnsureRelease(preferredTag, h.config.Now().UTC())
			return preferredTag, release.UploadURL, nil
		}
	}
	for _, existing := range metadata.Releases {
		if release, ok := releaseIndex[existing.Tag]; ok && len(release.Assets)+requiredSlots <= 1000 {
			return existing.Tag, release.UploadURL, nil
		}
	}
	tag, err := h.getNextReleaseTag(metadata, releases)
	if err != nil {
		return "", "", err
	}
	release, err := h.createRelease(ctx, project, tag, "StorHub storage "+tag)
	if err != nil {
		return "", "", err
	}
	metadata.EnsureRelease(tag, h.config.Now().UTC())
	return tag, release.UploadURL, nil
}

func (h *StorHub) getNextReleaseTag(metadata *RepoMetadata, releases []ghapi.Release) (string, error) {
	maxVersion := 0
	for _, release := range metadata.Releases {
		if n, ok := meta.ParseNumericReleaseTag(release.Tag); ok && n > maxVersion {
			maxVersion = n
		}
	}
	for _, release := range releases {
		if n, ok := meta.ParseNumericReleaseTag(release.TagName); ok && n > maxVersion {
			maxVersion = n
		}
	}
	return fmt.Sprintf("v%d", maxVersion+1), nil
}

func (h *StorHub) getReleaseByTag(ctx context.Context, project, tag string) (*ghapi.Release, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	return h.gh.GetReleaseByTag(ctx, h.owner, project, tag)
}

func (h *StorHub) createRelease(ctx context.Context, project, tag, name string) (*ghapi.Release, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	return h.gh.CreateRelease(ctx, h.owner, project, tag, name)
}

func (h *StorHub) listReleases(ctx context.Context, project string) ([]ghapi.Release, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	releases, err := h.gh.ListReleases(ctx, h.owner, project)
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil, fmt.Errorf("%w: %s", ErrProjectNotFound, project)
		}
		return nil, err
	}
	return releases, nil
}

func (h *StorHub) deleteReleaseByID(ctx context.Context, project string, releaseID int64) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	return h.gh.DeleteReleaseByID(ctx, h.owner, project, releaseID)
}

func (h *StorHub) deleteAssetByID(ctx context.Context, project string, assetID int64) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	return h.gh.DeleteAssetByID(ctx, h.owner, project, assetID)
}

func (h *StorHub) deleteRepo(ctx context.Context, project string) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	if err := h.gh.DeleteRepo(ctx, h.owner, project); err != nil {
		return err
	}
	h.setRepoState(project, false)
	h.invalidateRepoMetadata(project)
	return nil
}

func (h *StorHub) uploadAssetStreaming(ctx context.Context, project, releaseTag, uploadURL, assetName string, reader io.ReadSeeker, size int64) (int64, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return 0, err
	}
	assetID, err := h.gh.UploadAsset(ctx, h.owner, project, releaseTag, uploadURL, assetName, reader, size)
	if err != nil {
		return 0, err
	}
	return assetID, nil
}

func (h *StorHub) downloadAssetStream(ctx context.Context, project string, assetID, start, end int64) (io.ReadCloser, int64, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, 0, err
	}
	return h.gh.DownloadAssetStream(ctx, h.owner, project, assetID, start, end)
}

func (h *StorHub) readAssetRange(ctx context.Context, project string, chunk ChunkInfo) ([]byte, error) {
	if chunk.Size == 0 {
		return []byte{}, nil
	}
	data := make([]byte, chunk.Size)
	if err := h.fillAssetRange(ctx, project, chunk, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (h *StorHub) fillAssetRange(ctx context.Context, project string, chunk ChunkInfo, dst []byte) error {
	if int64(len(dst)) != chunk.Size {
		return fmt.Errorf("asset range size mismatch: expected buffer %d, got %d", chunk.Size, len(dst))
	}
	return h.withAssetRangeReader(ctx, project, chunk, func(reader io.Reader) error {
		read, err := io.ReadFull(reader, dst)
		if err != nil {
			return err
		}
		if int64(read) != chunk.Size {
			return fmt.Errorf("asset range size mismatch: expected %d, got %d", chunk.Size, read)
		}
		return nil
	})
}

func (h *StorHub) withAssetRangeReader(ctx context.Context, project string, chunk ChunkInfo, fn func(io.Reader) error) error {
	for attempt := 0; attempt <= h.config.MaxRetries; attempt++ {
		reader, _, err := h.downloadAssetStream(ctx, project, chunk.AssetID, chunk.AssetOffset, chunk.AssetOffset+chunk.Size-1)
		if err != nil {
			if !isRetryableDownloadError(err) || attempt == h.config.MaxRetries {
				return err
			}
			if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, extractAPIError(err))); sleepErr != nil {
				return sleepErr
			}
			continue
		}
		err = fn(reader)
		closeErr := reader.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
		if err == nil {
			return nil
		}
		if !isRetryableDownloadError(err) || attempt == h.config.MaxRetries {
			return err
		}
		if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, extractAPIError(err))); sleepErr != nil {
			return sleepErr
		}
	}
	return errors.New("asset range read exhausted retries")
}

func (h *StorHub) setRepoState(project string, exists bool) {
	h.repoMu.Lock()
	defer h.repoMu.Unlock()
	h.repoState[project] = exists
}

func (h *StorHub) cachedRepoMetadata(project string) (*RepoMetadata, string, bool) {
	h.metaMu.RLock()
	defer h.metaMu.RUnlock()
	entry, ok := h.metaCache[project]
	if !ok {
		return nil, "", false
	}
	meta := entry.meta.Clone()
	meta.RebuildIndexes()
	return &meta, entry.sha, true
}

func (h *StorHub) cachedRepoMetadataReadonly(project string) (*RepoMetadata, string, bool) {
	h.metaMu.RLock()
	defer h.metaMu.RUnlock()
	entry, ok := h.metaCache[project]
	if !ok {
		return nil, "", false
	}
	meta := entry.meta
	return &meta, entry.sha, true
}

func (h *StorHub) storeRepoMetadata(project string, meta RepoMetadata, sha string) {
	clone := meta.Clone()
	clone.RebuildIndexes()
	h.metaMu.Lock()
	h.metaCache[project] = cachedMetadata{sha: sha, meta: clone}
	h.metaMu.Unlock()
}

func (h *StorHub) invalidateRepoMetadata(project string) {
	h.metaMu.Lock()
	delete(h.metaCache, project)
	h.metaMu.Unlock()
}

func isRepoAlreadyExistsError(apiErr *ghapi.APIError) bool {
	if apiErr == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(apiErr.Message))
	return strings.Contains(message, "already exists") || strings.Contains(message, "name already exists")
}
