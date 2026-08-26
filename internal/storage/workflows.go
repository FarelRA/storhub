package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

const metadataFilePath = ".storhub/metadata.json"

func (h *StorHub) uploadChunks(ctx context.Context, project, releaseTag, uploadURL string, planner *chunking.StreamingChunker) ([]ChunkInfo, error) {
	results := make([]ChunkInfo, planner.NumChunks())
	namer := newAssetNamer()
	for i := 0; i < planner.NumChunks(); i++ {
		chunk, err := planner.GetChunk(i)
		if err != nil {
			return results, err
		}
		assetName, err := namer.Next()
		if err != nil {
			return results, err
		}
		assetID, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, chunk, chunk.Size())
		if err != nil {
			return results, fmt.Errorf("upload chunk %d: %w", i, err)
		}
		results[i] = ChunkInfo{Size: chunk.Size(), Offset: chunk.Offset(), AssetOffset: 0, AssetID: assetID, Release: releaseTag}
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
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "load metadata start")
	if err := h.ensureOwner(ctx); err != nil {
		return nil, "", err
	}

	// Try go-git backend first
	if repo := h.getGitRepo(project); repo != nil {
		data, err := repo.readFileHead(ctx, metadataFilePath)
		if err != nil {
			if isMetadataNotFound(err) {
				exists, existsErr := h.repoExists(ctx, project)
				if existsErr != nil {
					return nil, "", existsErr
				}
				if !exists {
					return nil, "", shfs.NotFound(fmt.Sprintf("project %s", project))
				}
				meta := NewRepoMetadata(project)
				h.storeRepoMetadata(project, *meta, "")
				logging.Info(h.projectLogger(project), "load metadata initialized empty repository metadata", "elapsed", h.config.Now().UTC().Sub(started))
				return meta, "", nil
			}
			logging.Warn(h.projectLogger(project), "load metadata failed", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
			return nil, "", err
		}
		meta := NewRepoMetadata(project)
		if err := meta.FromJSON(data); err != nil {
			return nil, "", fmt.Errorf("parse metadata: %w", err)
		}
		meta.Normalize(project, h.config.Now().Unix())
		if err := meta.Validate(); err != nil {
			return nil, "", fmt.Errorf("validate metadata: %w", err)
		}
		sha := repo.headCommitSHA()
		h.storeRepoMetadata(project, *meta, sha)
		logging.Debug(h.projectLogger(project), "load metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "sha", shortSHA(sha), "bytes", len(data))
		return meta, sha, nil
	}

	// Fall back to GitHub REST API
	data, sha, err := h.gh.GetFileContent(ctx, h.owner, project, metadataFilePath, "")
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			exists, existsErr := h.repoExists(ctx, project)
			if existsErr != nil {
				return nil, "", existsErr
			}
			if !exists {
				return nil, "", shfs.NotFound(fmt.Sprintf("project %s", project))
			}
			meta := NewRepoMetadata(project)
			h.storeRepoMetadata(project, *meta, "")
			logging.Info(h.projectLogger(project), "load metadata initialized empty repository metadata", "elapsed", h.config.Now().UTC().Sub(started))
			return meta, "", nil
		}
		logging.Warn(h.projectLogger(project), "load metadata failed", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, "", err
	}
	meta := NewRepoMetadata(project)
	if err := meta.FromJSON(data); err != nil {
		return nil, "", fmt.Errorf("parse metadata: %w", err)
	}
	meta.Normalize(project, h.config.Now().Unix())
	if err := meta.Validate(); err != nil {
		return nil, "", fmt.Errorf("validate metadata: %w", err)
	}
	// NOTE: sha here is the metadata BLOB sha from the contents API - it
	// doubles as the version token for conditional PUTs. It must never be
	// consumed as a git ref: pins capture chunk layouts instead, so no
	// ref resolution is needed anywhere.
	h.storeRepoMetadata(project, *meta, sha)
	logging.Debug(h.projectLogger(project), "load metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "sha", shortSHA(sha), "bytes", len(data))
	return meta, sha, nil
}

func (h *StorHub) commitRepoMetadata(ctx context.Context, project string, metadata RepoMetadata, previousSHA, message string) (string, string, error) {
	started := h.config.Now().UTC()
	logging.Info(h.projectLogger(project), "commit metadata start", "message", message, "previous_sha", shortSHA(previousSHA))
	if err := h.ensureOwner(ctx); err != nil {
		return "", "", err
	}
	metadata.Normalize(project, h.config.Now().Unix())
	metadata.LastMod = h.config.Now().Unix()
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
	if repo := h.getGitRepo(project); repo != nil {
		commitSHA, contentSHA, err := repo.writeCommitPush(ctx, metadataFilePath, payload, message)
		if err != nil {
			logging.Error(h.projectLogger(project), "commit metadata failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
			return "", "", err
		}
		h.storeRepoMetadata(project, metadata, contentSHA)
		logging.Info(h.projectLogger(project), "commit metadata complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "commit_sha", shortSHA(commitSHA), "content_sha", shortSHA(contentSHA), "bytes", len(payload))
		return commitSHA, contentSHA, nil
	}
	commitSHA, contentSHA, err := h.gh.PutFileContent(ctx, h.owner, project, metadataFilePath, payload, previousSHA, message)
	if err != nil {
		logging.Error(h.projectLogger(project), "commit metadata failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return "", "", err
	}
	h.storeRepoMetadata(project, metadata, contentSHA)
	logging.Info(h.projectLogger(project), "commit metadata complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "commit_sha", shortSHA(commitSHA), "content_sha", shortSHA(contentSHA), "bytes", len(payload))
	return commitSHA, contentSHA, nil
}

func (h *StorHub) listMetadataRevisions(ctx context.Context, project string) ([]MetadataRevision, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	if repo := h.getGitRepo(project); repo != nil {
		revisions, err := repo.listFileCommits(ctx, metadataFilePath)
		if err != nil {
			// Infrastructure failures must not masquerade as "project not
			// found"; propagate them so callers can retry or report.
			return nil, fmt.Errorf("list metadata revisions: %w", err)
		}
		if len(revisions) == 0 {
			return nil, shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		return revisions, nil
	}
	commits, err := h.gh.ListFileCommits(ctx, h.owner, project, metadataFilePath)
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil, shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		return nil, err
	}
	revisions := make([]MetadataRevision, 0, len(commits))
	for _, commit := range commits {
		revisions = append(revisions, MetadataRevision{CommitSHA: commit.SHA, Message: commit.Message, CommittedAt: commit.CommittedAt.Unix()})
	}
	return revisions, nil
}

func (h *StorHub) getMetadataRevision(ctx context.Context, project, commitSHA string) (*RepoMetadata, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	var data []byte
	var err error
	if repo := h.getGitRepo(project); repo != nil {
		data, err = repo.readFileRef(ctx, commitSHA, metadataFilePath)
	} else {
		data, _, err = h.gh.GetFileContent(ctx, h.owner, project, metadataFilePath, commitSHA)
	}
	if err != nil {
		return nil, err
	}
	meta := NewRepoMetadata(project)
	if err := meta.FromJSON(data); err != nil {
		return nil, fmt.Errorf("parse metadata revision: %w", err)
	}
	meta.Normalize(project, h.config.Now().Unix())
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
	for _, file := range metadata.AllFiles() {
		for _, chunkName := range file.Chunks {
			chunk, ok := metadata.Chunks[chunkName]
			if !ok {
				continue
			}
			release, ok := releaseIndex[chunk.Release]
			if !ok {
				return fmt.Errorf("rollback metadata references missing release: %s", chunk.Release)
			}
			if _, ok := assetIndex[release.TagName][chunk.AssetID]; !ok {
				return fmt.Errorf("rollback metadata references missing asset %d in release %s", chunk.AssetID, chunk.Release)
			}
		}
	}
	return nil
}

func (h *StorHub) getOrCreateUploadRelease(ctx context.Context, project string, metadata *RepoMetadata, requiredSlots int) (string, string, error) {
	releases, err := h.listReleasesCached(ctx, project)
	if err != nil {
		return "", "", err
	}
	if requiredSlots <= 0 {
		for _, r := range releases {
			metadata.EnsureRelease(r.TagName, h.config.Now().Unix())
			return r.TagName, r.UploadURL, nil
		}
	} else {
		for _, r := range releases {
			if len(r.Assets)+requiredSlots <= 1000 {
				metadata.EnsureRelease(r.TagName, h.config.Now().Unix())
				return r.TagName, r.UploadURL, nil
			}
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
	metadata.EnsureRelease(tag, h.config.Now().Unix())
	return tag, release.UploadURL, nil
}

func (h *StorHub) getNextReleaseTag(metadata *RepoMetadata, releases []ghapi.Release) (string, error) {
	maxVersion := 0
	for tag := range metadata.Releases {
		if n, ok := meta.ParseNumericReleaseTag(tag); ok && n > maxVersion {
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

func (h *StorHub) createRelease(ctx context.Context, project, tag, name string) (*ghapi.Release, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	release, err := h.gh.CreateRelease(ctx, h.owner, project, tag, name)
	if err != nil {
		return nil, err
	}
	h.addReleaseToCache(project, release)
	return release, nil
}

func (h *StorHub) listReleases(ctx context.Context, project string) ([]ghapi.Release, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	releases, err := h.gh.ListReleases(ctx, h.owner, project)
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil, shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		return nil, err
	}
	h.setCachedReleases(project, releases)
	return releases, nil
}

func (h *StorHub) listReleasesCached(ctx context.Context, project string) ([]ghapi.Release, error) {
	if cached, ok := h.getCachedReleases(project); ok {
		return cached, nil
	}
	return h.listReleases(ctx, project)
}

func (h *StorHub) deleteReleaseByID(ctx context.Context, project string, releaseID int64) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	err := h.gh.DeleteReleaseByID(ctx, h.owner, project, releaseID)
	if err == nil {
		h.invalidateReleaseCache(project)
	}
	return err
}

func (h *StorHub) deleteAssetByID(ctx context.Context, project string, assetID int64) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	err := h.gh.DeleteAssetByID(ctx, h.owner, project, assetID)
	if err == nil {
		h.invalidateReleaseCache(project)
	}
	return err
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
	h.invalidateReleaseCache(project)
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
	h.bumpCachedReleaseAssetCount(project, releaseTag)
	return assetID, nil
}

func (h *StorHub) downloadAssetStream(ctx context.Context, project string, assetID, start, end int64) (io.ReadCloser, int64, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, 0, err
	}
	return h.gh.DownloadAssetStream(ctx, h.owner, project, assetID, start, end)
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
			delay := h.retryDelay(attempt, extractAPIError(err))
			h.debugf("asset range open retry project=%s asset=%d attempt=%d delay=%s err=%v", project, chunk.AssetID, attempt+1, delay, err)
			if sleepErr := h.config.Sleep(ctx, delay); sleepErr != nil {
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
		delay := h.retryDelay(attempt, extractAPIError(err))
		h.debugf("asset range read retry project=%s asset=%d attempt=%d delay=%s err=%v", project, chunk.AssetID, attempt+1, delay, err)
		if sleepErr := h.config.Sleep(ctx, delay); sleepErr != nil {
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
	entry, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return nil, "", false
	}
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	meta := entry.meta.Clone()
	meta.RebuildIndexes()
	return &meta, entry.sha, true
}

func (h *StorHub) cachedRepoMetadataReadonly(project string) (*RepoMetadata, string, bool) {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return nil, "", false
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	meta := pm.meta.Clone()
	return &meta, pm.sha, true
}

func (h *StorHub) storeRepoMetadata(project string, meta RepoMetadata, sha string) {
	clone := meta.Clone()
	clone.RebuildIndexes()

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	pm.meta = &clone
	pm.sha = sha
	pm.dirty = false // Just stored, so not dirty
	pm.hydrated = true
	pm.mu.Unlock()
}

func (h *StorHub) invalidateRepoMetadata(project string) {
	h.metaMu.Lock()
	pm, ok := h.metaCache[project]
	if ok {
		delete(h.metaCache, project)
		// Teardown mirrors eviction: without stopping the loop, deleting
		// the cache entry would leak a live commit goroutine per deleted
		// repo (and a duplicate loop if the project returns). metaMu→pm.mu
		// ordering matches getOrCreateProjectMeta and eviction.
		pm.mu.Lock()
		pm.stopped = true
		close(pm.stopCh)
		pm.mu.Unlock()
	}
	h.metaMu.Unlock()
}

func isMetadataNotFound(err error) bool {
	if err == nil {
		return false
	}
	// The git backend surfaces a missing metadata file as an fs-not-exist
	// error chain; match sentinels, never message text.
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, shfs.ErrNotFound) {
		return true
	}
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) && apiErr.NotFound() {
		return true
	}
	return false
}

func isRepoAlreadyExistsError(apiErr *ghapi.APIError) bool {
	if apiErr == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(apiErr.Message))
	return strings.Contains(message, "already exists") || strings.Contains(message, "name already exists")
}
