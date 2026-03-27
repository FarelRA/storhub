package storhub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

const metadataFilePath = ".storhub/metadata.json"

const githubPageSize = 100

type githubRelease struct {
	ID        int64         `json:"id"`
	TagName   string        `json:"tag_name"`
	Name      string        `json:"name"`
	UploadURL string        `json:"upload_url"`
	Draft     bool          `json:"draft"`
	Assets    []githubAsset `json:"assets"`
}

type githubAsset struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type githubContent struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	SHA      string `json:"sha"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
	Type     string `json:"type"`
}

type githubPutContentResponse struct {
	Content githubContent `json:"content"`
	Commit  struct {
		SHA string `json:"sha"`
	} `json:"commit"`
}

type githubCommit struct {
	SHA    string `json:"sha"`
	Commit struct {
		Message string `json:"message"`
		Author  struct {
			Date time.Time `json:"date"`
		} `json:"author"`
	} `json:"commit"`
}

func (h *StorHub) uploadChunks(ctx context.Context, project, releaseTag, uploadURL string, planner *StreamingChunker, uploadKey string) ([]ChunkInfo, error) {
	results := make([]ChunkInfo, planner.NumChunks())
	err := runConcurrent(ctx, h.config.MaxConcurrentTransfers, planner.NumChunks(), func(i int) error {
		chunk, err := planner.GetChunk(i)
		if err != nil {
			return err
		}
		assetName := chunk.Name()
		if uploadKey != "" {
			assetName = uploadKey + "." + assetName
		}
		assetID, checksum, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, chunk, chunk.Size())
		if err != nil {
			return fmt.Errorf("upload chunk %d: %w", i, err)
		}
		results[i] = ChunkInfo{Name: assetName, Size: chunk.Size(), Index: i, Offset: chunk.Offset(), Release: releaseTag, AssetOffset: 0, AssetID: assetID, CRC32C: checksum}
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
	h.repoMu.Lock()
	if h.repoState[project] {
		h.repoMu.Unlock()
		return nil
	}
	h.repoMu.Unlock()

	body := map[string]any{
		"name":        project,
		"description": h.config.RepoDescription,
		"private":     !h.config.CreatePublicRepo,
		"auto_init":   true,
	}
	resp, err := h.doJSONWithRetryable(ctx, http.MethodPost, h.apiURL("/user/repos"), body, true)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusUnprocessableEntity && isRepoAlreadyExistsError(apiErr) {
			exists, existsErr := h.repoExists(ctx, project)
			if existsErr == nil && exists {
				return nil
			}
		}
		return fmt.Errorf("ensure repository: %w", err)
	}
	defer resp.Body.Close()
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

	resp, err := h.doJSON(ctx, http.MethodGet, h.apiURL(fmt.Sprintf("/repos/%s/%s", h.owner, project)), nil)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			h.setRepoState(project, false)
			return false, nil
		}
		return false, err
	}
	resp.Body.Close()
	h.setRepoState(project, true)
	return true, nil
}

func (h *StorHub) getAuthenticatedUser(ctx context.Context) (string, error) {
	var user struct {
		Login string `json:"login"`
	}
	if err := h.getJSON(ctx, h.apiURL("/user"), &user); err != nil {
		return "", err
	}
	if user.Login == "" {
		return "", errors.New("authenticated user response missing login")
	}
	return user.Login, nil
}

func (h *StorHub) loadRepoMetadata(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if meta, sha, ok := h.cachedRepoMetadata(project); ok {
		return meta, sha, nil
	}
	return h.loadRepoMetadataFresh(ctx, project)
}

func (h *StorHub) loadRepoMetadataFresh(ctx context.Context, project string) (*RepoMetadata, string, error) {
	data, sha, err := h.getFileContent(ctx, project, metadataFilePath, "")
	if err != nil {
		var apiErr *APIError
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
	commitSHA, contentSHA, err := h.putFileContent(ctx, project, metadataFilePath, payload, previousSHA, message)
	if err != nil {
		return "", "", err
	}
	h.storeRepoMetadata(project, metadata, contentSHA)
	return commitSHA, contentSHA, nil
}

func (h *StorHub) listMetadataRevisions(ctx context.Context, project string) ([]MetadataRevision, error) {
	commits, err := h.listMetadataCommits(ctx, project)
	if err != nil {
		return nil, err
	}
	revisions := make([]MetadataRevision, 0, len(commits))
	for _, commit := range commits {
		revisions = append(revisions, MetadataRevision{
			CommitSHA:   commit.SHA,
			Message:     commit.Commit.Message,
			CommittedAt: commit.Commit.Author.Date.UTC(),
		})
	}
	return revisions, nil
}

func (h *StorHub) getMetadataRevision(ctx context.Context, project, commitSHA string) (*RepoMetadata, error) {
	data, _, err := h.getFileContent(ctx, project, metadataFilePath, commitSHA)
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
	releaseIndex := make(map[string]githubRelease, len(releases))
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
	releaseIndex := make(map[string]githubRelease, len(releases))
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

func (h *StorHub) getNextReleaseTag(metadata *RepoMetadata, releases []githubRelease) (string, error) {
	maxVersion := 0
	for _, release := range metadata.Releases {
		if n, ok := parseNumericReleaseTag(release.Tag); ok && n > maxVersion {
			maxVersion = n
		}
	}
	for _, release := range releases {
		if n, ok := parseNumericReleaseTag(release.TagName); ok && n > maxVersion {
			maxVersion = n
		}
	}
	return fmt.Sprintf("v%d", maxVersion+1), nil
}

func (h *StorHub) getReleaseByTag(ctx context.Context, project, tag string) (*githubRelease, error) {
	var release githubRelease
	if err := h.getJSON(ctx, h.apiURL(fmt.Sprintf("/repos/%s/%s/releases/tags/%s", h.owner, project, tag)), &release); err != nil {
		return nil, err
	}
	return &release, nil
}

func (h *StorHub) findAssetIDByName(ctx context.Context, project, tag, name string) (int64, error) {
	release, err := h.getReleaseByTag(ctx, project, tag)
	if err != nil {
		return 0, err
	}
	for _, asset := range release.Assets {
		if asset.Name == name {
			return asset.ID, nil
		}
	}
	return 0, errors.New("asset not found by name")
}

func (h *StorHub) createRelease(ctx context.Context, project, tag, name string) (*githubRelease, error) {
	requestBody := map[string]any{"tag_name": tag, "name": name, "body": "", "draft": false}
	resp, err := h.doJSONWithRetryable(ctx, http.MethodPost, h.apiURL(fmt.Sprintf("/repos/%s/%s/releases", h.owner, project)), requestBody, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, fmt.Errorf("decode release response: %w", err)
	}
	return &release, nil
}

func (h *StorHub) listReleases(ctx context.Context, project string) ([]githubRelease, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	var releases []githubRelease
	for page := 1; ; page++ {
		var batch []githubRelease
		endpoint := h.apiURL(fmt.Sprintf("/repos/%s/%s/releases?per_page=%d&page=%d", h.owner, project, githubPageSize, page))
		if err := h.getJSON(ctx, endpoint, &batch); err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.NotFound() {
				return nil, fmt.Errorf("%w: %s", ErrProjectNotFound, project)
			}
			return nil, err
		}
		releases = append(releases, batch...)
		if len(batch) < githubPageSize {
			return releases, nil
		}
	}
}

func (h *StorHub) deleteReleaseByID(ctx context.Context, project string, releaseID int64) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	resp, err := h.doRequest(ctx, http.MethodDelete, h.apiURL(fmt.Sprintf("/repos/%s/%s/releases/%d", h.owner, project, releaseID)), func() (io.Reader, error) {
		return nil, nil
	}, requestOptions{retryable: false})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (h *StorHub) deleteAssetByID(ctx context.Context, project string, assetID int64) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	resp, err := h.doRequest(ctx, http.MethodDelete, h.apiURL(fmt.Sprintf("/repos/%s/%s/releases/assets/%d", h.owner, project, assetID)), func() (io.Reader, error) {
		return nil, nil
	}, requestOptions{retryable: false})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (h *StorHub) deleteRepo(ctx context.Context, project string) error {
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	resp, err := h.doJSON(ctx, http.MethodDelete, h.apiURL(fmt.Sprintf("/repos/%s/%s", h.owner, project)), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	h.setRepoState(project, false)
	h.invalidateRepoMetadata(project)
	return nil
}

func (h *StorHub) uploadAssetStreaming(ctx context.Context, project, releaseTag, uploadURL, assetName string, reader io.ReadSeeker, size int64) (int64, string, error) {
	cleanURL := strings.Split(uploadURL, "{")[0]
	parsed, err := url.Parse(cleanURL)
	if err != nil {
		return 0, "", fmt.Errorf("parse upload url: %w", err)
	}
	query := parsed.Query()
	query.Set("name", assetName)
	parsed.RawQuery = query.Encode()
	endpoint := parsed.String()
	hashingReader := newHashingReadSeeker(reader)
	for attempt := 0; attempt <= h.config.MaxRetries; attempt++ {
		assetID, err := h.uploadAssetAttempt(ctx, endpoint, assetName, hashingReader, size)
		if err == nil {
			return assetID, hashingReader.Checksum(), nil
		}
		if existingID, resolveErr := h.findAssetIDByName(ctx, project, releaseTag, assetName); resolveErr == nil && existingID != 0 {
			return existingID, hashingReader.Checksum(), nil
		}
		var apiErr *APIError
		if !(errors.As(err, &apiErr) && apiErr.IsRetryable()) && !isRetryableNetworkError(err) {
			return 0, "", fmt.Errorf("upload asset: %w", err)
		}
		if attempt == h.config.MaxRetries {
			return 0, "", fmt.Errorf("upload asset: %w", err)
		}
		delay := h.retryDelay(attempt, apiErr)
		if sleepErr := h.config.Sleep(ctx, delay); sleepErr != nil {
			return 0, "", sleepErr
		}
	}
	return 0, "", errors.New("upload asset: exhausted retries")
}

func (h *StorHub) uploadAssetAttempt(ctx context.Context, endpoint, assetName string, reader io.ReadSeeker, size int64) (int64, error) {
	resp, err := h.doRequest(ctx, http.MethodPost, endpoint, func() (io.Reader, error) {
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind upload reader: %w", err)
		}
		return reader, nil
	}, requestOptions{contentType: "application/octet-stream", accept: "application/vnd.github+json", contentSize: size, retryable: false})
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var asset struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&asset); err != nil {
		return 0, fmt.Errorf("decode asset response: %w", err)
	}
	if asset.ID == 0 {
		return 0, fmt.Errorf("asset response missing id for %s", assetName)
	}
	return asset.ID, nil
}

func (h *StorHub) downloadAssetStream(ctx context.Context, project string, assetID, start, end int64) (io.ReadCloser, int64, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, 0, err
	}
	rangeHeader := ""
	if start >= 0 && end >= start {
		rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
	}
	resp, err := h.doRequest(ctx, http.MethodGet, h.apiURL(fmt.Sprintf("/repos/%s/%s/releases/assets/%d", h.owner, project, assetID)), func() (io.Reader, error) {
		return nil, nil
	}, requestOptions{accept: "application/octet-stream", rangeHeader: rangeHeader, retryable: true})
	if err != nil {
		return nil, 0, fmt.Errorf("download asset: %w", err)
	}
	return resp.Body, resp.ContentLength, nil
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

func (h *StorHub) checksumAssetRange(ctx context.Context, project string, chunk ChunkInfo) (string, error) {
	if chunk.Size == 0 {
		return formatCRC32C(0), nil
	}
	buf := h.getBuffer()
	defer h.putBuffer(buf)
	var checksum string
	err := h.withAssetRangeReader(ctx, project, chunk, func(reader io.Reader) error {
		hasher := newHashingReadSeeker(nopReadSeeker{reader: reader})
		written, err := io.CopyBuffer(io.Discard, hasher, *buf)
		if err != nil {
			return err
		}
		if written != chunk.Size {
			return fmt.Errorf("asset range size mismatch: expected %d, got %d", chunk.Size, written)
		}
		checksum = hasher.Checksum()
		return nil
	})
	if err != nil {
		return "", err
	}
	return checksum, nil
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

func (h *StorHub) getFileContent(ctx context.Context, project, filePath, ref string) ([]byte, string, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, "", err
	}
	endpoint := h.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", h.owner, project, path.Clean(filePath)))
	if ref != "" {
		endpoint += "?ref=" + url.QueryEscape(ref)
	}
	var content githubContent
	if err := h.getJSON(ctx, endpoint, &content); err != nil {
		return nil, "", err
	}
	if content.Encoding != "base64" {
		return nil, "", fmt.Errorf("unsupported content encoding: %s", content.Encoding)
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(content.Content, "\n", ""))
	if err != nil {
		return nil, "", fmt.Errorf("decode content: %w", err)
	}
	return decoded, content.SHA, nil
}

func (h *StorHub) putFileContent(ctx context.Context, project, filePath string, content []byte, previousSHA, message string) (string, string, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return "", "", err
	}
	body := map[string]any{
		"message": message,
		"content": base64.StdEncoding.EncodeToString(content),
	}
	if previousSHA != "" {
		body["sha"] = previousSHA
	}
	resp, err := h.doJSONWithRetryable(ctx, http.MethodPut, h.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", h.owner, project, path.Clean(filePath))), body, true)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	var result githubPutContentResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", fmt.Errorf("decode content response: %w", err)
	}
	return result.Commit.SHA, result.Content.SHA, nil
}

func (h *StorHub) apiURL(path string) string { return h.apiBaseURL + path }

func (h *StorHub) listMetadataCommits(ctx context.Context, project string) ([]githubCommit, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	commits := make([]githubCommit, 0)
	for page := 1; ; page++ {
		var batch []githubCommit
		endpoint := h.apiURL(fmt.Sprintf("/repos/%s/%s/commits?path=%s&per_page=%d&page=%d", h.owner, project, url.QueryEscape(metadataFilePath), githubPageSize, page))
		if err := h.getJSON(ctx, endpoint, &batch); err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.NotFound() {
				return nil, fmt.Errorf("%w: %s", ErrProjectNotFound, project)
			}
			return nil, err
		}
		commits = append(commits, batch...)
		if len(batch) < githubPageSize {
			return commits, nil
		}
	}
}

func (h *StorHub) setRepoState(project string, exists bool) {
	h.repoMu.Lock()
	defer h.repoMu.Unlock()
	h.repoState[project] = exists
}

func (h *StorHub) cachedRepoMetadata(project string) (*RepoMetadata, string, bool) {
	h.metaMu.Lock()
	defer h.metaMu.Unlock()
	entry, ok := h.metaCache[project]
	if !ok {
		return nil, "", false
	}
	meta := entry.meta.Clone()
	meta.rebuildIndexes()
	return &meta, entry.sha, true
}

func (h *StorHub) storeRepoMetadata(project string, meta RepoMetadata, sha string) {
	clone := meta.Clone()
	clone.rebuildIndexes()
	h.metaMu.Lock()
	h.metaCache[project] = cachedMetadata{sha: sha, meta: clone}
	h.metaMu.Unlock()
}

func (h *StorHub) invalidateRepoMetadata(project string) {
	h.metaMu.Lock()
	delete(h.metaCache, project)
	h.metaMu.Unlock()
}

func isRepoAlreadyExistsError(apiErr *APIError) bool {
	if apiErr == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(apiErr.Message))
	return strings.Contains(message, "already exists") || strings.Contains(message, "name already exists")
}
