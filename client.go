package storhub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultAPIBaseURL      = "https://api.github.com"
	defaultAPIVersion      = "2022-11-28"
	defaultRequestTimeout  = 5 * time.Minute
	defaultRepoDescription = "StorHub storage project"
	maxMetadataBytes       = 8 << 20
)

var githubRepoNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type Config struct {
	APIBaseURL             string
	APIVersion             string
	HTTPClient             *http.Client
	ChunkSize              int64
	BufferSize             int
	MaxConcurrentTransfers int
	RepoDescription        string
	CreatePublicRepo       bool
	MaxRetries             int
	BaseRetryDelay         time.Duration
	MaxRetryDelay          time.Duration
	Now                    func() time.Time
	Sleep                  func(context.Context, time.Duration) error
}

func DefaultConfig() Config {
	return Config{
		APIBaseURL:             defaultAPIBaseURL,
		APIVersion:             defaultAPIVersion,
		HTTPClient:             newDefaultHTTPClient(),
		ChunkSize:              DefaultChunkSize,
		BufferSize:             DefaultBufferSize,
		MaxConcurrentTransfers: DefaultMaxConcurrentTransfers,
		RepoDescription:        defaultRepoDescription,
		CreatePublicRepo:       false,
		MaxRetries:             4,
		BaseRetryDelay:         500 * time.Millisecond,
		MaxRetryDelay:          8 * time.Second,
		Now:                    time.Now,
		Sleep:                  sleepWithContext,
	}
}

func newDefaultHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 128
	transport.MaxIdleConnsPerHost = 32
	transport.MaxConnsPerHost = 64
	transport.IdleConnTimeout = 90 * time.Second
	return &http.Client{Timeout: defaultRequestTimeout, Transport: transport}
}

func (c Config) withDefaults() Config {
	if isZeroConfig(c) {
		return DefaultConfig()
	}
	defaults := DefaultConfig()
	if c.APIBaseURL == "" {
		c.APIBaseURL = defaults.APIBaseURL
	}
	if c.APIVersion == "" {
		c.APIVersion = defaults.APIVersion
	}
	if c.HTTPClient == nil {
		c.HTTPClient = defaults.HTTPClient
	}
	if c.ChunkSize <= 0 {
		c.ChunkSize = defaults.ChunkSize
	}
	if c.BufferSize <= 0 {
		c.BufferSize = defaults.BufferSize
	}
	if c.MaxConcurrentTransfers <= 0 {
		c.MaxConcurrentTransfers = defaults.MaxConcurrentTransfers
	}
	if c.RepoDescription == "" {
		c.RepoDescription = defaults.RepoDescription
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.BaseRetryDelay <= 0 {
		c.BaseRetryDelay = defaults.BaseRetryDelay
	}
	if c.MaxRetryDelay <= 0 {
		c.MaxRetryDelay = defaults.MaxRetryDelay
	}
	if c.Now == nil {
		c.Now = defaults.Now
	}
	if c.Sleep == nil {
		c.Sleep = defaults.Sleep
	}
	return c
}

func isZeroConfig(c Config) bool {
	return c.APIBaseURL == "" &&
		c.APIVersion == "" &&
		c.HTTPClient == nil &&
		c.ChunkSize == 0 &&
		c.BufferSize == 0 &&
		c.MaxConcurrentTransfers == 0 &&
		c.RepoDescription == "" &&
		!c.CreatePublicRepo &&
		c.MaxRetries == 0 &&
		c.BaseRetryDelay == 0 &&
		c.MaxRetryDelay == 0 &&
		c.Now == nil &&
		c.Sleep == nil
}

type StorHub struct {
	token      string
	owner      string
	apiBaseURL string
	apiVersion string
	client     *http.Client
	config     Config
	bufferPool sync.Pool
	ownerMu    sync.Mutex
	repoMu     sync.Mutex
	repoState  map[string]bool
	metaMu     sync.Mutex
	metaCache  map[string]cachedMetadata
}

type cachedMetadata struct {
	sha  string
	meta RepoMetadata
}

func NewStorHub(token string) (*StorHub, error) {
	return NewStorHubWithContext(context.Background(), token, DefaultConfig())
}

func NewStorHubWithConfig(token string, cfg Config) (*StorHub, error) {
	return NewStorHubWithContext(context.Background(), token, cfg)
}

func NewStorHubWithContext(ctx context.Context, token string, cfg Config) (*StorHub, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("token is required")
	}

	cfg = cfg.withDefaults()
	hub := &StorHub{
		token:      token,
		apiBaseURL: strings.TrimRight(cfg.APIBaseURL, "/"),
		apiVersion: cfg.APIVersion,
		client:     cfg.HTTPClient,
		config:     cfg,
		repoState:  make(map[string]bool),
		metaCache:  make(map[string]cachedMetadata),
		bufferPool: sync.Pool{New: func() any {
			buf := make([]byte, cfg.BufferSize)
			return &buf
		}},
	}
	return hub, nil
}

func (h *StorHub) Owner() string { return h.owner }

func (h *StorHub) ensureOwner(ctx context.Context) error {
	h.ownerMu.Lock()
	defer h.ownerMu.Unlock()
	if strings.TrimSpace(h.owner) != "" {
		return nil
	}
	owner, err := h.getAuthenticatedUser(ctx)
	if err != nil {
		return fmt.Errorf("resolve authenticated user: %w", err)
	}
	h.owner = owner
	return nil
}

func (h *StorHub) UploadFile(project, fileName, inputPath string) (*FileMetadata, error) {
	return h.UploadFileContext(context.Background(), project, fileName, inputPath)
}

func (h *StorHub) UploadFileContext(ctx context.Context, project, fileName, inputPath string) (*FileMetadata, error) {
	return h.putFileContext(ctx, project, fileName, inputPath, false)
}

func (h *StorHub) ReplaceFile(project, fileName, inputPath string) (*FileMetadata, error) {
	return h.ReplaceFileContext(context.Background(), project, fileName, inputPath)
}

func (h *StorHub) ReplaceFileContext(ctx context.Context, project, fileName, inputPath string) (*FileMetadata, error) {
	return h.putFileContext(ctx, project, fileName, inputPath, true)
}

func (h *StorHub) PatchFile(project, fileName string, offset, deleteSize int64, edit []byte) (*FileMetadata, error) {
	return h.PatchFileContext(context.Background(), project, fileName, offset, deleteSize, edit)
}

func (h *StorHub) PatchFileContext(ctx context.Context, project, fileName string, offset, deleteSize int64, edit []byte) (*FileMetadata, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := normalizeFSPath(fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if offset < 0 {
		return nil, errors.New("patch offset must be non-negative")
	}
	if deleteSize < 0 {
		return nil, errors.New("patch delete size must be non-negative")
	}
	if deleteSize == 0 && len(edit) == 0 {
		return nil, errors.New("patch edit or delete size is required")
	}

	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
	}
	patchEnd := offset + deleteSize
	if offset > fileMeta.Size || patchEnd > fileMeta.Size {
		return nil, fmt.Errorf("patch range [%d,%d) exceeds file size %d", offset, patchEnd, fileMeta.Size)
	}

	return h.patchFileWithMetadataContext(ctx, project, cleanName, repoMeta, fileMeta, offset, deleteSize, edit)
}

func (h *StorHub) patchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *RepoMetadata, fileMeta *FileMetadata, offset, deleteSize int64, edit []byte) (*FileMetadata, error) {
	newChunks, targetRelease, err := h.buildPatchedChunks(ctx, project, repoMeta, *fileMeta, offset, deleteSize, edit)
	if err != nil {
		return nil, err
	}
	patched := fileMeta.Clone()
	patched.Chunks = newChunks
	patched.Release = targetRelease
	patched.Size = fileMeta.Size - deleteSize + int64(len(edit))
	patched.UploadedAt = h.config.Now().UTC()
	patched.CRC32C, err = CombineChunkCRC32Cs(patched.Chunks)
	if err != nil {
		return nil, err
	}
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		current := meta.FindFile(cleanName)
		if current == nil {
			return fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
		}
		meta.RemoveFile(cleanName)
		meta.UpsertFile(patched, h.config.Now().UTC())
		return nil
	}, fmt.Sprintf("storhub: patch %s at %d delete %d insert %d", cleanName, offset, deleteSize, len(edit))); err != nil {
		return nil, err
	}
	return &patched, nil
}

func (h *StorHub) putFileContext(ctx context.Context, project, fileName, inputPath string, replace bool) (*FileMetadata, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := normalizeFSPath(fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}

	fileInfo, err := os.Stat(inputPath)
	if err != nil {
		return nil, fmt.Errorf("stat input file: %w", err)
	}
	if fileInfo.IsDir() {
		return nil, fmt.Errorf("input path %q is a directory", inputPath)
	}

	if err := h.ensureRepo(ctx, project); err != nil {
		return nil, err
	}

	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	if err := requireParentDirectory(repoMeta, cleanName); err != nil {
		return nil, err
	}
	var preferredRelease string
	if existing := repoMeta.FindFile(cleanName); existing != nil {
		preferredRelease = existing.Release
	}
	if !replace && repoMeta.FindFile(cleanName) != nil {
		return nil, fmt.Errorf("file already exists: %s", cleanName)
	}

	planner, err := NewStreamingChunker(inputPath, cleanName, h.config.ChunkSize)
	if err != nil {
		return nil, err
	}
	defer planner.Close()

	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(cleanName)
	requiredSlots := planner.NumChunks()
	if fileInfo.Size() == 0 {
		requiredSlots = 0
	}
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots, preferredRelease)
	if err != nil {
		return nil, err
	}

	results := []ChunkInfo{}
	if fileInfo.Size() > 0 {
		results, err = h.uploadChunks(ctx, project, releaseTag, uploadURL, planner, uploadAssetKey(fileName, h.config.Now().UTC()))
		if err != nil {
			return nil, err
		}
	}
	crc32cSum, err := CombineChunkCRC32Cs(results)
	if err != nil {
		return nil, err
	}

	fileMeta := FileMetadata{
		Name:       cleanName,
		Size:       fileInfo.Size(),
		Chunks:     results,
		Release:    releaseTag,
		UploadedAt: h.config.Now().UTC(),
		CRC32C:     crc32cSum,
	}
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if err := requireParentDirectory(meta, cleanName); err != nil {
			return err
		}
		if !replace && meta.FindFile(cleanName) != nil {
			return fmt.Errorf("file already exists: %s", cleanName)
		}
		meta.RemoveFile(cleanName)
		meta.UpsertFile(fileMeta, h.config.Now().UTC())
		return nil
	}, metadataCommitMessage(cleanName, replace)); err != nil {
		return nil, err
	}
	return &fileMeta, nil
}

func (h *StorHub) DownloadFile(project, fileName, outputPath string) error {
	return h.DownloadFileContext(context.Background(), project, fileName, outputPath)
}

func (h *StorHub) DownloadFileContext(ctx context.Context, project, fileName, outputPath string) (err error) {
	if err := validateProject(project); err != nil {
		return err
	}
	cleanName, err := normalizeFSPath(fileName)
	if err != nil {
		return err
	}
	if cleanName == "" {
		return errors.New("file name is required")
	}

	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer func() {
		outFile.Close()
		if err != nil {
			_ = os.Remove(outputPath)
		}
	}()

	if err := outFile.Truncate(fileMeta.Size); err != nil {
		return fmt.Errorf("preallocate output file: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	chunkCRC32Cs := make([]string, len(fileMeta.Chunks))

	err = runConcurrent(ctx, h.config.MaxConcurrentTransfers, len(fileMeta.Chunks), func(i int) error {
		crc32cSum, err := h.downloadChunkWithRetry(ctx, project, outFile, fileMeta.Chunks[i])
		if err != nil {
			return err
		}
		chunkCRC32Cs[i] = crc32cSum
		return nil
	})
	if err != nil {
		return err
	}

	if err := outFile.Sync(); err != nil {
		return fmt.Errorf("sync output file: %w", err)
	}
	if err := outFile.Close(); err != nil {
		return fmt.Errorf("close output file: %w", err)
	}
	verifiedChunks := append([]ChunkInfo(nil), fileMeta.Chunks...)
	for i := range verifiedChunks {
		verifiedChunks[i].CRC32C = chunkCRC32Cs[i]
	}
	crc32cSum, err := CombineChunkCRC32Cs(verifiedChunks)
	if err != nil {
		return err
	}
	if crc32cSum != fileMeta.CRC32C {
		return fmt.Errorf("crc32c mismatch: expected %s, got %s", fileMeta.CRC32C, crc32cSum)
	}
	return nil
}

func (h *StorHub) ListFiles(project string) ([]FileMetadata, error) {
	return h.ListFilesContext(context.Background(), project)
}

func (h *StorHub) ListFilesContext(ctx context.Context, project string) ([]FileMetadata, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	files := repoMeta.AllFiles()
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })
	return files, nil
}

func (h *StorHub) ListReleases(project string) ([]ReleaseMetadata, error) {
	return h.ListReleasesContext(context.Background(), project)
}

func (h *StorHub) ListReleasesContext(ctx context.Context, project string) ([]ReleaseMetadata, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	releases := make([]ReleaseMetadata, len(repoMeta.Releases))
	for i := range repoMeta.Releases {
		releases[i] = repoMeta.Releases[i].Clone()
	}
	return releases, nil
}

func (h *StorHub) ListMetadataRevisions(project string) ([]MetadataRevision, error) {
	return h.ListMetadataRevisionsContext(context.Background(), project)
}

func (h *StorHub) ListMetadataRevisionsContext(ctx context.Context, project string) ([]MetadataRevision, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	return h.listMetadataRevisions(ctx, project)
}

func (h *StorHub) RollbackMetadata(project, commitSHA string) error {
	return h.RollbackMetadataContext(context.Background(), project, commitSHA)
}

func (h *StorHub) RollbackMetadataContext(ctx context.Context, project, commitSHA string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	if strings.TrimSpace(commitSHA) == "" {
		return errors.New("commit sha is required")
	}
	currentMeta, currentSHA, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return err
	}
	if err := currentMeta.Validate(); err != nil {
		return err
	}
	rollbackMeta, err := h.getMetadataRevision(ctx, project, commitSHA)
	if err != nil {
		return err
	}
	if err := h.validateMetadataSnapshot(ctx, project, rollbackMeta); err != nil {
		return err
	}
	_, _, err = h.commitRepoMetadata(ctx, project, *rollbackMeta, currentSHA, fmt.Sprintf("storhub: rollback metadata to %s", shortSHA(commitSHA)))
	return err
}

func (h *StorHub) getBuffer() *[]byte { return h.bufferPool.Get().(*[]byte) }

func (h *StorHub) putBuffer(buf *[]byte) { h.bufferPool.Put(buf) }

func (h *StorHub) updateRepoMetadata(ctx context.Context, project string, apply func(*RepoMetadata) error, message string) (*RepoMetadata, error) {
	for attempt := 0; attempt <= h.config.MaxRetries+1; attempt++ {
		meta, sha, err := h.loadRepoMetadataFresh(ctx, project)
		if err != nil {
			return nil, err
		}
		if err := apply(meta); err != nil {
			return nil, err
		}
		if _, _, err := h.commitRepoMetadata(ctx, project, *meta, sha, message); err != nil {
			if isConflictError(err) && attempt < h.config.MaxRetries+1 {
				continue
			}
			return nil, err
		}
		return meta, nil
	}
	return nil, errors.New("metadata update exhausted retries")
}

func (h *StorHub) downloadChunkWithRetry(ctx context.Context, project string, outFile *os.File, chunk ChunkInfo) (string, error) {
	if chunk.Size == 0 {
		return formatCRC32C(0), nil
	}
	buf := h.getBuffer()
	defer h.putBuffer(buf)

	for attempt := 0; attempt <= h.config.MaxRetries; attempt++ {
		reader, _, err := h.downloadAssetStream(ctx, project, chunk.AssetID, chunk.AssetOffset, chunk.AssetOffset+chunk.Size-1)
		if err != nil {
			if !isRetryableDownloadError(err) || attempt == h.config.MaxRetries {
				return "", fmt.Errorf("download chunk %d: %w", chunk.Index, err)
			}
			if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, extractAPIError(err))); sleepErr != nil {
				return "", sleepErr
			}
			continue
		}

		written, crc32cSum, copyErr := h.writeChunk(outFile, reader, *buf, chunk)
		closeErr := reader.Close()
		if copyErr == nil && closeErr != nil {
			copyErr = closeErr
		}
		if copyErr == nil && written != chunk.Size {
			copyErr = fmt.Errorf("chunk %d size mismatch: expected %d, got %d", chunk.Index, chunk.Size, written)
		}
		if copyErr == nil {
			if crc32cSum != chunk.CRC32C {
				copyErr = fmt.Errorf("chunk %d crc32c mismatch: expected %s, got %s", chunk.Index, chunk.CRC32C, crc32cSum)
			} else {
				return crc32cSum, nil
			}
		}
		if !isRetryableDownloadError(copyErr) || attempt == h.config.MaxRetries {
			return "", fmt.Errorf("download chunk %d: %w", chunk.Index, copyErr)
		}
		if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, extractAPIError(copyErr))); sleepErr != nil {
			return "", sleepErr
		}
	}
	return "", fmt.Errorf("download chunk %d: exhausted retries", chunk.Index)
}

func (h *StorHub) writeChunk(outFile *os.File, reader io.Reader, buf []byte, chunk ChunkInfo) (int64, string, error) {
	written := int64(0)
	hasher := newHashingReadSeeker(nopReadSeeker{reader: reader})
	for {
		n, readErr := hasher.Read(buf)
		if n > 0 {
			if _, writeErr := outFile.WriteAt(buf[:n], chunk.Offset+written); writeErr != nil {
				return written, "", fmt.Errorf("write chunk %d: %w", chunk.Index, writeErr)
			}
			written += int64(n)
		}
		if readErr == io.EOF {
			return written, hasher.Checksum(), nil
		}
		if readErr != nil {
			return written, "", fmt.Errorf("read chunk %d: %w", chunk.Index, readErr)
		}
	}
}

type nopReadSeeker struct{ reader io.Reader }

func (n nopReadSeeker) Read(p []byte) (int, error)   { return n.reader.Read(p) }
func (nopReadSeeker) Seek(int64, int) (int64, error) { return 0, errors.New("seek not supported") }

func runConcurrent(ctx context.Context, maxConcurrent, items int, fn func(index int) error) error {
	if items == 0 {
		return nil
	}
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	type result struct{ err error }
	results := make(chan result, items)
	jobs := make(chan int)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerCount := maxConcurrent
	if workerCount > items {
		workerCount = items
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case idx, ok := <-jobs:
					if !ok {
						return
					}
					if err := fn(idx); err != nil {
						cancel()
						results <- result{err: err}
						return
					}
					results <- result{}
				}
			}
		}()
	}

dispatch:
	for i := 0; i < items; i++ {
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	close(results)
	var firstErr error
	for res := range results {
		if res.err != nil && firstErr == nil {
			firstErr = res.err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func validateProject(project string) error {
	project = strings.TrimSpace(project)
	if project == "" {
		return errors.New("project is required")
	}
	if len(project) > 100 {
		return fmt.Errorf("project name too long: %d", len(project))
	}
	if project == "." || project == ".." {
		return fmt.Errorf("invalid project name: %s", project)
	}
	if !githubRepoNamePattern.MatchString(project) {
		return fmt.Errorf("invalid project name: %s", project)
	}
	if strings.HasPrefix(project, ".") || strings.HasSuffix(project, ".") {
		return fmt.Errorf("invalid project name: %s", project)
	}
	return nil
}

func metadataCommitMessage(fileName string, replace bool) string {
	if replace {
		return fmt.Sprintf("storhub: replace %s", fileName)
	}
	return fmt.Sprintf("storhub: add %s", fileName)
}

func uploadAssetKey(fileName string, now time.Time) string {
	return shortSHA(fileName) + "-" + strconv.FormatInt(now.UnixNano(), 36)
}

func shortSHA(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isConflictError(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict
}

func extractAPIError(err error) *APIError {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return nil
}

func isRetryableDownloadError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}
	return isRetryableNetworkError(err)
}
