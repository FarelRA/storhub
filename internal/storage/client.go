package storage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

const (
	maxMetadataBytes       = 8 << 20
	minParallelSegmentSize = 1 << 20 // 1 MiB — minimum segment size to parallelize
)

var githubRepoNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type (
	Config            = storcfg.Config
	ChunkInfo         = metadata.ChunkInfo
	FileMeta          = metadata.FileMeta
	RepoMetadata      = metadata.RepoMetadata
	MetadataRevision  = metadata.MetadataRevision
	DirMeta           = metadata.DirMeta
	NodeKind          = metadata.NodeKind
)

const (
	NodeKindFile    = metadata.NodeKindFile
	NodeKindSymlink = metadata.NodeKindSymlink
)

var ErrFileNotFound = shfs.ErrNotFound

func DefaultConfig() Config {
	return storcfg.Default()
}

func NewRepoMetadata(project string) *RepoMetadata {
	return metadata.NewRepoMetadata(project)
}

type StorHub struct {
	token      string
	owner      string
	gh         *ghapi.Client
	config     storcfg.Config
	bufferPool sync.Pool
	ownerMu    sync.Mutex
	repoMu     sync.Mutex
	repoState  map[string]bool
	logger     *slog.Logger

	// Metadata management
	metaMu    sync.RWMutex
	metaCache map[string]*projectMetadata

	// Git repository cache for metadata operations
	gitMu     sync.Mutex
	gitRepos  map[string]*gitRepo

	// Shutdown coordination
	shutdownOnce  sync.Once
	shutdownCh    chan struct{}
	shutdownWg    sync.WaitGroup
	janitorCtx    context.Context
	janitorCancel context.CancelFunc
}

// projectMetadata holds metadata for a single project with batched commit support
type projectMetadata struct {
	mu         sync.RWMutex
	commitMu   sync.Mutex
	meta       *metadata.RepoMetadata
	sha        string
	dirty      bool
	version    uint64
	lastCommit time.Time
	lastAccess time.Time
	stopCh     chan struct{}
	stoppedCh  chan struct{}
	triggerCh  chan struct{}
}

func markProjectDirtyLocked(pm *projectMetadata) {
	pm.dirty = true
	pm.version++
}

func (h *StorHub) debugf(format string, args ...any) {
	logging.Debug(h.logger, fmt.Sprintf(format, args...))
}

func (h *StorHub) logOpStart(project, op string, args ...any) time.Time {
	logging.Info(h.projectLogger(project), op+" start", args...)
	return time.Now().UTC()
}

func (h *StorHub) logOpFinish(project, op string, started time.Time, err error, args ...any) {
	args = append(args, "elapsed", time.Since(started))
	if err != nil {
		args = append(args, "err", err)
		logging.Error(h.projectLogger(project), op+" failed", args...)
		return
	}
	logging.Info(h.projectLogger(project), op+" complete", args...)
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

	cfg = cfg.WithDefaults()
	hub := &StorHub{
		token:      token,
		gh:         ghapi.NewClient(token, cfg),
		config:     cfg,
		repoState:  make(map[string]bool),
		metaCache:  make(map[string]*projectMetadata),
		gitRepos:   make(map[string]*gitRepo),
		logger:     logging.WithComponent(cfg.Logger, "storage"),
		shutdownCh: make(chan struct{}),
		bufferPool: sync.Pool{New: func() any {
			buf := make([]byte, cfg.BufferSize)
			return &buf
		}},
	}
	hub.janitorCtx, hub.janitorCancel = context.WithCancel(ctx)
	go hub.startJanitor(hub.janitorCtx, 30*time.Minute)
	return hub, nil
}

func (h *StorHub) getGitRepo(project string) *gitRepo {
	if h.config.DisableGitBackend {
		return nil
	}
	h.gitMu.Lock()
	defer h.gitMu.Unlock()
	if r, ok := h.gitRepos[project]; ok {
		return r
	}
	r := newGitRepo(h.config.GitCacheDir, h.owner, project, h.token)
	h.gitRepos[project] = r
	return r
}

func (h *StorHub) Owner() string { return h.owner }

func (h *StorHub) ensureOwner(ctx context.Context) error {
	h.ownerMu.Lock()
	if strings.TrimSpace(h.owner) != "" {
		h.ownerMu.Unlock()
		return nil
	}
	h.ownerMu.Unlock()

	owner, err := h.getAuthenticatedUser(ctx)
	if err != nil {
		return fmt.Errorf("resolve authenticated user: %w", err)
	}

	h.ownerMu.Lock()
	if strings.TrimSpace(h.owner) == "" {
		h.owner = owner
	}
	h.ownerMu.Unlock()
	return nil
}

// getOrCreateProjectMeta returns the projectMetadata for a project, creating it if needed
func (h *StorHub) getOrCreateProjectMeta(project string) *projectMetadata {
	// Fast path: read lock to check if exists
	h.metaMu.RLock()
	pm, exists := h.metaCache[project]
	h.metaMu.RUnlock()

	if exists {
		pm.mu.Lock()
		pm.lastAccess = h.config.Now()
		pm.mu.Unlock()
		return pm
	}

	// Slow path: write lock to create
	h.metaMu.Lock()
	defer h.metaMu.Unlock()

	// Double-check after acquiring write lock
	pm, exists = h.metaCache[project]
	if exists {
		pm.mu.Lock()
		pm.lastAccess = h.config.Now()
		pm.mu.Unlock()
		return pm
	}

	now := h.config.Now()
	// Create new projectMetadata
	pm = &projectMetadata{
		meta:       metadata.NewRepoMetadata(project),
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
		triggerCh:  make(chan struct{}, 1),
		lastAccess: now,
	}
	h.metaCache[project] = pm

	// Start commit loop goroutine
	h.shutdownWg.Add(1)
	go h.commitLoop(project, pm)

	return pm
}

// commitLoop periodically commits dirty metadata to GitHub
func (h *StorHub) commitLoop(project string, pm *projectMetadata) {
	defer h.shutdownWg.Done()
	defer close(pm.stoppedCh)

	ticker := time.NewTicker(h.config.MetadataCommitInterval)
	defer ticker.Stop()

	logger := h.projectLogger(project)

	for {
		select {
		case <-pm.stopCh:
			// Project evicted, stop the commit loop
			return
		case <-ticker.C:
			if err := h.commitProjectMetadata(context.Background(), project, pm); err != nil {
				h.recoverMetadataCommitFailure(project, err)
				continue
			}

		case <-pm.triggerCh:
			// Wake up and commit if dirty, then continue loop.
			if err := h.commitProjectMetadata(context.Background(), project, pm); err != nil {
				h.recoverMetadataCommitFailure(project, err)
				continue
			}

		case <-h.shutdownCh:
			// Shutdown requested
			if err := h.commitProjectMetadata(context.Background(), project, pm); err != nil {
				logging.Error(logger, "shutdown metadata commit failed", "err", err)
				return
			}
			return
		}
	}
}

func (h *StorHub) recoverMetadataCommitFailure(project string, err error) {
	logger := h.projectLogger(project)
	logging.Error(logger, "metadata commit failed, reloading from github", "err", err)
	if _, _, loadErr := h.loadRepoMetadataFresh(context.Background(), project); loadErr != nil {
		logging.Error(logger, "failed to reload metadata after commit failure; preserving dirty metadata for retry", "err", loadErr)
		return
	}
	logging.Info(logger, "metadata reloaded from github, in-memory changes discarded")
}

// commitProjectMetadata commits dirty metadata without holding pm.mu during GitHub I/O.
func (h *StorHub) commitProjectMetadata(ctx context.Context, project string, pm *projectMetadata) error {
	pm.commitMu.Lock()
	defer pm.commitMu.Unlock()

	started := h.config.Now().UTC()
	pm.mu.Lock()
	if !pm.dirty {
		pm.mu.Unlock()
		return nil
	}
	pm.meta.Normalize(project, h.config.Now())
	pm.meta.LastMod = h.config.Now().UTC()
	pm.meta.RecomputeStats()
	meta := pm.meta.Clone()
	previousSHA := pm.sha
	version := pm.version
	pm.mu.Unlock()

	logging.Info(h.projectLogger(project), "commit metadata start", "previous_sha", shortSHA(previousSHA))

	if err := h.ensureOwner(ctx); err != nil {
		return err
	}

	if err := meta.Validate(); err != nil {
		logging.Error(h.projectLogger(project), "commit metadata failed", "step", "validate", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return fmt.Errorf("invalid metadata: %w", err)
	}

	metaBytes, err := meta.ToJSON()
	if err != nil {
		logging.Error(h.projectLogger(project), "commit metadata failed", "step", "serialize", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return fmt.Errorf("marshal metadata: %w", err)
	}

	if len(metaBytes) > maxMetadataBytes {
		err := fmt.Errorf("metadata too large: %d bytes (max %d)", len(metaBytes), maxMetadataBytes)
		logging.Error(h.projectLogger(project), "commit metadata failed", "step", "size_check", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return err
	}

	// Commit metadata
	message := "storhub: update metadata"
	var commitSHA, contentSHA string
	if repo := h.getGitRepo(project); repo != nil {
		commitSHA, contentSHA, err = repo.writeCommitPush(ctx, metadataFilePath, metaBytes, message)
	} else {
		commitSHA, contentSHA, err = h.gh.PutFileContent(ctx, h.owner, project, metadataFilePath, metaBytes, previousSHA, message)
	}
	if err != nil {
		logging.Error(h.projectLogger(project), "commit metadata failed", "step", "git_commit", "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return fmt.Errorf("commit metadata: %w", err)
	}

	pm.mu.Lock()
	pm.sha = contentSHA
	if pm.version == version {
		pm.dirty = false
		pm.lastCommit = h.config.Now()
	}
	pm.mu.Unlock()

	logging.Info(h.projectLogger(project), "commit metadata complete", "elapsed", h.config.Now().UTC().Sub(started), "commit_sha", shortSHA(commitSHA), "content_sha", shortSHA(contentSHA), "bytes", len(metaBytes))

	return nil
}

// startJanitor periodically evicts idle, non-dirty project metadata to prevent unbounded cache growth
func (h *StorHub) startJanitor(ctx context.Context, idleTimeout time.Duration) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.evictIdleProjects(idleTimeout)
		case <-ctx.Done():
			return
		}
	}
}

func (h *StorHub) evictIdleProjects(idleTimeout time.Duration) {
	h.metaMu.Lock()
	defer h.metaMu.Unlock()
	now := time.Now()
	for name, pm := range h.metaCache {
		pm.mu.Lock()
		if !pm.dirty && now.Sub(pm.lastAccess) > idleTimeout {
			close(pm.stopCh)
			delete(h.metaCache, name)
		}
		pm.mu.Unlock()
	}
}

// Shutdown gracefully shuts down the StorHub, committing any dirty metadata
func (h *StorHub) Shutdown(ctx context.Context) error {
	var shutdownErr error
	h.shutdownOnce.Do(func() {
		logging.Info(h.logger, "shutdown initiated")

		// Stop the janitor
		h.janitorCancel()

		// Signal all commit loops to stop
		close(h.shutdownCh)

		// Wait for all commit loops to finish with timeout
		done := make(chan struct{})
		go func() {
			h.shutdownWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			logging.Info(h.logger, "shutdown complete")
		case <-ctx.Done():
			shutdownErr = fmt.Errorf("shutdown timeout: %w", ctx.Err())
			logging.Error(h.logger, "shutdown timeout", "err", ctx.Err())
		}
	})

	return shutdownErr
}

// FlushMetadata forces an immediate commit of all dirty metadata for all projects
// This is useful for testing or when you need to ensure metadata is persisted immediately
func (h *StorHub) FlushMetadata(ctx context.Context) error {
	h.metaMu.RLock()
	type projectWithName struct {
		name string
		meta *projectMetadata
	}
	projects := make([]projectWithName, 0, len(h.metaCache))
	for name, pm := range h.metaCache {
		projects = append(projects, projectWithName{name: name, meta: pm})
	}
	h.metaMu.RUnlock()

	var firstErr error
	for _, p := range projects {
		if err := h.commitProjectMetadata(ctx, p.name, p.meta); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

func (h *StorHub) UploadFile(project, fileName, inputPath string) (*FileMeta, error) {
	return h.UploadFileContext(context.Background(), project, fileName, inputPath)
}

func (h *StorHub) UploadFileContext(ctx context.Context, project, fileName, inputPath string) (*FileMeta, error) {
	return h.putFileContext(ctx, project, fileName, inputPath, false)
}

func (h *StorHub) ReplaceFile(project, fileName, inputPath string) (*FileMeta, error) {
	return h.ReplaceFileContext(context.Background(), project, fileName, inputPath)
}

func (h *StorHub) ReplaceFileContext(ctx context.Context, project, fileName, inputPath string) (*FileMeta, error) {
	return h.putFileContext(ctx, project, fileName, inputPath, true)
}

func (h *StorHub) PrepareReplaceContext(ctx context.Context, project, fileName string, requiredSlots int) (releaseTag string, uploadURL string, err error) {
	started := h.logOpStart(project, "prepare-replace", "path", fileName, "required_slots", requiredSlots)
	defer func() {
		h.logOpFinish(project, "prepare-replace", started, err, "path", fileName, "required_slots", requiredSlots, "release", releaseTag)
	}()
	if err := validateProject(project); err != nil {
		return "", "", err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return "", "", err
	}
	if cleanName == "" {
		return "", "", errors.New("file name is required")
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return "", "", err
	}
	if err := shfs.RequireParentDirectory(repoMeta, cleanName); err != nil {
		return "", "", err
	}
	existing := repoMeta.FindFile(cleanName)
	if existing == nil {
		return "", "", fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
	}
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(cleanName)
	releaseTag, uploadURL, err = h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots, "")
	return releaseTag, uploadURL, err
}

func (h *StorHub) UploadChunkDataContext(ctx context.Context, project, releaseTag, uploadURL string, index int, offset int64, data []byte) (ChunkInfo, error) {
	const maxNameRetries = 5
	for attempt := 0; attempt < maxNameRetries; attempt++ {
		assetName, err := randomAssetName()
		if err != nil {
			return ChunkInfo{}, err
		}
		assetID, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, bytes.NewReader(data), int64(len(data)))
		if err == nil {
			return ChunkInfo{
				Size:        int64(len(data)),
				Offset:      offset,
				Release:     releaseTag,
				AssetID:     assetID,
				AssetOffset: 0,
			}, nil
		}
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == 422 {
			h.debugf("upload chunk asset name collision, retry=%d asset=%s", attempt+1, assetName)
			continue
		}
		return ChunkInfo{}, err
	}
	return ChunkInfo{}, fmt.Errorf("upload chunk data failed after %d name retries", maxNameRetries)
}

// trimChunks drops chunks at or beyond size and re-sorts/re-indexes them.
func trimChunks(chunks []ChunkInfo, size int64) []ChunkInfo {
	if len(chunks) == 0 {
		return chunks
	}
	filtered := make([]ChunkInfo, 0, len(chunks))
	for _, c := range chunks {
		if c.Offset < size {
			if c.Offset+c.Size > size {
				c.Size = size - c.Offset
			}
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		return filtered
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Offset < filtered[j].Offset
	})
	return filtered
}

func (h *StorHub) FinalizeReplaceChunksContext(ctx context.Context, project, fileName, releaseTag string, size int64, chunks []ChunkInfo) (result *FileMeta, err error) {
	started := h.logOpStart(project, "finalize-replace", "path", fileName, "release", releaseTag, "size", size, "chunks", len(chunks))
	defer func() {
		h.logOpFinish(project, "finalize-replace", started, err, "path", fileName, "release", releaseTag, "size", size, "chunks", len(chunks))
	}()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	current := repoMeta.FindFile(cleanName)
	if current == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
	}
	// Trim chunks beyond the logical file size.
	// Kernel writeback cache may flush stale dirty pages from the previous
	// file content (before truncation), producing chunks past the new EOF.
	chunks = trimChunks(chunks, size)

	now := h.config.Now().UTC()
	fileMeta := current.Clone()

	// Add new chunks to the repo's Chunks map
	chunkNames := make([]string, len(chunks))
	for i, chunk := range chunks {
		name := fmt.Sprintf("%s/chunk/%d", cleanName, i)
		repoMeta.Chunks[name] = chunk
		chunkNames[i] = name
	}
	fileMeta.Chunks = chunkNames
	fileMeta.Size = size
	implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, current, now)

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	for i, name := range chunkNames {
		pm.meta.Chunks[name] = chunks[i]
	}
	latest := pm.meta.FindFile(cleanName)
	if latest == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, latest, now)
	implposix.ReplaceInodeFamily(pm.meta, cleanName, latest, fileMeta, now)
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	select {
	case pm.triggerCh <- struct{}{}:
	default:
	}

	result = &fileMeta
	return result, nil
}

func (h *StorHub) ReplaceFileFromReader(project, filePath string, body io.Reader) (*metadata.FileMeta, error) {
	return h.ReplaceFileFromReaderContext(context.Background(), project, filePath, body)
}

func (h *StorHub) ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader) (result *metadata.FileMeta, err error) {
	if body == nil {
		return nil, fmt.Errorf("request body is nil")
	}
	chunkSize := h.ChunkSize()

	releaseTag, uploadURL, err := h.PrepareReplaceContext(ctx, project, filePath, 0)
	if err != nil {
		return nil, err
	}

	var chunks []ChunkInfo
	var uploaded int64
	var index int
	buf := h.getBuffer()
	defer h.putBuffer(buf)

	// Wrap in bufio for efficiency on small-read bodies (e.g. HTTP).
	reader := body
	if _, ok := body.(*bufio.Reader); !ok {
		reader = bufio.NewReaderSize(body, int(chunkSize))
	}

	for {
		n, readErr := reader.Read(*buf)
		if n > 0 {
			chunk, uploadErr := h.UploadChunkDataContext(ctx, project, releaseTag, uploadURL, index, uploaded, (*buf)[:n])
			if uploadErr != nil {
				return nil, uploadErr
			}
			chunks = append(chunks, chunk)
			uploaded += int64(n)
			index++
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, readErr
		}
	}

	return h.FinalizeReplaceChunksContext(ctx, project, filePath, releaseTag, uploaded, chunks)
}

func (h *StorHub) FillChunkRangeContext(ctx context.Context, project string, chunk metadata.ChunkInfo, dst []byte) error {
	return h.fillAssetRange(ctx, project, chunk, dst)
}

func (h *StorHub) PatchFile(project, fileName string, offset, deleteSize int64, edit []byte) (*FileMeta, error) {
	return h.PatchFileContext(context.Background(), project, fileName, offset, deleteSize, edit)
}

func (h *StorHub) PatchFileContext(ctx context.Context, project, fileName string, offset, deleteSize int64, edit []byte) (result *FileMeta, err error) {
	started := h.logOpStart(project, "patch-file", "path", fileName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	defer func() {
		h.logOpFinish(project, "patch-file", started, err, "path", fileName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	}()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := shfs.NormalizePath(fileName)
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

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
	}
	if fileMeta.Symlink != "" {
		return nil, fmt.Errorf("cannot patch symlink: %s", cleanName)
	}
	patchEnd := offset + deleteSize
	if offset > fileMeta.Size || patchEnd > fileMeta.Size {
		return nil, fmt.Errorf("patch range [%d,%d) exceeds file size %d", offset, patchEnd, fileMeta.Size)
	}

	result, err = h.patchFileWithMetadataContext(ctx, project, cleanName, repoMeta, fileMeta, offset, deleteSize, edit)
	return result, err
}

func (h *StorHub) patchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *RepoMetadata, fileMeta *FileMeta, offset, deleteSize int64, edit []byte) (*FileMeta, error) {
	newChunks, _, err := h.buildPatchedChunks(ctx, project, repoMeta, *fileMeta, cleanName, offset, deleteSize, edit)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().UTC()
	patched := fileMeta.Clone()
	chunkNames := make([]string, len(newChunks))
	for i, chunk := range newChunks {
		name := fmt.Sprintf("%s/chunk/%d_%d", cleanName, now.UnixNano(), i)
		repoMeta.Chunks[name] = chunk
		chunkNames[i] = name
	}
	patched.Chunks = chunkNames
	patched.Size = fileMeta.Size - deleteSize + int64(len(edit))
	patched.Mode = shfs.SanitizeWrittenFileMode(patched.Mode)
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	for i, name := range chunkNames {
		pm.meta.Chunks[name] = newChunks[i]
	}
	current := pm.meta.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &patched, current, now)
	implposix.ReplaceInodeFamily(pm.meta, cleanName, current, patched, now)
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	select {
	case pm.triggerCh <- struct{}{}:
	default:
	}

	return &patched, nil
}

func (h *StorHub) rewriteFileRangesWithMetadataContext(ctx context.Context, project, cleanName, snapshotPath string, repoMeta *RepoMetadata, fileMeta *FileMeta, finalSize int64, dirtyRanges []byteRange) (*FileMeta, error) {
	newChunks, _, err := h.buildRewrittenChunks(ctx, project, repoMeta, *fileMeta, cleanName, snapshotPath, finalSize, dirtyRanges)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().UTC()
	rewritten := fileMeta.Clone()
	chunkNames := make([]string, len(newChunks))
	for i, chunk := range newChunks {
		name := fmt.Sprintf("%s/chunk/%d_%d", cleanName, now.UnixNano(), i)
		repoMeta.Chunks[name] = chunk
		chunkNames[i] = name
	}
	rewritten.Chunks = chunkNames
	rewritten.Size = finalSize
	rewritten.Mode = shfs.SanitizeWrittenFileMode(rewritten.Mode)
	rewritten.ModifiedAt = now
	rewritten.ChangedAt = now
	rewritten.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	for i, name := range chunkNames {
		pm.meta.Chunks[name] = newChunks[i]
	}
	current := pm.meta.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &rewritten, current, now)
	implposix.ReplaceInodeFamily(pm.meta, cleanName, current, rewritten, now)
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	select {
	case pm.triggerCh <- struct{}{}:
	default:
	}

	return &rewritten, nil
}

func (h *StorHub) putFileContext(ctx context.Context, project, fileName, inputPath string, replace bool) (result *FileMeta, err error) {
	op := "upload-file"
	if replace {
		op = "replace-file"
	}
	started := h.logOpStart(project, op, "path", fileName, "input", inputPath)
	defer func() { h.logOpFinish(project, op, started, err, "path", fileName, "input", inputPath) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanName, err := shfs.NormalizePath(fileName)
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
		return nil, shfs.IsDirectory(inputPath)
	}

	if err := h.ensureRepo(ctx, project); err != nil {
		return nil, err
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	if err := shfs.RequireParentDirectory(repoMeta, cleanName); err != nil {
		return nil, err
	}
	if err := shfs.CheckParentWrite(ctx, repoMeta, cleanName); err != nil {
		return nil, err
	}
	existing := repoMeta.FindFile(cleanName)
	var preferredRelease string
	if !replace && existing != nil {
		return nil, shfs.AlreadyExists(cleanName)
	}

	planner, err := chunking.NewStreamingChunker(inputPath, cleanName, h.config.ChunkSize)
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
		results, err = h.uploadChunks(ctx, project, releaseTag, uploadURL, planner)
		if err != nil {
			return nil, err
		}
	}
	chunkNames := make([]string, len(results))
	for i, chunk := range results {
		name := fmt.Sprintf("%s/chunk/%d", cleanName, i)
		workingMeta.Chunks[name] = chunk
		chunkNames[i] = name
	}
	fileMeta := FileMeta{
		Size:    fileInfo.Size(),
		Chunks:  chunkNames,
	}
	implposix.ApplyUploadIdentity(repoMeta, cleanName, existing, &fileMeta, h.config.Now().UTC())
	if existing == nil {
		defaultUID, defaultGID := h.DefaultOwnerIDs()
		fileMeta.UID, fileMeta.GID = shfs.OwnerIDsForCreate(ctx, defaultUID, defaultGID)
	}
	if existing != nil {
		fileMeta.Mode = shfs.SanitizeWrittenFileMode(fileMeta.Mode)
	}
	fileMeta.Mode, fileMeta.UID, fileMeta.GID = shfs.ApplyParentInheritance(repoMeta, cleanName, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	if err := shfs.CheckParentWrite(ctx, pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		return nil, err
	}
	if err := shfs.RequireParentDirectory(pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		return nil, err
	}
	if !replace && pm.meta.FindFile(cleanName) != nil {
		pm.mu.Unlock()
		return nil, shfs.AlreadyExists(cleanName)
	}
	pm.meta.EnsureRelease(releaseTag, h.config.Now().UTC())
	for i, name := range chunkNames {
		pm.meta.Chunks[name] = results[i]
	}
	current := pm.meta.FindFile(cleanName)
	if current != nil {
		implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, current, h.config.Now().UTC())
		implposix.ReplaceInodeFamily(pm.meta, cleanName, current, fileMeta, h.config.Now().UTC())
	} else {
		fileMeta.Mode, fileMeta.UID, fileMeta.GID = shfs.ApplyParentInheritance(pm.meta, cleanName, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
		metadata.InitializeNewFileIdentity(pm.meta, &fileMeta, h.config.Now().UTC())
		pm.meta.UpsertFile(cleanName, fileMeta, h.config.Now().UTC())
	}
	shfs.TouchParentDirectory(pm.meta, cleanName, h.config.Now().UTC())
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	select {
	case pm.triggerCh <- struct{}{}:
	default:
	}

	result = &fileMeta
	return result, nil
}

func (h *StorHub) DownloadFile(project, fileName, outputPath string) error {
	return h.DownloadFileContext(context.Background(), project, fileName, outputPath)
}

func (h *StorHub) DownloadFileContext(ctx context.Context, project, fileName, outputPath string) (err error) {
	started := h.logOpStart(project, "download-file", "path", fileName, "output", outputPath)
	defer func() { h.logOpFinish(project, "download-file", started, err, "path", fileName, "output", outputPath) }()
	if err := validateProject(project); err != nil {
		return err
	}
	cleanName, err := shfs.NormalizePath(fileName)
	if err != nil {
		return err
	}
	if cleanName == "" {
		return errors.New("file name is required")
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
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
	err = runConcurrent(ctx, h.config.MaxConcurrentTransfers, len(fileMeta.Chunks), func(i int) error {
		chunkName := fileMeta.Chunks[i]
		chunk, ok := repoMeta.Chunks[chunkName]
		if !ok {
			return fmt.Errorf("chunk %s not found", chunkName)
		}
		return h.downloadChunkWithRetry(ctx, project, outFile, chunk)
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
	return nil
}

func (h *StorHub) ListFiles(project string) ([]FileMeta, error) {
	return h.ListFilesContext(context.Background(), project)
}

func (h *StorHub) ListFilesContext(ctx context.Context, project string) (result []FileMeta, err error) {
	started := h.logOpStart(project, "list-files")
	defer func() { h.logOpFinish(project, "list-files", started, err, "count", len(result)) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	files := repoMeta.AllFiles()
	result = files
	return result, nil
}

func (h *StorHub) ListReleases(project string) ([]metadata.ReleaseRef, error) {
	return h.ListReleasesContext(context.Background(), project)
}

func (h *StorHub) ListReleasesContext(ctx context.Context, project string) (result []metadata.ReleaseRef, err error) {
	started := h.logOpStart(project, "list-releases")
	defer func() { h.logOpFinish(project, "list-releases", started, err, "count", len(result)) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	result = make([]metadata.ReleaseRef, 0, len(repoMeta.Releases))
	for _, ref := range repoMeta.Releases {
		result = append(result, ref.Clone())
	}
	return result, nil
}

func (h *StorHub) ListMetadataRevisions(project string) ([]MetadataRevision, error) {
	return h.ListMetadataRevisionsContext(context.Background(), project)
}

func (h *StorHub) ListMetadataRevisionsContext(ctx context.Context, project string) (result []MetadataRevision, err error) {
	started := h.logOpStart(project, "list-metadata-revisions")
	defer func() { h.logOpFinish(project, "list-metadata-revisions", started, err, "count", len(result)) }()
	if err := validateProject(project); err != nil {
		return nil, err
	}
	result, err = h.listMetadataRevisions(ctx, project)
	return result, err
}

func (h *StorHub) RollbackMetadata(project, commitSHA string) error {
	return h.RollbackMetadataContext(context.Background(), project, commitSHA)
}

func (h *StorHub) RollbackMetadataContext(ctx context.Context, project, commitSHA string) (err error) {
	started := h.logOpStart(project, "rollback-metadata", "commit_sha", commitSHA)
	defer func() { h.logOpFinish(project, "rollback-metadata", started, err, "commit_sha", commitSHA) }()
	if err := validateProject(project); err != nil {
		return err
	}
	if strings.TrimSpace(commitSHA) == "" {
		return errors.New("commit sha is required")
	}
	// Flush any dirty metadata first so cached SHA matches GitHub
	pm := h.getOrCreateProjectMeta(project)
	if err := h.commitProjectMetadata(ctx, project, pm); err != nil {
		return err
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

func (h *StorHub) downloadChunkWithRetry(ctx context.Context, project string, outFile *os.File, chunk ChunkInfo) error {
	if chunk.Size == 0 {
		return nil
	}
	buf := h.getBuffer()
	defer h.putBuffer(buf)

	for attempt := 0; attempt <= h.config.MaxRetries; attempt++ {
		reader, _, err := h.downloadAssetStream(ctx, project, chunk.AssetID, chunk.AssetOffset, chunk.AssetOffset+chunk.Size-1)
		if err != nil {
			if !isRetryableDownloadError(err) || attempt == h.config.MaxRetries {
				return fmt.Errorf("download chunk %d: %w", chunk.AssetID, err)
			}
			if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, extractAPIError(err))); sleepErr != nil {
				return sleepErr
			}
			continue
		}

		written, copyErr := h.writeChunk(outFile, reader, *buf, chunk)
		closeErr := reader.Close()
		if copyErr == nil && closeErr != nil {
			copyErr = closeErr
		}
		if copyErr == nil && written != chunk.Size {
			copyErr = fmt.Errorf("chunk %d size mismatch: expected %d, got %d", chunk.AssetID, chunk.Size, written)
		}
		if copyErr == nil {
			return nil
		}
		if !isRetryableDownloadError(copyErr) || attempt == h.config.MaxRetries {
			return fmt.Errorf("download chunk %d: %w", chunk.AssetID, copyErr)
		}
		if sleepErr := h.config.Sleep(ctx, h.retryDelay(attempt, extractAPIError(copyErr))); sleepErr != nil {
			return sleepErr
		}
	}
	return fmt.Errorf("download chunk %d: exhausted retries", chunk.AssetID)
}

func (h *StorHub) writeChunk(outFile *os.File, reader io.Reader, buf []byte, chunk ChunkInfo) (int64, error) {
	written := int64(0)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if _, writeErr := outFile.WriteAt(buf[:n], chunk.Offset+written); writeErr != nil {
				return written, fmt.Errorf("write chunk %d: %w", chunk.AssetID, writeErr)
			}
			written += int64(n)
		}
		if readErr == io.EOF {
			return written, nil
		}
		if readErr != nil {
			return written, fmt.Errorf("read chunk %d: %w", chunk.AssetID, readErr)
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
	var apiErr *ghapi.APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict
}

func extractAPIError(err error) *ghapi.APIError {
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return nil
}

func isRetryableDownloadError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}
	return isRetryableNetworkError(err)
}

func defaultFileMode(kind NodeKind) uint32 {
	switch kind {
	case NodeKindSymlink:
		return 0o777
	default:
		return 0o644
	}
}

func defaultDirMode() uint32 {
	return 0o755
}

func defaultOwnerIDs() (uint32, uint32) {
	return implposix.DefaultOwnerIDs()
}

func (h *StorHub) NewFUSE(project string, opts fusefs.Options) (*fusefs.Filesystem, error) {
	if opts.Logger == nil {
		opts.Logger = logging.WithComponent(h.logger, "fuse")
	}
	return fusefs.New(h, project, opts)
}

func (h *StorHub) Now() time.Time {
	return h.config.Now().UTC()
}

func (h *StorHub) ChunkSize() int64 {
	return h.config.ChunkSize
}

func (h *StorHub) LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*metadata.RepoMetadata, string, error) {
	return h.loadRepoMetadataReadonly(ctx, project)
}

// UpdateRepoMetadataContext updates metadata using the new batching system
// The mutation is applied to in-memory metadata and marked dirty for periodic commit
func (h *StorHub) UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*metadata.RepoMetadata) error, message string) (*metadata.RepoMetadata, error) {
	lockStarted := h.config.Now().UTC()
	pm := h.getOrCreateProjectMeta(project)

	logging.Debug(h.projectLogger(project), "metadata writer wait", "message", message)
	pm.mu.Lock()

	logging.Debug(h.projectLogger(project), "metadata writer acquired", "message", message, "wait", h.config.Now().UTC().Sub(lockStarted))

	started := h.config.Now().UTC()
	h.debugf("metadata update start project=%s message=%q", project, message)

	// Apply mutation to in-memory metadata
	if err := fn(pm.meta); err != nil {
		pm.mu.Unlock()
		h.debugf("metadata update failed project=%s step=apply elapsed=%s err=%v", project, h.config.Now().UTC().Sub(started), err)
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, err
	}

	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	// Trigger the commit loop to wake up immediately
	select {
	case pm.triggerCh <- struct{}{}:
	default:
	}

	h.debugf("metadata update complete project=%s elapsed=%s", project, h.config.Now().UTC().Sub(started))
	logging.Debug(h.projectLogger(project), "metadata update complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started))

	return pm.meta, nil
}

func (h *StorHub) RewriteFileRangesWithMetadataContext(ctx context.Context, project, cleanName, snapshotPath string, repoMeta *metadata.RepoMetadata, fileMeta *metadata.FileMeta, finalSize int64, dirtyRanges []fusefs.ByteRange) (*metadata.FileMeta, error) {
	ranges := make([]byteRange, len(dirtyRanges))
	for i, dirty := range dirtyRanges {
		ranges[i] = byteRange{start: dirty.Start, end: dirty.End}
	}
	return h.rewriteFileRangesWithMetadataContext(ctx, project, cleanName, snapshotPath, repoMeta, fileMeta, finalSize, ranges)
}

func (h *StorHub) ValidateProjectName(project string) error {
	return validateProject(project)
}

func (h *StorHub) EnsureRepoContext(ctx context.Context, project string) error {
	return h.ensureRepo(ctx, project)
}

func (h *StorHub) LoadRepoMetadataContext(ctx context.Context, project string) (*metadata.RepoMetadata, string, error) {
	return h.loadRepoMetadata(ctx, project)
}

func (h *StorHub) GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *metadata.RepoMetadata, requiredSize int, preferredTag string) (string, string, error) {
	return h.getOrCreateUploadRelease(ctx, project, repoMeta, requiredSize, preferredTag)
}

func (h *StorHub) PatchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *metadata.RepoMetadata, fileMeta *metadata.FileMeta, offset, deleteSize int64, edit []byte) (*metadata.FileMeta, error) {
	return h.patchFileWithMetadataContext(ctx, project, cleanName, repoMeta, fileMeta, offset, deleteSize, edit)
}

func (h *StorHub) FillAssetRangeContext(ctx context.Context, project string, segment metadata.ChunkInfo, dst []byte) error {
	return h.fillAssetRange(ctx, project, segment, dst)
}

func (h *StorHub) FileNotFound(path string) error {
	return shfs.NotFound(path)
}

func (h *StorHub) DefaultFileMode(kind metadata.NodeKind) uint32 {
	return defaultFileMode(kind)
}

func (h *StorHub) DefaultOwnerIDs() (uint32, uint32) {
	return defaultOwnerIDs()
}

func (h *StorHub) AtimePolicy() storcfg.AtimePolicy {
	return h.config.AtimePolicy
}

func (h *StorHub) fsService() *shfs.Service {
	return shfs.NewService(h)
}

func (h *StorHub) posixService() *implposix.Service {
	return implposix.NewService(h)
}

func (h *StorHub) CreateFile(project, filePath string) (*metadata.FileMeta, error) {
	return h.CreateFileContext(context.Background(), project, filePath)
}

func (h *StorHub) CreateFileContext(ctx context.Context, project, filePath string) (*metadata.FileMeta, error) {
	return h.fsService().CreateFileContext(ctx, project, filePath)
}

func (h *StorHub) Mkdir(project, dirPath string) error {
	return h.MkdirContext(context.Background(), project, dirPath)
}

func (h *StorHub) MkdirContext(ctx context.Context, project, dirPath string) error {
	return h.fsService().MkdirContext(ctx, project, dirPath)
}

func (h *StorHub) Unlink(project, filePath string) error {
	return h.DeleteFile(project, filePath)
}

func (h *StorHub) UnlinkContext(ctx context.Context, project, filePath string) error {
	return h.DeleteFileContext(ctx, project, filePath)
}

func (h *StorHub) Rmdir(project, dirPath string) error {
	return h.RmdirContext(context.Background(), project, dirPath)
}

func (h *StorHub) RmdirContext(ctx context.Context, project, dirPath string) error {
	return h.fsService().RmdirContext(ctx, project, dirPath)
}

func (h *StorHub) Rename(project, oldPath, newPath string) error {
	return h.RenameContext(context.Background(), project, oldPath, newPath)
}

func (h *StorHub) RenameContext(ctx context.Context, project, oldPath, newPath string) error {
	return h.fsService().RenameContext(ctx, project, oldPath, newPath)
}

func (h *StorHub) TruncateFile(project, filePath string, size int64) (*metadata.FileMeta, error) {
	return h.TruncateFileContext(context.Background(), project, filePath, size)
}

func (h *StorHub) TruncateFileContext(ctx context.Context, project, filePath string, size int64) (*metadata.FileMeta, error) {
	return h.fsService().TruncateFileContext(ctx, project, filePath, size)
}

func (h *StorHub) AppendFile(project, filePath string, data []byte) (*metadata.FileMeta, error) {
	return h.AppendFileContext(context.Background(), project, filePath, data)
}

func (h *StorHub) AppendFileContext(ctx context.Context, project, filePath string, data []byte) (*metadata.FileMeta, error) {
	return h.fsService().AppendFileContext(ctx, project, filePath, data)
}

func (h *StorHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*metadata.FileMeta, error) {
	return h.WriteFileAtContext(context.Background(), project, filePath, offset, data)
}

func (h *StorHub) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte) (*metadata.FileMeta, error) {
	return h.fsService().WriteFileAtContext(ctx, project, filePath, offset, data)
}

func (h *StorHub) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	return h.ReadFileAtContext(context.Background(), project, filePath, offset, length)
}

func (h *StorHub) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	result := make([]byte, length)
	n, err := h.ReadFileAtBufferContext(ctx, project, filePath, offset, result)
	if err != nil {
		return nil, err
	}
	return result[:n], nil
}

func (h *StorHub) ReadFileAtBufferContext(ctx context.Context, project, filePath string, offset int64, result []byte) (int, error) {
	if err := validateProject(project); err != nil {
		return 0, err
	}
	cleanPath, err := shfs.NormalizePath(filePath)
	if err != nil {
		return 0, err
	}
	if cleanPath == "" {
		return 0, errors.New("file name is required")
	}
	if offset < 0 {
		return 0, errors.New("read offset and length must be non-negative")
	}
	repo, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return 0, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return 0, fmt.Errorf("%w: %s", ErrFileNotFound, cleanPath)
	}
	if err := shfs.CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return 0, err
	}
	if file.Symlink != "" {
		return 0, shfs.InvalidSymlink(cleanPath)
	}
	if offset > file.Size {
		return 0, io.EOF
	}
	if len(result) == 0 {
		return 0, nil
	}
	end := offset + int64(len(result))
	if end > file.Size {
		end = file.Size
	}
	segments := overlappingFileSegments(file, repo.Chunks, offset, end)
	if len(segments) == 0 {
		shfs.TouchFileAccessTime(ctx, h, project, cleanPath, h.config.Now().UTC())
		return int(end - offset), nil
	}
	// Flatten two-tier parallelism: sub-split every large segment, then
	// dispatch all sub-segments through a single runConcurrent pool.
	if h.config.MaxConcurrentTransfers > 1 {
		all := make([]fileReadSegment, 0, len(segments)*2)
		for _, seg := range segments {
			if seg.end-seg.start >= minParallelSegmentSize {
				all = append(all, splitSegment(seg, h.config.MaxConcurrentTransfers)...)
			} else {
				all = append(all, seg)
			}
		}
		if len(all) > 1 {
			if err := runConcurrent(ctx, h.config.MaxConcurrentTransfers, len(all), func(i int) error {
				sub := all[i]
				return h.fillAssetRange(ctx, project, sub.chunk, result[sub.start:sub.end])
			}); err != nil {
				return 0, err
			}
			shfs.TouchFileAccessTime(ctx, h, project, cleanPath, h.config.Now().UTC())
			return int(end - offset), nil
		}
	}
	for _, segment := range segments {
		if err := h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end]); err != nil {
			return 0, err
		}
	}
	shfs.TouchFileAccessTime(ctx, h, project, cleanPath, h.config.Now().UTC())
	return int(end - offset), nil
}

type fileReadSegment struct {
	chunk metadata.ChunkInfo
	start int
	end   int
}

func splitSegment(seg fileReadSegment, maxParts int) []fileReadSegment {
	size := seg.end - seg.start
	if maxParts > size {
		maxParts = size
	}
	partSize := (size + maxParts - 1) / maxParts
	subs := make([]fileReadSegment, 0, maxParts)
	for off := 0; off < size; off += partSize {
		end := off + partSize
		if end > size {
			end = size
		}
		partLen := end - off
		subs = append(subs, fileReadSegment{
			chunk: metadata.ChunkInfo{
				Size:        int64(partLen),
				Offset:      seg.chunk.Offset + int64(off),
				AssetOffset: seg.chunk.AssetOffset + int64(off),
				AssetID:     seg.chunk.AssetID,
			},
			start: seg.start + off,
			end:   seg.start + end,
		})
	}
	return subs
}

func overlappingFileSegments(file *metadata.FileMeta, repoChunks map[string]metadata.ChunkInfo, offset, end int64) []fileReadSegment {
	if file == nil || end <= offset {
		return nil
	}
	segments := make([]fileReadSegment, 0, len(file.Chunks))
	chunks := make([]metadata.ChunkInfo, 0, len(file.Chunks))
	for _, name := range file.Chunks {
		if chunk, ok := repoChunks[name]; ok {
			chunks = append(chunks, chunk)
		}
	}
	startIndex := sort.Search(len(chunks), func(i int) bool {
		return chunks[i].Offset+chunks[i].Size > offset
	})
	for _, chunk := range chunks[startIndex:] {
		chunkEnd := chunk.Offset + chunk.Size
		if chunk.Offset >= end {
			break
		}
		if chunkEnd <= offset || chunk.Size == 0 {
			continue
		}
		segmentStart := shfs.MaxInt64(offset, chunk.Offset)
		segmentEnd := shfs.MinInt64(end, chunkEnd)
		segment := chunk
		segment.Offset = segmentStart
		segment.AssetOffset = chunk.AssetOffset + (segmentStart - chunk.Offset)
		segment.Size = segmentEnd - segmentStart
		segments = append(segments, fileReadSegment{
			chunk: segment,
			start: int(segmentStart - offset),
			end:   int(segmentEnd - offset),
		})
	}
	return segments
}

func (h *StorHub) StatPath(project, targetPath string) (*shfs.EntryInfo, error) {
	return h.StatPathContext(context.Background(), project, targetPath)
}

func (h *StorHub) StatPathContext(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	return h.fsService().StatPathContext(ctx, project, targetPath)
}

func (h *StorHub) ReadDir(project, dirPath string) ([]shfs.DirEntry, error) {
	return h.ReadDirContext(context.Background(), project, dirPath)
}

func (h *StorHub) ReadDirContext(ctx context.Context, project, dirPath string) ([]shfs.DirEntry, error) {
	return h.fsService().ReadDirContext(ctx, project, dirPath)
}

func (h *StorHub) StatFS(project string) (*shfs.FSStats, error) {
	return h.StatFSContext(context.Background(), project)
}

func (h *StorHub) StatFSContext(ctx context.Context, project string) (*shfs.FSStats, error) {
	return h.fsService().StatFSContext(ctx, project)
}

func (h *StorHub) Symlink(project, target, linkPath string) (*metadata.FileMeta, error) {
	return h.SymlinkContext(context.Background(), project, target, linkPath)
}

func (h *StorHub) SymlinkContext(ctx context.Context, project, target, linkPath string) (*metadata.FileMeta, error) {
	return h.posixService().SymlinkContext(ctx, project, target, linkPath)
}

func (h *StorHub) Readlink(project, linkPath string) (string, error) {
	return h.ReadlinkContext(context.Background(), project, linkPath)
}

func (h *StorHub) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	return h.posixService().ReadlinkContext(ctx, project, linkPath)
}

func (h *StorHub) Link(project, existingPath, newPath string) (*metadata.FileMeta, error) {
	return h.LinkContext(context.Background(), project, existingPath, newPath)
}

func (h *StorHub) LinkContext(ctx context.Context, project, existingPath, newPath string) (*metadata.FileMeta, error) {
	return h.posixService().LinkContext(ctx, project, existingPath, newPath)
}

func (h *StorHub) Chmod(project, targetPath string, mode uint32) error {
	return h.ChmodContext(context.Background(), project, targetPath, mode)
}

func (h *StorHub) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error {
	return h.posixService().ChmodContext(ctx, project, targetPath, mode)
}

func (h *StorHub) Chown(project, targetPath string, uid, gid uint32) error {
	return h.ChownContext(context.Background(), project, targetPath, uid, gid)
}

func (h *StorHub) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error {
	return h.posixService().ChownContext(ctx, project, targetPath, uid, gid)
}

func (h *StorHub) Chtimes(project, targetPath string, atime, mtime time.Time) error {
	return h.ChtimesContext(context.Background(), project, targetPath, atime, mtime)
}

func (h *StorHub) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime time.Time) error {
	return h.posixService().ChtimesContext(ctx, project, targetPath, atime, mtime)
}

func (h *StorHub) SetXAttr(project, targetPath, attr string, data []byte) error {
	return h.SetXAttrContext(context.Background(), project, targetPath, attr, data)
}

func (h *StorHub) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte) error {
	return h.posixService().SetXAttrContext(ctx, project, targetPath, attr, data)
}

func (h *StorHub) GetXAttr(project, targetPath, attr string) ([]byte, error) {
	return h.GetXAttrContext(context.Background(), project, targetPath, attr)
}

func (h *StorHub) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
	return h.posixService().GetXAttrContext(ctx, project, targetPath, attr)
}

func (h *StorHub) ListXAttr(project, targetPath string) ([]string, error) {
	return h.ListXAttrContext(context.Background(), project, targetPath)
}

func (h *StorHub) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	return h.posixService().ListXAttrContext(ctx, project, targetPath)
}

func (h *StorHub) RemoveXAttr(project, targetPath, attr string) error {
	return h.RemoveXAttrContext(context.Background(), project, targetPath, attr)
}

func (h *StorHub) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error {
	return h.posixService().RemoveXAttrContext(ctx, project, targetPath, attr)
}

func (h *StorHub) ApplyMetadataPatchContext(ctx context.Context, project, targetPath string, patch shfs.MetadataPatch) error {
	return h.posixService().ApplyMetadataPatchContext(ctx, project, targetPath, patch)
}
