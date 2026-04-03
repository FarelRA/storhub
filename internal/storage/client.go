package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
	metadata "github.com/FarelRA/storhub/internal/metadata"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

const maxMetadataBytes = 8 << 20

var githubRepoNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type (
	Config            = storcfg.Config
	ChunkInfo         = metadata.ChunkInfo
	FileMetadata      = metadata.FileMetadata
	ReleaseMetadata   = metadata.ReleaseMetadata
	RepoMetadata      = metadata.RepoMetadata
	MetadataRevision  = metadata.MetadataRevision
	DirectoryMetadata = metadata.DirectoryMetadata
	RootMetadata      = metadata.RootMetadata
	NodeKind          = metadata.NodeKind
)

const (
	NodeKindFile    = metadata.NodeKindFile
	NodeKindSymlink = metadata.NodeKindSymlink
)

var ErrFileNotFound = errors.New("file not found")

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
	metaMu     sync.RWMutex
	metaCache  map[string]cachedMetadata
}

type cachedMetadata struct {
	sha  string
	meta metadata.RepoMetadata
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
		token:     token,
		gh:        ghapi.NewClient(token, cfg),
		config:    cfg,
		repoState: make(map[string]bool),
		metaCache: make(map[string]cachedMetadata),
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

func (h *StorHub) PrepareReplaceContext(ctx context.Context, project, fileName string, requiredSlots int) (string, string, error) {
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
	return h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots, existing.Release)
}

func (h *StorHub) UploadChunkDataContext(ctx context.Context, project, releaseTag, uploadURL string, index int, offset int64, data []byte) (ChunkInfo, error) {
	assetName, err := randomAssetName()
	if err != nil {
		return ChunkInfo{}, err
	}
	assetID, checksum, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return ChunkInfo{}, err
	}
	return ChunkInfo{
		Name:        assetName,
		Size:        int64(len(data)),
		Index:       index,
		Offset:      offset,
		Release:     releaseTag,
		AssetID:     assetID,
		AssetOffset: 0,
		CRC32C:      checksum,
	}, nil
}

func (h *StorHub) FinalizeReplaceChunksContext(ctx context.Context, project, fileName, releaseTag string, size int64, chunks []ChunkInfo) (*FileMetadata, error) {
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
	crc32cSum, err := chunking.CombineChunkCRC32Cs(chunks)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().UTC()
	fileMeta := current.Clone()
	fileMeta.Chunks = append([]ChunkInfo(nil), chunks...)
	fileMeta.Release = releaseTag
	fileMeta.Size = size
	fileMeta.CRC32C = crc32cSum
	implposix.ApplyUpdatedFileIdentity(&fileMeta, current, now)
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		latest := meta.FindFile(cleanName)
		if latest == nil {
			return fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
		}
		implposix.ApplyUpdatedFileIdentity(&fileMeta, latest, now)
		implposix.ReplaceInodeFamily(meta, latest, fileMeta, now)
		return nil
	}, metadataCommitMessage(cleanName, true)); err != nil {
		return nil, err
	}
	return &fileMeta, nil
}

func (h *StorHub) FillChunkRangeContext(ctx context.Context, project string, chunk metadata.ChunkInfo, dst []byte) error {
	return h.fillAssetRange(ctx, project, chunk, dst)
}

func (h *StorHub) PatchFile(project, fileName string, offset, deleteSize int64, edit []byte) (*FileMetadata, error) {
	return h.PatchFileContext(context.Background(), project, fileName, offset, deleteSize, edit)
}

func (h *StorHub) PatchFileContext(ctx context.Context, project, fileName string, offset, deleteSize int64, edit []byte) (*FileMetadata, error) {
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
	if fileMeta.Kind == NodeKindSymlink {
		return nil, fmt.Errorf("cannot patch symlink: %s", cleanName)
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
	now := h.config.Now().UTC()
	patched := fileMeta.Clone()
	patched.Chunks = newChunks
	patched.Release = targetRelease
	patched.Size = fileMeta.Size - deleteSize + int64(len(edit))
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	patched.CRC32C, err = chunking.CombineChunkCRC32Cs(patched.Chunks)
	if err != nil {
		return nil, err
	}
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		current := meta.FindFile(cleanName)
		if current == nil {
			return fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
		}
		implposix.ApplyUpdatedFileIdentity(&patched, current, now)
		implposix.ReplaceInodeFamily(meta, current, patched, now)
		return nil
	}, fmt.Sprintf("storhub: patch %s at %d delete %d insert %d", cleanName, offset, deleteSize, len(edit))); err != nil {
		return nil, err
	}
	return &patched, nil
}

func (h *StorHub) rewriteFileRangesWithMetadataContext(ctx context.Context, project, cleanName, snapshotPath string, repoMeta *RepoMetadata, fileMeta *FileMetadata, finalSize int64, dirtyRanges []byteRange) (*FileMetadata, error) {
	newChunks, targetRelease, err := h.buildRewrittenChunks(ctx, project, repoMeta, *fileMeta, snapshotPath, finalSize, dirtyRanges)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().UTC()
	rewritten := fileMeta.Clone()
	rewritten.Chunks = newChunks
	rewritten.Release = targetRelease
	rewritten.Size = finalSize
	rewritten.ModifiedAt = now
	rewritten.ChangedAt = now
	rewritten.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	rewritten.CRC32C, err = chunking.CombineChunkCRC32Cs(rewritten.Chunks)
	if err != nil {
		return nil, err
	}
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		current := meta.FindFile(cleanName)
		if current == nil {
			return fmt.Errorf("%w: %s", ErrFileNotFound, cleanName)
		}
		implposix.ApplyUpdatedFileIdentity(&rewritten, current, now)
		implposix.ReplaceInodeFamily(meta, current, rewritten, now)
		return nil
	}, fmt.Sprintf("storhub: rewrite chunks for %s", cleanName)); err != nil {
		return nil, err
	}
	return &rewritten, nil
}

func (h *StorHub) putFileContext(ctx context.Context, project, fileName, inputPath string, replace bool) (*FileMetadata, error) {
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
		return nil, fmt.Errorf("input path %q is a directory", inputPath)
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
	existing := repoMeta.FindFile(cleanName)
	var preferredRelease string
	if existing != nil {
		preferredRelease = existing.Release
	}
	if !replace && existing != nil {
		return nil, fmt.Errorf("file already exists: %s", cleanName)
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
	crc32cSum, err := chunking.CombineChunkCRC32Cs(results)
	if err != nil {
		return nil, err
	}

	fileMeta := FileMetadata{
		Name:    cleanName,
		Kind:    NodeKindFile,
		Size:    fileInfo.Size(),
		Chunks:  results,
		Release: releaseTag,
		CRC32C:  crc32cSum,
	}
	implposix.ApplyUploadIdentity(repoMeta, existing, &fileMeta, h.config.Now().UTC())
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if err := shfs.RequireParentDirectory(meta, cleanName); err != nil {
			return err
		}
		if !replace && meta.FindFile(cleanName) != nil {
			return fmt.Errorf("file already exists: %s", cleanName)
		}
		current := meta.FindFile(cleanName)
		if current != nil {
			implposix.ApplyUpdatedFileIdentity(&fileMeta, current, h.config.Now().UTC())
			implposix.ReplaceInodeFamily(meta, current, fileMeta, h.config.Now().UTC())
		} else {
			metadata.InitializeNewFileIdentity(meta, &fileMeta, h.config.Now().UTC())
			meta.UpsertFile(fileMeta, h.config.Now().UTC())
		}
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
	crc32cSum, err := chunking.CombineChunkCRC32Cs(verifiedChunks)
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

func (h *StorHub) UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*metadata.RepoMetadata) error, message string) (*metadata.RepoMetadata, error) {
	return h.updateRepoMetadata(ctx, project, fn, message)
}

func (h *StorHub) RewriteFileRangesWithMetadataContext(ctx context.Context, project, cleanName, snapshotPath string, repoMeta *metadata.RepoMetadata, fileMeta *metadata.FileMetadata, finalSize int64, dirtyRanges []fusefs.ByteRange) (*metadata.FileMetadata, error) {
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

func (h *StorHub) PatchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *metadata.RepoMetadata, fileMeta *metadata.FileMetadata, offset, deleteSize int64, edit []byte) (*metadata.FileMetadata, error) {
	return h.patchFileWithMetadataContext(ctx, project, cleanName, repoMeta, fileMeta, offset, deleteSize, edit)
}

func (h *StorHub) FillAssetRangeContext(ctx context.Context, project string, segment metadata.ChunkInfo, dst []byte) error {
	return h.fillAssetRange(ctx, project, segment, dst)
}

func (h *StorHub) FileNotFound(path string) error {
	return fmt.Errorf("%w: %s", ErrFileNotFound, path)
}

func (h *StorHub) DefaultFileMode(kind metadata.NodeKind) uint32 {
	return defaultFileMode(kind)
}

func (h *StorHub) DefaultOwnerIDs() (uint32, uint32) {
	return defaultOwnerIDs()
}

func (h *StorHub) CombineChunkCRC32Cs(chunks []metadata.ChunkInfo) (string, error) {
	return chunking.CombineChunkCRC32Cs(chunks)
}

func (h *StorHub) fsService() *shfs.Service {
	return shfs.NewService(h)
}

func (h *StorHub) posixService() *implposix.Service {
	return implposix.NewService(h)
}

func (h *StorHub) CreateFile(project, filePath string) (*metadata.FileMetadata, error) {
	return h.CreateFileContext(context.Background(), project, filePath)
}

func (h *StorHub) CreateFileContext(ctx context.Context, project, filePath string) (*metadata.FileMetadata, error) {
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

func (h *StorHub) TruncateFile(project, filePath string, size int64) (*metadata.FileMetadata, error) {
	return h.TruncateFileContext(context.Background(), project, filePath, size)
}

func (h *StorHub) TruncateFileContext(ctx context.Context, project, filePath string, size int64) (*metadata.FileMetadata, error) {
	return h.fsService().TruncateFileContext(ctx, project, filePath, size)
}

func (h *StorHub) AppendFile(project, filePath string, data []byte) (*metadata.FileMetadata, error) {
	return h.AppendFileContext(context.Background(), project, filePath, data)
}

func (h *StorHub) AppendFileContext(ctx context.Context, project, filePath string, data []byte) (*metadata.FileMetadata, error) {
	return h.fsService().AppendFileContext(ctx, project, filePath, data)
}

func (h *StorHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*metadata.FileMetadata, error) {
	return h.WriteFileAtContext(context.Background(), project, filePath, offset, data)
}

func (h *StorHub) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte) (*metadata.FileMetadata, error) {
	return h.fsService().WriteFileAtContext(ctx, project, filePath, offset, data)
}

func (h *StorHub) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	return h.ReadFileAtContext(context.Background(), project, filePath, offset, length)
}

func (h *StorHub) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanPath, err := shfs.NormalizePath(filePath)
	if err != nil {
		return nil, err
	}
	if cleanPath == "" {
		return nil, errors.New("file name is required")
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	repo, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanPath)
	}
	if file.Kind == NodeKindSymlink {
		return nil, fmt.Errorf("cannot read symlink as file: %s", cleanPath)
	}
	if offset > file.Size {
		return nil, io.EOF
	}
	if length == 0 {
		return []byte{}, nil
	}
	end := offset + length
	if end > file.Size {
		end = file.Size
	}
	result := make([]byte, end-offset)
	segments := overlappingFileSegments(file, offset, end)
	if len(segments) == 0 {
		return result, nil
	}
	if len(segments) == 1 || h.config.MaxConcurrentTransfers <= 1 {
		for _, segment := range segments {
			if err := h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end]); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	if err := runConcurrent(ctx, h.config.MaxConcurrentTransfers, len(segments), func(i int) error {
		segment := segments[i]
		return h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end])
	}); err != nil {
		return nil, err
	}
	return result, nil
}

type fileReadSegment struct {
	chunk metadata.ChunkInfo
	start int
	end   int
}

func overlappingFileSegments(file *metadata.FileMetadata, offset, end int64) []fileReadSegment {
	if file == nil || end <= offset {
		return nil
	}
	segments := make([]fileReadSegment, 0, len(file.Chunks))
	startIndex := sort.Search(len(file.Chunks), func(i int) bool {
		return file.Chunks[i].Offset+file.Chunks[i].Size > offset
	})
	for _, chunk := range file.Chunks[startIndex:] {
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

func (h *StorHub) Symlink(project, target, linkPath string) (*metadata.FileMetadata, error) {
	return h.SymlinkContext(context.Background(), project, target, linkPath)
}

func (h *StorHub) SymlinkContext(ctx context.Context, project, target, linkPath string) (*metadata.FileMetadata, error) {
	return h.posixService().SymlinkContext(ctx, project, target, linkPath)
}

func (h *StorHub) Readlink(project, linkPath string) (string, error) {
	return h.ReadlinkContext(context.Background(), project, linkPath)
}

func (h *StorHub) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	return h.posixService().ReadlinkContext(ctx, project, linkPath)
}

func (h *StorHub) Link(project, existingPath, newPath string) (*metadata.FileMetadata, error) {
	return h.LinkContext(context.Background(), project, existingPath, newPath)
}

func (h *StorHub) LinkContext(ctx context.Context, project, existingPath, newPath string) (*metadata.FileMetadata, error) {
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
