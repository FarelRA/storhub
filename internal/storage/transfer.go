package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

func (h *StorHub) PrepareReplaceContext(ctx context.Context, project, fileName string, requiredSlots int) (releaseTag string, uploadURL string, err error) {
	started := h.logOpStart(project, "prepare-replace", "path", fileName, "required_slots", requiredSlots)
	defer func() {
		h.logOpFinish(project, "prepare-replace", started, err, "path", fileName, "required_slots", requiredSlots, "release", releaseTag)
	}()
	if err := validateProject(project); err != nil {
		return "", "", err
	}
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return "", "", err
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return "", "", err
	}
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return "", "", err
	}
	if cleanName == "" {
		return "", "", errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
		return "", "", err
	}
	if err := shfs.RequireParentDirectory(repoMeta, cleanName); err != nil {
		return "", "", err
	}
	existing := repoMeta.FindFile(cleanName)
	if existing == nil {
		return "", "", fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(cleanName)
	releaseTag, uploadURL, err = h.getOrCreateUploadRelease(ctx, project, workingMeta, requiredSlots)
	return releaseTag, uploadURL, err
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
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
		return nil, err
	}
	current := repoMeta.FindFile(cleanName)
	if current == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// Trim chunks beyond the logical file size.
	// Kernel writeback cache may flush stale dirty pages from the previous
	// file content (before truncation), producing chunks past the new EOF.
	chunks = trimChunks(chunks, size)

	now := h.config.Now().Unix()
	fileMeta := current.Clone()

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, chunks)
		return nil, err
	}

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. PrepareReplaceContext EnsureReleases only on a local
	// clone that is discarded before this call. All mutations apply to a
	// private COW copy; the shared tree is swapped in only on success.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, now)
	ensureChunkReleases(tree, chunks, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(chunks))
	for i := range chunks {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, chunks[i])
		chunkIDs[i] = id
	}
	fileMeta.Chunks = chunkIDs
	fileMeta.Size = size
	latest := tree.FindFile(cleanName)
	if latest == nil {
		// The file vanished between the readonly pre-check and this
		// locked re-check (concurrent delete). The chunks are already
		// uploaded and their IDs already allocated into the catalog:
		// roll both back or the assets leak until PurgeUntracked and the
		// catalog carries chunks no file references. The COW copy is
		// discarded, so the shared tree never saw the allocation.
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, chunks)
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, latest, now)
	implposix.ReplaceInodeFamily(tree, cleanName, latest, fileMeta, now)
	siblings := tree.FindFilesByInode(fileMeta.Inode)
	publishTreeLocked(pm, tree)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	opFile := fileMeta.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPutFile, Paths: []string{cleanName}, Cause: "replace-chunks",
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, fileMeta.Inode, cleanName, "replace-chunks-family", now)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	result = &fileMeta
	return result, nil
}

func (h *StorHub) ReplaceFileFromReader(project, filePath string, body io.Reader) (*metadata.FileMeta, error) {
	return h.ReplaceFileFromReaderContext(context.Background(), project, filePath, body)
}

func (h *StorHub) ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (result *metadata.FileMeta, err error) {
	if body == nil {
		return nil, fmt.Errorf("request body is nil")
	}
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}

	size, hasSize := shfs.ApplyMutateOptions(opts).ExpectedSize()
	if !hasSize {
		return nil, fmt.Errorf("upload size unknown: pass fs.WithSize(n) (REST callers: Content-Length)")
	}
	chunkSize := chunking.NormalizedSize(h.ChunkSize())
	requiredSlots := 0
	if size > 0 {
		requiredSlots = int((size + chunkSize - 1) / chunkSize)
	}
	releaseTag, uploadURL, err := h.PrepareReplaceContext(ctx, project, filePath, requiredSlots)
	if err != nil {
		return nil, err
	}

	// Stream the body straight into per-chunk GitHub uploads. Each window is
	// tee-mirrored to a spool file so transport retries rewind from disk
	// instead of re-reading the network; a failed window compensates by
	// deleting earlier windows of this call, keeping metadata-atomicity.
	// Name-collision retries and release-full rotation live in the sink.
	totalChunks := 0
	if size > 0 {
		totalChunks = int((size + chunkSize - 1) / chunkSize)
	}
	prepare := func(remaining int) (string, string, error) {
		return h.PrepareReplaceContext(ctx, project, filePath, remaining)
	}
	sink := h.newChunkSink(ctx, project, releaseTag, uploadURL, totalChunks, prepare)
	var uploaded int64
	for uploaded < size {
		windowSize := min64(chunkSize, size-uploaded)

		win, cleanup, werr := newWindowReader(body, windowSize)
		if werr != nil {
			h.compensateDeleteAssets(ctx, project, sink.results)
			return nil, werr
		}

		if err := sink.put(win, windowSize, uploaded); err != nil {
			cleanup()
			h.compensateDeleteAssets(ctx, project, sink.results)
			return nil, err
		}
		cleanup()
		uploaded += windowSize
	}

	return h.FinalizeReplaceChunksContext(ctx, project, filePath, sink.releaseTag, uploaded, sink.results)
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
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return nil, err
	}

	// Backpressure gate: reserve the project's metadata slot BEFORE any
	// upload work. A brand-new project is refused here (cheaply, with no
	// orphaned assets) when the cache is at cap and every resident project
	// holds unpushed changes; an already-resident project passes through.
	if _, err := h.getOrCreateProjectMetaAdmitted(project); err != nil {
		return nil, err
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
	// put has open(O_CREAT|O_TRUNC) semantics: a final symlink is followed
	// to its target, so followFinal is true.
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
		return nil, err
	}
	if err := shfs.RequireParentDirectory(repoMeta, cleanName); err != nil {
		return nil, err
	}
	if err := shfs.CheckParentWrite(ctx, repoMeta, cleanName); err != nil {
		return nil, err
	}
	if repoMeta.HasDirectory(cleanName) {
		return nil, shfs.IsDirectory(cleanName)
	}
	existing := repoMeta.FindFile(cleanName)
	if !replace && existing != nil {
		return nil, shfs.AlreadyExists(cleanName)
	}

	planner, err := chunking.NewStreamingChunker(inputPath, cleanName, h.config.ChunkSize)
	if err != nil {
		return nil, err
	}
	defer func() { _ = planner.Close() }()

	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(cleanName)
	requiredSlots := planner.NumChunks()
	if fileInfo.Size() == 0 {
		requiredSlots = 0
	}
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, workingMeta, requiredSlots)
	if err != nil {
		return nil, err
	}

	results := []ChunkInfo{}
	if fileInfo.Size() > 0 {
		prepare := func(remaining int) (string, string, error) {
			return h.getOrCreateUploadRelease(ctx, project, workingMeta, remaining)
		}
		results, err = h.uploadChunks(ctx, project, releaseTag, uploadURL, planner, prepare)
		if err != nil {
			// The file never commits: delete this call's orphaned assets so
			// a mid-upload failure cannot leak storage. Rotation inside the
			// sink already moved later chunks; only compensate what landed.
			h.compensateDeleteAssets(ctx, project, results)
			return nil, err
		}
	}
	fileMeta := FileMeta{
		Size:   fileInfo.Size(),
		Chunks: nil,
	}
	implposix.ApplyUploadIdentity(cleanName, existing, &fileMeta, h.config.Now().Unix())
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
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}

	if err := shfs.CheckTraversal(ctx, pm.meta, traversed); err != nil {
		pm.mu.Unlock()
		// The chunks are already uploaded by now; a late permission
		// failure must not leak them as orphans.
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}
	if err := shfs.CheckParentWrite(ctx, pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		// The chunks are already uploaded by now; a late permission
		// failure must not leak them as orphans.
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}
	if err := shfs.RequireParentDirectory(pm.meta, cleanName); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}
	if pm.meta.HasDirectory(cleanName) {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, shfs.IsDirectory(cleanName)
	}
	if !replace && pm.meta.FindFile(cleanName) != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, shfs.AlreadyExists(cleanName)
	}
	// All mutations apply to a private COW copy; the shared tree is swapped
	// in only once every fallible step has succeeded.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, h.config.Now().Unix())
	// Rotation may have spread this file's chunks across releases;
	// ensuring only the initial tag would strand rotated chunks outside
	// the catalog where PurgeUntracked deletes live data.
	ensureChunkReleases(tree, results, h.config.Now().Unix())
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(results))
	for i := range results {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, results[i])
		chunkIDs[i] = id
	}
	fileMeta.Chunks = chunkIDs
	current := tree.FindFile(cleanName)
	if current != nil {
		implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, current, h.config.Now().Unix())
		implposix.ReplaceInodeFamily(tree, cleanName, current, fileMeta, h.config.Now().Unix())
	} else {
		fileMeta.Mode, fileMeta.UID, fileMeta.GID = shfs.ApplyParentInheritance(tree, cleanName, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
		metadata.InitializeNewFileIdentity(tree, &fileMeta, h.config.Now().Unix())
		tree.UpsertFile(cleanName, fileMeta, h.config.Now().Unix())
	}
	shfs.TouchParentDirectory(tree, cleanName, h.config.Now().Unix())
	siblings := tree.FindFilesByInode(fileMeta.Inode)
	publishTreeLocked(pm, tree)
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	cause := "upload"
	if replace {
		cause = "replace"
	}
	now := h.config.Now().Unix()
	opFile := fileMeta.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPutFile, Paths: []string{cleanName}, Cause: cause,
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, fileMeta.Inode, cleanName, cause+"-family", now)
	h.emitParentDirOpLocked(project, pm, cleanName, cause+"-parent", now)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	result = &fileMeta
	return result, nil
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

type fileReadSegment struct {
	chunk metadata.ChunkInfo
	start int
	end   int
}

func overlappingFileSegments(file *metadata.FileMeta, repoChunks map[int64]metadata.ChunkInfo, offset, end int64) []fileReadSegment {
	if file == nil || end <= offset {
		return nil
	}
	segments := make([]fileReadSegment, 0, len(file.Chunks))
	chunks := make([]metadata.ChunkInfo, 0, len(file.Chunks))
	for _, id := range file.Chunks {
		if chunk, ok := repoChunks[id]; ok {
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
		segmentStart := max(offset, chunk.Offset)
		segmentEnd := min(end, chunkEnd)
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

// compensateDeleteAssets best-effort removes windows uploaded by this call
// after a later failure; metadata was never committed, so these are pure
// orphans. Individual failures are logged, not fatal - the original error
// is what matters.
func (h *StorHub) compensateDeleteAssets(ctx context.Context, project string, chunks []ChunkInfo) {
	for _, c := range chunks {
		if err := h.deleteAssetByID(ctx, project, c.AssetID); err != nil {
			h.debugf("compensating delete failed project=%s asset=%d err=%v", project, c.AssetID, err)
		}
	}
}
