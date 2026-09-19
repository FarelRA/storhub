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
	cleanName, _, err := h.resolveAuthedPath(ctx, repoMeta, fileName, true)
	if err != nil {
		return "", "", err
	}
	if err := shfs.RequireParentDirectory(repoMeta, cleanName); err != nil {
		return "", "", err
	}
	existing := repoMeta.FindFile(cleanName)
	if existing == nil {
		return "", "", fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// Release probe, not a full tree clone (audit 33): picking only reads
	// the release set + counts, so a throwaway carrying just the catalog
	// avoids an O(tree) memcpy per upload/patch.
	probe, err := newReleaseProbe(repoMeta, project, h.config.Now().UnixNano())
	if err != nil {
		return "", "", err
	}
	probe.RemoveFile(cleanName)
	releaseTag, uploadURL, err = h.getOrCreateUploadRelease(ctx, project, probe, requiredSlots)
	return releaseTag, uploadURL, err
}

// resolveAuthedPath is the single home of the 6-line validate+resolve+
// traversal preamble pasted across Prepare/Finalize/put/patch/delete/read:
// shape-check the raw path, resolve it against meta (following the final
// symlink when followFinal, e.g. open(O_CREAT|O_TRUNC) put semantics vs
// unlink(2) delete semantics), reject empties, and enforce traversal
// policy. Callers add their own parent/existence checks after.
func (h *StorHub) resolveAuthedPath(ctx context.Context, repoMeta *RepoMetadata, rawPath string, followFinal bool) (cleanName string, traversed []string, err error) {
	if err := shfs.ValidateAccessPathShape(rawPath); err != nil {
		return "", nil, err
	}
	cleanName, traversed, err = shfs.ResolveAccessPath(repoMeta, rawPath, followFinal)
	if err != nil {
		return "", nil, err
	}
	if cleanName == "" {
		return "", nil, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
		return "", nil, err
	}
	return cleanName, traversed, nil
}

// newReleaseProbe builds a minimal catalog-only tree for release picking:
// the release set is copied, files/chunks/dirs are not. getOrCreateUploadRelease
// only reads releases (+ EnsureRelease bookkeeping on the throwaway), so a
// full Clone (O(tree) memcpy of all four stored maps) per upload/patch was
// pure waste. The probe is discarded after picking; the commit path
// re-ensures the landed releases on the authoritative tree.
func newReleaseProbe(repoMeta *RepoMetadata, project string, now int64) (*RepoMetadata, error) {
	probe := NewRepoMetadata(project)
	for tag := range repoMeta.Releases() {
		if _, err := probe.EnsureRelease(tag, now); err != nil {
			return nil, err
		}
	}
	return probe, nil
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
	cleanName, _, err := h.resolveAuthedPath(ctx, repoMeta, fileName, true)
	if err != nil {
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

	now := h.config.Now().UnixNano()
	fileMeta := current.Clone()
	fileMeta.Mode = shfs.SanitizeWrittenFileModeForContext(ctx, fileMeta.Mode)

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
	if _, err := tree.EnsureRelease(releaseTag, now); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, chunks)
		return nil, err
	}
	if err := ensureChunkReleases(tree, chunks, now); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, chunks)
		return nil, err
	}
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(chunks))
	for i := range chunks {
		id := tree.AllocateChunkID()
		if err := tree.PutChunk(id, chunks[i]); err != nil {
			pm.mu.Unlock()
			h.compensateDeleteAssets(ctx, project, chunks)
			return nil, err
		}
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
	publishTreeLocked(pm, tree, []string{cleanName})
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

// uploadFileContext creates a file; replaceFileContext overwrites one.
// Both share putFileInner: the only flag branches left are the op
// name/cause strings and the exists checks.
func (h *StorHub) uploadFileContext(ctx context.Context, project, fileName, inputPath string) (result *FileMeta, err error) {
	return h.putFileInner(ctx, project, fileName, inputPath, false)
}

func (h *StorHub) replaceFileContext(ctx context.Context, project, fileName, inputPath string) (result *FileMeta, err error) {
	return h.putFileInner(ctx, project, fileName, inputPath, true)
}

func (h *StorHub) putFileInner(ctx context.Context, project, fileName, inputPath string, replace bool) (result *FileMeta, err error) {
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
	cleanName, traversed, err := h.resolveAuthedPath(ctx, repoMeta, fileName, true)
	if err != nil {
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

	workingMeta, err := newReleaseProbe(repoMeta, project, h.config.Now().UnixNano())
	if err != nil {
		return nil, err
	}
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
	implposix.ApplyUploadIdentity(cleanName, existing, &fileMeta, h.config.Now().UnixNano())
	if existing == nil {
		defaultUID, defaultGID := h.DefaultOwnerIDs()
		fileMeta.UID, fileMeta.GID = shfs.OwnerIDsForCreate(ctx, defaultUID, defaultGID)
	}
	if existing != nil {
		fileMeta.Mode = shfs.SanitizeWrittenFileModeForContext(ctx, fileMeta.Mode)
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
	if _, err := tree.EnsureRelease(releaseTag, h.config.Now().UnixNano()); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}
	// Rotation may have spread this file's chunks across releases;
	// ensuring only the initial tag would strand rotated chunks outside
	// the catalog where PurgeUntracked deletes live data.
	if err := ensureChunkReleases(tree, results, h.config.Now().UnixNano()); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, results)
		return nil, err
	}
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(results))
	for i := range results {
		id := tree.AllocateChunkID()
		if err := tree.PutChunk(id, results[i]); err != nil {
			pm.mu.Unlock()
			h.compensateDeleteAssets(ctx, project, results)
			return nil, err
		}
		chunkIDs[i] = id
	}
	fileMeta.Chunks = chunkIDs
	current := tree.FindFile(cleanName)
	if current != nil {
		implposix.ApplyUpdatedFileIdentity(cleanName, &fileMeta, current, h.config.Now().UnixNano())
		implposix.ReplaceInodeFamily(tree, cleanName, current, fileMeta, h.config.Now().UnixNano())
	} else {
		fileMeta.Mode, fileMeta.UID, fileMeta.GID = shfs.ApplyParentInheritance(tree, cleanName, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
		metadata.InitializeNewFileIdentity(tree, &fileMeta, h.config.Now().UnixNano())
		tree.UpsertFile(cleanName, fileMeta, h.config.Now().UnixNano())
	}
	shfs.TouchParentDirectory(tree, cleanName, h.config.Now().UnixNano())
	siblings := tree.FindFilesByInode(fileMeta.Inode)
	publishTreeLocked(pm, tree, []string{cleanName})
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	cause := "upload"
	if replace {
		cause = "replace"
	}
	now := h.config.Now().UnixNano()
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

	// Single-attempt closure over the open→copy→close sequence; withRetry
	// (retry.go) owns the backoff/sleep shape. Open and copy errors share
	// the isRetryableDownloadError gate, preserving the old semantics.
	attempt := func() error {
		reader, _, err := h.downloadAssetStream(ctx, project, chunk.AssetID, chunk.AssetOffset, chunk.AssetOffset+chunk.Size-1)
		if err != nil {
			return fmt.Errorf("download chunk %d: %w", chunk.AssetID, err)
		}
		written, copyErr := h.writeChunk(outFile, reader, *buf, chunk)
		if closeErr := reader.Close(); copyErr == nil {
			copyErr = closeErr
		}
		if copyErr == nil && written != chunk.Size {
			copyErr = fmt.Errorf("chunk %d size mismatch: expected %d, got %d", chunk.AssetID, chunk.Size, written)
		}
		if copyErr != nil {
			return fmt.Errorf("download chunk %d: %w", chunk.AssetID, copyErr)
		}
		return nil
	}
	// The "download chunk %d" wrapper uses %w, so errors.As/Is inside
	// isRetryableDownloadError still see the causal API/CDN/network
	// error through it.
	return h.withRetry(ctx, "download-chunk", h.config.MaxRetries+1, func(err error) bool {
		return isRetryableDownloadError(err)
	}, attempt)
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
