package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

// verbs_upload.go: transfer verbs: upload, replace, patch, download.

// UploadFileContext stores a local file as new project content.
func (h *StorHub) UploadFileContext(ctx context.Context, project, fileName, inputPath string) (result *FileMeta, err error) {
	started := h.logOpStart(project, "upload-file", "path", fileName, "input", inputPath)
	defer func() {
		size := int64(0)
		if result != nil {
			size = result.Size
		}
		h.logOpFinish(project, "upload-file", started, err, "path", fileName, "input", inputPath, "size", size)
	}()
	// Degraded-mode admission first: refuse before any upload work mints
	// assets that could never commit.
	if err := h.admitMutation(project); err != nil {
		return nil, err
	}
	result, err = h.uploadFileContext(ctx, project, fileName, inputPath)
	return result, err
}

// ReplaceFileContext swaps a stored file for new local content.
func (h *StorHub) ReplaceFileContext(ctx context.Context, project, fileName, inputPath string, opts ...shfs.MutateOption) (result *FileMeta, err error) {
	started := h.logOpStart(project, "replace-file", "path", fileName, "input", inputPath)
	defer func() {
		size := int64(0)
		if result != nil {
			size = result.Size
		}
		h.logOpFinish(project, "replace-file", started, err, "path", fileName, "input", inputPath, "size", size)
	}()
	// Degraded-mode admission first: refuse before the revision check pays
	// for a remote load.
	if err := h.admitMutation(project); err != nil {
		return nil, err
	}
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	ctx = gateRevisionFromOpts(ctx, opts)
	result, err = h.replaceFileContext(ctx, project, fileName, inputPath)
	return result, err
}

// PatchFileContext splices one edit into a stored file at offset.
func (h *StorHub) PatchFileContext(ctx context.Context, project, fileName string, offset, deleteSize int64, edit []byte, opts ...shfs.MutateOption) (*FileMeta, error) {
	var result *FileMeta
	var err error
	started := h.logOpStart(project, "patch-file", "path", fileName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	defer func() {
		h.logOpFinish(project, "patch-file", started, err, "path", fileName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	}()
	// Degraded-mode admission before the revision check's remote load.
	if err := h.admitMutation(project); err != nil {
		return nil, err
	}
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	ctx = gateRevisionFromOpts(ctx, opts)
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
	cleanName, traversed, err := shfs.StatResolveTracked(repoMeta, fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if err := shfs.CheckWalkResolved(ctx, repoMeta, traversed); err != nil {
		return nil, err
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
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	patchEnd := offset + deleteSize
	if offset > fileMeta.Size || patchEnd > fileMeta.Size {
		return nil, fmt.Errorf("patch range [%d,%d) exceeds file size %d", offset, patchEnd, fileMeta.Size)
	}

	result, err = h.patchFileWithMetadataContext(ctx, project, cleanName, repoMeta, fileMeta, offset, deleteSize, edit)
	return result, err
}

// PatchFileRangesContext applies a batch of ascending, disjoint edits as
// ONE operation: one release resolution, one asset per chunk of edited
// bytes, one playlist rebuild, one metadata mutation. Compared to looping
// PatchFileContext per range it removes the per-range release listing
// round-trip and the N-1 intermediate playlist states - on a slow link
// that turns N+2 latency chains into one. Either the whole batch commits
// or none of it does.
func (h *StorHub) PatchFileRangesContext(ctx context.Context, project, fileName string, edits []shfs.RangeEdit) (*FileMeta, error) {
	var err error
	started := h.logOpStart(project, "patch-file-ranges", "path", fileName, "edits", len(edits))
	defer func() {
		h.logOpFinish(project, "patch-file-ranges", started, err, "path", fileName, "edits", len(edits))
	}()
	// Degraded-mode admission before any chunk uploads mint assets.
	if err := h.admitMutation(project); err != nil {
		return nil, err
	}
	if err := validateProject(project); err != nil {
		return nil, err
	}
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return nil, err
	}
	if len(edits) == 0 {
		return nil, errors.New("patch batch is empty")
	}
	for i, edit := range edits {
		if edit.Start < 0 || edit.DeleteSize < 0 {
			return nil, fmt.Errorf("patch edit %d has negative range", i)
		}
		if edit.DeleteSize == 0 && edit.Len() == 0 {
			return nil, fmt.Errorf("patch edit %d is empty", i)
		}
		if i > 0 && edit.Start < edits[i-1].End() {
			return nil, fmt.Errorf("patch edit %d overlaps its predecessor", i)
		}
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanName, traversed, err := shfs.StatResolveTracked(repoMeta, fileName)
	if err != nil {
		return nil, err
	}
	if cleanName == "" {
		return nil, errors.New("file name is required")
	}
	if err := shfs.CheckWalkResolved(ctx, repoMeta, traversed); err != nil {
		return nil, err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	for i, edit := range edits {
		if edit.End() > fileMeta.Size {
			return nil, fmt.Errorf("patch edit %d range [%d,%d) exceeds file size %d", i, edit.Start, edit.End(), fileMeta.Size)
		}
	}

	newChunks, uploaded, releaseTag, err := h.buildPatchedRangeChunks(ctx, project, repoMeta, *fileMeta, cleanName, edits)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().UnixNano()
	patched := fileMeta.Clone()

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, uploaded)
		return nil, err
	}
	// Mutations apply to a private COW copy; publish only on success.
	// A patch shrinking to empty (or a no-op edit) mints no chunks and
	// picks no release: the helper skips registration for the empty tag
	// (EnsureRelease rejects it).
	tree := cowTree(pm.meta)
	chunkIDs, allocErr := allocateChunkRecords(tree, newChunks, releaseTag, now)
	if allocErr != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, uploaded)
		return nil, allocErr
	}
	patched.Chunks = chunkIDs
	totalDelete, totalInsert := int64(0), int64(0)
	for _, edit := range edits {
		totalDelete += edit.DeleteSize
		totalInsert += edit.Len()
	}
	patched.Size = fileMeta.Size - totalDelete + totalInsert
	patched.Mode = shfs.SanitizeWrittenFileModeForContext(ctx, patched.Mode)
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := tree.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, uploaded)
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// Same concurrency guard as the single-edit path: the snapshot may be
	// stale by the time uploads finish; one size check covers the batch.
	if current.Size != fileMeta.Size || edits[len(edits)-1].End() > current.Size {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, uploaded)
		return nil, fmt.Errorf("file %s changed concurrently (size %d, expected %d); patch batch rejected", cleanName, current.Size, fileMeta.Size)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &patched, current, now)
	implposix.ReplaceInodeFamily(tree, cleanName, current, patched, now)
	siblings := tree.FindFilesByInode(patched.Inode)
	publishTreeLocked(pm, tree, []string{cleanName})
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	opFile := patched.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPatch, Paths: []string{cleanName}, Cause: "patch-ranges",
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, patched.Inode, cleanName, "patch-ranges-family", now)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	return &patched, nil
}

func (h *StorHub) patchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *RepoMetadata, fileMeta *FileMeta, offset, deleteSize int64, edit []byte) (*FileMeta, error) {
	newChunks, fresh, releaseTag, err := h.buildPatchedChunksFresh(ctx, project, repoMeta, *fileMeta, cleanName, offset, deleteSize, edit)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().UnixNano()
	patched := fileMeta.Clone()

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, fresh)
		return nil, err
	}

	// Register the release holding the new chunks so purge cannot
	// delete live data. buildPatchedChunksFresh EnsureReleases only on a local
	// clone that is discarded here. Mutations apply to a private COW copy;
	// publish only on success. A patch shrinking to empty mints no chunks
	// and picks no release: the helper skips registration for the empty tag
	// (EnsureRelease rejects it).
	tree := cowTree(pm.meta)
	chunkIDs, allocErr := allocateChunkRecords(tree, newChunks, releaseTag, now)
	if allocErr != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, fresh)
		return nil, allocErr
	}
	patched.Chunks = chunkIDs
	patched.Size = fileMeta.Size - deleteSize + int64(len(edit))
	patched.Mode = shfs.SanitizeWrittenFileModeForContext(ctx, patched.Mode)
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := tree.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, fresh)
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// The pre-network validation ran against a snapshot; the file may have
	// changed since. Re-check the edited range against current state before
	// committing, so a concurrent truncate/replace cannot be clobbered.
	if current.Size != fileMeta.Size || offset+deleteSize > current.Size {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, fresh)
		return nil, fmt.Errorf("file %s changed concurrently (size %d, expected %d); patch rejected", cleanName, current.Size, fileMeta.Size)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &patched, current, now)
	implposix.ReplaceInodeFamily(tree, cleanName, current, patched, now)
	siblings := tree.FindFilesByInode(patched.Inode)
	publishTreeLocked(pm, tree, []string{cleanName})
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	opFile := patched.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPatch, Paths: []string{cleanName}, Cause: "patch",
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, patched.Inode, cleanName, "patch-family", now)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	return &patched, nil
}

func (h *StorHub) rewriteFileRangesWithMetadataContext(ctx context.Context, project, cleanName, snapshotPath string, repoMeta *RepoMetadata, fileMeta *FileMeta, finalSize int64, dirtyRanges []byteRange) (*FileMeta, error) {
	newChunks, uploaded, releaseTag, err := h.buildRewrittenChunks(ctx, project, repoMeta, *fileMeta, cleanName, snapshotPath, finalSize, dirtyRanges)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().UnixNano()
	rewritten := fileMeta.Clone()

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, uploaded)
		return nil, err
	}

	// Register the release holding the new chunks so purge cannot
	// delete live data. buildRewrittenChunks EnsureReleases only on a local
	// clone that is discarded here. Mutations apply to a private COW copy;
	// publish only on success. A rewrite shrinking to empty mints no chunks
	// and picks no release: the helper skips registration for the empty tag
	// (EnsureRelease rejects it).
	tree := cowTree(pm.meta)
	chunkIDs, allocErr := allocateChunkRecords(tree, newChunks, releaseTag, now)
	if allocErr != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, uploaded)
		return nil, allocErr
	}
	rewritten.Chunks = chunkIDs
	rewritten.Size = finalSize
	rewritten.Mode = shfs.SanitizeWrittenFileModeForContext(ctx, rewritten.Mode)
	rewritten.ModifiedAt = now
	rewritten.ChangedAt = now
	rewritten.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := tree.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, uploaded)
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &rewritten, current, now)
	implposix.ReplaceInodeFamily(tree, cleanName, current, rewritten, now)
	siblings := tree.FindFilesByInode(rewritten.Inode)
	publishTreeLocked(pm, tree, []string{cleanName})
	trigger := h.markProjectDirtyLiveLocked(project, pm)
	opFile := rewritten.Clone()
	h.appendOpLocked(project, pm, Op{
		Type: OpPutFile, Paths: []string{cleanName}, Cause: "rewrite-ranges",
		Timestamp: now,
		File:      &opFile,
		Chunks:    chunkRecordsFor(tree, chunkIDs),
	})
	h.emitFamilySiblingsLocked(project, pm, tree, siblings, rewritten.Inode, cleanName, "rewrite-ranges-family", now)
	pm.mu.Unlock()

	select {
	case trigger <- struct{}{}:
	default:
	}

	return &rewritten, nil
}

// DownloadFileContext reassembles a stored file to a local path.
func (h *StorHub) DownloadFileContext(ctx context.Context, project, fileName, outputPath string) error {
	var err error
	started := h.logOpStart(project, "download-file", "path", fileName, "output", outputPath)
	defer func() { h.logOpFinish(project, "download-file", started, err, "path", fileName, "output", outputPath) }()
	if err := validateProject(project); err != nil {
		return err
	}
	if err := shfs.ValidateAccessPathShape(fileName); err != nil {
		return err
	}

	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return err
	}
	cleanName, traversed, err := shfs.StatResolveTracked(repoMeta, fileName)
	if err != nil {
		return err
	}
	if cleanName == "" {
		return errors.New("file name is required")
	}
	if err := shfs.CheckWalkResolved(ctx, repoMeta, traversed); err != nil {
		return err
	}
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer func() {
		cerr := outFile.Close()
		if err == nil && cerr != nil {
			err = fmt.Errorf("close output file: %w", cerr)
		}
		if err != nil {
			_ = os.Remove(outputPath)
		}
	}()

	if err := outFile.Truncate(fileMeta.Size); err != nil {
		return fmt.Errorf("preallocate output file: %w", err)
	}

	for _, chunkName := range fileMeta.Chunks {
		chunk, ok := repoMeta.Chunks()[chunkName]
		if !ok {
			return fmt.Errorf("chunk %d not found", chunkName)
		}
		if err := h.downloadChunkWithRetry(ctx, project, outFile, chunk); err != nil {
			return err
		}
	}

	if err := outFile.Sync(); err != nil {
		return fmt.Errorf("sync output file: %w", err)
	}
	// The deferred closer owns Close so it fires exactly once, whether the
	// success path completes here or an error unwinds through the defers.
	return err
}

// RewriteFileRangesWithMetadataContext rewrites dirty ranges against loaded metadata.
func (h *StorHub) RewriteFileRangesWithMetadataContext(ctx context.Context, project, cleanName, snapshotPath string, repoMeta *metadata.RepoMetadata, fileMeta *metadata.FileMeta, finalSize int64, dirtyRanges []fusefs.ByteRange) (result *metadata.FileMeta, err error) {
	started := h.logOpStart(project, "rewrite-file-ranges", "path", cleanName, "size", finalSize, "ranges", len(dirtyRanges))
	defer func() {
		resultSize := int64(0)
		if result != nil {
			resultSize = result.Size
		}
		h.logOpFinish(project, "rewrite-file-ranges", started, err, "path", cleanName, "size", finalSize, "ranges", len(dirtyRanges), "result_size", resultSize)
	}()
	// Degraded-mode admission: a rewrite mints chunks like any mutation.
	if err := h.admitMutation(project); err != nil {
		return nil, err
	}
	ranges := make([]byteRange, len(dirtyRanges))
	for i, dirty := range dirtyRanges {
		ranges[i] = byteRange{start: dirty.Start, end: dirty.End}
	}
	result, err = h.rewriteFileRangesWithMetadataContext(ctx, project, cleanName, snapshotPath, repoMeta, fileMeta, finalSize, ranges)
	return result, err
}

// GetOrCreateUploadReleaseContext returns a release with room for requiredSize bytes.
func (h *StorHub) GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *metadata.RepoMetadata, requiredSize int) (string, string, error) {
	return h.getOrCreateUploadRelease(ctx, project, repoMeta, requiredSize)
}

// PatchFileWithMetadataContext splices one edit using caller-loaded metadata.
func (h *StorHub) PatchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *metadata.RepoMetadata, fileMeta *metadata.FileMeta, offset, deleteSize int64, edit []byte) (result *metadata.FileMeta, err error) {
	started := h.logOpStart(project, "patch-file-with-metadata", "path", cleanName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	defer func() {
		resultSize := int64(0)
		if result != nil {
			resultSize = result.Size
		}
		h.logOpFinish(project, "patch-file-with-metadata", started, err, "path", cleanName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit), "result_size", resultSize)
	}()
	result, err = h.patchFileWithMetadataContext(ctx, project, cleanName, repoMeta, fileMeta, offset, deleteSize, edit)
	return result, err
}

// FillAssetRangeContext downloads one chunk segment into dst.
func (h *StorHub) FillAssetRangeContext(ctx context.Context, project string, segment metadata.ChunkInfo, dst []byte) error {
	return h.fillAssetRange(ctx, project, segment, dst)
}
