package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

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

	// Collect every failure: stopping at the first error would leave other
	// projects' dirty metadata unflushed with no diagnostic.
	var errs []error
	for _, p := range projects {
		if err := h.commitProjectMetadata(ctx, p.name, p.meta); err != nil {
			// FlushMetadata gets the same conflict recovery as the
			// commit loop: a 409 reloads remote HEAD into the cache so a
			// stale flush converges instead of staying stale. The error
			// is still reported to the caller.
			h.recoverMetadataCommitFailure(p.name, err)
			errs = append(errs, fmt.Errorf("flush %s: %w", p.name, err))
		}
	}
	return errors.Join(errs...)
}

// FlushProjectContext commits dirty metadata for one project, creating
// the tracking entry if absent (an unknown project name therefore starts
// residency with an empty tree and reports success without network
// traffic). It is the per-project counterpart of FlushMetadata and the
// remedy after a failed push: healing requires a later operation on that
// project, this call, or Shutdown.
func (h *StorHub) FlushProjectContext(ctx context.Context, project string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	return h.commitProjectMetadata(ctx, project, h.getOrCreateProjectMeta(project))
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

func (h *StorHub) ReplaceFileContext(ctx context.Context, project, fileName, inputPath string, opts ...shfs.MutateOption) (*FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.putFileContext(ctx, project, fileName, inputPath, true)
}

func (h *StorHub) PatchFile(project, fileName string, offset, deleteSize int64, edit []byte) (*FileMeta, error) {
	return h.PatchFileContext(context.Background(), project, fileName, offset, deleteSize, edit)
}

func (h *StorHub) PatchFileContext(ctx context.Context, project, fileName string, offset, deleteSize int64, edit []byte, opts ...shfs.MutateOption) (result *FileMeta, err error) {
	started := h.logOpStart(project, "patch-file", "path", fileName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	defer func() {
		h.logOpFinish(project, "patch-file", started, err, "path", fileName, "offset", offset, "delete_size", deleteSize, "edit_bytes", len(edit))
	}()
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
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

// PatchFileRangesWithMetadataContext applies a batch of ascending,
// disjoint edits as ONE operation: one release resolution, one asset per
// chunk of edited bytes, one playlist rebuild, one metadata mutation.
// Compared to looping PatchFileContext per range it removes the per-range
// release listing round-trip and the N-1 intermediate playlist states -
// on a slow link that turns N+2 latency chains into one. The caller owns
// the pre-network validation (range bounds, sort order); this layer
// re-validates defensively and re-checks concurrent size changes once
// before committing.
// PatchFileRangesContext applies a batch of ascending, disjoint edits as
// ONE operation: one release resolution, one asset per chunk of edited
// bytes, one playlist rebuild, one metadata mutation. Compared to looping
// PatchFileContext per range it removes the per-range release listing
// round-trip and the N-1 intermediate playlist states - on a slow link
// that turns N+2 latency chains into one. Either the whole batch commits
// or none of it does.
func (h *StorHub) PatchFileRangesContext(ctx context.Context, project, fileName string, edits []shfs.RangeEdit) (result *FileMeta, err error) {
	started := h.logOpStart(project, "patch-file-ranges", "path", fileName, "edits", len(edits))
	defer func() {
		h.logOpFinish(project, "patch-file-ranges", started, err, "path", fileName, "edits", len(edits))
	}()
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
	fileMeta := repoMeta.FindFile(cleanName)
	if fileMeta == nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	for i, edit := range edits {
		if edit.End() > fileMeta.Size {
			return nil, fmt.Errorf("patch edit %d range [%d,%d) exceeds file size %d", i, edit.Start, edit.End(), fileMeta.Size)
		}
	}

	newChunks, releaseTag, err := h.buildPatchedRangeChunks(ctx, project, repoMeta, *fileMeta, cleanName, edits)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().Unix()
	patched := fileMeta.Clone()

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, newChunks)
		return nil, err
	}
	// Mutations apply to a private COW copy; publish only on success.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, now)
	ensureChunkReleases(tree, newChunks, now)
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, newChunks[i])
		chunkIDs[i] = id
	}
	patched.Chunks = chunkIDs
	totalDelete, totalInsert := int64(0), int64(0)
	for _, edit := range edits {
		totalDelete += edit.DeleteSize
		totalInsert += edit.Len()
	}
	patched.Size = fileMeta.Size - totalDelete + totalInsert
	patched.Mode = shfs.SanitizeWrittenFileMode(patched.Mode)
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := tree.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// Same concurrency guard as the single-edit path: the snapshot may be
	// stale by the time uploads finish; one size check covers the batch.
	if current.Size != fileMeta.Size || edits[len(edits)-1].End() > current.Size {
		pm.mu.Unlock()
		return nil, fmt.Errorf("file %s changed concurrently (size %d, expected %d); patch batch rejected", cleanName, current.Size, fileMeta.Size)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &patched, current, now)
	implposix.ReplaceInodeFamily(tree, cleanName, current, patched, now)
	siblings := tree.FindFilesByInode(patched.Inode)
	publishTreeLocked(pm, tree)
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
	newChunks, releaseTag, err := h.buildPatchedChunks(ctx, project, repoMeta, *fileMeta, cleanName, offset, deleteSize, edit)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().Unix()
	patched := fileMeta.Clone()

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, newChunks)
		return nil, err
	}

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. buildPatchedChunks EnsureReleases only on a local
	// clone that is discarded here. Mutations apply to a private COW copy;
	// publish only on success.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, now)
	ensureChunkReleases(tree, newChunks, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, newChunks[i])
		chunkIDs[i] = id
	}
	patched.Chunks = chunkIDs
	patched.Size = fileMeta.Size - deleteSize + int64(len(edit))
	patched.Mode = shfs.SanitizeWrittenFileMode(patched.Mode)
	patched.ModifiedAt = now
	patched.ChangedAt = now
	patched.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := tree.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	// The pre-network validation ran against a snapshot; the file may have
	// changed since. Re-check the edited range against current state before
	// committing, so a concurrent truncate/replace cannot be clobbered.
	if current.Size != fileMeta.Size || offset+deleteSize > current.Size {
		pm.mu.Unlock()
		return nil, fmt.Errorf("file %s changed concurrently (size %d, expected %d); patch rejected", cleanName, current.Size, fileMeta.Size)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &patched, current, now)
	implposix.ReplaceInodeFamily(tree, cleanName, current, patched, now)
	siblings := tree.FindFilesByInode(patched.Inode)
	publishTreeLocked(pm, tree)
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
	newChunks, releaseTag, err := h.buildRewrittenChunks(ctx, project, repoMeta, *fileMeta, cleanName, snapshotPath, finalSize, dirtyRanges)
	if err != nil {
		return nil, err
	}
	now := h.config.Now().Unix()
	rewritten := fileMeta.Clone()

	// Update metadata directly
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	if err := h.ensureMutableLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		h.compensateDeleteAssets(ctx, project, newChunks)
		return nil, err
	}

	// Register the release holding the new chunks so PurgeUntracked cannot
	// delete live data. buildRewrittenChunks EnsureReleases only on a local
	// clone that is discarded here. Mutations apply to a private COW copy;
	// publish only on success.
	tree := cowTree(pm.meta)
	tree.EnsureRelease(releaseTag, now)
	ensureChunkReleases(tree, newChunks, now)
	// Allocate identifiers against the authoritative in-memory metadata so
	// concurrent operations can never mint colliding chunk IDs.
	chunkIDs := make([]int64, len(newChunks))
	for i := range newChunks {
		id := tree.AllocateChunkID()
		tree.PutChunk(id, newChunks[i])
		chunkIDs[i] = id
	}
	rewritten.Chunks = chunkIDs
	rewritten.Size = finalSize
	rewritten.Mode = shfs.SanitizeWrittenFileMode(rewritten.Mode)
	rewritten.ModifiedAt = now
	rewritten.ChangedAt = now
	rewritten.AccessedAt = implposix.ChooseNonZeroTime(fileMeta.AccessedAt, now)
	current := tree.FindFile(cleanName)
	if current == nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	}
	implposix.ApplyUpdatedFileIdentity(cleanName, &rewritten, current, now)
	implposix.ReplaceInodeFamily(tree, cleanName, current, rewritten, now)
	siblings := tree.FindFilesByInode(rewritten.Inode)
	publishTreeLocked(pm, tree)
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

func (h *StorHub) DownloadFile(project, fileName, outputPath string) error {
	return h.DownloadFileContext(context.Background(), project, fileName, outputPath)
}

func (h *StorHub) DownloadFileContext(ctx context.Context, project, fileName, outputPath string) (err error) {
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
	cleanName, traversed, err := shfs.ResolveAccessPath(repoMeta, fileName, true)
	if err != nil {
		return err
	}
	if cleanName == "" {
		return errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, traversed); err != nil {
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
	result = make([]metadata.ReleaseRef, 0, len(repoMeta.Releases()))
	for _, ref := range repoMeta.Releases() {
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
	// A branch name is not a revision. The contents API resolves
	// unknown refs to HEAD content, so passing 'main' would silently
	// roll back to HEAD (a no-op that reports success). Only a commit
	// SHA from this file's own revision history is accepted.
	if strings.ContainsAny(commitSHA, "/ 	\n") {
		return fmt.Errorf("invalid metadata revision %q: not a commit SHA", commitSHA)
	}
	if err := h.validateMetadataRevision(ctx, project, commitSHA); err != nil {
		return err
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
	// Git-path CAS pin: cached/fresh loads carry "" as the version token,
	// which would make the write-time compare vacuous and let this rollback
	// silently overwrite a concurrent writer. Re-sync and pin the real HEAD
	// commit, so commitRepoMetadata aborts with 409 when HEAD moved. The
	// fresh load returns the HEAD token paired atomically with the index
	// content it read; re-reading headCommitSHA separately could pair
	// content at commit N with token N+1.
	if !h.config.DisableGitBackend {
		if fresh, freshSHA, freshErr := h.loadRepoMetadataFresh(ctx, project); freshErr == nil && freshSHA != "" {
			currentMeta = fresh
			if err := currentMeta.Validate(); err != nil {
				return err
			}
			currentSHA = freshSHA
		}
	}
	rollbackMeta, err := h.getMetadataRevision(ctx, project, commitSHA)
	if err != nil {
		return err
	}
	if err := h.validateMetadataSnapshot(ctx, project, rollbackMeta); err != nil {
		return err
	}
	// The snapshot was validated against a listing taken moments ago;
	// assets can be deleted between that check and this commit. Re-check
	// immediately before committing to narrow the race window.
	if err := h.validateMetadataSnapshot(ctx, project, rollbackMeta); err != nil {
		return fmt.Errorf("rollback snapshot changed before commit: %w", err)
	}
	_, _, err = h.commitRepoMetadata(ctx, project, rollbackMeta, currentSHA, fmt.Sprintf("storhub: rollback metadata to %s", shortSHA(commitSHA)))
	if err != nil {
		return err
	}
	// The delete can also land mid-commit (after the re-check above).
	// Verify the committed snapshot against fresh server state and fail
	// loudly instead of blessing bytes that can no longer be downloaded.
	if err := h.validateMetadataSnapshot(ctx, project, rollbackMeta); err != nil {
		return fmt.Errorf("rollback committed but snapshot no longer validates: %w", err)
	}
	return nil
}

// RevertPath restores a single path (a file or an entire directory subtree) to
// its state at commitSHA, leaving every other path untouched, as a NEW commit.
func (h *StorHub) RevertPath(project, path, commitSHA string) error {
	return h.RevertPathContext(context.Background(), project, path, commitSHA)
}

// RevertPathContext is the per-path counterpart of RollbackMetadataContext:
// instead of repointing the whole index at an old revision, it replays just
// `path`'s historical state onto the current tree. It is a revert, not a
// force-push: history is preserved and the result flows through the normal
// transaction path (op synthesis, journal, rebase, commit). The reverted
// subtree's assets are validated against live releases before and after the
// commit, so restoring a path whose bytes were purged fails loudly rather
// than committing a dangling reference.
func (h *StorHub) RevertPathContext(ctx context.Context, project, path, commitSHA string) (err error) {
	started := h.logOpStart(project, "revert-path", "path", path, "commit_sha", commitSHA)
	defer func() { h.logOpFinish(project, "revert-path", started, err, "path", path, "commit_sha", commitSHA) }()
	if err := validateProject(project); err != nil {
		return err
	}
	if err := shfs.ValidateAccessPathShape(path); err != nil {
		return err
	}
	if strings.TrimSpace(commitSHA) == "" {
		return errors.New("commit sha is required")
	}
	// A branch name is not a revision (the contents API resolves unknown
	// refs to HEAD, which would silently "revert" to current).
	if strings.ContainsAny(commitSHA, "/ \t\n") {
		return fmt.Errorf("invalid metadata revision %q: not a commit SHA", commitSHA)
	}
	if err := h.validateMetadataRevision(ctx, project, commitSHA); err != nil {
		return err
	}
	// Flush pending mutations so the revert is built on committed truth.
	pm := h.getOrCreateProjectMeta(project)
	if err := h.commitProjectMetadata(ctx, project, pm); err != nil {
		return err
	}
	historical, err := h.getMetadataRevision(ctx, project, commitSHA)
	if err != nil {
		return err
	}
	// Validate the would-be result before committing: the reverted subtree's
	// chunks must resolve to releases/assets that still exist.
	current, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return err
	}
	// A revert addresses the node the path names, so the final symlink is
	// followed; the historical tree is keyed by concrete paths, hence the
	// resolved key (not the raw spelling) is what RevertSubtree replays.
	cleanPath, _, err := shfs.ResolveAccessPath(current, path, true)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		return errors.New("revert requires a non-root path")
	}
	preview := current.Clone()
	if err := metadata.RevertSubtree(preview, historical, cleanPath, h.config.Now().Unix()); err != nil {
		return err
	}
	preview.Normalize(project, h.config.Now().Unix())
	if err := h.validateMetadataSnapshot(ctx, project, preview); err != nil {
		return fmt.Errorf("revert %s: %w", cleanPath, err)
	}
	message := fmt.Sprintf("storhub: revert %s to %s", cleanPath, shortSHA(commitSHA))
	if _, err := h.UpdateRepoMetadataContext(ctx, project, func(m *metadata.RepoMetadata) error {
		return metadata.RevertSubtree(m, historical, cleanPath, h.config.Now().Unix())
	}, message); err != nil {
		return err
	}
	// Commit synchronously: a revert is a discrete operation the caller
	// expects to be durable on return, not left to the async flush loop.
	if err := h.commitProjectMetadata(ctx, project, h.getOrCreateProjectMeta(project)); err != nil {
		return err
	}
	// Assets can be deleted between the pre-check and the commit; re-check
	// the committed state against fresh server truth.
	committed, _, err := h.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		return err
	}
	if err := h.validateMetadataSnapshot(ctx, project, committed); err != nil {
		return fmt.Errorf("revert committed but snapshot no longer validates: %w", err)
	}
	return nil
}

// validateMetadataRevision ensures revision is a known commit SHA of the
// project's metadata history, never a branch or tag name.
func (h *StorHub) validateMetadataRevision(ctx context.Context, project, revision string) error {
	revisions, err := h.listMetadataRevisions(ctx, project)
	if err != nil {
		return err
	}
	for _, rev := range revisions {
		if rev.CommitSHA == revision {
			return nil
		}
	}
	return fmt.Errorf("invalid metadata revision %q: not a known commit SHA for project %s", revision, project)
}

func (h *StorHub) LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*metadata.RepoMetadata, string, error) {
	return h.loadRepoMetadataReadonly(ctx, project)
}

// UpdateRepoMetadataContext applies fn as a transaction against the project's
// metadata. The mutation is applied to a private copy-on-write tree under the
// exclusive lock and marked dirty for event-driven commit; the shared tree is
// swapped in only after every fallible step (apply, normalize, admission) has
// succeeded, so a rejected mutation leaves shared state, the dirty flag, and
// the op stack exactly as they were (rollback is free: the copy is discarded).
func (h *StorHub) UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*metadata.RepoMetadata) error, message string) (*metadata.RepoMetadata, error) {
	pm, err := h.getOrCreateProjectMetaAdmitted(project)
	if err != nil {
		return nil, err
	}
	lockStarted := h.config.Now().UTC()

	logging.Debug(h.projectLogger(project), "metadata writer wait", "message", message)
	pm.mu.Lock()

	logging.Debug(h.projectLogger(project), "metadata writer acquired", "message", message, "wait", h.config.Now().UTC().Sub(lockStarted))

	started := h.config.Now().UTC()
	h.debugf("metadata update start project=%s message=%q", project, message)

	// Hydration guard: a freshly created projectMetadata starts EMPTY. If
	// the project exists remotely, applying a mutation to that empty tree
	// and committing would replace the entire remote state (files, dirs,
	// chunk catalog) with just this one change. Load remote truth first;
	// only a confirmed-new project may proceed on an empty tree.
	if !pm.hydrated {
		pm.mu.Unlock()
		loaded, loadedSHA, loadErr := h.loadRepoMetadataFresh(ctx, project)
		pm.mu.Lock()
		switch {
		case loadErr == nil:
			if !pm.hydrated && !pm.dirty {
				pm.meta = loaded
				pm.sha = loadedSHA
			}
			pm.hydrated = true
		case errors.Is(loadErr, shfs.ErrNotFound):
			// Confirmed-new project: empty tree is the truth.
			pm.hydrated = true
		default:
			pm.mu.Unlock()
			return nil, fmt.Errorf("hydrate metadata before mutation: %w", loadErr)
		}
	}

	// 8MB ceiling — fail fast, never accept-then-never-commit. Apply the
	// mutation to a throwaway COW copy and measure the result before
	// touching shared state. An oversize growth is rejected at admission
	// with a remediation pointer; shared state, dirty, and usability stay
	// exactly as they were. Shrinks stay open: a mutation that reduces the
	// tree is admitted even while oversize, so the project can always fold
	// back under the ceiling.
	//
	// The blob ceiling is a legacy constraint: a split project has no
	// single-blob size limit (admission is per-object at commit), so
	// measuring the blob serialization must not reject growth on it.
	candidate := cowTree(pm.meta)
	// beforeSize is the pre-transaction serialized size, measured on the
	// private copy (the engine's incremental SerializedSize, not a whole-tree
	// ToJSON marshal). It is captured before fn so the shrink test below has
	// a baseline without ever touching the shared tree's derived state.
	beforeSize, err := candidate.SerializedSize()
	if err != nil {
		pm.mu.Unlock()
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, fmt.Errorf("size metadata: %w", err)
	}
	if err := fn(candidate); err != nil {
		pm.mu.Unlock()
		h.debugf("metadata update failed project=%s step=apply elapsed=%s err=%v", project, h.config.Now().UTC().Sub(started), err)
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, err
	}
	// Op synthesis is deferred until after admission passes: appending
	// ops before a rejection would leave the rejected mutation's ops in the
	// shared stack and the journal, where a later rebase or crash replay
	// resurrects work that was never acknowledged.
	cause := causeFromMessage(message)
	admitNow := h.config.Now().Unix()
	candidate.Normalize(project, admitNow)
	candidate.RecomputeStats()
	// Admission, expressed for the split layout (version 5). The whole-tree
	// serialized size is a cheap upper bound (incremental counter, no
	// allocation-heavy encode): if the entire tree serializes under the
	// contents-API limit, every object (a strict subset) does too, so the
	// mutation is admitted without building the tree. Only when the size
	// breaches the limit do we pay for a BuildTree to find whether a SINGLE
	// object (one enormous directory) is the culprit; a tree that merely
	// exceeds the old blob ceiling but splits into small objects is admitted,
	// because the split removed that ceiling. Shrinks always stay open.
	afterSize, err := candidate.SerializedSize()
	if err != nil {
		pm.mu.Unlock()
		h.debugf("metadata update failed project=%s step=size elapsed=%s err=%v", project, h.config.Now().UTC().Sub(started), err)
		logging.Error(h.projectLogger(project), "metadata update failed", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, fmt.Errorf("size metadata: %w", err)
	}
	if afterSize > maxMetadataBytes {
		shrinking := afterSize < beforeSize
		// Fail-fast: once the ceiling is armed, a growth mutation
		// can never commit - reject it here instead of paying the full
		// BuildTree + publish cycle on every trigger. Shrinks stay open so
		// the project can always fold back under the ceiling.
		if pm.sizeCapped && !shrinking {
			pm.mu.Unlock()
			h.debugf("metadata update rejected project=%s step=admission-capped bytes=%d elapsed=%s", project, afterSize, h.config.Now().UTC().Sub(started))
			logging.Error(h.projectLogger(project), "metadata update rejected: project is over the size ceiling; growth mutations fail fast", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "bytes", afterSize, "max", maxMetadataBytes)
			return nil, fmt.Errorf("metadata over size ceiling (%d bytes, max %d): growth is rejected until the tree fits again; delete entries or run `storhub prune`", afterSize, maxMetadataBytes)
		}
		oversizeObject := false
		if !shrinking {
			if res, berr := metadata.BuildTree(candidate); berr == nil {
				for _, obj := range res.Objects {
					if len(obj) > maxMetadataBytes {
						oversizeObject = true
						break
					}
				}
			}
		}
		if oversizeObject {
			pm.sizeCapped = true
			pm.mu.Unlock()
			h.debugf("metadata update rejected project=%s step=admission bytes=%d elapsed=%s", project, afterSize, h.config.Now().UTC().Sub(started))
			logging.Error(h.projectLogger(project), "metadata update rejected: single index object over ceiling", "message", message, "elapsed", h.config.Now().UTC().Sub(started), "bytes", afterSize, "max", maxMetadataBytes)
			return nil, fmt.Errorf("metadata too large: one directory serializes past %d bytes; distribute entries across subdirectories or run PurgeUntracked to shrink", maxMetadataBytes)
		}
		// The tree exceeds the old blob ceiling but splits into small
		// objects: admitted under the split layout.
		pm.sizeCapped = false
	} else {
		pm.sizeCapped = false
	}
	// Op synthesis: diff the pre-transaction tree against the (normalized,
	// admitted) candidate so every transaction-level mutation (fs/posix
	// ops, prune, release catalog changes) lands in the op stack - rich
	// commit messages, the crash-recovery journal, and rebase all read
	// from it. Runs only after admission: a rejected mutation leaves the
	// shared stack and journal untouched.
	//
	// NEEDS-INTEGRATION(11): intent-based op synthesis (the caller declaring
	// what it changed) would replace this full before/after diff; until then
	// the diff is the only way a generic fn's effect is captured.
	for _, op := range synthesizeOpsFromDiff(pm.meta, candidate, cause, h.config.Now().Unix()) {
		h.appendOpLocked(project, pm, op)
	}
	// Publish: candidate was normalized (indexes rebuilt) and is private, so
	// the swap is the atomic publish point. No second rebuild needed.
	pm.meta = candidate

	trigger := h.markProjectDirtyLiveLocked(project, pm)
	pm.mu.Unlock()

	// Trigger the commit loop to wake up immediately
	select {
	case trigger <- struct{}{}:
	default:
	}

	h.debugf("metadata update complete project=%s elapsed=%s", project, h.config.Now().UTC().Sub(started))
	logging.Debug(h.projectLogger(project), "metadata update complete", "message", message, "elapsed", h.config.Now().UTC().Sub(started))

	// Return a snapshot Clone, never the live pointer: callers mutating the
	// result must not corrupt the hub's in-memory truth (or the pending
	// batch) behind pm.mu's back. Pinned by TestUpdateRepoMetadataReturnsClone.
	pm.mu.RLock()
	out := pm.meta.Clone()
	pm.mu.RUnlock()
	out.RebuildIndexes()
	return out, nil
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

func (h *StorHub) GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *metadata.RepoMetadata, requiredSize int) (string, string, error) {
	return h.getOrCreateUploadRelease(ctx, project, repoMeta, requiredSize)
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

func (h *StorHub) RmdirContext(ctx context.Context, project, dirPath string, opts ...shfs.MutateOption) error {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return err
	}
	return h.fsService().RmdirContext(ctx, project, dirPath)
}

func (h *StorHub) Rename(project, oldPath, newPath string) error {
	return h.RenameContext(context.Background(), project, oldPath, newPath)
}

func (h *StorHub) RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...shfs.MutateOption) error {
	return h.fsService().RenameContext(ctx, project, oldPath, newPath, opts...)
}

func (h *StorHub) Copy(project, srcPath, dstPath string) error {
	return h.CopyContext(context.Background(), project, srcPath, dstPath)
}

func (h *StorHub) CopyContext(ctx context.Context, project, srcPath, dstPath string) error {
	return h.fsService().CopyContext(ctx, project, srcPath, dstPath)
}

func (h *StorHub) TruncateFile(project, filePath string, size int64) (*metadata.FileMeta, error) {
	return h.TruncateFileContext(context.Background(), project, filePath, size)
}

func (h *StorHub) TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.fsService().TruncateFileContext(ctx, project, filePath, size)
}

func (h *StorHub) AppendFile(project, filePath string, data []byte) (*metadata.FileMeta, error) {
	return h.AppendFileContext(context.Background(), project, filePath, data)
}

func (h *StorHub) AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.fsService().AppendFileContext(ctx, project, filePath, data)
}

func (h *StorHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*metadata.FileMeta, error) {
	return h.WriteFileAtContext(context.Background(), project, filePath, offset, data)
}

func (h *StorHub) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
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
	if err := shfs.ValidateAccessPathShape(filePath); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, errors.New("read offset and length must be non-negative")
	}
	repo, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return 0, err
	}
	cleanPath, traversed, err := shfs.ResolveAccessPath(repo, filePath, true)
	if err != nil {
		return 0, err
	}
	if cleanPath == "" {
		return 0, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repo, traversed); err != nil {
		return 0, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return 0, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanPath)
	}
	if err := shfs.CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return 0, err
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
	segments := overlappingFileSegments(file, repo.Chunks(), offset, end)
	for _, segment := range segments {
		if err := h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end]); err != nil {
			return 0, err
		}
	}
	shfs.TouchFileAccessTime(ctx, h, project, cleanPath, h.config.Now().Unix())
	return int(end - offset), nil
}

// ReadPinnedFileContext reads bytes for a caller-held metadata snapshot -
// a file entry plus the chunk descriptors it referenced when captured,
// typically at FUSE open time. Later renames or unlinks of the source
// path cannot change what this returns: resolution never touches live
// metadata, and chunk assets are content-addressed. Access was already
// authorized when the snapshot was taken, so no permission re-check
// runs here, and atime is left untouched because a historical read must
// not refresh the live entry.
func (h *StorHub) ReadPinnedFileContext(ctx context.Context, project string, file *metadata.FileMeta, chunks map[int64]metadata.ChunkInfo, offset, length int64) ([]byte, error) {
	if file == nil {
		return nil, shfs.NotFound("pinned file")
	}
	if length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	if length == 0 || offset >= file.Size {
		return []byte{}, nil
	}
	result := make([]byte, length)
	end := offset + length
	if end > file.Size {
		end = file.Size
	}
	for _, segment := range overlappingFileSegments(file, chunks, offset, end) {
		if err := h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end]); err != nil {
			return nil, err
		}
	}
	return result[:end-offset], nil
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

func (h *StorHub) Chtimes(project, targetPath string, atime, mtime int64) error {
	return h.ChtimesContext(context.Background(), project, targetPath, atime, mtime)
}

func (h *StorHub) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error {
	return h.posixService().ChtimesContext(ctx, project, targetPath, atime, mtime)
}

// ChtimesExplicitContext forwards utimensat-style trinary semantics:
// nil omits a timestamp, non-nil sets it exactly (epoch included).
func (h *StorHub) ChtimesExplicitContext(ctx context.Context, project, targetPath string, atime, mtime *time.Time) error {
	return h.posixService().ChtimesExplicitContext(ctx, project, targetPath, atime, mtime)
}

func (h *StorHub) SetXAttr(project, targetPath, attr string, data []byte) error {
	return h.SetXAttrContext(context.Background(), project, targetPath, attr, data)
}

func (h *StorHub) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte, mode ...shfs.XAttrMode) error {
	return h.posixService().SetXAttrContext(ctx, project, targetPath, attr, data, mode...)
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
