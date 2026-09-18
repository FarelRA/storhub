package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"

	shfs "github.com/FarelRA/storhub/internal/fs"
	implposix "github.com/FarelRA/storhub/internal/posix"
)

// Server-side copy (reflink plus range clone).
//
// CloneRange copies the byte range [srcOff, srcOff+length) of one file onto
// another file (or the same file) at dstOff, transferring zero bytes: every
// destination record references the same release asset the source bytes
// already live in. The whole-file clone is the full-range case
// (srcOff 0, length equal to the source size); there is no separate entry
// point.
//
// Record construction (mirrors referenceFileRangeChunks in patch.go, with
// fresh identifiers): a source chunk fully covered by the cloned range is
// re-linked as a FRESH chunk ID pointing at the same release, asset ID and
// asset offset, so record IDs are never shared between files. A source
// chunk straddling a range edge becomes a narrowed sub-chunk reference with
// adjusted file offset, asset offset and size against the same asset.
// Source holes stay holes: no records are minted for uncovered spans, and a
// destination gap (dstOff past the old destination EOF, or a hole inherited
// from the source) zero-fills by size accounting alone, because readers
// serve unrecorded spans from a zero-initialized buffer.
//
// Overlap follows memmove snapshot semantics: the source layout is captured
// from the pre-operation tree before any destination record is written, so
// forward and backward intra-file overlaps read the pre-op bytes. Cloning a
// range onto itself is therefore an exact no-op duplicate: allowed, and
// still applied through the one atomic transaction like every other clone.
//
// Permissions are read on the source plus write on the destination, checked
// once at call time with the same Check functions the sibling verbs use. A
// clone is not a read for atime: the source entry is left fully untouched
// (no atime refresh), and the destination keeps its own atime.
//
// Atomicity is one exclusive UpdateRepoMetadataContext transaction:
// all records land or none do. Expected-revision preconditions (opts) are
// enforced like the sibling verbs.
//
// The metadata schema cannot represent a file with Size greater than zero
// and zero chunk records, so a clone whose result would be exactly that
// (for example a hole-only range into a fresh destination) is refused with
// an error before anything commits, instead of landing state the commit
// path could never validate.
func (h *StorHub) CloneRange(ctx context.Context, project, src string, srcOff int64, dst string, dstOff int64, length int64, opts ...shfs.MutateOption) (result *FileMeta, err error) {
	started := h.logOpStart(project, "clone-range", "src", src, "src_off", srcOff, "dst", dst, "dst_off", dstOff, "length", length)
	defer func() {
		h.logOpFinish(project, "clone-range", started, err, "src", src, "src_off", srcOff, "dst", dst, "dst_off", dstOff, "length", length)
	}()
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	if err := validateProject(project); err != nil {
		return nil, err
	}
	if err := shfs.ValidateAccessPathShape(src); err != nil {
		return nil, err
	}
	if err := shfs.ValidateAccessPathShape(dst); err != nil {
		return nil, err
	}
	if srcOff < 0 || dstOff < 0 || length < 0 {
		return nil, errors.New("clone offsets and length must be non-negative")
	}
	repoMeta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	// Clone follows a final symlink at both ends (open semantics, like the
	// read and write verbs), so resolution is stat-style on both paths.
	srcClean, srcTraversed, err := shfs.ResolveAccessPath(repoMeta, src, true)
	if err != nil {
		return nil, err
	}
	dstClean, dstTraversed, err := shfs.ResolveAccessPath(repoMeta, dst, true)
	if err != nil {
		return nil, err
	}
	if srcClean == "" || dstClean == "" {
		return nil, errors.New("clone source and destination names are required")
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, srcTraversed); err != nil {
		return nil, err
	}
	if err := shfs.CheckTraversal(ctx, repoMeta, dstTraversed); err != nil {
		return nil, err
	}
	if repoMeta.HasDirectory(srcClean) {
		return nil, shfs.IsDirectory(srcClean)
	}
	srcFile := repoMeta.FindFile(srcClean)
	if srcFile == nil {
		return nil, shfs.NotFound(srcClean)
	}
	if repoMeta.HasDirectory(dstClean) {
		return nil, shfs.IsDirectory(dstClean)
	}
	dstFile := repoMeta.FindFile(dstClean)
	// Read on the source plus write on the destination, checked once at
	// call time. A missing destination is a creation: parent-directory
	// presence plus parent write, the same rule the session layer uses for
	// new paths.
	if err := shfs.CheckReadAccessResolved(ctx, repoMeta, srcClean, srcTraversed); err != nil {
		return nil, err
	}
	if dstFile != nil {
		if err := shfs.CheckWriteAccessResolved(ctx, repoMeta, dstClean, dstTraversed); err != nil {
			return nil, err
		}
	} else {
		if err := shfs.RequireParentDirectory(repoMeta, dstClean); err != nil {
			return nil, err
		}
		if err := shfs.CheckParentWriteResolved(ctx, repoMeta, dstClean, dstTraversed); err != nil {
			return nil, err
		}
	}
	if srcOff > srcFile.Size {
		return nil, fmt.Errorf("clone source offset %d exceeds file size %d", srcOff, srcFile.Size)
	}
	if length > srcFile.Size-srcOff {
		return nil, fmt.Errorf("clone source range [%d,%d) exceeds file size %d", srcOff, srcOff+length, srcFile.Size)
	}
	if length > int64(^uint64(0)>>1)-dstOff {
		return nil, fmt.Errorf("clone destination range [%d,%d) overflows", dstOff, dstOff+length)
	}
	if length == 0 {
		// Zero-length clone is a validated no-op: permissions, existence
		// and ranges already checked above, nothing to mutate.
		if dstFile == nil {
			return nil, shfs.NotFound(dstClean)
		}
		noop := dstFile.Clone()
		return &noop, nil
	}
	now := h.config.Now().Unix()
	snapSrc := cloneFileSnapshot{size: srcFile.Size, chunks: append([]int64(nil), srcFile.Chunks...)}
	snapDst := cloneFileSnapshot{existed: dstFile != nil}
	if dstFile != nil {
		snapDst.size = dstFile.Size
		snapDst.chunks = append([]int64(nil), dstFile.Chunks...)
	}

	live, err := h.UpdateRepoMetadataContext(ctx, project, func(tree *RepoMetadata) error {
		return h.applyCloneRange(ctx, tree, srcClean, dstClean, srcOff, dstOff, length, snapSrc, snapDst, now)
	}, fmt.Sprintf("storhub: clone-range %s to %s", srcClean, dstClean))
	if err != nil {
		return nil, err
	}
	updated := live.FindFile(dstClean)
	if updated == nil {
		return nil, shfs.NotFound(dstClean)
	}
	out := updated.Clone()
	return &out, nil
}

// cloneFileSnapshot pins the pre-operation layout one clone decision was
// based on, so the transaction can refuse concurrent rewrites instead of
// cloning stale bytes.
type cloneFileSnapshot struct {
	size    int64
	chunks  []int64
	existed bool
}

// applyCloneRange runs inside the exclusive metadata transaction, where the
// candidate tree is the pre-operation truth no other writer can reach. It
// captures the source layout from that tree before writing any destination
// record (memmove snapshot semantics), rebuilds the destination playlist
// with fresh chunk IDs, and writes the destination entry. Any error aborts
// the transaction with nothing committed.
func (h *StorHub) applyCloneRange(ctx context.Context, tree *RepoMetadata, srcClean, dstClean string, srcOff, dstOff, length int64, snapSrc, snapDst cloneFileSnapshot, now int64) error {
	srcCur := tree.FindFile(srcClean)
	if srcCur == nil {
		return shfs.NotFound(srcClean)
	}
	if tree.HasDirectory(srcClean) {
		return shfs.IsDirectory(srcClean)
	}
	if srcCur.Size != snapSrc.size || !slices.Equal(srcCur.Chunks, snapSrc.chunks) {
		return fmt.Errorf("file %s changed concurrently (size %d, expected %d); clone rejected", srcClean, srcCur.Size, snapSrc.size)
	}
	if srcOff > srcCur.Size || length > srcCur.Size-srcOff {
		return fmt.Errorf("clone source range [%d,%d) exceeds file size %d", srcOff, srcOff+length, srcCur.Size)
	}
	dstCur := tree.FindFile(dstClean)
	if tree.HasDirectory(dstClean) {
		return shfs.IsDirectory(dstClean)
	}
	if dstCur == nil && snapDst.existed {
		return shfs.NotFound(dstClean)
	}
	if dstCur != nil && !snapDst.existed {
		return fmt.Errorf("file %s created concurrently; clone rejected", dstClean)
	}
	if dstCur != nil && (dstCur.Size != snapDst.size || !slices.Equal(dstCur.Chunks, snapDst.chunks)) {
		return fmt.Errorf("file %s changed concurrently (size %d, expected %d); clone rejected", dstClean, dstCur.Size, snapDst.size)
	}

	srcEnd := srcOff + length
	dstEnd := dstOff + length

	// Snapshot the source records before touching the destination, so an
	// intra-file overlap clones the pre-op bytes.
	repoChunks := tree.Chunks()
	cloned := make([]ChunkInfo, 0, len(srcCur.Chunks))
	for _, id := range srcCur.Chunks {
		rec, ok := repoChunks[id]
		if !ok {
			return fmt.Errorf("chunk %d not found", id)
		}
		recEnd := rec.Offset + rec.Size
		if recEnd <= srcOff || rec.Offset >= srcEnd {
			continue
		}
		segStart := max(rec.Offset, srcOff)
		segEnd := min(recEnd, srcEnd)
		cloned = append(cloned, ChunkInfo{
			Size:        segEnd - segStart,
			Offset:      dstOff + (segStart - srcOff),
			Release:     rec.Release,
			AssetOffset: rec.AssetOffset + (segStart - rec.Offset),
			AssetID:     rec.AssetID,
		})
	}

	// Rebuild the destination playlist with pwrite (overwrite) semantics:
	// survivors fully outside the range pass through under their existing
	// IDs, survivors straddling an edge are narrowed into fresh IDs (an
	// in-place record edit would corrupt any other file sharing the
	// record), and fully covered survivors are dropped. The mapping from
	// source to destination offsets is monotonic, so the cloned records
	// arrive sorted and the concatenation stays offset-ordered.
	var keptIDs []int64
	var fresh []ChunkInfo
	if dstCur != nil {
		for _, id := range dstCur.Chunks {
			rec, ok := repoChunks[id]
			if !ok {
				return fmt.Errorf("chunk %d not found", id)
			}
			recEnd := rec.Offset + rec.Size
			if recEnd <= dstOff || rec.Offset >= dstEnd {
				keptIDs = append(keptIDs, id)
				continue
			}
			if rec.Offset < dstOff {
				fresh = append(fresh, ChunkInfo{
					Size:        dstOff - rec.Offset,
					Offset:      rec.Offset,
					Release:     rec.Release,
					AssetOffset: rec.AssetOffset,
					AssetID:     rec.AssetID,
				})
			}
			if recEnd > dstEnd {
				fresh = append(fresh, ChunkInfo{
					Size:        recEnd - dstEnd,
					Offset:      dstEnd,
					Release:     rec.Release,
					AssetOffset: rec.AssetOffset + (dstEnd - rec.Offset),
					AssetID:     rec.AssetID,
				})
			}
		}
	}
	fresh = append(fresh, cloned...)

	var dstSize int64
	if dstCur != nil {
		dstSize = dstCur.Size
	}
	finalSize := max(dstSize, dstEnd)
	if len(keptIDs)+len(fresh) == 0 {
		// Length is positive here, so finalSize is positive too: a file
		// with size and no records is unrepresentable (see the package
		// doc comment), and refusing keeps the transaction all-or-nothing
		// instead of landing state no commit could validate.
		return fmt.Errorf("clone range [%d,%d) holds no data; refusing a destination with size %d and no chunk records", srcOff, srcEnd, finalSize)
	}
	if err := ensureChunkReleases(tree, fresh, now); err != nil {
		return err
	}
	finalIDs := make([]int64, 0, len(keptIDs)+len(fresh))
	finalIDs = append(finalIDs, keptIDs...)
	for i := range fresh {
		id := tree.AllocateChunkID()
		if err := tree.PutChunk(id, fresh[i]); err != nil {
			return err
		}
		finalIDs = append(finalIDs, id)
	}
	// Kept survivors sort before dstOff and cloned records are ascending,
	// but narrowed suffixes interleave by construction order: prefix
	// narrows were emitted before the clones that follow them. Re-sort by
	// offset to restore the stored-order invariant regardless of shape.
	sortCloneIDsByOffset(tree.Chunks(), finalIDs)

	if dstCur != nil {
		updated := dstCur.Clone()
		updated.Chunks = finalIDs
		updated.Size = finalSize
		updated.Mode = shfs.SanitizeWrittenFileModeForContext(ctx, updated.Mode)
		// A clone writes the destination but is not a read of either side
		// for atime: ApplyUpdatedFileIdentity preserves atime, and the
		// source entry is never touched at all.
		implposix.ApplyUpdatedFileIdentity(dstClean, &updated, dstCur, now)
		implposix.ReplaceInodeFamily(tree, dstClean, dstCur, updated, now)
		return nil
	}
	created := FileMeta{
		Chunks: finalIDs,
		Size:   finalSize,
		Mode:   shfs.SanitizeWrittenFileModeForContext(ctx, srcCur.Mode),
	}
	implposix.ApplyUploadIdentity(dstClean, nil, &created, now)
	defaultUID, defaultGID := h.DefaultOwnerIDs()
	created.UID, created.GID = shfs.OwnerIDsForCreate(ctx, defaultUID, defaultGID)
	created.Mode, created.UID, created.GID = shfs.ApplyParentInheritance(tree, dstClean, false, created.Mode, created.UID, created.GID)
	tree.UpsertFile(dstClean, created, now)
	shfs.TouchParentDirectory(tree, dstClean, now)
	return nil
}

// sortCloneIDsByOffset orders one destination playlist by data offset. The
// builder usually emits in order already, but a narrowed destination suffix
// is collected before the clones it trails, so order by construction.
func sortCloneIDsByOffset(chunks map[int64]ChunkInfo, ids []int64) {
	for i := 1; i < len(ids); i++ {
		if chunks[ids[i]].Offset < chunks[ids[i-1]].Offset {
			slices.SortStableFunc(ids, func(a, b int64) int {
				oa, ob := chunks[a].Offset, chunks[b].Offset
				switch {
				case oa < ob:
					return -1
				case oa > ob:
					return 1
				default:
					return 0
				}
			})
			return
		}
	}
}
