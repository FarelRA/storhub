package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	shfs "github.com/FarelRA/storhub/internal/fs"
)

// preparePatchWorkspace resolves ONE upload release for a patch batch
// against a catalog-only probe (newReleaseProbe): picking reads the release
// set + counts, never file bytes, so the old per-builder full-tree Clone
// (O(tree) memcpy just to RemoveFile + EnsureRelease on a throwaway) was
// pure waste. It returns the release tag/URL plus the probe for the
// rotation prepare closures. Partial-upload compensation stays with the
// callers (they own sink.results).
func (h *StorHub) preparePatchWorkspace(ctx context.Context, project string, repoMeta *RepoMetadata, filePath string, requiredSlots int) (releaseTag, uploadURL string, probe *RepoMetadata, err error) {
	probe, err = newReleaseProbe(repoMeta, project, h.config.Now().Unix())
	if err != nil {
		return "", "", nil, err
	}
	probe.RemoveFile(filePath)
	releaseTag, uploadURL, err = h.getOrCreateUploadRelease(ctx, project, probe, requiredSlots)
	if err != nil {
		return "", "", nil, err
	}
	return releaseTag, uploadURL, probe, nil
}

// finalizePlaylist orders an assembled chunk playlist by file offset. The
// builders usually emit in order already (rewrites sweep offsets ascending;
// single edits splice into an ordered base), so the sort runs only when a
// linear scan finds an inversion — skipping the O(k log k) re-sort per op
// on wide files (audit 33). The returned slice is always offset-sorted.
func finalizePlaylist(assembled []ChunkInfo) []ChunkInfo {
	for i := 1; i < len(assembled); i++ {
		if assembled[i].Offset < assembled[i-1].Offset {
			sort.SliceStable(assembled, func(a, b int) bool { return assembled[a].Offset < assembled[b].Offset })
			return assembled
		}
	}
	return assembled
}

func (h *StorHub) buildPatchedChunks(ctx context.Context, project string, repoMeta *RepoMetadata, fileMeta FileMeta, filePath string, patchOffset, deleteSize int64, edit []byte) ([]ChunkInfo, string, error) {
	finalSize := fileMeta.Size - deleteSize + int64(len(edit))
	requiredSlots := inlineChunkCount(int64(len(edit)), h.config.ChunkSize)
	if finalSize == 0 && requiredSlots == 0 {
		return []ChunkInfo{}, "", nil
	}
	releaseTag, uploadURL, probe, err := h.preparePatchWorkspace(ctx, project, repoMeta, filePath, requiredSlots)
	if err != nil {
		return nil, "", err
	}

	patchedChunks, actualTag, _, err := h.uploadInlineChunks(ctx, project, releaseTag, uploadURL, patchOffset, edit, func(remaining int) (string, string, error) {
		return h.getOrCreateUploadRelease(ctx, project, probe, remaining)
	})
	if err != nil {
		h.compensateDeleteAssets(ctx, project, patchedChunks)
		return nil, "", err
	}

	resolved := make([]ChunkInfo, 0, len(fileMeta.Chunks))
	for _, name := range fileMeta.Chunks {
		if chunk, ok := repoMeta.Chunks()[name]; ok {
			resolved = append(resolved, chunk)
		}
	}

	assembled := finalizePlaylist(spliceEdit(resolved, patchOffset, deleteSize, int64(len(edit)), patchedChunks))
	// Report the release that actually holds the new chunks. A
	// release-full rotation inside the sink moves later chunks; the
	// initial tag may no longer hold them.
	return assembled, actualTag, nil
}

// spliceEdit rewrites a playlist for one edit: chunks entirely before the
// edit span pass through, chunks after it shift by delta, chunks spanning
// it are cut into prefix/suffix views of their original assets, and the
// inserted chunks - whose offsets must already be final - slot into place.
// Pure metadata math; no I/O. Both the single-edit and batched patch
// builders funnel through here so the layout rules have one definition.
func spliceEdit(chunks []ChunkInfo, patchOffset, deleteSize, insertedLen int64, inserted []ChunkInfo) []ChunkInfo {
	assembled := make([]ChunkInfo, 0, len(chunks)+len(inserted)+2)
	patchEnd := patchOffset + deleteSize
	delta := insertedLen - deleteSize
	for _, chunk := range chunks {
		chunkEnd := chunk.Offset + chunk.Size
		if chunkEnd <= patchOffset || chunk.Offset >= patchEnd {
			if chunk.Offset >= patchEnd {
				chunk.Offset += delta
			}
			assembled = append(assembled, chunk)
			continue
		}
		if chunk.Offset < patchOffset {
			prefix := chunk
			prefix.Size = patchOffset - chunk.Offset
			assembled = append(assembled, prefix)
		}
		if chunkEnd > patchEnd {
			suffix := chunk
			suffix.Offset = patchEnd
			suffix.Size = chunkEnd - patchEnd
			suffix.AssetOffset = chunk.AssetOffset + (patchEnd - chunk.Offset)
			suffix.Offset += delta
			assembled = append(assembled, suffix)
		}
	}
	assembled = append(assembled, inserted...)
	return assembled
}

func (h *StorHub) uploadInlineChunks(ctx context.Context, project, releaseTag, uploadURL string, fileOffset int64, data []byte, prepare func(remaining int) (string, string, error)) (chunks []ChunkInfo, actualTag, actualURL string, err error) {
	count := inlineChunkCount(int64(len(data)), h.config.ChunkSize)
	sink := h.newChunkSink(ctx, project, releaseTag, uploadURL, count, prepare)
	chunkSize := chunking.NormalizedSize(h.config.ChunkSize)
	for i := 0; i < count; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if err := sink.put(bytes.NewReader(data[start:end]), end-start, fileOffset+start); err != nil {
			return sink.results, sink.releaseTag, sink.uploadURL, err
		}
	}
	return sink.results, sink.releaseTag, sink.uploadURL, nil
}

// sliceChunkView cuts a sub-view [newOffset, newOffset+newSize) of an
// uploaded chunk: pure metadata math, no I/O, never fails. A sliced view
// is not the whole uploaded asset, so its digest no longer applies;
// verification is skipped for such chunks.
func sliceChunkView(original ChunkInfo, newOffset, newSize int64) ChunkInfo {
	segment := original
	segment.Offset = newOffset
	segment.Size = newSize
	segment.AssetOffset = original.AssetOffset + (newOffset - original.Offset)
	return segment
}

func inlineChunkCount(size, chunkSize int64) int {
	chunkSize = chunking.NormalizedSize(chunkSize)
	if size == 0 {
		return 0
	}
	return int((size + chunkSize - 1) / chunkSize)
}

func (h *StorHub) buildRewrittenChunks(ctx context.Context, project string, repoMeta *RepoMetadata, file FileMeta, filePath, snapshotPath string, finalSize int64, dirtyRanges []byteRange) (assembled, uploaded []ChunkInfo, tag string, err error) {
	chunkSize := chunking.NormalizedSize(h.config.ChunkSize)
	dirtySegments := make([]byteRange, 0, len(dirtyRanges))
	for _, dirty := range dirtyRanges {
		dirtySegments = mergeByteRange(dirtySegments, dirty)
	}
	// mergeByteRange preserves input order, not sorted order: establish the
	// merged-sorted invariant rangeOverlapsAny's early exit depends on.
	sort.Slice(dirtySegments, func(i, j int) bool { return dirtySegments[i].start < dirtySegments[j].start })
	requiredSlots := 0
	for _, dirty := range dirtySegments {
		requiredSlots += inlineChunkCount(dirty.end-dirty.start, chunkSize)
	}
	releaseTag, uploadURL, probe, err := h.preparePatchWorkspace(ctx, project, repoMeta, filePath, requiredSlots)
	if err != nil {
		return nil, nil, "", err
	}
	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return nil, nil, "", fmt.Errorf("open snapshot: %w", err)
	}
	defer func() { _ = snapshot.Close() }()
	assembled = make([]ChunkInfo, 0, inlineChunkCount(finalSize, chunkSize)+len(file.Chunks))
	var uploadedAll []ChunkInfo
	// Track where the bytes actually land; a rotation mid-rewrite
	// moves the sink and the reported tag must follow it.
	curTag, curURL := releaseTag, uploadURL
	for offset := int64(0); offset < finalSize; offset += chunkSize {
		end := offset + chunkSize
		if end > finalSize {
			end = finalSize
		}
		segment := byteRange{start: offset, end: end}
		if rangeOverlapsAny(segment, dirtySegments) {
			uploaded, landedTag, landedURL, err := h.uploadFileRangeChunks(ctx, project, curTag, curURL, snapshot, segment.start, segment.end, func(remaining int) (string, string, error) {
				return h.getOrCreateUploadRelease(ctx, project, probe, remaining)
			})
			if err != nil {
				h.compensateDeleteAssets(ctx, project, append(uploadedAll, uploaded...))
				return nil, nil, "", err
			}
			curTag, curURL = landedTag, landedURL
			uploadedAll = append(uploadedAll, uploaded...)
			assembled = append(assembled, uploaded...)
			continue
		}
		reused, err := h.referenceFileRangeChunks(ctx, project, repoMeta.Chunks(), file, segment.start, segment.end)
		if err != nil {
			h.compensateDeleteAssets(ctx, project, uploadedAll)
			return nil, nil, "", err
		}
		assembled = append(assembled, reused...)
	}
	// assembled mixes reused committed chunks with fresh uploads; only
	// uploadedAll may ever be compensated (same regression as the patch
	// batch path: deleting a reused chunk orphans live data).
	return finalizePlaylist(assembled), uploadedAll, curTag, nil
}

// rangeOverlapsAny reports whether target overlaps any range.
// INVARIANT: ranges must be merged-sorted by start (mergeByteRange output
// re-sorted, as buildRewrittenChunks establishes). The early `return false`
// on the first range starting past target.end is only valid under that
// order; unsorted input can miss an overlap. Callers with unordered ranges
// must sort first (cheap: dirty lists are tiny) rather than dropping the
// early exit into an O(n) scan on this hot path.
func rangeOverlapsAny(target byteRange, ranges []byteRange) bool {
	for _, current := range ranges {
		if current.end <= target.start {
			continue
		}
		if current.start >= target.end {
			return false
		}
		return true
	}
	return false
}

func (h *StorHub) uploadFileRangeChunks(ctx context.Context, project, releaseTag, uploadURL string, snapshot *os.File, start, end int64, prepare func(remaining int) (string, string, error)) (chunks []ChunkInfo, actualTag, actualURL string, err error) {
	if end <= start {
		return nil, releaseTag, uploadURL, nil
	}
	chunkSize := chunking.NormalizedSize(h.config.ChunkSize)
	count := inlineChunkCount(end-start, chunkSize)
	sink := h.newChunkSink(ctx, project, releaseTag, uploadURL, count, prepare)
	for i := 0; i < count; i++ {
		chunkStart := start + int64(i)*chunkSize
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > end {
			chunkEnd = end
		}
		section := io.NewSectionReader(snapshot, chunkStart, chunkEnd-chunkStart)
		if err := sink.put(section, chunkEnd-chunkStart, chunkStart); err != nil {
			return sink.results, sink.releaseTag, sink.uploadURL, err
		}
	}
	return sink.results, sink.releaseTag, sink.uploadURL, nil
}

func (h *StorHub) referenceFileRangeChunks(ctx context.Context, project string, repoChunks map[int64]ChunkInfo, file FileMeta, start, end int64) ([]ChunkInfo, error) {
	if end <= start {
		return nil, nil
	}
	assembled := make([]ChunkInfo, 0, len(file.Chunks))
	for _, id := range file.Chunks {
		chunk, ok := repoChunks[id]
		if !ok {
			continue
		}
		chunkEnd := chunk.Offset + chunk.Size
		if chunkEnd <= start || chunk.Offset >= end {
			continue
		}
		segStart := max(chunk.Offset, start)
		segEnd := min(chunkEnd, end)
		if segStart == chunk.Offset && segEnd == chunkEnd {
			segment := chunk
			segment.Offset = segStart
			assembled = append(assembled, segment)
			continue
		}
		assembled = append(assembled, sliceChunkView(chunk, segStart, segEnd-segStart))
	}
	return assembled, nil
}

// buildPatchedRangeChunks applies a batch of ascending, disjoint edits in
// one pass: ONE release resolution for the whole batch, sequential
// uploads of exactly the edited bytes, and one playlist rebuild. Edits
// are folded through spliceEdit left to right with a running shift, so
// the layout math stays identical to the single-edit path by
// construction.
func (h *StorHub) buildPatchedRangeChunks(ctx context.Context, project string, repoMeta *RepoMetadata, fileMeta FileMeta, filePath string, edits []shfs.RangeEdit) (assembled, uploaded []ChunkInfo, tag string, err error) {
	if len(edits) == 0 {
		return nil, nil, "", errors.New("patch batch is empty")
	}
	chunkSize := chunking.NormalizedSize(h.config.ChunkSize)

	requiredSlots := 0
	for _, edit := range edits {
		requiredSlots += inlineChunkCount(edit.Len(), chunkSize)
	}
	releaseTag, uploadURL, probe, err := h.preparePatchWorkspace(ctx, project, repoMeta, filePath, requiredSlots)
	if err != nil {
		return nil, nil, "", err
	}

	resolved := make([]ChunkInfo, 0, len(fileMeta.Chunks))
	for _, name := range fileMeta.Chunks {
		if chunk, ok := repoMeta.Chunks()[name]; ok {
			resolved = append(resolved, chunk)
		}
	}

	assembled = resolved
	shift := int64(0)
	var uploadedAll []ChunkInfo
	curTag, curURL := releaseTag, uploadURL
	for _, edit := range edits {
		inserted, landedTag, landedURL, err := h.uploadInlineChunks(ctx, project, curTag, curURL, edit.Start+shift, edit.Data, func(remaining int) (string, string, error) {
			return h.getOrCreateUploadRelease(ctx, project, probe, remaining)
		})
		if err != nil {
			h.compensateDeleteAssets(ctx, project, append(uploadedAll, inserted...))
			return nil, nil, "", err
		}
		// A rotation inside this edit moves the sink; later edits
		// must upload to where the bytes actually land.
		curTag, curURL = landedTag, landedURL
		uploadedAll = append(uploadedAll, inserted...)
		assembled = spliceEdit(assembled, edit.Start+shift, edit.DeleteSize, edit.Len(), inserted)
		shift += edit.Len() - edit.DeleteSize
	}
	// assembled mixes reused committed chunks with fresh uploads; only
	// uploadedAll may ever be compensated (deleting a reused chunk would
	// orphan live data: regression TestConcurrentAppendByteExactness).
	return finalizePlaylist(assembled), uploadedAll, curTag, nil
}
