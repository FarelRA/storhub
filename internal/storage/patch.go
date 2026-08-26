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

func (h *StorHub) buildPatchedChunks(ctx context.Context, project string, repoMeta *RepoMetadata, fileMeta FileMeta, filePath string, patchOffset, deleteSize int64, edit []byte) ([]ChunkInfo, string, error) {
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(filePath)
	finalSize := fileMeta.Size - deleteSize + int64(len(edit))
	requiredSlots := inlineChunkCount(int64(len(edit)), h.config.ChunkSize)
	if finalSize == 0 && requiredSlots == 0 {
		return []ChunkInfo{}, "", nil
	}
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots)
	if err != nil {
		return nil, "", err
	}

	patchedChunks, err := h.uploadInlineChunks(ctx, project, releaseTag, uploadURL, patchOffset, edit)
	if err != nil {
		return nil, "", err
	}

	resolved := make([]ChunkInfo, 0, len(fileMeta.Chunks))
	for _, name := range fileMeta.Chunks {
		if chunk, ok := repoMeta.Chunks[name]; ok {
			resolved = append(resolved, chunk)
		}
	}

	assembled := spliceEdit(resolved, patchOffset, deleteSize, int64(len(edit)), patchedChunks)
	sort.SliceStable(assembled, func(i, j int) bool { return assembled[i].Offset < assembled[j].Offset })
	return assembled, releaseTag, nil
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

func (h *StorHub) uploadInlineChunks(ctx context.Context, project, releaseTag, uploadURL string, fileOffset int64, data []byte) ([]ChunkInfo, error) {
	count := inlineChunkCount(int64(len(data)), h.config.ChunkSize)
	results := make([]ChunkInfo, 0, count)
	namer := newAssetNamer()
	chunkSize := normalizedChunkSize(h.config.ChunkSize)
	for i := 0; i < count; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		part := data[start:end]
		assetName, err := namer.Next()
		if err != nil {
			return results, err
		}
		assetID, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, bytes.NewReader(part), int64(len(part)))
		if err != nil {
			return results, fmt.Errorf("upload patch chunk %d: %w", i, err)
		}
		results = append(results, ChunkInfo{
			Size:        int64(len(part)),
			Offset:      fileOffset + start,
			AssetOffset: 0,
			AssetID:     assetID,
			Release:     releaseTag,
		})
	}
	return results, nil
}

func (h *StorHub) sliceChunk(ctx context.Context, project string, original ChunkInfo, newOffset, newSize int64) (ChunkInfo, error) {
	segment := original
	segment.Offset = newOffset
	segment.Size = newSize
	segment.AssetOffset = original.AssetOffset + (newOffset - original.Offset)
	// A sliced view is not the whole uploaded asset, so its digest no
	// longer applies; verification is skipped for such chunks.
	return segment, nil
}

func inlineChunkCount(size, chunkSize int64) int {
	chunkSize = normalizedChunkSize(chunkSize)
	if size == 0 {
		return 0
	}
	return int((size + chunkSize - 1) / chunkSize)
}

// normalizedChunkSize delegates to the chunking package's single clamping
// definition.
func normalizedChunkSize(chunkSize int64) int64 {
	return chunking.NormalizedSize(chunkSize)
}

func (h *StorHub) buildRewrittenChunks(ctx context.Context, project string, repoMeta *RepoMetadata, file FileMeta, filePath, snapshotPath string, finalSize int64, dirtyRanges []byteRange) ([]ChunkInfo, string, error) {
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(filePath)
	chunkSize := normalizedChunkSize(h.config.ChunkSize)
	dirtySegments := make([]byteRange, 0, len(dirtyRanges))
	for _, dirty := range dirtyRanges {
		dirtySegments = mergeByteRange(dirtySegments, dirty)
	}
	requiredSlots := 0
	for _, dirty := range dirtySegments {
		requiredSlots += inlineChunkCount(dirty.end-dirty.start, chunkSize)
	}
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots)
	if err != nil {
		return nil, "", err
	}
	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return nil, "", fmt.Errorf("open snapshot: %w", err)
	}
	defer func() { _ = snapshot.Close() }()
	assembled := make([]ChunkInfo, 0, inlineChunkCount(finalSize, chunkSize)+len(file.Chunks))
	for offset := int64(0); offset < finalSize; offset += chunkSize {
		end := offset + chunkSize
		if end > finalSize {
			end = finalSize
		}
		segment := byteRange{start: offset, end: end}
		if rangeOverlapsAny(segment, dirtySegments) {
			uploaded, err := h.uploadFileRangeChunks(ctx, project, releaseTag, uploadURL, snapshot, segment.start, segment.end)
			if err != nil {
				return nil, "", err
			}
			assembled = append(assembled, uploaded...)
			continue
		}
		reused, err := h.referenceFileRangeChunks(ctx, project, repoMeta.Chunks, file, segment.start, segment.end)
		if err != nil {
			return nil, "", err
		}
		assembled = append(assembled, reused...)
	}
	sort.SliceStable(assembled, func(i, j int) bool { return assembled[i].Offset < assembled[j].Offset })
	return assembled, releaseTag, nil
}

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

func (h *StorHub) uploadFileRangeChunks(ctx context.Context, project, releaseTag, uploadURL string, snapshot *os.File, start, end int64) ([]ChunkInfo, error) {
	if end <= start {
		return nil, nil
	}
	chunkSize := normalizedChunkSize(h.config.ChunkSize)
	count := inlineChunkCount(end-start, chunkSize)
	results := make([]ChunkInfo, 0, count)
	namer := newAssetNamer()
	for i := 0; i < count; i++ {
		chunkStart := start + int64(i)*chunkSize
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > end {
			chunkEnd = end
		}
		section := io.NewSectionReader(snapshot, chunkStart, chunkEnd-chunkStart)
		assetName, err := namer.Next()
		if err != nil {
			return results, err
		}
		assetID, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, section, chunkEnd-chunkStart)
		if err != nil {
			return results, fmt.Errorf("upload rewritten chunk %d: %w", i, err)
		}
		results = append(results, ChunkInfo{Size: chunkEnd - chunkStart, Offset: chunkStart, AssetOffset: 0, AssetID: assetID, Release: releaseTag})
	}
	return results, nil
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
		segment, err := h.sliceChunk(ctx, project, chunk, segStart, segEnd-segStart)
		if err != nil {
			return nil, err
		}
		assembled = append(assembled, segment)
	}
	return assembled, nil
}

// buildPatchedRangeChunks applies a batch of ascending, disjoint edits in
// one pass: ONE release resolution for the whole batch, sequential
// uploads of exactly the edited bytes, and one playlist rebuild. Edits
// are folded through spliceEdit left to right with a running shift, so
// the layout math stays identical to the single-edit path by
// construction.
func (h *StorHub) buildPatchedRangeChunks(ctx context.Context, project string, repoMeta *RepoMetadata, fileMeta FileMeta, filePath string, edits []shfs.RangeEdit) ([]ChunkInfo, string, error) {
	if len(edits) == 0 {
		return nil, "", errors.New("patch batch is empty")
	}
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(filePath)
	chunkSize := normalizedChunkSize(h.config.ChunkSize)

	requiredSlots := 0
	for _, edit := range edits {
		requiredSlots += inlineChunkCount(edit.Len(), chunkSize)
	}
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots)
	if err != nil {
		return nil, "", err
	}

	resolved := make([]ChunkInfo, 0, len(fileMeta.Chunks))
	for _, name := range fileMeta.Chunks {
		if chunk, ok := repoMeta.Chunks[name]; ok {
			resolved = append(resolved, chunk)
		}
	}

	assembled := resolved
	shift := int64(0)
	for _, edit := range edits {
		inserted, err := h.uploadInlineChunks(ctx, project, releaseTag, uploadURL, edit.Start+shift, edit.Data)
		if err != nil {
			return nil, "", err
		}
		assembled = spliceEdit(assembled, edit.Start+shift, edit.DeleteSize, edit.Len(), inserted)
		shift += edit.Len() - edit.DeleteSize
	}
	sort.SliceStable(assembled, func(i, j int) bool { return assembled[i].Offset < assembled[j].Offset })
	return assembled, releaseTag, nil
}
