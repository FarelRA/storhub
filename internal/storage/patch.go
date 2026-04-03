package storage

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	shfs "github.com/FarelRA/storhub/internal/fs"
)

func (h *StorHub) buildPatchedChunks(ctx context.Context, project string, repoMeta *RepoMetadata, file FileMetadata, patchOffset, deleteSize int64, edit []byte) ([]ChunkInfo, string, error) {
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(file.Name)
	finalSize := file.Size - deleteSize + int64(len(edit))
	requiredSlots := inlineChunkCount(int64(len(edit)), h.config.ChunkSize)
	if finalSize == 0 && requiredSlots == 0 {
		return []ChunkInfo{}, file.Release, nil
	}
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots, file.Release)
	if err != nil {
		return nil, "", err
	}

	patchedChunks, err := h.uploadInlineChunks(ctx, project, releaseTag, uploadURL, file.Name, patchOffset, edit)
	if err != nil {
		return nil, "", err
	}

	assembled := make([]ChunkInfo, 0, len(file.Chunks)+len(patchedChunks)+2)
	patchEnd := patchOffset + deleteSize
	delta := int64(len(edit)) - deleteSize
	for _, chunk := range file.Chunks {
		chunkEnd := chunk.Offset + chunk.Size
		if chunkEnd <= patchOffset || chunk.Offset >= patchEnd {
			if chunk.Offset >= patchEnd {
				chunk.Offset += delta
			}
			assembled = append(assembled, chunk)
			continue
		}
		if chunk.Offset < patchOffset {
			prefix, err := h.sliceChunk(ctx, project, chunk, chunk.Offset, patchOffset-chunk.Offset)
			if err != nil {
				return nil, "", err
			}
			assembled = append(assembled, prefix)
		}
		if chunkEnd > patchEnd {
			suffix, err := h.sliceChunk(ctx, project, chunk, patchEnd, chunkEnd-patchEnd)
			if err != nil {
				return nil, "", err
			}
			suffix.Offset += delta
			assembled = append(assembled, suffix)
		}
	}
	assembled = append(assembled, patchedChunks...)
	sort.SliceStable(assembled, func(i, j int) bool { return assembled[i].Offset < assembled[j].Offset })
	for i := range assembled {
		assembled[i].Index = i
	}
	return assembled, releaseTag, nil
}

func (h *StorHub) uploadInlineChunks(ctx context.Context, project, releaseTag, uploadURL, fileName string, fileOffset int64, data []byte) ([]ChunkInfo, error) {
	count := inlineChunkCount(int64(len(data)), h.config.ChunkSize)
	results := make([]ChunkInfo, count)
	_ = fileName
	_ = fileOffset
	namer := newAssetNamer()
	err := runConcurrent(ctx, h.config.MaxConcurrentTransfers, count, func(i int) error {
		start := int64(i) * normalizedChunkSize(h.config.ChunkSize)
		end := start + normalizedChunkSize(h.config.ChunkSize)
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		part := data[start:end]
		assetName, err := namer.Next()
		if err != nil {
			return err
		}
		assetID, checksum, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, bytes.NewReader(part), int64(len(part)))
		if err != nil {
			return fmt.Errorf("upload patch chunk %d: %w", i, err)
		}
		results[i] = ChunkInfo{
			Name:        assetName,
			Size:        int64(len(part)),
			Index:       i,
			Offset:      fileOffset + start,
			Release:     releaseTag,
			AssetOffset: 0,
			AssetID:     assetID,
			CRC32C:      checksum,
		}
		return nil
	})
	return results, err
}

func (h *StorHub) sliceChunk(ctx context.Context, project string, original ChunkInfo, newOffset, newSize int64) (ChunkInfo, error) {
	segment := original
	segment.Offset = newOffset
	segment.Size = newSize
	segment.AssetOffset = original.AssetOffset + (newOffset - original.Offset)
	checksum, err := h.checksumAssetRange(ctx, project, segment)
	if err != nil {
		return ChunkInfo{}, fmt.Errorf("slice chunk %d: %w", original.Index, err)
	}
	segment.CRC32C = checksum
	return segment, nil
}

func inlineChunkCount(size, chunkSize int64) int {
	chunkSize = normalizedChunkSize(chunkSize)
	if size == 0 {
		return 0
	}
	return int((size + chunkSize - 1) / chunkSize)
}

func normalizedChunkSize(chunkSize int64) int64 {
	if chunkSize <= 0 {
		chunkSize = chunking.DefaultChunkSize
	}
	if chunkSize > chunking.MaxReleaseAssetSize {
		return chunking.MaxReleaseAssetSize
	}
	return chunkSize
}

func sumCRC32C(data []byte) string { return formatCRC32C(crc32.Checksum(data, crc32cTable)) }

func (h *StorHub) buildRewrittenChunks(ctx context.Context, project string, repoMeta *RepoMetadata, file FileMetadata, snapshotPath string, finalSize int64, dirtyRanges []byteRange) ([]ChunkInfo, string, error) {
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile(file.Name)
	chunkSize := normalizedChunkSize(h.config.ChunkSize)
	dirtySegments := make([]byteRange, 0, len(dirtyRanges))
	for _, dirty := range dirtyRanges {
		dirtySegments = mergeByteRange(dirtySegments, dirty)
	}
	requiredSlots := 0
	for _, dirty := range dirtySegments {
		requiredSlots += inlineChunkCount(dirty.end-dirty.start, chunkSize)
	}
	releaseTag, uploadURL, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, requiredSlots, file.Release)
	if err != nil {
		return nil, "", err
	}
	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return nil, "", fmt.Errorf("open snapshot: %w", err)
	}
	defer snapshot.Close()
	assembled := make([]ChunkInfo, 0, inlineChunkCount(finalSize, chunkSize)+len(file.Chunks))
	for offset := int64(0); offset < finalSize; offset += chunkSize {
		end := offset + chunkSize
		if end > finalSize {
			end = finalSize
		}
		segment := byteRange{start: offset, end: end}
		if rangeOverlapsAny(segment, dirtySegments) {
			uploaded, err := h.uploadFileRangeChunks(ctx, project, releaseTag, uploadURL, file.Name, snapshot, segment.start, segment.end)
			if err != nil {
				return nil, "", err
			}
			assembled = append(assembled, uploaded...)
			continue
		}
		reused, err := h.referenceFileRangeChunks(ctx, project, file, segment.start, segment.end)
		if err != nil {
			return nil, "", err
		}
		assembled = append(assembled, reused...)
	}
	sort.SliceStable(assembled, func(i, j int) bool { return assembled[i].Offset < assembled[j].Offset })
	for i := range assembled {
		assembled[i].Index = i
		if assembled[i].Release == "" {
			assembled[i].Release = releaseTag
		}
	}
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

func (h *StorHub) uploadFileRangeChunks(ctx context.Context, project, releaseTag, uploadURL, fileName string, snapshot *os.File, start, end int64) ([]ChunkInfo, error) {
	if end <= start {
		return nil, nil
	}
	chunkSize := normalizedChunkSize(h.config.ChunkSize)
	count := inlineChunkCount(end-start, chunkSize)
	results := make([]ChunkInfo, count)
	_ = fileName
	namer := newAssetNamer()
	err := runConcurrent(ctx, h.config.MaxConcurrentTransfers, count, func(i int) error {
		chunkStart := start + int64(i)*chunkSize
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > end {
			chunkEnd = end
		}
		section := io.NewSectionReader(snapshot, chunkStart, chunkEnd-chunkStart)
		assetName, err := namer.Next()
		if err != nil {
			return err
		}
		assetID, checksum, err := h.uploadAssetStreaming(ctx, project, releaseTag, uploadURL, assetName, section, chunkEnd-chunkStart)
		if err != nil {
			return fmt.Errorf("upload rewritten chunk %d: %w", i, err)
		}
		results[i] = ChunkInfo{Name: assetName, Size: chunkEnd - chunkStart, Index: i, Offset: chunkStart, Release: releaseTag, AssetOffset: 0, AssetID: assetID, CRC32C: checksum}
		return nil
	})
	return results, err
}

func (h *StorHub) referenceFileRangeChunks(ctx context.Context, project string, file FileMetadata, start, end int64) ([]ChunkInfo, error) {
	if end <= start {
		return nil, nil
	}
	assembled := make([]ChunkInfo, 0, len(file.Chunks))
	for _, chunk := range file.Chunks {
		chunkEnd := chunk.Offset + chunk.Size
		if chunkEnd <= start || chunk.Offset >= end {
			continue
		}
		segStart := shfs.MaxInt64(chunk.Offset, start)
		segEnd := shfs.MinInt64(chunkEnd, end)
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
