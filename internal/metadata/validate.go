package metadata

import (
	"fmt"
	"sort"
	"strings"
)

// sortFileChunksByOffset enforces the stored-order invariant every reader
// relies on: a file's chunk IDs are ordered by data offset, so binary search
// over FileChunks is valid.
func (m *RepoMetadata) sortFileChunksByOffset() {
	for path, file := range m.files {
		if len(file.Chunks) < 2 {
			continue
		}
		if chunksOffsetSorted(m.chunks, file.Chunks) {
			continue
		}
		original := file
		sorted := append([]int64(nil), file.Chunks...)
		sort.SliceStable(sorted, func(i, j int) bool {
			return m.chunks[sorted[i]].Offset < m.chunks[sorted[j]].Offset
		})
		file.Chunks = sorted
		m.files[path] = file
		m.sizePutFile(path, original, true, file)
	}
}

// chunksOffsetSorted reports whether the chunk ids are already ordered by
// data offset (the comparator sort.SliceStable applies is a no-op on such a
// sequence, including when a referenced chunk is missing and resolves to the
// zero offset).
func chunksOffsetSorted(chunks map[int64]ChunkInfo, ids []int64) bool {
	var prev int64 = -1
	for _, id := range ids {
		offset := chunks[id].Offset
		if offset < prev {
			return false
		}
		prev = offset
	}
	return true
}

// reconcileCounters raises the allocation counters past every id actually
// present, so deleting the highest-numbered entry cannot mint duplicates.
func (m *RepoMetadata) reconcileCounters() {
	maxInode := m.Root.Inode
	for _, dir := range m.dirs {
		if dir.Inode > maxInode {
			maxInode = dir.Inode
		}
	}
	for _, file := range m.files {
		if file.Inode > maxInode {
			maxInode = file.Inode
		}
	}
	if m.NextInode <= maxInode {
		m.NextInode = maxInode + 1
	}

	maxChunkID := int64(0)
	for id := range m.chunks {
		if id > maxChunkID {
			maxChunkID = id
		}
	}
	if m.NextChunkID <= maxChunkID {
		m.NextChunkID = maxChunkID + 1
	}
}

func (m *RepoMetadata) Validate() error {
	if strings.TrimSpace(m.Project) == "" {
		return fmt.Errorf("metadata project is required")
	}
	if m.Root.Inode == 0 {
		return fmt.Errorf("metadata root inode is required")
	}
	if m.files == nil {
		return fmt.Errorf("metadata files map is nil")
	}
	if m.chunks == nil {
		return fmt.Errorf("metadata chunks map is nil")
	}
	if m.releases == nil {
		return fmt.Errorf("metadata releases map is nil")
	}
	if m.dirs == nil {
		return fmt.Errorf("metadata dirs map is nil")
	}

	seenDirs := map[string]struct{}{}
	seenInodes := map[uint64]struct{}{m.Root.Inode: {}}

	for path, dir := range m.dirs {
		if err := dir.Validate(); err != nil {
			return fmt.Errorf("directory %s: %w", path, err)
		}
		if err := validateStoredPathKey(path); err != nil {
			return fmt.Errorf("directory %s: %w", path, err)
		}
		if _, ok := seenDirs[path]; ok {
			return fmt.Errorf("duplicate directory: %s", path)
		}
		seenDirs[path] = struct{}{}
		if _, ok := seenInodes[dir.Inode]; ok {
			return fmt.Errorf("duplicate inode %d", dir.Inode)
		}
		seenInodes[dir.Inode] = struct{}{}
		if parent := parentPath(path); parent != "" {
			if _, ok := m.dirs[parent]; !ok {
				return fmt.Errorf("directory %s missing parent %s", path, parent)
			}
		}
	}

	totalFiles := 0
	totalSize := int64(0)
	// One scratch map for every file's duplicate-chunk-reference check:
	// cleared per file instead of allocated per file.
	seenChunk := make(map[int64]struct{})
	for path, file := range m.files {
		if path == "" {
			return fmt.Errorf("file entry with empty path")
		}
		if err := validateStoredPathKey(path); err != nil {
			return fmt.Errorf("file %s: %w", path, err)
		}
		if err := file.Validate(); err != nil {
			return fmt.Errorf("file %s: %w", path, err)
		}
		// One path, one node: a key present in both maps makes the FS view
		// ambiguous even though the flat maps round-trip fine.
		if _, ok := seenDirs[path]; ok {
			return fmt.Errorf("path %q is both a file and a directory", path)
		}
		if file.Inode == 0 {
			return fmt.Errorf("file %s inode is required", path)
		}
		// Files may share an inode (hardlinks), but a file must never
		// collide with a directory or root inode. seenInodes holds only
		// root+directory inodes here; file inodes are deliberately not
		// added.
		if _, ok := seenInodes[file.Inode]; ok {
			return fmt.Errorf("file %s reuses directory inode %d", path, file.Inode)
		}
		if parent := parentPath(path); parent != "" {
			if _, ok := m.dirs[parent]; !ok {
				return fmt.Errorf("file %s missing parent directory %s", path, parent)
			}
		}
		if file.Symlink == "" {
			totalFiles++
			totalSize += file.Size
		}
		clear(seenChunk)
		var prevOffset int64 = -1
		var prevEnd int64 = -1
		for _, id := range file.Chunks {
			chunk, ok := m.chunks[id]
			if !ok {
				return fmt.Errorf("file %s references missing chunk %d", path, id)
			}
			if _, ok := seenChunk[id]; ok {
				return fmt.Errorf("file %s: duplicate chunk reference: %d", path, id)
			}
			seenChunk[id] = struct{}{}
			if chunk.Size < 0 {
				return fmt.Errorf("file %s: chunk %d has negative size %d", path, id, chunk.Size)
			}
			if chunk.Offset < 0 {
				return fmt.Errorf("file %s: chunk %d has negative offset %d", path, id, chunk.Offset)
			}
			if chunk.Offset < prevOffset {
				return fmt.Errorf("file %s: chunks not stored in offset order (%d after %d)", path, chunk.Offset, prevOffset)
			}
			// Ranges must be disjoint: equal or overlapping starts make
			// binary-search readers ambiguous. prevEnd is overflow-free
			// because the bounds check below already passed for it.
			if chunk.Offset < prevEnd {
				return fmt.Errorf("file %s: chunk %d overlaps the previous chunk (offset %d < %d)", path, id, chunk.Offset, prevEnd)
			}
			prevOffset = chunk.Offset
			// Overflow-free form of `chunk.Offset+chunk.Size > file.Size`:
			// a naive sum wraps negative at extreme offsets and lets a
			// chunk claim bytes past EOF.
			if file.Symlink == "" && chunk.Size > file.Size-chunk.Offset {
				return fmt.Errorf("file %s: chunk data extends beyond file size (%d+%d > %d)", path, chunk.Offset, chunk.Size, file.Size)
			}
			prevEnd = chunk.Offset + chunk.Size
		}
	}

	if m.TotalFiles != totalFiles {
		return fmt.Errorf("metadata total files mismatch: expected %d, got %d", totalFiles, m.TotalFiles)
	}
	if m.TotalSize != totalSize {
		return fmt.Errorf("metadata total size mismatch: expected %d, got %d", totalSize, m.TotalSize)
	}

	// Note: chunk.Release tags are intentionally allowed to reference tags
	// absent from the Releases catalog - DeleteRelease hides catalog entries
	// while live chunks keep pointing at them, and PurgeUntracked treats any
	// chunk-referenced release as tracked.

	for tag, ref := range m.releases {
		if ref.AssetCount < 0 {
			return fmt.Errorf("release %s has negative asset count %d", tag, ref.AssetCount)
		}
	}

	return nil
}

// validateStoredPathKey rejects map keys the storage normalizer could never
// produce: absolute paths, empty segments, ".." traversal, and any key that
// is not its own canonical form. Non-canonical keys ("a/", "a//b", "a/./b")
// pass a naive check but re-emerge canonicalized through the split round-trip
// - mutating the key or silently clobbering the entry already stored under
// the canonical form.
func validateStoredPathKey(path string) error {
	if path == "" || strings.HasPrefix(path, "/") {
		return fmt.Errorf("invalid stored path %q", path)
	}
	if path == ".." || strings.HasPrefix(path, "../") || strings.Contains(path, "/../") {
		return fmt.Errorf("stored path escapes root: %q", path)
	}
	if cleaned := normalizeStoredPath(path); cleaned != path {
		return fmt.Errorf("stored path %q is not canonical (normalizes to %q)", path, cleaned)
	}
	return nil
}

func (d DirMeta) Validate() error {
	if d.Inode == 0 {
		return fmt.Errorf("directory inode is required")
	}
	return nil
}

func (f FileMeta) Validate() error {
	if f.Size < 0 {
		return fmt.Errorf("file size must be non-negative")
	}
	if f.Inode == 0 {
		return fmt.Errorf("file inode is required")
	}
	// Directory type bits on a file entry indicate structural corruption.
	if f.Mode&0o170000 == 0o040000 {
		return fmt.Errorf("file entry carries directory type bits")
	}
	if f.Symlink != "" {
		if f.Size != int64(len([]byte(f.Symlink))) {
			return fmt.Errorf("symlink size mismatch")
		}
		if len(f.Chunks) != 0 {
			return fmt.Errorf("symlink must not contain chunks")
		}
		return nil
	}
	if len(f.Chunks) == 0 && f.Size > 0 {
		return fmt.Errorf("file must contain at least one chunk reference")
	}
	seen := make(map[int64]struct{}, len(f.Chunks))
	for _, id := range f.Chunks {
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate chunk reference: %d", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}
