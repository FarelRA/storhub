package metadata

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// RevertSubtree restores dst's `path` (a file or an entire directory subtree)
// to its state in src, leaving every other path in dst untouched. It is the
// per-path revert primitive behind RollbackMetadata-for-one-path: the caller
// commits the result as a NEW revision (a revert, never a force-push), so
// history stays intact.
//
// The historical identity (inode, chunk ids) is preserved where it does not
// collide with unrelated live data; on collision it is remapped to a fresh
// allocation so a revert can never clobber a node it was not asked to touch.
// Hardlink siblings in src are related data, not collisions: reverting one
// name of a family reuses the family inode. Every reused id also advances
// dst's allocation counters past it, so later allocations in the same
// operation cannot hand out a live identifier. Chunk records and any release
// a reverted chunk references are copied in, so the restored file resolves
// against assets that full-history retention keeps alive. Reverting a path
// that is absent in src removes it from dst (restoring "this did not exist
// then").
func RevertSubtree(dst, src *RepoMetadata, path string, now int64) error {
	clean := normalizeStoredPath(path)
	if clean == "" {
		return errors.New("cannot revert the root path")
	}
	if strings.HasPrefix(clean, "../") || clean == ".." {
		return fmt.Errorf("revert path escapes root: %q", path)
	}
	removeSubtree(dst, clean)

	if sf := src.FindFile(clean); sf != nil {
		ensureAncestors(dst, src, clean, now)
		return copyFile(dst, src, clean, now)
	}
	if src.HasDirectory(clean) {
		ensureAncestors(dst, src, clean, now)
		// Build the children index once for the whole walk: the public
		// DirectoryChildren deliberately rebuilds eagerly on every call,
		// which would make this traversal quadratic.
		childDirs, childFiles := buildChildIndexes(src)
		return copyDirTree(dst, src, clean, now, childDirs, childFiles)
	}
	// Absent in history: the revert is a deletion, already applied above.
	return nil
}

// buildChildIndexes maps each directory to its immediate child paths in one
// pass (sorted, matching DirectoryChildren's output).
func buildChildIndexes(m *RepoMetadata) (childDirs, childFiles map[string][]string) {
	childDirs = make(map[string][]string, len(m.dirs))
	childFiles = make(map[string][]string, len(m.files))
	for p := range m.dirs {
		parent := parentPath(p)
		childDirs[parent] = append(childDirs[parent], p)
	}
	for p := range m.files {
		parent := parentPath(p)
		childFiles[parent] = append(childFiles[parent], p)
	}
	for _, children := range childDirs {
		sort.Strings(children)
	}
	for _, children := range childFiles {
		sort.Strings(children)
	}
	return childDirs, childFiles
}

// removeSubtree deletes path (file or directory and everything under it) from
// m. Chunk records are left in place; PruneUnreferencedChunks reclaims them
// once nothing references them.
func removeSubtree(m *RepoMetadata, path string) {
	childDirs, childFiles := buildChildIndexes(m)
	removeSubtreeWith(m, path, childDirs, childFiles)
}

func removeSubtreeWith(m *RepoMetadata, path string, childDirs, childFiles map[string][]string) {
	m.RemoveFile(path)
	if _, ok := m.dirs[path]; !ok {
		return
	}
	for _, f := range childFiles[path] {
		m.RemoveFile(f)
	}
	for _, d := range childDirs[path] {
		removeSubtreeWith(m, d, childDirs, childFiles)
	}
	m.RemoveDirectory(path)
}

// ensureAncestors recreates every missing ancestor directory of path in dst,
// taking its attributes from src when src still has it (so a restored file
// lands in the directory it lived in) and a plain default otherwise.
func ensureAncestors(dst, src *RepoMetadata, path string, now int64) {
	segments := strings.Split(path, "/")
	cur := ""
	for _, seg := range segments[:len(segments)-1] {
		if cur == "" {
			cur = seg
		} else {
			cur = cur + "/" + seg
		}
		if dst.HasDirectory(cur) {
			continue
		}
		// A file squatting on the ancestor path would end up both a file
		// and a directory once the ancestor is recreated; the directory
		// wins (the reverted entry lives under it).
		dst.RemoveFile(cur)
		if sd := src.GetDirectory(cur); sd != nil {
			// Deep-clone: a shallow struct copy aliases src's XAttrs map
			// (and its values) into dst, leaking preview mutations back
			// into the historical tree the commit reverts from.
			clone := sd.Clone()
			if inodeFreeFor(dst, clone.Inode, cur) {
				bumpInodePast(dst, clone.Inode)
				dst.WriteDirDirect(cur, clone)
				continue
			}
		}
		dst.EnsureDirectory(cur, now)
	}
}

func copyFile(dst, src *RepoMetadata, path string, now int64) error {
	sf := src.FindFile(path)
	if sf == nil {
		return fmt.Errorf("revert source file vanished: %s", path)
	}
	clone := sf.Clone()
	ids, dropped := remapChunks(dst, src, clone.Chunks)
	clone.Chunks = ids
	if dropped && clone.Symlink == "" {
		// Dangling source references were dropped; the declared size must
		// not keep counting bytes the restored file can no longer serve.
		clone.Size = chunksCoveredSize(dst.chunks, ids)
	}
	if inodeFreeFor(dst, clone.Inode, path) || hardlinkFamilyHolds(dst, src, clone.Inode, path) {
		bumpInodePast(dst, clone.Inode)
	} else {
		clone.Inode = dst.AllocateInode()
	}
	_ = now
	dst.WriteFileDirect(path, clone)
	return nil
}

func copyDirTree(dst, src *RepoMetadata, path string, now int64, childDirs, childFiles map[string][]string) error {
	sd := src.GetDirectory(path)
	if sd == nil {
		return fmt.Errorf("revert source directory vanished: %s", path)
	}
	clone := sd.Clone()
	if inodeFreeFor(dst, clone.Inode, path) {
		bumpInodePast(dst, clone.Inode)
	} else {
		clone.Inode = dst.AllocateInode()
	}
	dst.WriteDirDirect(path, clone)

	for _, f := range childFiles[path] {
		if err := copyFile(dst, src, f, now); err != nil {
			return err
		}
	}
	for _, d := range childDirs[path] {
		if err := copyDirTree(dst, src, d, now, childDirs, childFiles); err != nil {
			return err
		}
	}
	return nil
}

// remapChunks copies the historical chunk records for ids into dst's catalog,
// returning the (possibly remapped) id list the restored file should use and
// whether any dangling reference was dropped. An id whose record already
// matches is reused; an id reused for different content gets a fresh
// allocation so the live data is not clobbered.
func remapChunks(dst, src *RepoMetadata, ids []int64) ([]int64, bool) {
	out := make([]int64, 0, len(ids))
	dropped := false
	for _, id := range ids {
		record, ok := src.chunks[id]
		if !ok {
			// Dangling reference in the source; drop it rather than invent
			// a record. Validate would reject a file pointing at a missing
			// chunk, so skipping keeps the revert loadable.
			dropped = true
			continue
		}
		if existing, taken := dst.chunks[id]; taken && existing != record {
			newID := dst.AllocateChunkID()
			dst.chunks[newID] = record
			out = append(out, newID)
		} else {
			dst.chunks[id] = record
			bumpChunkPast(dst, id)
			out = append(out, id)
		}
		if record.Release != "" {
			if _, have := dst.releases[record.Release]; !have {
				if ref, ok := src.releases[record.Release]; ok {
					dst.releases[record.Release] = ref
				} else {
					dst.EnsureRelease(record.Release, 0)
				}
			}
		}
	}
	return out, dropped
}

// chunksCoveredSize is the highest byte offset the given chunk records
// reach: the size a restored file can actually serve.
func chunksCoveredSize(records map[int64]ChunkInfo, ids []int64) int64 {
	var end int64
	for _, id := range ids {
		if c, ok := records[id]; ok {
			if e := c.Offset + c.Size; e > end {
				end = e
			}
		}
	}
	return end
}

// bumpInodePast and bumpChunkPast keep the allocation counters ahead of every
// live id after revert reuses a historical identifier. AllocateInode and
// AllocateChunkID trust the counter unconditionally, so a revision whose
// persisted counters regressed behind the ids revert restores would otherwise
// re-mint them and clobber the just-restored records.
func bumpInodePast(m *RepoMetadata, inode uint64) {
	if inode >= m.NextInode {
		m.NextInode = inode + 1
	}
}

func bumpChunkPast(m *RepoMetadata, id int64) {
	if id >= m.NextChunkID {
		m.NextChunkID = id + 1
	}
}

// inodeFreeFor reports whether inode is unused in dst, or already belongs to
// this exact path (identity, not collision). It checks both files and dirs,
// scanning the maps directly: the inode index would have to be rebuilt on
// every call during a subtree walk.
func inodeFreeFor(m *RepoMetadata, inode uint64, path string) bool {
	if inode == 0 {
		return false
	}
	for p, f := range m.files {
		if f.Inode == inode && p != path {
			return false
		}
	}
	for dp, d := range m.dirs {
		if d.Inode == inode && dp != path {
			return false
		}
	}
	return true
}

// hardlinkFamilyHolds reports whether the dst paths currently squatting on
// inode are all members of the same hardlink family as path in src (they
// share the inode there too). Reverting one name of a family must reuse the
// family inode - the sibling is related data, not an unrelated collision -
// while any holder that is unrelated in src, or a directory, still forces a
// remap.
func hardlinkFamilyHolds(dst, src *RepoMetadata, inode uint64, path string) bool {
	if inode == 0 {
		return false
	}
	holders := 0
	for p, f := range dst.files {
		if f.Inode != inode || p == path {
			continue
		}
		holders++
		if sf := src.FindFile(p); sf == nil || sf.Inode != inode {
			return false
		}
	}
	for dp, d := range dst.dirs {
		if d.Inode == inode && dp != path {
			return false
		}
	}
	return holders > 0
}
