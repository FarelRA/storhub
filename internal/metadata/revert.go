package metadata

import (
	"errors"
	"fmt"
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
// Chunk records and any release a reverted chunk references are copied in, so
// the restored file resolves against assets that full-history retention keeps
// alive. Reverting a path that is absent in src removes it from dst (restoring
// "this did not exist then").
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
		return copyDirTree(dst, src, clean, now)
	}
	// Absent in history: the revert is a deletion, already applied above.
	return nil
}

// removeSubtree deletes path (file or directory and everything under it) from
// m. Chunk records are left in place; PruneUnreferencedChunks reclaims them
// once nothing references them.
func removeSubtree(m *RepoMetadata, path string) {
	if m.FindFile(path) != nil {
		m.RemoveFile(path)
	}
	if !m.HasDirectory(path) {
		return
	}
	dirs, files := m.DirectoryChildren(path)
	for _, f := range files {
		m.RemoveFile(f)
	}
	for _, d := range dirs {
		removeSubtree(m, d)
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
		if sd := src.GetDirectory(cur); sd != nil {
			clone := *sd
			if inodeFreeFor(dst, clone.Inode, cur) {
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
	clone.Chunks = remapChunks(dst, src, clone.Chunks)
	if !inodeFreeFor(dst, clone.Inode, path) {
		clone.Inode = dst.AllocateInode()
	}
	_ = now
	dst.WriteFileDirect(path, clone)
	return nil
}

func copyDirTree(dst, src *RepoMetadata, path string, now int64) error {
	sd := src.GetDirectory(path)
	if sd == nil {
		return fmt.Errorf("revert source directory vanished: %s", path)
	}
	clone := *sd
	clone.XAttrs = normalizeXAttrs(clone.XAttrs)
	if !inodeFreeFor(dst, clone.Inode, path) {
		clone.Inode = dst.AllocateInode()
	}
	dst.WriteDirDirect(path, clone)

	dirs, files := src.DirectoryChildren(path)
	for _, f := range files {
		if err := copyFile(dst, src, f, now); err != nil {
			return err
		}
	}
	for _, d := range dirs {
		if err := copyDirTree(dst, src, d, now); err != nil {
			return err
		}
	}
	return nil
}

// remapChunks copies the historical chunk records for ids into dst's catalog,
// returning the (possibly remapped) id list the restored file should use. An
// id whose record already matches is reused; an id reused for different
// content gets a fresh allocation so the live data is not clobbered.
func remapChunks(dst, src *RepoMetadata, ids []int64) []int64 {
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		record, ok := src.Chunks[id]
		if !ok {
			// Dangling reference in the source; drop it rather than invent
			// a record. Validate would reject a file pointing at a missing
			// chunk, so skipping keeps the revert loadable.
			continue
		}
		if existing, taken := dst.Chunks[id]; taken && existing != record {
			newID := dst.AllocateChunkID()
			dst.Chunks[newID] = record
			out = append(out, newID)
		} else {
			dst.Chunks[id] = record
			out = append(out, id)
		}
		if record.Release != "" {
			if _, have := dst.Releases[record.Release]; !have {
				if ref, ok := src.Releases[record.Release]; ok {
					dst.Releases[record.Release] = ref
				} else {
					dst.EnsureRelease(record.Release, 0)
				}
			}
		}
	}
	return out
}

// inodeFreeFor reports whether inode is unused in dst, or already belongs to
// this exact path (identity, not collision). It checks both files and dirs.
func inodeFreeFor(m *RepoMetadata, inode uint64, path string) bool {
	if inode == 0 {
		return false
	}
	for _, p := range m.FindFilesByInode(inode) {
		if p != path {
			return false
		}
	}
	for dp, d := range m.Dirs {
		if d.Inode == inode && dp != path {
			return false
		}
	}
	return true
}
