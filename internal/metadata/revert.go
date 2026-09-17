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
// Hardlink siblings in src are related data, not collisions: reverting one
// name of a family reuses the family inode. Every reused id also advances
// dst's allocation counters past it, so later allocations in the same
// operation cannot hand out a live identifier. Chunk records and any release
// a reverted chunk references are copied in, so the restored file resolves
// against assets that full-history retention keeps alive. Reverting a path
// that is absent in src removes it from dst (restoring "this did not exist
// then").
func RevertSubtree(dst, src *RepoMetadata, path string, now int64) error {
	clean, err := normalizeStoredPathErr(path)
	if err != nil {
		return fmt.Errorf("revert path escapes root: %q", path)
	}
	if clean == "" {
		return errors.New("cannot revert the root path")
	}
	removeSubtree(dst, clean)
	// Index dst's inode holders once for the whole walk: a full files+dirs
	// scan per restored path made subtree reverts quadratic. The index is
	// threaded through the copy helpers and updated at every mutation, so
	// reuse decisions always see the current holders.
	holders := newHolderIndex(dst)

	if sf := src.FindFile(clean); sf != nil {
		ensureAncestors(dst, src, clean, now, holders)
		return copyFile(dst, src, clean, now, holders)
	}
	if src.HasDirectory(clean) {
		ensureAncestors(dst, src, clean, now, holders)
		// Build the children index once for the whole walk: the public
		// DirectoryChildren deliberately rebuilds eagerly on every call,
		// which would make this traversal quadratic. src is never mutated
		// here, so one snapshot covers the walk.
		childDirs := groupByParent(src.dirs)
		childFiles := groupByParent(src.files)
		return copyDirTree(dst, src, clean, now, holders, childDirs, childFiles)
	}
	// Absent in history: the revert is a deletion, already applied above.
	return nil
}

// removeSubtree deletes path (file or directory and everything under it) from
// m. Chunk records are left in place; PruneUnreferencedChunks reclaims them
// once nothing references them.
func removeSubtree(m *RepoMetadata, path string) {
	removeSubtreeWith(m, path, groupByParent(m.dirs), groupByParent(m.files))
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
func ensureAncestors(dst, src *RepoMetadata, path string, now int64, holders *holderIndex) {
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
		if squatter, ok := dst.files[cur]; ok {
			holders.remove(squatter.Inode, cur)
		}
		dst.RemoveFile(cur)
		if sd := src.GetDirectory(cur); sd != nil {
			// Deep-clone: a shallow struct copy aliases src's XAttrs map
			// (and its values) into dst, leaking preview mutations back
			// into the historical tree the commit reverts from.
			clone := sd.Clone()
			if holders.freeFor(clone.Inode, cur) {
				bumpPast(&dst.NextInode, clone.Inode)
				dst.WriteDirDirect(cur, clone)
				holders.add(clone.Inode, cur)
				continue
			}
		}
		dst.EnsureDirectory(cur, now)
		holders.add(dst.dirs[cur].Inode, cur)
	}
}

func copyFile(dst, src *RepoMetadata, path string, now int64, holders *holderIndex) error {
	sf := src.FindFile(path)
	if sf == nil {
		return fmt.Errorf("revert source file vanished: %s", path)
	}
	clone := sf.Clone()
	// now is load-bearing here: it stamps releases the source itself no
	// longer knows, and EnsureRelease rejects a zero timestamp. That is why
	// copyFile/copyDirTree keep their now parameter even though nothing
	// else in the copy path needs a clock.
	ids, dropped, err := remapChunks(dst, src, clone.Chunks, now)
	if err != nil {
		return err
	}
	clone.Chunks = ids
	if dropped && clone.Symlink == "" {
		// Dangling source references were dropped; the declared size must
		// not keep counting bytes the restored file can no longer serve.
		clone.Size = chunksCoveredSize(dst.chunks, ids)
	}
	if holders.freeFor(clone.Inode, path) || holders.familyHolds(dst, src, clone.Inode, path) {
		bumpPast(&dst.NextInode, clone.Inode)
	} else {
		clone.Inode = dst.allocateInode()
	}
	dst.WriteFileDirect(path, clone)
	holders.add(clone.Inode, path)
	return nil
}

func copyDirTree(dst, src *RepoMetadata, path string, now int64, holders *holderIndex, childDirs, childFiles map[string][]string) error {
	sd := src.GetDirectory(path)
	if sd == nil {
		return fmt.Errorf("revert source directory vanished: %s", path)
	}
	clone := sd.Clone()
	if holders.freeFor(clone.Inode, path) {
		bumpPast(&dst.NextInode, clone.Inode)
	} else {
		clone.Inode = dst.allocateInode()
	}
	dst.WriteDirDirect(path, clone)
	holders.add(clone.Inode, path)

	for _, f := range childFiles[path] {
		if err := copyFile(dst, src, f, now, holders); err != nil {
			return err
		}
	}
	for _, d := range childDirs[path] {
		if err := copyDirTree(dst, src, d, now, holders, childDirs, childFiles); err != nil {
			return err
		}
	}
	return nil
}

// remapChunks copies the historical chunk records for ids into dst's catalog
// through the tracked PutChunk/EnsureRelease mutators - never raw map
// writes, so asset counts, the size cache, and the intent recorder stay
// exact - returning the (possibly remapped) id list the restored file should
// use and whether any dangling reference was dropped. An id whose record
// already matches is reused; an id reused for different content gets a fresh
// allocation so the live data is not clobbered. now stamps releases the
// source itself no longer knows (never zero: EnsureRelease rejects it).
//
// Releases are created AFTER all chunk puts (two phases): a PutChunk for a
// tag with no ref waits in pendingAssets, and the trailing EnsureRelease
// drains exactly those waits into the new ref. Creating the ref first - via
// PutRelease with the source's snapshot count, or via EnsureRelease before
// the puts - double-counts: the snapshot already includes the chunks being
// restored, so adding the restored puts on top overstates AssetCount (e.g.
// restoring 2 chunks of a 2-asset release yielded 4). Draining pending after
// the puts counts only what dst actually holds.
func remapChunks(dst, src *RepoMetadata, ids []int64, now int64) ([]int64, bool, error) {
	out := make([]int64, 0, len(ids))
	dropped := false
	var tagsInOrder []string
	tagsSeen := make(map[string]struct{})
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
			newID := dst.allocateChunkID()
			if err := dst.PutChunk(newID, record); err != nil {
				return nil, false, fmt.Errorf("revert chunk %d: %w", id, err)
			}
			out = append(out, newID)
		} else {
			if err := dst.PutChunk(id, record); err != nil {
				return nil, false, fmt.Errorf("revert chunk %d: %w", id, err)
			}
			bumpPast(&dst.NextChunkID, id)
			out = append(out, id)
		}
		if tag := strings.TrimSpace(record.Release); tag != "" {
			if _, seen := tagsSeen[tag]; !seen {
				tagsSeen[tag] = struct{}{}
				tagsInOrder = append(tagsInOrder, tag)
			}
		}
	}
	for _, tag := range tagsInOrder {
		if _, have := dst.releases[tag]; have {
			continue
		}
		createdAt := now
		if ref, ok := src.releases[tag]; ok && ref.CreatedAt != 0 {
			createdAt = ref.CreatedAt
		} else {
			for raw, ref := range src.releases {
				if strings.TrimSpace(raw) == tag && ref.CreatedAt != 0 {
					createdAt = ref.CreatedAt
					break
				}
			}
		}
		if createdAt == 0 {
			createdAt = now
		}
		if _, err := dst.EnsureRelease(tag, createdAt); err != nil {
			return nil, false, fmt.Errorf("revert release %q: %w", tag, err)
		}
	}
	return out, dropped, nil
}

// chunksCoveredSize is the highest byte offset the given chunk records
// reach: the size a restored file can actually serve. Ends saturate to
// MaxInt64 instead of wrapping negative at extreme offsets.
func chunksCoveredSize(records map[int64]ChunkInfo, ids []int64) int64 {
	var end int64
	for _, id := range ids {
		if c, ok := records[id]; ok {
			if c.Offset < 0 || c.Size < 0 {
				continue
			}
			if e, ok := checkedAdd(c.Offset, c.Size); ok {
				if e > end {
					end = e
				}
			} else if end < int64(^uint64(0)>>1) {
				end = int64(^uint64(0) >> 1)
			}
		}
	}
	return end
}

// bumpPast advances an allocation counter past a reused historical id.
// allocateInode/allocateChunkID trust the counter unconditionally, so a
// revision whose persisted counters regressed behind the ids revert restores
// would otherwise re-mint them and clobber the just-restored records. One
// generic covers both counters, whose bodies differed only in type.
func bumpPast[T uint64 | int64](counter *T, id T) {
	if id >= *counter {
		*counter = id + 1
	}
}

// holderIndex maps each live inode to the dst paths holding it: one O(N)
// scan per revert instead of one per restored path. Copy helpers consult it
// for reuse-vs-remap decisions and update it at every mutation, so it always
// reflects dst.
type holderIndex struct {
	byInode map[uint64]map[string]struct{}
}

func newHolderIndex(m *RepoMetadata) *holderIndex {
	h := &holderIndex{byInode: make(map[uint64]map[string]struct{})}
	for p, f := range m.files {
		h.add(f.Inode, p)
	}
	for p, d := range m.dirs {
		h.add(d.Inode, p)
	}
	return h
}

func (h *holderIndex) add(inode uint64, path string) {
	if inode == 0 {
		return
	}
	holders := h.byInode[inode]
	if holders == nil {
		holders = make(map[string]struct{})
		h.byInode[inode] = holders
	}
	holders[path] = struct{}{}
}

func (h *holderIndex) remove(inode uint64, path string) {
	holders := h.byInode[inode]
	if holders == nil {
		return
	}
	delete(holders, path)
	if len(holders) == 0 {
		delete(h.byInode, inode)
	}
}

// freeFor reports whether inode is unused in dst, or already belongs to this
// exact path (identity, not collision).
func (h *holderIndex) freeFor(inode uint64, path string) bool {
	if inode == 0 {
		return false
	}
	holders := h.byInode[inode]
	if len(holders) == 0 {
		return true
	}
	if len(holders) == 1 {
		_, ok := holders[path]
		return ok
	}
	return false
}

// familyHolds reports whether the dst paths currently squatting on inode are
// all members of the same hardlink family as path in src (they share the
// inode there too). Reverting one name of a family must reuse the family
// inode - the sibling is related data, not an unrelated collision - while
// any holder that is unrelated in src, or a directory, still forces a remap.
func (h *holderIndex) familyHolds(dst, src *RepoMetadata, inode uint64, path string) bool {
	if inode == 0 {
		return false
	}
	holders := h.byInode[inode]
	family := 0
	for p := range holders {
		if p == path {
			continue
		}
		family++
		// A directory holder is never family: directories can't hardlink.
		if _, isDir := dst.dirs[p]; isDir {
			return false
		}
		if sf := src.FindFile(p); sf == nil || sf.Inode != inode {
			return false
		}
	}
	return family > 0
}
