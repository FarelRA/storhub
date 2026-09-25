package metadata

import (
	"fmt"
	"sort"
	"strings"
)

// GetRelease returns a snapshot of the release ref for tag, if present.
func (m *RepoMetadata) GetRelease(tag string) *ReleaseRef {
	// Snapshot footgun (same as FindFile): the pointer targets a copy of the
	// map value, so field mutations of the result are silently dropped.
	// Apply changes through EnsureRelease or a transaction that writes the
	// map back.
	if ref, ok := m.releases[tag]; ok {
		return &ref
	}
	return nil
}

// HasDirectory reports whether the directory key exists (root always does).
func (m *RepoMetadata) HasDirectory(path string) bool {
	path = normalizeStoredPath(path)
	if path == "" {
		return true
	}
	_, ok := m.dirs[path]
	return ok
}

// GetDirectory returns a snapshot of the directory entry, if present.
func (m *RepoMetadata) GetDirectory(path string) *DirMeta {
	path = normalizeStoredPath(path)
	if path == "" {
		root := m.Root
		return &root
	}
	if dir, ok := m.dirs[path]; ok {
		return &dir
	}
	return nil
}

// EnsureDirectory creates the directory and missing parents as needed.
func (m *RepoMetadata) EnsureDirectory(path string, now int64) {
	path = normalizeStoredPath(path)
	if path == "" || m.HasDirectory(path) {
		return
	}
	parent := parentPath(path)
	if parent != "" {
		m.EnsureDirectory(parent, now)
	}
	uid, gid := defaultOwnerIDs()
	dir := DirMeta{
		CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now,
		Mode: defaultDirMode(), UID: uid, GID: gid, Inode: m.allocateInode(),
	}
	dir.Normalize()
	m.dirs[path] = dir
	m.trackDirPut(path, putTransition(DirMeta{}, false, dir))
	m.recordDirPut(path, DirMeta{}, false)
}

// RemoveDirectory deletes the directory key, reporting whether it existed.
func (m *RepoMetadata) RemoveDirectory(path string) bool {
	path = normalizeStoredPath(path)
	old, ok := m.dirs[path]
	if !ok {
		return false
	}
	delete(m.dirs, path)
	m.trackDirRemove(path, old)
	m.recordDirRemove(path, old)
	return true
}

// DirectoryChildren lists the immediate child dirs and files of a path.
func (m *RepoMetadata) DirectoryChildren(path string) (dirs, files []string) {
	path = normalizeStoredPath(path)
	// Lazily rebuilt: every tracked mutation maintains the child lists
	// incrementally, invalidateIndexes marks them stale, and the structural
	// fingerprint (map identity + length) is defense in depth for in-package
	// bulk paths that swap or rebuild whole maps (a wholesale swap or a
	// key add/remove changes the fingerprint). The stored maps are
	// unexported, so no code outside this package can write them at all;
	// in-package writers that change a file's inode must go through
	// WriteFileDirect/ReplaceFile so the index stays warm.
	//
	// The returned slices are copies: the cached lists are shared state and
	// a caller appending to them must not reach into the index's backing
	// array.
	m.ensureIndexes()
	d := m.derived
	return cloneStrings(d.childDirs[path]), cloneStrings(d.childFiles[path])
}

// EnsureRelease returns the release ref for tag, creating it when missing.
func (m *RepoMetadata) EnsureRelease(tag string, createdAt int64) (*ReleaseRef, error) {
	// Snapshot footgun (same as FindFile/GetRelease): the returned pointer
	// targets a copy, so mutating its fields never reaches stored state.
	// The pointer is a read convenience only; write through this method or a
	// transaction.
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return nil, fmt.Errorf("release tag is required")
	}
	if createdAt == 0 {
		return nil, fmt.Errorf("release %q: CreatedAt is required (epoch zero is not a timestamp)", tag)
	}
	if ref, ok := m.releases[tag]; ok {
		return &ref, nil
	}
	ref := ReleaseRef{CreatedAt: createdAt}
	if d := m.derived; d != nil {
		// Chunks may reference this tag before the release exists (replay
		// order, or a put racing the release op): their counts waited in
		// pendingAssets and land here, keeping AssetCount exact. Pending
		// cannot go negative through the tracked mutators (puts and deletes
		// balance), but a defensive check keeps a drifted negative from
		// persisting as a negative AssetCount that Validate would reject
		// with less context.
		ref.AssetCount = d.pendingAssets[tag]
		delete(d.pendingAssets, tag)
		if ref.AssetCount < 0 {
			return nil, fmt.Errorf("release %q: asset count went negative (%d) after draining pending", tag, ref.AssetCount)
		}
	}
	m.releases[tag] = ref
	m.sizePutRelease(tag, putTransition(ReleaseRef{}, false, ref))
	m.recordReleasePut(tag, ReleaseRef{}, false)
	return &ref, nil
}

// UpsertFile stores the file entry, preserving identity across updates.
func (m *RepoMetadata) UpsertFile(path string, file FileMeta, createdAt int64) {
	path = normalizeStoredPath(path)
	// Clone the caller's value so later mutations of its slices cannot
	// alias into stored metadata.
	file = file.Clone()
	if parent := parentPath(path); parent != "" {
		m.EnsureDirectory(parent, createdAt)
	}
	existing, existed := m.files[path]
	if existed {
		if (file.Symlink == "") != (existing.Symlink == "") {
			// Type change (regular file <-> symlink): the old node identity
			// is discarded and a fresh one allocated, mirroring replacement.
			initializeNewFileIdentity(m, &file, createdAt)
		} else {
			preserveFileIdentity(&file, &existing, createdAt)
		}
	} else {
		initializeNewFileIdentity(m, &file, createdAt)
	}
	file.Normalize()
	m.files[path] = file
	m.trackFilePut(path, putTransition(existing, existed, file))
	m.statsFilePut(putTransition(existing, existed, file))
	m.recordFilePut(path, existing, existed)
}

// FindFile returns a SNAPSHOT of the entry: the pointer targets a copy of
// the struct, so FIELD-level mutations never reach stored state. (Slices
// and maps inside - Chunks, XAttrs - still share backing storage; clone
// before mutating those.) Apply changes through UpsertFile or an
// UpdateRepoMetadataContext transaction.
func (m *RepoMetadata) FindFile(path string) *FileMeta {
	path = normalizeStoredPath(path)
	if file, ok := m.files[path]; ok {
		return &file
	}
	return nil
}

// SetFileAtime updates atime in place. FindFile returns a pointer to a
// copy (map values are not addressable), so mutating its result silently
// drops the write; use this setter for mutations.
func (m *RepoMetadata) SetFileAtime(path string, atime int64) bool {
	path = normalizeStoredPath(path)
	if file, ok := m.files[path]; ok {
		original := file
		file.AccessedAt = atime
		m.files[path] = file
		m.sizePutFile(path, putTransition(original, true, file))
		m.recordFilePut(path, original, true)
		return true
	}
	return false
}

// SetDirAtime updates atime in place; same copy-pointer footgun as
// FindFile applies to GetDirectory results.
func (m *RepoMetadata) SetDirAtime(path string, atime int64) bool {
	path = normalizeStoredPath(path)
	if dir, ok := m.dirs[path]; ok {
		original := dir
		dir.AccessedAt = atime
		m.dirs[path] = dir
		m.sizePutDir(path, putTransition(original, true, dir))
		m.recordDirPut(path, original, true)
		return true
	}
	return false
}

// FindFilesByInode lists the file names sharing one inode.
func (m *RepoMetadata) FindFilesByInode(inode uint64) []string {
	m.ensureIndexes()
	names := m.derived.filesByInode[inode]
	out := make([]string, len(names))
	copy(out, names)
	return out
}

// WriteFileDirect stores an entry verbatim - no creation defaults, no
// identity repair. It exists for family updates that must preserve every
// field exactly (including authoritative epoch zeros) while bypassing the
// new-node path of UpsertFile. The stored value is treated as immutable
// from here on (callers pass a private clone); the derived indexes and size
// cache are maintained incrementally for the replacement.
func (m *RepoMetadata) WriteFileDirect(path string, file FileMeta) {
	path = normalizeStoredPath(path)
	existing, existed := m.files[path]
	// Clone the caller's value (mirroring UpsertFile): without this a
	// caller-retained Chunks slice aliases the stored entry.
	file = file.Clone()
	file.Normalize()
	m.files[path] = file
	m.trackFilePut(path, putTransition(existing, existed, file))
	m.statsFilePut(putTransition(existing, existed, file))
	m.recordFilePut(path, existing, existed)
}

// WriteDirDirect stores a directory entry verbatim, mirroring
// WriteFileDirect for op replay and family updates that must preserve
// every field exactly.
func (m *RepoMetadata) WriteDirDirect(path string, dir DirMeta) {
	path = normalizeStoredPath(path)
	existing, existed := m.dirs[path]
	dir.Normalize()
	m.dirs[path] = dir
	m.trackDirPut(path, putTransition(existing, existed, dir))
	m.recordDirPut(path, existing, existed)
}

// RemoveFile deletes the file key, reporting whether it existed.
func (m *RepoMetadata) RemoveFile(path string) bool {
	path = normalizeStoredPath(path)
	old, ok := m.files[path]
	if !ok {
		return false
	}
	delete(m.files, path)
	m.trackFileRemove(path, old)
	m.statsFileRemove(old)
	m.recordFileRemove(path, old)
	return true
}

// RemoveRelease deletes the release ref, parking counts for reuse.
func (m *RepoMetadata) RemoveRelease(tag string) bool {
	tag = strings.TrimSpace(tag)
	old, ok := m.releases[tag]
	if !ok {
		return false
	}
	delete(m.releases, tag)
	// Releases are not part of the derived indexes; only the size cache
	// needs the removal.
	m.sizeRemoveRelease(tag, old)
	// The released count is not forgotten: chunks still referencing the tag
	// keep their records, so the count waits in pendingAssets for the next
	// EnsureRelease/PutRelease to drain. RemoveRelease+EnsureRelease
	// round-trips AssetCount exactly instead of undercounting (or, with
	// interleaved puts, going negative).
	d := m.ensureDerived()
	if d.pendingAssets == nil {
		d.pendingAssets = make(map[string]int)
	}
	d.pendingAssets[tag] += old.AssetCount
	m.recordReleaseRemove(tag, old)
	return true
}

// PutChunk stores a chunk record, maintaining the serialized-size cache and
// the release asset count incrementally. Direct `meta.chunks[id] = info`
// writes bypass both, so tracked writers must use this. The stored ChunkInfo
// is a pure value type; no cloning is needed.
//
// The tag is trimmed and negative sizes/offsets/asset fields are rejected:
// an advisory-only contract let corrupt records in that Validate then had to
// chase. Callers replaying untrusted data must handle the error.
func (m *RepoMetadata) PutChunk(id int64, info ChunkInfo) error {
	info.Release = strings.TrimSpace(info.Release)
	if info.Size < 0 {
		return fmt.Errorf("chunk %d: negative size %d", id, info.Size)
	}
	if info.Offset < 0 {
		return fmt.Errorf("chunk %d: negative offset %d", id, info.Offset)
	}
	if info.AssetOffset < 0 {
		return fmt.Errorf("chunk %d: negative asset offset %d", id, info.AssetOffset)
	}
	if info.AssetID < 0 {
		return fmt.Errorf("chunk %d: negative asset id %d", id, info.AssetID)
	}
	old, existed := m.chunks[id]
	m.chunks[id] = info
	sizeApplySection(&m.ensureDerived().sections[secChunks], m.chunks, id, putTransition(old, existed, info))
	if existed && old.Release != info.Release {
		m.countAsset(old.Release, -1)
	}
	if !existed || old.Release != info.Release {
		m.countAsset(info.Release, +1)
	}
	m.recordChunkPut(id, existed)
	return nil
}

// DeleteChunk removes a chunk record, maintaining the serialized-size cache
// incrementally (the mirror of PutChunk for tracked deletions).
func (m *RepoMetadata) DeleteChunk(id int64) bool {
	old, ok := m.chunks[id]
	if !ok {
		return false
	}
	delete(m.chunks, id)
	sizeApplySection(&m.ensureDerived().sections[secChunks], m.chunks, id, removeTransition(old))
	m.countAsset(old.Release, -1)
	m.recordChunkDelete(id)
	return true
}

// PutRelease stores a release ref, maintaining the serialized-size cache
// incrementally (the release mirror of PutChunk). Direct
// `meta.releases[tag] = ref` writes bypass size accounting when they
// overwrite an existing tag (a new tag is still caught by the length
// fingerprint), so tracked writers must use this.
//
// The tag is trimmed, a zero CreatedAt is rejected (Normalize treats it as
// non-canonical), and a negative AssetCount is rejected: refs are
// authoritative counts, not deltas.
func (m *RepoMetadata) PutRelease(tag string, ref ReleaseRef) error {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return fmt.Errorf("release tag is required")
	}
	if ref.CreatedAt == 0 {
		return fmt.Errorf("release %q: CreatedAt is required (epoch zero is not a timestamp)", tag)
	}
	if ref.AssetCount < 0 {
		return fmt.Errorf("release %q: negative asset count %d", tag, ref.AssetCount)
	}
	old, existed := m.releases[tag]
	if d := m.derived; d != nil {
		// Fold in counts that arrived while the tag had no ref, exactly
		// like EnsureRelease; then the stored ref is authoritative.
		// The sum must stay non-negative: a negative pending (more removals
		// than puts while unreferenced) means the stored snapshot and the
		// chunk walk already diverged, and persisting a negative count
		// would only trip Validate later with less context.
		ref.AssetCount += d.pendingAssets[tag]
		delete(d.pendingAssets, tag)
		if ref.AssetCount < 0 {
			return fmt.Errorf("release %q: asset count went negative (%d) after draining pending", tag, ref.AssetCount)
		}
	}
	m.releases[tag] = ref
	sizeApplySection(&m.ensureDerived().sections[secReleases], m.releases, tag, putTransition(old, existed, ref))
	m.recordReleasePut(tag, old, existed)
	return nil
}

// AllFiles returns deep copies of every file entry sorted by name.
func (m *RepoMetadata) AllFiles() []FileMeta {
	names := make([]string, 0, len(m.files))
	for name := range m.files {
		names = append(names, name)
	}
	sort.Strings(names)
	files := make([]FileMeta, len(names))
	for i, name := range names {
		files[i] = m.files[name].Clone()
	}
	return files
}

// FileChunks resolves a file's chunk IDs to chunk records in stored order
// (which Normalize keeps sorted by data offset).
func (m *RepoMetadata) FileChunks(path string) []ChunkInfo {
	file, ok := m.files[normalizeStoredPath(path)]
	if !ok {
		return nil
	}
	chunks := make([]ChunkInfo, 0, len(file.Chunks))
	for _, id := range file.Chunks {
		if c, ok := m.chunks[id]; ok {
			chunks = append(chunks, c)
		}
	}
	return chunks
}

// DirNLink returns the POSIX directory link count: 2 plus the number of
// immediate subdirectories.
func (m *RepoMetadata) DirNLink(path string) int {
	path = normalizeStoredPath(path)
	if path != "" {
		if _, ok := m.dirs[path]; !ok {
			return 0
		}
	}
	m.ensureIndexes()
	return 2 + len(m.derived.childDirs[path])
}

// FileNLink returns the hardlink count of the file at path.
func (m *RepoMetadata) FileNLink(path string) int {
	file, ok := m.files[normalizeStoredPath(path)]
	if !ok {
		return 0
	}
	return m.NLink(file.Inode)
}

// NLink returns the hardlink count of one inode.
func (m *RepoMetadata) NLink(inode uint64) int {
	m.ensureIndexes()
	return len(m.derived.filesByInode[inode])
}

// ReplaceFile overwrites a stored file entry verbatim (no identity
// preservation). Returns false when the path holds no file entry. Use this
// for authoritative rewrites such as metadata-mutation callbacks; prefer
// UpsertFile for create/update flows where identity carries over. Single
// verbatim writer: the body is WriteFileDirect (the existed gate is the
// only difference), so index, stats, size, and recorder handling cannot
// drift between the two.
func (m *RepoMetadata) ReplaceFile(path string, file FileMeta) bool {
	if m.FindFile(path) == nil {
		return false
	}
	m.WriteFileDirect(path, file)
	return true
}
