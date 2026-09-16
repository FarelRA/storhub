package metadata

import (
	"reflect"
	"sort"
	"sync/atomic"
)

// derivedState bundles the index maps, their structural fingerprint, and the
// per-map serialized-size sections. The struct is owned by exactly one
// RepoMetadata; only its index maps may be shared (with clones), guarded by
// mapsShared. It is NOT safe for concurrent use, exactly like RepoMetadata
// itself (the storage layer serializes access via its per-project mutex);
// only mapsShared is atomic so concurrent Clones of the same tree cannot
// race.
type derivedState struct {
	idxDirty bool
	// owner is the tree that may mutate this state's index maps in place.
	// A plain value copy of a RepoMetadata (t := *m, not Clone) would share
	// the state pointer WITHOUT marking anything; that pattern is now a vet
	// copylocks error (noCopy), and this guard remains as runtime defense:
	// when the state's owner is
	// not the tree mutating it, the mutation replaces the state with a
	// private dirty one instead of writing into maps the original tree
	// still reads. owner is nil for states handed to a Clone (whose final
	// address the builder cannot know).
	owner        *RepoMetadata
	mapsShared   atomic.Bool
	filesByInode map[uint64][]string
	childDirs    map[string][]string
	childFiles   map[string][]string
	// Fingerprint of the flat maps the indexes were last consistent with.
	// The refs pin the map headers so a pointer match cannot alias a
	// reallocated (GC'd) map.
	dirsRef  map[string]DirMeta
	dirsLen  int
	filesRef map[string]FileMeta
	filesLen int

	// sections caches the JSON byte contribution of each stored map so
	// SerializedSize can answer without marshalling the whole tree.
	sections [4]sectionSize
}

// invalidateIndexes marks the derived indexes stale without rebuilding them;
// the next index-dependent read pays one rebuild.
func (m *RepoMetadata) invalidateIndexes() {
	m.ensureDerived().idxDirty = true
}

// ensureDerived returns the tree's derived state, creating a dirty one when
// the tree never had any. A state reached through a plain value copy (its
// owner is not this tree) is replaced by a private dirty one first: every
// write path (index maintenance, size deltas, invalidation) goes through
// here, so no tree ever mutates a state another tree owns. Read paths check
// the fingerprint directly and never call this.
func (m *RepoMetadata) ensureDerived() *derivedState {
	d := m.derived
	if d == nil {
		m.derived = &derivedState{idxDirty: true, owner: m}
		return m.derived
	}
	if d.owner != m {
		m.derived = &derivedState{idxDirty: true, owner: m, sections: d.sections}
		return m.derived
	}
	return d
}

// indexForIncremental returns the derived state when an O(log) list update
// can keep it exact (clean and with maps exclusively owned by this state),
// or nil when the index is stale or shares its maps with a Clone - in the
// latter case the state is marked dirty (and the shared maps dropped) so
// the next read rebuilds a private set.
func (m *RepoMetadata) indexForIncremental() *derivedState {
	d := m.ensureDerived()
	if d.idxDirty {
		return nil
	}
	if d.mapsShared.Load() {
		d.idxDirty = true
		d.filesByInode = nil
		d.childDirs = nil
		d.childFiles = nil
		return nil
	}
	return d
}

// syncIndexFingerprint records the flat-map identity the indexes currently
// reflect after a successful incremental update.
func (m *RepoMetadata) syncIndexFingerprint(d *derivedState) {
	d.dirsRef = m.dirs
	d.dirsLen = len(m.dirs)
	d.filesRef = m.files
	d.filesLen = len(m.files)
}

// indexFresh reports whether the derived indexes currently reflect m.dirs
// and m.files. Besides the dirty flag it checks a structural fingerprint:
// map identity (pinned by the stored ref, so a pointer match cannot alias a
// freed map) and length. The maps are unexported, so nothing outside this
// package can write them; the fingerprint is defense in depth for in-package
// bulk paths (wholesale swaps, load/migrate construction). In-place value
// writes cannot change the index keys, and inode-changing value writes must
// go through the tracked mutators.
func (m *RepoMetadata) indexFresh() bool {
	d := m.derived
	return d != nil && !d.idxDirty &&
		sameMap(d.dirsRef, m.dirs) && d.dirsLen == len(m.dirs) &&
		sameMap(d.filesRef, m.files) && d.filesLen == len(m.files)
}

// ensureIndexes rebuilds the derived indexes only when they are stale.
func (m *RepoMetadata) ensureIndexes() {
	if !m.indexFresh() {
		m.RebuildIndexes()
	}
}

// RebuildIndexes performs a FULL rebuild of the derived indexes. Incremental
// maintenance keeps the indexes fresh across tracked mutations, so this is
// only needed after untracked structural writes or an explicit
// invalidation; it stays exported because load/repair paths call it.
func (m *RepoMetadata) RebuildIndexes() {
	d := &derivedState{owner: m}
	if m.derived != nil {
		d.sections = m.derived.sections
	}
	d.filesByInode = make(map[uint64][]string, len(m.files))
	d.childDirs = make(map[string][]string, len(m.dirs)+1)
	d.childFiles = make(map[string][]string, len(m.files)+1)

	for path := range m.dirs {
		parent := parentPath(path)
		d.childDirs[parent] = append(d.childDirs[parent], path)
	}
	for path, file := range m.files {
		d.filesByInode[file.Inode] = append(d.filesByInode[file.Inode], path)
		parent := parentPath(path)
		d.childFiles[parent] = append(d.childFiles[parent], path)
	}
	for parent := range d.childDirs {
		stableSortStrings(d.childDirs[parent])
	}
	for parent := range d.childFiles {
		stableSortStrings(d.childFiles[parent])
	}
	// Sort the inode families too: incremental maintenance keeps them in
	// path order (binary insert), so the full rebuild must match exactly,
	// not just as a set.
	for ino := range d.filesByInode {
		stableSortStrings(d.filesByInode[ino])
	}

	d.idxDirty = false
	m.syncIndexFingerprint(d)
	m.derived = d
}

// --- incremental index maintenance -------------------------------------

// trackDirPut maintains the childDirs index and the dirs size section after
// m.dirs[path] was set to cur (hadOld reports whether an entry was
// replaced). Call AFTER the map write.
func (m *RepoMetadata) trackDirPut(path string, old DirMeta, hadOld bool, cur DirMeta) {
	m.sizePutDir(path, old, hadOld, cur)
	if d := m.indexForIncremental(); d != nil && !hadOld {
		parent := parentPath(path)
		d.childDirs[parent] = insertSortedString(d.childDirs[parent], path)
		m.syncIndexFingerprint(d)
	}
}

// trackDirRemove maintains the indexes after m.dirs[path] was deleted
// (old is the removed value). Call AFTER the map delete.
func (m *RepoMetadata) trackDirRemove(path string, old DirMeta) {
	m.sizeRemoveDir(path, old)
	if d := m.indexForIncremental(); d != nil {
		parent := parentPath(path)
		list := removeSortedString(d.childDirs[parent], path)
		if len(list) == 0 {
			delete(d.childDirs, parent)
		} else {
			d.childDirs[parent] = list
		}
		m.syncIndexFingerprint(d)
	}
}

// trackFilePut maintains the filesByInode/childFiles indexes and the files
// size section after m.files[name] was set to cur (hadOld reports whether
// an entry was replaced). Call AFTER the map write.
func (m *RepoMetadata) trackFilePut(name string, old FileMeta, hadOld bool, cur FileMeta) {
	m.sizePutFile(name, old, hadOld, cur)
	d := m.indexForIncremental()
	if d == nil {
		return
	}
	if !hadOld {
		parent := parentPath(name)
		d.childFiles[parent] = insertSortedString(d.childFiles[parent], name)
	} else if old.Inode != cur.Inode {
		list := removeSortedString(d.filesByInode[old.Inode], name)
		if len(list) == 0 {
			delete(d.filesByInode, old.Inode)
		} else {
			d.filesByInode[old.Inode] = list
		}
	}
	d.filesByInode[cur.Inode] = insertSortedString(d.filesByInode[cur.Inode], name)
	m.syncIndexFingerprint(d)
}

// trackFileRemove maintains the indexes after m.files[name] was deleted
// (old is the removed value). Call AFTER the map delete.
func (m *RepoMetadata) trackFileRemove(name string, old FileMeta) {
	m.sizeRemoveFile(name, old)
	if d := m.indexForIncremental(); d != nil {
		parent := parentPath(name)
		list := removeSortedString(d.childFiles[parent], name)
		if len(list) == 0 {
			delete(d.childFiles, parent)
		} else {
			d.childFiles[parent] = list
		}
		family := removeSortedString(d.filesByInode[old.Inode], name)
		if len(family) == 0 {
			delete(d.filesByInode, old.Inode)
		} else {
			d.filesByInode[old.Inode] = family
		}
		m.syncIndexFingerprint(d)
	}
}

// insertSortedString adds name to a sorted child list in place (via one
// shift), keeping the exact ordering a full RebuildIndexes produces.
func insertSortedString(list []string, name string) []string {
	i := sort.SearchStrings(list, name)
	if i < len(list) && list[i] == name {
		return list
	}
	list = append(list, "")
	copy(list[i+1:], list[i:])
	list[i] = name
	return list
}

// removeSortedString drops name from a sorted child list, returning the
// (possibly re-sliced) remainder.
func removeSortedString(list []string, name string) []string {
	i := sort.SearchStrings(list, name)
	if i >= len(list) || list[i] != name {
		return list
	}
	return append(list[:i], list[i+1:]...)
}

// sameMap reports whether both maps are nil or are the very same map. The
// caller pins the recorded map (the fingerprint holds a reference), so an
// equal header pointer cannot alias a reallocated map.
func sameMap[K comparable, V any](a, b map[K]V) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

func (m *RepoMetadata) markSectionStale(i int) {
	m.ensureDerived().sections[i].ok = false
}

func stableSortStrings(strs []string) {
	sort.SliceStable(strs, func(i, j int) bool { return strs[i] < strs[j] })
}
