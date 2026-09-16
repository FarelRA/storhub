package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ChunkInfo struct {
	Size        int64  `json:"s"`
	Offset      int64  `json:"o,omitempty"`
	Release     string `json:"r"`
	AssetOffset int64  `json:"ao,omitempty"`
	AssetID     int64  `json:"a"`
}

// XAttrMap holds extended attributes as raw bytes. JSON values are base64
type XAttrMap map[string][]byte

func (x XAttrMap) Clone() XAttrMap {
	if len(x) == 0 {
		return nil
	}
	dst := make(XAttrMap, len(x))
	for k, v := range x {
		dst[k] = append([]byte(nil), v...)
	}
	return dst
}

type FileMeta struct {
	// Normalize materializes Chunks as an empty (non-nil) slice for every
	// regular file and symlink - it is NEVER nil after normalization, and
	// empty files carry zero chunks since the sentinel-part removal.
	// Compare with len(), never DeepEqual against nil or []int64{}.
	Chunks     []int64 `json:"cs,omitempty"`
	Size       int64   `json:"s"`
	Symlink    string  `json:"sl,omitempty"`
	UploadedAt int64   `json:"ua"`
	ModifiedAt int64   `json:"ma,omitempty"`
	AccessedAt int64   `json:"aa,omitempty"`
	ChangedAt  int64   `json:"ch,omitempty"`
	Mode       uint32  `json:"md,omitempty"`
	UID        uint32  `json:"u,omitempty"`
	GID        uint32  `json:"g,omitempty"`
	Inode      uint64  `json:"i,omitempty"`
	// XAttrs is nil-means-empty: normalizeXAttrs collapses empty maps to
	// nil so serialized metadata omits the field.
	XAttrs XAttrMap `json:"x,omitempty"`
}

func (f FileMeta) Clone() FileMeta {
	clone := f
	if f.Chunks != nil {
		clone.Chunks = append([]int64(nil), f.Chunks...)
	}
	clone.XAttrs = f.XAttrs.Clone()
	return clone
}

type DirMeta struct {
	CreatedAt  int64    `json:"cr"`
	ModifiedAt int64    `json:"ma"`
	AccessedAt int64    `json:"aa,omitempty"`
	ChangedAt  int64    `json:"ch,omitempty"`
	Mode       uint32   `json:"m,omitempty"`
	UID        uint32   `json:"u,omitempty"`
	GID        uint32   `json:"g,omitempty"`
	Inode      uint64   `json:"i,omitempty"`
	XAttrs     XAttrMap `json:"x,omitempty"`
}

func (d DirMeta) Clone() DirMeta {
	clone := d
	clone.XAttrs = d.XAttrs.Clone()
	return clone
}

type ReleaseRef struct {
	AssetCount int   `json:"ac"`
	CreatedAt  int64 `json:"cr"`
}

func (r ReleaseRef) Clone() ReleaseRef {
	return r
}

type RepoMetadata struct {
	Version    int    `json:"v"`
	Project    string `json:"p"`
	TotalFiles int    `json:"tf"`
	TotalSize  int64  `json:"ts"`
	LastMod    int64  `json:"lm"`
	Root       DirMeta
	// dirs/files/chunks/releases are the stored maps. They are unexported so
	// every mutation flows through the tracked mutators, which maintain the
	// derived index and the serialized-size cache; the blob wire format is
	// pinned explicitly by repoMetadataJSON (encoding/json ignores
	// unexported fields, so these tags would be dead metadata).
	dirs     map[string]DirMeta
	files    map[string]FileMeta
	chunks   map[int64]ChunkInfo
	releases map[string]ReleaseRef

	// NextInode/NextChunkID are persisted so deleting the highest-numbered
	// entry cannot silently reuse identifiers across reloads.
	NextInode   uint64 `json:"ni,omitempty"`
	NextChunkID int64  `json:"nc,omitempty"`

	// derived carries the lazily maintained indexes (filesByInode,
	// childDirs, childFiles) and the incremental serialized-size cache.
	// The state itself is per-tree (its fingerprint must pin THIS tree's
	// maps), but a clean set of index MAPS is shared with Clones: Clone
	// marks the maps shared and the clone's state references them, so
	// snapshot reads never rebuild. The first tracked mutation on either
	// side sees the shared flag and drops to a dirty state instead of
	// writing into the shared maps.
	derived *derivedState `json:"-"`

	// noCopy makes struct copies (t := *m) a `go vet` copylocks error. A
	// copy would alias the derived state pointer without the Clone-time
	// sharing handshake; the runtime owner guard still defends against it,
	// but the hazard should be caught before it ships. Clone is the only
	// sanctioned copy path.
	noCopy noCopy
}

// noCopy implements the sync.WaitGroup pattern: embedding a type with
// pointer-receiver Lock/Unlock methods makes `go vet`'s copylocks check
// flag every copy of the enclosing struct.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

type MetadataRevision struct {
	CommitSHA   string `json:"commit_sha"`
	Message     string `json:"message"`
	CommittedAt int64  `json:"committed_at"`
}

func NewRepoMetadata(project string) *RepoMetadata {
	now := time.Now().Unix()
	uid, gid := defaultOwnerIDs()
	return &RepoMetadata{
		Version:     maxMetadataVersion,
		Project:     project,
		NextInode:   2,
		NextChunkID: 1,
		Root: DirMeta{
			Inode: 1, Mode: defaultDirMode(), UID: uid, GID: gid,
			CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now,
		},
		dirs:     make(map[string]DirMeta),
		files:    make(map[string]FileMeta),
		chunks:   make(map[int64]ChunkInfo),
		releases: make(map[string]ReleaseRef),
	}
}

// Clone produces an independent snapshot of the tree. The four stored maps
// are copied (so entry insert/remove/replace on either side is invisible to
// the other), but the ENTRY VALUES are shared: a stored FileMeta/DirMeta is
// treated as immutable once written - every in-package mutation replaces the
// map entry with a fresh value (UpsertFile clones its input; Normalize and
// the identity helpers never mutate a stored Chunks backing array or XAttrs
// map in place). Callers that want to mutate an entry obtained from a map
// must FileMeta.Clone/DirMeta.Clone it first, which every existing caller
// already does. Only Root is deep-copied (it is a single value, and callers
// mutate its XAttrs through the returned pointer).
//
// A clean derived index is SHARED (the read-only maps are referenced, not
// rebuilt) with the source: reads on the snapshot (DirectoryChildren,
// NLink, ...) hit the shared index with zero rebuild, and the first tracked
// mutation on either side drops to a dirty state instead of writing into
// shared maps. This is what makes a per-operation snapshot cheap.
// Dirs returns the stored directory map. READ-ONLY: this is the live backing
// store, so a write through it would silently corrupt the derived index and
// the serialized-size cache. Mutate through EnsureDirectory, WriteDirDirect,
// RemoveDirectory.
func (m *RepoMetadata) Dirs() map[string]DirMeta { return m.dirs }

// Files returns the stored file map. READ-ONLY: see Dirs. Mutate through
// UpsertFile, WriteFileDirect, ReplaceFile, RemoveFile.
func (m *RepoMetadata) Files() map[string]FileMeta { return m.files }

// Chunks returns the stored chunk map. READ-ONLY: see Dirs. Mutate through
// PutChunk / DeleteChunk.
func (m *RepoMetadata) Chunks() map[int64]ChunkInfo { return m.chunks }

// Releases returns the stored release map. READ-ONLY: see Dirs. Mutate
// through EnsureRelease, PutRelease, RemoveRelease.
func (m *RepoMetadata) Releases() map[string]ReleaseRef { return m.releases }

// Chunk returns one chunk record and whether it exists.
func (m *RepoMetadata) Chunk(id int64) (ChunkInfo, bool) {
	c, ok := m.chunks[id]
	return c, ok
}

func (m *RepoMetadata) Clone() *RepoMetadata {
	// Explicit construction, not a struct copy: RepoMetadata embeds noCopy,
	// so copying it is a vet error. Clone is the one sanctioned copy path.
	clone := &RepoMetadata{
		Version:     m.Version,
		Project:     m.Project,
		TotalFiles:  m.TotalFiles,
		TotalSize:   m.TotalSize,
		LastMod:     m.LastMod,
		Root:        m.Root.Clone(),
		dirs:        cloneDirMetaMap(m.dirs),
		files:       cloneFileMetaMap(m.files),
		chunks:      cloneChunkInfoMap(m.chunks),
		releases:    cloneReleaseRefMap(m.releases),
		NextInode:   m.NextInode,
		NextChunkID: m.NextChunkID,
	}
	if d := m.derived; d != nil {
		cd := &derivedState{
			idxDirty: d.idxDirty,
			sections: d.sections,
		}
		if !d.idxDirty {
			// Publish the shared maps read-only: mark them shared on BOTH
			// sides (so neither ever mutates them in place again) and
			// reference them.
			d.mapsShared.Store(true)
			cd.mapsShared.Store(true)
			cd.filesByInode = d.filesByInode
			cd.childDirs = d.childDirs
			cd.childFiles = d.childFiles
		}
		cd.dirsRef, cd.dirsLen = clone.dirs, len(clone.dirs)
		cd.filesRef, cd.filesLen = clone.files, len(clone.files)
		clone.derived = cd
	}
	return clone
}

// cloneDirMetaMap copies the map; DirMeta values are copied by struct and
// share their (immutable-by-contract) XAttrs maps.
func cloneDirMetaMap(src map[string]DirMeta) map[string]DirMeta {
	if src == nil {
		return nil
	}
	dst := make(map[string]DirMeta, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// cloneFileMetaMap copies the map; FileMeta values are copied by struct and
// share their (immutable-by-contract) Chunks backing array and XAttrs map.
func cloneFileMetaMap(src map[string]FileMeta) map[string]FileMeta {
	if src == nil {
		return nil
	}
	dst := make(map[string]FileMeta, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneChunkInfoMap(src map[int64]ChunkInfo) map[int64]ChunkInfo {
	if src == nil {
		return nil
	}
	dst := make(map[int64]ChunkInfo, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneReleaseRefMap(src map[string]ReleaseRef) map[string]ReleaseRef {
	if src == nil {
		return nil
	}
	dst := make(map[string]ReleaseRef, len(src))
	for k, v := range src {
		dst[k] = v.Clone()
	}
	return dst
}

// maxMetadataVersion is the newest DOCUMENT version this build reads and
// writes. Versions 1..maxBlobVersion are single-blob layouts (the whole index
// in one metadata.json, entry shapes evolving); version 5 is the split layout
// (a manifest plus content-addressed Merkle objects). The split is therefore
// just the next step on the ONE version axis, not a parallel numbering.
//
// A RepoMetadata.Version records the document version its tree corresponds to:
// 5 when loaded from a manifest or newly created (the split layout), or <=4
// when loaded from a legacy single-blob document (it migrates to 5 on its next
// write). The entry SHAPE is identical at 4 and 5; only the on-disk layout
// differs, so no pure migrator crosses the 4->5 boundary (that step is the
// write-time split, in the storage layer).
const maxMetadataVersion = 5

// maxBlobVersion is the newest single-blob schema: what Migrate upgrades legacy
// blobs to, and the version a tree loaded from a metadata.json carries until it
// is written as a split (version 5) document.
const maxBlobVersion = 4

// IsSplit reports whether this tree corresponds to the split (version-5)
// layout: a manifest plus content-addressed objects. A tree loaded from a
// legacy single-blob document reports false until it is migrated on write.
func (m *RepoMetadata) IsSplit() bool { return m.Version >= maxMetadataVersion }

// MarkSplit records that this tree is (or will be) stored in the split
// (version-5) layout. The write path calls it before publishing, so a legacy
// tree migrates on its first commit.
func (m *RepoMetadata) MarkSplit() { m.Version = maxMetadataVersion }

// xattrMapFromStrings converts legacy string-valued xattrs from v1/v2
// payloads into the v3 byte representation.
func xattrMapFromStrings(src map[string]string) XAttrMap {
	if len(src) == 0 {
		return nil
	}
	dst := make(XAttrMap, len(src))
	for k, v := range src {
		dst[k] = []byte(v)
	}
	return dst
}

func (m *RepoMetadata) Normalize(project string, now int64) {
	// Version is preserved (it records the document/layout the tree came from
	// or will become), never forced here: a legacy blob stays maxBlobVersion
	// until written as a split document, a split/new tree is maxMetadataVersion.
	m.Project = chooseNonEmpty(m.Project, project)
	m.normalizeRoot(now)
	if m.dirs == nil {
		m.dirs = make(map[string]DirMeta)
	}
	if m.files == nil {
		m.files = make(map[string]FileMeta)
	}
	if m.chunks == nil {
		m.chunks = make(map[int64]ChunkInfo)
	}
	if m.releases == nil {
		m.releases = make(map[string]ReleaseRef)
	}
	// Entry loops write back ONLY when normalization actually changed the
	// value: an already-normalized tree (the common per-transaction case)
	// then leaves the derived indexes and the size cache untouched, so no
	// rebuild or re-marshalling happens here.
	for path, dir := range m.dirs {
		original := dir
		dir.Normalize(now)
		if !dirMetaEqual(original, dir) {
			m.dirs[path] = dir
			m.sizePutDir(path, original, true, dir)
		}
	}
	for path, file := range m.files {
		original := file
		file.Normalize(now)
		if !fileMetaEqual(original, file) {
			m.files[path] = file
			m.sizePutFile(path, original, true, file)
		}
	}
	m.sortFileChunksByOffset()
	for tag, ref := range m.releases {
		if ref.CreatedAt == 0 {
			original := ref
			ref.CreatedAt = now
			m.releases[tag] = ref
			m.sizePutRelease(tag, original, true, ref)
		}
	}
	m.RecomputeStats()
	if m.LastMod == 0 {
		m.LastMod = now
	}
	// RecomputeStats already rebuilt the indexes; a second full rebuild
	// here was pure duplicated O((F+D) log) work.
}

func (m *RepoMetadata) RecomputeStats() {
	totalFiles := 0
	totalSize := int64(0)
	assetCounts := make(map[string]int)

	for _, chunk := range m.chunks {
		if chunk.Release != "" {
			assetCounts[chunk.Release]++
		}
	}
	for _, file := range m.files {
		if file.Symlink == "" {
			totalFiles++
			totalSize += file.Size
		}
	}

	for tag := range m.releases {
		ref := m.releases[tag]
		if ref.AssetCount != assetCounts[tag] {
			original := ref
			ref.AssetCount = assetCounts[tag]
			m.releases[tag] = ref
			m.sizePutRelease(tag, original, true, ref)
		}
	}

	m.TotalFiles = totalFiles
	m.TotalSize = totalSize
	if m.Version == 0 {
		m.Version = maxMetadataVersion
	}
	m.RebuildIndexes()
}

// PruneUnreferencedChunks drops chunk records that no file references
// anymore. Overwrites and deletions otherwise leave stale entries behind,
// and the catalog grows monotonically until metadata hits the size ceiling
// and every subsequent commit fails permanently. Callers must only prune at
// points where no retained history still needs the records (storhub prunes
// immediately before squashing git history, after PurgeUntracked has
// reclaimed the corresponding remote assets) - a rollback to an older
// revision restores its own chunk catalog wholesale.
func (m *RepoMetadata) PruneUnreferencedChunks() int {
	referenced := make(map[int64]struct{})
	for _, file := range m.files {
		for _, id := range file.Chunks {
			referenced[id] = struct{}{}
		}
	}
	removed := 0
	for id := range m.chunks {
		if _, ok := referenced[id]; !ok {
			delete(m.chunks, id)
			removed++
		}
	}
	if removed > 0 {
		m.markSectionStale(secChunks)
	}
	return removed
}

func (d *DirMeta) Normalize(now int64) {
	if d.Mode == 0 {
		d.Mode = defaultDirMode()
	}
	// Timestamps are NOT repaired here: the v4 contract is complete,
	// authoritative values (the stacked migrator completes legacy docs;
	// creation paths stamp real times). Zeros are real epoch values.
	_ = now
	d.XAttrs = normalizeXAttrs(d.XAttrs)
}

func (f *FileMeta) Normalize(now int64) {
	if f.Mode == 0 {
		f.Mode = defaultFileMode(nodeKindOf(f))
	}
	// See DirMeta.Normalize: zeros are authoritative epoch values, never
	// gaps to repair here.
	if f.Chunks == nil {
		f.Chunks = make([]int64, 0)
	}
	// Stored chunk order is by data offset, a RepoMetadata-level invariant
	// enforced by sortFileChunksByOffset; sorting by id here would break it
	// for standalone callers.
	if f.Symlink != "" {
		f.Chunks = make([]int64, 0)
		f.Size = int64(len([]byte(f.Symlink)))
	}
	f.XAttrs = normalizeXAttrs(f.XAttrs)
}

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

func (m *RepoMetadata) HasDirectory(path string) bool {
	path = normalizeStoredPath(path)
	if path == "" {
		return true
	}
	_, ok := m.dirs[path]
	return ok
}

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
	m.dirs[path] = dir
	m.trackDirPut(path, DirMeta{}, false, dir)
}

func (m *RepoMetadata) RemoveDirectory(path string) bool {
	path = normalizeStoredPath(path)
	old, ok := m.dirs[path]
	if !ok {
		return false
	}
	delete(m.dirs, path)
	m.trackDirRemove(path, old)
	return true
}

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

func (m *RepoMetadata) EnsureRelease(tag string, createdAt int64) *ReleaseRef {
	// Snapshot footgun (same as FindFile/GetRelease): the returned pointer
	// targets a copy, so mutating its fields never reaches stored state.
	// The pointer is a read convenience only; write through this method or a
	// transaction.
	if ref, ok := m.releases[tag]; ok {
		return &ref
	}
	m.releases[tag] = ReleaseRef{CreatedAt: createdAt}
	ref := m.releases[tag]
	m.sizePutRelease(tag, ReleaseRef{}, false, ref)
	return &ref
}

func (m *RepoMetadata) UpsertFile(name string, file FileMeta, createdAt int64) {
	name = normalizeStoredPath(name)
	// Clone the caller's value so later mutations of its slices cannot
	// alias into stored metadata.
	file = file.Clone()
	if parent := parentPath(name); parent != "" {
		m.EnsureDirectory(parent, createdAt)
	}
	existing, existed := m.files[name]
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
	m.files[name] = file
	m.trackFilePut(name, existing, existed, file)
}

// FindFile returns a SNAPSHOT of the entry: the pointer targets a copy of
// the struct, so FIELD-level mutations never reach stored state. (Slices
// and maps inside - Chunks, XAttrs - still share backing storage; clone
// before mutating those.) Apply changes through UpsertFile or an
// UpdateRepoMetadataContext transaction.
func (m *RepoMetadata) FindFile(name string) *FileMeta {
	name = normalizeStoredPath(name)
	if file, ok := m.files[name]; ok {
		return &file
	}
	return nil
}

// SetFileAtime updates atime in place. FindFile returns a pointer to a
// copy (map values are not addressable), so mutating its result silently
// drops the write — use this setter for mutations.
func (m *RepoMetadata) SetFileAtime(name string, atime int64) bool {
	name = normalizeStoredPath(name)
	if file, ok := m.files[name]; ok {
		original := file
		file.AccessedAt = atime
		m.files[name] = file
		m.sizePutFile(name, original, true, file)
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
		m.sizePutDir(path, original, true, dir)
		return true
	}
	return false
}

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
func (m *RepoMetadata) WriteFileDirect(name string, file FileMeta) {
	name = normalizeStoredPath(name)
	existing, existed := m.files[name]
	m.files[name] = file
	m.trackFilePut(name, existing, existed, file)
}

// WriteDirDirect stores a directory entry verbatim, mirroring
// WriteFileDirect for op replay and family updates that must preserve
// every field exactly.
func (m *RepoMetadata) WriteDirDirect(path string, dir DirMeta) {
	path = normalizeStoredPath(path)
	existing, existed := m.dirs[path]
	m.dirs[path] = dir
	m.trackDirPut(path, existing, existed, dir)
}

func (m *RepoMetadata) RemoveFile(name string) bool {
	name = normalizeStoredPath(name)
	old, ok := m.files[name]
	if !ok {
		return false
	}
	delete(m.files, name)
	m.trackFileRemove(name, old)
	return true
}

// AllocateInode returns the next inode number. It has no hidden side effects;
// callers normalize metadata at commit/load boundaries.
func (m *RepoMetadata) AllocateInode() uint64 {
	return m.allocateInode()
}

// AllocateChunkID returns the next chunk identifier.
func (m *RepoMetadata) AllocateChunkID() int64 {
	return m.allocateChunkID()
}

// InitializeNewFileIdentity materializes a complete identity (inode, mode,
// owner, timestamps) for a newly created file entry.
func InitializeNewFileIdentity(meta *RepoMetadata, file *FileMeta, now int64) {
	initializeNewFileIdentity(meta, file, now)
}

// InitializeNewFileIdentityFields applies creation defaults (mode, owner,
// timestamps) without minting an inode. Callers that later create the node
// against the authoritative metadata must let InitializeNewFileIdentity
// allocate there, so the inode counter bumps exactly once.
func InitializeNewFileIdentityFields(file *FileMeta, now int64) {
	initializeNewFileIdentityFields(file, now)
}

// PreserveFileIdentity carries the existing node's stable identity onto an
// updated entry. A type change (regular file <-> symlink) carries nothing.
func PreserveFileIdentity(file *FileMeta, existing *FileMeta, now int64) {
	preserveFileIdentity(file, existing, now)
}

// ParseNumericReleaseTag extracts the numeric part of a "v<N>" release tag.
func ParseNumericReleaseTag(tag string) (int, bool) {
	return parseNumericReleaseTag(tag)
}

func (m *RepoMetadata) RemoveRelease(tag string) bool {
	old, ok := m.releases[tag]
	if !ok {
		return false
	}
	delete(m.releases, tag)
	// Releases are not part of the derived indexes; only the size cache
	// needs the removal.
	m.sizeRemoveRelease(tag, old)
	return true
}

// PutChunk stores a chunk record verbatim, maintaining the serialized-size
// cache incrementally. Direct `meta.chunks[id] = info` writes bypass the
// size accounting when they overwrite an existing id (a new id is still
// caught by the length fingerprint), so tracked writers should use this.
// The stored ChunkInfo is a pure value type; no cloning is needed.
func (m *RepoMetadata) PutChunk(id int64, info ChunkInfo) {
	old, existed := m.chunks[id]
	m.chunks[id] = info
	sizeApplySection(&m.ensureDerived().sections[secChunks], m.chunks, id, old, existed, info, true)
}

// DeleteChunk removes a chunk record, maintaining the serialized-size cache
// incrementally (the mirror of PutChunk for tracked deletions).
func (m *RepoMetadata) DeleteChunk(id int64) bool {
	old, ok := m.chunks[id]
	if !ok {
		return false
	}
	delete(m.chunks, id)
	sizeApplySection(&m.ensureDerived().sections[secChunks], m.chunks, id, old, true, ChunkInfo{}, false)
	return true
}

// PutRelease stores a release ref verbatim, maintaining the serialized-size
// cache incrementally (the release mirror of PutChunk). Direct
// `meta.releases[tag] = ref` writes bypass size accounting when they
// overwrite an existing tag (a new tag is still caught by the length
// fingerprint), so tracked writers must use this.
func (m *RepoMetadata) PutRelease(tag string, ref ReleaseRef) {
	old, existed := m.releases[tag]
	m.releases[tag] = ref
	sizeApplySection(&m.ensureDerived().sections[secReleases], m.releases, tag, old, existed, ref, true)
}

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
func (m *RepoMetadata) FileChunks(name string) []ChunkInfo {
	file, ok := m.files[normalizeStoredPath(name)]
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

func (m *RepoMetadata) FileNLink(name string) int {
	file, ok := m.files[normalizeStoredPath(name)]
	if !ok {
		return 0
	}
	return m.NLink(file.Inode)
}

func (m *RepoMetadata) NLink(inode uint64) int {
	m.ensureIndexes()
	return len(m.derived.filesByInode[inode])
}

func cloneStrings(src []string) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

// --- value equality helpers ----------------------------------------------

// xAttrsEqual is strict about nil vs empty so callers that write back
// normalized values cannot skip the nil-collapse Normalize guarantees.
func xAttrsEqual(a, b XAttrMap) bool {
	if a == nil || b == nil {
		return a == nil && b == nil && len(a) == len(b)
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok || !bytes.Equal(v, bv) {
			return false
		}
	}
	return true
}

func dirMetaEqual(a, b DirMeta) bool {
	return a.CreatedAt == b.CreatedAt && a.ModifiedAt == b.ModifiedAt &&
		a.AccessedAt == b.AccessedAt && a.ChangedAt == b.ChangedAt &&
		a.Mode == b.Mode && a.UID == b.UID && a.GID == b.GID &&
		a.Inode == b.Inode && xAttrsEqual(a.XAttrs, b.XAttrs)
}

func fileMetaEqual(a, b FileMeta) bool {
	if a.Size != b.Size || a.Symlink != b.Symlink || a.UploadedAt != b.UploadedAt ||
		a.ModifiedAt != b.ModifiedAt || a.AccessedAt != b.AccessedAt ||
		a.ChangedAt != b.ChangedAt || a.Mode != b.Mode || a.UID != b.UID ||
		a.GID != b.GID || a.Inode != b.Inode {
		return false
	}
	if (a.Chunks == nil) != (b.Chunks == nil) {
		return false
	}
	if !slices.Equal(a.Chunks, b.Chunks) {
		return false
	}
	return xAttrsEqual(a.XAttrs, b.XAttrs)
}

func preserveFileIdentity(file *FileMeta, existing *FileMeta, now int64) {
	// A type change (regular file <-> symlink) replaces the whole node rather
	// than updating it: no identity carries over. In particular the old
	// symlink target must never leak onto a regular file - Normalize treats
	// any file with a symlink target as pure link data and would silently
	// discard the freshly written content.
	if (file.Symlink == "") != (existing.Symlink == "") {
		return
	}
	if file.Inode == 0 {
		file.Inode = existing.Inode
	}
	if file.Mode == 0 {
		file.Mode = existing.Mode
	}
	if file.UID == 0 {
		file.UID = existing.UID
	}
	if file.GID == 0 {
		file.GID = existing.GID
	}
	if file.UploadedAt == 0 {
		file.UploadedAt = chooseNonZeroTime(existing.UploadedAt, now)
	}
	if file.ModifiedAt == 0 {
		file.ModifiedAt = chooseNonZeroTime(now, existing.ModifiedAt, file.UploadedAt)
	}
	if file.AccessedAt == 0 {
		file.AccessedAt = chooseNonZeroTime(existing.AccessedAt, file.ModifiedAt)
	}
	if file.ChangedAt == 0 {
		file.ChangedAt = chooseNonZeroTime(now, existing.ChangedAt, file.ModifiedAt)
	}
	if len(file.XAttrs) == 0 && len(existing.XAttrs) > 0 {
		file.XAttrs = existing.XAttrs.Clone()
	}
}

func initializeNewFileIdentity(meta *RepoMetadata, file *FileMeta, now int64) {
	if file.Inode == 0 {
		file.Inode = meta.allocateInode()
	}
	initializeNewFileIdentityFields(file, now)
}

// initializeNewFileIdentityFields applies every creation default EXCEPT the
// inode: mode, owner, and the full timestamp set. Inode minting is reserved
// for initializeNewFileIdentity because the counter lives on exactly one
// authoritative RepoMetadata; stamping an inode against a throwaway clone
// (a readonly snapshot, a working copy) silently skips the counter bump and
// the next allocation re-issues the same inode.
func initializeNewFileIdentityFields(file *FileMeta, now int64) {
	uid, gid := defaultOwnerIDs()
	if file.Mode == 0 {
		file.Mode = defaultFileMode(nodeKindOf(file))
	}
	// Owner IDs are always materialized at creation (0 legitimately means
	// root); they are never re-stamped afterwards.
	if file.UID == 0 {
		file.UID = uid
	}
	if file.GID == 0 {
		file.GID = gid
	}
	// Fresh nodes get complete timestamps at creation. This runs ONLY on
	// the new-node path (UpdateFileFamily writes the map directly), so an
	// explicit epoch on an existing entry can never be rewritten here.
	if file.UploadedAt == 0 {
		file.UploadedAt = now
	}
	if file.ModifiedAt == 0 {
		file.ModifiedAt = file.UploadedAt
	}
	if file.AccessedAt == 0 {
		file.AccessedAt = file.ModifiedAt
	}
	if file.ChangedAt == 0 {
		file.ChangedAt = file.ModifiedAt
	}
}

func (m *RepoMetadata) normalizeRoot(now int64) {
	if m.Root.Inode == 0 {
		m.Root.Inode = 1
	}
	if m.Root.Mode == 0 {
		m.Root.Mode = defaultDirMode()
	}
	// Root timestamps are authoritative under v4 (see DirMeta.Normalize).
	m.Root.XAttrs = normalizeXAttrs(m.Root.XAttrs)
	m.reconcileCounters()
}

func (m *RepoMetadata) allocateInode() uint64 {
	ino := m.NextInode
	m.NextInode++
	return ino
}

func (m *RepoMetadata) allocateChunkID() int64 {
	id := m.NextChunkID
	m.NextChunkID++
	return id
}

func (m *RepoMetadata) migrateV1(data []byte) error {
	var v1 struct {
		Version      int       `json:"version"`
		Project      string    `json:"project"`
		NextInode    uint64    `json:"next_inode,omitempty"`
		TotalFiles   int       `json:"total_files"`
		TotalSize    int64     `json:"total_size"`
		LastModified time.Time `json:"last_modified"`
		Root         struct {
			Inode      uint64            `json:"inode"`
			Mode       uint32            `json:"mode"`
			UID        uint32            `json:"uid"`
			GID        uint32            `json:"gid"`
			NLink      uint32            `json:"nlink"`
			CreatedAt  time.Time         `json:"created_at"`
			ModifiedAt time.Time         `json:"modified_at"`
			AccessedAt time.Time         `json:"accessed_at"`
			ChangedAt  time.Time         `json:"changed_at"`
			XAttrs     map[string]string `json:"xattrs,omitempty"`
		} `json:"root"`
		Directories []struct {
			Path       string            `json:"path"`
			CreatedAt  time.Time         `json:"created_at"`
			ModifiedAt time.Time         `json:"modified_at"`
			AccessedAt time.Time         `json:"accessed_at,omitempty"`
			ChangedAt  time.Time         `json:"changed_at,omitempty"`
			Mode       uint32            `json:"mode,omitempty"`
			UID        uint32            `json:"uid,omitempty"`
			GID        uint32            `json:"gid,omitempty"`
			Inode      uint64            `json:"inode,omitempty"`
			NLink      uint32            `json:"nlink,omitempty"`
			XAttrs     map[string]string `json:"xattrs,omitempty"`
		} `json:"directories"`
		Releases []struct {
			Tag        string    `json:"tag"`
			AssetCount int       `json:"asset_count"`
			CreatedAt  time.Time `json:"created_at"`
			Files      []struct {
				Name          string            `json:"name"`
				Kind          string            `json:"kind,omitempty"`
				Size          int64             `json:"size"`
				Release       string            `json:"release"`
				UploadedAt    time.Time         `json:"uploaded_at"`
				ModifiedAt    time.Time         `json:"modified_at,omitempty"`
				AccessedAt    time.Time         `json:"accessed_at,omitempty"`
				ChangedAt     time.Time         `json:"changed_at,omitempty"`
				Mode          uint32            `json:"mode,omitempty"`
				UID           uint32            `json:"uid,omitempty"`
				GID           uint32            `json:"gid,omitempty"`
				Inode         uint64            `json:"inode,omitempty"`
				NLink         uint32            `json:"nlink,omitempty"`
				SymlinkTarget string            `json:"symlink_target,omitempty"`
				XAttrs        map[string]string `json:"xattrs,omitempty"`
				Chunks        []struct {
					Name        string `json:"name"`
					Size        int64  `json:"size"`
					Index       int    `json:"index"`
					Offset      int64  `json:"offset"`
					Release     string `json:"release"`
					AssetOffset int64  `json:"asset_offset"`
					AssetID     int64  `json:"asset_id"`
				} `json:"chunks"`
			} `json:"files"`
		} `json:"releases"`
	}
	if err := json.Unmarshal(data, &v1); err != nil {
		return fmt.Errorf("unmarshal v1 metadata: %w", err)
	}

	m.Version = maxBlobVersion
	m.Project = v1.Project
	m.TotalFiles = v1.TotalFiles
	m.TotalSize = v1.TotalSize
	m.LastMod = timeToUnix(v1.LastModified)
	// Seed the inode counter past everything the document declares so
	// allocations during migration (synthesized parents) can never mint a
	// colliding or zero inode.
	maxInode := v1.Root.Inode
	if v1.NextInode > maxInode {
		maxInode = v1.NextInode
	}
	for _, d := range v1.Directories {
		if d.Inode > maxInode {
			maxInode = d.Inode
		}
	}
	for _, r := range v1.Releases {
		for _, f := range r.Files {
			if f.Inode > maxInode {
				maxInode = f.Inode
			}
		}
	}
	// +1: allocateInode returns the counter and then increments, so seeding
	// with maxInode itself would mint a duplicate of the highest inode.
	m.NextInode = maxInode + 1
	// Chunk IDs start at 1; the live maximum is raised by allocation below.
	m.NextChunkID = 1

	m.Root = DirMeta{
		CreatedAt: timeToUnix(v1.Root.CreatedAt), ModifiedAt: timeToUnix(v1.Root.ModifiedAt),
		AccessedAt: timeToUnix(v1.Root.AccessedAt), ChangedAt: timeToUnix(v1.Root.ChangedAt),
		Mode: v1.Root.Mode, UID: v1.Root.UID, GID: v1.Root.GID,
		Inode: v1.Root.Inode, XAttrs: xattrMapFromStrings(v1.Root.XAttrs),
	}

	m.dirs = make(map[string]DirMeta, len(v1.Directories))
	for _, d := range v1.Directories {
		dirMeta := DirMeta{
			CreatedAt: timeToUnix(d.CreatedAt), ModifiedAt: timeToUnix(d.ModifiedAt),
			AccessedAt: timeToUnix(d.AccessedAt), ChangedAt: timeToUnix(d.ChangedAt),
			Mode: d.Mode, UID: d.UID, GID: d.GID,
			Inode: d.Inode, XAttrs: xattrMapFromStrings(d.XAttrs),
		}
		if dirMeta.Inode == 0 {
			dirMeta.Inode = m.allocateInode()
		}
		m.dirs[d.Path] = dirMeta
	}

	m.files = make(map[string]FileMeta)
	m.chunks = make(map[int64]ChunkInfo)
	m.releases = make(map[string]ReleaseRef)

	// A path must map to exactly one node; v1 documents carrying the same
	// name twice are corrupt, and silently keeping one copy would hide
	// which bytes survived.
	seenFiles := make(map[string]struct{})

	// ensureAncestors synthesizes any directory components a migrated path
	// needs but the v1 document never listed, so Validate cannot trip over
	// dangling parents.
	now := timeToUnix(v1.LastModified)
	ensureAncestors := func(path string) {
		for dir := parentPath(path); dir != "" && dir != "."; dir = parentPath(dir) {
			if _, ok := m.dirs[dir]; ok {
				return
			}
			m.dirs[dir] = DirMeta{
				Inode:      m.allocateInode(),
				CreatedAt:  now,
				ModifiedAt: now,
				AccessedAt: now,
				ChangedAt:  now,
			}
		}
	}

	for _, r := range v1.Releases {
		ref := ReleaseRef{
			AssetCount: r.AssetCount,
			CreatedAt:  timeToUnix(r.CreatedAt),
		}
		m.releases[r.Tag] = ref

		for _, f := range r.Files {
			if _, dup := seenFiles[f.Name]; dup {
				return fmt.Errorf("v1 metadata lists %q more than once; refusing to guess which copy is real", f.Name)
			}
			seenFiles[f.Name] = struct{}{}
			symlink := ""
			if f.Kind == "symlink" || f.SymlinkTarget != "" {
				symlink = f.SymlinkTarget
			}

			chunkIDs := make([]int64, 0, len(f.Chunks))
			for _, c := range f.Chunks {
				chunkID := m.allocateChunkID()
				chunkIDs = append(chunkIDs, chunkID)
				ci := ChunkInfo{
					Size: c.Size, Offset: c.Offset, Release: chooseNonEmpty(c.Release, f.Release, r.Tag),
					AssetOffset: c.AssetOffset, AssetID: c.AssetID,
				}
				m.chunks[chunkID] = ci
			}

			fileMeta := FileMeta{
				Chunks:     chunkIDs,
				Size:       f.Size,
				Symlink:    symlink,
				UploadedAt: timeToUnix(f.UploadedAt),
				ModifiedAt: timeToUnix(f.ModifiedAt),
				AccessedAt: timeToUnix(f.AccessedAt),
				ChangedAt:  timeToUnix(f.ChangedAt),
				Mode:       f.Mode,
				UID:        f.UID,
				GID:        f.GID,
				Inode:      f.Inode,
				XAttrs:     xattrMapFromStrings(f.XAttrs),
			}
			if symlink != "" {
				fileMeta.Size = int64(len(symlink))
				fileMeta.Chunks = nil
			}
			if fileMeta.Inode == 0 {
				fileMeta.Inode = m.allocateInode()
			}
			ensureAncestors(f.Name)
			m.files[f.Name] = fileMeta
		}
	}

	// Declared directories may also sit under undeclared parents.
	dirPaths := make([]string, 0, len(m.dirs))
	for dir := range m.dirs {
		dirPaths = append(dirPaths, dir)
	}
	sort.Strings(dirPaths) // parents before children via lexicographic order
	for _, dir := range dirPaths {
		ensureAncestors(dir)
	}

	// The v1 counters may be stale or absent; recompute from what actually
	// migrated so Validate compares against reality.
	m.RecomputeStats()
	return nil
}

func parseNumericReleaseTag(tag string) (int, bool) {
	trimmed := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(tag)), "v")
	if trimmed == "" || trimmed == "-" || trimmed[0] < '0' || trimmed[0] > '9' {
		return 0, false
	}
	n, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, false
	}
	return n, true
}

// chooseNonEmpty returns the first value that is non-blank, trimmed.
func chooseNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// ReplaceFile overwrites a stored file entry verbatim (no identity
// preservation). Returns false when the path holds no file entry. Use this
// for authoritative rewrites such as metadata-mutation callbacks; prefer
// UpsertFile for create/update flows where identity carries over.
func (m *RepoMetadata) ReplaceFile(name string, file FileMeta) bool {
	name = normalizeStoredPath(name)
	existing, ok := m.files[name]
	if !ok {
		return false
	}
	replacement := file.Clone()
	m.files[name] = replacement
	m.trackFilePut(name, existing, true, replacement)
	return true
}
