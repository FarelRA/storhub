package metadata

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
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
	Version    int                   `json:"v"`
	Project    string                `json:"p"`
	TotalFiles int                   `json:"tf"`
	TotalSize  int64                 `json:"ts"`
	LastMod    int64                 `json:"lm"`
	Root       DirMeta               `json:"rt"`
	Dirs       map[string]DirMeta    `json:"d"`
	Files      map[string]FileMeta   `json:"f"`
	Chunks     map[int64]ChunkInfo   `json:"c"`
	Releases   map[string]ReleaseRef `json:"r"`

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
}

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
	// A plain value copy of a RepoMetadata (t := *m, not Clone) shares the
	// state pointer WITHOUT marking anything: when the state's owner is
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

// sectionSize tracks the serialized size of one stored map: sum is
// Σ(len(quoted key)+1+len(value)) over its entries; the map's own braces and
// inter-entry commas are added by the caller. ok=false means the section is
// stale and must be recomputed before use.
type sectionSize struct {
	ok  bool
	ref any
	ptr uintptr
	n   int
	sum int64
}

// Size-cache section indices, ordered as the JSON struct fields.
const (
	secDirs = iota
	secFiles
	secChunks
	secReleases
)

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
		Dirs:     make(map[string]DirMeta),
		Files:    make(map[string]FileMeta),
		Chunks:   make(map[int64]ChunkInfo),
		Releases: make(map[string]ReleaseRef),
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
func (m RepoMetadata) Clone() RepoMetadata {
	clone := m
	clone.Dirs = cloneDirMetaMap(m.Dirs)
	clone.Files = cloneFileMetaMap(m.Files)
	clone.Chunks = cloneChunkInfoMap(m.Chunks)
	clone.Releases = cloneReleaseRefMap(m.Releases)
	clone.Root = m.Root.Clone()
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
		cd.dirsRef, cd.dirsLen = clone.Dirs, len(clone.Dirs)
		cd.filesRef, cd.filesLen = clone.Files, len(clone.Files)
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

func (m *RepoMetadata) ToJSON() ([]byte, error) {
	// A blob document is always maxBlobVersion: version 5 is the split
	// (manifest + objects) layout, which ToJSON cannot express. The in-memory
	// Version records the tree's target layout; the write path re-stamps it
	// via MarkSplit when publishing the split.
	out := m
	if m.Version > maxBlobVersion {
		trimmed := *m
		trimmed.Version = maxBlobVersion
		out = &trimmed
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	return data, nil
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

// FromJSON parses a metadata document into current form. Version detection
// and any upgrades belong entirely to Migrate (stacked, eager); this parser
// understands ONLY the current blob schema - legacy spellings never reach it.
// A version-5 split-index manifest is NOT a blob and must go through
// ParseManifest. The resulting tree carries the document version it was read
// as (maxBlobVersion: blobs stop at 4; 5 is manifest-only).
func (m *RepoMetadata) FromJSON(data []byte) error {
	upgraded, version, err := Migrate(data)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(upgraded, m); err != nil {
		return fmt.Errorf("unmarshal metadata: %w", err)
	}
	m.Version = version
	return nil
}

// UnmarshalJSON enforces the current-schema contract at the type level: only
// documents written in the current blob schema (version maxBlobVersion)
// decode. Older payloads must go through FromJSON/Migrate, and a split-index
// manifest must go through ParseManifest/LoadTree - a direct unmarshal fails
// loudly instead of silently yielding an empty tree from ignored unknown
// fields. Version 5 is manifest-only: a v5 document without a non-empty tree
// root is a truncated manifest, not a blob, and decoding it as one would
// hand the next commit an empty tree over the real index.
func (m *RepoMetadata) UnmarshalJSON(data []byte) error {
	var probe struct {
		V        *int   `json:"v"`
		TreeRoot string `json:"tr"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return fmt.Errorf("metadata probe: %w", err)
	}
	if probe.V == nil {
		return errors.New("metadata document lacks a schema version; use metadata.Migrate for older formats")
	}
	if probe.TreeRoot != "" {
		return fmt.Errorf("document is a v%d split-index manifest; load it via ParseManifest/LoadTree, not RepoMetadata", maxMetadataVersion)
	}
	if *probe.V == maxMetadataVersion {
		return fmt.Errorf("document claims v%d with no tree root: v%d documents are manifests (load via ParseManifest/LoadTree); blobs are v%d or older", maxMetadataVersion, maxMetadataVersion, maxBlobVersion)
	}
	if *probe.V != maxBlobVersion {
		return fmt.Errorf("metadata version %d is not a current blob version (%d); migrate first", *probe.V, maxBlobVersion)
	}
	type alias RepoMetadata
	var raw alias
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*m = RepoMetadata(raw)
	m.Version = *probe.V
	return nil
}

func (m *RepoMetadata) Normalize(project string, now int64) {
	// Version is preserved (it records the document/layout the tree came from
	// or will become), never forced here: a legacy blob stays maxBlobVersion
	// until written as a split document, a split/new tree is maxMetadataVersion.
	m.Project = chooseNonEmpty(m.Project, project)
	m.normalizeRoot(now)
	if m.Dirs == nil {
		m.Dirs = make(map[string]DirMeta)
	}
	if m.Files == nil {
		m.Files = make(map[string]FileMeta)
	}
	if m.Chunks == nil {
		m.Chunks = make(map[int64]ChunkInfo)
	}
	if m.Releases == nil {
		m.Releases = make(map[string]ReleaseRef)
	}
	// Entry loops write back ONLY when normalization actually changed the
	// value: an already-normalized tree (the common per-transaction case)
	// then leaves the derived indexes and the size cache untouched, so no
	// rebuild or re-marshalling happens here.
	for path, dir := range m.Dirs {
		original := dir
		dir.Normalize(now)
		if !dirMetaEqual(original, dir) {
			m.Dirs[path] = dir
			m.sizePutDir(path, original, true, dir)
		}
	}
	for path, file := range m.Files {
		original := file
		file.Normalize(now)
		if !fileMetaEqual(original, file) {
			m.Files[path] = file
			m.sizePutFile(path, original, true, file)
		}
	}
	m.sortFileChunksByOffset()
	for tag, ref := range m.Releases {
		if ref.CreatedAt == 0 {
			original := ref
			ref.CreatedAt = now
			m.Releases[tag] = ref
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

// sortFileChunksByOffset enforces the stored-order invariant every reader
// relies on: a file's chunk IDs are ordered by data offset, so binary search
// over FileChunks is valid.
func (m *RepoMetadata) sortFileChunksByOffset() {
	for path, file := range m.Files {
		if len(file.Chunks) < 2 {
			continue
		}
		if chunksOffsetSorted(m.Chunks, file.Chunks) {
			continue
		}
		original := file
		sorted := append([]int64(nil), file.Chunks...)
		sort.SliceStable(sorted, func(i, j int) bool {
			return m.Chunks[sorted[i]].Offset < m.Chunks[sorted[j]].Offset
		})
		file.Chunks = sorted
		m.Files[path] = file
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

func (m *RepoMetadata) RecomputeStats() {
	totalFiles := 0
	totalSize := int64(0)
	assetCounts := make(map[string]int)

	for _, chunk := range m.Chunks {
		if chunk.Release != "" {
			assetCounts[chunk.Release]++
		}
	}
	for _, file := range m.Files {
		if file.Symlink == "" {
			totalFiles++
			totalSize += file.Size
		}
	}

	for tag := range m.Releases {
		ref := m.Releases[tag]
		if ref.AssetCount != assetCounts[tag] {
			original := ref
			ref.AssetCount = assetCounts[tag]
			m.Releases[tag] = ref
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
	for _, file := range m.Files {
		for _, id := range file.Chunks {
			referenced[id] = struct{}{}
		}
	}
	removed := 0
	for id := range m.Chunks {
		if _, ok := referenced[id]; !ok {
			delete(m.Chunks, id)
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
	if ref, ok := m.Releases[tag]; ok {
		return &ref
	}
	return nil
}

func (m *RepoMetadata) HasDirectory(path string) bool {
	path = normalizeStoredPath(path)
	if path == "" {
		return true
	}
	_, ok := m.Dirs[path]
	return ok
}

func (m *RepoMetadata) GetDirectory(path string) *DirMeta {
	path = normalizeStoredPath(path)
	if path == "" {
		root := m.Root
		return &root
	}
	if dir, ok := m.Dirs[path]; ok {
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
	m.Dirs[path] = dir
	m.trackDirPut(path, DirMeta{}, false, dir)
}

func (m *RepoMetadata) RemoveDirectory(path string) bool {
	path = normalizeStoredPath(path)
	old, ok := m.Dirs[path]
	if !ok {
		return false
	}
	delete(m.Dirs, path)
	m.trackDirRemove(path, old)
	return true
}

func (m *RepoMetadata) DirectoryChildren(path string) (dirs, files []string) {
	path = normalizeStoredPath(path)
	// Lazily rebuilt: every tracked mutation maintains the child lists
	// incrementally, invalidateIndexes marks them stale, and the structural
	// fingerprint (map identity + length) catches entries written directly
	// into Dirs/Files outside this package (a wholesale map swap or a
	// key add/remove changes the fingerprint). External writers that mutate
	// map CONTENT in place under the same keys (e.g. a chmod writing
	// repo.Dirs[p] = *dir) do not affect these indexes at all; if such a
	// write ever changes a file's inode it must go through
	// WriteFileDirect/ReplaceFile, or call InvalidateIndexes.
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
	if ref, ok := m.Releases[tag]; ok {
		return &ref
	}
	m.Releases[tag] = ReleaseRef{CreatedAt: createdAt}
	ref := m.Releases[tag]
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
	existing, existed := m.Files[name]
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
	m.Files[name] = file
	m.trackFilePut(name, existing, existed, file)
}

// FindFile returns a SNAPSHOT of the entry: the pointer targets a copy of
// the struct, so FIELD-level mutations never reach stored state. (Slices
// and maps inside - Chunks, XAttrs - still share backing storage; clone
// before mutating those.) Apply changes through UpsertFile or an
// UpdateRepoMetadataContext transaction.
func (m *RepoMetadata) FindFile(name string) *FileMeta {
	name = normalizeStoredPath(name)
	if file, ok := m.Files[name]; ok {
		return &file
	}
	return nil
}

// SetFileAtime updates atime in place. FindFile returns a pointer to a
// copy (map values are not addressable), so mutating its result silently
// drops the write — use this setter for mutations.
func (m *RepoMetadata) SetFileAtime(name string, atime int64) bool {
	name = normalizeStoredPath(name)
	if file, ok := m.Files[name]; ok {
		original := file
		file.AccessedAt = atime
		m.Files[name] = file
		m.sizePutFile(name, original, true, file)
		return true
	}
	return false
}

// SetDirAtime updates atime in place; same copy-pointer footgun as
// FindFile applies to GetDirectory results.
func (m *RepoMetadata) SetDirAtime(path string, atime int64) bool {
	path = normalizeStoredPath(path)
	if dir, ok := m.Dirs[path]; ok {
		original := dir
		dir.AccessedAt = atime
		m.Dirs[path] = dir
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
	existing, existed := m.Files[name]
	m.Files[name] = file
	m.trackFilePut(name, existing, existed, file)
}

// WriteDirDirect stores a directory entry verbatim, mirroring
// WriteFileDirect for op replay and family updates that must preserve
// every field exactly.
func (m *RepoMetadata) WriteDirDirect(path string, dir DirMeta) {
	path = normalizeStoredPath(path)
	existing, existed := m.Dirs[path]
	m.Dirs[path] = dir
	m.trackDirPut(path, existing, existed, dir)
}

func (m *RepoMetadata) RemoveFile(name string) bool {
	name = normalizeStoredPath(name)
	old, ok := m.Files[name]
	if !ok {
		return false
	}
	delete(m.Files, name)
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
	old, ok := m.Releases[tag]
	if !ok {
		return false
	}
	delete(m.Releases, tag)
	// Releases are not part of the derived indexes; only the size cache
	// needs the removal.
	m.sizeRemoveRelease(tag, old)
	return true
}

// PutChunk stores a chunk record verbatim, maintaining the serialized-size
// cache incrementally. Direct `meta.Chunks[id] = info` writes bypass the
// size accounting when they overwrite an existing id (a new id is still
// caught by the length fingerprint), so tracked writers should use this.
// The stored ChunkInfo is a pure value type; no cloning is needed.
func (m *RepoMetadata) PutChunk(id int64, info ChunkInfo) {
	old, existed := m.Chunks[id]
	m.Chunks[id] = info
	sizeApplySection(&m.ensureDerived().sections[secChunks], m.Chunks, id, old, existed, info, true)
}

// DeleteChunk removes a chunk record, maintaining the serialized-size cache
// incrementally (the mirror of PutChunk for tracked deletions).
func (m *RepoMetadata) DeleteChunk(id int64) bool {
	old, ok := m.Chunks[id]
	if !ok {
		return false
	}
	delete(m.Chunks, id)
	sizeApplySection(&m.ensureDerived().sections[secChunks], m.Chunks, id, old, true, ChunkInfo{}, false)
	return true
}

func (m *RepoMetadata) AllFiles() []FileMeta {
	names := make([]string, 0, len(m.Files))
	for name := range m.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	files := make([]FileMeta, len(names))
	for i, name := range names {
		files[i] = m.Files[name].Clone()
	}
	return files
}

// FileChunks resolves a file's chunk IDs to chunk records in stored order
// (which Normalize keeps sorted by data offset).
func (m *RepoMetadata) FileChunks(name string) []ChunkInfo {
	file, ok := m.Files[normalizeStoredPath(name)]
	if !ok {
		return nil
	}
	chunks := make([]ChunkInfo, 0, len(file.Chunks))
	for _, id := range file.Chunks {
		if c, ok := m.Chunks[id]; ok {
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
		if _, ok := m.Dirs[path]; !ok {
			return 0
		}
	}
	m.ensureIndexes()
	return 2 + len(m.derived.childDirs[path])
}

func (m *RepoMetadata) FileNLink(name string) int {
	file, ok := m.Files[normalizeStoredPath(name)]
	if !ok {
		return 0
	}
	return m.NLink(file.Inode)
}

// invalidateIndexes marks the derived indexes stale without rebuilding them;
// the next index-dependent read pays one rebuild.
func (m *RepoMetadata) invalidateIndexes() {
	m.ensureDerived().idxDirty = true
}

// InvalidateIndexes marks the derived indexes AND the serialized-size cache
// stale. Exported for callers that write into Dirs/Files/Chunks/Releases
// directly (bypassing UpsertFile/WriteFileDirect/...): a wholesale map swap
// or a key add/remove is caught by the structural fingerprint anyway, but an
// in-place value write under an unchanged key (notably a file inode change)
// is invisible to it, so such writers must invalidate explicitly. Prefer the
// tracked mutators (WriteFileDirect, WriteDirDirect, ReplaceFile, PutChunk,
// ...) over calling this: they keep the caches warm instead of forcing a
// rebuild.
func (m *RepoMetadata) InvalidateIndexes() {
	d := m.ensureDerived()
	d.idxDirty = true
	for i := range d.sections {
		d.sections[i].ok = false
	}
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
	d.dirsRef = m.Dirs
	d.dirsLen = len(m.Dirs)
	d.filesRef = m.Files
	d.filesLen = len(m.Files)
}

// indexFresh reports whether the derived indexes currently reflect m.Dirs
// and m.Files. Besides the dirty flag it checks a structural fingerprint:
// map identity (pinned by the stored ref, so a pointer match cannot alias a
// freed map) and length. That catches entries added/removed by direct map
// writes outside this package; in-place value writes cannot change the
// index keys, and inode-changing value writes must go through the tracked
// mutators (see InvalidateIndexes).
func (m *RepoMetadata) indexFresh() bool {
	d := m.derived
	return d != nil && !d.idxDirty &&
		sameMap(d.dirsRef, m.Dirs) && d.dirsLen == len(m.Dirs) &&
		sameMap(d.filesRef, m.Files) && d.filesLen == len(m.Files)
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
	d.filesByInode = make(map[uint64][]string, len(m.Files))
	d.childDirs = make(map[string][]string, len(m.Dirs)+1)
	d.childFiles = make(map[string][]string, len(m.Files)+1)

	for path := range m.Dirs {
		parent := parentPath(path)
		d.childDirs[parent] = append(d.childDirs[parent], path)
	}
	for path, file := range m.Files {
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

func (m *RepoMetadata) NLink(inode uint64) int {
	m.ensureIndexes()
	return len(m.derived.filesByInode[inode])
}

// --- incremental index maintenance -------------------------------------

// trackDirPut maintains the childDirs index and the dirs size section after
// m.Dirs[path] was set to cur (hadOld reports whether an entry was
// replaced). Call AFTER the map write.
func (m *RepoMetadata) trackDirPut(path string, old DirMeta, hadOld bool, cur DirMeta) {
	m.sizePutDir(path, old, hadOld, cur)
	if d := m.indexForIncremental(); d != nil && !hadOld {
		parent := parentPath(path)
		d.childDirs[parent] = insertSortedString(d.childDirs[parent], path)
		m.syncIndexFingerprint(d)
	}
}

// trackDirRemove maintains the indexes after m.Dirs[path] was deleted
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
// size section after m.Files[name] was set to cur (hadOld reports whether
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

// trackFileRemove maintains the indexes after m.Files[name] was deleted
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

func cloneStrings(src []string) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
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

func mapPtr[K comparable, V any](m map[K]V) uintptr {
	return reflect.ValueOf(m).Pointer()
}

// --- incremental serialized-size accounting -----------------------------

// SerializedSize returns the exact byte length of ToJSON's output,
// maintained incrementally: per-entry deltas are applied by the tracked
// mutators, and only the constant-size document skeleton (scalars + root)
// is marshalled per call. A section whose map changed outside the tracked
// paths (fingerprint mismatch) is recomputed once, on demand. Callers
// performing size admission can use this instead of marshalling the whole
// tree.
func (m *RepoMetadata) SerializedSize() (int, error) {
	base, err := m.sizeSkeletonLen()
	if err != nil {
		return 0, err
	}
	d := m.ensureDerived()
	total := int64(base)
	for _, s := range []struct {
		i     int
		extra func(*sectionSize) (int64, error)
	}{
		{secDirs, func(s *sectionSize) (int64, error) { return sectionExtra(s, m.Dirs) }},
		{secFiles, func(s *sectionSize) (int64, error) { return sectionExtra(s, m.Files) }},
		{secChunks, func(s *sectionSize) (int64, error) { return sectionExtra(s, m.Chunks) }},
		{secReleases, func(s *sectionSize) (int64, error) { return sectionExtra(s, m.Releases) }},
	} {
		extra, err := s.extra(&d.sections[s.i])
		if err != nil {
			return 0, err
		}
		total += extra
	}
	return int(total), nil
}

// sizeSkeletonLen marshals the document with the four stored maps emptied
// (preserving nil vs non-nil) and returns its length: the constant overhead
// every SerializedSize answer is built on. It mirrors ToJSON's version trim
// exactly.
func (m *RepoMetadata) sizeSkeletonLen() (int, error) {
	trimmed := *m
	if trimmed.Version > maxBlobVersion {
		trimmed.Version = maxBlobVersion
	}
	if trimmed.Dirs != nil {
		trimmed.Dirs = map[string]DirMeta{}
	}
	if trimmed.Files != nil {
		trimmed.Files = map[string]FileMeta{}
	}
	if trimmed.Chunks != nil {
		trimmed.Chunks = map[int64]ChunkInfo{}
	}
	if trimmed.Releases != nil {
		trimmed.Releases = map[string]ReleaseRef{}
	}
	data, err := json.Marshal(&trimmed)
	if err != nil {
		return 0, fmt.Errorf("marshal metadata skeleton: %w", err)
	}
	return len(data), nil
}

// sectionExtra returns the bytes the map contributes beyond its empty
// serialization ("{}" or "null"), recomputing the section when its
// fingerprint no longer matches the live map.
func sectionExtra[K comparable, V any](s *sectionSize, mp map[K]V) (int64, error) {
	if mp == nil {
		return 0, nil
	}
	ptr, n := mapPtr(mp), len(mp)
	if !s.ok || s.ptr != ptr || s.n != n {
		sum := int64(0)
		for k, v := range mp {
			c, err := entryBytes(k, v)
			if err != nil {
				return 0, err
			}
			sum += int64(c)
		}
		s.ok, s.ref, s.ptr, s.n, s.sum = true, mp, ptr, n, sum
	}
	extra := s.sum
	if n > 1 {
		extra += int64(n - 1)
	}
	return extra, nil
}

// entryBytes is the exact byte cost of one map entry inside its parent
// object: quoted key + colon + value (the inter-entry comma is accounted
// per-section, not per-entry).
func entryBytes[K comparable, V any](key K, val V) (int, error) {
	kb, err := json.Marshal(key)
	if err != nil {
		return 0, err
	}
	klen := len(kb)
	if _, isString := any(key).(string); !isString {
		// Numeric map keys are quoted in JSON objects.
		klen += 2
	}
	vb, err := json.Marshal(val)
	if err != nil {
		return 0, err
	}
	return klen + 1 + len(vb), nil
}

// sizeApplySection adjusts one section for a single key transition
// (old, present iff hadOld) -> (cur, present iff hasCur) on the map mp,
// which must already reflect the change. A section that no longer matches
// the map (external swap, or a length the delta cannot explain) is marked
// stale for on-demand recompute instead of being silently wrong.
func sizeApplySection[K comparable, V any](s *sectionSize, mp map[K]V, key K, old V, hadOld bool, cur V, hasCur bool) {
	ptr, n := mapPtr(mp), len(mp)
	oldN := n
	if hasCur {
		oldN--
	}
	if hadOld {
		oldN++
	}
	if !s.ok || s.ptr != ptr || s.n != oldN {
		s.ok = false
		return
	}
	if hadOld {
		c, err := entryBytes(key, old)
		if err != nil {
			s.ok = false
			return
		}
		s.sum -= int64(c)
	}
	if hasCur {
		c, err := entryBytes(key, cur)
		if err != nil {
			s.ok = false
			return
		}
		s.sum += int64(c)
	}
	s.n = n
}

func (m *RepoMetadata) sizePutDir(path string, old DirMeta, hadOld bool, cur DirMeta) {
	sizeApplySection(&m.ensureDerived().sections[secDirs], m.Dirs, path, old, hadOld, cur, true)
}

func (m *RepoMetadata) sizeRemoveDir(path string, old DirMeta) {
	sizeApplySection(&m.ensureDerived().sections[secDirs], m.Dirs, path, old, true, DirMeta{}, false)
}

func (m *RepoMetadata) sizePutFile(name string, old FileMeta, hadOld bool, cur FileMeta) {
	sizeApplySection(&m.ensureDerived().sections[secFiles], m.Files, name, old, hadOld, cur, true)
}

func (m *RepoMetadata) sizeRemoveFile(name string, old FileMeta) {
	sizeApplySection(&m.ensureDerived().sections[secFiles], m.Files, name, old, true, FileMeta{}, false)
}

func (m *RepoMetadata) sizePutRelease(tag string, old ReleaseRef, hadOld bool, cur ReleaseRef) {
	sizeApplySection(&m.ensureDerived().sections[secReleases], m.Releases, tag, old, hadOld, cur, true)
}

func (m *RepoMetadata) sizeRemoveRelease(tag string, old ReleaseRef) {
	sizeApplySection(&m.ensureDerived().sections[secReleases], m.Releases, tag, old, true, ReleaseRef{}, false)
}

func (m *RepoMetadata) markSectionStale(i int) {
	m.ensureDerived().sections[i].ok = false
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

// reconcileCounters raises the allocation counters past every id actually
// present, so deleting the highest-numbered entry cannot mint duplicates.
func (m *RepoMetadata) reconcileCounters() {
	maxInode := m.Root.Inode
	for _, dir := range m.Dirs {
		if dir.Inode > maxInode {
			maxInode = dir.Inode
		}
	}
	for _, file := range m.Files {
		if file.Inode > maxInode {
			maxInode = file.Inode
		}
	}
	if m.NextInode <= maxInode {
		m.NextInode = maxInode + 1
	}

	maxChunkID := int64(0)
	for id := range m.Chunks {
		if id > maxChunkID {
			maxChunkID = id
		}
	}
	if m.NextChunkID <= maxChunkID {
		m.NextChunkID = maxChunkID + 1
	}
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

func (m *RepoMetadata) Validate() error {
	if strings.TrimSpace(m.Project) == "" {
		return fmt.Errorf("metadata project is required")
	}
	if m.Root.Inode == 0 {
		return fmt.Errorf("metadata root inode is required")
	}
	if m.Files == nil {
		return fmt.Errorf("metadata files map is nil")
	}
	if m.Chunks == nil {
		return fmt.Errorf("metadata chunks map is nil")
	}
	if m.Releases == nil {
		return fmt.Errorf("metadata releases map is nil")
	}
	if m.Dirs == nil {
		return fmt.Errorf("metadata dirs map is nil")
	}

	seenDirs := map[string]struct{}{}
	seenInodes := map[uint64]struct{}{m.Root.Inode: {}}

	for path, dir := range m.Dirs {
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
			if _, ok := m.Dirs[parent]; !ok {
				return fmt.Errorf("directory %s missing parent %s", path, parent)
			}
		}
	}

	totalFiles := 0
	totalSize := int64(0)
	// One scratch map for every file's duplicate-chunk-reference check:
	// cleared per file instead of allocated per file.
	seenChunk := make(map[int64]struct{})
	for path, file := range m.Files {
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
			if _, ok := m.Dirs[parent]; !ok {
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
			chunk, ok := m.Chunks[id]
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

	for tag, ref := range m.Releases {
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

	m.Dirs = make(map[string]DirMeta, len(v1.Directories))
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
		m.Dirs[d.Path] = dirMeta
	}

	m.Files = make(map[string]FileMeta)
	m.Chunks = make(map[int64]ChunkInfo)
	m.Releases = make(map[string]ReleaseRef)

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
			if _, ok := m.Dirs[dir]; ok {
				return
			}
			m.Dirs[dir] = DirMeta{
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
		m.Releases[r.Tag] = ref

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
				m.Chunks[chunkID] = ci
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
			m.Files[f.Name] = fileMeta
		}
	}

	// Declared directories may also sit under undeclared parents.
	dirPaths := make([]string, 0, len(m.Dirs))
	for dir := range m.Dirs {
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

func stableSortStrings(strs []string) {
	sort.SliceStable(strs, func(i, j int) bool { return strs[i] < strs[j] })
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
	existing, ok := m.Files[name]
	if !ok {
		return false
	}
	replacement := file.Clone()
	m.Files[name] = replacement
	m.trackFilePut(name, existing, true, replacement)
	return true
}
