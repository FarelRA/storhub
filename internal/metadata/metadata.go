package metadata

import (
	"bytes"
	"fmt"
	"maps"
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

	// recorder, when non-nil, collects one transaction's mutation intents
	// (see recorder.go). It is deliberately NOT copied by Clone: it belongs
	// to exactly one transaction on exactly one tree, and it must never
	// reach the JSON shadow (unexported, and the shadow is built
	// explicitly).
	recorder *IntentRecorder `json:"-"`

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

// newBareRepoMetadata is the canonical bare-tree constructor: empty stored
// maps, no derived state, no stamps. Migration (migrateV1ToV2,
// migrateV3ToV4) and LoadTree share it so wholesale-construction paths start
// from one shape; NewRepoMetadata above is the stamped public variant.
func newBareRepoMetadata() *RepoMetadata {
	return &RepoMetadata{
		dirs:     make(map[string]DirMeta),
		files:    make(map[string]FileMeta),
		chunks:   make(map[int64]ChunkInfo),
		releases: make(map[string]ReleaseRef),
	}
}

// Clone produces an independent snapshot of the tree. See Clone for the
// sharing contract.
// Dirs returns the stored directory map. READ-ONLY: do not write through it.
func (m *RepoMetadata) Dirs() map[string]DirMeta { return m.dirs }

// Files returns the stored file map. READ-ONLY: do not write through it.
func (m *RepoMetadata) Files() map[string]FileMeta { return m.files }

// Chunks returns the stored chunk map. READ-ONLY: do not write through it.
func (m *RepoMetadata) Chunks() map[int64]ChunkInfo { return m.chunks }

// Releases returns the stored release map. READ-ONLY: do not write through
// it.
func (m *RepoMetadata) Releases() map[string]ReleaseRef { return m.releases }

// Chunk returns one chunk record and whether it exists.
func (m *RepoMetadata) Chunk(id int64) (ChunkInfo, bool) {
	c, ok := m.chunks[id]
	return c, ok
}

func (m *RepoMetadata) Clone() *RepoMetadata {
	// Sharing contract: the four stored maps are copied (so entry
	// insert/remove/replace on either side is invisible to the other), but
	// the ENTRY VALUES are shared: a stored FileMeta/DirMeta is treated as
	// immutable once written - every in-package mutation replaces the map
	// entry with a fresh value (UpsertFile clones its input; Normalize and
	// the identity helpers never mutate a stored Chunks backing array or
	// XAttrs map in place). Callers that want to mutate an entry obtained
	// from a map must FileMeta.Clone/DirMeta.Clone it first, which every
	// existing caller already does. Only Root is deep-copied (it is a
	// single value, and callers mutate its XAttrs through the returned
	// pointer).
	//
	// A clean derived index is SHARED (the read-only maps are referenced,
	// not rebuilt) with the source: reads on the snapshot
	// (DirectoryChildren, NLink, ...) hit the shared index with zero
	// rebuild, and the first tracked mutation on either side drops to a
	// dirty state instead of writing into shared maps. This is what makes
	// a per-operation snapshot cheap.
	//
	// Explicit construction, not a struct copy: RepoMetadata embeds noCopy,
	// so copying it is a vet error. Clone is the one sanctioned copy path.
	clone := &RepoMetadata{
		Version:     m.Version,
		Project:     m.Project,
		TotalFiles:  m.TotalFiles,
		TotalSize:   m.TotalSize,
		LastMod:     m.LastMod,
		Root:        m.Root.Clone(),
		dirs:        cloneMap(m.dirs),
		files:       cloneMap(m.files),
		chunks:      cloneMap(m.chunks),
		releases:    cloneMap(m.releases),
		NextInode:   m.NextInode,
		NextChunkID: m.NextChunkID,
	}
	if d := m.derived; d != nil {
		cd := &derivedState{
			idxDirty:      d.idxDirty,
			sections:      d.sections,
			pendingAssets: maps.Clone(d.pendingAssets),
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

// cloneMap copies a stored map. Values are copied by struct assignment and
// share their (immutable-by-contract) backing storage: Chunks arrays, XAttrs
// maps - exactly what Clone's sharing contract requires. A nil map stays
// nil so wire shape (omitted vs {}) round-trips.
func cloneMap[K comparable, V any](src map[K]V) map[K]V {
	if src == nil {
		return nil
	}
	dst := make(map[K]V, len(src))
	for k, v := range src {
		dst[k] = v
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
	m.normalizeRoot()
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
		dir.Normalize()
		if !dirMetaEqual(original, dir) ||
			(original.XAttrs == nil) != (dir.XAttrs == nil) {
			// The second clause persists the nil-collapse Normalize
			// guarantees: lenient equality alone would leave an
			// empty-but-non-nil map stored.
			m.dirs[path] = dir
			m.sizePutDir(path, putTransition(original, true, dir))
		}
	}
	for path, file := range m.files {
		original := file
		file.Normalize()
		if !fileMetaEqual(original, file) ||
			(original.Chunks == nil) != (file.Chunks == nil) ||
			(original.XAttrs == nil) != (file.XAttrs == nil) {
			m.files[path] = file
			m.sizePutFile(path, putTransition(original, true, file))
		}
	}
	m.sortFileChunksByOffset()
	for tag, ref := range m.releases {
		if ref.CreatedAt == 0 {
			original := ref
			ref.CreatedAt = now
			m.releases[tag] = ref
			m.sizePutRelease(tag, putTransition(original, true, ref))
		}
	}
	m.RecomputeStats()
	if m.LastMod == 0 {
		m.LastMod = now
	}
	// Indexes are rebuilt by RecomputeStats above; no second rebuild here.
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

	// pendingAssets is rebuilt from the chunk walk: counts that belong to
	// existing refs land in the refs above; counts for tags with no ref
	// survive here for a later EnsureRelease/PutRelease to drain. This is
	// the authoritative reset that keeps the incremental path exact.
	d := m.ensureDerived()
	d.pendingAssets = make(map[string]int)
	for tag := range m.releases {
		ref := m.releases[tag]
		if ref.AssetCount != assetCounts[tag] {
			original := ref
			ref.AssetCount = assetCounts[tag]
			m.releases[tag] = ref
			m.sizePutRelease(tag, putTransition(original, true, ref))
		}
	}
	for _, chunk := range m.chunks {
		if chunk.Release != "" {
			if _, ok := m.releases[chunk.Release]; !ok {
				d.pendingAssets[chunk.Release]++
			}
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
//
// Removals go through DeleteChunk (not raw map deletes) so the tracked
// mutator stays the single chokepoint for chunk removal: the size section
// stays exact incrementally and a transaction's intent recorder sees the
// prune.
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
			m.DeleteChunk(id)
			removed++
		}
	}
	return removed
}

func (d *DirMeta) Normalize() {
	if d.Mode == 0 {
		d.Mode = defaultDirMode()
	}
	// Timestamps are NOT repaired here: the v4 contract is complete,
	// authoritative values (the stacked migrator completes legacy docs;
	// creation paths stamp real times). Zeros are real epoch values.
	d.XAttrs = normalizeXAttrs(d.XAttrs)
}

func (f *FileMeta) Normalize() {
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
	dir.Normalize()
	m.dirs[path] = dir
	m.trackDirPut(path, putTransition(DirMeta{}, false, dir))
	m.recordDirPut(path, DirMeta{}, false)
}

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
	file.Normalize()
	m.files[name] = file
	m.trackFilePut(name, putTransition(existing, existed, file))
	m.statsFilePut(putTransition(existing, existed, file))
	m.recordFilePut(name, existing, existed)
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
		m.sizePutFile(name, putTransition(original, true, file))
		m.recordFilePut(name, original, true)
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
	// Clone the caller's value (mirroring UpsertFile): without this a
	// caller-retained Chunks slice aliases the stored entry.
	file = file.Clone()
	file.Normalize()
	m.files[name] = file
	m.trackFilePut(name, putTransition(existing, existed, file))
	m.statsFilePut(putTransition(existing, existed, file))
	m.recordFilePut(name, existing, existed)
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

func (m *RepoMetadata) RemoveFile(name string) bool {
	name = normalizeStoredPath(name)
	old, ok := m.files[name]
	if !ok {
		return false
	}
	delete(m.files, name)
	m.trackFileRemove(name, old)
	m.statsFileRemove(old)
	m.recordFileRemove(name, old)
	return true
}

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

// xAttrsEqual compares by length and content: nil and empty are EQUAL. Go's
// encoding/json omits both nil and len-0 maps under omitempty, so the two
// serialize identically and distinguishing them only causes spurious cache
// misses (and skipped canonical write-backs, which Normalize triggers
// explicitly instead).
func xAttrsEqual(a, b XAttrMap) bool {
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
	// Chunks compare by content only (slices.Equal treats nil and empty as
	// equal): both serialize identically under omitempty, so the
	// distinction only causes spurious cache misses. Normalize triggers
	// canonical write-backs explicitly (see its loops), so collapsing here
	// cannot leave a non-canonical entry stored.
	if !slices.Equal(a.Chunks, b.Chunks) {
		return false
	}
	return xAttrsEqual(a.XAttrs, b.XAttrs)
}

// preserveFileIdentity carries the existing node's stable identity onto an
// updated entry. A type change (regular file <-> symlink) carries nothing.
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
		file.UploadedAt = existing.UploadedAt
		if file.UploadedAt == 0 {
			file.UploadedAt = now
		}
	}
	if file.ModifiedAt == 0 {
		file.ModifiedAt = now
		if file.ModifiedAt == 0 {
			file.ModifiedAt = existing.ModifiedAt
		}
		if file.ModifiedAt == 0 {
			file.ModifiedAt = file.UploadedAt
		}
	}
	if file.AccessedAt == 0 {
		file.AccessedAt = existing.AccessedAt
		if file.AccessedAt == 0 {
			file.AccessedAt = file.ModifiedAt
		}
	}
	if file.ChangedAt == 0 {
		file.ChangedAt = now
		if file.ChangedAt == 0 {
			file.ChangedAt = existing.ChangedAt
		}
		if file.ChangedAt == 0 {
			file.ChangedAt = file.ModifiedAt
		}
	}
	if len(file.XAttrs) == 0 && len(existing.XAttrs) > 0 {
		file.XAttrs = existing.XAttrs.Clone()
	}
}

// PreserveFileIdentity carries the existing node's stable identity onto an
// updated entry. Exported because POSIX update paths outside this package
// assemble entries before storing them through the tracked mutators; the
// carry-over rule stays owned here.
func PreserveFileIdentity(file *FileMeta, existing *FileMeta, now int64) {
	preserveFileIdentity(file, existing, now)
}

// initializeNewFileIdentity materializes a complete identity (inode, mode,
// owner, timestamps) for a newly created file entry.
func initializeNewFileIdentity(meta *RepoMetadata, file *FileMeta, now int64) {
	if file.Inode == 0 {
		file.Inode = meta.allocateInode()
	}
	initializeNewFileIdentityFields(file, now)
}

// InitializeNewFileIdentity materializes a complete identity for a newly
// created file entry against this tree. Exported for the same reason as
// InitializeNewFileIdentityFields: upload paths assemble entries before
// storing them; the counter rule in AllocateInode applies.
func InitializeNewFileIdentity(meta *RepoMetadata, file *FileMeta, now int64) {
	initializeNewFileIdentity(meta, file, now)
}

// initializeNewFileIdentityFields applies every creation default EXCEPT the
// inode: mode, owner, and the full timestamp set. Inode minting is reserved
// for initializeNewFileIdentity because the counter lives on exactly one
// authoritative RepoMetadata; stamping an inode against a throwaway clone
// (a readonly snapshot, a working copy) silently skips the counter bump and
// the next allocation re-issues the same inode.
func initializeNewFileIdentityFields(file *FileMeta, now int64) {
	if file.Mode == 0 {
		file.Mode = defaultFileMode(nodeKindOf(file))
	}
	// Owner IDs are NEVER materialized here: 0 legitimately means root,
	// so a zero value cannot double as "unset". Every creation path
	// provisions the owner explicitly before storing (OwnerIDsForCreate
	// for fresh entries, PreserveFileIdentity for updates). Stamping the
	// process user here used to silently reassign root-owned entries to
	// whoever ran the process (regression: TestCloneRangePermissions
	// passed or failed depending on the runner's UID).
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

// InitializeNewFileIdentityFields applies every creation default EXCEPT the
// inode: mode, owner, and the full timestamp set. Exported because upload
// paths outside this package assemble entries before storing them; the
// inode itself is minted by initializeNewFileIdentity (via UpsertFile) so
// the counter rule above stays intact.
func InitializeNewFileIdentityFields(file *FileMeta, now int64) {
	initializeNewFileIdentityFields(file, now)
}

func (m *RepoMetadata) normalizeRoot() {
	m.normalizeRootFast()
	m.reconcileCounters()
}

// normalizeRootFast applies the O(1) root touch-ups (inode/mode defaults,
// xattr normalization) without the O(files+dirs+chunks) counter
// reconciliation. normalizeRoot (load/normalize paths) adds the
// reconciliation; SealTransaction uses only this fast path so sealing stays
// O(1) per transaction.
func (m *RepoMetadata) normalizeRootFast() {
	if m.Root.Inode == 0 {
		m.Root.Inode = 1
	}
	if m.Root.Mode == 0 {
		m.Root.Mode = defaultDirMode()
	}
	// Root timestamps are authoritative under v4 (see DirMeta.Normalize).
	m.Root.XAttrs = normalizeXAttrs(m.Root.XAttrs)
}

// allocateInode mints the next inode number and bumps the counter. It
// trusts the counter unconditionally; load paths reconcile it first via
// reconcileCounters.
func (m *RepoMetadata) allocateInode() uint64 {
	ino := m.NextInode
	m.NextInode++
	return ino
}

// AllocateInode mints a fresh inode against this tree. Call it ONLY on the
// tree that will be published (the UpdateRepoMetadataContext candidate, or a
// working copy swapped in on success) while holding the owner's
// authoritative lock. Minting against a throwaway clone silently skips the
// counter bump and the next allocation re-issues the same inode, so the
// counter lives on exactly one authoritative RepoMetadata per project.
func (m *RepoMetadata) AllocateInode() uint64 {
	return m.allocateInode()
}

// allocateChunkID mints the next chunk identifier and bumps the counter,
// mirroring allocateInode.
func (m *RepoMetadata) allocateChunkID() int64 {
	id := m.NextChunkID
	m.NextChunkID++
	return id
}

// AllocateChunkID mints a fresh chunk identifier against this tree. Same
// ownership rule as AllocateInode: the tree that will be published, under
// the authoritative lock. Rebase collision remapping and upload chunk-ID
// assignment need concrete IDs before any store call, which is why this
// cannot be folded into PutChunk.
func (m *RepoMetadata) AllocateChunkID() int64 {
	return m.allocateChunkID()
}

// parseNumericReleaseTag extracts the numeric part of a "v<N>" release tag.
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

// ParseNumericReleaseTag extracts the numeric part of a "v<N>" release tag.
// Exported because release-rotation code outside this package walks the
// catalog by number; the parsing rule stays owned here.
func ParseNumericReleaseTag(tag string) (int, bool) {
	return parseNumericReleaseTag(tag)
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
	replacement.Normalize()
	m.files[name] = replacement
	m.trackFilePut(name, putTransition(existing, true, replacement))
	m.statsFilePut(putTransition(existing, true, replacement))
	m.recordFilePut(name, existing, true)
	return true
}
