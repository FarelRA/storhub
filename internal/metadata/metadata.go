package metadata

import (
	"maps"
	"strings"
	"time"
)

// ChunkInfo is one content chunk record: size, offsets, and release asset.
type ChunkInfo struct {
	Size        int64  `json:"s"`
	Offset      int64  `json:"o,omitempty"`
	Release     string `json:"r"`
	AssetOffset int64  `json:"ao,omitempty"`
	AssetID     int64  `json:"a"`
}

// XAttrMap holds extended attributes as raw bytes. JSON values are base64
type XAttrMap map[string][]byte

// Clone returns a deep copy of the attribute map.
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

// FileMeta is one stored file or symlink entry with identity and times.
type FileMeta struct {
	// Normalize materializes Chunks as an empty (non-nil) slice for every
	// regular file and symlink - it is NEVER nil after normalization, and
	// empty files carry zero chunks since the sentinel-part removal.
	// Compare with len(), never DeepEqual against nil or []int64{}.
	Chunks  []int64 `json:"cs,omitempty"`
	Size    int64   `json:"s"`
	Symlink string  `json:"sl,omitempty"`
	// Timestamps are Unix NANOSECONDS (time.Time.UnixNano), never seconds.
	UploadedAt int64  `json:"ua"`
	ModifiedAt int64  `json:"ma,omitempty"`
	AccessedAt int64  `json:"aa,omitempty"`
	ChangedAt  int64  `json:"ch,omitempty"`
	Mode       uint32 `json:"md,omitempty"`
	UID        uint32 `json:"u,omitempty"`
	GID        uint32 `json:"g,omitempty"`
	Inode      uint64 `json:"i,omitempty"`
	// XAttrs is nil-means-empty: normalizeXAttrs collapses empty maps to
	// nil so serialized metadata omits the field.
	XAttrs XAttrMap `json:"x,omitempty"`
}

// Clone returns a deep copy of the file entry.
func (f FileMeta) Clone() FileMeta {
	clone := f
	if f.Chunks != nil {
		clone.Chunks = append([]int64(nil), f.Chunks...)
	}
	clone.XAttrs = f.XAttrs.Clone()
	return clone
}

// DirMeta is one stored directory entry with identity and times.
type DirMeta struct {
	// Timestamps are Unix NANOSECONDS (time.Time.UnixNano), never seconds.
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

// Clone returns a deep copy of the directory entry.
func (d DirMeta) Clone() DirMeta {
	clone := d
	clone.XAttrs = d.XAttrs.Clone()
	return clone
}

// ReleaseRef is one chunk-holding release with its asset count.
type ReleaseRef struct {
	AssetCount int `json:"ac"`
	// CreatedAt is a Unix NANOSECONDS timestamp (time.Time.UnixNano).
	CreatedAt int64 `json:"cr"`
}

// RepoMetadata is the full stored tree of one project.
type RepoMetadata struct {
	Version    int    `json:"v"`
	Project    string `json:"p"`
	TotalFiles int    `json:"tf"`
	TotalSize  int64  `json:"ts"`
	// LastMod is a Unix NANOSECONDS timestamp (time.Time.UnixNano).
	LastMod int64 `json:"lm"`
	Root    DirMeta
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

func (*noCopy) Lock() {}

func (*noCopy) Unlock() {}

// MetadataRevision keeps its name: it is referenced across storage,
// REST, CLI, and test helpers, so the rename churn outweighs the stutter.
//
//revive:disable-next-line:exported
type MetadataRevision struct {
	CommitSHA string `json:"commit_sha"`
	Message   string `json:"message"`
	// CommittedAt is a Unix NANOSECONDS timestamp (time.Time.UnixNano).
	CommittedAt int64 `json:"committed_at"`
}

// NewRepoMetadata builds an empty stamped tree for the project.
func NewRepoMetadata(project string) *RepoMetadata {
	now := time.Now().UnixNano()
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

// Clone produces an independent snapshot of the tree with shared indexes.
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
// in one metadata.json, entry shapes evolving); version 6 is the split layout
// (a manifest plus content-addressed Merkle objects). The split is therefore
// just the next step on the ONE version axis, not a parallel numbering.
//
// A RepoMetadata.Version records the document version its tree corresponds to:
// 6 when loaded from a manifest or newly created (the split layout), or <=5
// when loaded from a legacy single-blob document (it migrates to 6 on its next
// write). The entry SHAPE is identical at 5 and 6; only the on-disk layout
// differs, so no pure migrator crosses the 5->6 boundary (that step is the
// write-time split, in the storage layer).
const maxMetadataVersion = 6

// maxBlobVersion is the newest single-blob schema: what Migrate upgrades legacy
// blobs to, and the version a tree loaded from a metadata.json carries until it
// is written as a split (version 6) document.
const maxBlobVersion = 5

// IsSplit reports whether this tree corresponds to the split (version-6)
// layout: a manifest plus content-addressed objects. A tree loaded from a
// legacy single-blob document reports false until it is migrated on write.
func (m *RepoMetadata) IsSplit() bool { return m.Version >= maxMetadataVersion }

// MarkSplit records that this tree is (or will be) stored in the split
// (version-6) layout. The write path calls it before publishing, so a legacy
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

func cloneStrings(src []string) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

// --- value equality helpers ----------------------------------------------

// chooseNonEmpty returns the first value that is non-blank, trimmed.
func chooseNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
