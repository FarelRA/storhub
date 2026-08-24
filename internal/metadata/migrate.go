package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
)

// CurrentVersion is the newest metadata schema version this build reads
// and writes.
const CurrentVersion = maxMetadataVersion

// detectVersion reports a document's schema version. Historical spellings:
// v1 documents wrote "version"; v2 onward write "v". That history belongs
// to the migrator alone — the main parser never sees version detection.
func detectVersion(data []byte) (int, error) {
	var probe struct {
		V       *int `json:"v"`
		Version *int `json:"version"` // v1-era spelling, consumed here only
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return 0, fmt.Errorf("metadata version probe failed: %w", err)
	}
	switch {
	case probe.V != nil:
		return *probe.V, nil
	case probe.Version != nil:
		return *probe.Version, nil
	default:
		return 0, errors.New("metadata payload has no version field; refusing to guess the schema")
	}
}

// migrators stacks one step per version boundary: migrators[n] upgrades a
// version-n document to version n+1. Every step is pure bytes->bytes:
// no clock, no I/O, deterministic output for a given input.
var migrators = [...]func([]byte) ([]byte, error){
	1: migrateV1ToV2,
	2: migrateV2ToV3,
	3: migrateV3ToV4,
}

// Migrate upgrades a serialized metadata document to CurrentVersion by
// applying every required step in order; a document already current passes
// through unchanged. Loading is eager: every parse funnels through here,
// so no code path outside this file can observe an older schema shape.
// The upgraded document persists when the next mutation commits it.
func Migrate(data []byte) ([]byte, int, error) {
	from, err := detectVersion(data)
	if err != nil {
		return nil, 0, err
	}
	if from > CurrentVersion {
		return nil, from, fmt.Errorf("metadata version %d is newer than supported version %d", from, CurrentVersion)
	}
	if from < 1 {
		return nil, from, fmt.Errorf("invalid metadata version %d", from)
	}
	if from == CurrentVersion {
		return data, from, nil
	}
	for v := from; v < CurrentVersion; v++ {
		step := migrators[v]
		if step == nil {
			return nil, v, fmt.Errorf("no migration path from metadata version %d", v)
		}
		if data, err = step(data); err != nil {
			return nil, v, fmt.Errorf("migrate metadata v%d->v%d: %w", v, v+1, err)
		}
	}
	return data, CurrentVersion, nil
}

// ---------------------------------------------------------------------------
// Era documents: each mirrors exactly what its version wrote on disk, so a
// fixture of that era decodes faithfully and re-encodes stably.
// ---------------------------------------------------------------------------

type docRelease struct {
	AssetCount int   `json:"ac"`
	CreatedAt  int64 `json:"ca"`
}

type docChunk struct {
	Size        int64  `json:"s"`
	Offset      int64  `json:"o,omitempty"`
	Release     string `json:"r"`
	AssetOffset int64  `json:"ao,omitempty"`
	AssetID     int64  `json:"a"`
	Digest      string `json:"d,omitempty"` // dropped at v4: digests left the schema
}

type docFileV2 struct {
	Size       int64             `json:"s"`
	Chunks     []int64           `json:"cs,omitempty"`
	Symlink    string            `json:"sl,omitempty"`
	UploadedAt int64             `json:"ua"`
	ModifiedAt int64             `json:"ma,omitempty"`
	AccessedAt int64             `json:"aa,omitempty"`
	ChangedAt  int64             `json:"ca,omitempty"`
	Mode       uint32            `json:"md,omitempty"`
	UID        uint32            `json:"u,omitempty"`
	GID        uint32            `json:"g,omitempty"`
	Inode      uint64            `json:"i,omitempty"`
	XAttrs     XAttrMap          `json:"x,omitempty"` // base64 since the compact era began
}

type docDirV2 struct {
	CreatedAt  int64             `json:"ca"`
	ModifiedAt int64             `json:"ma"`
	AccessedAt int64             `json:"aa,omitempty"`
	ChangedAt  int64             `json:"cha,omitempty"`
	Mode       uint32            `json:"m,omitempty"`
	UID        uint32            `json:"u,omitempty"`
	GID        uint32            `json:"g,omitempty"`
	Inode      uint64            `json:"i,omitempty"`
	XAttrs     XAttrMap          `json:"x,omitempty"`
}

type docTopV2 struct {
	V          int                   `json:"v"`
	Project    string                `json:"p"`
	TotalFiles int                   `json:"tf"`
	TotalSize  int64                 `json:"ts"`
	LastMod    int64                 `json:"lm"`
	Root       docDirV2              `json:"rt"`
	Dirs       map[string]docDirV2   `json:"d,omitempty"`
	Files      map[string]docFileV2  `json:"f,omitempty"`
	Chunks     map[int64]docChunk    `json:"c,omitempty"`
	Releases   map[string]docRelease `json:"r,omitempty"`
}

type docFileV3 struct {
	docFileV2
	TimesExplicit bool `json:"tsx,omitempty"` // authoritative-zero marker; consumed by v4
}

type docDirV3 struct {
	docDirV2
	TimesExplicit bool `json:"tsx,omitempty"`
}

type docTopV3 struct {
	V           int                   `json:"v"`
	Project     string                `json:"p"`
	TotalFiles  int                   `json:"tf"`
	TotalSize   int64                 `json:"ts"`
	LastMod     int64                 `json:"lm"`
	Root        docDirV3              `json:"rt"`
	Dirs        map[string]docDirV3   `json:"d,omitempty"`
	Files       map[string]docFileV3  `json:"f,omitempty"`
	Chunks      map[int64]docChunk    `json:"c,omitempty"`
	Releases    map[string]docRelease `json:"r,omitempty"`
	NextInode   uint64                `json:"ni,omitempty"` // persisted since v3
	NextChunkID int64                 `json:"nc,omitempty"`
}

// ---------------------------------------------------------------------------
// v1 -> v2: restructure the verbose first format into compact maps.
// ---------------------------------------------------------------------------

func migrateV1ToV2(data []byte) ([]byte, error) {
	m := newBareRepoMetadata()
	if err := m.migrateV1(data); err != nil {
		return nil, err
	}
	out := docTopV2{
		V: 2, Project: m.Project,
		TotalFiles: m.TotalFiles, TotalSize: m.TotalSize, LastMod: m.LastMod,
		Root:     dirToV2(m.Root),
		Dirs:     make(map[string]docDirV2, len(m.Dirs)),
		Files:    make(map[string]docFileV2, len(m.Files)),
		Chunks:   make(map[int64]docChunk, len(m.Chunks)),
		Releases: make(map[string]docRelease, len(m.Releases)),
	}
	for path, d := range m.Dirs {
		out.Dirs[path] = dirToV2(d)
	}
	for path, f := range m.Files {
		out.Files[path] = fileToV2(f)
	}
	for id, c := range m.Chunks {
		out.Chunks[id] = chunkToDoc(c)
	}
	for tag, r := range m.Releases {
		out.Releases[tag] = docRelease{AssetCount: r.AssetCount, CreatedAt: r.CreatedAt}
	}
	return json.Marshal(out)
}

func newBareRepoMetadata() *RepoMetadata {
	m := &RepoMetadata{}
	m.Dirs = make(map[string]DirMeta)
	m.Files = make(map[string]FileMeta)
	m.Chunks = make(map[int64]ChunkInfo)
	m.Releases = make(map[string]ReleaseRef)
	return m
}

func dirToV2(d DirMeta) docDirV2 {
	return docDirV2{
		CreatedAt: d.CreatedAt, ModifiedAt: d.ModifiedAt, AccessedAt: d.AccessedAt,
		ChangedAt: d.ChangedAt, Mode: d.Mode, UID: d.UID, GID: d.GID,
		Inode: d.Inode, XAttrs: d.XAttrs.Clone(),
	}
}

func fileToV2(f FileMeta) docFileV2 {
	return docFileV2{
		Size: f.Size, Chunks: f.Chunks, Symlink: f.Symlink,
		UploadedAt: f.UploadedAt, ModifiedAt: f.ModifiedAt,
		AccessedAt: f.AccessedAt, ChangedAt: f.ChangedAt,
		Mode: f.Mode, UID: f.UID, GID: f.GID, Inode: f.Inode,
		XAttrs: f.XAttrs.Clone(),
	}
}

func chunkToDoc(c ChunkInfo) docChunk {
	return docChunk{Size: c.Size, Offset: c.Offset, Release: c.Release,
		AssetOffset: c.AssetOffset, AssetID: c.AssetID, Digest: c.Digest}
}

// ---------------------------------------------------------------------------
// v2 -> v3: owners materialized, string xattrs became base64 bytes, inode/
// chunk counters became persisted state.
// ---------------------------------------------------------------------------

func migrateV2ToV3(data []byte) ([]byte, error) {
	var in docTopV2
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("decode v2: %w", err)
	}
	uid, _ := defaultOwnerIDs()
	materialize := func(owner *uint32) {
		if *owner == 0 && uid != 0 {
			*owner = uid
		}
	}

	out := docTopV3{
		V: 3, Project: in.Project,
		TotalFiles: in.TotalFiles, TotalSize: in.TotalSize, LastMod: in.LastMod,
		Root:     dirV2ToV3(in.Root),
		Dirs:     make(map[string]docDirV3, len(in.Dirs)),
		Files:    make(map[string]docFileV3, len(in.Files)),
		Chunks:   in.Chunks,
		Releases: in.Releases,
	}
	materialize(&out.Root.UID)
	maxInode := out.Root.Inode
	for path, d := range in.Dirs {
		dv3 := dirV2ToV3(d)
		materialize(&dv3.UID)
		if dv3.Inode > maxInode {
			maxInode = dv3.Inode
		}
		out.Dirs[path] = dv3
	}
	for path, f := range in.Files {
		fv3 := fileV2ToV3(f)
		materialize(&fv3.UID)
		if fv3.Inode > maxInode {
			maxInode = fv3.Inode
		}
		out.Files[path] = fv3
	}
	maxChunk := int64(0)
	for id := range out.Chunks {
		if id > maxChunk {
			maxChunk = id
		}
	}
	// +1 mirrors allocateInode/allocateChunkID semantics: the persisted
	// counter is the NEXT identifier to mint.
	out.NextInode = maxInode + 1
	out.NextChunkID = maxChunk + 1
	return json.Marshal(out)
}

func dirV2ToV3(d docDirV2) docDirV3 {
	return docDirV3{docDirV2: d}
}

func fileV2ToV3(f docFileV2) docFileV3 {
	return docFileV3{docFileV2: f}
}

// ---------------------------------------------------------------------------
// v3 -> v4: unambiguous timestamp keys (cr=created / ch=changed uniformly),
// digest field dropped, timestamps completed deterministically for entries
// that predate the authoritative-zero marker.
// ---------------------------------------------------------------------------

func migrateV3ToV4(data []byte) ([]byte, error) {
	var in docTopV3
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("decode v3: %w", err)
	}
	m := newBareRepoMetadata()
	m.Project = in.Project
	m.TotalFiles = in.TotalFiles
	m.TotalSize = in.TotalSize
	m.LastMod = in.LastMod
	m.Root = dirV3ToV4(in.Root, in.LastMod)
	for path, d := range in.Dirs {
		m.Dirs[path] = dirV3ToV4(d, in.LastMod)
	}
	for path, f := range in.Files {
		fv4 := fileV3ToV4(f, in.LastMod)
		m.Files[path] = fv4
		for _, id := range fv4.Chunks {
			c, ok := in.Chunks[id]
			if !ok {
				// Repair, not fatal: a dangling reference could never
				// download anyway; keeping it would only trip Validate.
				continue
			}
			// Digest deliberately dropped: it left the schema at v4.
			m.Chunks[id] = ChunkInfo{Size: c.Size, Offset: c.Offset,
				Release: c.Release, AssetOffset: c.AssetOffset, AssetID: c.AssetID}
		}
	}
	for tag, r := range in.Releases {
		m.Releases[tag] = ReleaseRef{AssetCount: r.AssetCount, CreatedAt: r.CreatedAt}
	}
	m.NextInode = in.NextInode
	m.NextChunkID = in.NextChunkID
	m.Version = CurrentVersion
	return json.Marshal(m)
}

// completeTimes fills zero-valued timestamps deterministically from the
// entry's own UploadedAt (then ModifiedAt), finally from the document's
// LastMod — the same chains the old load-time repair used, minus the wall
// clock. Entries flagged TimesExplicit are already authoritative and pass
// through untouched.
func completeTimes(uploaded, modified, accessed, changed, fallback int64, explicit bool) (int64, int64, int64, int64) {
	if explicit {
		return uploaded, modified, accessed, changed
	}
	if uploaded == 0 {
		uploaded = fallback
	}
	if modified == 0 {
		modified = uploaded
	}
	if accessed == 0 {
		accessed = modified
	}
	if changed == 0 {
		changed = modified
	}
	return uploaded, modified, accessed, changed
}

func completeMode(mode uint32, dir bool) uint32 {
	if mode != 0 {
		return mode
	}
	if dir {
		return defaultDirMode()
	}
	return defaultFileMode(NodeKindFile)
}

func dirV3ToV4(d docDirV3, lastMod int64) DirMeta {
	cr, ma, aa, ch := completeTimes(d.CreatedAt, d.ModifiedAt, d.AccessedAt, d.ChangedAt, lastMod, d.TimesExplicit)
	return DirMeta{
		CreatedAt: cr, ModifiedAt: ma, AccessedAt: aa, ChangedAt: ch,
		Mode: completeMode(d.Mode, true), UID: d.UID, GID: d.GID,
		Inode: d.Inode, XAttrs: d.XAttrs.Clone(),
	}
}

func fileV3ToV4(f docFileV3, lastMod int64) FileMeta {
	up, ma, aa, ch := completeTimes(f.UploadedAt, f.ModifiedAt, f.AccessedAt, f.ChangedAt, lastMod, f.TimesExplicit)
	out := FileMeta{
		Size: f.Size, Chunks: f.Chunks, Symlink: f.Symlink,
		UploadedAt: up, ModifiedAt: ma, AccessedAt: aa, ChangedAt: ch,
		Mode: completeMode(f.Mode, false), UID: f.UID, GID: f.GID,
		Inode: f.Inode, XAttrs: f.XAttrs.Clone(),
	}
	if out.Symlink != "" {
		// v1/v2-era links could carry stale size/chunk residue; the v4
		// contract is pure link data.
		out.Size = int64(len(out.Symlink))
		out.Chunks = []int64{}
	}
	if out.Chunks == nil {
		out.Chunks = []int64{}
	}
	return out
}
