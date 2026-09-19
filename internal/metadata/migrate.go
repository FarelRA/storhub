package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// CurrentVersion is the newest document version this build reads and writes
// (5, the split layout). The pure blob migrators below only ever produce
// maxBlobVersion (4); the 4->5 step is a write-time layout split, not a bytes
// transform.
const CurrentVersion = maxMetadataVersion

// versionProbe is the single version/shape envelope for every JSON document
// the package reads: v1 ("version"), v2-v4 ("v"), and v5 split manifests
// ("v" plus a non-empty "tr"). One struct replaces the three triplicated
// anonymous probes in detectVersion, UnmarshalJSON, and IsManifest.
type versionProbe struct {
	V        *int   `json:"v"`
	Version  *int   `json:"version"` // v1-era spelling, consumed here only
	TreeRoot string `json:"tr"`
}

// probeVersion decodes only the version/shape envelope of data.
func probeVersion(data []byte) (versionProbe, error) {
	var probe versionProbe
	if err := json.Unmarshal(data, &probe); err != nil {
		return probe, fmt.Errorf("metadata version probe failed: %w", err)
	}
	return probe, nil
}

// detectVersion reports a document's schema version. Historical spellings:
// v1 documents wrote "version"; v2 onward write "v". That history belongs to
// the migrator alone - the main parser never sees version detection. A
// split-index manifest is distinguished from a blob by IsManifest, not here.
func detectVersion(data []byte) (int, error) {
	probe, err := probeVersion(data)
	if err != nil {
		return 0, err
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
// no clock, no I/O, deterministic output for a given input. There is no
// 4->5 step: that boundary is the write-time layout split (it emits objects),
// not a blob transform.
var migrators = [...]func([]byte) ([]byte, error){
	1: migrateV1ToV2,
	2: migrateV2ToV3,
	3: migrateV3ToV4,
}

// Migrate upgrades a serialized metadata BLOB to the current blob schema by
// applying every required step in order; a document already current (version
// maxBlobVersion) passes through unchanged. A split-index manifest is
// rejected: it loads through ParseManifest/LoadTree, never as a blob - and so
// is any version-5 document lacking a tree root, which is a truncated
// manifest, not a blob (v5 documents are manifests; blobs are v<=4). Loading
// is eager: every blob parse funnels through here, so no code path outside
// this file can observe an older schema shape. The upgraded document persists
// when the next mutation commits it (as a version-5 split).
func Migrate(data []byte) ([]byte, int, error) {
	if IsManifest(data) {
		return nil, maxMetadataVersion, fmt.Errorf("metadata version %d is the split-index manifest; load it via ParseManifest, not Migrate", maxMetadataVersion)
	}
	from, err := detectVersion(data)
	if err != nil {
		return nil, 0, err
	}
	if from > maxMetadataVersion {
		return nil, from, fmt.Errorf("metadata version %d is newer than supported version %d", from, maxMetadataVersion)
	}
	if from < 1 {
		return nil, from, fmt.Errorf("invalid metadata version %d", from)
	}
	if from == maxMetadataVersion {
		return nil, from, fmt.Errorf("metadata version %d with no tree root is a corrupt split-index manifest, not a blob; blobs are v%d or older", maxMetadataVersion, maxBlobVersion)
	}
	if from == maxBlobVersion {
		// Version 4 is the newest single-blob schema; the 4->5 step is the
		// write-time layout split, not a blob transform.
		return data, from, nil
	}
	for v := from; v < maxBlobVersion; v++ {
		step := migrators[v]
		if step == nil {
			return nil, v, fmt.Errorf("no migration path from metadata version %d", v)
		}
		if data, err = step(data); err != nil {
			return nil, v, fmt.Errorf("migrate metadata v%d->v%d: %w", v, v+1, err)
		}
	}
	return data, maxBlobVersion, nil
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
	Size       int64    `json:"s"`
	Chunks     []int64  `json:"cs,omitempty"`
	Symlink    string   `json:"sl,omitempty"`
	UploadedAt int64    `json:"ua"`
	ModifiedAt int64    `json:"ma,omitempty"`
	AccessedAt int64    `json:"aa,omitempty"`
	ChangedAt  int64    `json:"ca,omitempty"`
	Mode       uint32   `json:"md,omitempty"`
	UID        uint32   `json:"u,omitempty"`
	GID        uint32   `json:"g,omitempty"`
	Inode      uint64   `json:"i,omitempty"`
	XAttrs     XAttrMap `json:"x,omitempty"` // base64 since the compact era began
}

type docDirV2 struct {
	CreatedAt  int64    `json:"ca"`
	ModifiedAt int64    `json:"ma"`
	AccessedAt int64    `json:"aa,omitempty"`
	ChangedAt  int64    `json:"cha,omitempty"`
	Mode       uint32   `json:"m,omitempty"`
	UID        uint32   `json:"u,omitempty"`
	GID        uint32   `json:"g,omitempty"`
	Inode      uint64   `json:"i,omitempty"`
	XAttrs     XAttrMap `json:"x,omitempty"`
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
		Dirs:     make(map[string]docDirV2, len(m.dirs)),
		Files:    make(map[string]docFileV2, len(m.files)),
		Chunks:   make(map[int64]docChunk, len(m.chunks)),
		Releases: make(map[string]docRelease, len(m.releases)),
	}
	for path, d := range m.dirs {
		out.Dirs[path] = dirToV2(d)
	}
	for path, f := range m.files {
		out.Files[path] = fileToV2(f)
	}
	for id, c := range m.chunks {
		out.Chunks[id] = chunkToDoc(c)
	}
	for tag, r := range m.releases {
		out.Releases[tag] = docRelease(r)
	}
	return json.Marshal(out)
}

// ---------------------------------------------------------------------------
// v1 document shape + loader: the verbose first format, restructured into
// the flat model by migrateV1ToV2 through the parse/seed/synthesize
// pipeline below (not in metadata.go: this is migration, not the live
// model).
// ---------------------------------------------------------------------------

// v1Document mirrors exactly what version 1 wrote on disk, so a fixture of
// that era decodes faithfully.
type v1Document struct {
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

// parseV1Document decodes a v1 payload into the era document.
func parseV1Document(data []byte) (*v1Document, error) {
	var v1 v1Document
	if err := json.Unmarshal(data, &v1); err != nil {
		return nil, fmt.Errorf("unmarshal v1 metadata: %w", err)
	}
	return &v1, nil
}

// maxInode returns the highest inode the v1 document declares anywhere
// (root, declared counter, directories, files), so the counter can be seeded
// past it.
func (v1 *v1Document) maxInode() uint64 {
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
	return maxInode
}

// synthesizeV1Parents creates any directory components path needs but the v1
// document never listed, so Validate cannot trip over dangling parents.
// Synthesized entries are canonical (0755, fully stamped); the +1 seeding in
// migrateV1 guarantees the allocations below never collide.
func (m *RepoMetadata) synthesizeV1Parents(path string, now int64) {
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
			Mode:       defaultDirMode(),
		}
	}
}

func (m *RepoMetadata) migrateV1(data []byte) error {
	v1, err := parseV1Document(data)
	if err != nil {
		return err
	}

	m.Version = maxBlobVersion
	m.Project = v1.Project
	m.TotalFiles = v1.TotalFiles
	m.TotalSize = v1.TotalSize
	m.LastMod = timeToUnix(v1.LastModified)
	// Seed the inode counter past everything the document declares so
	// allocations during migration (synthesized parents) can never mint a
	// colliding or zero inode. +1: allocateInode returns the counter and
	// then increments, so seeding with maxInode itself would mint a
	// duplicate of the highest inode.
	m.NextInode = v1.maxInode() + 1
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

	now := timeToUnix(v1.LastModified)

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
			m.synthesizeV1Parents(f.Name, now)
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
		m.synthesizeV1Parents(dir, now)
	}

	// The v1 counters may be stale or absent; recompute from what actually
	// migrated so Validate compares against reality.
	m.RecomputeStats()
	return nil
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
		AssetOffset: c.AssetOffset, AssetID: c.AssetID}
}

// ---------------------------------------------------------------------------
// v2 -> v3: string xattrs became base64 bytes, inode/chunk counters became
// persisted state.
//
// Owner IDs copy verbatim: 0 means root, never unset. An earlier revision
// materialized zero IDs into the daemon process user, silently reassigning
// root-owned entries to whoever ran the migration (the same
// zero-confusion class as the file-stamping regression documented on
// initializeNewFileIdentityFields). Like the v1 and v3 to v4 steps, this
// migrator must not invent owners.
// ---------------------------------------------------------------------------

func migrateV2ToV3(data []byte) ([]byte, error) {
	var in docTopV2
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("decode v2: %w", err)
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
	maxInode := out.Root.Inode
	for path, d := range in.Dirs {
		dv3 := dirV2ToV3(d)
		if dv3.Inode > maxInode {
			maxInode = dv3.Inode
		}
		out.Dirs[path] = dv3
	}
	for path, f := range in.Files {
		fv3 := fileV2ToV3(f)
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
		m.dirs[path] = dirV3ToV4(d, in.LastMod)
	}
	for path, f := range in.Files {
		fv4 := fileV3ToV4(f, in.LastMod)
		if len(fv4.Chunks) > 0 {
			kept := make([]int64, 0, len(fv4.Chunks))
			var end int64
			for _, id := range fv4.Chunks {
				c, ok := in.Chunks[id]
				if !ok {
					// Repair for real: the record is gone, so the bytes it
					// claimed are unrecoverable. Strip the dead reference
					// and shrink the declared size to what survives - a
					// kept id with no record would trip Validate and make
					// the whole project permanently unloadable.
					continue
				}
				kept = append(kept, id)
				// Digest deliberately dropped: it left the schema at v4.
				m.chunks[id] = ChunkInfo{Size: c.Size, Offset: c.Offset,
					Release: c.Release, AssetOffset: c.AssetOffset, AssetID: c.AssetID}
				// Overflow-safe end: a naive sum wraps negative at extreme
				// offsets and would shrink the size to the wrong value.
				if e, ok := checkedAdd(c.Offset, c.Size); ok && e > end {
					end = e
				} else if !ok && end < int64(^uint64(0)>>1) {
					end = int64(^uint64(0) >> 1)
				}
			}
			if len(kept) != len(fv4.Chunks) {
				fv4.Chunks = kept
				if len(kept) == 0 {
					m.TotalSize -= fv4.Size
					fv4.Size = 0
				} else if end < fv4.Size {
					m.TotalSize -= fv4.Size - end
					fv4.Size = end
				}
			}
		}
		m.files[path] = fv4
	}
	for tag, r := range in.Releases {
		m.releases[tag] = ReleaseRef(r)
	}
	m.NextInode = in.NextInode
	m.NextChunkID = in.NextChunkID
	m.Version = maxBlobVersion
	// Reconcile every derived counter from the walked content: stripping
	// dangling chunk refs above shrinks file sizes (TotalSize adjusted
	// inline) but leaves the per-release AssetCounts stale, and a v3
	// document may already carry drifted totals. RecomputeStats rewrites
	// TotalFiles/TotalSize from the surviving entries, fixes each ref's
	// AssetCount from the chunk walk, and rebuilds pendingAssets; the
	// counter reconciliation then raises any regressed allocation floors
	// past the live ids. Version is already maxBlobVersion so it is
	// preserved.
	m.RecomputeStats()
	m.reconcileCounters()
	return json.Marshal(m)
}

// checkedAdd returns a+b and whether it fit: chunk offsets near MaxInt64
// wrap negative with a naive sum and would corrupt size/overlap math.
func checkedAdd(a, b int64) (int64, bool) {
	if b > 0 && a > int64(^uint64(0)>>1)-b {
		return 0, false
	}
	if b < 0 && a < int64(-1<<63)-b {
		return 0, false
	}
	return a + b, true
}

// incompleteTimes is the input to completeTimes: an entry's stored
// timestamps plus the document LastMod fallback and the authoritative-zero
// marker. One struct replaces the 5×int64 + bool flag soup whose call sites
// were unreadable positional argument lists.
type incompleteTimes struct {
	uploaded int64
	modified int64
	accessed int64
	changed  int64
	fallback int64
	explicit bool
}

// completeTimes fills zero-valued timestamps deterministically from the
// entry's own UploadedAt (then ModifiedAt), finally from the document's
// LastMod - the same chains the old load-time repair used, minus the wall
// clock. Entries flagged TimesExplicit are already authoritative and pass
// through untouched.
func completeTimes(in incompleteTimes) (uploaded, modified, accessed, changed int64) {
	uploaded, modified, accessed, changed = in.uploaded, in.modified, in.accessed, in.changed
	if in.explicit {
		return uploaded, modified, accessed, changed
	}
	if uploaded == 0 {
		uploaded = in.fallback
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

// modeOrDefault materializes a zero mode deterministically: directories get
// 0755, symlinks 0777, regular files 0644. It subsumes the old
// completeMode/fileModeOrDefault pair, whose only difference was which
// default the zero case picked.
func modeOrDefault(mode uint32, kind NodeKind, dir bool) uint32 {
	if mode != 0 {
		return mode
	}
	if dir {
		return defaultDirMode()
	}
	return defaultFileMode(kind)
}

func dirV3ToV4(d docDirV3, lastMod int64) DirMeta {
	cr, ma, aa, ch := completeTimes(incompleteTimes{
		uploaded: d.CreatedAt, modified: d.ModifiedAt, accessed: d.AccessedAt,
		changed: d.ChangedAt, fallback: lastMod, explicit: d.TimesExplicit,
	})
	return DirMeta{
		CreatedAt: cr, ModifiedAt: ma, AccessedAt: aa, ChangedAt: ch,
		Mode: modeOrDefault(d.Mode, NodeKindFile, true), UID: d.UID, GID: d.GID,
		Inode: d.Inode, XAttrs: d.XAttrs.Clone(),
	}
}

func fileV3ToV4(f docFileV3, lastMod int64) FileMeta {
	up, ma, aa, ch := completeTimes(incompleteTimes{
		uploaded: f.UploadedAt, modified: f.ModifiedAt, accessed: f.AccessedAt,
		changed: f.ChangedAt, fallback: lastMod, explicit: f.TimesExplicit,
	})
	// A v3-era link with no stored mode must materialize as a link, not a
	// regular file.
	kind := NodeKindFile
	if f.Symlink != "" {
		kind = NodeKindSymlink
	}
	out := FileMeta{
		Size: f.Size, Chunks: f.Chunks, Symlink: f.Symlink,
		UploadedAt: up, ModifiedAt: ma, AccessedAt: aa, ChangedAt: ch,
		Mode: modeOrDefault(f.Mode, kind, false), UID: f.UID, GID: f.GID,
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
