package metadata

import (
	"encoding/json"
	"fmt"
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
	// Digest is the hex-encoded SHA-256 of the chunk payload. Optional;
	// populated by newer uploads and verified by download paths.
	Digest string `json:"d,omitempty"`
}

// XAttrMap holds extended attributes as raw bytes. JSON values are base64
// encoded. Decoding also accepts legacy plain-string values from v1/v2
// metadata so old payloads migrate losslessly.
type XAttrMap map[string][]byte

func (x *XAttrMap) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*x = nil
		return nil
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	decoded := make(XAttrMap, len(raw))
	for k, v := range raw {
		var b []byte
		if err := json.Unmarshal(v, &b); err == nil {
			decoded[k] = b
			continue
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return fmt.Errorf("xattr %s: %w", k, err)
		}
		decoded[k] = []byte(s)
	}
	*x = decoded
	return nil
}

func (x XAttrMap) MarshalJSON() ([]byte, error) {
	if x == nil {
		return []byte("null"), nil
	}
	return json.Marshal(map[string][]byte(x))
}

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
	Chunks     []int64  `json:"cs,omitempty"`
	Size       int64    `json:"s"`
	Symlink    string   `json:"sl,omitempty"`
	UploadedAt int64    `json:"ua"`
	ModifiedAt int64    `json:"ma,omitempty"`
	AccessedAt int64    `json:"aa,omitempty"`
	ChangedAt  int64    `json:"ca,omitempty"`
	Mode       uint32   `json:"md,omitempty"`
	UID        uint32   `json:"u,omitempty"`
	GID        uint32   `json:"g,omitempty"`
	Inode      uint64   `json:"i,omitempty"`
	XAttrs     XAttrMap `json:"x,omitempty"`
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

func (d DirMeta) Clone() DirMeta {
	clone := d
	clone.XAttrs = d.XAttrs.Clone()
	return clone
}

type ReleaseRef struct {
	AssetCount int   `json:"ac"`
	CreatedAt  int64 `json:"ca"`
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
	NextInode    uint64              `json:"ni,omitempty"`
	NextChunkID  int64               `json:"nc,omitempty"`
	filesByInode map[uint64][]string `json:"-"`
	childDirs    map[string][]string `json:"-"`
	childFiles   map[string][]string `json:"-"`
}

type MetadataRevision struct {
	CommitSHA   string `json:"commit_sha"`
	Message     string `json:"message"`
	CommittedAt int64  `json:"committed_at"`
}

func NewRepoMetadata(project string) *RepoMetadata {
	now := time.Now().Unix()
	uid, gid := defaultOwnerIDs()
	return &RepoMetadata{
		Version:     3,
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

func (m RepoMetadata) Clone() RepoMetadata {
	clone := m
	clone.Dirs = cloneDirMetaMap(m.Dirs)
	clone.Files = cloneFileMetaMap(m.Files)
	clone.Chunks = cloneChunkInfoMap(m.Chunks)
	clone.Releases = cloneReleaseRefMap(m.Releases)
	clone.Root = m.Root.Clone()
	clone.filesByInode = nil
	clone.childDirs = nil
	clone.childFiles = nil
	return clone
}

func cloneDirMetaMap(src map[string]DirMeta) map[string]DirMeta {
	if src == nil {
		return nil
	}
	dst := make(map[string]DirMeta, len(src))
	for k, v := range src {
		dst[k] = v.Clone()
	}
	return dst
}

func cloneFileMetaMap(src map[string]FileMeta) map[string]FileMeta {
	if src == nil {
		return nil
	}
	dst := make(map[string]FileMeta, len(src))
	for k, v := range src {
		dst[k] = v.Clone()
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
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	return data, nil
}

// maxMetadataVersion is the newest schema version this build reads and writes.
const maxMetadataVersion = 3

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

// FromJSON parses metadata, migrating older schema versions forward. It fails
// loudly on payloads that are corrupt (no version) or from a newer schema;
// it never guesses.
func (m *RepoMetadata) FromJSON(data []byte) error {
	var raw struct {
		V       int `json:"v"`
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("parse metadata: %w", err)
	}
	version := raw.V
	if version == 0 {
		version = raw.Version
	}
	switch {
	case version == 0:
		return fmt.Errorf("metadata payload has no version field; refusing to guess the schema")
	case version > maxMetadataVersion:
		return fmt.Errorf("metadata version %d is newer than supported version %d", version, maxMetadataVersion)
	case version < 2:
		if err := m.migrateV1(data); err != nil {
			return err
		}
	default:
		if err := json.Unmarshal(data, m); err != nil {
			return fmt.Errorf("unmarshal v%d metadata: %w", version, err)
		}
	}
	m.Version = maxMetadataVersion
	m.backfillLegacyOwners()
	return nil
}

// backfillLegacyOwners stamps process ownership onto entries loaded from
// older schemas whose owners were omitted. New writes always persist real
// owner IDs (including 0 for root), so this never runs on v3+ data.
func (m *RepoMetadata) backfillLegacyOwners() {
	uid, _ := defaultOwnerIDs()
	backfill := func(owner *uint32) {
		if *owner == 0 && uid != 0 {
			*owner = uid
		}
	}
	backfill(&m.Root.UID)
	for path, dir := range m.Dirs {
		backfill(&dir.UID)
		m.Dirs[path] = dir
	}
	for path, file := range m.Files {
		backfill(&file.UID)
		m.Files[path] = file
	}
}

func (m *RepoMetadata) Normalize(project string, now int64) {
	m.Version = maxMetadataVersion
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
	for path, dir := range m.Dirs {
		dir.Normalize(now)
		m.Dirs[path] = dir
	}
	for path, file := range m.Files {
		file.Normalize(now)
		m.Files[path] = file
	}
	m.sortFileChunksByOffset()
	for tag, ref := range m.Releases {
		if ref.CreatedAt == 0 {
			ref.CreatedAt = now
			m.Releases[tag] = ref
		}
	}
	m.RecomputeStats()
	if m.LastMod == 0 {
		m.LastMod = now
	}
	m.RebuildIndexes()
}

// sortFileChunksByOffset enforces the stored-order invariant every reader
// relies on: a file's chunk IDs are ordered by data offset, so binary search
// over FileChunks is valid.
func (m *RepoMetadata) sortFileChunksByOffset() {
	for path, file := range m.Files {
		if len(file.Chunks) < 2 {
			continue
		}
		sorted := append([]int64(nil), file.Chunks...)
		sort.SliceStable(sorted, func(i, j int) bool {
			return m.Chunks[sorted[i]].Offset < m.Chunks[sorted[j]].Offset
		})
		file.Chunks = sorted
		m.Files[path] = file
	}
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
		ref.AssetCount = assetCounts[tag]
		m.Releases[tag] = ref
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
// reclaimed the corresponding remote assets) — a rollback to an older
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
	return removed
}

func (d *DirMeta) Normalize(now int64) {
	if d.Mode == 0 {
		d.Mode = defaultDirMode()
	}
	if d.CreatedAt == 0 {
		d.CreatedAt = now
	}
	if d.ModifiedAt == 0 {
		d.ModifiedAt = d.CreatedAt
	}
	if d.AccessedAt == 0 {
		d.AccessedAt = d.ModifiedAt
	}
	if d.ChangedAt == 0 {
		d.ChangedAt = d.ModifiedAt
	}
	d.XAttrs = normalizeXAttrs(d.XAttrs)
}

func (f *FileMeta) Normalize(now int64) {
	if f.Mode == 0 {
		f.Mode = defaultFileMode(NodeKindFile)
	}
	if f.UploadedAt == 0 {
		f.UploadedAt = now
	}
	if f.ModifiedAt == 0 {
		f.ModifiedAt = f.UploadedAt
	}
	if f.AccessedAt == 0 {
		f.AccessedAt = f.ModifiedAt
	}
	if f.ChangedAt == 0 {
		f.ChangedAt = f.ModifiedAt
	}
	if f.Chunks == nil {
		f.Chunks = make([]int64, 0)
	}
	sort.Slice(f.Chunks, func(i, j int) bool { return f.Chunks[i] < f.Chunks[j] })
	if f.Symlink != "" {
		f.Chunks = make([]int64, 0)
		f.Size = int64(len([]byte(f.Symlink)))
	}
	f.XAttrs = normalizeXAttrs(f.XAttrs)
}

func (m *RepoMetadata) GetRelease(tag string) *ReleaseRef {
	if ref, ok := m.Releases[tag]; ok {
		return &ref
	}
	return nil
}

func (m *RepoMetadata) HasDirectory(path string) bool {
	if path == "" {
		return true
	}
	_, ok := m.Dirs[path]
	return ok
}

func (m *RepoMetadata) GetDirectory(path string) *DirMeta {
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
	m.Dirs[path] = DirMeta{
		CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now,
		Mode: defaultDirMode(), UID: uid, GID: gid, Inode: m.allocateInode(),
	}
	m.invalidateIndexes()
}

func (m *RepoMetadata) RemoveDirectory(path string) bool {
	path = normalizeStoredPath(path)
	if _, ok := m.Dirs[path]; !ok {
		return false
	}
	delete(m.Dirs, path)
	m.invalidateIndexes()
	return true
}

func (m *RepoMetadata) DirectoryChildren(path string) (dirs, files []string) {
	path = normalizeStoredPath(path)
	m.RebuildIndexes()
	return m.childDirs[path], m.childFiles[path]
}

func (m *RepoMetadata) EnsureRelease(tag string, createdAt int64) *ReleaseRef {
	if ref, ok := m.Releases[tag]; ok {
		return &ref
	}
	m.Releases[tag] = ReleaseRef{CreatedAt: createdAt}
	ref := m.Releases[tag]
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
	if existing, ok := m.Files[name]; ok {
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
	m.invalidateIndexes()
}

func (m *RepoMetadata) FindFile(name string) *FileMeta {
	name = normalizeStoredPath(name)
	if file, ok := m.Files[name]; ok {
		return &file
	}
	return nil
}

func (m *RepoMetadata) FindFilesByInode(inode uint64) []string {
	m.RebuildIndexes()
	names := m.filesByInode[inode]
	out := make([]string, len(names))
	copy(out, names)
	return out
}

func (m *RepoMetadata) RemoveFile(name string) bool {
	name = normalizeStoredPath(name)
	if _, ok := m.Files[name]; !ok {
		return false
	}
	delete(m.Files, name)
	m.invalidateIndexes()
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
	if _, ok := m.Releases[tag]; !ok {
		return false
	}
	delete(m.Releases, tag)
	m.invalidateIndexes()
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
	if path != "" {
		if _, ok := m.Dirs[path]; !ok {
			return 0
		}
	}
	if m.childDirs == nil {
		m.RebuildIndexes()
	}
	return 2 + len(m.childDirs[path])
}

func (m *RepoMetadata) FileNLink(name string) int {
	file, ok := m.Files[name]
	if !ok {
		return 0
	}
	return m.NLink(file.Inode)
}

func (m *RepoMetadata) invalidateIndexes() {
	m.filesByInode = nil
	m.childDirs = nil
	m.childFiles = nil
}

func (m *RepoMetadata) RebuildIndexes() {
	m.filesByInode = make(map[uint64][]string, len(m.Files))
	m.childDirs = make(map[string][]string, len(m.Dirs)+1)
	m.childFiles = make(map[string][]string, len(m.Files)+1)

	for path := range m.Dirs {
		parent := parentPath(path)
		m.childDirs[parent] = append(m.childDirs[parent], path)
	}
	for path, file := range m.Files {
		m.filesByInode[file.Inode] = append(m.filesByInode[file.Inode], path)
		parent := parentPath(path)
		m.childFiles[parent] = append(m.childFiles[parent], path)
	}
	for parent := range m.childDirs {
		stableSortStrings(m.childDirs[parent])
	}
	for parent := range m.childFiles {
		stableSortStrings(m.childFiles[parent])
	}
}

func (m *RepoMetadata) NLink(inode uint64) int {
	if m.filesByInode == nil {
		m.RebuildIndexes()
	}
	return len(m.filesByInode[inode])
}

func preserveFileIdentity(file *FileMeta, existing *FileMeta, now int64) {
	// A type change (regular file <-> symlink) replaces the whole node rather
	// than updating it: no identity carries over. In particular the old
	// symlink target must never leak onto a regular file — Normalize treats
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
	uid, gid := defaultOwnerIDs()
	if file.Inode == 0 {
		file.Inode = meta.allocateInode()
	}
	if file.Mode == 0 {
		file.Mode = defaultFileMode(NodeKindFile)
	}
	// Owner IDs are always materialized at creation (0 legitimately means
	// root); they are never re-stamped afterwards.
	if file.UID == 0 {
		file.UID = uid
	}
	if file.GID == 0 {
		file.GID = gid
	}
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
	if m.Root.CreatedAt == 0 {
		m.Root.CreatedAt = now
	}
	if m.Root.ModifiedAt == 0 {
		m.Root.ModifiedAt = m.Root.CreatedAt
	}
	if m.Root.AccessedAt == 0 {
		m.Root.AccessedAt = m.Root.ModifiedAt
	}
	if m.Root.ChangedAt == 0 {
		m.Root.ChangedAt = m.Root.ModifiedAt
	}
	m.Root.XAttrs = normalizeXAttrs(m.Root.XAttrs)

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
		seenChunk := map[int64]struct{}{}
		var prevOffset int64 = -1
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
			prevOffset = chunk.Offset
			if file.Symlink == "" && chunk.Offset+chunk.Size > file.Size {
				return fmt.Errorf("file %s: chunk data extends beyond file size (%d > %d)", path, chunk.Offset+chunk.Size, file.Size)
			}
		}
	}

	if m.TotalFiles != totalFiles {
		return fmt.Errorf("metadata total files mismatch: expected %d, got %d", totalFiles, m.TotalFiles)
	}
	if m.TotalSize != totalSize {
		return fmt.Errorf("metadata total size mismatch: expected %d, got %d", totalSize, m.TotalSize)
	}

	// Note: chunk.Release tags are intentionally allowed to reference tags
	// absent from the Releases catalog — DeleteRelease hides catalog entries
	// while live chunks keep pointing at them, and PurgeUntracked treats any
	// chunk-referenced release as tracked.

	for tag, ref := range m.Releases {
		if ref.AssetCount < 0 {
			return fmt.Errorf("release %s has negative asset count %d", tag, ref.AssetCount)
		}
	}

	return nil
}

// validateStoredPathKey rejects map keys that can never be produced by the
// path normalizer: absolute paths, empty segments, and ".." traversal.
func validateStoredPathKey(path string) error {
	if path == "" || strings.HasPrefix(path, "/") {
		return fmt.Errorf("invalid stored path %q", path)
	}
	if path == ".." || strings.HasPrefix(path, "../") || strings.Contains(path, "/../") {
		return fmt.Errorf("stored path escapes root: %q", path)
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

	m.Version = maxMetadataVersion
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
	if _, ok := m.Files[name]; !ok {
		return false
	}
	m.Files[name] = file.Clone()
	return true
}
