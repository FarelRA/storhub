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
	Offset      int64  `json:"o"`
	Release     string `json:"r"`
	AssetOffset int64  `json:"ao"`
	AssetID     int64  `json:"a"`
}

type FileMeta struct {
	Chunks     []string          `json:"cs,omitempty"`
	Size       int64             `json:"s"`
	Symlink    string            `json:"sl,omitempty"`
	UploadedAt int64             `json:"ua"`
	ModifiedAt int64             `json:"ma,omitempty"`
	AccessedAt int64             `json:"aa,omitempty"`
	ChangedAt  int64             `json:"ca,omitempty"`
	Mode       uint32            `json:"md,omitempty"`
	UID        uint32            `json:"u,omitempty"`
	GID        uint32            `json:"g,omitempty"`
	Inode      uint64            `json:"i,omitempty"`
	XAttrs     map[string]string `json:"x,omitempty"`
}

func (f FileMeta) Clone() FileMeta {
	clone := f
	if f.Chunks != nil {
		clone.Chunks = append([]string(nil), f.Chunks...)
	}
	clone.XAttrs = cloneStringMap(f.XAttrs)
	return clone
}

type DirMeta struct {
	CreatedAt  int64             `json:"ca"`
	ModifiedAt int64             `json:"ma"`
	AccessedAt int64             `json:"aa,omitempty"`
	ChangedAt  int64             `json:"cha,omitempty"`
	Mode       uint32            `json:"m,omitempty"`
	UID        uint32            `json:"u,omitempty"`
	GID        uint32            `json:"g,omitempty"`
	Inode      uint64            `json:"i,omitempty"`
	XAttrs     map[string]string `json:"x,omitempty"`
}

func (d DirMeta) Clone() DirMeta {
	clone := d
	clone.XAttrs = cloneStringMap(d.XAttrs)
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
	Chunks     map[string]ChunkInfo  `json:"c"`
	Releases   map[string]ReleaseRef `json:"r"`

	NextInode    uint64              `json:"-"`
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
		Version:   2,
		Project:   project,
		NextInode: 2,
		Root: DirMeta{
			Inode: 1, Mode: defaultDirMode(), UID: uid, GID: gid,
			CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now,
		},
		Dirs:     make(map[string]DirMeta),
		Files:    make(map[string]FileMeta),
		Chunks:   make(map[string]ChunkInfo),
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

func cloneChunkInfoMap(src map[string]ChunkInfo) map[string]ChunkInfo {
	if src == nil {
		return nil
	}
	dst := make(map[string]ChunkInfo, len(src))
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

func (m *RepoMetadata) FromJSON(data []byte) error {
	var raw struct {
		Version int `json:"v"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Version < 2 {
		return m.migrateV1(data)
	}
	return json.Unmarshal(data, m)
}

func (m *RepoMetadata) Normalize(project string, now int64) {
	m.Version = 2
	m.Project = chooseNonEmpty(m.Project, project)
	m.normalizeRoot(now)
	if m.Dirs == nil {
		m.Dirs = make(map[string]DirMeta)
	}
	if m.Files == nil {
		m.Files = make(map[string]FileMeta)
	}
	if m.Chunks == nil {
		m.Chunks = make(map[string]ChunkInfo)
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
		m.Version = 2
	}
	m.RebuildIndexes()
}

func (d *DirMeta) Normalize(now int64) {
	uid, gid := defaultOwnerIDs()
	if d.Mode == 0 {
		d.Mode = defaultDirMode()
	}
	if d.UID == 0 && uid != 0 {
		d.UID = uid
	}
	if d.GID == 0 && gid != 0 {
		d.GID = gid
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
	uid, gid := defaultOwnerIDs()
	if f.Mode == 0 {
		f.Mode = defaultFileMode(NodeKindFile)
	}
	if f.UID == 0 && uid != 0 {
		f.UID = uid
	}
	if f.GID == 0 && gid != 0 {
		f.GID = gid
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
	if f.Inode == 0 {
		f.Inode = 0
	}
	if f.Chunks == nil {
		f.Chunks = make([]string, 0)
	}
	stableSortChunkNames(f.Chunks)
	if f.Symlink != "" {
		f.Chunks = make([]string, 0)
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
		return nil
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
	m.invalidateIndexes()
	ref := m.Releases[tag]
	return &ref
}

func (m *RepoMetadata) UpsertFile(name string, file FileMeta, createdAt int64) {
	name = normalizeStoredPath(name)
	if parent := parentPath(name); parent != "" {
		m.EnsureDirectory(parent, createdAt)
	}
	if existing, ok := m.Files[name]; ok {
		preserveFileIdentity(&file, &existing, createdAt)
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
		files[i] = m.Files[name]
	}
	return files
}

func (m *RepoMetadata) FileChunks(name string) []ChunkInfo {
	file, ok := m.Files[name]
	if !ok {
		return nil
	}
	chunks := make([]ChunkInfo, 0, len(file.Chunks))
	for _, cname := range file.Chunks {
		if c, ok := m.Chunks[cname]; ok {
			chunks = append(chunks, c)
		}
	}
	return chunks
}

func (m *RepoMetadata) DirNLink(path string) int {
	count := 2
	for _, dirPath := range m.childDirs[path] {
		if dirPath != path {
			count++
		}
	}
	rootDirCount := 0
	for dirPath := range m.Dirs {
		if parentPath(dirPath) == path {
			rootDirCount++
		}
	}
	return count + rootDirCount
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
	if file.Inode == 0 {
		file.Inode = existing.Inode
	}
	if file.Mode == 0 {
		file.Mode = existing.Mode
	}
	if file.UID == 0 && existing.UID != 0 {
		file.UID = existing.UID
	}
	if file.GID == 0 && existing.GID != 0 {
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
		file.XAttrs = cloneStringMap(existing.XAttrs)
	}
	if file.Symlink == "" && existing.Symlink != "" {
		file.Symlink = existing.Symlink
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
	if file.UID == 0 && uid != 0 {
		file.UID = uid
	}
	if file.GID == 0 && gid != 0 {
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
	uid, gid := defaultOwnerIDs()
	if m.Root.Inode == 0 {
		m.Root.Inode = 1
	}
	if m.Root.Mode == 0 {
		m.Root.Mode = defaultDirMode()
	}
	if m.Root.UID == 0 && uid != 0 {
		m.Root.UID = uid
	}
	if m.Root.GID == 0 && gid != 0 {
		m.Root.GID = gid
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
}

func (m *RepoMetadata) allocateInode() uint64 {
	m.normalizeRoot(time.Now().Unix())
	ino := m.NextInode
	m.NextInode++
	return ino
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
		if err := file.Validate(); err != nil {
			return fmt.Errorf("file %s: %w", path, err)
		}
		if file.Inode == 0 {
			return fmt.Errorf("file %s inode is required", path)
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
		seenChunk := map[string]struct{}{}
		for _, chunkName := range file.Chunks {
			if _, ok := m.Chunks[chunkName]; !ok {
				return fmt.Errorf("file %s references missing chunk %s", path, chunkName)
			}
			if _, ok := seenChunk[chunkName]; ok {
				return fmt.Errorf("file %s: duplicate chunk reference: %s", path, chunkName)
			}
			seenChunk[chunkName] = struct{}{}
		}
		chunks := make([]ChunkInfo, 0, len(file.Chunks))
		for _, name := range file.Chunks {
			chunks = append(chunks, m.Chunks[name])
		}
		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].Offset < chunks[j].Offset
		})
		nextOffset := int64(0)
		for _, c := range chunks {
			if c.Offset < 0 {
				return fmt.Errorf("file %s: chunk has negative offset %d", path, c.Offset)
			}
			if c.Offset > nextOffset {
				return fmt.Errorf("file %s: chunk gap at offset %d (expected %d)", path, c.Offset, nextOffset)
			}
			if c.Offset < nextOffset {
				return fmt.Errorf("file %s: chunk overlap at offset %d", path, c.Offset)
			}
			nextOffset = c.Offset + c.Size
		}
		if file.Symlink == "" && nextOffset > file.Size {
			return fmt.Errorf("file %s: chunk data extends beyond file size (%d > %d)", path, nextOffset, file.Size)
		}
	}

	if m.TotalFiles != totalFiles {
		return fmt.Errorf("metadata total files mismatch: expected %d, got %d", totalFiles, m.TotalFiles)
	}
	if m.TotalSize != totalSize {
		return fmt.Errorf("metadata total size mismatch: expected %d, got %d", totalSize, m.TotalSize)
	}

	for tag, ref := range m.Releases {
		if ref.AssetCount < 0 {
			return fmt.Errorf("release %s has negative asset count %d", tag, ref.AssetCount)
		}
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
	seen := make(map[string]struct{}, len(f.Chunks))
	for _, name := range f.Chunks {
		if _, ok := seen[name]; ok {
			return fmt.Errorf("duplicate chunk reference: %s", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func (m *RepoMetadata) migrateV1(data []byte) error {
	var v1 struct {
		Version     int    `json:"version"`
		Project     string `json:"project"`
		NextInode   uint64 `json:"next_inode,omitempty"`
		TotalFiles  int    `json:"total_files"`
		TotalSize   int64  `json:"total_size"`
		LastModified time.Time `json:"last_modified"`
		Root struct {
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

	m.Version = 2
	m.Project = v1.Project
	m.TotalFiles = v1.TotalFiles
	m.TotalSize = v1.TotalSize
	m.LastMod = timeToUnix(v1.LastModified)
	m.NextInode = v1.NextInode

	m.Root = DirMeta{
		CreatedAt: timeToUnix(v1.Root.CreatedAt), ModifiedAt: timeToUnix(v1.Root.ModifiedAt),
		AccessedAt: timeToUnix(v1.Root.AccessedAt), ChangedAt: timeToUnix(v1.Root.ChangedAt),
		Mode: v1.Root.Mode, UID: v1.Root.UID, GID: v1.Root.GID,
		Inode: v1.Root.Inode, XAttrs: v1.Root.XAttrs,
	}

	m.Dirs = make(map[string]DirMeta, len(v1.Directories))
	for _, d := range v1.Directories {
		m.Dirs[d.Path] = DirMeta{
			CreatedAt: timeToUnix(d.CreatedAt), ModifiedAt: timeToUnix(d.ModifiedAt),
			AccessedAt: timeToUnix(d.AccessedAt), ChangedAt: timeToUnix(d.ChangedAt),
			Mode: d.Mode, UID: d.UID, GID: d.GID,
			Inode: d.Inode, XAttrs: d.XAttrs,
		}
	}

	m.Files = make(map[string]FileMeta)
	m.Chunks = make(map[string]ChunkInfo)
	m.Releases = make(map[string]ReleaseRef)

	for _, r := range v1.Releases {
		ref := ReleaseRef{
			AssetCount: r.AssetCount,
			CreatedAt:  timeToUnix(r.CreatedAt),
		}
		m.Releases[r.Tag] = ref

		for _, f := range r.Files {
			symlink := ""
			if f.Kind == "symlink" || f.SymlinkTarget != "" {
				symlink = f.SymlinkTarget
			}

			chunkNames := make([]string, 0, len(f.Chunks))
			for _, c := range f.Chunks {
				chunkNames = append(chunkNames, c.Name)
				ci := ChunkInfo{
					Size: c.Size, Offset: c.Offset, Release: chooseNonEmpty(c.Release, f.Release, r.Tag),
					AssetOffset: c.AssetOffset, AssetID: c.AssetID,
				}
				m.Chunks[c.Name] = ci
			}

			fileMeta := FileMeta{
				Chunks:     chunkNames,
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
				XAttrs:     f.XAttrs,
			}
			if symlink != "" {
				fileMeta.Size = int64(len(symlink))
				fileMeta.Chunks = nil
			}
			m.Files[f.Name] = fileMeta
		}
	}

	return nil
}

func stableSortReleases(releases []string) {
	sort.SliceStable(releases, func(i, j int) bool {
		left, lok := parseNumericReleaseTag(releases[i])
		right, rok := parseNumericReleaseTag(releases[j])
		if lok && rok && left != right {
			return left < right
		}
		return releases[i] < releases[j]
	})
}

func stableSortChunkNames(names []string) {
	sort.SliceStable(names, func(i, j int) bool { return names[i] < names[j] })
}

func stableSortStrings(strs []string) {
	sort.SliceStable(strs, func(i, j int) bool { return strs[i] < strs[j] })
}

func parseNumericReleaseTag(tag string) (int, bool) {
	trimmed := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(tag)), "v")
	n, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, false
	}
	return n, true
}

func chooseNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
