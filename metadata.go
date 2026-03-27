package storhub

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ChunkInfo struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	Index       int    `json:"index"`
	Offset      int64  `json:"offset"`
	Release     string `json:"release"`
	AssetOffset int64  `json:"asset_offset"`
	AssetID     int64  `json:"asset_id"`
	CRC32C      string `json:"crc32c"`
}

type FileMetadata struct {
	Name       string      `json:"name"`
	Size       int64       `json:"size"`
	Chunks     []ChunkInfo `json:"chunks"`
	Release    string      `json:"release"`
	UploadedAt time.Time   `json:"uploaded_at"`
	CRC32C     string      `json:"crc32c"`
}

type DirectoryMetadata struct {
	Path       string    `json:"path"`
	CreatedAt  time.Time `json:"created_at"`
	ModifiedAt time.Time `json:"modified_at"`
}

func (d DirectoryMetadata) Clone() DirectoryMetadata { return d }

func (f FileMetadata) Clone() FileMetadata {
	clone := f
	if f.Chunks != nil {
		clone.Chunks = append([]ChunkInfo(nil), f.Chunks...)
	}
	return clone
}

type ReleaseMetadata struct {
	Tag        string         `json:"tag"`
	AssetCount int            `json:"asset_count"`
	Files      []FileMetadata `json:"files"`
	CreatedAt  time.Time      `json:"created_at"`
}

func (r ReleaseMetadata) Clone() ReleaseMetadata {
	clone := r
	if r.Files != nil {
		clone.Files = make([]FileMetadata, len(r.Files))
		for i := range r.Files {
			clone.Files[i] = r.Files[i].Clone()
		}
	}
	return clone
}

type RepoMetadata struct {
	Version      int                            `json:"version"`
	Project      string                         `json:"project"`
	TotalFiles   int                            `json:"total_files"`
	TotalSize    int64                          `json:"total_size"`
	Directories  []DirectoryMetadata            `json:"directories"`
	Releases     []ReleaseMetadata              `json:"releases"`
	LastModified time.Time                      `json:"last_modified"`
	fileIndex    map[string]*FileMetadata       `json:"-"`
	releaseIndex map[string]*ReleaseMetadata    `json:"-"`
	dirIndex     map[string]struct{}            `json:"-"`
	childDirs    map[string][]DirectoryMetadata `json:"-"`
	childFiles   map[string][]FileMetadata      `json:"-"`
	allFiles     []FileMetadata                 `json:"-"`
}

type MetadataRevision struct {
	CommitSHA   string    `json:"commit_sha"`
	Message     string    `json:"message"`
	CommittedAt time.Time `json:"committed_at"`
}

func NewRepoMetadata(project string) *RepoMetadata {
	return &RepoMetadata{
		Version:     1,
		Project:     project,
		Directories: make([]DirectoryMetadata, 0),
		Releases:    make([]ReleaseMetadata, 0),
	}
}

func NewReleaseMetadata(tag string, createdAt time.Time) *ReleaseMetadata {
	return &ReleaseMetadata{
		Tag:       tag,
		Files:     make([]FileMetadata, 0),
		CreatedAt: createdAt.UTC(),
	}
}

func (m RepoMetadata) Clone() RepoMetadata {
	clone := m
	if m.Directories != nil {
		clone.Directories = append([]DirectoryMetadata(nil), m.Directories...)
	}
	if m.Releases != nil {
		clone.Releases = make([]ReleaseMetadata, len(m.Releases))
		for i := range m.Releases {
			clone.Releases[i] = m.Releases[i].Clone()
		}
	}
	clone.fileIndex = nil
	clone.releaseIndex = nil
	clone.dirIndex = nil
	clone.childDirs = nil
	clone.childFiles = nil
	clone.allFiles = nil
	return clone
}

func (m *RepoMetadata) ToJSON() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	return data, nil
}

func (m *RepoMetadata) FromJSON(data []byte) error {
	return json.Unmarshal(data, m)
}

func (m *RepoMetadata) Normalize(project string, now time.Time) {
	m.Version = 1
	m.Project = chooseNonEmpty(m.Project, project)
	if m.Releases == nil {
		m.Releases = make([]ReleaseMetadata, 0)
	}
	if m.Directories == nil {
		m.Directories = make([]DirectoryMetadata, 0)
	}
	for i := range m.Directories {
		m.Directories[i].Normalize(now)
	}
	for i := range m.Releases {
		m.Releases[i].Normalize(m.Releases[i].Tag, now)
	}
	m.RecomputeStats()
	stableSortReleases(m.Releases)
	if m.LastModified.IsZero() {
		m.LastModified = now.UTC()
	}
	m.rebuildIndexes()
}

func (m *RepoMetadata) RecomputeStats() {
	totalFiles := 0
	totalSize := int64(0)
	assetRefs := make(map[string]map[int64]struct{})
	referencedReleases := make(map[string]struct{})
	for _, release := range m.Releases {
		for _, file := range release.Files {
			for _, chunk := range file.Chunks {
				tag := chooseNonEmpty(chunk.Release, file.Release, release.Tag)
				if tag == "" {
					continue
				}
				if assetRefs[tag] == nil {
					assetRefs[tag] = make(map[int64]struct{})
				}
				assetRefs[tag][chunk.AssetID] = struct{}{}
				referencedReleases[tag] = struct{}{}
			}
		}
	}
	filtered := m.Releases[:0]
	for _, release := range m.Releases {
		release.RecomputeStats()
		release.AssetCount = len(assetRefs[release.Tag])
		if len(release.Files) == 0 && release.AssetCount == 0 {
			continue
		}
		totalFiles += len(release.Files)
		for _, file := range release.Files {
			totalSize += file.Size
		}
		filtered = append(filtered, release)
	}
	for tag := range referencedReleases {
		if containsRelease(filtered, tag) {
			continue
		}
		filtered = append(filtered, ReleaseMetadata{Tag: tag, AssetCount: len(assetRefs[tag]), Files: make([]FileMetadata, 0)})
	}
	m.Releases = filtered
	m.TotalFiles = totalFiles
	m.TotalSize = totalSize
	stableSortDirectories(m.Directories)
	stableSortReleases(m.Releases)
	for i := range m.Releases {
		stableSortFiles(m.Releases[i].Files)
	}
	if m.Version == 0 {
		m.Version = 1
	}
	m.rebuildIndexes()
}

func (r *ReleaseMetadata) Normalize(tag string, createdAt time.Time) {
	r.Tag = chooseNonEmpty(r.Tag, tag)
	if r.Files == nil {
		r.Files = make([]FileMetadata, 0)
	}
	for i := range r.Files {
		r.Files[i].Normalize(r.Tag)
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = createdAt.UTC()
	}
	r.RecomputeStats()
}

func (f *FileMetadata) Normalize(release string) {
	f.Release = chooseNonEmpty(f.Release, release)
	f.Name = normalizeStoredPath(f.Name)
	if f.Chunks == nil {
		f.Chunks = make([]ChunkInfo, 0)
	}
	for i := range f.Chunks {
		f.Chunks[i].Release = chooseNonEmpty(f.Chunks[i].Release, f.Release)
	}
	stableSortChunks(f.Chunks)
}

func (d *DirectoryMetadata) Normalize(now time.Time) {
	d.Path = normalizeStoredPath(d.Path)
	if d.CreatedAt.IsZero() {
		d.CreatedAt = now.UTC()
	}
	if d.ModifiedAt.IsZero() {
		d.ModifiedAt = d.CreatedAt.UTC()
	}
}

func (r *ReleaseMetadata) RecomputeStats() {
	filtered := r.Files[:0]
	for _, file := range r.Files {
		if strings.TrimSpace(file.Name) == "" {
			continue
		}
		file.Normalize(r.Tag)
		filtered = append(filtered, file)
	}
	r.Files = filtered
	stableSortFiles(r.Files)
}

func (m *RepoMetadata) GetRelease(tag string) *ReleaseMetadata {
	m.ensureIndexes()
	return m.releaseIndex[tag]
}

func (m *RepoMetadata) HasDirectory(path string) bool {
	if path == "" {
		return true
	}
	m.ensureIndexes()
	_, ok := m.dirIndex[path]
	return ok
}

func (m *RepoMetadata) EnsureDirectory(path string, now time.Time) {
	path = normalizeStoredPath(path)
	if path == "" || m.HasDirectory(path) {
		return
	}
	parent := parentPath(path)
	if parent != "" {
		m.EnsureDirectory(parent, now)
	}
	m.Directories = append(m.Directories, DirectoryMetadata{Path: path, CreatedAt: now.UTC(), ModifiedAt: now.UTC()})
	stableSortDirectories(m.Directories)
	m.invalidateIndexes()
}

func (m *RepoMetadata) RemoveDirectory(path string) bool {
	path = normalizeStoredPath(path)
	removed := false
	filtered := m.Directories[:0]
	for _, dir := range m.Directories {
		if dir.Path == path {
			removed = true
			continue
		}
		filtered = append(filtered, dir)
	}
	m.Directories = filtered
	m.invalidateIndexes()
	return removed
}

func (m *RepoMetadata) DirectoryChildren(path string) ([]DirectoryMetadata, []FileMetadata) {
	path = normalizeStoredPath(path)
	m.ensureIndexes()
	dirs := append([]DirectoryMetadata(nil), m.childDirs[path]...)
	files := append([]FileMetadata(nil), m.childFiles[path]...)
	return dirs, files
}

func (m *RepoMetadata) EnsureRelease(tag string, createdAt time.Time) *ReleaseMetadata {
	if release := m.GetRelease(tag); release != nil {
		return release
	}
	release := NewReleaseMetadata(tag, createdAt)
	m.Releases = append(m.Releases, *release)
	stableSortReleases(m.Releases)
	m.invalidateIndexes()
	return m.GetRelease(tag)
}

func (m *RepoMetadata) UpsertFile(file FileMetadata, createdAt time.Time) {
	if parent := parentPath(file.Name); parent != "" {
		m.EnsureDirectory(parent, createdAt)
	}
	m.RemoveFile(file.Name)
	release := m.EnsureRelease(file.Release, createdAt)
	release.UpsertFile(file)
	m.RecomputeStats()
}

func (r *ReleaseMetadata) UpsertFile(file FileMetadata) {
	for i := range r.Files {
		if r.Files[i].Name == file.Name {
			r.Files[i] = file
			r.RecomputeStats()
			return
		}
	}
	r.Files = append(r.Files, file)
	r.RecomputeStats()
}

func (r *ReleaseMetadata) GetFile(name string) *FileMetadata {
	for i := range r.Files {
		if r.Files[i].Name == name {
			return &r.Files[i]
		}
	}
	return nil
}

func (m *RepoMetadata) FindFile(name string) *FileMetadata {
	m.ensureIndexes()
	return m.fileIndex[name]
}

func (m *RepoMetadata) RemoveFile(name string) bool {
	removed := false
	for i := range m.Releases {
		filtered := m.Releases[i].Files[:0]
		for _, file := range m.Releases[i].Files {
			if file.Name == name {
				removed = true
				continue
			}
			filtered = append(filtered, file)
		}
		m.Releases[i].Files = filtered
		m.Releases[i].RecomputeStats()
	}
	m.RecomputeStats()
	m.invalidateIndexes()
	return removed
}

func (m *RepoMetadata) RemoveRelease(tag string) bool {
	removed := false
	filtered := m.Releases[:0]
	for _, release := range m.Releases {
		if release.Tag == tag {
			removed = true
			continue
		}
		filtered = append(filtered, release)
	}
	m.Releases = filtered
	m.RecomputeStats()
	m.invalidateIndexes()
	return removed
}

func (m *RepoMetadata) AllFiles() []FileMetadata {
	m.ensureIndexes()
	return append([]FileMetadata(nil), m.allFiles...)
}

func (m *RepoMetadata) invalidateIndexes() {
	m.fileIndex = nil
	m.releaseIndex = nil
	m.dirIndex = nil
	m.childDirs = nil
	m.childFiles = nil
	m.allFiles = nil
}

func (m *RepoMetadata) ensureIndexes() {
	if m.fileIndex != nil && m.releaseIndex != nil && m.dirIndex != nil && m.childDirs != nil && m.childFiles != nil && m.allFiles != nil {
		return
	}
	m.rebuildIndexes()
}

func (m *RepoMetadata) rebuildIndexes() {
	m.fileIndex = make(map[string]*FileMetadata, m.TotalFiles)
	m.releaseIndex = make(map[string]*ReleaseMetadata, len(m.Releases))
	m.dirIndex = make(map[string]struct{}, len(m.Directories))
	m.childDirs = make(map[string][]DirectoryMetadata, len(m.Directories)+1)
	m.childFiles = make(map[string][]FileMetadata, len(m.Releases)+1)
	m.allFiles = make([]FileMetadata, 0, m.TotalFiles)
	for i := range m.Directories {
		dir := m.Directories[i]
		m.dirIndex[dir.Path] = struct{}{}
		parent := parentPath(dir.Path)
		m.childDirs[parent] = append(m.childDirs[parent], dir)
	}
	for i := range m.Releases {
		release := &m.Releases[i]
		m.releaseIndex[release.Tag] = release
		for j := range release.Files {
			file := &release.Files[j]
			m.fileIndex[file.Name] = file
			m.childFiles[parentPath(file.Name)] = append(m.childFiles[parentPath(file.Name)], file.Clone())
			m.allFiles = append(m.allFiles, file.Clone())
		}
	}
	for parent := range m.childDirs {
		stableSortDirectories(m.childDirs[parent])
	}
	for parent := range m.childFiles {
		stableSortFiles(m.childFiles[parent])
	}
	stableSortFiles(m.allFiles)
}

func (m *RepoMetadata) Validate() error {
	if strings.TrimSpace(m.Project) == "" {
		return fmt.Errorf("metadata project is required")
	}
	seenReleases := make(map[string]struct{}, len(m.Releases))
	assetRefs := make(map[string]map[int64]struct{})
	totalFiles := 0
	totalSize := int64(0)
	seenDirs := make(map[string]struct{}, len(m.Directories))
	for _, dir := range m.Directories {
		if err := dir.Validate(); err != nil {
			return err
		}
		if _, ok := seenDirs[dir.Path]; ok {
			return fmt.Errorf("duplicate directory: %s", dir.Path)
		}
		seenDirs[dir.Path] = struct{}{}
		if parent := parentPath(dir.Path); parent != "" {
			if _, ok := seenDirs[parent]; !ok && !m.HasDirectory(parent) {
				return fmt.Errorf("directory %s missing parent %s", dir.Path, parent)
			}
		}
	}
	for _, release := range m.Releases {
		if err := release.Validate(); err != nil {
			return err
		}
		if _, ok := seenReleases[release.Tag]; ok {
			return fmt.Errorf("duplicate release tag: %s", release.Tag)
		}
		seenReleases[release.Tag] = struct{}{}
		totalFiles += len(release.Files)
		for _, file := range release.Files {
			if parent := parentPath(file.Name); parent != "" && !m.HasDirectory(parent) {
				return fmt.Errorf("file %s missing parent directory %s", file.Name, parent)
			}
			totalSize += file.Size
			for _, chunk := range file.Chunks {
				tag := chooseNonEmpty(chunk.Release, file.Release, release.Tag)
				if assetRefs[tag] == nil {
					assetRefs[tag] = make(map[int64]struct{})
				}
				assetRefs[tag][chunk.AssetID] = struct{}{}
			}
		}
	}
	if m.TotalFiles != totalFiles {
		return fmt.Errorf("metadata total files mismatch: expected %d, got %d", totalFiles, m.TotalFiles)
	}
	if m.TotalSize != totalSize {
		return fmt.Errorf("metadata total size mismatch: expected %d, got %d", totalSize, m.TotalSize)
	}
	for _, release := range m.Releases {
		expected := len(assetRefs[release.Tag])
		if release.AssetCount != expected {
			return fmt.Errorf("release %s asset count mismatch: expected %d, got %d", release.Tag, expected, release.AssetCount)
		}
	}
	return nil
}

func (d DirectoryMetadata) Validate() error {
	if d.Path == "" {
		return fmt.Errorf("directory path is required")
	}
	return nil
}

func (r *ReleaseMetadata) Validate() error {
	if strings.TrimSpace(r.Tag) == "" {
		return fmt.Errorf("release tag is required")
	}
	seenFiles := make(map[string]struct{}, len(r.Files))
	for _, file := range r.Files {
		if err := file.Validate(r.Tag); err != nil {
			return err
		}
		if _, ok := seenFiles[file.Name]; ok {
			return fmt.Errorf("duplicate file in release %s: %s", r.Tag, file.Name)
		}
		seenFiles[file.Name] = struct{}{}
	}
	return nil
}

func (f *FileMetadata) Validate(release string) error {
	if strings.TrimSpace(f.Name) == "" {
		return fmt.Errorf("file name is required")
	}
	if f.Size < 0 {
		return fmt.Errorf("file %s size must be non-negative", f.Name)
	}
	if strings.TrimSpace(f.Release) == "" {
		return fmt.Errorf("file %s release is required", f.Name)
	}
	if release != "" && f.Release != release {
		return fmt.Errorf("file %s release mismatch: expected %s, got %s", f.Name, release, f.Release)
	}
	if strings.TrimSpace(f.CRC32C) == "" {
		return fmt.Errorf("file %s crc32c is required", f.Name)
	}
	if len(f.Chunks) == 0 {
		if f.Size == 0 {
			return nil
		}
		return fmt.Errorf("file %s must contain at least one chunk", f.Name)
	}
	chunks := append([]ChunkInfo(nil), f.Chunks...)
	stableSortChunks(chunks)
	seenIndexes := make(map[int]struct{}, len(chunks))
	nextOffset := int64(0)
	totalSize := int64(0)
	for _, chunk := range chunks {
		if chunk.Index < 0 {
			return fmt.Errorf("file %s chunk index must be non-negative", f.Name)
		}
		if chunk.AssetID <= 0 {
			return fmt.Errorf("file %s chunk %d asset id must be positive", f.Name, chunk.Index)
		}
		if strings.TrimSpace(chunk.Release) == "" {
			return fmt.Errorf("file %s chunk %d release is required", f.Name, chunk.Index)
		}
		if chunk.AssetOffset < 0 {
			return fmt.Errorf("file %s chunk %d asset offset must be non-negative", f.Name, chunk.Index)
		}
		if chunk.Size < 0 {
			return fmt.Errorf("file %s chunk %d size must be non-negative", f.Name, chunk.Index)
		}
		if strings.TrimSpace(chunk.CRC32C) == "" {
			return fmt.Errorf("file %s chunk %d crc32c is required", f.Name, chunk.Index)
		}
		if chunk.Offset != nextOffset {
			return fmt.Errorf("file %s chunk %d offset mismatch: expected %d, got %d", f.Name, chunk.Index, nextOffset, chunk.Offset)
		}
		if _, ok := seenIndexes[chunk.Index]; ok {
			return fmt.Errorf("file %s duplicate chunk index %d", f.Name, chunk.Index)
		}
		seenIndexes[chunk.Index] = struct{}{}
		nextOffset += chunk.Size
		totalSize += chunk.Size
	}
	if totalSize != f.Size {
		return fmt.Errorf("file %s chunk size total mismatch: expected %d, got %d", f.Name, f.Size, totalSize)
	}
	return nil
}

func stableSortReleases(releases []ReleaseMetadata) {
	sort.SliceStable(releases, func(i, j int) bool {
		left, lok := parseNumericReleaseTag(releases[i].Tag)
		right, rok := parseNumericReleaseTag(releases[j].Tag)
		if lok && rok && left != right {
			return left < right
		}
		return releases[i].Tag < releases[j].Tag
	})
}

func stableSortFiles(files []FileMetadata) {
	sort.SliceStable(files, func(i, j int) bool { return files[i].Name < files[j].Name })
}

func stableSortDirectories(dirs []DirectoryMetadata) {
	sort.SliceStable(dirs, func(i, j int) bool { return dirs[i].Path < dirs[j].Path })
}

func stableSortChunks(chunks []ChunkInfo) {
	sort.SliceStable(chunks, func(i, j int) bool {
		if chunks[i].Offset != chunks[j].Offset {
			return chunks[i].Offset < chunks[j].Offset
		}
		return chunks[i].Index < chunks[j].Index
	})
}

func containsRelease(releases []ReleaseMetadata, tag string) bool {
	for _, release := range releases {
		if release.Tag == tag {
			return true
		}
	}
	return false
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
