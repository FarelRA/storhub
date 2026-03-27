package storhub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
)

type EntryInfo struct {
	Path       string    `json:"path"`
	IsDir      bool      `json:"is_dir"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modified_at"`
	CreatedAt  time.Time `json:"created_at"`
}

type DirEntry struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	IsDir bool   `json:"is_dir"`
	Size  int64  `json:"size"`
}

type FSStats struct {
	Files       int   `json:"files"`
	Directories int   `json:"directories"`
	Bytes       int64 `json:"bytes"`
	Releases    int   `json:"releases"`
	Assets      int   `json:"assets"`
}

func (h *StorHub) CreateFile(project, filePath string) (*FileMetadata, error) {
	return h.CreateFileContext(context.Background(), project, filePath)
}

func (h *StorHub) CreateFileContext(ctx context.Context, project, filePath string) (*FileMetadata, error) {
	cleanPath, err := normalizeFSPath(filePath)
	if err != nil {
		return nil, err
	}
	if cleanPath == "" {
		return nil, errors.New("file path is required")
	}
	if err := validateProject(project); err != nil {
		return nil, err
	}
	if err := h.ensureRepo(ctx, project); err != nil {
		return nil, err
	}
	repoMeta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	if err := requireParentDirectory(repoMeta, cleanPath); err != nil {
		return nil, err
	}
	if repoMeta.FindFile(cleanPath) != nil {
		return nil, fmt.Errorf("file already exists: %s", cleanPath)
	}
	workingMeta := repoMeta.Clone()
	releaseTag, _, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, 0, "")
	if err != nil {
		return nil, err
	}
	fileMeta := FileMetadata{Name: cleanPath, Size: 0, Chunks: []ChunkInfo{}, Release: releaseTag, UploadedAt: h.config.Now().UTC()}
	fileMeta.CRC32C, err = CombineChunkCRC32Cs(fileMeta.Chunks)
	if err != nil {
		return nil, err
	}
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if err := requireParentDirectory(meta, cleanPath); err != nil {
			return err
		}
		if meta.FindFile(cleanPath) != nil {
			return fmt.Errorf("file already exists: %s", cleanPath)
		}
		meta.UpsertFile(fileMeta, h.config.Now().UTC())
		return nil
	}, fmt.Sprintf("storhub: create %s", cleanPath)); err != nil {
		return nil, err
	}
	return &fileMeta, nil
}

func (h *StorHub) Mkdir(project, dirPath string) error {
	return h.MkdirContext(context.Background(), project, dirPath)
}

func (h *StorHub) Unlink(project, filePath string) error {
	return h.DeleteFile(project, filePath)
}

func (h *StorHub) UnlinkContext(ctx context.Context, project, filePath string) error {
	return h.DeleteFileContext(ctx, project, filePath)
}

func (h *StorHub) MkdirContext(ctx context.Context, project, dirPath string) error {
	cleanPath, err := normalizeFSPath(dirPath)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		return nil
	}
	if err := validateProject(project); err != nil {
		return err
	}
	if err := h.ensureRepo(ctx, project); err != nil {
		return err
	}
	_, err = h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if meta.HasDirectory(cleanPath) {
			return fmt.Errorf("directory already exists: %s", cleanPath)
		}
		if meta.FindFile(cleanPath) != nil {
			return fmt.Errorf("file already exists at path: %s", cleanPath)
		}
		if parent := parentPath(cleanPath); parent != "" && !meta.HasDirectory(parent) {
			return fmt.Errorf("parent directory does not exist: %s", parent)
		}
		meta.EnsureDirectory(cleanPath, h.config.Now().UTC())
		return nil
	}, fmt.Sprintf("storhub: mkdir %s", cleanPath))
	return err
}

func (h *StorHub) Rmdir(project, dirPath string) error {
	return h.RmdirContext(context.Background(), project, dirPath)
}

func (h *StorHub) RmdirContext(ctx context.Context, project, dirPath string) error {
	cleanPath, err := normalizeFSPath(dirPath)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		return errors.New("cannot remove root directory")
	}
	_, err = h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if !meta.HasDirectory(cleanPath) {
			return fmt.Errorf("directory not found: %s", cleanPath)
		}
		childDirs, childFiles := meta.DirectoryChildren(cleanPath)
		if len(childDirs) > 0 || len(childFiles) > 0 {
			return fmt.Errorf("directory not empty: %s", cleanPath)
		}
		meta.RemoveDirectory(cleanPath)
		return nil
	}, fmt.Sprintf("storhub: rmdir %s", cleanPath))
	return err
}

func (h *StorHub) Rename(project, oldPath, newPath string) error {
	return h.RenameContext(context.Background(), project, oldPath, newPath)
}

func (h *StorHub) RenameContext(ctx context.Context, project, oldPath, newPath string) error {
	oldClean, err := normalizeFSPath(oldPath)
	if err != nil {
		return err
	}
	newClean, err := normalizeFSPath(newPath)
	if err != nil {
		return err
	}
	if oldClean == newClean {
		return nil
	}
	_, err = h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if parent := parentPath(newClean); parent != "" && !meta.HasDirectory(parent) {
			return fmt.Errorf("parent directory does not exist: %s", parent)
		}
		if file := meta.FindFile(oldClean); file != nil {
			if meta.FindFile(newClean) != nil || meta.HasDirectory(newClean) {
				return fmt.Errorf("destination already exists: %s", newClean)
			}
			renamed := file.Clone()
			renamed.Name = newClean
			meta.RemoveFile(oldClean)
			meta.UpsertFile(renamed, h.config.Now().UTC())
			return nil
		}
		if !meta.HasDirectory(oldClean) {
			return fmt.Errorf("path not found: %s", oldClean)
		}
		if isParentOrSame(oldClean, newClean) {
			return fmt.Errorf("cannot move directory %s into itself %s", oldClean, newClean)
		}
		if meta.FindFile(newClean) != nil || meta.HasDirectory(newClean) {
			return fmt.Errorf("destination already exists: %s", newClean)
		}
		for i := range meta.Directories {
			if isParentOrSame(oldClean, meta.Directories[i].Path) {
				meta.Directories[i].Path = remapPath(oldClean, newClean, meta.Directories[i].Path)
				meta.Directories[i].ModifiedAt = h.config.Now().UTC()
			}
		}
		for i := range meta.Releases {
			for j := range meta.Releases[i].Files {
				if isParentOrSame(oldClean, meta.Releases[i].Files[j].Name) {
					meta.Releases[i].Files[j].Name = remapPath(oldClean, newClean, meta.Releases[i].Files[j].Name)
				}
			}
		}
		meta.RecomputeStats()
		return nil
	}, fmt.Sprintf("storhub: rename %s to %s", oldClean, newClean))
	return err
}

func (h *StorHub) TruncateFile(project, filePath string, size int64) (*FileMetadata, error) {
	return h.TruncateFileContext(context.Background(), project, filePath, size)
}

func (h *StorHub) TruncateFileContext(ctx context.Context, project, filePath string, size int64) (*FileMetadata, error) {
	cleanPath, err := normalizeFSPath(filePath)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, errors.New("truncate size must be non-negative")
	}
	meta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	file := meta.FindFile(cleanPath)
	if file == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanPath)
	}
	if size == file.Size {
		clone := file.Clone()
		return &clone, nil
	}
	if size < file.Size {
		return h.patchFileWithMetadataContext(ctx, project, cleanPath, meta, file, size, file.Size-size, nil)
	}
	return h.patchFileWithMetadataContext(ctx, project, cleanPath, meta, file, file.Size, 0, make([]byte, size-file.Size))
}

func (h *StorHub) AppendFile(project, filePath string, data []byte) (*FileMetadata, error) {
	return h.AppendFileContext(context.Background(), project, filePath, data)
}

func (h *StorHub) AppendFileContext(ctx context.Context, project, filePath string, data []byte) (*FileMetadata, error) {
	meta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath, err := normalizeFSPath(filePath)
	if err != nil {
		return nil, err
	}
	file := meta.FindFile(cleanPath)
	if file == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanPath)
	}
	return h.patchFileWithMetadataContext(ctx, project, cleanPath, meta, file, file.Size, 0, data)
}

func (h *StorHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*FileMetadata, error) {
	return h.WriteFileAtContext(context.Background(), project, filePath, offset, data)
}

func (h *StorHub) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte) (*FileMetadata, error) {
	cleanPath, err := normalizeFSPath(filePath)
	if err != nil {
		return nil, err
	}
	meta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	file := meta.FindFile(cleanPath)
	if file == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanPath)
	}
	if offset < 0 {
		return nil, errors.New("write offset must be non-negative")
	}
	if len(data) == 0 {
		clone := file.Clone()
		return &clone, nil
	}
	if offset > file.Size {
		gap := make([]byte, offset-file.Size)
		return h.patchFileWithMetadataContext(ctx, project, cleanPath, meta, file, file.Size, 0, append(gap, data...))
	}
	deleteSize := int64(len(data))
	if max := file.Size - offset; deleteSize > max {
		deleteSize = max
	}
	return h.patchFileWithMetadataContext(ctx, project, cleanPath, meta, file, offset, deleteSize, data)
}

func (h *StorHub) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	return h.ReadFileAtContext(context.Background(), project, filePath, offset, length)
}

func (h *StorHub) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	cleanPath, err := normalizeFSPath(filePath)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	meta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	file := meta.FindFile(cleanPath)
	if file == nil {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, cleanPath)
	}
	if offset > file.Size {
		return nil, io.EOF
	}
	if length == 0 {
		return []byte{}, nil
	}
	end := offset + length
	if end > file.Size {
		end = file.Size
	}
	result := make([]byte, end-offset)
	for _, chunk := range file.Chunks {
		chunkEnd := chunk.Offset + chunk.Size
		if chunkEnd <= offset || chunk.Offset >= end || chunk.Size == 0 {
			continue
		}
		start := maxInt64(offset, chunk.Offset)
		stop := minInt64(end, chunkEnd)
		segment := chunk
		segment.Offset = start
		segment.AssetOffset = chunk.AssetOffset + (start - chunk.Offset)
		segment.Size = stop - start
		dst := result[start-offset : stop-offset]
		if err := h.fillAssetRange(ctx, project, segment, dst); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (h *StorHub) StatPath(project, targetPath string) (*EntryInfo, error) {
	return h.StatPathContext(context.Background(), project, targetPath)
}

func (h *StorHub) StatPathContext(ctx context.Context, project, targetPath string) (*EntryInfo, error) {
	cleanPath := ""
	if strings.TrimSpace(targetPath) != "" {
		var err error
		cleanPath, err = normalizeFSPath(targetPath)
		if err != nil {
			return nil, err
		}
	}
	meta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	if cleanPath == "" {
		return &EntryInfo{Path: "", IsDir: true}, nil
	}
	if file := meta.FindFile(cleanPath); file != nil {
		return &EntryInfo{Path: file.Name, Size: file.Size, CreatedAt: file.UploadedAt, ModifiedAt: file.UploadedAt}, nil
	}
	for _, dir := range meta.Directories {
		if dir.Path == cleanPath {
			return &EntryInfo{Path: dir.Path, IsDir: true, CreatedAt: dir.CreatedAt, ModifiedAt: dir.ModifiedAt}, nil
		}
	}
	return nil, fmt.Errorf("path not found: %s", cleanPath)
}

func (h *StorHub) ReadDir(project, dirPath string) ([]DirEntry, error) {
	return h.ReadDirContext(context.Background(), project, dirPath)
}

func (h *StorHub) StatFS(project string) (*FSStats, error) {
	return h.StatFSContext(context.Background(), project)
}

func (h *StorHub) StatFSContext(ctx context.Context, project string) (*FSStats, error) {
	meta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	stats := &FSStats{Files: meta.TotalFiles, Directories: len(meta.Directories), Bytes: meta.TotalSize, Releases: len(meta.Releases)}
	for _, release := range meta.Releases {
		stats.Assets += release.AssetCount
	}
	return stats, nil
}

func (h *StorHub) ReadDirContext(ctx context.Context, project, dirPath string) ([]DirEntry, error) {
	cleanPath := ""
	if strings.TrimSpace(dirPath) != "" {
		var err error
		cleanPath, err = normalizeFSPath(dirPath)
		if err != nil {
			return nil, err
		}
	}
	meta, _, err := h.loadRepoMetadata(ctx, project)
	if err != nil {
		return nil, err
	}
	if cleanPath != "" && !meta.HasDirectory(cleanPath) {
		return nil, fmt.Errorf("directory not found: %s", cleanPath)
	}
	dirs, files := meta.DirectoryChildren(cleanPath)
	entries := make([]DirEntry, 0, len(dirs)+len(files))
	for _, dir := range dirs {
		entries = append(entries, DirEntry{Name: path.Base(dir.Path), Path: dir.Path, IsDir: true})
	}
	for _, file := range files {
		entries = append(entries, DirEntry{Name: path.Base(file.Name), Path: file.Name, Size: file.Size})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

func requireParentDirectory(meta *RepoMetadata, filePath string) error {
	if parent := parentPath(filePath); parent != "" && !meta.HasDirectory(parent) {
		return fmt.Errorf("parent directory does not exist: %s", parent)
	}
	return nil
}

func remapPath(oldBase, newBase, target string) string {
	if target == oldBase {
		return newBase
	}
	return newBase + strings.TrimPrefix(target, oldBase)
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
