package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type Backend interface {
	ValidateProjectName(project string) error
	EnsureRepoContext(ctx context.Context, project string) error
	LoadRepoMetadataContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error)
	GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *meta.RepoMetadata, requiredSize int, preferredTag string) (string, string, error)
	PatchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *meta.RepoMetadata, fileMeta *meta.FileMetadata, offset, deleteSize int64, edit []byte) (*meta.FileMetadata, error)
	FillAssetRangeContext(ctx context.Context, project string, segment meta.ChunkInfo, dst []byte) error
	Now() time.Time
	AtimePolicy() storcfg.AtimePolicy
	FileNotFound(path string) error
	DefaultFileMode(kind meta.NodeKind) uint32
	DefaultOwnerIDs() (uint32, uint32)
}

type Service struct {
	backend Backend
}

func NewService(backend Backend) *Service {
	return &Service{backend: backend}
}

func (s *Service) CreateFileContext(ctx context.Context, project, filePath string) (*meta.FileMetadata, error) {
	cleanPath, err := NormalizePath(filePath)
	if err != nil {
		return nil, err
	}
	if cleanPath == "" {
		return nil, errors.New("file path is required")
	}
	if err := s.backend.ValidateProjectName(project); err != nil {
		return nil, err
	}
	if err := s.backend.EnsureRepoContext(ctx, project); err != nil {
		return nil, err
	}
	repoMeta, _, err := s.backend.LoadRepoMetadataContext(ctx, project)
	if err != nil {
		return nil, err
	}
	if err := CheckParentWrite(ctx, repoMeta, cleanPath); err != nil {
		return nil, err
	}
	if err := RequireParentDirectory(repoMeta, cleanPath); err != nil {
		return nil, err
	}
	if repoMeta.FindFile(cleanPath) != nil {
		return nil, fmt.Errorf("file already exists: %s", cleanPath)
	}
	workingMeta := repoMeta.Clone()
	releaseTag, _, err := s.backend.GetOrCreateUploadReleaseContext(ctx, project, &workingMeta, 0, "")
	if err != nil {
		return nil, err
	}
	now := s.backend.Now().UTC()
	uid, gid := s.backend.DefaultOwnerIDs()
	fileMeta := meta.FileMetadata{
		Name:       cleanPath,
		Kind:       meta.NodeKindFile,
		Size:       0,
		Chunks:     []meta.ChunkInfo{},
		Release:    releaseTag,
		UploadedAt: now,
		ModifiedAt: now,
		AccessedAt: now,
		ChangedAt:  now,
		Mode:       ApplyCreateMode(ctx, s.backend.DefaultFileMode(meta.NodeKindFile)),
		UID:        uid,
		GID:        gid,
	}
	fileMeta.Mode, fileMeta.UID, fileMeta.GID = ApplyParentInheritance(repoMeta, cleanPath, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
	if _, err := s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := CheckParentWrite(ctx, repo, cleanPath); err != nil {
			return err
		}
		if err := RequireParentDirectory(repo, cleanPath); err != nil {
			return err
		}
		if repo.FindFile(cleanPath) != nil {
			return fmt.Errorf("file already exists: %s", cleanPath)
		}
		fileMeta.Mode, fileMeta.UID, fileMeta.GID = ApplyParentInheritance(repo, cleanPath, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
		fileMeta.Inode = repo.AllocateInode()
		repo.UpsertFile(fileMeta, now)
		TouchParentDirectory(repo, cleanPath, now)
		return nil
	}, fmt.Sprintf("storhub: create %s", cleanPath)); err != nil {
		return nil, err
	}
	return &fileMeta, nil
}

func (s *Service) MkdirContext(ctx context.Context, project, dirPath string) error {
	cleanPath, err := NormalizePath(dirPath)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		return nil
	}
	if err := s.backend.ValidateProjectName(project); err != nil {
		return err
	}
	if err := s.backend.EnsureRepoContext(ctx, project); err != nil {
		return err
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := CheckParentWrite(ctx, repo, cleanPath); err != nil {
			return err
		}
		if repo.HasDirectory(cleanPath) {
			return fmt.Errorf("directory already exists: %s", cleanPath)
		}
		if repo.FindFile(cleanPath) != nil {
			return fmt.Errorf("file already exists at path: %s", cleanPath)
		}
		if parent := ParentPath(cleanPath); parent != "" && !repo.HasDirectory(parent) {
			return fmt.Errorf("parent directory does not exist: %s", parent)
		}
		repo.EnsureDirectory(cleanPath, s.backend.Now().UTC())
		if dir := repo.GetDirectory(cleanPath); dir != nil {
			dir.Mode, dir.UID, dir.GID = ApplyParentInheritance(repo, cleanPath, true, ApplyCreateMode(ctx, dir.Mode), dir.UID, dir.GID)
			dir.ChangedAt = s.backend.Now().UTC()
		}
		TouchParentDirectory(repo, cleanPath, s.backend.Now().UTC())
		return nil
	}, fmt.Sprintf("storhub: mkdir %s", cleanPath))
	return err
}

func (s *Service) RmdirContext(ctx context.Context, project, dirPath string) error {
	cleanPath, err := NormalizePath(dirPath)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		return errors.New("cannot remove root directory")
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := CheckParentWrite(ctx, repo, cleanPath); err != nil {
			return err
		}
		if err := CheckStickyDelete(ctx, repo, ParentPath(cleanPath), cleanPath); err != nil {
			return err
		}
		if repo.FindFile(cleanPath) != nil {
			return fmt.Errorf("not a directory: %s", cleanPath)
		}
		if !repo.HasDirectory(cleanPath) {
			return fmt.Errorf("directory not found: %s", cleanPath)
		}
		childDirs, childFiles := repo.DirectoryChildren(cleanPath)
		if len(childDirs) > 0 || len(childFiles) > 0 {
			return fmt.Errorf("directory not empty: %s", cleanPath)
		}
		repo.RemoveDirectory(cleanPath)
		TouchParentDirectory(repo, cleanPath, s.backend.Now().UTC())
		return nil
	}, fmt.Sprintf("storhub: rmdir %s", cleanPath))
	return err
}

func (s *Service) RenameContext(ctx context.Context, project, oldPath, newPath string) error {
	oldClean, err := NormalizePath(oldPath)
	if err != nil {
		return err
	}
	newClean, err := NormalizePath(newPath)
	if err != nil {
		return err
	}
	if oldClean == newClean {
		return nil
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := CheckParentWrite(ctx, repo, oldClean); err != nil {
			return err
		}
		if err := CheckParentWrite(ctx, repo, newClean); err != nil {
			return err
		}
		if err := CheckTraverse(ctx, repo, oldClean); err != nil {
			return err
		}
		if parent := ParentPath(newClean); parent != "" && !repo.HasDirectory(parent) {
			return fmt.Errorf("parent directory does not exist: %s", parent)
		}
		if file := repo.FindFile(oldClean); file != nil {
			if err := CheckStickyDelete(ctx, repo, ParentPath(oldClean), oldClean); err != nil {
				return err
			}
			if existing := repo.FindFile(newClean); existing != nil {
				if err := CheckStickyDelete(ctx, repo, ParentPath(newClean), newClean); err != nil {
					return err
				}
			}
			if repo.FindFile(newClean) != nil || repo.HasDirectory(newClean) {
				return fmt.Errorf("destination already exists: %s", newClean)
			}
			renamed := file.Clone()
			renamed.Name = newClean
			renamed.ChangedAt = s.backend.Now().UTC()
			repo.RemoveFile(oldClean)
			repo.UpsertFile(renamed, s.backend.Now().UTC())
			TouchParentDirectory(repo, oldClean, s.backend.Now().UTC())
			TouchParentDirectory(repo, newClean, s.backend.Now().UTC())
			return nil
		}
		if !repo.HasDirectory(oldClean) {
			return fmt.Errorf("path not found: %s", oldClean)
		}
		if err := CheckStickyDelete(ctx, repo, ParentPath(oldClean), oldClean); err != nil {
			return err
		}
		if existing := repo.GetDirectory(newClean); existing != nil {
			if err := CheckStickyDelete(ctx, repo, ParentPath(newClean), newClean); err != nil {
				return err
			}
			_ = existing
		}
		if IsParentOrSame(oldClean, newClean) {
			return fmt.Errorf("cannot move directory %s into itself %s", oldClean, newClean)
		}
		if repo.FindFile(newClean) != nil || repo.HasDirectory(newClean) {
			return fmt.Errorf("destination already exists: %s", newClean)
		}
		now := s.backend.Now().UTC()
		for i := range repo.Directories {
			if IsParentOrSame(oldClean, repo.Directories[i].Path) {
				repo.Directories[i].Path = RemapPath(oldClean, newClean, repo.Directories[i].Path)
				repo.Directories[i].ModifiedAt = now
			}
		}
		for i := range repo.Releases {
			for j := range repo.Releases[i].Files {
				if IsParentOrSame(oldClean, repo.Releases[i].Files[j].Name) {
					repo.Releases[i].Files[j].Name = RemapPath(oldClean, newClean, repo.Releases[i].Files[j].Name)
					repo.Releases[i].Files[j].ChangedAt = now
				}
			}
		}
		TouchParentDirectory(repo, oldClean, now)
		TouchParentDirectory(repo, newClean, now)
		repo.RecomputeStats()
		return nil
	}, fmt.Sprintf("storhub: rename %s to %s", oldClean, newClean))
	return err
}

func (s *Service) TruncateFileContext(ctx context.Context, project, filePath string, size int64) (*meta.FileMetadata, error) {
	cleanPath, err := NormalizePath(filePath)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, errors.New("truncate size must be non-negative")
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckWriteAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if file.Kind == meta.NodeKindSymlink {
		return nil, fmt.Errorf("cannot truncate symlink: %s", cleanPath)
	}
	if size == file.Size {
		clone := file.Clone()
		return &clone, nil
	}
	if size < file.Size {
		return s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, size, file.Size-size, nil)
	}
	return s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, make([]byte, size-file.Size))
}

func (s *Service) AppendFileContext(ctx context.Context, project, filePath string, data []byte) (*meta.FileMetadata, error) {
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath, err := NormalizePath(filePath)
	if err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckWriteAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if file.Kind == meta.NodeKindSymlink {
		return nil, fmt.Errorf("cannot append to symlink: %s", cleanPath)
	}
	return s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, data)
}

func (s *Service) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte) (*meta.FileMetadata, error) {
	cleanPath, err := NormalizePath(filePath)
	if err != nil {
		return nil, err
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckWriteAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if file.Kind == meta.NodeKindSymlink {
		return nil, fmt.Errorf("cannot write symlink: %s", cleanPath)
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
		return s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, append(gap, data...))
	}
	deleteSize := int64(len(data))
	if max := file.Size - offset; deleteSize > max {
		deleteSize = max
	}
	return s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, offset, deleteSize, data)
}

func (s *Service) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	cleanPath, err := NormalizePath(filePath)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if file.Kind == meta.NodeKindSymlink {
		return nil, fmt.Errorf("cannot read symlink as file: %s", cleanPath)
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
	startIndex := sort.Search(len(file.Chunks), func(i int) bool {
		return file.Chunks[i].Offset+file.Chunks[i].Size > offset
	})
	for _, chunk := range file.Chunks[startIndex:] {
		chunkEnd := chunk.Offset + chunk.Size
		if chunk.Offset >= end {
			break
		}
		if chunkEnd <= offset || chunk.Size == 0 {
			continue
		}
		start := MaxInt64(offset, chunk.Offset)
		stop := MinInt64(end, chunkEnd)
		segment := chunk
		segment.Offset = start
		segment.AssetOffset = chunk.AssetOffset + (start - chunk.Offset)
		segment.Size = stop - start
		dst := result[start-offset : stop-offset]
		if err := s.backend.FillAssetRangeContext(ctx, project, segment, dst); err != nil {
			return nil, err
		}
	}
	TouchFileAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now().UTC())
	return result, nil
}

func (s *Service) StatPathContext(ctx context.Context, project, targetPath string) (*EntryInfo, error) {
	cleanPath := ""
	if strings.TrimSpace(targetPath) != "" {
		var err error
		cleanPath, err = NormalizePath(targetPath)
		if err != nil {
			return nil, err
		}
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	if err := CheckTraverse(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if cleanPath == "" {
		return &EntryInfo{Path: "", IsDir: true, Inode: repo.Root.Inode, Mode: repo.Root.Mode, UID: repo.Root.UID, GID: repo.Root.GID, NLink: repo.Root.NLink, CreatedAt: repo.Root.CreatedAt, ModifiedAt: repo.Root.ModifiedAt, AccessedAt: repo.Root.AccessedAt, ChangedAt: repo.Root.ChangedAt}, nil
	}
	if file := repo.FindFile(cleanPath); file != nil {
		return EntryInfoFromFile(file), nil
	}
	if dir := repo.GetDirectory(cleanPath); dir != nil {
		return EntryInfoFromDirectory(dir), nil
	}
	return nil, fmt.Errorf("path not found: %s", cleanPath)
}

func (s *Service) StatFSContext(ctx context.Context, project string) (*FSStats, error) {
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	stats := &FSStats{Files: repo.TotalFiles, Directories: len(repo.Directories), Inodes: CountUniqueInodes(repo), Bytes: repo.TotalSize, Releases: len(repo.Releases)}
	for _, release := range repo.Releases {
		stats.Assets += release.AssetCount
	}
	return stats, nil
}

func (s *Service) ReadDirContext(ctx context.Context, project, dirPath string) ([]DirEntry, error) {
	cleanPath := ""
	if strings.TrimSpace(dirPath) != "" {
		var err error
		cleanPath, err = NormalizePath(dirPath)
		if err != nil {
			return nil, err
		}
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	if err := CheckListDirAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if cleanPath != "" && repo.FindFile(cleanPath) != nil {
		return nil, fmt.Errorf("not a directory: %s", cleanPath)
	}
	if cleanPath != "" && !repo.HasDirectory(cleanPath) {
		return nil, fmt.Errorf("directory not found: %s", cleanPath)
	}
	dirs, files := repo.DirectoryChildren(cleanPath)
	entries := make([]DirEntry, 0, len(dirs)+len(files))
	for _, dir := range dirs {
		entries = append(entries, DirEntryFromDirectory(dir))
	}
	for _, file := range files {
		entries = append(entries, DirEntryFromFile(file))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	TouchDirectoryAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now().UTC())
	return entries, nil
}

func RequireParentDirectory(repo *meta.RepoMetadata, filePath string) error {
	if parent := ParentPath(filePath); parent != "" && !repo.HasDirectory(parent) {
		return fmt.Errorf("parent directory does not exist: %s", parent)
	}
	return nil
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func EntryInfoFromFile(file *meta.FileMetadata) *EntryInfo {
	return &EntryInfo{
		Path:          file.Name,
		Kind:          file.Kind,
		IsSymlink:     file.Kind == meta.NodeKindSymlink,
		Size:          file.Size,
		Inode:         file.Inode,
		Mode:          file.Mode,
		UID:           file.UID,
		GID:           file.GID,
		NLink:         file.NLink,
		CreatedAt:     file.UploadedAt,
		ModifiedAt:    file.ModifiedAt,
		AccessedAt:    file.AccessedAt,
		ChangedAt:     file.ChangedAt,
		SymlinkTarget: file.SymlinkTarget,
	}
}

func EntryInfoFromDirectory(dir *meta.DirectoryMetadata) *EntryInfo {
	return &EntryInfo{
		Path:       dir.Path,
		IsDir:      true,
		Inode:      dir.Inode,
		Mode:       dir.Mode,
		UID:        dir.UID,
		GID:        dir.GID,
		NLink:      dir.NLink,
		CreatedAt:  dir.CreatedAt,
		ModifiedAt: dir.ModifiedAt,
		AccessedAt: dir.AccessedAt,
		ChangedAt:  dir.ChangedAt,
	}
}

func DirEntryFromDirectory(dir meta.DirectoryMetadata) DirEntry {
	return DirEntry{Name: path.Base(dir.Path), Path: dir.Path, IsDir: true, Inode: dir.Inode, Mode: dir.Mode, NLink: dir.NLink}
}

func DirEntryFromFile(file meta.FileMetadata) DirEntry {
	return DirEntry{Name: path.Base(file.Name), Path: file.Name, Kind: file.Kind, IsSymlink: file.Kind == meta.NodeKindSymlink, Size: file.Size, Inode: file.Inode, Mode: file.Mode, NLink: file.NLink}
}

func CountUniqueInodes(repo *meta.RepoMetadata) int {
	seen := map[uint64]struct{}{repo.Root.Inode: {}}
	for _, dir := range repo.Directories {
		seen[dir.Inode] = struct{}{}
	}
	for _, file := range repo.AllFiles() {
		seen[file.Inode] = struct{}{}
	}
	return len(seen)
}
