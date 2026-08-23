package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type Backend interface {
	ValidateProjectName(project string) error
	EnsureRepoContext(ctx context.Context, project string) error
	LoadRepoMetadataContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error)
	GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *meta.RepoMetadata, requiredSize int, preferredTag string) (string, string, error)
	PatchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *meta.RepoMetadata, fileMeta *meta.FileMeta, offset, deleteSize int64, edit []byte) (*meta.FileMeta, error)
	FillAssetRangeContext(ctx context.Context, project string, segment meta.ChunkInfo, dst []byte) error
	QueueAtimeUpdateContext(ctx context.Context, project, targetPath string, isDir bool, now int64)
	Logger() *slog.Logger
	Now() int64
	AtimePolicy() storcfg.AtimePolicy
	FileNotFound(path string) error
	DefaultFileMode(kind meta.NodeKind) uint32
	DefaultOwnerIDs() (uint32, uint32)
}

type Service struct {
	backend Backend
}

const PendingReleaseTag = "pending"

func NewService(backend Backend) *Service {
	return &Service{backend: backend}
}

func (s *Service) logger(project string) *slog.Logger {
	return logging.WithComponent(s.backend.Logger(), "fs").With("project", project)
}

func (s *Service) logFinish(project, op string, started time.Time, err error, args ...any) {
	args = append(args, "elapsed", time.Since(started))
	if err != nil {
		args = append(args, "err", err)
		logging.Error(s.logger(project), op+" failed", args...)
		return
	}
	logging.Info(s.logger(project), op+" complete", args...)
}

func (s *Service) CreateFileContext(ctx context.Context, project, filePath string) (result *meta.FileMeta, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "create-file start", "path", filePath)
	defer func() { s.logFinish(project, "create-file", started, err, "path", filePath) }()
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
	if repoMeta.HasDirectory(cleanPath) {
		return nil, IsDirectory(cleanPath)
	}
	if repoMeta.FindFile(cleanPath) != nil {
		return nil, AlreadyExists(cleanPath)
	}
	now := s.backend.Now()
	defaultUID, defaultGID := s.backend.DefaultOwnerIDs()
	uid, gid := OwnerIDsForCreate(ctx, defaultUID, defaultGID)
	fileMeta := meta.FileMeta{
		Size:       0,
		Chunks:     []int64{},
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
		if repo.HasDirectory(cleanPath) {
			return IsDirectory(cleanPath)
		}
		if repo.FindFile(cleanPath) != nil {
			return AlreadyExists(cleanPath)
		}
		fileMeta.Mode, fileMeta.UID, fileMeta.GID = ApplyParentInheritance(repo, cleanPath, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
		fileMeta.Inode = repo.AllocateInode()
		repo.UpsertFile(cleanPath, fileMeta, now)
		TouchParentDirectory(repo, cleanPath, now)
		return nil
	}, fmt.Sprintf("storhub: create %s", cleanPath)); err != nil {
		return nil, err
	}
	result = &fileMeta
	return result, nil
}

func (s *Service) MkdirContext(ctx context.Context, project, dirPath string) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "mkdir start", "path", dirPath)
	defer func() { s.logFinish(project, "mkdir", started, err, "path", dirPath) }()
	cleanPath, err := NormalizePath(dirPath)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		// POSIX: mkdir on an existing directory fails; the root always
		// exists.
		return AlreadyExists("/")
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
			return AlreadyExists(cleanPath)
		}
		if repo.FindFile(cleanPath) != nil {
			return AlreadyExists(cleanPath)
		}
		if parent := ParentPath(cleanPath); parent != "" && !repo.HasDirectory(parent) {
			return NotFound(parent)
		}
		repo.EnsureDirectory(cleanPath, s.backend.Now())
		if dir := repo.GetDirectory(cleanPath); dir != nil {
			dir.UID, dir.GID = OwnerIDsForCreate(ctx, dir.UID, dir.GID)
			dir.Mode, dir.UID, dir.GID = ApplyParentInheritance(repo, cleanPath, true, ApplyCreateMode(ctx, dir.Mode), dir.UID, dir.GID)
			dir.ChangedAt = s.backend.Now()
			repo.Dirs[cleanPath] = *dir
		}
		TouchParentDirectory(repo, cleanPath, s.backend.Now())
		return nil
	}, fmt.Sprintf("storhub: mkdir %s", cleanPath))
	return err
}

func (s *Service) RmdirContext(ctx context.Context, project, dirPath string) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "rmdir start", "path", dirPath)
	defer func() { s.logFinish(project, "rmdir", started, err, "path", dirPath) }()
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
			return NotDirectory(cleanPath)
		}
		if !repo.HasDirectory(cleanPath) {
			return NotFound(cleanPath)
		}
		childDirs, childFiles := repo.DirectoryChildren(cleanPath)
		if len(childDirs) > 0 || len(childFiles) > 0 {
			return NotEmpty(cleanPath)
		}
		repo.RemoveDirectory(cleanPath)
		TouchParentDirectory(repo, cleanPath, s.backend.Now())
		return nil
	}, fmt.Sprintf("storhub: rmdir %s", cleanPath))
	return err
}

func (s *Service) RenameContext(ctx context.Context, project, oldPath, newPath string) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "rename start", "old_path", oldPath, "new_path", newPath)
	defer func() { s.logFinish(project, "rename", started, err, "old_path", oldPath, "new_path", newPath) }()
	oldClean, err := NormalizePath(oldPath)
	if err != nil {
		return err
	}
	newClean, err := NormalizePath(newPath)
	if err != nil {
		return err
	}
	if oldClean == newClean {
		// POSIX: rename(x, x) succeeds when x exists, ENOENT otherwise.
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		if repo.FindFile(oldClean) == nil && !repo.HasDirectory(oldClean) {
			return NotFound(oldClean)
		}
		return nil
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		srcFile := repo.FindFile(oldClean)
		srcIsDir := repo.HasDirectory(oldClean)
		if srcFile == nil && !srcIsDir {
			return NotFound(oldClean)
		}
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
			return NotFound(parent)
		}
		dstFile := repo.FindFile(newClean)
		dstDir := repo.GetDirectory(newClean)
		now := s.backend.Now()

		if srcFile != nil {
			if err := CheckStickyDelete(ctx, repo, ParentPath(oldClean), oldClean); err != nil {
				return err
			}
			if dstFile != nil || dstDir != nil {
				if err := CheckStickyDelete(ctx, repo, ParentPath(newClean), newClean); err != nil {
					return err
				}
			}
			// POSIX: renaming a file onto an existing directory fails with
			// EISDIR; onto an existing file it atomically replaces it.
			if dstDir != nil {
				return syscall.EISDIR
			}
			renamed := srcFile.Clone()
			renamed.ChangedAt = now
			repo.RemoveFile(oldClean)
			if dstFile != nil {
				repo.RemoveFile(newClean)
			}
			repo.UpsertFile(newClean, renamed, now)
			TouchParentDirectory(repo, oldClean, now)
			TouchParentDirectory(repo, newClean, now)
			return nil
		}

		if err := CheckStickyDelete(ctx, repo, ParentPath(oldClean), oldClean); err != nil {
			return err
		}
		// POSIX: renaming a directory onto an existing file fails with
		// ENOTDIR regardless of path relation.
		if dstFile != nil {
			return syscall.ENOTDIR
		}
		if dstDir != nil {
			if err := CheckStickyDelete(ctx, repo, ParentPath(newClean), newClean); err != nil {
				return err
			}
		}
		if IsParentOrSame(oldClean, newClean) {
			return fmt.Errorf("cannot move directory %s into itself %s", oldClean, newClean)
		}
		// Directory onto empty directory replaces it.
		if dstDir != nil {
			childDirs, childFiles := repo.DirectoryChildren(newClean)
			if len(childDirs) > 0 || len(childFiles) > 0 {
				return NotEmpty(newClean)
			}
			repo.RemoveDirectory(newClean)
		}
		updatedDirs := make(map[string]meta.DirMeta, len(repo.Dirs))
		for path, dir := range repo.Dirs {
			if IsParentOrSame(oldClean, path) {
				newPath := RemapPath(oldClean, newClean, path)
				dir.ModifiedAt = now
				dir.ChangedAt = now
				updatedDirs[newPath] = dir
			} else {
				updatedDirs[path] = dir
			}
		}
		repo.Dirs = updatedDirs
		updatedFiles := make(map[string]meta.FileMeta, len(repo.Files))
		for path, file := range repo.Files {
			if IsParentOrSame(oldClean, path) {
				newPath := RemapPath(oldClean, newClean, path)
				file.ChangedAt = now
				updatedFiles[newPath] = file
			} else {
				updatedFiles[path] = file
			}
		}
		repo.Files = updatedFiles
		TouchParentDirectory(repo, oldClean, now)
		TouchParentDirectory(repo, newClean, now)
		repo.RecomputeStats()
		return nil
	}, fmt.Sprintf("storhub: rename %s to %s", oldClean, newClean))
	return err
}

func (s *Service) TruncateFileContext(ctx context.Context, project, filePath string, size int64) (result *meta.FileMeta, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "truncate start", "path", filePath, "size", size)
	defer func() { s.logFinish(project, "truncate", started, err, "path", filePath, "size", size) }()
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
	if file.Symlink != "" {
		return nil, fmt.Errorf("cannot truncate symlink: %s", cleanPath)
	}
	if size == file.Size {
		clone := file.Clone()
		result = &clone
		return result, nil
	}
	if size < file.Size {
		result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, size, file.Size-size, nil)
		return result, err
	}
	result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, make([]byte, size-file.Size))
	return result, err
}

func (s *Service) AppendFileContext(ctx context.Context, project, filePath string, data []byte) (result *meta.FileMeta, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "append start", "path", filePath, "bytes", len(data))
	defer func() { s.logFinish(project, "append", started, err, "path", filePath, "bytes", len(data)) }()
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
	if file.Symlink != "" {
		return nil, fmt.Errorf("cannot append to symlink: %s", cleanPath)
	}
	result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, data)
	return result, err
}

func (s *Service) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte) (result *meta.FileMeta, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "write-at start", "path", filePath, "offset", offset, "bytes", len(data))
	defer func() {
		s.logFinish(project, "write-at", started, err, "path", filePath, "offset", offset, "bytes", len(data))
	}()
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
	if file.Symlink != "" {
		return nil, fmt.Errorf("cannot write symlink: %s", cleanPath)
	}
	if offset < 0 {
		return nil, errors.New("write offset must be non-negative")
	}
	if len(data) == 0 {
		clone := file.Clone()
		result = &clone
		return result, nil
	}
	if offset > file.Size {
		gap := make([]byte, offset-file.Size)
		result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, append(gap, data...))
		return result, err
	}
	deleteSize := int64(len(data))
	if max := file.Size - offset; deleteSize > max {
		deleteSize = max
	}
	result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, offset, deleteSize, data)
	return result, err
}

func (s *Service) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) (result []byte, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "read-at start", "path", filePath, "offset", offset, "length", length)
	defer func() {
		s.logFinish(project, "read-at", started, err, "path", filePath, "offset", offset, "length", length)
	}()
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
	if file.Symlink != "" {
		return nil, fmt.Errorf("cannot read symlink as file: %s", cleanPath)
	}
	if offset > file.Size {
		return nil, io.EOF
	}
	if length == 0 {
		result = []byte{}
		return result, nil
	}
	end := offset + length
	if end > file.Size {
		end = file.Size
	}
	result = make([]byte, end-offset)
	chunks := repo.FileChunks(cleanPath)
	startIndex := sort.Search(len(chunks), func(i int) bool {
		return chunks[i].Offset+chunks[i].Size > offset
	})
	for _, chunk := range chunks[startIndex:] {
		chunkEnd := chunk.Offset + chunk.Size
		if chunk.Offset >= end {
			break
		}
		if chunkEnd <= offset || chunk.Size == 0 {
			continue
		}
		start := max(offset, chunk.Offset)
		stop := min(end, chunkEnd)
		segment := chunk
		segment.Offset = start
		segment.AssetOffset = chunk.AssetOffset + (start - chunk.Offset)
		segment.Size = stop - start
		dst := result[start-offset : stop-offset]
		if err := s.backend.FillAssetRangeContext(ctx, project, segment, dst); err != nil {
			return nil, err
		}
	}
	TouchFileAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now())
	return result, nil
}

func (s *Service) StatPathContext(ctx context.Context, project, targetPath string) (result *EntryInfo, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "stat-path start", "path", targetPath)
	defer func() { s.logFinish(project, "stat-path", started, err, "path", targetPath) }()
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
		result = &EntryInfo{Path: "", IsDir: true, Inode: repo.Root.Inode, Mode: repo.Root.Mode, UID: repo.Root.UID, GID: repo.Root.GID, NLink: uint32(repo.DirNLink("")), CreatedAt: repo.Root.CreatedAt, ModifiedAt: repo.Root.ModifiedAt, AccessedAt: repo.Root.AccessedAt, ChangedAt: repo.Root.ChangedAt}
		return result, nil
	}
	// lstat semantics: a terminal symlink reports itself; intermediate
	// components were already resolved by CheckTraverse. Callers wanting
	// stat() semantics compose LookupNodeFollowed/ResolvePath.
	if file := repo.FindFile(cleanPath); file != nil {
		result = EntryInfoFromFile(file, cleanPath, repo.FileNLink(cleanPath))
		return result, nil
	}
	if dir := repo.GetDirectory(cleanPath); dir != nil {
		result = EntryInfoFromDirectory(dir, cleanPath, repo.DirNLink(cleanPath))
		return result, nil
	}
	return nil, NotFound(cleanPath)
}

func (s *Service) StatFSContext(ctx context.Context, project string) (result *FSStats, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "statfs start")
	defer func() { s.logFinish(project, "statfs", started, err) }()
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	// Aggregate live instead of trusting cached counters: those only
	// refresh during metadata commits, so a freshly mutated tree would
	// otherwise report stale numbers.
	files := 0
	var totalBytes int64
	for _, file := range repo.Files {
		if file.Symlink == "" {
			files++
			totalBytes += file.Size
		}
	}
	stats := &FSStats{Files: files, Directories: len(repo.Dirs), Inodes: CountUniqueInodes(repo), Bytes: totalBytes, Releases: len(repo.Releases)}
	assetCounts := make(map[string]int, len(repo.Chunks))
	for _, chunk := range repo.Chunks {
		if chunk.Release != "" {
			assetCounts[chunk.Release]++
		}
	}
	for _, count := range assetCounts {
		stats.Assets += count
	}
	result = stats
	return result, nil
}

func (s *Service) ReadDirContext(ctx context.Context, project, dirPath string) (result []DirEntry, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "readdir start", "path", dirPath)
	defer func() { s.logFinish(project, "readdir", started, err, "path", dirPath) }()
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
		return nil, NotDirectory(cleanPath)
	}
	if cleanPath != "" && !repo.HasDirectory(cleanPath) {
		return nil, NotFound(cleanPath)
	}
	dirNames, fileNames := repo.DirectoryChildren(cleanPath)
	entries := make([]DirEntry, 0, len(dirNames)+len(fileNames))
	for _, name := range dirNames {
		if dir := repo.GetDirectory(name); dir != nil {
			entries = append(entries, DirEntryFromDirectory(*dir, name, repo.DirNLink(name)))
		}
	}
	for _, name := range fileNames {
		if file := repo.FindFile(name); file != nil {
			entries = append(entries, DirEntryFromFile(*file, name, repo.FileNLink(name)))
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	TouchDirectoryAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now())
	result = entries
	return result, nil
}

func RequireParentDirectory(repo *meta.RepoMetadata, filePath string) error {
	if parent := ParentPath(filePath); parent != "" && !repo.HasDirectory(parent) {
		return fmt.Errorf("%w: parent directory does not exist: %s", ErrNotFound, parent)
	}
	return nil
}

func EntryInfoFromFile(file *meta.FileMeta, path string, nlink int) *EntryInfo {
	kind := meta.NodeKindFile
	if file.Symlink != "" {
		kind = meta.NodeKindSymlink
	}
	return &EntryInfo{
		Path:          path,
		Kind:          kind,
		IsSymlink:     file.Symlink != "",
		Size:          file.Size,
		Inode:         file.Inode,
		Mode:          file.Mode,
		UID:           file.UID,
		GID:           file.GID,
		NLink:         uint32(nlink),
		CreatedAt:     file.UploadedAt,
		ModifiedAt:    file.ModifiedAt,
		AccessedAt:    file.AccessedAt,
		ChangedAt:     file.ChangedAt,
		SymlinkTarget: file.Symlink,
	}
}

func EntryInfoFromDirectory(dir *meta.DirMeta, path string, nlink int) *EntryInfo {
	return &EntryInfo{
		Path:       path,
		IsDir:      true,
		Inode:      dir.Inode,
		Mode:       dir.Mode,
		UID:        dir.UID,
		GID:        dir.GID,
		NLink:      uint32(nlink),
		CreatedAt:  dir.CreatedAt,
		ModifiedAt: dir.ModifiedAt,
		AccessedAt: dir.AccessedAt,
		ChangedAt:  dir.ChangedAt,
	}
}

func DirEntryFromDirectory(dir meta.DirMeta, dirPath string, nlink int) DirEntry {
	return DirEntry{Name: path.Base(dirPath), Path: dirPath, IsDir: true, Inode: dir.Inode, Mode: dir.Mode, NLink: uint32(nlink)}
}

func DirEntryFromFile(file meta.FileMeta, filePath string, nlink int) DirEntry {
	return DirEntry{Name: path.Base(filePath), Path: filePath, IsSymlink: file.Symlink != "", Size: file.Size, Inode: file.Inode, Mode: file.Mode, NLink: uint32(nlink)}
}

func CountUniqueInodes(repo *meta.RepoMetadata) int {
	seen := map[uint64]struct{}{repo.Root.Inode: {}}
	for _, dir := range repo.Dirs {
		seen[dir.Inode] = struct{}{}
	}
	for _, file := range repo.AllFiles() {
		seen[file.Inode] = struct{}{}
	}
	return len(seen)
}
