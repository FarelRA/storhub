package fs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sort"
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
	GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *meta.RepoMetadata, requiredSize int) (string, string, error)
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
	// Create addresses the new node itself (O_CREAT|O_EXCL never follows a
	// final symlink), so followFinal is false; intermediate symlink
	// components are still resolved physically.
	if err := ValidateAccessPathShape(filePath); err != nil {
		return nil, err
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
	cleanPath, traversed, err := ResolveAccessPath(repoMeta, filePath, false)
	if err != nil {
		return nil, err
	}
	if cleanPath == "" {
		return nil, errors.New("file path is required")
	}
	if err := CheckTraversal(ctx, repoMeta, traversed); err != nil {
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
		if err := CheckTraversal(ctx, repo, traversed); err != nil {
			return err
		}
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
	// mkdir never creates through a final symlink (EEXIST on the link
	// itself), so followFinal is false; intermediate components resolve
	// physically.
	if err := ValidateAccessPathShape(dirPath); err != nil {
		return err
	}
	if err := s.backend.ValidateProjectName(project); err != nil {
		return err
	}
	if err := s.backend.EnsureRepoContext(ctx, project); err != nil {
		return err
	}
	repoMeta, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return err
	}
	cleanPath, traversed, err := ResolveAccessPath(repoMeta, dirPath, false)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		// POSIX: mkdir on an existing directory fails; the root always
		// exists.
		return AlreadyExists("/")
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := CheckTraversal(ctx, repo, traversed); err != nil {
			return err
		}
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
	// rmdir removes the final component itself; a symlink there must not be
	// followed (POSIX rmdir on a symlink is ENOTDIR), so followFinal is
	// false.
	if err := ValidateAccessPathShape(dirPath); err != nil {
		return err
	}
	repoMeta, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return err
	}
	cleanPath, traversed, err := ResolveAccessPath(repoMeta, dirPath, false)
	if err != nil {
		return err
	}
	if cleanPath == "" {
		// POSIX: rmdir("/") fails with EBUSY, not a generic error that
		// errno mapping would surface as EIO.
		return syscall.EBUSY
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := CheckTraversal(ctx, repo, traversed); err != nil {
			return err
		}
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

func (s *Service) RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...MutateOption) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "rename start", "old_path", oldPath, "new_path", newPath)
	defer func() { s.logFinish(project, "rename", started, err, "old_path", oldPath, "new_path", newPath) }()
	mutate := ApplyMutateOptions(opts)
	// rename(2) renames the final component itself: a symlink endpoint is
	// moved, never followed, so followFinal is false on both endpoints
	// (intermediate components still resolve physically).
	if err := ValidateAccessPathShape(oldPath); err != nil {
		return err
	}
	if err := ValidateAccessPathShape(newPath); err != nil {
		return err
	}
	preRepo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return err
	}
	oldClean, oldTraversed, err := ResolveAccessPath(preRepo, oldPath, false)
	if err != nil {
		return err
	}
	newClean, newTraversed, err := ResolveAccessPath(preRepo, newPath, false)
	if err != nil {
		return err
	}
	if oldClean == newClean {
		// POSIX: rename(x, x) succeeds when x exists, ENOENT otherwise.
		if preRepo.FindFile(oldClean) == nil && !preRepo.HasDirectory(oldClean) {
			return NotFound(oldClean)
		}
		return nil
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := CheckTraversal(ctx, repo, oldTraversed); err != nil {
			return err
		}
		if err := CheckTraversal(ctx, repo, newTraversed); err != nil {
			return err
		}
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
		if parent := ParentPath(newClean); parent != "" && !repo.HasDirectory(parent) {
			return NotFound(parent)
		}
		dstFile := repo.FindFile(newClean)
		dstDir := repo.GetDirectory(newClean)
		// RENAME_NOREPLACE: the existence decision is made against the
		// live transaction state, closing the TOCTOU window a pre-stat
		// check leaves open.
		if mutate.NoReplace() && (dstFile != nil || dstDir != nil) {
			return AlreadyExists(newClean)
		}
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
		for dirPath, dir := range repo.Dirs {
			if IsParentOrSame(oldClean, dirPath) {
				newPath := RemapPath(oldClean, newClean, dirPath)
				dir.ModifiedAt = now
				dir.ChangedAt = now
				updatedDirs[newPath] = dir
			} else {
				updatedDirs[dirPath] = dir
			}
		}
		repo.Dirs = updatedDirs
		updatedFiles := make(map[string]meta.FileMeta, len(repo.Files))
		for filePath, file := range repo.Files {
			if IsParentOrSame(oldClean, filePath) {
				newPath := RemapPath(oldClean, newClean, filePath)
				file.ChangedAt = now
				updatedFiles[newPath] = file
			} else {
				updatedFiles[filePath] = file
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

func (s *Service) CopyContext(ctx context.Context, project, srcPath, dstPath string) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "copy start", "src", srcPath, "dst", dstPath)
	defer func() { s.logFinish(project, "copy", started, err, "src", srcPath, "dst", dstPath) }()
	// cp follows symlinks at both endpoints: the source is read through
	// (stat semantics) and the destination is written through (open
	// semantics), so followFinal is true on both.
	if err := ValidateAccessPathShape(srcPath); err != nil {
		return err
	}
	if err := ValidateAccessPathShape(dstPath); err != nil {
		return err
	}
	preRepo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return err
	}
	srcClean, srcTraversed, err := ResolveAccessPath(preRepo, srcPath, true)
	if err != nil {
		return err
	}
	dstClean, dstTraversed, err := ResolveAccessPath(preRepo, dstPath, true)
	if err != nil {
		return err
	}
	if srcClean == dstClean {
		if preRepo.FindFile(srcClean) == nil && !preRepo.HasDirectory(srcClean) {
			return NotFound(srcClean)
		}
		return AlreadyExists(srcClean)
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := CheckTraversal(ctx, repo, srcTraversed); err != nil {
			return err
		}
		if err := CheckTraversal(ctx, repo, dstTraversed); err != nil {
			return err
		}
		srcFile := repo.FindFile(srcClean)
		srcIsDir := repo.HasDirectory(srcClean)
		if srcFile == nil && !srcIsDir {
			return NotFound(srcClean)
		}
		// A copy is a read of the source: mirror the read-side DAC of
		// ReadFileAtContext/ReadDirContext, or an attacker could duplicate
		// unreadable 0600 files (content refs, sizes, symlink targets)
		// into their own directory.
		if srcFile != nil {
			if err := CheckReadAccess(ctx, repo, srcClean); err != nil {
				return err
			}
		} else {
			if err := CheckListDirAccess(ctx, repo, srcClean); err != nil {
				return err
			}
		}
		if err := CheckParentWrite(ctx, repo, dstClean); err != nil {
			return err
		}
		if parent := ParentPath(dstClean); parent != "" && !repo.HasDirectory(parent) {
			return NotFound(parent)
		}
		dstFile := repo.FindFile(dstClean)
		dstDir := repo.GetDirectory(dstClean)
		now := s.backend.Now()
		if srcFile != nil {
			if dstDir != nil {
				return syscall.EISDIR
			}
			if dstFile != nil {
				if err := CheckStickyDelete(ctx, repo, ParentPath(dstClean), dstClean); err != nil {
					return err
				}
				repo.RemoveFile(dstClean)
			}
			cloned := srcFile.Clone()
			cloned.Inode = repo.AllocateInode()
			cloned.ChangedAt = now
			repo.UpsertFile(dstClean, cloned, now)
			TouchParentDirectory(repo, dstClean, now)
			return nil
		}
		if dstFile != nil {
			return syscall.ENOTDIR
		}
		if dstDir != nil {
			childDirs, childFiles := repo.DirectoryChildren(dstClean)
			if len(childDirs) > 0 || len(childFiles) > 0 {
				return NotEmpty(dstClean)
			}
			if err := CheckStickyDelete(ctx, repo, ParentPath(dstClean), dstClean); err != nil {
				return err
			}
			repo.RemoveDirectory(dstClean)
		}
		if IsParentOrSame(srcClean, dstClean) {
			return fmt.Errorf("cannot copy directory %s into itself %s", srcClean, dstClean)
		}
		srcDir := repo.GetDirectory(srcClean)
		if srcDir == nil {
			return NotFound(srcClean)
		}
		newDir := srcDir.Clone()
		newDir.Inode = repo.AllocateInode()
		newDir.ModifiedAt = now
		newDir.ChangedAt = now
		newDir.AccessedAt = now
		newDir.CreatedAt = now
		repo.Dirs[dstClean] = newDir
		dirsToCopy := make(map[string]meta.DirMeta)
		for p, d := range repo.Dirs {
			if p == srcClean {
				continue
			}
			if IsParentOrSame(srcClean, p) {
				newPath := RemapPath(srcClean, dstClean, p)
				if repo.HasDirectory(newPath) || repo.FindFile(newPath) != nil {
					return AlreadyExists(newPath)
				}
				if _, pending := dirsToCopy[newPath]; pending {
					return AlreadyExists(newPath)
				}
				cloned := d.Clone()
				cloned.Inode = repo.AllocateInode()
				cloned.ModifiedAt = now
				cloned.ChangedAt = now
				cloned.AccessedAt = now
				dirsToCopy[newPath] = cloned
			}
		}
		for newPath, d := range dirsToCopy {
			repo.Dirs[newPath] = d
		}
		filesToCopy := make(map[string]meta.FileMeta)
		for p, f := range repo.Files {
			if IsParentOrSame(srcClean, p) {
				newPath := RemapPath(srcClean, dstClean, p)
				if repo.HasDirectory(newPath) || repo.FindFile(newPath) != nil {
					return AlreadyExists(newPath)
				}
				if _, pending := filesToCopy[newPath]; pending {
					return AlreadyExists(newPath)
				}
				if _, pending := dirsToCopy[newPath]; pending {
					return AlreadyExists(newPath)
				}
				cloned := f.Clone()
				cloned.Inode = repo.AllocateInode()
				cloned.ChangedAt = now
				filesToCopy[newPath] = cloned
			}
		}
		for newPath, f := range filesToCopy {
			repo.UpsertFile(newPath, f, now)
		}
		TouchParentDirectory(repo, dstClean, now)
		repo.RecomputeStats()
		return nil
	}, fmt.Sprintf("storhub: copy %s to %s", srcClean, dstClean))
	return err
}

func (s *Service) TruncateFileContext(ctx context.Context, project, filePath string, size int64) (result *meta.FileMeta, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "truncate start", "path", filePath, "size", size)
	defer func() { s.logFinish(project, "truncate", started, err, "path", filePath, "size", size) }()
	// truncate(2) has open() semantics: a final symlink is followed to its
	// target.
	if err := ValidateAccessPathShape(filePath); err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, errors.New("truncate size must be non-negative")
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath, traversed, err := ResolveAccessPath(repo, filePath, true)
	if err != nil {
		return nil, err
	}
	if err := CheckTraversal(ctx, repo, traversed); err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckWriteAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if size == file.Size {
		// POSIX: even a no-op truncate updates mtime/ctime.
		now := s.backend.Now()
		if _, err := s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
			if err := CheckTraversal(ctx, repo, traversed); err != nil {
				return err
			}
			if err := CheckWriteAccess(ctx, repo, cleanPath); err != nil {
				return err
			}
			current := repo.FindFile(cleanPath)
			if current == nil {
				return s.backend.FileNotFound(cleanPath)
			}
			clone := current.Clone()
			clone.ModifiedAt = now
			clone.ChangedAt = now
			repo.ReplaceFile(cleanPath, clone)
			return nil
		}, fmt.Sprintf("storhub: truncate touch %s", cleanPath)); err != nil {
			return nil, err
		}
		clone := file.Clone()
		clone.ModifiedAt = now
		clone.ChangedAt = now
		result = &clone
		return result, nil
	}
	if size < file.Size {
		result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, size, file.Size-size, nil)
		return result, err
	}
	result, err = s.zeroExtendFile(ctx, project, cleanPath, traversed, size)
	return result, err
}

// zeroFillChunkSize bounds one streamed zero-fill patch: the extension
// region is never materialized in RAM (a `truncate -s 1T` must not OOM the
// process), it is written to the backend in fixed-size chunks.
const zeroFillChunkSize = 1 << 20

// zeroExtendFile grows a file to targetSize by streaming fixed-size zero
// chunks through the backend's range-patch verb. The repo view is
// re-loaded per chunk so each patch plans against the layout its previous
// patch produced.
func (s *Service) zeroExtendFile(ctx context.Context, project, cleanPath string, traversed []string, targetSize int64) (*meta.FileMeta, error) {
	buf := make([]byte, zeroFillChunkSize)
	for {
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return nil, err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return nil, s.backend.FileNotFound(cleanPath)
		}
		if err := CheckTraversal(ctx, repo, traversed); err != nil {
			return nil, err
		}
		if err := CheckWriteAccess(ctx, repo, cleanPath); err != nil {
			return nil, err
		}
		if file.Size >= targetSize {
			return file, nil
		}
		n := targetSize - file.Size
		if n > int64(len(buf)) {
			n = int64(len(buf))
		}
		if _, err := s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, buf[:n]); err != nil {
			return nil, err
		}
	}
}

func (s *Service) AppendFileContext(ctx context.Context, project, filePath string, data []byte) (result *meta.FileMeta, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "append start", "path", filePath, "bytes", len(data))
	defer func() { s.logFinish(project, "append", started, err, "path", filePath, "bytes", len(data)) }()
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	// append has open() semantics: a final symlink is followed.
	if err := ValidateAccessPathShape(filePath); err != nil {
		return nil, err
	}
	cleanPath, traversed, err := ResolveAccessPath(repo, filePath, true)
	if err != nil {
		return nil, err
	}
	if err := CheckTraversal(ctx, repo, traversed); err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckWriteAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
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
	// pwrite has open() semantics: a final symlink is followed.
	if err := ValidateAccessPathShape(filePath); err != nil {
		return nil, err
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath, traversed, err := ResolveAccessPath(repo, filePath, true)
	if err != nil {
		return nil, err
	}
	if err := CheckTraversal(ctx, repo, traversed); err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckWriteAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
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
		// The hole is streamed as fixed-size zero chunks (never
		// materialized in RAM), then the real data lands at offset.
		if _, err := s.zeroExtendFile(ctx, project, cleanPath, traversed, offset); err != nil {
			return nil, err
		}
		repo, _, err = s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return nil, err
		}
		file = repo.FindFile(cleanPath)
		if file == nil {
			return nil, s.backend.FileNotFound(cleanPath)
		}
		result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, offset, 0, data)
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
	// pread has open() semantics: a final symlink is followed.
	if err := ValidateAccessPathShape(filePath); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath, traversed, err := ResolveAccessPath(repo, filePath, true)
	if err != nil {
		return nil, err
	}
	if err := CheckTraversal(ctx, repo, traversed); err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if offset >= file.Size {
		// POSIX read(2) at or past EOF returns 0 bytes, not an error;
		// surfacing io.EOF here mapped to EIO at the FUSE boundary.
		result = []byte{}
		return result, nil
	}
	if length == 0 {
		result = []byte{}
		return result, nil
	}
	// Clamp before adding: a REST-supplied length near MaxInt64 would
	// overflow `offset + length` negative and panic the make below.
	if length > file.Size-offset {
		length = file.Size - offset
	}
	end := offset + length
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
	// lstat semantics: the final symlink is NOT followed (followFinal
	// false); intermediate symlink components are resolved physically.
	// Callers wanting stat() semantics resolve first (ResolvePath) and then
	// look up.
	if targetPath != "" {
		if err := ValidateAccessPathShape(targetPath); err != nil {
			return nil, err
		}
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath, traversed, err := ResolveAccessPath(repo, targetPath, false)
	if err != nil {
		return nil, err
	}
	if err := CheckTraversal(ctx, repo, traversed); err != nil {
		return nil, err
	}
	if cleanPath == "" {
		result = &EntryInfo{Path: "", IsDir: true, Inode: repo.Root.Inode, Mode: repo.Root.Mode, UID: repo.Root.UID, GID: repo.Root.GID, NLink: uint32(repo.DirNLink("")), CreatedAt: repo.Root.CreatedAt, ModifiedAt: repo.Root.ModifiedAt, AccessedAt: repo.Root.AccessedAt, ChangedAt: repo.Root.ChangedAt}
		return result, nil
	}
	// lstat semantics: a terminal symlink reports itself; intermediate
	// components were resolved by ResolveAccessPath above.
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
	// opendir follows a final symlink to a directory, so followFinal is
	// true.
	if dirPath != "" {
		if err := ValidateAccessPathShape(dirPath); err != nil {
			return nil, err
		}
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath, traversed, err := ResolveAccessPath(repo, dirPath, true)
	if err != nil {
		return nil, err
	}
	if err := CheckTraversal(ctx, repo, traversed); err != nil {
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
