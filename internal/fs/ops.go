// Package fs implements the key-addressed file verbs (create/mkdir/rmdir/
// rename/copy/truncate/read/write/stat/readdir) and the DAC that guards
// them. Layering boundary: fs owns concrete storage keys and permission
// checks (path resolution, Check* DAC, atime policy); the posix package
// owns metadata verbs (symlink/link/chmod/chown/chtimes/xattr) on top of
// this contract. Both facades share the single Backend interface defined
// here; the FUSE Hub stitches the two.
package fs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"sync"
	"sync/atomic"
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

	// states is the per-project derived state (logger, mutation counter,
	// StatFS cache). A sync.Map keeps the per-op lookup lock-free: the hub
	// owns one long-lived Service, so every verb used to serialize on the
	// old stateMu twice per call. The atomic count bounds churn-driven
	// growth (see maxProjectStates); entries are otherwise reclaimed via
	// ForgetProject or with the hub.
	states     sync.Map // map[string]*projectState
	stateCount atomic.Int64
}

// maxProjectStates backstops the states map against project-churn growth:
// each entry is only a logger plus a small StatFS aggregate, but an
// unbounded map would pin one logger per project ever addressed. Inserts
// past the cap evict one arbitrary entry; the next op rebuilds it.
const maxProjectStates = 256

const PendingReleaseTag = "pending"

func NewService(backend Backend) *Service {
	return &Service{backend: backend}
}

// projectState carries the per-project derived state reused across FS
// operations: the lazily-built logger, a mutation counter that invalidates
// the StatFS cache, and the cached StatFS aggregate itself.
type projectState struct {
	project   string
	mu        sync.Mutex
	logger    *slog.Logger
	mutations atomic.Uint64
	statfs    *FSStats
	statfsSha string
	statfsGen uint64
	statfsAt  time.Time
}

// statfsCacheTTL bounds how long cached StatFS aggregates may serve even
// when neither the commit SHA nor the local mutation counter moved: verbs
// that mutate metadata outside this Service (xattr/symlink/chmod on the
// hub) bump neither key, so staleness is capped by time as well.
const statfsCacheTTL = 40 * storcfg.TickUnit

func (s *Service) state(project string) *projectState {
	if v, ok := s.states.Load(project); ok {
		return v.(*projectState)
	}
	st := &projectState{project: project}
	actual, loaded := s.states.LoadOrStore(project, st)
	if !loaded {
		if s.stateCount.Add(1) > maxProjectStates {
			s.evictOneState()
		}
		return st
	}
	return actual.(*projectState)
}

// evictOneState deletes an arbitrary entry to hold the cap. Best-effort
// backstop only: concurrent inserts may each evict once, so the count is
// approximate under contention.
func (s *Service) evictOneState() {
	s.states.Range(func(key, _ any) bool {
		s.states.Delete(key)
		s.stateCount.Add(-1)
		return false
	})
}

// ForgetProject drops the cached per-project state (logger, mutation
// counter, StatFS aggregate). Call it when a project is evicted or
// deleted so long-dead projects stop pinning loggers; the next op for the
// project rebuilds its state lazily.
func (s *Service) ForgetProject(project string) {
	if _, loaded := s.states.LoadAndDelete(project); loaded {
		s.stateCount.Add(-1)
	}
}

// log returns the per-project fs logger, built once per project.
func (p *projectState) log(backendLogger *slog.Logger) *slog.Logger {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.logger == nil {
		p.logger = logging.WithComponent(backendLogger, "fs").With("project", p.project)
	}
	return p.logger
}

// bump records that a metadata-mutating operation succeeded, so any cached
// StatFS aggregate for the project is stale from now on.
func (p *projectState) bump() {
	p.mutations.Add(1)
}

// withOp wraps one service verb with start/finish debug logging and StatFS
// invalidation. A mutating verb passes mutating=true so a successful call
// bumps the mutation counter; read-only verbs pass false. It replaces the
// per-verb Debug+logFinish+bumpMutations boilerplate (13 copies) with one
// funnel.
func (s *Service) withOp(project, op string, mutating bool, args []any, fn func() error) (err error) {
	state := s.state(project)
	started := time.Now().UTC()
	logger := state.log(s.backend.Logger())
	logging.Debug(logger, op+" start", args...)
	defer func() {
		if mutating && err == nil {
			state.bump()
		}
		s.logFinishState(state, op, started, err, args...)
	}()
	return fn()
}

func (s *Service) logFinishState(state *projectState, op string, started time.Time, err error, args ...any) {
	args = append(args, "elapsed", time.Since(started))
	logger := state.log(s.backend.Logger())
	if err != nil {
		args = append(args, "err", err)
		logging.Error(logger, op+" failed", args...)
		return
	}
	// Debug, not Info: per-op completion lines are a steady-state fire
	// hose on a mount (every stat/read/write), and the default level no
	// longer wants them.
	logging.Debug(logger, op+" complete", args...)
}

// validateCreate runs the creation checks shared by CreateFileContext's
// pre-transaction fast path and its in-transaction enforcement, so the two
// cannot drift: traversal DAC, parent write permission, parent existence,
// and destination absence.
func validateCreate(ctx context.Context, repo *meta.RepoMetadata, cleanPath string, traversed []string) error {
	if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
		return err
	}
	if err := CheckParentWriteResolved(ctx, repo, cleanPath, traversed); err != nil {
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
	return nil
}

func (s *Service) CreateFileContext(ctx context.Context, project, filePath string) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "create", true, []any{"path", filePath}, func() error {
		// Create addresses the new node itself (O_CREAT|O_EXCL never follows a
		// final symlink), so resolution is lstat-style; intermediate symlink
		// components are still resolved physically.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		if err := s.backend.ValidateProjectName(project); err != nil {
			return err
		}
		if err := s.backend.EnsureRepoContext(ctx, project); err != nil {
			return err
		}
		repoMeta, _, err := s.backend.LoadRepoMetadataContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := LstatResolveTracked(repoMeta, filePath)
		if err != nil {
			return err
		}
		if cleanPath == "" {
			return errors.New("file path is required")
		}
		if err := validateCreate(ctx, repoMeta, cleanPath, traversed); err != nil {
			return err
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
			if err := validateCreate(ctx, repo, cleanPath, traversed); err != nil {
				return err
			}
			fileMeta.Mode, fileMeta.UID, fileMeta.GID = ApplyParentInheritance(repo, cleanPath, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
			fileMeta.Inode = repo.AllocateInode()
			repo.UpsertFile(cleanPath, fileMeta, now)
			TouchParentDirectory(repo, cleanPath, now)
			return nil
		}, fmt.Sprintf("storhub: create %s", cleanPath)); err != nil {
			return err
		}
		result = &fileMeta
		return nil
	})
	return result, err
}

func (s *Service) MkdirContext(ctx context.Context, project, dirPath string) (err error) {
	return s.withOp(project, "mkdir", true, []any{"path", dirPath}, func() error {
		// mkdir never creates through a final symlink (EEXIST on the link
		// itself), so resolution is lstat-style; intermediate components
		// resolve physically.
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
		cleanPath, traversed, err := LstatResolveTracked(repoMeta, dirPath)
		if err != nil {
			return err
		}
		if cleanPath == "" {
			// POSIX: mkdir on an existing directory fails; the root always
			// exists.
			return AlreadyExists("/")
		}
		_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
			if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
				return err
			}
			if err := CheckParentWriteResolved(ctx, repo, cleanPath, traversed); err != nil {
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
				repo.WriteDirDirect(cleanPath, *dir)
			}
			TouchParentDirectory(repo, cleanPath, s.backend.Now())
			return nil
		}, fmt.Sprintf("storhub: mkdir %s", cleanPath))
		return err
	})
}

func (s *Service) RmdirContext(ctx context.Context, project, dirPath string) (err error) {
	return s.withOp(project, "rmdir", true, []any{"path", dirPath}, func() error {
		// rmdir removes the final component itself; a symlink there must not be
		// followed (POSIX rmdir on a symlink is ENOTDIR), so resolution is
		// lstat-style.
		if err := ValidateAccessPathShape(dirPath); err != nil {
			return err
		}
		repoMeta, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := LstatResolveTracked(repoMeta, dirPath)
		if err != nil {
			return err
		}
		if cleanPath == "" {
			// POSIX: rmdir("/") fails with EBUSY, not a generic error that
			// errno mapping would surface as EIO.
			return syscall.EBUSY
		}
		_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
			if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
				return err
			}
			if err := CheckParentWriteResolved(ctx, repo, cleanPath, traversed); err != nil {
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
	})
}

func (s *Service) RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...MutateOption) (err error) {
	return s.withOp(project, "rename", true, []any{"old_path", oldPath, "new_path", newPath}, func() error {
		mutate := ApplyMutateOptions(opts)
		// rename(2) renames the final component itself: a symlink endpoint is
		// moved, never followed, so resolution is lstat-style on both
		// endpoints (intermediate components still resolve physically).
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
		oldClean, oldTraversed, err := LstatResolveTracked(preRepo, oldPath)
		if err != nil {
			return err
		}
		newClean, newTraversed, err := LstatResolveTracked(preRepo, newPath)
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
			if err := CheckWalkResolved(ctx, repo, oldTraversed); err != nil {
				return err
			}
			if err := CheckWalkResolved(ctx, repo, newTraversed); err != nil {
				return err
			}
			srcFile := repo.FindFile(oldClean)
			srcIsDir := repo.HasDirectory(oldClean)
			if srcFile == nil && !srcIsDir {
				return NotFound(oldClean)
			}
			if err := CheckParentWriteResolved(ctx, repo, oldClean, oldTraversed); err != nil {
				return err
			}
			if err := CheckParentWriteResolved(ctx, repo, newClean, newTraversed); err != nil {
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
				return renameFileInTxn(ctx, repo, oldClean, newClean, dstFile, dstDir, now)
			}
			return renameDirInTxn(ctx, repo, oldClean, newClean, dstFile, dstDir, now)
		}, fmt.Sprintf("storhub: rename %s to %s", oldClean, newClean))
		return err
	})
}

// renameFileInTxn moves one file key inside the update transaction.
// Replacing an existing file is atomic here; renaming onto a directory is
// EISDIR per POSIX.
func renameFileInTxn(ctx context.Context, repo *meta.RepoMetadata, oldClean, newClean string, dstFile *meta.FileMeta, dstDir *meta.DirMeta, now int64) error {
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
	srcFile := repo.FindFile(oldClean)
	if srcFile == nil {
		return NotFound(oldClean)
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

// renameDirInTxn moves one directory subtree inside the update transaction.
// Renaming onto a file is ENOTDIR; onto a non-empty directory is ENOTEMPTY.
func renameDirInTxn(ctx context.Context, repo *meta.RepoMetadata, oldClean, newClean string, dstFile *meta.FileMeta, dstDir *meta.DirMeta, now int64) error {
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
	remapTree(repo, oldClean, newClean, now)
	TouchParentDirectory(repo, oldClean, now)
	TouchParentDirectory(repo, newClean, now)
	return nil
}

// collectSubtree lists the subtree rooted at base (base first) by walking
// down from DirectoryChildren: O(subtree), not O(tree).
func collectSubtree(repo *meta.RepoMetadata, base string) (dirs, files []string) {
	dirs = append(dirs, base)
	queue := []string{base}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		childDirs, childFiles := repo.DirectoryChildren(cur)
		for _, d := range childDirs {
			dirs = append(dirs, d)
			queue = append(queue, d)
		}
		files = append(files, childFiles...)
	}
	return dirs, files
}

// remapTree moves the whole subtree rooted at oldBase to newBase through
// the tracked mutators. The subtree is collected before any mutation and
// each entry is re-written via Remove+Write, so the engine's derived index
// and size cache stay incremental (a wholesale map swap is not possible:
// the maps are unexported).
func remapTree(repo *meta.RepoMetadata, oldBase, newBase string, now int64) {
	dirs, files := collectSubtree(repo, oldBase)
	type dirRemap struct {
		from, to string
		dir      meta.DirMeta
	}
	dirRemaps := make([]dirRemap, 0, len(dirs))
	for _, dirPath := range dirs {
		if dir := repo.GetDirectory(dirPath); dir != nil {
			cloned := *dir
			cloned.ModifiedAt = now
			cloned.ChangedAt = now
			dirRemaps = append(dirRemaps, dirRemap{from: dirPath, to: RemapPath(oldBase, newBase, dirPath), dir: cloned})
		}
	}
	for _, r := range dirRemaps {
		repo.RemoveDirectory(r.from)
		repo.WriteDirDirect(r.to, r.dir)
	}
	type fileRemap struct {
		from, to string
		file     meta.FileMeta
	}
	fileRemaps := make([]fileRemap, 0, len(files))
	for _, filePath := range files {
		if file := repo.FindFile(filePath); file != nil {
			cloned := file.Clone()
			cloned.ChangedAt = now
			fileRemaps = append(fileRemaps, fileRemap{from: filePath, to: RemapPath(oldBase, newBase, filePath), file: cloned})
		}
	}
	for _, r := range fileRemaps {
		repo.RemoveFile(r.from)
		repo.WriteFileDirect(r.to, r.file)
	}
}

func (s *Service) CopyContext(ctx context.Context, project, srcPath, dstPath string) (err error) {
	return s.withOp(project, "copy", true, []any{"src", srcPath, "dst", dstPath}, func() error {
		// cp follows symlinks at both endpoints: the source is read through
		// (stat semantics) and the destination is written through (open
		// semantics), so resolution is stat-style on both.
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
		srcClean, srcTraversed, err := StatResolveTracked(preRepo, srcPath)
		if err != nil {
			return err
		}
		dstClean, dstTraversed, err := StatResolveTracked(preRepo, dstPath)
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
			if err := CheckWalkResolved(ctx, repo, srcTraversed); err != nil {
				return err
			}
			if err := CheckWalkResolved(ctx, repo, dstTraversed); err != nil {
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
				if err := CheckReadAccessResolved(ctx, repo, srcClean, srcTraversed); err != nil {
					return err
				}
			} else {
				if err := CheckListDirAccessResolved(ctx, repo, srcClean, srcTraversed); err != nil {
					return err
				}
			}
			if err := CheckParentWriteResolved(ctx, repo, dstClean, dstTraversed); err != nil {
				return err
			}
			if parent := ParentPath(dstClean); parent != "" && !repo.HasDirectory(parent) {
				return NotFound(parent)
			}
			dstFile := repo.FindFile(dstClean)
			dstDir := repo.GetDirectory(dstClean)
			now := s.backend.Now()
			// Caller ownership for the new nodes: provisioned once here so
			// both copy helpers stamp the same identity (OwnerIDsForCreate
			// falls back to the process owner only when no identity is
			// attached, mirroring CreateFileContext).
			defaultUID, defaultGID := s.backend.DefaultOwnerIDs()
			createUID, createGID := OwnerIDsForCreate(ctx, defaultUID, defaultGID)
			if srcFile != nil {
				return copyFileInTxn(ctx, repo, srcClean, dstClean, dstFile, dstDir, now, createUID, createGID)
			}
			return copyDirInTxn(ctx, repo, srcClean, dstClean, dstFile, dstDir, now, createUID, createGID)
		}, fmt.Sprintf("storhub: copy %s to %s", srcClean, dstClean))
		return err
	})
}

// copyFileInTxn duplicates one file key inside the update transaction with
// a fresh inode. Copying onto a directory is EISDIR; onto a file replaces
// it subject to the sticky check.
//
// Ownership and privilege model: a copy is a creation, not an identity
// transfer. The destination carries the caller's owner IDs (plus
// setgid-parent inheritance) and the source permission bits minus
// setuid/setgid for unprivileged callers (decision 1A), exactly like a
// CloneRange new destination and data writes. Admin (the CAP_FSETID
// equivalent) keeps the source owner and bits. The store bypasses
// UpsertFile for WriteFileDirect (the destination parent is verified
// above): UpsertFile would route the fresh node through creation
// defaults, which would widen an explicit 000 mode back to 0644.
func copyFileInTxn(ctx context.Context, repo *meta.RepoMetadata, srcClean, dstClean string, dstFile *meta.FileMeta, dstDir *meta.DirMeta, now int64, createUID, createGID uint32) error {
	if dstDir != nil {
		return syscall.EISDIR
	}
	if dstFile != nil {
		if err := CheckStickyDelete(ctx, repo, ParentPath(dstClean), dstClean); err != nil {
			return err
		}
		repo.RemoveFile(dstClean)
	}
	srcFile := repo.FindFile(srcClean)
	if srcFile == nil {
		return NotFound(srcClean)
	}
	cloned := srcFile.Clone()
	cloned.Inode = repo.AllocateInode()
	cloned.UID, cloned.GID = createUID, createGID
	cloned.Mode = SanitizeWrittenFileModeForContext(ctx, cloned.Mode)
	cloned.Mode, cloned.UID, cloned.GID = ApplyParentInheritance(repo, dstClean, false, cloned.Mode, cloned.UID, cloned.GID)
	cloned.ChangedAt = now
	repo.WriteFileDirect(dstClean, cloned)
	TouchParentDirectory(repo, dstClean, now)
	return nil
}

// copyDirInTxn duplicates one directory subtree inside the update
// transaction, minting a fresh inode per entry. The subtree walk is
// O(subtree): entries are collected from DirectoryChildren, collision
// checked against the live tree, and written as they are visited (RemapPath
// is injective, so visited targets can never collide with each other).
//
// Every new node follows the copyFileInTxn ownership and privilege model
// (caller owner, setuid/setgid cleared for unprivileged callers, setgid
// re-inherited from the new parent): without it a copied tree would mint
// foreign-owned setuid nodes for any caller with read access.
func copyDirInTxn(ctx context.Context, repo *meta.RepoMetadata, srcClean, dstClean string, dstFile *meta.FileMeta, dstDir *meta.DirMeta, now int64, createUID, createGID uint32) error {
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
	newDir.UID, newDir.GID = createUID, createGID
	newDir.Mode = SanitizeWrittenFileModeForContext(ctx, newDir.Mode)
	newDir.Mode, newDir.UID, newDir.GID = ApplyParentInheritance(repo, dstClean, true, newDir.Mode, newDir.UID, newDir.GID)
	newDir.ModifiedAt = now
	newDir.ChangedAt = now
	newDir.AccessedAt = now
	newDir.CreatedAt = now
	repo.WriteDirDirect(dstClean, newDir)
	dirs, files := collectSubtree(repo, srcClean)
	for _, dirPath := range dirs {
		if dirPath == srcClean {
			continue
		}
		newPath := RemapPath(srcClean, dstClean, dirPath)
		if repo.HasDirectory(newPath) || repo.FindFile(newPath) != nil {
			return AlreadyExists(newPath)
		}
		sub := repo.GetDirectory(dirPath)
		if sub == nil {
			return NotFound(dirPath)
		}
		cloned := sub.Clone()
		cloned.Inode = repo.AllocateInode()
		cloned.UID, cloned.GID = createUID, createGID
		cloned.Mode = SanitizeWrittenFileModeForContext(ctx, cloned.Mode)
		cloned.Mode, cloned.UID, cloned.GID = ApplyParentInheritance(repo, newPath, true, cloned.Mode, cloned.UID, cloned.GID)
		cloned.ModifiedAt = now
		cloned.ChangedAt = now
		cloned.AccessedAt = now
		repo.WriteDirDirect(newPath, cloned)
	}
	for _, filePath := range files {
		newPath := RemapPath(srcClean, dstClean, filePath)
		if repo.HasDirectory(newPath) || repo.FindFile(newPath) != nil {
			return AlreadyExists(newPath)
		}
		sub := repo.FindFile(filePath)
		if sub == nil {
			return NotFound(filePath)
		}
		cloned := sub.Clone()
		cloned.Inode = repo.AllocateInode()
		cloned.UID, cloned.GID = createUID, createGID
		cloned.Mode = SanitizeWrittenFileModeForContext(ctx, cloned.Mode)
		cloned.Mode, cloned.UID, cloned.GID = ApplyParentInheritance(repo, newPath, false, cloned.Mode, cloned.UID, cloned.GID)
		cloned.ChangedAt = now
		// WriteFileDirect, not UpsertFile: the fresh identity is fully
		// provisioned above, and UpsertFile creation defaults would
		// widen an explicit 000 mode (see copyFileInTxn).
		repo.WriteFileDirect(newPath, cloned)
	}
	TouchParentDirectory(repo, dstClean, now)
	return nil
}

func (s *Service) TruncateFileContext(ctx context.Context, project, filePath string, size int64) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "truncate", true, []any{"path", filePath, "size", size}, func() error {
		// truncate(2) has open() semantics: a final symlink is followed to its
		// target.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		if size < 0 {
			return errors.New("truncate size must be non-negative")
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, filePath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if size == file.Size {
			// POSIX: even a no-op truncate updates mtime/ctime and, for
			// non-admin callers, clears setuid+setgid (decision 1A).
			now := s.backend.Now()
			sanitizedMode := SanitizeWrittenFileModeForContext(ctx, file.Mode)
			if _, err := s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
				if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
					return err
				}
				if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
					return err
				}
				current := repo.FindFile(cleanPath)
				if current == nil {
					return s.backend.FileNotFound(cleanPath)
				}
				clone := current.Clone()
				clone.Mode = SanitizeWrittenFileModeForContext(ctx, clone.Mode)
				clone.ModifiedAt = now
				clone.ChangedAt = now
				repo.ReplaceFile(cleanPath, clone)
				return nil
			}, fmt.Sprintf("storhub: truncate touch %s", cleanPath)); err != nil {
				return err
			}
			clone := file.Clone()
			clone.Mode = sanitizedMode
			clone.ModifiedAt = now
			clone.ChangedAt = now
			result = &clone
			return nil
		}
		if size < file.Size {
			result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, size, file.Size-size, nil)
			return err
		}
		result, err = s.zeroExtendFile(ctx, project, cleanPath, traversed, size)
		return err
	})
	return result, err
}

// maxZeroExtendBytes caps how much a single zeroExtendFile call may grow a
// file. The extension uploads real zero bytes in ONE batched patch (a
// single metadata transaction and release resolution) instead of looping
// per chunk: without a cap, `truncate -s 1T` would issue ~1M uploads with
// no sparse representation. Requests beyond the cap fail with EFBIG rather
// than burning unbounded backend work. True hole-aware chunks (zero ranges
// without assets) need a storage-layer format change and are NOT done here.
const maxZeroExtendBytes = 16 << 20

// zeroExtendFile grows a file to targetSize with a single zero-filled
// range patch through the backend's patch verb.
func (s *Service) zeroExtendFile(ctx context.Context, project, cleanPath string, traversed []string, targetSize int64) (*meta.FileMeta, error) {
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
		return nil, err
	}
	if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
		return nil, err
	}
	if file.Size >= targetSize {
		return file, nil
	}
	if targetSize-file.Size > maxZeroExtendBytes {
		return nil, syscall.EFBIG
	}
	zeros := make([]byte, targetSize-file.Size)
	return s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, zeros)
}

func (s *Service) AppendFileContext(ctx context.Context, project, filePath string, data []byte) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "append", true, []any{"path", filePath, "bytes", len(data)}, func() error {
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		// append has open() semantics: a final symlink is followed.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, filePath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, data)
		return err
	})
	return result, err
}

func (s *Service) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "write-at", true, []any{"path", filePath, "offset", offset, "bytes", len(data)}, func() error {
		// pwrite has open() semantics: a final symlink is followed.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, filePath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if offset < 0 {
			return errors.New("write offset must be non-negative")
		}
		if len(data) == 0 {
			clone := file.Clone()
			result = &clone
			return nil
		}
		if offset > file.Size {
			// The hole is zero-filled in one capped patch, then the real
			// data lands at offset.
			if _, err := s.zeroExtendFile(ctx, project, cleanPath, traversed, offset); err != nil {
				return err
			}
			repo, _, err = s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
			if err != nil {
				return err
			}
			file = repo.FindFile(cleanPath)
			if file == nil {
				return s.backend.FileNotFound(cleanPath)
			}
			result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, offset, 0, data)
			return err
		}
		deleteSize := int64(len(data))
		if max := file.Size - offset; deleteSize > max {
			deleteSize = max
		}
		result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, offset, deleteSize, data)
		return err
	})
	return result, err
}

func (s *Service) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) (result []byte, err error) {
	err = s.withOp(project, "read-at", false, []any{"path", filePath, "offset", offset, "length", length}, func() error {
		// pread has open() semantics: a final symlink is followed.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		if offset < 0 || length < 0 {
			return errors.New("read offset and length must be non-negative")
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, filePath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if err := CheckReadAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if offset >= file.Size {
			// POSIX read(2) at or past EOF returns 0 bytes, not an error;
			// surfacing io.EOF here mapped to EIO at the FUSE boundary.
			result = []byte{}
			return nil
		}
		if length == 0 {
			result = []byte{}
			return nil
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
				return err
			}
		}
		TouchFileAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now())
		return nil
	})
	return result, err
}

func (s *Service) StatPathContext(ctx context.Context, project, targetPath string) (result *EntryInfo, err error) {
	err = s.withOp(project, "stat-path", false, []any{"path", targetPath}, func() error {
		// lstat semantics: the final symlink is NOT followed; intermediate
		// symlink components are resolved physically. Callers wanting
		// stat() semantics resolve first (StatResolve) and then look up.
		if targetPath != "" {
			if err := ValidateAccessPathShape(targetPath); err != nil {
				return err
			}
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := LstatResolveTracked(repo, targetPath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		if cleanPath == "" {
			result = &EntryInfo{Path: "", IsDir: true, Inode: repo.Root.Inode, Mode: repo.Root.Mode, UID: repo.Root.UID, GID: repo.Root.GID, NLink: uint32(repo.DirNLink("")), CreatedAt: repo.Root.CreatedAt, ModifiedAt: repo.Root.ModifiedAt, AccessedAt: repo.Root.AccessedAt, ChangedAt: repo.Root.ChangedAt}
			return nil
		}
		// lstat semantics: a terminal symlink reports itself; intermediate
		// components were resolved above.
		if file := repo.FindFile(cleanPath); file != nil {
			result = EntryFromFile(file, cleanPath, repo.FileNLink(cleanPath))
			return nil
		}
		if dir := repo.GetDirectory(cleanPath); dir != nil {
			result = EntryFromDirectory(dir, cleanPath, repo.DirNLink(cleanPath))
			return nil
		}
		return NotFound(cleanPath)
	})
	return result, err
}

func (s *Service) StatFSContext(ctx context.Context, project string) (result *FSStats, err error) {
	err = s.withOp(project, "statfs", false, nil, func() error {
		repo, sha, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		// Cache the O(files+chunks) aggregation per project, invalidated by
		// (commit SHA, local mutation counter) plus a short TTL backstop.
		// Rescanning the whole tree on every df burned permanently under
		// monitoring loops.
		state := s.state(project)
		now := time.Now()
		if cached, ok := state.cachedStatFS(sha, now); ok {
			result = cached
			return nil
		}
		// Aggregate live instead of trusting the repo's own cached counters:
		// those only refresh during metadata commits, so a freshly mutated
		// tree would otherwise report stale numbers.
		files := 0
		var totalBytes int64
		for _, file := range repo.Files() {
			if file.Symlink == "" {
				files++
				totalBytes += file.Size
			}
		}
		stats := &FSStats{Files: files, Directories: len(repo.Dirs()), Inodes: CountUniqueInodes(repo), Bytes: totalBytes, Releases: len(repo.Releases())}
		assetCounts := make(map[string]int, len(repo.Chunks()))
		for _, chunk := range repo.Chunks() {
			if chunk.Release != "" {
				assetCounts[chunk.Release]++
			}
		}
		for _, count := range assetCounts {
			stats.Assets += count
		}
		state.storeStatFS(sha, stats)
		result = stats
		return nil
	})
	return result, err
}

// cachedStatFS returns a fresh copy of the cached aggregate when it still
// matches the commit SHA, the local mutation counter, and the TTL.
func (p *projectState) cachedStatFS(sha string, now time.Time) (*FSStats, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statfs == nil || p.statfsSha != sha || p.statfsGen != p.mutations.Load() || now.Sub(p.statfsAt) >= statfsCacheTTL {
		return nil, false
	}
	copied := *p.statfs
	return &copied, true
}

// storeStatFS records the aggregate together with the invalidation keys
// observed at computation time. A mutation that landed during the scan
// bumps the counter afterwards, so the next read recomputes (the stored
// numbers are then merely redundant, never stale).
func (p *projectState) storeStatFS(sha string, stats *FSStats) {
	p.mu.Lock()
	defer p.mu.Unlock()
	copied := *stats
	p.statfs = &copied
	p.statfsSha = sha
	p.statfsGen = p.mutations.Load()
	p.statfsAt = time.Now()
}

func (s *Service) ReadDirContext(ctx context.Context, project, dirPath string) (result []DirEntry, err error) {
	err = s.withOp(project, "readdir", false, []any{"path", dirPath}, func() error {
		// opendir follows a final symlink to a directory, so resolution is
		// stat-style.
		if dirPath != "" {
			if err := ValidateAccessPathShape(dirPath); err != nil {
				return err
			}
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, dirPath)
		if err != nil {
			return err
		}
		// CheckListDirAccessResolved consumes the walk's traversed chain
		// itself: no separate walk pass (and no re-resolution) here.
		if err := CheckListDirAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if cleanPath != "" && repo.FindFile(cleanPath) != nil {
			return NotDirectory(cleanPath)
		}
		if cleanPath != "" && !repo.HasDirectory(cleanPath) {
			return NotFound(cleanPath)
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
		return nil
	})
	return result, err
}

func RequireParentDirectory(repo *meta.RepoMetadata, filePath string) error {
	if parent := ParentPath(filePath); parent != "" && !repo.HasDirectory(parent) {
		return fmt.Errorf("%w: parent directory does not exist: %s", ErrNotFound, parent)
	}
	return nil
}

// EntryFromFile builds the stat view of one file node. It is the single
// constructor behind EntryInfoFromFile and the FUSE entry fills: every
// renderer calls this instead of re-spelling the field mapping.
func EntryFromFile(file *meta.FileMeta, path string, nlink int) *EntryInfo {
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

// EntryInfoFromFile is kept for existing callers; new code calls EntryFromFile.
func EntryInfoFromFile(file *meta.FileMeta, path string, nlink int) *EntryInfo {
	return EntryFromFile(file, path, nlink)
}

// EntryFromDirectory builds the stat view of one directory node: the
// single constructor behind EntryInfoFromDirectory.
func EntryFromDirectory(dir *meta.DirMeta, path string, nlink int) *EntryInfo {
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

// EntryInfoFromDirectory is kept for existing callers; new code calls EntryFromDirectory.
func EntryInfoFromDirectory(dir *meta.DirMeta, path string, nlink int) *EntryInfo {
	return EntryFromDirectory(dir, path, nlink)
}

// EntryFromDirEntry lifts a listing row into the full attribute view the
// kernel's entry cache wants. The listing already carries mode, owner,
// link count and timestamps, so no re-stat is needed.
func EntryFromDirEntry(e DirEntry, childPath string) *EntryInfo {
	return &EntryInfo{
		Path:       childPath,
		Kind:       e.Kind,
		IsDir:      e.IsDir,
		IsSymlink:  e.IsSymlink,
		Size:       e.Size,
		Inode:      e.Inode,
		Mode:       e.Mode,
		UID:        e.UID,
		GID:        e.GID,
		NLink:      e.NLink,
		CreatedAt:  e.CreatedAt,
		ModifiedAt: e.ModifiedAt,
		AccessedAt: e.AccessedAt,
		ChangedAt:  e.ChangedAt,
	}
}

func DirEntryFromDirectory(dir meta.DirMeta, dirPath string, nlink int) DirEntry {
	return DirEntry{Name: path.Base(dirPath), Path: dirPath, IsDir: true, Inode: dir.Inode, Mode: dir.Mode, NLink: uint32(nlink), UID: dir.UID, GID: dir.GID, CreatedAt: dir.CreatedAt, ModifiedAt: dir.ModifiedAt, AccessedAt: dir.AccessedAt, ChangedAt: dir.ChangedAt}
}

func DirEntryFromFile(file meta.FileMeta, filePath string, nlink int) DirEntry {
	entry := DirEntry{Name: path.Base(filePath), Path: filePath, IsSymlink: file.Symlink != "", Size: file.Size, Inode: file.Inode, Mode: file.Mode, NLink: uint32(nlink), UID: file.UID, GID: file.GID, CreatedAt: file.UploadedAt, ModifiedAt: file.ModifiedAt, AccessedAt: file.AccessedAt, ChangedAt: file.ChangedAt}
	if file.Symlink != "" {
		entry.Kind = meta.NodeKindSymlink
	} else {
		entry.Kind = meta.NodeKindFile
	}
	return entry
}

func CountUniqueInodes(repo *meta.RepoMetadata) int {
	seen := map[uint64]struct{}{repo.Root.Inode: {}}
	for _, dir := range repo.Dirs() {
		seen[dir.Inode] = struct{}{}
	}
	// Files() is the live map. AllFiles sorts a throwaway copy of every
	// name just to count, turning each StatFS miss into O(F log F).
	for _, file := range repo.Files() {
		seen[file.Inode] = struct{}{}
	}
	return len(seen)
}
