package fs

import (
	"context"
	"fmt"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

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

// CreateFileContext creates an empty file at targetPath.
func (s *Service) CreateFileContext(ctx context.Context, project, targetPath string) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "create", true, []any{"path", targetPath}, func() error {
		// Create addresses the new node itself (O_CREAT|O_EXCL never follows a
		// final symlink), so resolution is lstat-style; intermediate symlink
		// components are still resolved physically.
		if err := ValidateAccessPathShape(targetPath); err != nil {
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
		cleanPath, traversed, err := LstatResolveTracked(repoMeta, targetPath)
		if err != nil {
			return err
		}
		if cleanPath == "" {
			return RequiredPath("file path is required")
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
			// Re-resolve against the live transaction state: the
			// pre-transaction walk above is fast-fail only.
			liveClean, liveTraversed, err := LstatResolveTracked(repo, targetPath)
			if err != nil {
				return err
			}
			if liveClean == "" {
				return RequiredPath("file path is required")
			}
			if err := validateCreate(ctx, repo, liveClean, liveTraversed); err != nil {
				return err
			}
			fileMeta.Mode, fileMeta.UID, fileMeta.GID = ApplyParentInheritance(repo, liveClean, false, fileMeta.Mode, fileMeta.UID, fileMeta.GID)
			fileMeta.Inode = repo.AllocateInode()
			repo.UpsertFile(liveClean, fileMeta, now)
			TouchParentDirectory(repo, liveClean, now)
			return nil
		}, fmt.Sprintf("storhub: create %s", cleanPath)); err != nil {
			return err
		}
		result = &fileMeta
		return nil
	})
	return result, err
}

// MkdirContext creates the directory at targetPath.
func (s *Service) MkdirContext(ctx context.Context, project, targetPath string) (err error) {
	return s.withOp(project, "mkdir", true, []any{"path", targetPath}, func() error {
		// mkdir never creates through a final symlink (EEXIST on the link
		// itself), so resolution is lstat-style; intermediate components
		// resolve physically.
		if err := ValidateAccessPathShape(targetPath); err != nil {
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
		// Fast-fail only (resolution errors, root short-circuit): the
		// transaction below re-resolves against live state.
		cleanPath, _, err := LstatResolveTracked(repoMeta, targetPath)
		if err != nil {
			return err
		}
		if cleanPath == "" {
			// POSIX: mkdir on an existing directory fails; the root always
			// exists.
			return AlreadyExists("/")
		}
		_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
			// Re-resolve against the live transaction state: the
			// pre-transaction walk above is fast-fail only.
			liveClean, liveTraversed, err := LstatResolveTracked(repo, targetPath)
			if err != nil {
				return err
			}
			if liveClean == "" {
				return AlreadyExists("/")
			}
			if err := CheckWalkResolved(ctx, repo, liveTraversed); err != nil {
				return err
			}
			if err := CheckParentWriteResolved(ctx, repo, liveClean, liveTraversed); err != nil {
				return err
			}
			if repo.HasDirectory(liveClean) {
				return AlreadyExists(liveClean)
			}
			if repo.FindFile(liveClean) != nil {
				return AlreadyExists(liveClean)
			}
			if parent := ParentPath(liveClean); parent != "" && !repo.HasDirectory(parent) {
				return NotFound(parent)
			}
			// One timestamp per op: the directory, its inheritance, and its
			// parent all stamp the same instant.
			now := s.backend.Now()
			repo.EnsureDirectory(liveClean, now)
			if dir := repo.GetDirectory(liveClean); dir != nil {
				dir.UID, dir.GID = OwnerIDsForCreate(ctx, dir.UID, dir.GID)
				dir.Mode, dir.UID, dir.GID = ApplyParentInheritance(repo, liveClean, true, ApplyCreateMode(ctx, dir.Mode), dir.UID, dir.GID)
				dir.ChangedAt = now
				repo.WriteDirDirect(liveClean, *dir)
			}
			TouchParentDirectory(repo, liveClean, now)
			return nil
		}, fmt.Sprintf("storhub: mkdir %s", cleanPath))
		return err
	})
}

// RmdirContext removes the empty directory at targetPath.
func (s *Service) RmdirContext(ctx context.Context, project, targetPath string) (err error) {
	return s.withOp(project, "rmdir", true, []any{"path", targetPath}, func() error {
		// rmdir removes the final component itself; a symlink there must not be
		// followed (POSIX rmdir on a symlink is ENOTDIR), so resolution is
		// lstat-style.
		if err := ValidateAccessPathShape(targetPath); err != nil {
			return err
		}
		repoMeta, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		// Fast-fail only (resolution errors, root short-circuit): the
		// transaction below re-resolves against live state.
		cleanPath, _, err := LstatResolveTracked(repoMeta, targetPath)
		if err != nil {
			return err
		}
		if cleanPath == "" {
			// POSIX: rmdir("/") fails with EBUSY, not a generic error that
			// errno mapping would surface as EIO.
			return Busy("/")
		}
		_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
			// Re-resolve against the live transaction state: the
			// pre-transaction walk above is fast-fail only.
			liveClean, liveTraversed, err := LstatResolveTracked(repo, targetPath)
			if err != nil {
				return err
			}
			if liveClean == "" {
				return Busy("/")
			}
			if err := CheckWalkResolved(ctx, repo, liveTraversed); err != nil {
				return err
			}
			if err := CheckParentWriteResolved(ctx, repo, liveClean, liveTraversed); err != nil {
				return err
			}
			if err := CheckStickyDelete(ctx, repo, ParentPath(liveClean), liveClean); err != nil {
				return err
			}
			if repo.FindFile(liveClean) != nil {
				return NotDirectory(liveClean)
			}
			if !repo.HasDirectory(liveClean) {
				return NotFound(liveClean)
			}
			childDirs, childFiles := repo.DirectoryChildren(liveClean)
			if len(childDirs) > 0 || len(childFiles) > 0 {
				return NotEmpty(liveClean)
			}
			repo.RemoveDirectory(liveClean)
			TouchParentDirectory(repo, liveClean, s.backend.Now())
			return nil
		}, fmt.Sprintf("storhub: rmdir %s", cleanPath))
		return err
	})
}

// RenameContext moves oldPath to newPath honoring mutate options. The log
// keys are src/dst, matching copy and the canonical dst vocabulary.
func (s *Service) RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...MutateOption) (err error) {
	return s.withOp(project, "rename", true, []any{"src", oldPath, "dst", newPath}, func() error {
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
		// Fast-fail only: the transaction below re-resolves both endpoints
		// against live state before authorizing anything.
		oldClean, _, err := LstatResolveTracked(preRepo, oldPath)
		if err != nil {
			return err
		}
		newClean, _, err := LstatResolveTracked(preRepo, newPath)
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
			// Re-resolve both endpoints against the live transaction state:
			// the pre-transaction walk above is fast-fail only. Authorizing
			// the stale chains would bless a walk a concurrent
			// rename/replace of a symlink component already redirected.
			oldLive, oldChain, err := LstatResolveTracked(repo, oldPath)
			if err != nil {
				return err
			}
			newLive, newChain, err := LstatResolveTracked(repo, newPath)
			if err != nil {
				return err
			}
			if oldLive == newLive {
				if repo.FindFile(oldLive) == nil && !repo.HasDirectory(oldLive) {
					return NotFound(oldLive)
				}
				return nil
			}
			if err := CheckWalkResolved(ctx, repo, oldChain); err != nil {
				return err
			}
			if err := CheckWalkResolved(ctx, repo, newChain); err != nil {
				return err
			}
			srcFile := repo.FindFile(oldLive)
			srcIsDir := repo.HasDirectory(oldLive)
			if srcFile == nil && !srcIsDir {
				return NotFound(oldLive)
			}
			if err := CheckParentWriteResolved(ctx, repo, oldLive, oldChain); err != nil {
				return err
			}
			if err := CheckParentWriteResolved(ctx, repo, newLive, newChain); err != nil {
				return err
			}
			if parent := ParentPath(newLive); parent != "" && !repo.HasDirectory(parent) {
				return NotFound(parent)
			}
			dstFile := repo.FindFile(newLive)
			dstDir := repo.GetDirectory(newLive)
			// RENAME_NOREPLACE: the existence decision is made against the
			// live transaction state, closing the TOCTOU window a pre-stat
			// check leaves open.
			if mutate.NoReplace() && (dstFile != nil || dstDir != nil) {
				return AlreadyExists(newLive)
			}
			now := s.backend.Now()
			if srcFile != nil {
				return renameFileInTxn(ctx, repo, oldLive, newLive, dstFile, dstDir, now)
			}
			return renameDirInTxn(ctx, repo, oldLive, newLive, dstFile, dstDir, now)
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
		return IsDirectory(newClean)
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
		return NotDirectory(newClean)
	}
	if dstDir != nil {
		if err := CheckStickyDelete(ctx, repo, ParentPath(newClean), newClean); err != nil {
			return err
		}
	}
	if IsParentOrSame(oldClean, newClean) {
		return InvalidArgument(fmt.Sprintf("cannot move directory %s into itself %s", oldClean, newClean))
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
	for _, targetPath := range dirs {
		if dir := repo.GetDirectory(targetPath); dir != nil {
			cloned := *dir
			cloned.ModifiedAt = now
			cloned.ChangedAt = now
			dirRemaps = append(dirRemaps, dirRemap{from: targetPath, to: RemapPath(oldBase, newBase, targetPath), dir: cloned})
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
	for _, targetPath := range files {
		if file := repo.FindFile(targetPath); file != nil {
			cloned := file.Clone()
			cloned.ChangedAt = now
			fileRemaps = append(fileRemaps, fileRemap{from: targetPath, to: RemapPath(oldBase, newBase, targetPath), file: cloned})
		}
	}
	for _, r := range fileRemaps {
		repo.RemoveFile(r.from)
		repo.WriteFileDirect(r.to, r.file)
	}
}

// CopyContext duplicates srcPath to dstPath with fresh inodes.
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
		// Fast-fail only: the transaction below re-resolves both endpoints
		// against live state before authorizing anything.
		srcClean, _, err := StatResolveTracked(preRepo, srcPath)
		if err != nil {
			return err
		}
		dstClean, _, err := StatResolveTracked(preRepo, dstPath)
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
			// Re-resolve both endpoints against the live transaction state:
			// the pre-transaction walk above is fast-fail only.
			srcLive, srcChain, err := StatResolveTracked(repo, srcPath)
			if err != nil {
				return err
			}
			dstLive, dstChain, err := StatResolveTracked(repo, dstPath)
			if err != nil {
				return err
			}
			if srcLive == dstLive {
				if repo.FindFile(srcLive) == nil && !repo.HasDirectory(srcLive) {
					return NotFound(srcLive)
				}
				return AlreadyExists(srcLive)
			}
			if err := CheckWalkResolved(ctx, repo, srcChain); err != nil {
				return err
			}
			if err := CheckWalkResolved(ctx, repo, dstChain); err != nil {
				return err
			}
			srcFile := repo.FindFile(srcLive)
			srcIsDir := repo.HasDirectory(srcLive)
			if srcFile == nil && !srcIsDir {
				return NotFound(srcLive)
			}
			// A copy is a read of the source: mirror the read-side DAC of
			// ReadFileAtContext/ReadDirContext, or an attacker could duplicate
			// unreadable 0600 files (content refs, sizes, symlink targets)
			// into their own directory.
			if srcFile != nil {
				if err := CheckReadAccessResolved(ctx, repo, srcLive, srcChain); err != nil {
					return err
				}
			} else {
				if err := CheckListDirAccessResolved(ctx, repo, srcLive, srcChain); err != nil {
					return err
				}
			}
			if err := CheckParentWriteResolved(ctx, repo, dstLive, dstChain); err != nil {
				return err
			}
			if parent := ParentPath(dstLive); parent != "" && !repo.HasDirectory(parent) {
				return NotFound(parent)
			}
			dstFile := repo.FindFile(dstLive)
			dstDir := repo.GetDirectory(dstLive)
			now := s.backend.Now()
			// Caller ownership for the new nodes: provisioned once here so
			// both copy helpers stamp the same identity (OwnerIDsForCreate
			// falls back to the process owner only when no identity is
			// attached, mirroring CreateFileContext).
			defaultUID, defaultGID := s.backend.DefaultOwnerIDs()
			createUID, createGID := OwnerIDsForCreate(ctx, defaultUID, defaultGID)
			if srcFile != nil {
				return copyFileInTxn(ctx, repo, srcLive, dstLive, dstFile, dstDir, now, createUID, createGID)
			}
			return copyDirInTxn(ctx, repo, srcLive, dstLive, dstFile, dstDir, now, createUID, createGID)
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
// setuid/setgid for unprivileged callers (non-admin data writes clear
// setuid+setgid, see SanitizeWrittenFileModeForContext), exactly like a
// CloneRange new destination and data writes. Admin (the CAP_FSETID
// equivalent) keeps the source owner and bits. The store bypasses
// UpsertFile for WriteFileDirect (the destination parent is verified
// above): UpsertFile would route the fresh node through creation
// defaults, which would widen an explicit 000 mode back to 0644.
func copyFileInTxn(ctx context.Context, repo *meta.RepoMetadata, srcClean, dstClean string, dstFile *meta.FileMeta, dstDir *meta.DirMeta, now int64, createUID, createGID uint32) error {
	if dstDir != nil {
		return IsDirectory(dstClean)
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
		return NotDirectory(dstClean)
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
		return InvalidArgument(fmt.Sprintf("cannot copy directory %s into itself %s", srcClean, dstClean))
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
	for _, targetPath := range dirs {
		if targetPath == srcClean {
			continue
		}
		newPath := RemapPath(srcClean, dstClean, targetPath)
		if repo.HasDirectory(newPath) || repo.FindFile(newPath) != nil {
			return AlreadyExists(newPath)
		}
		sub := repo.GetDirectory(targetPath)
		if sub == nil {
			return NotFound(targetPath)
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
	for _, targetPath := range files {
		newPath := RemapPath(srcClean, dstClean, targetPath)
		if repo.HasDirectory(newPath) || repo.FindFile(newPath) != nil {
			return AlreadyExists(newPath)
		}
		sub := repo.FindFile(targetPath)
		if sub == nil {
			return NotFound(targetPath)
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
