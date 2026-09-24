package posix

import (
	"context"
	"errors"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"syscall"
)

// SymlinkContext creates a symlink at linkPath pointing at target. The
// debug line logs the target length, not the target: symlink targets are
// user-controlled bytes that can embed secrets (the xattr verbs apply the
// same lengths-only discipline to values).
func (s *Service) SymlinkContext(ctx context.Context, project, target, linkPath string) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "symlink", []any{"target_len", len(target), "path", linkPath}, func() error {
		if err := s.backend.ValidateProjectName(project); err != nil {
			return err
		}
		// Symlink(2) resource limits, enforced server-side because
		// REST-originated calls bypass the kernel's VFS checks. An empty
		// target is ENOENT (symlink(2) on an empty oldpath), an over-long
		// one ENAMETOOLONG.
		if target == "" {
			return syscall.ENOENT
		}
		if len(target) > shfs.PathMax {
			return syscall.ENAMETOOLONG
		}
		if err := shfs.ValidateAccessPathShape(linkPath); err != nil {
			return err
		}
		if err := s.backend.EnsureRepoContext(ctx, project); err != nil {
			return err
		}
		repo, _, err := s.backend.LoadRepoMetadataContext(ctx, project)
		if err != nil {
			return err
		}
		// symlink(2) creates the link itself: a final component that already
		// exists (including as a symlink) is EEXIST, never followed, so
		// resolution is lstat-style.
		cleanPath, traversed, err := shfs.LstatResolveTracked(repo, linkPath)
		if err != nil {
			return err
		}
		if cleanPath == "" {
			return errors.New("symlink path is required")
		}
		if err := shfs.CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		if err := shfs.CheckParentWriteResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if err := shfs.RequireParentDirectory(repo, cleanPath); err != nil {
			return err
		}
		if repo.FindFile(cleanPath) != nil || repo.HasDirectory(cleanPath) {
			return shfs.AlreadyExists(cleanPath)
		}
		now := s.backend.Now()
		defaultUID, defaultGID := s.backend.DefaultOwnerIDs()
		uid, gid := shfs.OwnerIDsForCreate(ctx, defaultUID, defaultGID)
		symlink := meta.FileMeta{
			Size:       int64(len([]byte(target))),
			Chunks:     []int64{},
			UploadedAt: now,
			ModifiedAt: now,
			AccessedAt: now,
			ChangedAt:  now,
			Mode:       s.backend.DefaultFileMode(meta.NodeKindSymlink),
			UID:        uid,
			GID:        gid,
			Symlink:    target,
		}
		symlink.Mode, symlink.UID, symlink.GID = shfs.ApplyParentInheritance(repo, cleanPath, false, symlink.Mode, symlink.UID, symlink.GID)
		if _, err := s.backend.UpdateRepoMetadataContext(ctx, project, func(current *meta.RepoMetadata) error {
			// Re-resolve against the live transaction state: the
			// pre-transaction walk above is fast-fail only.
			liveClean, liveChain, err := shfs.LstatResolveTracked(current, linkPath)
			if err != nil {
				return err
			}
			if liveClean == "" {
				return errors.New("symlink path is required")
			}
			if err := shfs.CheckWalkResolved(ctx, current, liveChain); err != nil {
				return err
			}
			if err := shfs.CheckParentWriteResolved(ctx, current, liveClean, liveChain); err != nil {
				return err
			}
			if err := shfs.RequireParentDirectory(current, liveClean); err != nil {
				return err
			}
			if current.FindFile(liveClean) != nil || current.HasDirectory(liveClean) {
				return shfs.AlreadyExists(liveClean)
			}
			symlink.Mode, symlink.UID, symlink.GID = shfs.ApplyParentInheritance(current, liveClean, false, symlink.Mode, symlink.UID, symlink.GID)
			symlink.Inode = current.AllocateInode()
			current.UpsertFile(liveClean, symlink, now)
			shfs.TouchParentDirectory(current, liveClean, now)
			return nil
		}, fmt.Sprintf("storhub: symlink %s -> %s", cleanPath, target)); err != nil {
			return err
		}
		result = &symlink
		return nil
	}, nil)
	return result, err
}

// ReadlinkContext returns the target of the symlink at linkPath.
func (s *Service) ReadlinkContext(ctx context.Context, project, linkPath string) (target string, err error) {
	err = s.withOp(project, "readlink", []any{"path", linkPath}, func() error {
		if err := shfs.ValidateAccessPathShape(linkPath); err != nil {
			return err
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		// readlink(2) operates on the link itself: lstat-style resolution.
		cleanPath, traversed, err := shfs.LstatResolveTracked(repo, linkPath)
		if err != nil {
			return err
		}
		if err := shfs.CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if file.Symlink == "" {
			return shfs.InvalidSymlink(cleanPath)
		}
		// No atime bump: readlink leaves access times alone, matching the
		// stat verbs in this tree (which never bump atime on read). Linux
		// leaves readlink atime behavior implementation-defined, so the
		// consistent choice is to not touch in either.
		target = file.Symlink
		return nil
	}, nil)
	return target, err
}

// LinkContext creates a hardlink at newPath for existingPath.
func (s *Service) LinkContext(ctx context.Context, project, existingPath, newPath string) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "link", []any{"source", existingPath, "path", newPath}, func() error {
		if err := shfs.ValidateAccessPathShape(existingPath); err != nil {
			return err
		}
		if err := shfs.ValidateAccessPathShape(newPath); err != nil {
			return err
		}
		repoPre, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		// link(2) does not follow a final symlink on either endpoint (storhub
		// policy: hard links to symlinks are EPERM, see the branch below),
		// so resolution is lstat-style for both.
		sourcePath, _, err := shfs.LstatResolveTracked(repoPre, existingPath)
		if err != nil {
			return err
		}
		linkPath, _, err := shfs.LstatResolveTracked(repoPre, newPath)
		if err != nil {
			return err
		}
		if sourcePath == "" || linkPath == "" {
			return errors.New("source and link paths are required")
		}
		if sourcePath == linkPath {
			// POSIX: link(x, x) succeeds when x exists. A directory source is
			// EPERM (hard links to directories are not permitted) - returning
			// (nil, nil) for it would hand callers a nil entry to dereference.
			if file := repoPre.FindFile(sourcePath); file != nil {
				// Clone locally: FindFile returns a copy today, but the
				// result must not depend on another package's doc comment.
				clone := file.Clone()
				result = &clone
				return nil
			}
			if repoPre.HasDirectory(sourcePath) {
				return syscall.EPERM
			}
			return s.backend.FileNotFound(sourcePath)
		}
		now := s.backend.Now()
		var linked meta.FileMeta
		if _, err := s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
			// Re-resolve both endpoints against the live transaction
			// state: the pre-transaction walk above is fast-fail only. A
			// concurrent rename/replace of a symlink component between
			// the two must fail closed, not authorize the old chain.
			sourceLive, sourceChain, err := shfs.LstatResolveTracked(repo, existingPath)
			if err != nil {
				return err
			}
			linkLive, linkChain, err := shfs.LstatResolveTracked(repo, newPath)
			if err != nil {
				return err
			}
			if sourceLive == "" || linkLive == "" {
				return errors.New("source and link paths are required")
			}
			if sourceLive == linkLive {
				if file := repo.FindFile(sourceLive); file != nil {
					linked = file.Clone()
					return nil
				}
				if repo.HasDirectory(sourceLive) {
					return syscall.EPERM
				}
				return s.backend.FileNotFound(sourceLive)
			}
			if err := shfs.CheckWalkResolved(ctx, repo, sourceChain); err != nil {
				return err
			}
			if err := shfs.CheckWalkResolved(ctx, repo, linkChain); err != nil {
				return err
			}
			if err := shfs.CheckReadAccessResolved(ctx, repo, sourceLive, sourceChain); err != nil {
				return err
			}
			if err := shfs.CheckParentWriteResolved(ctx, repo, linkLive, linkChain); err != nil {
				return err
			}
			if err := shfs.RequireParentDirectory(repo, linkLive); err != nil {
				return err
			}
			if repo.FindFile(linkLive) != nil || repo.HasDirectory(linkLive) {
				return shfs.AlreadyExists(linkLive)
			}
			source := repo.FindFile(sourceLive)
			if source == nil {
				if repo.HasDirectory(sourceLive) {
					// POSIX: hard links to directories are not permitted.
					return syscall.EPERM
				}
				return s.backend.FileNotFound(sourceLive)
			}
			if source.Symlink != "" {
				// Storhub policy: hard links to symlinks are EPERM.
				return syscall.EPERM
			}
			linked = source.Clone()
			linked.ChangedAt = now
			linked.AccessedAt = now
			// No ApplyParentInheritance here: POSIX hard links share one inode
			// and therefore one identical attribute set. Inheriting the link
			// parent's setgid GID would leave two names for the same inode
			// with divergent GIDs, breaking the convergence that
			// FindFilesByInode/UpdateFileFamily assume.
			if err := TouchInodeFamilyChangedAt(repo, source.Inode, now); err != nil {
				return err
			}
			repo.UpsertFile(linkLive, linked, now)
			shfs.TouchParentDirectory(repo, linkLive, now)
			return nil
		}, fmt.Sprintf("storhub: link %s to %s", sourcePath, linkPath)); err != nil {
			return err
		}
		result = &linked
		return nil
	}, nil)
	return result, err
}
