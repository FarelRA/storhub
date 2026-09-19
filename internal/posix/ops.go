// Package posix implements the metadata verbs (symlink/link/chmod/chown/
// chtimes/xattr and the metadata-patch applier) on top of the fs contract.
// Boundary: fs owns concrete storage keys and DAC (path resolution,
// Check* checks, atime policy); posix owns metadata verbs and calls shfs.*
// for resolve/DAC/touch. The split is by verb family, not by layer: both
// facades share the single Backend contract aliased below, and the FUSE
// Hub stitches the two.
package posix

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Backend is the storage contract the POSIX facade operates against. It is
// an alias of fs.Backend so both facades share exactly one interface
// definition; the extra methods posix itself never calls (patch, asset
// fills) are part of that single contract rather than a divergent copy.
type Backend = shfs.Backend

type Service struct {
	backend Backend

	// loggers is the per-project logger cache. A sync.Map keeps the per-op
	// lookup lock-free; the atomic count bounds churn-driven growth (see
	// maxProjectLoggers). Entries are reclaimed via ForgetProject or with
	// the hub.
	loggers     sync.Map // map[string]*slog.Logger
	loggerCount atomic.Int64
}

// maxProjectLoggers backstops the loggers map against project-churn
// growth. Inserts past the cap evict one arbitrary entry; the next op
// rebuilds it.
const maxProjectLoggers = 256

func NewService(backend Backend) *Service {
	return &Service{backend: backend}
}

func (s *Service) logger(project string) *slog.Logger {
	if v, ok := s.loggers.Load(project); ok {
		return v.(*slog.Logger)
	}
	l := logging.WithComponent(s.backend.Logger(), "posix").With("project", project)
	actual, loaded := s.loggers.LoadOrStore(project, l)
	if !loaded {
		if s.loggerCount.Add(1) > maxProjectLoggers {
			s.loggers.Range(func(key, _ any) bool {
				s.loggers.Delete(key)
				s.loggerCount.Add(-1)
				return false
			})
		}
		return l
	}
	return actual.(*slog.Logger)
}

// ForgetProject drops the cached per-project logger so long-dead projects
// stop pinning loggers; the next op for the project rebuilds it lazily.
func (s *Service) ForgetProject(project string) {
	if _, loaded := s.loggers.LoadAndDelete(project); loaded {
		s.loggerCount.Add(-1)
	}
}

func (s *Service) logFinish(project, op string, started time.Time, err error, args ...any) {
	args = append(args, "elapsed", time.Since(started))
	if err != nil {
		args = append(args, "err", err)
		logging.Error(s.logger(project), op+" failed", args...)
		return
	}
	logging.Debug(s.logger(project), op+" complete", args...)
}

// withOp wraps one service verb with start/finish debug logging. quiet,
// when non-nil, suppresses the finish line for expected hot-path outcomes
// (e.g. xattr ENODATA on GetXAttr). It replaces the per-verb Info+logFinish
// boilerplate with one Debug-level funnel.
func (s *Service) withOp(project, op string, args []any, fn func() error, quiet func(error) bool) (err error) {
	logger := s.logger(project)
	started := time.Now().UTC()
	logging.Debug(logger, op+" start", args...)
	defer func() {
		if quiet != nil && quiet(err) {
			return
		}
		s.logFinish(project, op, started, err, args...)
	}()
	return fn()
}

func (s *Service) SymlinkContext(ctx context.Context, project, target, linkPath string) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "symlink", []any{"target", target, "path", linkPath}, func() error {
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
			if err := shfs.CheckWalkResolved(ctx, current, traversed); err != nil {
				return err
			}
			if err := shfs.CheckParentWriteResolved(ctx, current, cleanPath, traversed); err != nil {
				return err
			}
			if err := shfs.RequireParentDirectory(current, cleanPath); err != nil {
				return err
			}
			if current.FindFile(cleanPath) != nil || current.HasDirectory(cleanPath) {
				return shfs.AlreadyExists(cleanPath)
			}
			symlink.Mode, symlink.UID, symlink.GID = shfs.ApplyParentInheritance(current, cleanPath, false, symlink.Mode, symlink.UID, symlink.GID)
			symlink.Inode = current.AllocateInode()
			current.UpsertFile(cleanPath, symlink, now)
			shfs.TouchParentDirectory(current, cleanPath, now)
			return nil
		}, fmt.Sprintf("storhub: symlink %s -> %s", cleanPath, target)); err != nil {
			return err
		}
		result = &symlink
		return nil
	}, nil)
	return result, err
}

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
		shfs.TouchFileAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now())
		target = file.Symlink
		return nil
	}, nil)
	return target, err
}

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
		// link(2) does not follow a final symlink on either endpoint (the VFS
		// refuses to hard link a symlink, which the EPERM branch below
		// reproduces), so resolution is lstat-style for both.
		sourcePath, sourceTraversed, err := shfs.LstatResolveTracked(repoPre, existingPath)
		if err != nil {
			return err
		}
		linkPath, linkTraversed, err := shfs.LstatResolveTracked(repoPre, newPath)
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
				result = file
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
			if err := shfs.CheckWalkResolved(ctx, repo, sourceTraversed); err != nil {
				return err
			}
			if err := shfs.CheckWalkResolved(ctx, repo, linkTraversed); err != nil {
				return err
			}
			if err := shfs.CheckReadAccessResolved(ctx, repo, sourcePath, sourceTraversed); err != nil {
				return err
			}
			if err := shfs.CheckParentWriteResolved(ctx, repo, linkPath, linkTraversed); err != nil {
				return err
			}
			if err := shfs.RequireParentDirectory(repo, linkPath); err != nil {
				return err
			}
			if repo.FindFile(linkPath) != nil || repo.HasDirectory(linkPath) {
				return shfs.AlreadyExists(linkPath)
			}
			source := repo.FindFile(sourcePath)
			if source == nil {
				if repo.HasDirectory(sourcePath) {
					// POSIX: hard links to directories are not permitted.
					return syscall.EPERM
				}
				return s.backend.FileNotFound(sourcePath)
			}
			if source.Symlink != "" {
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
			repo.UpsertFile(linkPath, linked, now)
			shfs.TouchParentDirectory(repo, linkPath, now)
			return nil
		}, fmt.Sprintf("storhub: link %s to %s", sourcePath, linkPath)); err != nil {
			return err
		}
		result = &linked
		return nil
	}, nil)
	return result, err
}

// entryForRepoPath builds the access-check view of a node from the repo
// state visible inside an update transaction.
func entryForRepoPath(repo *meta.RepoMetadata, targetPath string) (*shfs.EntryInfo, error) {
	if file := repo.FindFile(targetPath); file != nil {
		return shfs.EntryFromFile(file, targetPath, repo.FileNLink(targetPath)), nil
	}
	if dir := repo.GetDirectory(targetPath); dir != nil {
		return shfs.EntryFromDirectory(dir, targetPath, repo.DirNLink(targetPath)), nil
	}
	return nil, shfs.NotFound(targetPath)
}

// pathTxn carries the single in-transaction resolution: the concrete key,
// the traversed chain, and the live repo. Resolving once per transaction
// (instead of re-resolving per check) closes the authorize/mutate window
// without paying three resolves per chmod/chown/chtimes/xattr: the outer
// lookupPath pre-check is only a fast-fail, and everything inside the
// transaction consumes this one resolution.
type pathTxn struct {
	repo      *meta.RepoMetadata
	key       string
	traversed []string
}

// reauthorize re-runs the traversal DAC against the transaction's chain
// and the operation's ownership check against the live node. The
// pre-transaction snapshot exists for fast rejection; only this
// in-transaction check closes the window where a concurrent rename/replace
// could swap the node between authorize and mutate.
func (tx *pathTxn) reauthorize(ctx context.Context, check func(entry *shfs.EntryInfo) error) error {
	if err := shfs.CheckWalkResolved(ctx, tx.repo, tx.traversed); err != nil {
		return err
	}
	entry, err := entryForRepoPath(tx.repo, tx.key)
	if err != nil {
		return err
	}
	return check(entry)
}

// persistDir writes back a mutated directory work copy: the root lands on
// repo.Root, anything else via WriteDirDirect. File branches persist
// explicitly through UpdateFileFamily instead.
func (tx *pathTxn) persistDir(dir *meta.DirMeta) {
	if tx.key == "" {
		tx.repo.Root.Mode = dir.Mode
		tx.repo.Root.UID = dir.UID
		tx.repo.Root.GID = dir.GID
		tx.repo.Root.CreatedAt = dir.CreatedAt
		tx.repo.Root.ModifiedAt = dir.ModifiedAt
		tx.repo.Root.AccessedAt = dir.AccessedAt
		tx.repo.Root.ChangedAt = dir.ChangedAt
		tx.repo.Root.XAttrs = dir.XAttrs.Clone()
		return
	}
	tx.repo.WriteDirDirect(tx.key, *dir)
}

// loadWorkCopy resolves targetPath against the live transaction state
// (metadata verbs follow a final symlink) and returns work copies the
// callback may mutate. The traversal DAC runs in updatePathMetadataContext
// against the returned chain before the callback fires.
func (s *Service) loadWorkCopy(repo *meta.RepoMetadata, targetPath string) (tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta, err error) {
	key, traversed, err := shfs.StatResolveTracked(repo, targetPath)
	if err != nil {
		return nil, nil, nil, err
	}
	tx = &pathTxn{repo: repo, key: key, traversed: traversed}
	if key == "" {
		root := &meta.DirMeta{
			Inode:      repo.Root.Inode,
			Mode:       repo.Root.Mode,
			UID:        repo.Root.UID,
			GID:        repo.Root.GID,
			CreatedAt:  repo.Root.CreatedAt,
			ModifiedAt: repo.Root.ModifiedAt,
			AccessedAt: repo.Root.AccessedAt,
			ChangedAt:  repo.Root.ChangedAt,
			XAttrs:     repo.Root.XAttrs.Clone(),
		}
		return tx, nil, root, nil
	}
	if f := repo.FindFile(key); f != nil {
		work := f.Clone()
		return tx, &work, nil, nil
	}
	if d := repo.GetDirectory(key); d != nil {
		work := d.Clone()
		return tx, nil, &work, nil
	}
	return nil, nil, nil, s.backend.FileNotFound(key)
}

func (s *Service) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) (err error) {
	err = s.withOp(project, "chmod", []any{"path", targetPath, "mode", mode}, func() error {
		entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
		if err := shfs.CanChmod(ctx, entry); err != nil {
			return err
		}
		mode = shfs.SanitizeChmodMode(ctx, entry, mode)
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				if err := shfs.CanChmod(ctx, current); err != nil {
					return err
				}
				mode = shfs.SanitizeChmodMode(ctx, current, mode)
				return nil
			}); err != nil {
				return err
			}
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					current.Mode = mode
					current.ChangedAt = now
				})
			}
			dir.Mode = mode
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

func (s *Service) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) (err error) {
	err = s.withOp(project, "chown", []any{"path", targetPath, "uid", uid, "gid", gid}, func() error {
		entryForAccess, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
		const keepOwner = ^uint32(0)
		if err := shfs.CanChown(ctx, entryForAccess, uid, gid); err != nil {
			return err
		}
		// POSIX chown(2): an owner value of (uid_t)-1 means "leave unchanged".
		// uid_t is unsigned, so -1's wire encoding is all-ones; accept it per
		// field. The kernel resolves these before FUSE CHOWN, so this only
		// affects direct library callers.
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				return shfs.CanChown(ctx, current, uid, gid)
			}); err != nil {
				return err
			}
			// Decision 1A: chown clears setuid+setgid for unprivileged
			// callers only; Admin (CAP_FSETID equivalent) keeps them.
			// POSIX clears on directories as well.
			keepBits := shfs.IdentityFromContext(ctx).Admin
			applyOwner := func(current *meta.FileMeta) {
				if uid != keepOwner {
					current.UID = uid
				}
				if gid != keepOwner {
					current.GID = gid
				}
				if !keepBits {
					current.Mode &^= 0o6000
				}
				current.ChangedAt = now
			}
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, applyOwner)
			}
			if uid != keepOwner {
				dir.UID = uid
			}
			if gid != keepOwner {
				dir.GID = gid
			}
			if !keepBits {
				dir.Mode &^= 0o6000
			}
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

func (s *Service) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) (err error) {
	err = s.withOp(project, "chtimes", []any{"path", targetPath, "atime", atime, "mtime", mtime}, func() error {
		entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
		// A caller with only write permission may set "now" (the verb's
		// zero-means-now contract), not arbitrary timestamps.
		var atimePtr, mtimePtr *time.Time
		if atime != 0 {
			t := time.Unix(0, atime)
			atimePtr = &t
		}
		if mtime != 0 {
			t := time.Unix(0, mtime)
			mtimePtr = &t
		}
		now := s.backend.Now()
		if err := shfs.CanSetTimesValues(ctx, entry, atimePtr, mtimePtr, now); err != nil {
			return err
		}
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				return shfs.CanSetTimesValues(ctx, current, atimePtr, mtimePtr, now)
			}); err != nil {
				return err
			}
			atime = ChooseNonZeroTime(atime, now)
			mtime = ChooseNonZeroTime(mtime, now)
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					current.AccessedAt = atime
					current.ModifiedAt = mtime
					current.ChangedAt = now
				})
			}
			dir.AccessedAt = atime
			dir.ModifiedAt = mtime
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

// ChtimesExplicitContext sets timestamps with POSIX utimensat trinary
// semantics: a nil pointer omits that timestamp entirely (UTIME_OMIT), a
// non-nil pointer sets it to exactly that value - the epoch included,
// unlike ChtimesContext whose omit-on-zero contract maps zero to "now".
// Provided values are marked authoritative in metadata.
func (s *Service) ChtimesExplicitContext(ctx context.Context, project, targetPath string, atime, mtime *time.Time) (err error) {
	err = s.withOp(project, "chtimes-explicit", []any{"path", targetPath, "has_atime", atime != nil, "has_mtime", mtime != nil}, func() error {
		entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
		// Write-only callers may omit (nil) or set "now", not arbitrary
		// timestamps.
		now := s.backend.Now()
		if err := shfs.CanSetTimesValues(ctx, entry, atime, mtime, now); err != nil {
			return err
		}
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				return shfs.CanSetTimesValues(ctx, current, atime, mtime, now)
			}); err != nil {
				return err
			}
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					if atime != nil {
						current.AccessedAt = atime.UnixNano()
					}
					if mtime != nil {
						current.ModifiedAt = mtime.UnixNano()
					}
					current.ChangedAt = now
				})
			}
			if atime != nil {
				dir.AccessedAt = atime.UnixNano()
			}
			if mtime != nil {
				dir.ModifiedAt = mtime.UnixNano()
			}
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

// SetXAttrContext stores an extended attribute. The optional mode
// qualifies create/replace semantics (XAttrCreate/XAttrReplace); the
// existence test runs inside the update transaction so it cannot race a
// concurrent set/remove. Size and name limits are enforced server-side
// because REST-originated calls bypass the kernel's VFS caps.
func (s *Service) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte, mode ...shfs.XAttrMode) (err error) {
	err = s.withOp(project, "setxattr", []any{"path", targetPath, "attr", attr, "bytes", len(data)}, func() error {
		if strings.TrimSpace(attr) == "" {
			return errors.New("xattr name is required")
		}
		if len(attr) > shfs.XAttrNameMax {
			return syscall.ERANGE
		}
		if len(data) > shfs.XAttrSizeMax {
			return syscall.ERANGE
		}
		var m shfs.XAttrMode
		for _, option := range mode {
			m |= option
		}
		repo, cleanPath, traversed, _, _, err := s.lookupPathResolved(ctx, project, targetPath)
		if err != nil {
			return err
		}
		if err := shfs.CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		value := append([]byte(nil), data...)
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				return shfs.CanAccessEntry(shfs.IdentityFromContext(ctx), current, shfs.AccessWrite)
			}); err != nil {
				return err
			}
			// The work copies are cloned from the live transaction state,
			// so the existence test below cannot race a concurrent
			// set/remove.
			exists := false
			if file != nil {
				_, exists = file.XAttrs[attr]
			} else if dir != nil {
				_, exists = dir.XAttrs[attr]
			}
			if m&shfs.XAttrCreate != 0 && exists {
				return syscall.EEXIST
			}
			if m&shfs.XAttrReplace != 0 && !exists {
				return shfs.XAttrNotFound(tx.key)
			}
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					if current.XAttrs == nil {
						current.XAttrs = make(meta.XAttrMap)
					}
					current.XAttrs[attr] = value
					current.ChangedAt = now
				})
			}
			if dir.XAttrs == nil {
				dir.XAttrs = make(meta.XAttrMap)
			}
			dir.XAttrs[attr] = value
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

func (s *Service) GetXAttrContext(ctx context.Context, project, targetPath, attr string) (result []byte, err error) {
	err = s.withOp(project, "getxattr", []any{"path", targetPath, "attr", attr}, func() error {
		if strings.TrimSpace(attr) == "" {
			return errors.New("xattr name is required")
		}
		repo, cleanPath, traversed, file, dir, err := s.lookupPathResolved(ctx, project, targetPath)
		if err != nil {
			return err
		}
		if err := shfs.CheckReadAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		var value []byte
		var ok bool
		if file != nil {
			value, ok = file.XAttrs[attr]
		} else {
			value, ok = dir.XAttrs[attr]
		}
		if !ok {
			return shfs.XAttrNotFound(cleanPath)
		}
		result = append([]byte(nil), value...)
		return nil
	}, func(err error) bool {
		// A missing xattr is a hot negative path, not an operational event.
		return errors.Is(err, shfs.ErrXAttrNotFound)
	})
	return result, err
}

func (s *Service) ListXAttrContext(ctx context.Context, project, targetPath string) (result []string, err error) {
	err = s.withOp(project, "listxattr", []any{"path", targetPath}, func() error {
		repo, cleanPath, traversed, file, dir, err := s.lookupPathResolved(ctx, project, targetPath)
		if err != nil {
			return err
		}
		if err := shfs.CheckReadAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		var attrs meta.XAttrMap
		if file != nil {
			attrs = file.XAttrs
		} else {
			attrs = dir.XAttrs
		}
		names := make([]string, 0, len(attrs))
		for name := range attrs {
			names = append(names, name)
		}
		sort.Strings(names)
		result = names
		return nil
	}, nil)
	return result, err
}

func (s *Service) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) (err error) {
	err = s.withOp(project, "removexattr", []any{"path", targetPath, "attr", attr}, func() error {
		if strings.TrimSpace(attr) == "" {
			return errors.New("xattr name is required")
		}
		// The lookupPath repo is reused for the pre-check: no second
		// metadata load. It is only a fast-fail; the in-transaction
		// live re-check below is authoritative.
		repo, cleanPath, traversed, file, dir, err := s.lookupPathResolved(ctx, project, targetPath)
		if err != nil {
			return err
		}
		if err := shfs.CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if file != nil {
			if _, ok := file.XAttrs[attr]; !ok {
				return shfs.XAttrNotFound(cleanPath)
			}
		} else if _, ok := dir.XAttrs[attr]; !ok {
			return shfs.XAttrNotFound(cleanPath)
		}
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				return shfs.CanAccessEntry(shfs.IdentityFromContext(ctx), current, shfs.AccessWrite)
			}); err != nil {
				return err
			}
			// In-transaction live re-check against the transaction's own
			// resolution: a concurrent remover between the pre-check and
			// the transaction must report ENODATA, and a replace must not
			// delete the new inode's xattr sight unseen.
			if file != nil {
				if _, ok := file.XAttrs[attr]; !ok {
					return shfs.XAttrNotFound(tx.key)
				}
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					delete(current.XAttrs, attr)
					current.ChangedAt = now
				})
			}
			if dir == nil {
				return s.backend.FileNotFound(tx.key)
			}
			if _, ok := dir.XAttrs[attr]; !ok {
				return shfs.XAttrNotFound(tx.key)
			}
			delete(dir.XAttrs, attr)
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

func (s *Service) updatePathMetadataContext(ctx context.Context, project, targetPath string, mutate func(*pathTxn, *meta.FileMeta, *meta.DirMeta) error) (err error) {
	return s.withOp(project, "update-path-metadata", []any{"path", targetPath}, func() error {
		_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
			// Resolve once against the live transaction state, then run
			// the traversal DAC against the fresh chain. The callback
			// re-authorizes ownership via tx.reauthorize and persists
			// explicitly (UpdateFileFamily for files, tx.persistDir for
			// directories), so no compare-and-repersist dance is needed.
			tx, file, dir, err := s.loadWorkCopy(repo, targetPath)
			if err != nil {
				return err
			}
			if err := shfs.CheckWalkResolved(ctx, repo, tx.traversed); err != nil {
				return err
			}
			return mutate(tx, file, dir)
		}, fmt.Sprintf("storhub: update metadata for %s", targetPath))
		return err
	}, nil)
}

func (s *Service) ApplyMetadataPatchContext(ctx context.Context, project, targetPath string, patch shfs.MetadataPatch) (err error) {
	err = s.withOp(project, "apply-metadata-patch", []any{"path", targetPath, "has_mode", patch.HasMode, "has_owner", patch.HasOwner, "has_times", patch.HasTimes}, func() error {
		if !patch.HasMode && !patch.HasOwner && !patch.HasTimes {
			return nil
		}
		entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
		if patch.HasOwner {
			if err := shfs.CanChown(ctx, entry, patch.UID, patch.GID); err != nil {
				return err
			}
		}
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			// Re-authorize against LIVE transaction state: the pre-transaction
			// lookup above is only a fast-fail. A concurrent rename/chmod
			// between authorize and apply must fail closed, exactly like the
			// chmod/chown/chtimes/xattr verbs.
			sanitizedMode := patch.Mode
			if err := tx.reauthorize(ctx, func(live *shfs.EntryInfo) error {
				if patch.HasOwner {
					if err := shfs.CanChown(ctx, live, patch.UID, patch.GID); err != nil {
						return err
					}
				}
				if patch.HasMode {
					liveCopy := *live
					if patch.HasOwner {
						liveCopy.UID = patch.UID
						liveCopy.GID = patch.GID
					}
					if err := shfs.CanChmod(ctx, &liveCopy); err != nil {
						return err
					}
					sanitizedMode = shfs.SanitizeChmodMode(ctx, &liveCopy, patch.Mode)
				}
				if patch.HasTimes {
					var atimePtr, mtimePtr *time.Time
					if !patch.ATime.IsZero() {
						t := patch.ATime
						atimePtr = &t
					}
					if !patch.MTime.IsZero() {
						t := patch.MTime
						mtimePtr = &t
					}
					if err := shfs.CanSetTimesValues(ctx, live, atimePtr, mtimePtr, now); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					if patch.HasOwner {
						current.UID = patch.UID
						current.GID = patch.GID
						if !shfs.IdentityFromContext(ctx).Admin {
							current.Mode &^= 0o6000
						}
					}
					if patch.HasMode {
						current.Mode = sanitizedMode
					}
					if patch.HasTimes {
						if !patch.ATime.IsZero() {
							current.AccessedAt = patch.ATime.UnixNano()
						}
						if !patch.MTime.IsZero() {
							current.ModifiedAt = patch.MTime.UnixNano()
						}
					}
					current.ChangedAt = now
				})
			}
			if patch.HasOwner {
				dir.UID = patch.UID
				dir.GID = patch.GID
				if !shfs.IdentityFromContext(ctx).Admin {
					dir.Mode &^= 0o6000
				}
			}
			if patch.HasMode {
				dir.Mode = sanitizedMode
			}
			if patch.HasTimes {
				if !patch.ATime.IsZero() {
					dir.AccessedAt = patch.ATime.UnixNano()
				}
				if !patch.MTime.IsZero() {
					dir.ModifiedAt = patch.MTime.UnixNano()
				}
			}
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

// lookupPathResolved resolves a raw user path physically (metadata/xattr
// verbs follow a final symlink, so resolution is stat-style), enforces the
// traversal DAC of the real walk, and returns the concrete key, the
// traversed chain for *Resolved checks, and the node's snapshot.
func (s *Service) lookupPathResolved(ctx context.Context, project, targetPath string) (*meta.RepoMetadata, string, []string, *meta.FileMeta, *meta.DirMeta, error) {
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	cleanPath, traversed, err := shfs.StatResolveTracked(repo, targetPath)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	if err := shfs.CheckWalkResolved(ctx, repo, traversed); err != nil {
		return nil, "", nil, nil, nil, err
	}
	if cleanPath == "" {
		root := meta.DirMeta{
			Inode:      repo.Root.Inode,
			Mode:       repo.Root.Mode,
			UID:        repo.Root.UID,
			GID:        repo.Root.GID,
			CreatedAt:  repo.Root.CreatedAt,
			ModifiedAt: repo.Root.ModifiedAt,
			AccessedAt: repo.Root.AccessedAt,
			ChangedAt:  repo.Root.ChangedAt,
			XAttrs:     repo.Root.XAttrs.Clone(),
		}
		return repo, cleanPath, traversed, nil, &root, nil
	}
	if file := repo.FindFile(cleanPath); file != nil {
		clone := file.Clone()
		return repo, cleanPath, traversed, &clone, nil, nil
	}
	if dir := repo.GetDirectory(cleanPath); dir != nil {
		clone := dir.Clone()
		return repo, cleanPath, traversed, nil, &clone, nil
	}
	return nil, cleanPath, traversed, nil, nil, s.backend.FileNotFound(cleanPath)
}

// lookupPath resolves a raw user path physically (metadata/xattr verbs
// follow a final symlink, so resolution is stat-style), enforces the
// traversal DAC of the real walk, and returns the concrete key plus the
// node's snapshot.
func (s *Service) lookupPath(ctx context.Context, project, targetPath string) (*meta.RepoMetadata, string, *meta.FileMeta, *meta.DirMeta, error) {
	repo, cleanPath, _, file, dir, err := s.lookupPathResolved(ctx, project, targetPath)
	if err != nil {
		return nil, cleanPath, nil, nil, err
	}
	return repo, cleanPath, file, dir, nil
}

func (s *Service) lookupEntryForAccess(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	repo, cleanPath, file, dir, err := s.lookupPath(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if file != nil {
		return shfs.EntryFromFile(file, cleanPath, repo.FileNLink(cleanPath)), nil
	}
	return shfs.EntryFromDirectory(dir, cleanPath, repo.DirNLink(cleanPath)), nil
}

func UpdateFileFamily(repo *meta.RepoMetadata, inode uint64, mutate func(*meta.FileMeta)) error {
	names := repo.FindFilesByInode(inode)
	if len(names) == 0 {
		return fmt.Errorf("%w: inode family %d", shfs.ErrNotFound, inode)
	}
	updated := make(map[string]meta.FileMeta, len(names))
	for _, name := range names {
		file := repo.FindFile(name)
		if file == nil {
			continue
		}
		clone := file.Clone()
		mutate(&clone)
		updated[name] = clone
	}
	// WriteFileDirect: routing through UpsertFile would send family members
	// down the new-node path (RemoveFile erased the "existing" side),
	// letting creation defaults overwrite preserved values - including
	// authoritative epoch zeros.
	for _, name := range names {
		repo.RemoveFile(name)
	}
	for name, clone := range updated {
		repo.WriteFileDirect(name, clone)
	}
	return nil
}

func TouchInodeFamilyChangedAt(repo *meta.RepoMetadata, inode uint64, now int64) error {
	return UpdateFileFamily(repo, inode, func(current *meta.FileMeta) {
		current.ChangedAt = now
	})
}
