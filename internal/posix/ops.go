package posix

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
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
}

func NewService(backend Backend) *Service {
	return &Service{backend: backend}
}

func (s *Service) logger(project string) *slog.Logger {
	return logging.WithComponent(s.backend.Logger(), "posix").With("project", project)
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

func (s *Service) SymlinkContext(ctx context.Context, project, target, linkPath string) (result *meta.FileMeta, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "symlink start", "target", target, "path", linkPath)
	defer func() { s.logFinish(project, "symlink", started, err, "target", target, "path", linkPath) }()
	if err := s.backend.ValidateProjectName(project); err != nil {
		return nil, err
	}
	cleanPath, err := shfs.NormalizePath(linkPath)
	if err != nil {
		return nil, err
	}
	if cleanPath == "" {
		return nil, errors.New("symlink path is required")
	}
	if err := s.backend.EnsureRepoContext(ctx, project); err != nil {
		return nil, err
	}
	repo, _, err := s.backend.LoadRepoMetadataContext(ctx, project)
	if err != nil {
		return nil, err
	}
	if err := shfs.CheckParentWrite(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if err := shfs.RequireParentDirectory(repo, cleanPath); err != nil {
		return nil, err
	}
	if repo.FindFile(cleanPath) != nil || repo.HasDirectory(cleanPath) {
		return nil, shfs.AlreadyExists(cleanPath)
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
		if err := shfs.CheckParentWrite(ctx, current, cleanPath); err != nil {
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
		return nil, err
	}
	result = &symlink
	return result, nil
}

func (s *Service) ReadlinkContext(ctx context.Context, project, linkPath string) (target string, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "readlink start", "path", linkPath)
	defer func() { s.logFinish(project, "readlink", started, err, "path", linkPath) }()
	cleanPath, err := shfs.NormalizePath(linkPath)
	if err != nil {
		return "", err
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return "", err
	}
	if err := shfs.CheckTraverse(ctx, repo, cleanPath); err != nil {
		return "", err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return "", s.backend.FileNotFound(cleanPath)
	}
	if file.Symlink == "" {
		return "", shfs.InvalidSymlink(cleanPath)
	}
	shfs.TouchFileAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now())
	target = file.Symlink
	return target, nil
}

func (s *Service) LinkContext(ctx context.Context, project, existingPath, newPath string) (result *meta.FileMeta, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "link start", "source", existingPath, "path", newPath)
	defer func() { s.logFinish(project, "link", started, err, "source", existingPath, "path", newPath) }()
	sourcePath, err := shfs.NormalizePath(existingPath)
	if err != nil {
		return nil, err
	}
	linkPath, err := shfs.NormalizePath(newPath)
	if err != nil {
		return nil, err
	}
	if sourcePath == "" || linkPath == "" {
		return nil, errors.New("source and link paths are required")
	}
	if sourcePath == linkPath {
		// POSIX: link(x, x) succeeds when x exists.
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return nil, err
		}
		if repo.FindFile(sourcePath) == nil && !repo.HasDirectory(sourcePath) {
			return nil, s.backend.FileNotFound(sourcePath)
		}
		return repo.FindFile(sourcePath), nil
	}
	now := s.backend.Now()
	var linked meta.FileMeta
	if _, err := s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if err := shfs.CheckReadAccess(ctx, repo, sourcePath); err != nil {
			return err
		}
		if err := shfs.CheckParentWrite(ctx, repo, linkPath); err != nil {
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
		linked.Mode, linked.UID, linked.GID = shfs.ApplyParentInheritance(repo, linkPath, false, linked.Mode, linked.UID, linked.GID)
		if err := TouchInodeFamilyChangedAt(repo, source.Inode, now); err != nil {
			return err
		}
		repo.UpsertFile(linkPath, linked, now)
		shfs.TouchParentDirectory(repo, linkPath, now)
		return nil
	}, fmt.Sprintf("storhub: link %s to %s", sourcePath, linkPath)); err != nil {
		return nil, err
	}
	result = &linked
	return result, nil
}

// entryForRepoPath builds the access-check view of a node from the repo
// state visible inside an update transaction.
func entryForRepoPath(repo *meta.RepoMetadata, targetPath string) (*shfs.EntryInfo, error) {
	if file := repo.FindFile(targetPath); file != nil {
		return shfs.EntryInfoFromFile(file, targetPath, repo.FileNLink(targetPath)), nil
	}
	if dir := repo.GetDirectory(targetPath); dir != nil {
		return shfs.EntryInfoFromDirectory(dir, targetPath, repo.DirNLink(targetPath)), nil
	}
	return nil, shfs.NotFound(targetPath)
}

// reauthorizeInTransaction re-runs traversal and the operation's ownership
// check against the metadata state inside the update transaction. The
// pre-transaction snapshot exists for fast rejection; only the in-transaction
// check closes the window where a concurrent rename/replace could swap the
// node between authorize and mutate.
func (s *Service) reauthorizeInTransaction(ctx context.Context, repo *meta.RepoMetadata, targetPath string, check func(entry *shfs.EntryInfo) error) error {
	if err := shfs.CheckTraverse(ctx, repo, targetPath); err != nil {
		return err
	}
	entry, err := entryForRepoPath(repo, targetPath)
	if err != nil {
		return err
	}
	return check(entry)
}

func (s *Service) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "chmod start", "path", targetPath, "mode", mode)
	defer func() { s.logFinish(project, "chmod", started, err, "path", targetPath, "mode", mode) }()
	entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if err := shfs.CanChmod(ctx, entry); err != nil {
		return err
	}
	mode = shfs.SanitizeChmodMode(ctx, entry, mode)
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMeta, dir *meta.DirMeta) error {
		now := s.backend.Now()
		if err := s.reauthorizeInTransaction(ctx, repo, targetPath, func(current *shfs.EntryInfo) error {
			if err := shfs.CanChmod(ctx, current); err != nil {
				return err
			}
			mode = shfs.SanitizeChmodMode(ctx, current, mode)
			return nil
		}); err != nil {
			return err
		}
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMeta) {
				current.Mode = mode
				current.ChangedAt = now
			})
		}
		dir.Mode = mode
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "chown start", "path", targetPath, "uid", uid, "gid", gid)
	defer func() { s.logFinish(project, "chown", started, err, "path", targetPath, "uid", uid, "gid", gid) }()
	if _, err := s.lookupEntryForAccess(ctx, project, targetPath); err != nil {
		return err
	}
	if err := shfs.CanChown(ctx); err != nil {
		return err
	}
	// POSIX chown(2): an owner value of (uid_t)-1 means "leave unchanged".
	// uid_t is unsigned, so -1's wire encoding is all-ones; accept it per
	// field. The kernel resolves these before FUSE CHOWN, so this only
	// affects direct library callers.
	const keepOwner = ^uint32(0)
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMeta, dir *meta.DirMeta) error {
		now := s.backend.Now()
		if err := s.reauthorizeInTransaction(ctx, repo, targetPath, func(_ *shfs.EntryInfo) error {
			return shfs.CanChown(ctx)
		}); err != nil {
			return err
		}
		applyOwner := func(current *meta.FileMeta) {
			if uid != keepOwner {
				current.UID = uid
			}
			if gid != keepOwner {
				current.GID = gid
			}
			current.Mode &^= 0o6000
			current.ChangedAt = now
		}
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, applyOwner)
		}
		if uid != keepOwner {
			dir.UID = uid
		}
		if gid != keepOwner {
			dir.GID = gid
		}
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "chtimes start", "path", targetPath, "atime", atime, "mtime", mtime)
	defer func() { s.logFinish(project, "chtimes", started, err, "path", targetPath) }()
	entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if err := shfs.CanSetTimes(ctx, entry); err != nil {
		return err
	}
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMeta, dir *meta.DirMeta) error {
		now := s.backend.Now()
		if err := s.reauthorizeInTransaction(ctx, repo, targetPath, func(current *shfs.EntryInfo) error {
			return shfs.CanSetTimes(ctx, current)
		}); err != nil {
			return err
		}
		atime = ChooseNonZeroTime(atime, now)
		mtime = ChooseNonZeroTime(mtime, now)
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMeta) {
				current.AccessedAt = atime
				current.ModifiedAt = mtime
				current.ChangedAt = now
			})
		}
		dir.AccessedAt = atime
		dir.ModifiedAt = mtime
		dir.ChangedAt = now
		return nil
	})
}

// ChtimesExplicitContext sets timestamps with POSIX utimensat trinary
// semantics: a nil pointer omits that timestamp entirely (UTIME_OMIT), a
// non-nil pointer sets it to exactly that value - the epoch included,
// unlike ChtimesContext whose omit-on-zero contract maps zero to "now".
// Provided values are marked authoritative in metadata.
func (s *Service) ChtimesExplicitContext(ctx context.Context, project, targetPath string, atime, mtime *time.Time) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "chtimes-explicit start", "path", targetPath, "has_atime", atime != nil, "has_mtime", mtime != nil)
	defer func() { s.logFinish(project, "chtimes-explicit", started, err, "path", targetPath) }()
	if _, err := s.lookupEntryForAccess(ctx, project, targetPath); err != nil {
		return err
	}
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMeta, dir *meta.DirMeta) error {
		now := s.backend.Now()
		if err := s.reauthorizeInTransaction(ctx, repo, targetPath, func(current *shfs.EntryInfo) error {
			return shfs.CanSetTimes(ctx, current)
		}); err != nil {
			return err
		}
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMeta) {
				if atime != nil {
					current.AccessedAt = atime.Unix()
				}
				if mtime != nil {
					current.ModifiedAt = mtime.Unix()
				}
				current.ChangedAt = now
			})
		}
		if atime != nil {
			dir.AccessedAt = atime.Unix()
		}
		if mtime != nil {
			dir.ModifiedAt = mtime.Unix()
		}
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "setxattr start", "path", targetPath, "attr", attr, "bytes", len(data))
	defer func() {
		s.logFinish(project, "setxattr", started, err, "path", targetPath, "attr", attr, "bytes", len(data))
	}()
	if strings.TrimSpace(attr) == "" {
		return errors.New("xattr name is required")
	}
	repo, cleanPath, _, _, err := s.lookupPath(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if err := shfs.CheckWriteAccess(ctx, repo, cleanPath); err != nil {
		return err
	}
	value := append([]byte(nil), data...)
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMeta, dir *meta.DirMeta) error {
		now := s.backend.Now()
		if err := s.reauthorizeInTransaction(ctx, repo, targetPath, func(_ *shfs.EntryInfo) error {
			return shfs.CheckWriteAccess(ctx, repo, targetPath)
		}); err != nil {
			return err
		}
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMeta) {
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
		return nil
	})
}

func (s *Service) GetXAttrContext(ctx context.Context, project, targetPath, attr string) (result []byte, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "getxattr start", "path", targetPath, "attr", attr)
	defer func() {
		if err != nil && !errors.Is(err, shfs.ErrXAttrNotFound) {
			s.logFinish(project, "getxattr", started, err, "path", targetPath, "attr", attr)
		}
		if err == nil {
			s.logFinish(project, "getxattr", started, nil, "path", targetPath, "attr", attr)
		}
	}()
	if strings.TrimSpace(attr) == "" {
		return nil, errors.New("xattr name is required")
	}
	repo, cleanPath, file, dir, err := s.lookupPath(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if err := shfs.CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	var value []byte
	var ok bool
	if file != nil {
		value, ok = file.XAttrs[attr]
	} else {
		value, ok = dir.XAttrs[attr]
	}
	if !ok {
		return nil, shfs.XAttrNotFound(cleanPath)
	}
	result = append([]byte(nil), value...)
	return result, nil
}

func (s *Service) ListXAttrContext(ctx context.Context, project, targetPath string) (result []string, err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "listxattr start", "path", targetPath)
	defer func() { s.logFinish(project, "listxattr", started, err, "path", targetPath) }()
	repo, cleanPath, file, dir, err := s.lookupPath(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if err := shfs.CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
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
	return result, nil
}

func (s *Service) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "removexattr start", "path", targetPath, "attr", attr)
	defer func() { s.logFinish(project, "removexattr", started, err, "path", targetPath, "attr", attr) }()
	if strings.TrimSpace(attr) == "" {
		return errors.New("xattr name is required")
	}
	_, cleanPath, file, dir, err := s.lookupPath(ctx, project, targetPath)
	if err != nil {
		return err
	}
	repo, _, lookupErr := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if lookupErr != nil {
		return lookupErr
	}
	if err := shfs.CheckWriteAccess(ctx, repo, cleanPath); err != nil {
		return err
	}
	if file != nil {
		if _, ok := file.XAttrs[attr]; !ok {
			return shfs.XAttrNotFound(cleanPath)
		}
	} else if _, ok := dir.XAttrs[attr]; !ok {
		return shfs.XAttrNotFound(cleanPath)
	}
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMeta, dir *meta.DirMeta) error {
		now := s.backend.Now()
		if err := s.reauthorizeInTransaction(ctx, repo, targetPath, func(_ *shfs.EntryInfo) error {
			return shfs.CheckWriteAccess(ctx, repo, targetPath)
		}); err != nil {
			return err
		}
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMeta) {
				delete(current.XAttrs, attr)
				current.ChangedAt = now
			})
		}
		delete(dir.XAttrs, attr)
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) updatePathMetadataContext(ctx context.Context, project, targetPath string, mutate func(*meta.RepoMetadata, *meta.FileMeta, *meta.DirMeta) error) (err error) {
	started := time.Now().UTC()
	logging.Debug(s.logger(project), "update-path-metadata start", "path", targetPath)
	defer func() { s.logFinish(project, "update-path-metadata", started, err, "path", targetPath) }()
	cleanPath := ""
	if strings.TrimSpace(targetPath) != "" {
		var err error
		cleanPath, err = shfs.NormalizePath(targetPath)
		if err != nil {
			return err
		}
	}
	_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if cleanPath == "" {
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
			if err := mutate(repo, nil, root); err != nil {
				return err
			}
			repo.Root.Mode = root.Mode
			repo.Root.UID = root.UID
			repo.Root.GID = root.GID
			repo.Root.CreatedAt = root.CreatedAt
			repo.Root.ModifiedAt = root.ModifiedAt
			repo.Root.AccessedAt = root.AccessedAt
			repo.Root.ChangedAt = root.ChangedAt
			repo.Root.XAttrs = root.XAttrs.Clone()
			return nil
		}
		if file := repo.FindFile(cleanPath); file != nil {
			before := file.Clone()
			work := file.Clone()
			if err := mutate(repo, &work, nil); err != nil {
				return err
			}
			// Persist the mutated clone only when the callback did not
			// persist it itself (some callbacks route through
			// UpdateFileFamily to keep hardlink families consistent).
			if current := repo.FindFile(cleanPath); current != nil && equalFileMeta(*current, before) {
				repo.ReplaceFile(cleanPath, work)
			}
			return nil
		}
		if dir := repo.GetDirectory(cleanPath); dir != nil {
			copy := dir.Clone()
			if err := mutate(repo, nil, &copy); err != nil {
				return err
			}
			repo.Dirs[cleanPath] = copy
			return nil
		}
		return s.backend.FileNotFound(cleanPath)
	}, fmt.Sprintf("storhub: update metadata for %s", cleanPath))
	return err
}

func (s *Service) ApplyMetadataPatchContext(ctx context.Context, project, targetPath string, patch shfs.MetadataPatch) (err error) {
	started := time.Now().UTC()
	logging.Info(s.logger(project), "apply-metadata-patch start", "path", targetPath, "has_mode", patch.HasMode, "has_owner", patch.HasOwner, "has_times", patch.HasTimes)
	defer func() { s.logFinish(project, "apply-metadata-patch", started, err, "path", targetPath) }()
	if !patch.HasMode && !patch.HasOwner && !patch.HasTimes {
		return nil
	}
	if _, err := s.lookupEntryForAccess(ctx, project, targetPath); err != nil {
		return err
	}
	if patch.HasOwner {
		if err := shfs.CanChown(ctx); err != nil {
			return err
		}
	}
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMeta, dir *meta.DirMeta) error {
		now := s.backend.Now()
		// Re-authorize against LIVE transaction state: the pre-transaction
		// lookup above is only a fast-fail. A concurrent rename/chmod
		// between authorize and apply must fail closed, exactly like the
		// chmod/chown/chtimes/xattr verbs.
		sanitizedMode := patch.Mode
		if err := s.reauthorizeInTransaction(ctx, repo, targetPath, func(live *shfs.EntryInfo) error {
			if patch.HasOwner {
				if err := shfs.CanChown(ctx); err != nil {
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
				if err := shfs.CanSetTimes(ctx, live); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMeta) {
				if patch.HasOwner {
					current.UID = patch.UID
					current.GID = patch.GID
					current.Mode &^= 0o6000
				}
				if patch.HasMode {
					current.Mode = sanitizedMode
				}
				if patch.HasTimes {
					if !patch.ATime.IsZero() {
						current.AccessedAt = patch.ATime.Unix()
					}
					if !patch.MTime.IsZero() {
						current.ModifiedAt = patch.MTime.Unix()
					}
				}
				current.ChangedAt = now
			})
		}
		if patch.HasOwner {
			dir.UID = patch.UID
			dir.GID = patch.GID
			dir.Mode &^= 0o6000
		}
		if patch.HasMode {
			dir.Mode = sanitizedMode
		}
		if patch.HasTimes {
			if !patch.ATime.IsZero() {
				dir.AccessedAt = patch.ATime.Unix()
			}
			if !patch.MTime.IsZero() {
				dir.ModifiedAt = patch.MTime.Unix()
			}
		}
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) lookupPath(ctx context.Context, project, targetPath string) (*meta.RepoMetadata, string, *meta.FileMeta, *meta.DirMeta, error) {
	cleanPath := ""
	if strings.TrimSpace(targetPath) != "" {
		var err error
		cleanPath, err = shfs.NormalizePath(targetPath)
		if err != nil {
			return nil, "", nil, nil, err
		}
	}
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, "", nil, nil, err
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
		return repo, cleanPath, nil, &root, nil
	}
	if file := repo.FindFile(cleanPath); file != nil {
		clone := file.Clone()
		return repo, cleanPath, &clone, nil, nil
	}
	if dir := repo.GetDirectory(cleanPath); dir != nil {
		clone := dir.Clone()
		return repo, cleanPath, nil, &clone, nil
	}
	return nil, cleanPath, nil, nil, s.backend.FileNotFound(cleanPath)
}

func (s *Service) lookupEntryForAccess(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	repo, cleanPath, file, dir, err := s.lookupPath(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if err := shfs.CheckTraverse(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	if file != nil {
		return shfs.EntryInfoFromFile(file, cleanPath, repo.FileNLink(cleanPath)), nil
	}
	return shfs.EntryInfoFromDirectory(dir, cleanPath, repo.DirNLink(cleanPath)), nil
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

func equalFileMeta(a, b meta.FileMeta) bool {
	if a.Chunks == nil && b.Chunks != nil || a.Chunks != nil && b.Chunks == nil {
		return false
	}
	for i := range a.Chunks {
		if a.Chunks[i] != b.Chunks[i] {
			return false
		}
	}
	if len(a.XAttrs) != len(b.XAttrs) {
		return false
	}
	for k, v := range a.XAttrs {
		bv, ok := b.XAttrs[k]
		if !ok || string(v) != string(bv) {
			return false
		}
	}
	return a.Size == b.Size && a.Symlink == b.Symlink &&
		a.UploadedAt == b.UploadedAt && a.ModifiedAt == b.ModifiedAt &&
		a.AccessedAt == b.AccessedAt && a.ChangedAt == b.ChangedAt &&
		a.Mode == b.Mode && a.UID == b.UID && a.GID == b.GID && a.Inode == b.Inode
}
