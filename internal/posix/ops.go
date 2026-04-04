package posix

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type Backend interface {
	ValidateProjectName(project string) error
	EnsureRepoContext(ctx context.Context, project string) error
	LoadRepoMetadataContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error)
	GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *meta.RepoMetadata, requiredSize int, preferredTag string) (string, string, error)
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

func (s *Service) SymlinkContext(ctx context.Context, project, target, linkPath string) (*meta.FileMetadata, error) {
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
		return nil, fmt.Errorf("path already exists: %s", cleanPath)
	}
	workingMeta := repo.Clone()
	releaseTag, _, err := s.backend.GetOrCreateUploadReleaseContext(ctx, project, &workingMeta, 0, "")
	if err != nil {
		return nil, err
	}
	now := s.backend.Now().UTC()
	defaultUID, defaultGID := s.backend.DefaultOwnerIDs()
	uid, gid := shfs.OwnerIDsForCreate(ctx, defaultUID, defaultGID)
	symlink := meta.FileMetadata{
		Name:          cleanPath,
		Kind:          meta.NodeKindSymlink,
		Size:          int64(len([]byte(target))),
		Chunks:        []meta.ChunkInfo{},
		Release:       releaseTag,
		UploadedAt:    now,
		ModifiedAt:    now,
		AccessedAt:    now,
		ChangedAt:     now,
		Mode:          s.backend.DefaultFileMode(meta.NodeKindSymlink),
		UID:           uid,
		GID:           gid,
		SymlinkTarget: target,
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
			return fmt.Errorf("path already exists: %s", cleanPath)
		}
		symlink.Mode, symlink.UID, symlink.GID = shfs.ApplyParentInheritance(current, cleanPath, false, symlink.Mode, symlink.UID, symlink.GID)
		symlink.Inode = current.AllocateInode()
		current.UpsertFile(symlink, now)
		shfs.TouchParentDirectory(current, cleanPath, now)
		return nil
	}, fmt.Sprintf("storhub: symlink %s -> %s", cleanPath, target)); err != nil {
		return nil, err
	}
	return &symlink, nil
}

func (s *Service) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
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
	if file.Kind != meta.NodeKindSymlink {
		return "", fmt.Errorf("path is not a symlink: %s", cleanPath)
	}
	shfs.TouchFileAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now().UTC())
	return file.SymlinkTarget, nil
}

func (s *Service) LinkContext(ctx context.Context, project, existingPath, newPath string) (*meta.FileMetadata, error) {
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
		return nil, fmt.Errorf("source and destination are the same: %s", sourcePath)
	}
	now := s.backend.Now().UTC()
	var linked meta.FileMetadata
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
			return fmt.Errorf("path already exists: %s", linkPath)
		}
		source := repo.FindFile(sourcePath)
		if source == nil {
			return s.backend.FileNotFound(sourcePath)
		}
		if source.Kind != meta.NodeKindFile {
			return fmt.Errorf("hard links only support regular files: %s", sourcePath)
		}
		linked = source.Clone()
		linked.Name = linkPath
		linked.ChangedAt = now
		linked.AccessedAt = now
		linked.Mode, linked.UID, linked.GID = shfs.ApplyParentInheritance(repo, linkPath, false, linked.Mode, linked.UID, linked.GID)
		if err := TouchInodeFamilyChangedAt(repo, source.Inode, now); err != nil {
			return err
		}
		repo.UpsertFile(linked, now)
		shfs.TouchParentDirectory(repo, linkPath, now)
		return nil
	}, fmt.Sprintf("storhub: link %s to %s", sourcePath, linkPath)); err != nil {
		return nil, err
	}
	return &linked, nil
}

func (s *Service) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error {
	entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if err := shfs.CanChmod(ctx, entry); err != nil {
		return err
	}
	mode = shfs.SanitizeChmodMode(ctx, entry, mode)
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMetadata, dir *meta.DirectoryMetadata) error {
		now := s.backend.Now().UTC()
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMetadata) {
				current.Mode = mode
				current.ChangedAt = now
			})
		}
		dir.Mode = mode
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error {
	if _, err := s.lookupEntryForAccess(ctx, project, targetPath); err != nil {
		return err
	}
	if err := shfs.CanChown(ctx); err != nil {
		return err
	}
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMetadata, dir *meta.DirectoryMetadata) error {
		now := s.backend.Now().UTC()
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMetadata) {
				current.UID = uid
				current.GID = gid
				current.Mode &^= 0o6000
				current.ChangedAt = now
			})
		}
		dir.UID = uid
		dir.GID = gid
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime time.Time) error {
	entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if err := shfs.CanSetTimes(ctx, entry); err != nil {
		return err
	}
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMetadata, dir *meta.DirectoryMetadata) error {
		now := s.backend.Now().UTC()
		atime = ChooseNonZeroTime(atime, now)
		mtime = ChooseNonZeroTime(mtime, now)
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMetadata) {
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

func (s *Service) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte) error {
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
	value := string(append([]byte(nil), data...))
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMetadata, dir *meta.DirectoryMetadata) error {
		now := s.backend.Now().UTC()
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMetadata) {
				if current.XAttrs == nil {
					current.XAttrs = make(map[string]string)
				}
				current.XAttrs[attr] = value
				current.ChangedAt = now
			})
		}
		if dir.XAttrs == nil {
			dir.XAttrs = make(map[string]string)
		}
		dir.XAttrs[attr] = value
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
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
	var value string
	var ok bool
	if file != nil {
		value, ok = file.XAttrs[attr]
	} else {
		value, ok = dir.XAttrs[attr]
	}
	if !ok {
		return nil, fmt.Errorf("xattr not found: %s", cleanPath)
	}
	return []byte(value), nil
}

func (s *Service) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	repo, cleanPath, file, dir, err := s.lookupPath(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if err := shfs.CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return nil, err
	}
	var attrs map[string]string
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
	return names, nil
}

func (s *Service) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error {
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
			return fmt.Errorf("xattr not found: %s", cleanPath)
		}
	} else if _, ok := dir.XAttrs[attr]; !ok {
		return fmt.Errorf("xattr not found: %s", cleanPath)
	}
	return s.updatePathMetadataContext(ctx, project, targetPath, func(repo *meta.RepoMetadata, file *meta.FileMetadata, dir *meta.DirectoryMetadata) error {
		now := s.backend.Now().UTC()
		if file != nil {
			return UpdateFileFamily(repo, file.Inode, func(current *meta.FileMetadata) {
				delete(current.XAttrs, attr)
				current.ChangedAt = now
			})
		}
		delete(dir.XAttrs, attr)
		dir.ChangedAt = now
		return nil
	})
}

func (s *Service) updatePathMetadataContext(ctx context.Context, project, targetPath string, mutate func(*meta.RepoMetadata, *meta.FileMetadata, *meta.DirectoryMetadata) error) error {
	cleanPath := ""
	if strings.TrimSpace(targetPath) != "" {
		var err error
		cleanPath, err = shfs.NormalizePath(targetPath)
		if err != nil {
			return err
		}
	}
	_, err := s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if cleanPath == "" {
			root := &meta.DirectoryMetadata{
				Path:       "",
				Inode:      repo.Root.Inode,
				Mode:       repo.Root.Mode,
				UID:        repo.Root.UID,
				GID:        repo.Root.GID,
				NLink:      repo.Root.NLink,
				CreatedAt:  repo.Root.CreatedAt,
				ModifiedAt: repo.Root.ModifiedAt,
				AccessedAt: repo.Root.AccessedAt,
				ChangedAt:  repo.Root.ChangedAt,
				XAttrs:     CloneStringMap(repo.Root.XAttrs),
			}
			if err := mutate(repo, nil, root); err != nil {
				return err
			}
			repo.Root.Mode = root.Mode
			repo.Root.UID = root.UID
			repo.Root.GID = root.GID
			repo.Root.NLink = root.NLink
			repo.Root.CreatedAt = root.CreatedAt
			repo.Root.ModifiedAt = root.ModifiedAt
			repo.Root.AccessedAt = root.AccessedAt
			repo.Root.ChangedAt = root.ChangedAt
			repo.Root.XAttrs = CloneStringMap(root.XAttrs)
			return nil
		}
		if file := repo.FindFile(cleanPath); file != nil {
			copy := file.Clone()
			return mutate(repo, &copy, nil)
		}
		if dir := repo.GetDirectory(cleanPath); dir != nil {
			copy := dir.Clone()
			if err := mutate(repo, nil, &copy); err != nil {
				return err
			}
			*dir = copy
			return nil
		}
		return s.backend.FileNotFound(cleanPath)
	}, fmt.Sprintf("storhub: update metadata for %s", cleanPath))
	return err
}

func (s *Service) lookupPath(ctx context.Context, project, targetPath string) (*meta.RepoMetadata, string, *meta.FileMetadata, *meta.DirectoryMetadata, error) {
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
		root := meta.DirectoryMetadata{
			Path:       "",
			Inode:      repo.Root.Inode,
			Mode:       repo.Root.Mode,
			UID:        repo.Root.UID,
			GID:        repo.Root.GID,
			NLink:      repo.Root.NLink,
			CreatedAt:  repo.Root.CreatedAt,
			ModifiedAt: repo.Root.ModifiedAt,
			AccessedAt: repo.Root.AccessedAt,
			ChangedAt:  repo.Root.ChangedAt,
			XAttrs:     CloneStringMap(repo.Root.XAttrs),
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
		return shfs.EntryInfoFromFile(file), nil
	}
	return shfs.EntryInfoFromDirectory(dir), nil
}

func UpdateFileFamily(repo *meta.RepoMetadata, inode uint64, mutate func(*meta.FileMetadata)) error {
	files := repo.FindFilesByInode(inode)
	if len(files) == 0 {
		return fmt.Errorf("inode family not found: %d", inode)
	}
	updated := make([]meta.FileMetadata, 0, len(files))
	for _, file := range files {
		clone := file.Clone()
		mutate(&clone)
		updated = append(updated, clone)
	}
	for _, file := range files {
		repo.RemoveFile(file.Name)
	}
	for _, file := range updated {
		repo.UpsertFile(file, file.ChangedAt)
	}
	return nil
}

func TouchInodeFamilyChangedAt(repo *meta.RepoMetadata, inode uint64, now time.Time) error {
	return UpdateFileFamily(repo, inode, func(current *meta.FileMetadata) {
		current.ChangedAt = now
	})
}
