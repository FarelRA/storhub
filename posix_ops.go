package storhub

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

func (h *StorHub) Symlink(project, target, linkPath string) (*FileMetadata, error) {
	return h.SymlinkContext(context.Background(), project, target, linkPath)
}

func (h *StorHub) SymlinkContext(ctx context.Context, project, target, linkPath string) (*FileMetadata, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	cleanPath, err := normalizeFSPath(linkPath)
	if err != nil {
		return nil, err
	}
	if cleanPath == "" {
		return nil, errors.New("symlink path is required")
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
	if repoMeta.FindFile(cleanPath) != nil || repoMeta.HasDirectory(cleanPath) {
		return nil, fmt.Errorf("path already exists: %s", cleanPath)
	}
	workingMeta := repoMeta.Clone()
	releaseTag, _, err := h.getOrCreateUploadRelease(ctx, project, &workingMeta, 0, "")
	if err != nil {
		return nil, err
	}
	now := h.config.Now().UTC()
	uid, gid := defaultOwnerIDs()
	symlink := FileMetadata{
		Name:          cleanPath,
		Kind:          NodeKindSymlink,
		Size:          int64(len([]byte(target))),
		Chunks:        []ChunkInfo{},
		Release:       releaseTag,
		UploadedAt:    now,
		ModifiedAt:    now,
		AccessedAt:    now,
		ChangedAt:     now,
		Mode:          defaultFileMode(NodeKindSymlink),
		UID:           uid,
		GID:           gid,
		SymlinkTarget: target,
		XAttrs:        nil,
	}
	symlink.CRC32C = sumCRC32C([]byte(target))
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if err := requireParentDirectory(meta, cleanPath); err != nil {
			return err
		}
		if meta.FindFile(cleanPath) != nil || meta.HasDirectory(cleanPath) {
			return fmt.Errorf("path already exists: %s", cleanPath)
		}
		symlink.Inode = meta.allocateInode()
		meta.UpsertFile(symlink, now)
		return nil
	}, fmt.Sprintf("storhub: symlink %s -> %s", cleanPath, target)); err != nil {
		return nil, err
	}
	return &symlink, nil
}

func (h *StorHub) Readlink(project, linkPath string) (string, error) {
	return h.ReadlinkContext(context.Background(), project, linkPath)
}

func (h *StorHub) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	cleanPath, err := normalizeFSPath(linkPath)
	if err != nil {
		return "", err
	}
	meta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return "", err
	}
	file := meta.FindFile(cleanPath)
	if file == nil {
		return "", fmt.Errorf("%w: %s", ErrFileNotFound, cleanPath)
	}
	if file.Kind != NodeKindSymlink {
		return "", fmt.Errorf("path is not a symlink: %s", cleanPath)
	}
	return file.SymlinkTarget, nil
}

func (h *StorHub) Link(project, existingPath, newPath string) (*FileMetadata, error) {
	return h.LinkContext(context.Background(), project, existingPath, newPath)
}

func (h *StorHub) LinkContext(ctx context.Context, project, existingPath, newPath string) (*FileMetadata, error) {
	sourcePath, err := normalizeFSPath(existingPath)
	if err != nil {
		return nil, err
	}
	linkPath, err := normalizeFSPath(newPath)
	if err != nil {
		return nil, err
	}
	if sourcePath == "" || linkPath == "" {
		return nil, errors.New("source and link paths are required")
	}
	if sourcePath == linkPath {
		return nil, fmt.Errorf("source and destination are the same: %s", sourcePath)
	}
	now := h.config.Now().UTC()
	var linked FileMetadata
	if _, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if err := requireParentDirectory(meta, linkPath); err != nil {
			return err
		}
		if meta.FindFile(linkPath) != nil || meta.HasDirectory(linkPath) {
			return fmt.Errorf("path already exists: %s", linkPath)
		}
		source := meta.FindFile(sourcePath)
		if source == nil {
			return fmt.Errorf("%w: %s", ErrFileNotFound, sourcePath)
		}
		if source.Kind != NodeKindFile {
			return fmt.Errorf("hard links only support regular files: %s", sourcePath)
		}
		linked = source.Clone()
		linked.Name = linkPath
		linked.ChangedAt = now
		linked.AccessedAt = now
		if err := touchInodeFamilyChangedAt(meta, source.Inode, now); err != nil {
			return err
		}
		meta.UpsertFile(linked, now)
		return nil
	}, fmt.Sprintf("storhub: link %s to %s", sourcePath, linkPath)); err != nil {
		return nil, err
	}
	return &linked, nil
}

func (h *StorHub) Chmod(project, targetPath string, mode uint32) error {
	return h.ChmodContext(context.Background(), project, targetPath, mode)
}

func (h *StorHub) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error {
	return h.updatePathMetadataContext(ctx, project, targetPath, func(meta *RepoMetadata, file *FileMetadata, dir *DirectoryMetadata) error {
		now := h.config.Now().UTC()
		if file != nil {
			return updateFileFamily(meta, file.Inode, func(current *FileMetadata) {
				current.Mode = mode
				current.ChangedAt = now
			})
		}
		dir.Mode = mode
		dir.ChangedAt = now
		return nil
	})
}

func (h *StorHub) Chown(project, targetPath string, uid, gid uint32) error {
	return h.ChownContext(context.Background(), project, targetPath, uid, gid)
}

func (h *StorHub) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error {
	return h.updatePathMetadataContext(ctx, project, targetPath, func(meta *RepoMetadata, file *FileMetadata, dir *DirectoryMetadata) error {
		now := h.config.Now().UTC()
		if file != nil {
			return updateFileFamily(meta, file.Inode, func(current *FileMetadata) {
				current.UID = uid
				current.GID = gid
				current.ChangedAt = now
			})
		}
		dir.UID = uid
		dir.GID = gid
		dir.ChangedAt = now
		return nil
	})
}

func (h *StorHub) Chtimes(project, targetPath string, atime, mtime time.Time) error {
	return h.ChtimesContext(context.Background(), project, targetPath, atime, mtime)
}

func (h *StorHub) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime time.Time) error {
	return h.updatePathMetadataContext(ctx, project, targetPath, func(meta *RepoMetadata, file *FileMetadata, dir *DirectoryMetadata) error {
		now := h.config.Now().UTC()
		atime = chooseNonZeroTime(atime, now)
		mtime = chooseNonZeroTime(mtime, now)
		if file != nil {
			return updateFileFamily(meta, file.Inode, func(current *FileMetadata) {
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

func (h *StorHub) SetXAttr(project, targetPath, attr string, data []byte) error {
	return h.SetXAttrContext(context.Background(), project, targetPath, attr, data)
}

func (h *StorHub) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte) error {
	if strings.TrimSpace(attr) == "" {
		return errors.New("xattr name is required")
	}
	value := string(append([]byte(nil), data...))
	return h.updatePathMetadataContext(ctx, project, targetPath, func(meta *RepoMetadata, file *FileMetadata, dir *DirectoryMetadata) error {
		now := h.config.Now().UTC()
		if file != nil {
			return updateFileFamily(meta, file.Inode, func(current *FileMetadata) {
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

func (h *StorHub) GetXAttr(project, targetPath, attr string) ([]byte, error) {
	return h.GetXAttrContext(context.Background(), project, targetPath, attr)
}

func (h *StorHub) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
	if strings.TrimSpace(attr) == "" {
		return nil, errors.New("xattr name is required")
	}
	entry, err := h.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	meta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	if entry.IsDir {
		dir := meta.GetDirectory(entry.Path)
		if dir == nil {
			return nil, fmt.Errorf("path not found: %s", entry.Path)
		}
		value, ok := dir.XAttrs[attr]
		if !ok {
			return nil, fmt.Errorf("xattr not found: %s", attr)
		}
		return []byte(value), nil
	}
	file := meta.FindFile(entry.Path)
	if file == nil {
		return nil, fmt.Errorf("path not found: %s", entry.Path)
	}
	value, ok := file.XAttrs[attr]
	if !ok {
		return nil, fmt.Errorf("xattr not found: %s", attr)
	}
	return []byte(value), nil
}

func (h *StorHub) ListXAttr(project, targetPath string) ([]string, error) {
	return h.ListXAttrContext(context.Background(), project, targetPath)
}

func (h *StorHub) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	meta, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath := ""
	if strings.TrimSpace(targetPath) != "" {
		cleanPath, err = normalizeFSPath(targetPath)
		if err != nil {
			return nil, err
		}
	}
	attrs := map[string]string(nil)
	if cleanPath == "" {
		attrs = meta.Root.XAttrs
	} else if dir := meta.GetDirectory(cleanPath); dir != nil {
		attrs = dir.XAttrs
	} else if file := meta.FindFile(cleanPath); file != nil {
		attrs = file.XAttrs
	} else {
		return nil, fmt.Errorf("path not found: %s", cleanPath)
	}
	keys := make([]string, 0, len(attrs))
	for key := range attrs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (h *StorHub) RemoveXAttr(project, targetPath, attr string) error {
	return h.RemoveXAttrContext(context.Background(), project, targetPath, attr)
}

func (h *StorHub) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error {
	if strings.TrimSpace(attr) == "" {
		return errors.New("xattr name is required")
	}
	return h.updatePathMetadataContext(ctx, project, targetPath, func(meta *RepoMetadata, file *FileMetadata, dir *DirectoryMetadata) error {
		now := h.config.Now().UTC()
		if file != nil {
			if _, ok := file.XAttrs[attr]; !ok {
				return fmt.Errorf("xattr not found: %s", attr)
			}
			return updateFileFamily(meta, file.Inode, func(current *FileMetadata) {
				delete(current.XAttrs, attr)
				current.ChangedAt = now
			})
		}
		if _, ok := dir.XAttrs[attr]; !ok {
			return fmt.Errorf("xattr not found: %s", attr)
		}
		delete(dir.XAttrs, attr)
		dir.ChangedAt = now
		return nil
	})
}

func (h *StorHub) updatePathMetadataContext(ctx context.Context, project, targetPath string, fn func(meta *RepoMetadata, file *FileMetadata, dir *DirectoryMetadata) error) error {
	cleanPath := ""
	if strings.TrimSpace(targetPath) != "" {
		var err error
		cleanPath, err = normalizeFSPath(targetPath)
		if err != nil {
			return err
		}
	}
	_, err := h.updateRepoMetadata(ctx, project, func(meta *RepoMetadata) error {
		if cleanPath == "" {
			tmp := &DirectoryMetadata{Path: "", CreatedAt: meta.Root.CreatedAt, ModifiedAt: meta.Root.ModifiedAt, AccessedAt: meta.Root.AccessedAt, ChangedAt: meta.Root.ChangedAt, Mode: meta.Root.Mode, UID: meta.Root.UID, GID: meta.Root.GID, Inode: meta.Root.Inode, NLink: meta.Root.NLink, XAttrs: cloneStringMap(meta.Root.XAttrs)}
			if err := fn(meta, nil, tmp); err != nil {
				return err
			}
			meta.Root.Mode = tmp.Mode
			meta.Root.UID = tmp.UID
			meta.Root.GID = tmp.GID
			meta.Root.AccessedAt = tmp.AccessedAt
			meta.Root.ModifiedAt = tmp.ModifiedAt
			meta.Root.ChangedAt = tmp.ChangedAt
			meta.Root.XAttrs = cloneStringMap(tmp.XAttrs)
			return nil
		}
		if file := meta.FindFile(cleanPath); file != nil {
			return fn(meta, file, nil)
		}
		if dir := meta.GetDirectory(cleanPath); dir != nil {
			return fn(meta, nil, dir)
		}
		return fmt.Errorf("path not found: %s", cleanPath)
	}, fmt.Sprintf("storhub: update metadata %s", cleanPath))
	return err
}

func updateFileFamily(meta *RepoMetadata, inode uint64, mutate func(*FileMetadata)) error {
	updated := false
	for i := range meta.Releases {
		for j := range meta.Releases[i].Files {
			if meta.Releases[i].Files[j].Inode != inode {
				continue
			}
			mutate(&meta.Releases[i].Files[j])
			updated = true
		}
	}
	if !updated {
		return fmt.Errorf("inode not found: %d", inode)
	}
	meta.RecomputeStats()
	return nil
}
