package posix

import (
	"context"
	"errors"
	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"sort"
	"strings"
	"syscall"
)

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

// GetXAttrContext returns the extended attribute value for attr.
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

// ListXAttrContext lists the extended attribute names of targetPath.
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

// RemoveXAttrContext removes the extended attribute attr from targetPath.
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
