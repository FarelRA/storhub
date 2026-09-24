package posix

import (
	"context"
	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"time"
)

// ChmodContext changes the mode of targetPath.
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

// ChownContext changes the owner of targetPath.
func (s *Service) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) (err error) {
	err = s.withOp(project, "chown", []any{"path", targetPath, "uid", uid, "gid", gid}, func() error {
		entryForAccess, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
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
			// chown clears setuid+setgid for unprivileged callers only
			// (non-admin data writes clear setuid+setgid, see
			// shfs.SanitizeWrittenFileModeForContext); Admin (CAP_FSETID
			// equivalent) keeps them. POSIX clears on directories as well.
			keepBits := shfs.IdentityFromContext(ctx).Admin
			applyOwner := func(current *meta.FileMeta) {
				if uid != shfs.KeepOwnerID {
					current.UID = uid
				}
				if gid != shfs.KeepOwnerID {
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
			if uid != shfs.KeepOwnerID {
				dir.UID = uid
			}
			if gid != shfs.KeepOwnerID {
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

// ChtimesContext changes the access and modification times of targetPath.
// Zero means "now" per field (a caller with only write permission may set
// "now", not arbitrary timestamps); use ChtimesExplicitContext when the
// epoch itself must be stored (its nil-means-omit contract keeps zero
// settable).
func (s *Service) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) (err error) {
	err = s.withOp(project, "chtimes", []any{"path", targetPath, "atime", atime, "mtime", mtime}, func() error {
		entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
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
			// Resolve into locals: the parameters double as log args, so
			// the zero-means-now mapping must not mutate them.
			resolvedAtime := ChooseNonZeroTime(atime, now)
			resolvedMtime := ChooseNonZeroTime(mtime, now)
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					current.AccessedAt = resolvedAtime
					current.ModifiedAt = resolvedMtime
					current.ChangedAt = now
				})
			}
			dir.AccessedAt = resolvedAtime
			dir.ModifiedAt = resolvedMtime
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
	// Log the resolved timestamp values (unix nanos, -1 when omitted), not
	// just presence flags, so the debug line carries the full op surface.
	atimeNs, mtimeNs := int64(-1), int64(-1)
	if atime != nil {
		atimeNs = atime.UnixNano()
	}
	if mtime != nil {
		mtimeNs = mtime.UnixNano()
	}
	err = s.withOp(project, "chtimes-explicit", []any{"path", targetPath, "has_atime", atime != nil, "has_mtime", mtime != nil, "atime", atimeNs, "mtime", mtimeNs}, func() error {
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
