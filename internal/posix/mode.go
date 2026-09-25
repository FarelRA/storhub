package posix

import (
	"context"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// unixNano converts t to stored nanos, saturating outside the int64 window.
// time.UnixNano wraps silently there; a v1 document can legally carry
// pre-1678 or far-future stamps, so store the saturated bound instead of a
// wrapped value Validate would then chase. Mirrors metadata timeToUnix.
func unixNano(t time.Time) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1
	const nanosPerSecond = int64(1_000_000_000)
	if sec := t.Unix(); sec > maxInt64/nanosPerSecond {
		return maxInt64
	} else if sec < minInt64/nanosPerSecond {
		return minInt64
	}
	return t.UnixNano()
}

// authorizeMode checks a mode change against the live entry and returns the
// sanitized mode. Single spelling shared by the chmod verb and the metadata
// patch path; never re-spell the sanitizer at call sites.
func authorizeMode(ctx context.Context, live *shfs.EntryInfo, mode uint32) (uint32, error) {
	if err := shfs.CanChmod(ctx, live); err != nil {
		return 0, err
	}
	return shfs.SanitizeChmodMode(ctx, live, mode), nil
}

// authorizeOwner checks an owner change against the live entry. Single
// spelling shared by the chown verb and the metadata patch path.
func authorizeOwner(ctx context.Context, live *shfs.EntryInfo, uid, gid uint32) error {
	return shfs.CanChown(ctx, live, uid, gid)
}

// authorizeTimes checks a timestamp change against the live entry. Single
// spelling shared by the chtimes verbs and the metadata patch path.
func authorizeTimes(ctx context.Context, live *shfs.EntryInfo, atime, mtime *time.Time, now int64) error {
	return shfs.CanSetTimesValues(ctx, live, atime, mtime, now)
}

// applyOwner mutates one file entry's owner, clearing setuid+setgid for
// unprivileged callers only (Admin is the CAP_FSETID equivalent and keeps
// them). Single spelling shared by the chown verb and the patch path.
func applyOwner(ctx context.Context, current *meta.FileMeta, uid, gid uint32) {
	if uid != shfs.KeepOwnerID {
		current.UID = uid
	}
	if gid != shfs.KeepOwnerID {
		current.GID = gid
	}
	if !shfs.IdentityFromContext(ctx).Admin {
		current.Mode &^= 0o6000
	}
}

// applyOwnerDir mutates one directory entry's owner under the same rule.
func applyOwnerDir(ctx context.Context, dir *meta.DirMeta, uid, gid uint32) {
	if uid != shfs.KeepOwnerID {
		dir.UID = uid
	}
	if gid != shfs.KeepOwnerID {
		dir.GID = gid
	}
	if !shfs.IdentityFromContext(ctx).Admin {
		dir.Mode &^= 0o6000
	}
}

// applyTimes mutates one file entry's timestamps; nil omits (UTIME_OMIT).
// Single spelling shared by the explicit chtimes verb and the patch path.
func applyTimes(current *meta.FileMeta, atime, mtime *time.Time) {
	if atime != nil {
		current.AccessedAt = unixNano(*atime)
	}
	if mtime != nil {
		current.ModifiedAt = unixNano(*mtime)
	}
}

// applyTimesDir mutates one directory entry's timestamps under the same rule.
func applyTimesDir(dir *meta.DirMeta, atime, mtime *time.Time) {
	if atime != nil {
		dir.AccessedAt = unixNano(*atime)
	}
	if mtime != nil {
		dir.ModifiedAt = unixNano(*mtime)
	}
}

// patchTimes maps a MetadataPatch onto Explicit trinary pointers. HasTimes
// gates; IsZero means omit today. That is a value-type limitation: the epoch
// itself is unstorable via patch until fs.MetadataPatch moves to *time.Time.
// Centralized here so the migration touches one site.
func patchTimes(patch shfs.MetadataPatch) (atime, mtime *time.Time) {
	if !patch.HasTimes {
		return nil, nil
	}
	if !patch.ATime.IsZero() {
		t := patch.ATime
		atime = &t
	}
	if !patch.MTime.IsZero() {
		t := patch.MTime
		mtime = &t
	}
	return atime, mtime
}

// ChmodContext changes the mode of targetPath.
func (s *Service) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) (err error) {
	err = s.withOp(project, "chmod", []any{"path", targetPath, "mode", mode}, func() error {
		entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
		mode, err = authorizeMode(ctx, entry, mode)
		if err != nil {
			return err
		}
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				sanitized, err := authorizeMode(ctx, current, mode)
				mode = sanitized
				return err
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
		if err := authorizeOwner(ctx, entryForAccess, uid, gid); err != nil {
			return err
		}
		// POSIX chown(2): an owner value of (uid_t)-1 means "leave unchanged".
		// uid_t is unsigned, so -1's wire encoding is all-ones; accept it per
		// field. The kernel resolves these before FUSE CHOWN, so this only
		// affects direct library callers.
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				return authorizeOwner(ctx, current, uid, gid)
			}); err != nil {
				return err
			}
			// chown clears setuid+setgid for unprivileged callers only
			// (non-admin data writes clear setuid+setgid, see
			// shfs.SanitizeWrittenFileModeForContext); Admin (CAP_FSETID
			// equivalent) keeps them. POSIX clears on directories as well.
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					applyOwner(ctx, current, uid, gid)
					current.ChangedAt = now
				})
			}
			applyOwnerDir(ctx, dir, uid, gid)
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

// ChtimesContext is the legacy int64 form, kept as a thin wrapper over
// ChtimesExplicitContext (the single trinary path). Zero means "now" per
// field; use ChtimesExplicitContext when the epoch itself must be stored
// (its nil-means-omit contract keeps zero settable). REST utimes still
// decodes to the int64 form at the edge; FUSE already calls Explicit directly.
func (s *Service) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error {
	now := s.backend.Now()
	resolve := func(value int64) *time.Time {
		if value == 0 {
			t := time.Unix(0, now)
			return &t
		}
		t := time.Unix(0, value)
		return &t
	}
	return s.ChtimesExplicitContext(ctx, project, targetPath, resolve(atime), resolve(mtime))
}

// ChtimesExplicitContext sets timestamps with POSIX utimensat trinary
// semantics: a nil pointer omits that timestamp entirely (UTIME_OMIT), a
// non-nil pointer sets it to exactly that value - the epoch included.
// Provided values are marked authoritative in metadata.
func (s *Service) ChtimesExplicitContext(ctx context.Context, project, targetPath string, atime, mtime *time.Time) (err error) {
	// Log the resolved timestamp values (unix nanos, -1 when omitted), not
	// just presence flags, so the debug line carries the full op surface.
	atimeNs, mtimeNs := int64(-1), int64(-1)
	if atime != nil {
		atimeNs = unixNano(*atime)
	}
	if mtime != nil {
		mtimeNs = unixNano(*mtime)
	}
	err = s.withOp(project, "chtimes-explicit", []any{"path", targetPath, "has_atime", atime != nil, "has_mtime", mtime != nil, "atime", atimeNs, "mtime", mtimeNs}, func() error {
		entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
		// Write-only callers may omit (nil) or set "now", not arbitrary
		// timestamps.
		now := s.backend.Now()
		if err := authorizeTimes(ctx, entry, atime, mtime, now); err != nil {
			return err
		}
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			if err := tx.reauthorize(ctx, func(current *shfs.EntryInfo) error {
				return authorizeTimes(ctx, current, atime, mtime, now)
			}); err != nil {
				return err
			}
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					applyTimes(current, atime, mtime)
					current.ChangedAt = now
				})
			}
			applyTimesDir(dir, atime, mtime)
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}
