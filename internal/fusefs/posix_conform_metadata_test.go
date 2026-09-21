package fusefs

import (
	"context"
	"os"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

func (h *pcHub) ChmodContext(ctx context.Context, _ string, target string, mode uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return err
	}
	entry := shfs.EntryFromFile(h.fileMetaLocked(f), target, 1)
	if err := shfs.CanChmod(ctx, entry); err != nil {
		return err
	}
	f.mode = shfs.SanitizeChmodMode(ctx, entry, mode)
	f.ctime = time.Now().UnixNano()
	return nil
}

func (h *pcHub) ChownContext(ctx context.Context, _ string, target string, uid, gid uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return err
	}
	entry := shfs.EntryFromFile(h.fileMetaLocked(f), target, 1)
	if err := shfs.CanChown(ctx, entry, uid, gid); err != nil {
		return err
	}
	const keepOwner = ^uint32(0)
	if uid != keepOwner {
		f.uid = uid
	}
	if gid != keepOwner {
		f.gid = gid
	}
	// POSIX chown clears setuid/setgid, mirroring the posix service.
	f.mode &^= 0o6000
	f.ctime = time.Now().UnixNano()
	return nil
}

func (h *pcHub) ChtimesContext(ctx context.Context, _ string, target string, atime, mtime int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return err
	}
	now := time.Now().UnixNano()
	var atimePtr, mtimePtr *time.Time
	if atime != 0 {
		t := time.Unix(0, atime)
		atimePtr = &t
	}
	if mtime != 0 {
		t := time.Unix(0, mtime)
		mtimePtr = &t
	}
	entry := shfs.EntryFromFile(h.fileMetaLocked(f), target, 1)
	if err := shfs.CanSetTimesValues(ctx, entry, atimePtr, mtimePtr, now); err != nil {
		return err
	}
	if atime == 0 {
		atime = now
	}
	if mtime == 0 {
		mtime = now
	}
	f.atime = atime
	f.mtime = mtime
	f.ctime = now
	return nil
}

func (h *pcHub) ChtimesExplicitContext(ctx context.Context, _ string, target string, atime, mtime *time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return err
	}
	now := time.Now().UnixNano()
	entry := shfs.EntryFromFile(h.fileMetaLocked(f), target, 1)
	if err := shfs.CanSetTimesValues(ctx, entry, atime, mtime, now); err != nil {
		return err
	}
	// Nanosecond precision: metadata stamps are Unix nanoseconds end to
	// end, mirroring production. Sub-second input is preserved.
	if atime != nil {
		f.atime = atime.UnixNano()
	}
	if mtime != nil {
		f.mtime = mtime.UnixNano()
	}
	f.ctime = now
	return nil
}

func (h *pcHub) SymlinkContext(ctx context.Context, _ string, target, linkPath string) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if target == "" {
		return nil, syscall.EINVAL
	}
	if linkPath == "" {
		return nil, shfs.AlreadyExists(linkPath)
	}
	if _, ok := h.files[linkPath]; ok {
		return nil, shfs.AlreadyExists(linkPath)
	}
	if _, ok := h.dirs[linkPath]; ok {
		return nil, shfs.AlreadyExists(linkPath)
	}
	if _, ok := h.dirs[pcParent(linkPath)]; !ok {
		return nil, shfs.NotFound(linkPath)
	}
	now := time.Now().UnixNano()
	uid, gid := shfs.OwnerIDsForCreate(ctx, uint32(os.Getuid()), uint32(os.Getgid()))
	f := &pcFile{
		mode: 0o777, uid: uid, gid: gid,
		atime: now, mtime: now, ctime: now, inode: h.nextInode, symlink: target,
	}
	h.nextInode++
	h.files[linkPath] = f
	h.byInode[f.inode] = f
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) ReadlinkContext(_ context.Context, _ string, target string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if f, ok := h.files[target]; ok {
		if f.symlink == "" {
			return "", shfs.InvalidSymlink(target)
		}
		return f.symlink, nil
	}
	if _, ok := h.dirs[target]; ok {
		return "", shfs.InvalidSymlink(target)
	}
	return "", shfs.NotFound(target)
}

// CloneRange is unimplemented on the conformance hub: the shared table has
// no copy_file_range scenario, so ENOSYS keeps every existing outcome
// unchanged while satisfying the Hub contract.
func (h *pcHub) CloneRange(_ context.Context, _ string, _ string, _ int64, _ string, _ int64, _ int64, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	return nil, syscall.ENOSYS
}

func (h *pcHub) LinkContext(_ context.Context, _ string, _, _ string) (*meta.FileMeta, error) {
	// Hard links are not implemented by this test backend. The
	// conformance table never exercises them.
	return nil, syscall.EPERM
}

func (h *pcHub) GetXAttrContext(_ context.Context, _ string, target, attr string) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.files[target]; !ok {
		if _, ok := h.dirs[target]; !ok {
			return nil, shfs.NotFound(target)
		}
	}
	return nil, shfs.XAttrNotFound(attr)
}

func (h *pcHub) SetXAttrContext(_ context.Context, _ string, _, _ string, _ []byte, _ ...shfs.XAttrMode) error {
	return nil
}

func (h *pcHub) ListXAttrContext(_ context.Context, _ string, _ string) ([]string, error) {
	return nil, nil
}

func (h *pcHub) RemoveXAttrContext(_ context.Context, _ string, _ string, _ string) error {
	return nil
}

func (h *pcHub) ApplyMetadataPatchContext(ctx context.Context, project, target string, patch shfs.MetadataPatch) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return err
	}
	_ = project
	if patch.HasMode {
		entry := shfs.EntryFromFile(h.fileMetaLocked(f), target, 1)
		f.mode = shfs.SanitizeChmodMode(ctx, entry, patch.Mode)
	}
	if patch.HasOwner {
		f.uid = patch.UID
		f.gid = patch.GID
		f.mode &^= 0o6000
	}
	if patch.HasTimes {
		f.atime = patch.ATime.UnixNano()
		f.mtime = patch.MTime.UnixNano()
	}
	f.ctime = time.Now().UnixNano()
	return nil
}
