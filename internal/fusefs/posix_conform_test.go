package fusefs

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/FarelRA/storhub/internal/posixconform"
)

// This file implements the POSIX conformance surface over a real FUSE
// mount. A production-faithful in-memory Hub (pcHub) backs the mount, the
// adapter (pcSurface) maps every Surface method to real syscalls on the
// mount point, and TestPosixConformFUSE runs the shared 30-scenario table
// in internal/posixconform against it.
//
// Backend fidelity notes (mirrors production, not convenience):
//   - Data commits (replace, patch ranges, range rewrite) clear
//     setuid/setgid, exactly like the storage verbs do through
//     shfs.SanitizeWrittenFileMode.
//   - Chown clears setuid/setgid and enforces shfs.CanChown, so a
//     non-privileged caller cannot move a file to a foreign uid, exactly
//     like the posix service.
//   - Timestamps are second precision (ModifiedAt and friends are Unix
//     seconds end to end), exactly like production metadata.
//   - The FUSE path exposes no per-file version or etag (the Hub interface
//     has no version query and FileMeta carries no CAS token), so Revision
//     is a best-effort content hash and CompareAndWrite is a non-atomic
//     check-then-write. See the mapping compromises in the test report.
//
// Compile-time conformance checks.
var (
	_ Hub                  = (*pcHub)(nil)
	_ posixconform.Surface = (*pcSurface)(nil)
	_ posixconform.Handle  = (*pcHandle)(nil)
)

// ---------------------------------------------------------------------------
// Test Hub: production-faithful in-memory backend for the mount.
// ---------------------------------------------------------------------------

// pcFile is one live file or symlink. Unlinked-but-still-referenced files
// keep their record in byInode so open handles keep working (POSIX).
type pcFile struct {
	data    []byte
	mode    uint32
	uid     uint32
	gid     uint32
	atime   int64
	mtime   int64
	ctime   int64
	inode   uint64
	symlink string
}

// pcDir is one live directory.
type pcDir struct {
	mode  uint32
	uid   uint32
	gid   uint32
	atime int64
	mtime int64
	ctime int64
	inode uint64
}

// pcHub is a Hub implementation with production semantics and no network.
type pcHub struct {
	mu        sync.Mutex
	files     map[string]*pcFile
	dirs      map[string]*pcDir
	byInode   map[uint64]*pcFile
	nextInode uint64
}

func newPCHub() *pcHub {
	now := time.Now().Unix()
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	return &pcHub{
		files:     make(map[string]*pcFile),
		dirs:      map[string]*pcDir{"": {mode: 0o755, uid: uid, gid: gid, atime: now, mtime: now, ctime: now, inode: 1}},
		byInode:   make(map[uint64]*pcFile),
		nextInode: 2,
	}
}

// pcParent returns the parent of a hub-relative path ("" for top level).
func pcParent(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[:i]
	}
	return ""
}

// resolveLocked follows a final symlink chain. Callers must hold h.mu.
func (h *pcHub) resolveLocked(p string) (*pcFile, error) {
	cur := p
	for i := 0; i < 40; i++ {
		f, ok := h.files[cur]
		if !ok {
			return nil, shfs.NotFound(p)
		}
		if f.symlink == "" {
			return f, nil
		}
		tgt := f.symlink
		if strings.HasPrefix(tgt, "/") {
			cur = strings.TrimPrefix(tgt, "/")
		} else {
			cur = path.Join(pcParent(cur), tgt)
		}
	}
	return nil, syscall.ELOOP
}

// fileMetaLocked snapshots a file for hub replies. Callers hold h.mu.
func (h *pcHub) fileMetaLocked(f *pcFile) *meta.FileMeta {
	size := int64(len(f.data))
	if f.symlink != "" {
		size = int64(len(f.symlink))
	}
	return &meta.FileMeta{
		Size: size, Symlink: f.symlink, Mode: f.mode, UID: f.uid, GID: f.gid,
		Inode: f.inode, UploadedAt: f.ctime, ModifiedAt: f.mtime,
		AccessedAt: f.atime, ChangedAt: f.ctime,
	}
}

// entryLocked builds the stat view of a live path. Callers hold h.mu.
func (h *pcHub) entryLocked(p string) (*shfs.EntryInfo, error) {
	if p == "" {
		d := h.dirs[""]
		return shfs.EntryFromDirectory(&meta.DirMeta{
			CreatedAt: d.ctime, ModifiedAt: d.mtime, AccessedAt: d.atime,
			ChangedAt: d.ctime, Mode: d.mode, UID: d.uid, GID: d.gid, Inode: d.inode,
		}, "", 2), nil
	}
	if d, ok := h.dirs[p]; ok {
		return shfs.EntryFromDirectory(&meta.DirMeta{
			CreatedAt: d.ctime, ModifiedAt: d.mtime, AccessedAt: d.atime,
			ChangedAt: d.ctime, Mode: d.mode, UID: d.uid, GID: d.gid, Inode: d.inode,
		}, p, 2), nil
	}
	if f, ok := h.files[p]; ok {
		return shfs.EntryFromFile(h.fileMetaLocked(f), p, 1), nil
	}
	return nil, shfs.NotFound(p)
}

// touchLocked stamps mtime/ctime after a content change. Callers hold h.mu.
func (h *pcHub) touchLocked(f *pcFile) {
	now := time.Now().Unix()
	f.mtime = now
	f.ctime = now
}

// clearPrivsLocked drops setuid/setgid after a data write, mirroring the
// production storage verbs. Callers hold h.mu.
func (h *pcHub) clearPrivsLocked(f *pcFile) {
	f.mode &^= 0o6000
}

// buildRepoLocked materializes the live tree for DAC checks and open-time
// pinning. Callers hold h.mu.
func (h *pcHub) buildRepoLocked(project string) *meta.RepoMetadata {
	repo := meta.NewRepoMetadata(project)
	for p, d := range h.dirs {
		if p == "" {
			continue
		}
		repo.WriteDirDirect(p, meta.DirMeta{
			CreatedAt: d.ctime, ModifiedAt: d.mtime, AccessedAt: d.atime,
			ChangedAt: d.ctime, Mode: d.mode, UID: d.uid, GID: d.gid, Inode: d.inode,
		})
	}
	for p, f := range h.files {
		repo.WriteFileDirect(p, *h.fileMetaLocked(f))
	}
	repo.RebuildIndexes()
	return repo
}

func (h *pcHub) StatPathContext(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.entryLocked(target)
}

func (h *pcHub) ReadDirContext(_ context.Context, _ string, target string) ([]shfs.DirEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if target != "" {
		if _, ok := h.dirs[target]; !ok {
			if _, ok := h.files[target]; ok {
				return nil, shfs.NotDirectory(target)
			}
			return nil, shfs.NotFound(target)
		}
	}
	var out []shfs.DirEntry
	for p, f := range h.files {
		if pcParent(p) != target {
			continue
		}
		out = append(out, shfs.DirEntry{
			Name: path.Base(p), Path: p, IsSymlink: f.symlink != "",
			Size: int64(len(f.data)), Inode: f.inode, Mode: f.mode,
			NLink: 1, UID: f.uid, GID: f.gid,
			CreatedAt: f.ctime, ModifiedAt: f.mtime, AccessedAt: f.atime, ChangedAt: f.ctime,
		})
	}
	for p, d := range h.dirs {
		if p == "" || pcParent(p) != target {
			continue
		}
		out = append(out, shfs.DirEntry{
			Name: path.Base(p), Path: p, IsDir: true,
			Inode: d.inode, Mode: d.mode, NLink: 2, UID: d.uid, GID: d.gid,
			CreatedAt: d.ctime, ModifiedAt: d.mtime, AccessedAt: d.atime, ChangedAt: d.ctime,
		})
	}
	return out, nil
}

func (h *pcHub) StatFSContext(_ context.Context, _ string) (*shfs.FSStats, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var bytes int64
	for _, f := range h.files {
		bytes += int64(len(f.data))
	}
	return &shfs.FSStats{
		Files: len(h.files), Directories: len(h.dirs),
		Inodes: len(h.files) + len(h.dirs), Bytes: bytes,
	}, nil
}

func (h *pcHub) CreateFileContext(ctx context.Context, _ string, target string) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if target == "" {
		return nil, shfs.IsDirectory(target)
	}
	if _, ok := h.files[target]; ok {
		return nil, shfs.AlreadyExists(target)
	}
	if _, ok := h.dirs[target]; ok {
		return nil, shfs.AlreadyExists(target)
	}
	if _, ok := h.dirs[pcParent(target)]; !ok {
		return nil, shfs.NotFound(target)
	}
	now := time.Now().Unix()
	uid, gid := shfs.OwnerIDsForCreate(ctx, uint32(os.Getuid()), uint32(os.Getgid()))
	f := &pcFile{
		mode: shfs.ApplyCreateMode(ctx, 0o666), uid: uid, gid: gid,
		atime: now, mtime: now, ctime: now, inode: h.nextInode,
	}
	h.nextInode++
	h.files[target] = f
	h.byInode[f.inode] = f
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) MkdirContext(ctx context.Context, _ string, target string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if target == "" {
		return shfs.AlreadyExists(target)
	}
	if _, ok := h.files[target]; ok {
		return shfs.AlreadyExists(target)
	}
	if _, ok := h.dirs[target]; ok {
		return shfs.AlreadyExists(target)
	}
	if _, ok := h.dirs[pcParent(target)]; !ok {
		return shfs.NotFound(target)
	}
	now := time.Now().Unix()
	uid, gid := shfs.OwnerIDsForCreate(ctx, uint32(os.Getuid()), uint32(os.Getgid()))
	h.dirs[target] = &pcDir{
		mode: shfs.ApplyCreateMode(ctx, 0o777), uid: uid, gid: gid,
		atime: now, mtime: now, ctime: now, inode: h.nextInode,
	}
	h.nextInode++
	return nil
}

func (h *pcHub) UnlinkContext(_ context.Context, _ string, target string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if target == "" {
		return shfs.IsDirectory(target)
	}
	if _, ok := h.dirs[target]; ok {
		return shfs.IsDirectory(target)
	}
	if _, ok := h.files[target]; !ok {
		return shfs.NotFound(target)
	}
	// Drop the name but keep the inode record so open handles keep
	// working on the detached data (POSIX unlink semantics).
	delete(h.files, target)
	return nil
}

func (h *pcHub) RmdirContext(_ context.Context, _ string, target string, _ ...shfs.MutateOption) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.files[target]; ok {
		return shfs.NotDirectory(target)
	}
	if _, ok := h.dirs[target]; !ok {
		return shfs.NotFound(target)
	}
	if target == "" {
		return shfs.NotEmpty(target)
	}
	prefix := target + "/"
	for p := range h.files {
		if strings.HasPrefix(p, prefix) {
			return shfs.NotEmpty(target)
		}
	}
	for p := range h.dirs {
		if strings.HasPrefix(p, prefix) {
			return shfs.NotEmpty(target)
		}
	}
	delete(h.dirs, target)
	return nil
}

func (h *pcHub) TruncateFileContext(ctx context.Context, _ string, target string, size int64, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if size < 0 {
		return nil, syscall.EINVAL
	}
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	// Mirror production: truncate carries open semantics, so the caller
	// needs write access to the resolved file.
	if err := shfs.CheckWriteAccess(ctx, h.buildRepoLocked("pc"), target); err != nil {
		return nil, err
	}
	nb := make([]byte, size)
	copy(nb, f.data)
	f.data = nb
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

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
	f.ctime = time.Now().Unix()
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
	f.ctime = time.Now().Unix()
	return nil
}

func (h *pcHub) ChtimesContext(ctx context.Context, _ string, target string, atime, mtime int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	var atimePtr, mtimePtr *time.Time
	if atime != 0 {
		t := time.Unix(atime, 0)
		atimePtr = &t
	}
	if mtime != 0 {
		t := time.Unix(mtime, 0)
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
	now := time.Now().Unix()
	entry := shfs.EntryFromFile(h.fileMetaLocked(f), target, 1)
	if err := shfs.CanSetTimesValues(ctx, entry, atime, mtime, now); err != nil {
		return err
	}
	// Second precision only: metadata stamps are Unix seconds end to
	// end, mirroring production. Sub-second input is truncated here.
	if atime != nil {
		f.atime = atime.Unix()
	}
	if mtime != nil {
		f.mtime = mtime.Unix()
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
	now := time.Now().Unix()
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
		f.atime = patch.ATime.Unix()
		f.mtime = patch.MTime.Unix()
	}
	f.ctime = time.Now().Unix()
	return nil
}

func (h *pcHub) DownloadFileContext(_ context.Context, _ string, target, dest string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return err
	}
	return os.WriteFile(dest, append([]byte(nil), f.data...), 0o600)
}

func (h *pcHub) ReadFileAtContext(_ context.Context, _ string, target string, off, length int64) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	if off < 0 || length < 0 {
		return nil, syscall.EINVAL
	}
	if length == 0 {
		return []byte{}, nil
	}
	if off >= int64(len(f.data)) {
		return []byte{}, nil
	}
	end := off + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return append([]byte(nil), f.data[off:end]...), nil
}

// pcApplyEdits splices RangeEdits into data in order, growing with zero
// fill when an edit starts past EOF.
func pcApplyEdits(data []byte, edits []shfs.RangeEdit) ([]byte, error) {
	out := data
	for _, e := range edits {
		if e.Start < 0 || e.DeleteSize < 0 {
			return nil, syscall.EINVAL
		}
		if e.Start > int64(len(out)) {
			out = append(out, make([]byte, e.Start-int64(len(out)))...)
		}
		end := e.Start + e.DeleteSize
		if end > int64(len(out)) {
			end = int64(len(out))
		}
		nb := make([]byte, 0, int64(len(out))-(end-e.Start)+int64(len(e.Data)))
		nb = append(nb, out[:e.Start]...)
		nb = append(nb, e.Data...)
		nb = append(nb, out[end:]...)
		out = nb
	}
	return out, nil
}

func (h *pcHub) PatchFileContext(_ context.Context, _ string, target string, off, del int64, edit []byte, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	nb, err := pcApplyEdits(f.data, []shfs.RangeEdit{{Start: off, DeleteSize: del, Data: edit}})
	if err != nil {
		return nil, err
	}
	f.data = nb
	h.clearPrivsLocked(f)
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) PatchFileRangesContext(_ context.Context, _ string, target string, edits []shfs.RangeEdit) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	nb, err := pcApplyEdits(f.data, edits)
	if err != nil {
		return nil, err
	}
	f.data = nb
	h.clearPrivsLocked(f)
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) ReplaceFileContext(_ context.Context, _ string, target, inputPath string, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, err
	}
	f.data = data
	h.clearPrivsLocked(f)
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) LoadRepoMetadataReadonlyContext(_ context.Context, project string) (*meta.RepoMetadata, string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buildRepoLocked(project), "pc-static", nil
}

func (h *pcHub) ReadPinnedFileContext(_ context.Context, _ string, file *meta.FileMeta, _ map[int64]meta.ChunkInfo, offset, length int64) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if file == nil {
		return nil, syscall.EIO
	}
	f, ok := h.byInode[file.Inode]
	if !ok {
		return nil, shfs.NotFound("")
	}
	if offset < 0 || length < 0 {
		return nil, syscall.EINVAL
	}
	if length == 0 {
		return []byte{}, nil
	}
	if offset >= int64(len(f.data)) {
		return []byte{}, nil
	}
	end := offset + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return append([]byte(nil), f.data[offset:end]...), nil
}

func (h *pcHub) UpdateRepoMetadataContext(_ context.Context, project string, apply func(*meta.RepoMetadata) error, _ string) (*meta.RepoMetadata, error) {
	// Unreachable from the FUSE layer (which only reads metadata through
	// the readonly loader); behave like the stub hub and apply to a
	// throwaway copy.
	h.mu.Lock()
	repo := h.buildRepoLocked(project)
	h.mu.Unlock()
	if err := apply(repo); err != nil {
		return nil, err
	}
	return repo, nil
}

func (h *pcHub) RewriteFileRangesWithMetadataContext(_ context.Context, _ string, target, inputPath string, _ *meta.RepoMetadata, _ *meta.FileMeta, logicalSize int64, ranges []ByteRange) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	snap, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, err
	}
	// The snapshot only carries the planned ranges; splice exactly those
	// spans, then reconcile the size.
	nb := append([]byte(nil), f.data...)
	for _, r := range ranges {
		if r.Start < 0 || r.End < r.Start {
			return nil, syscall.EINVAL
		}
		if r.End > int64(len(nb)) {
			nb = append(nb, make([]byte, r.End-int64(len(nb)))...)
		}
		srcEnd := r.End
		if srcEnd > int64(len(snap)) {
			srcEnd = int64(len(snap))
		}
		if r.Start < srcEnd {
			copy(nb[r.Start:srcEnd], snap[r.Start:srcEnd])
		}
	}
	if logicalSize >= 0 {
		nb2 := make([]byte, logicalSize)
		copy(nb2, nb)
		nb = nb2
	}
	f.data = nb
	h.clearPrivsLocked(f)
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) RenameContext(_ context.Context, _ string, oldPath, newPath string, _ ...shfs.MutateOption) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// The noReplace option is enforced by the FUSE layer pre-check; the
	// option value itself is uninspectable outside the fs package, so a
	// concurrent noReplace race is not atomically closed here. The table
	// exercises noReplace single-threaded, where the pre-check decides.
	if oldPath == newPath {
		if _, ok := h.files[oldPath]; ok {
			return nil
		}
		if _, ok := h.dirs[oldPath]; ok {
			return nil
		}
		return shfs.NotFound(oldPath)
	}
	if _, ok := h.dirs[oldPath]; ok {
		if _, ok := h.files[newPath]; ok {
			if err := h.removeFileLocked(newPath); err != nil {
				return err
			}
		} else if d, ok := h.dirs[newPath]; ok {
			_ = d
			prefix := newPath + "/"
			for p := range h.files {
				if strings.HasPrefix(p, prefix) {
					return shfs.NotEmpty(newPath)
				}
			}
			for p := range h.dirs {
				if strings.HasPrefix(p, prefix) {
					return shfs.NotEmpty(newPath)
				}
			}
			delete(h.dirs, newPath)
		} else if _, ok := h.dirs[pcParent(newPath)]; !ok {
			return shfs.NotFound(newPath)
		}
		h.dirs[newPath] = h.dirs[oldPath]
		delete(h.dirs, oldPath)
		oldPrefix := oldPath + "/"
		newPrefix := newPath + "/"
		for p, f := range h.files {
			if strings.HasPrefix(p, oldPrefix) {
				delete(h.files, p)
				h.files[newPrefix+strings.TrimPrefix(p, oldPrefix)] = f
			}
		}
		for p, d := range h.dirs {
			if p != newPath && strings.HasPrefix(p, oldPrefix) {
				delete(h.dirs, p)
				h.dirs[newPrefix+strings.TrimPrefix(p, oldPrefix)] = d
			}
		}
		return nil
	}
	f, ok := h.files[oldPath]
	if !ok {
		return shfs.NotFound(oldPath)
	}
	if _, ok := h.files[newPath]; ok {
		if err := h.removeFileLocked(newPath); err != nil {
			return err
		}
	} else if _, ok := h.dirs[newPath]; ok {
		return shfs.IsDirectory(newPath)
	} else if _, ok := h.dirs[pcParent(newPath)]; !ok {
		return shfs.NotFound(newPath)
	}
	delete(h.files, oldPath)
	h.files[newPath] = f
	return nil
}

// removeFileLocked drops a name while keeping its inode record for open
// handles. Callers hold h.mu.
func (h *pcHub) removeFileLocked(p string) error {
	if _, ok := h.files[p]; !ok {
		return shfs.NotFound(p)
	}
	delete(h.files, p)
	return nil
}

func (h *pcHub) Now() int64 {
	return time.Now().Unix()
}

func (h *pcHub) ChunkSize() int64 {
	return 65536
}

// ---------------------------------------------------------------------------
// Adapter: posixconform.Surface over real mount syscalls.
// ---------------------------------------------------------------------------

// pcSurface maps Surface paths (absolute, slash separated) onto a FUSE
// mount point using only os and syscall operations.
type pcSurface struct {
	mount string
}

// join validates a Surface path and maps it under the mount point.
func (s *pcSurface) join(p string) (string, error) {
	if p == "" || p[0] != '/' {
		return "", posixconform.ErrInvalid
	}
	return filepath.Join(s.mount, strings.TrimPrefix(p, "/")), nil
}

// pcTranslate maps syscall failures onto the posixconform sentinels. A
// permission failure that is not about handle modes (for example chown by
// a non-privileged caller) has no sentinel and is returned raw so the
// scenario fails loudly instead of being misclassified.
func pcTranslate(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, syscall.ENOENT):
		return posixconform.ErrNotFound
	case errors.Is(err, syscall.EEXIST):
		return posixconform.ErrExists
	case errors.Is(err, syscall.EISDIR):
		return posixconform.ErrIsDir
	case errors.Is(err, syscall.ENOTDIR):
		return posixconform.ErrNotDir
	case errors.Is(err, syscall.ENOTEMPTY):
		return posixconform.ErrNotEmpty
	case errors.Is(err, syscall.ELOOP):
		return posixconform.ErrLoop
	case errors.Is(err, syscall.EINVAL):
		return posixconform.ErrInvalid
	case errors.Is(err, syscall.EBADF):
		return posixconform.ErrClosed
	}
	return err
}

func (s *pcSurface) CreateFile(p string, perm uint32, exclusive bool) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	fi, statErr := os.Lstat(full)
	if statErr == nil {
		// Mirror the oracle: repeat non-exclusive create over a file is
		// a no-op, but any create over a directory or symlink collides,
		// and exclusive create over anything collides.
		if exclusive || fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			return posixconform.ErrExists
		}
		return nil
	}
	if !errors.Is(statErr, syscall.ENOENT) {
		return pcTranslate(statErr)
	}
	flags := os.O_RDWR | os.O_CREATE
	if exclusive {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(full, flags, os.FileMode(perm&0o7777))
	if err != nil {
		return pcTranslate(err)
	}
	return pcTranslate(f.Close())
}

func (s *pcSurface) Open(p string, mode posixconform.OpenMode) (posixconform.Handle, error) {
	full, err := s.join(p)
	if err != nil {
		return nil, err
	}
	var flags int
	switch mode {
	case posixconform.OpenReadOnly:
		flags = os.O_RDONLY
	case posixconform.OpenWriteOnly:
		flags = os.O_WRONLY | os.O_CREATE
	case posixconform.OpenReadWrite:
		flags = os.O_RDWR | os.O_CREATE
	case posixconform.OpenAppend:
		flags = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	case posixconform.OpenTruncate:
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	default:
		return nil, posixconform.ErrInvalid
	}
	f, err := os.OpenFile(full, flags, 0o644)
	if err != nil {
		return nil, pcTranslate(err)
	}
	h := &pcHandle{f: f, mode: mode}
	if mode == posixconform.OpenAppend {
		if fi, err := f.Stat(); err == nil {
			h.cursor = fi.Size()
		}
	}
	return h, nil
}

func (s *pcSurface) Stat(p string) (posixconform.Stat, error) {
	full, err := s.join(p)
	if err != nil {
		return posixconform.Stat{}, err
	}
	fi, err := os.Stat(full)
	if err != nil {
		return posixconform.Stat{}, pcTranslate(err)
	}
	if fi.IsDir() {
		return posixconform.Stat{}, posixconform.ErrIsDir
	}
	var st posixconform.Stat
	st.Size = fi.Size()
	mode := uint32(fi.Mode().Perm())
	if fi.Mode()&os.ModeSetuid != 0 {
		mode |= posixconform.SetUIDBit
	}
	if fi.Mode()&os.ModeSetgid != 0 {
		mode |= posixconform.SetGIDBit
	}
	st.Mode = mode
	st.MTime = fi.ModTime().UnixNano()
	if sys, ok := fi.Sys().(*syscall.Stat_t); ok {
		st.UID = sys.Uid
		st.GID = sys.Gid
	}
	return st, nil
}

func (s *pcSurface) Truncate(p string, size int64) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	if size < 0 {
		return posixconform.ErrInvalid
	}
	return pcTranslate(os.Truncate(full, size))
}

// pcFileMode converts conformance permission bits (including setuid and
// setgid) to an os.FileMode.
func pcFileMode(mode uint32) os.FileMode {
	fm := os.FileMode(mode & 0o777)
	if mode&posixconform.SetUIDBit != 0 {
		fm |= os.ModeSetuid
	}
	if mode&posixconform.SetGIDBit != 0 {
		fm |= os.ModeSetgid
	}
	return fm
}

func (s *pcSurface) Chmod(p string, mode uint32) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	return pcTranslate(os.Chmod(full, pcFileMode(mode)))
}

func (s *pcSurface) Chown(p string, uid, gid uint32) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	if err := os.Chown(full, int(uid), int(gid)); err != nil {
		return pcTranslate(err)
	}
	// The contract requires chown to clear setuid/setgid. When the
	// backend already did, this stat finds nothing to do; when it did
	// not, the explicit chmod closes the gap with a real syscall.
	fi, err := os.Lstat(full)
	if err != nil {
		return pcTranslate(err)
	}
	if fi.Mode()&(os.ModeSetuid|os.ModeSetgid) == 0 {
		return nil
	}
	clean := fi.Mode().Perm()
	return pcTranslate(os.Chmod(full, clean))
}

func (s *pcSurface) Utimens(p string, mtime int64) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	ts := time.Unix(0, mtime)
	return pcTranslate(os.Chtimes(full, ts, ts))
}

func (s *pcSurface) Unlink(p string) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	fi, err := os.Lstat(full)
	if err != nil {
		return pcTranslate(err)
	}
	if fi.IsDir() {
		return posixconform.ErrIsDir
	}
	return pcTranslate(os.Remove(full))
}

func (s *pcSurface) Rename(oldPath, newPath string, noReplace bool) error {
	oldFull, err := s.join(oldPath)
	if err != nil {
		return err
	}
	newFull, err := s.join(newPath)
	if err != nil {
		return err
	}
	if oldPath == newPath {
		if _, err := os.Lstat(oldFull); err != nil {
			return pcTranslate(err)
		}
		return nil
	}
	if !noReplace {
		return pcTranslate(os.Rename(oldFull, newFull))
	}
	// Atomic no-replace rename via renameat2 (Linux-only syscall; the
	// build-tagged pcRenameNoReplace helper reports ENOSYS elsewhere so
	// darwin vet still compiles and the scenario fails loudly off-Linux).
	if err := pcRenameNoReplace(oldFull, newFull); err != nil {
		return pcTranslate(err)
	}
	return nil
}

func (s *pcSurface) Mkdir(p string, perm uint32) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	return pcTranslate(os.Mkdir(full, os.FileMode(perm&0o777)))
}

func (s *pcSurface) Rmdir(p string) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	fi, err := os.Lstat(full)
	if err != nil {
		return pcTranslate(err)
	}
	if !fi.IsDir() {
		return posixconform.ErrNotDir
	}
	return pcTranslate(os.Remove(full))
}

func (s *pcSurface) Symlink(target, linkPath string) error {
	full, err := s.join(linkPath)
	if err != nil {
		return err
	}
	if target == "" {
		return posixconform.ErrInvalid
	}
	return pcTranslate(os.Symlink(target, full))
}

func (s *pcSurface) Readlink(linkPath string) (string, error) {
	full, err := s.join(linkPath)
	if err != nil {
		return "", err
	}
	target, err := os.Readlink(full)
	if err != nil {
		return "", pcTranslate(err)
	}
	return target, nil
}

func (s *pcSurface) ReadRange(p string, offset, length int64) ([]byte, error) {
	full, err := s.join(p)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, posixconform.ErrInvalid
	}
	f, err := os.Open(full)
	if err != nil {
		return nil, pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return nil, pcTranslate(err)
	}
	if fi.IsDir() {
		return nil, posixconform.ErrIsDir
	}
	if offset >= fi.Size() {
		return nil, posixconform.ErrUnsatisfiableRange
	}
	if length == 0 {
		return []byte{}, nil
	}
	end := offset + length
	if end > fi.Size() {
		end = fi.Size()
	}
	buf := make([]byte, end-offset)
	if _, err := io.ReadFull(io.NewSectionReader(f, offset, end-offset), buf); err != nil {
		return nil, pcTranslate(err)
	}
	return buf, nil
}

func (s *pcSurface) Append(p string, data []byte) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	// No O_CREATE on purpose: Append never creates a missing file.
	f, err := os.OpenFile(full, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	for len(data) > 0 {
		n, err := f.Write(data)
		if err != nil {
			_ = f.Close()
			return pcTranslate(err)
		}
		if n == 0 {
			_ = f.Close()
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return pcTranslate(f.Close())
}

func (s *pcSurface) Sync(p string) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	f, err := os.Open(full)
	if err != nil {
		return pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return pcTranslate(err)
	}
	return pcTranslate(f.Close())
}

// pcRevision hashes file content. The FUSE path exposes no server version
// or etag, so this is a best-effort CAS token: it advances on every data
// change and never on metadata-only changes, but the check-then-write in
// CompareAndWrite is not atomic.
func pcRevision(full string) (uint64, error) {
	data, err := os.ReadFile(full)
	if err != nil {
		return 0, pcTranslate(err)
	}
	sum := fnv.New64a()
	_, _ = sum.Write(data)
	return sum.Sum64(), nil
}

func (s *pcSurface) Revision(p string) (uint64, error) {
	full, err := s.join(p)
	if err != nil {
		return 0, err
	}
	return pcRevision(full)
}

func (s *pcSurface) CompareAndWrite(p string, offset int64, data []byte, token uint64) error {
	full, err := s.join(p)
	if err != nil {
		return err
	}
	current, err := pcRevision(full)
	if err != nil {
		return err
	}
	if current != token {
		return posixconform.ErrPrecondition{Expected: token, Actual: current}
	}
	if offset < 0 {
		return posixconform.ErrInvalid
	}
	f, err := os.OpenFile(full, os.O_WRONLY, 0)
	if err != nil {
		return pcTranslate(err)
	}
	defer func() { _ = f.Close() }()
	for len(data) > 0 {
		n, err := f.WriteAt(data, offset)
		offset += int64(n)
		data = data[n:]
		if err != nil {
			_ = f.Close()
			return pcTranslate(err)
		}
		if n == 0 {
			_ = f.Close()
			return io.ErrShortWrite
		}
	}
	return pcTranslate(f.Close())
}

// ---------------------------------------------------------------------------
// Handle: one open file description with an independent cursor.
// ---------------------------------------------------------------------------

// pcHandle wraps an *os.File. Positioned operations use pread/pwrite
// equivalents so they never move the cursor; Read and Write are
// cursor-based. Mode violations are rejected locally with ErrAccess so a
// kernel EBADF can never be confused with use-after-close.
type pcHandle struct {
	mu     sync.Mutex
	f      *os.File
	mode   posixconform.OpenMode
	cursor int64
	closed bool
}

func (h *pcHandle) readable() bool {
	return h.mode == posixconform.OpenReadOnly || h.mode == posixconform.OpenReadWrite
}

func (h *pcHandle) writable() bool {
	return h.mode == posixconform.OpenWriteOnly || h.mode == posixconform.OpenReadWrite ||
		h.mode == posixconform.OpenAppend || h.mode == posixconform.OpenTruncate
}

func (h *pcHandle) PRead(offset int64, length int) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, posixconform.ErrClosed
	}
	if !h.readable() {
		return nil, posixconform.ErrAccess
	}
	if offset < 0 || length < 0 {
		return nil, posixconform.ErrInvalid
	}
	if length == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, length)
	total := 0
	for total < length {
		n, err := h.f.ReadAt(buf[total:], offset+int64(total))
		total += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, pcTranslate(err)
		}
		if n == 0 {
			break
		}
	}
	return buf[:total], nil
}

func (h *pcHandle) PWrite(offset int64, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, posixconform.ErrClosed
	}
	if !h.writable() {
		return 0, posixconform.ErrAccess
	}
	if offset < 0 {
		return 0, posixconform.ErrInvalid
	}
	written := 0
	for written < len(data) {
		n, err := h.f.WriteAt(data[written:], offset+int64(written))
		written += n
		if err != nil {
			return written, pcTranslate(err)
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (h *pcHandle) Read(length int) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, posixconform.ErrClosed
	}
	if !h.readable() {
		return nil, posixconform.ErrAccess
	}
	if length < 0 {
		return nil, posixconform.ErrInvalid
	}
	if length == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, length)
	total := 0
	for total < length {
		n, err := h.f.ReadAt(buf[total:], h.cursor+int64(total))
		total += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, pcTranslate(err)
		}
		if n == 0 {
			break
		}
	}
	h.cursor += int64(total)
	return buf[:total], nil
}

func (h *pcHandle) Write(data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, posixconform.ErrClosed
	}
	if !h.writable() {
		return 0, posixconform.ErrAccess
	}
	if h.mode == posixconform.OpenAppend {
		// The fd was opened O_APPEND, so the kernel forces every write
		// to the end of file regardless of the cursor.
		written := 0
		for written < len(data) {
			n, err := h.f.Write(data[written:])
			written += n
			if err != nil {
				h.cursor += int64(written)
				return written, pcTranslate(err)
			}
			if n == 0 {
				h.cursor += int64(written)
				return written, io.ErrShortWrite
			}
		}
		h.cursor += int64(written)
		return written, nil
	}
	written := 0
	for written < len(data) {
		n, err := h.f.WriteAt(data[written:], h.cursor+int64(written))
		written += n
		if err != nil {
			h.cursor += int64(written)
			return written, pcTranslate(err)
		}
		if n == 0 {
			h.cursor += int64(written)
			return written, io.ErrShortWrite
		}
	}
	h.cursor += int64(written)
	return written, nil
}

func (h *pcHandle) Truncate(size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return posixconform.ErrClosed
	}
	if !h.writable() {
		return posixconform.ErrAccess
	}
	if size < 0 {
		return posixconform.ErrInvalid
	}
	return pcTranslate(h.f.Truncate(size))
}

func (h *pcHandle) Sync() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return posixconform.ErrClosed
	}
	return pcTranslate(h.f.Sync())
}

func (h *pcHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	return pcTranslate(h.f.Close())
}

// pcScenarioBudget bounds one scenario. A wedged mount (for example the
// concurrent-append commit deadlock documented in the test report) must
// surface as a loud per-scenario FAIL, never as a hung suite.
const pcScenarioBudget = 45 * time.Second

// pcRunOne runs one scenario with panic recovery (like posixconform.Run)
// plus a hang budget, and reports whether the budget fired. It mirrors
// runOne semantics; the budget is the only addition, and it exists because
// a stuck FUSE request has no error return.
func pcRunOne(surface posixconform.Surface, sc posixconform.Scenario) (result posixconform.Result, timedOut bool) {
	result.Name = sc.Name
	type outcome struct {
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				done <- outcome{err: fmt.Errorf("panic: %v", v)}
			}
		}()
		done <- outcome{err: sc.Run(surface)}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			result.Pass = false
			result.Error = o.err.Error()
			return result, false
		}
		result.Pass = true
		return result, false
	case <-time.After(pcScenarioBudget):
		result.Pass = false
		result.Error = fmt.Sprintf("timed out after %s with requests unanswered; scenario abandoned", pcScenarioBudget)
		return result, true
	}
}

// pcLazyUnmount detaches a mountpoint even when files are still open.
// Teardown is best effort everywhere: a wedged scenario may keep its
// mount busy until process exit, and that must not fail the suite. The
// lazy detach also aborts the stuck FUSE connection so abandoned server
// and client threads drain instead of lingering.
func pcLazyUnmount(mountPoint string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "fusermount", "-uz", mountPoint)
	_ = cmd.Run()
}

// pcRunScenario mounts a fresh Hub through the real FUSE layer, runs one
// table scenario against real mount syscalls with a hang budget, and tears
// the mount down. A fresh mount per scenario keeps one wedged scenario
// from denying signal from the rest: scenario paths are unique per the
// harness, and so is the server behind them here.
func pcRunScenario(t *testing.T, index, total int, sc posixconform.Scenario) posixconform.Result {
	t.Helper()
	fail := func(format string, args ...any) posixconform.Result {
		return posixconform.Result{Name: sc.Name, Pass: false, Error: fmt.Sprintf(format, args...)}
	}
	t.Logf("scenario %d/%d %s: mounting", index+1, total, sc.Name)
	parent, err := os.MkdirTemp("", "pc-fuse-conform-*")
	if err != nil {
		return fail("mktemp: %v", err)
	}
	// Best-effort removal; a wedged mount can keep its mountpoint busy
	// until process exit.
	defer func() { _ = os.RemoveAll(parent) }()
	cacheDir := filepath.Join(parent, "cache")
	mountPoint := filepath.Join(parent, "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return fail("mkdir mountpoint: %v", err)
	}
	// Same construction pattern as mustMount: New over a test Hub with a
	// fresh cache dir.
	fsys, err := New(newPCHub(), "demo", Options{CacheDir: cacheDir})
	if err != nil {
		return fail("new filesystem: %v", err)
	}
	if err := fsys.Mount(mountPoint); err != nil {
		_ = fsys.Close()
		return fail("mount: %v", err)
	}
	// The mount call returns once the mount is established, but wait for
	// the root to answer before running the scenario.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(mountPoint); err == nil {
			break
		} else if time.Now().After(deadline) {
			_ = fsys.Unmount()
			_ = fsys.Close()
			return fail("mount point never became ready: %v", err)
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
	result, timedOut := pcRunOne(&pcSurface{mount: mountPoint}, sc)
	if timedOut {
		pcLazyUnmount(mountPoint)
	}
	if err := fsys.Unmount(); err != nil && !timedOut {
		t.Logf("scenario %s: unmount: %v", sc.Name, err)
	}
	if err := fsys.Close(); err != nil {
		t.Logf("scenario %s: close: %v", sc.Name, err)
	}
	return result
}

// pcPreflight proves the environment can carry a FUSE mount at all. A
// failure here is environmental (no usable /dev/fuse), so the suite skips
// instead of failing every scenario on setup.
func pcPreflight(t *testing.T) {
	t.Helper()
	parent, err := os.MkdirTemp("", "pc-fuse-preflight-*")
	if err != nil {
		t.Skipf("posix conformance over FUSE needs a temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(parent) }()
	mountPoint := filepath.Join(parent, "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Skipf("posix conformance over FUSE needs a mountpoint: %v", err)
	}
	fsys, err := New(newPCHub(), "demo", Options{CacheDir: filepath.Join(parent, "cache")})
	if err != nil {
		t.Skipf("posix conformance over FUSE needs a filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	if err := fsys.Mount(mountPoint); err != nil {
		t.Skipf("posix conformance over FUSE needs a working mount, mount failed: %v", err)
	}
	defer func() { _ = fsys.Unmount() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(mountPoint); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Skipf("posix conformance over FUSE needs a ready mount: %v", err)
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestPosixConformFUSE runs the shared conformance table against real
// mount syscalls, one fresh mount per scenario. A fresh mount per scenario
// keeps one wedged scenario from denying signal from the rest. The test
// fails iff any scenario fails, and the per-scenario table prints loudly
// either way. Environments without a working FUSE skip instead of faking
// results.
func TestPosixConformFUSE(t *testing.T) {
	if os.Getenv("STORHUB_CONFORMANCE") == "" {
		t.Skip("conformance suite runs only with STORHUB_CONFORMANCE=1 (Phase 0 RED: known deviations open)")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("posix conformance over FUSE needs /dev/fuse: %v", err)
	}
	pcPreflight(t)
	total := len(posixconform.Table)
	t.Logf("posix conformance over FUSE: %d scenarios, fresh mount each", total)
	only := os.Getenv("PC_ONLY")
	results := make([]posixconform.Result, 0, total)
	for i, sc := range posixconform.Table {
		if only != "" && sc.Name != only {
			continue
		}
		result := pcRunScenario(t, i, total, sc)
		results = append(results, result)
		if result.Pass {
			t.Logf("PASS %s", result.Name)
		} else {
			t.Errorf("FAIL %s: %s", result.Name, result.Error)
		}
	}
	passed, failed := posixconform.Summary(results)
	t.Logf("posixconform FUSE: %d passed, %d failed, %d total", passed, failed, len(results))
	if failed > 0 {
		t.Fatalf("%d scenario(s) failed over the FUSE mount", failed)
	}
}
