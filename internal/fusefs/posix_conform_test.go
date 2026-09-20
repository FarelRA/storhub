package fusefs

import (
	"context"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/FarelRA/storhub/internal/test"
)

// This file implements the POSIX conformance surface over a real FUSE
// mount. A production-faithful in-memory Hub (pcHub) backs the mount, the
// adapter (pcSurface) maps every Surface method to real syscalls on the
// mount point, and TestPosixConformFUSE runs the shared 30-scenario table
// in internal/test against it.
//
// Backend fidelity notes (mirrors production, not convenience):
//   - Data commits (replace, patch ranges, range rewrite) clear
//     setuid/setgid, exactly like the storage verbs do through
//     shfs.SanitizeWrittenFileMode.
//   - Chown clears setuid/setgid and enforces shfs.CanChown, so a
//     non-privileged caller cannot move a file to a foreign uid, exactly
//     like the posix service.
//   - Timestamps are nanosecond precision (ModifiedAt and friends are
//     Unix nanoseconds end to end), exactly like production metadata.
//   - The FUSE path exposes no per-file version or etag (the Hub interface
//     has no version query and FileMeta carries no CAS token), so Revision
//     is a best-effort content hash and CompareAndWrite is a non-atomic
//     check-then-write. See the mapping compromises in the test report.
//
// Compile-time conformance checks.
var (
	_ Hub             = (*pcHub)(nil)
	_ test.Surface    = (*pcSurface)(nil)
	_ test.PunchHoler = (*pcSurface)(nil)
	_ test.Handle     = (*pcHandle)(nil)
	_ test.SeekHandle = (*pcHandle)(nil)
	_ test.Handle     = (*pcPathHandle)(nil)
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
	// fanoutFn scripts the PublishedPathsSince journal for fan-out
	// tests (nil = capability absent, poller stays idle). Real REST
	// publishes are simulated by calling pcHub verbs directly, then
	// pointing this at the touched paths.
	fanoutFn func(since uint64) (paths []string, unknown bool, current uint64)
}

// PublishedPathsSince serves the scripted fan-out journal, or reports
// the capability absent when no script is installed.
// The test goroutine installs the script while the mount poller reads
// it, so both sides hold h.mu (test-only synchronization, mirroring the
// shared-mock backend.mu precedent).
func (h *pcHub) PublishedPathsSince(_ string, since uint64) ([]string, bool, uint64) {
	h.mu.Lock()
	fn := h.fanoutFn
	h.mu.Unlock()
	if fn == nil {
		return nil, false, since
	}
	return fn(since)
}

// setFanoutFn installs the scripted fan-out journal under h.mu; test
// goroutines must use it instead of assigning fanoutFn directly.
func (h *pcHub) setFanoutFn(fn func(since uint64) (paths []string, unknown bool, current uint64)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fanoutFn = fn
}

func newPCHub() *pcHub {
	now := time.Now().UnixNano()
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
// Like production metadata, every non-empty regular file carries chunk
// coverage for its bytes (one synthetic chunk keyed by inode); the seek
// handler resolves data extents from those descriptors, so omitting them
// would present every file as one big hole.
func (h *pcHub) fileMetaLocked(f *pcFile) *meta.FileMeta {
	size := int64(len(f.data))
	if f.symlink != "" {
		size = int64(len(f.symlink))
	}
	out := &meta.FileMeta{
		Size: size, Symlink: f.symlink, Mode: f.mode, UID: f.uid, GID: f.gid,
		Inode: f.inode, UploadedAt: f.ctime, ModifiedAt: f.mtime,
		AccessedAt: f.atime, ChangedAt: f.ctime,
	}
	if f.symlink == "" && len(f.data) > 0 {
		out.Chunks = []int64{int64(f.inode)}
	}
	return out
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
	now := time.Now().UnixNano()
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
		// Register the synthetic chunk recorded by fileMetaLocked so
		// open-time pinning resolves the same coverage production
		// serves (PutChunk errors only on negative fields, impossible
		// for a synthetic [0, len) span, so the error is ignored).
		if f.symlink == "" && len(f.data) > 0 {
			_ = repo.PutChunk(int64(f.inode), meta.ChunkInfo{
				Offset: 0, Size: int64(len(f.data)), Release: "pc", AssetID: 1,
			})
		}
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
	now := time.Now().UnixNano()
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
	now := time.Now().UnixNano()
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

func (h *pcHub) DrainProjectContext(context.Context, string) error { return nil }

func (h *pcHub) Now() int64 {
	return time.Now().UnixNano()
}

func (h *pcHub) ChunkSize() int64 {
	return 65536
}

// ---------------------------------------------------------------------------
// Adapter: test.Surface over real mount syscalls.
// ---------------------------------------------------------------------------

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
