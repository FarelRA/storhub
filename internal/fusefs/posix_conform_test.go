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
