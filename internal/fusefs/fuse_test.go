package fusefs

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type stubHub struct {
	createFile      func(context.Context, string, string) (*meta.FileMeta, error)
	statPath        func(context.Context, string, string) (*shfs.EntryInfo, error)
	readDir         func(context.Context, string, string) ([]shfs.DirEntry, error)
	statFS          func(context.Context, string) (*shfs.FSStats, error)
	readFileAt      func(context.Context, string, string, int64, int64) ([]byte, error)
	getXAttr        func(context.Context, string, string, string) ([]byte, error)
	setXAttr        func(context.Context, string, string, string, []byte) error
	listXAttr       func(context.Context, string, string) ([]string, error)
	removeXAttr     func(context.Context, string, string, string) error
	readlink        func(context.Context, string, string) (string, error)
	chmod           func(context.Context, string, string, uint32) error
	applyPatch      func(context.Context, string, string, shfs.MetadataPatch) error
	truncateFile    func(context.Context, string, string, int64) (*meta.FileMeta, error)
	loadReadonly    func(context.Context, string) (*meta.RepoMetadata, string, error)
	updateMeta      func(context.Context, string, func(*meta.RepoMetadata) error, string) (*meta.RepoMetadata, error)
	renameFn        func(context.Context, string, string, string) error
	cloneFn         func(context.Context, string, string, int64, string, int64, int64) (*meta.FileMeta, error)
	replaceFile     func(context.Context, string, string, string) (*meta.FileMeta, error)
	patchFile       func(context.Context, string, string, int64, int64, []byte) (*meta.FileMeta, error)
	patchRanges     func([]shfs.RangeEdit) (*meta.FileMeta, error)
	rewriteFn       func(ctx context.Context, project, target, inputPath string) (*meta.FileMeta, error)
	downloadFile    func(context.Context, string, string, string) error
	downloads       int
	patchRangeCalls int
	patchRangeEdits [][]shfs.RangeEdit
	drainFn         func(context.Context, string) error
	drainCalls      int
	now             int64
	chunkSize       int64
}

func (s *stubHub) StatPathContext(ctx context.Context, project, target string) (*shfs.EntryInfo, error) {
	if s.statPath != nil {
		return s.statPath(ctx, project, target)
	}
	return nil, syscall.ENOENT
}

func (s *stubHub) ReadDirContext(ctx context.Context, project, target string) ([]shfs.DirEntry, error) {
	if s.readDir != nil {
		return s.readDir(ctx, project, target)
	}
	return nil, nil
}

func (s *stubHub) StatFSContext(ctx context.Context, project string) (*shfs.FSStats, error) {
	if s.statFS != nil {
		return s.statFS(ctx, project)
	}
	return &shfs.FSStats{}, nil
}

func (s *stubHub) CreateFileContext(ctx context.Context, project, target string) (*meta.FileMeta, error) {
	if s.createFile != nil {
		return s.createFile(ctx, project, target)
	}
	return nil, io.EOF
}
func (*stubHub) MkdirContext(context.Context, string, string) error                       { return nil }
func (*stubHub) UnlinkContext(context.Context, string, string) error                      { return nil }
func (*stubHub) RmdirContext(context.Context, string, string, ...shfs.MutateOption) error { return nil }
func (s *stubHub) TruncateFileContext(ctx context.Context, project, target string, size int64, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	if s.truncateFile != nil {
		return s.truncateFile(ctx, project, target, size)
	}
	return nil, nil
}
func (s *stubHub) ChmodContext(ctx context.Context, project, target string, mode uint32) error {
	if s.chmod != nil {
		return s.chmod(ctx, project, target, mode)
	}
	return nil
}
func (s *stubHub) ApplyMetadataPatchContext(ctx context.Context, project, target string, patch shfs.MetadataPatch) error {
	if s.applyPatch != nil {
		return s.applyPatch(ctx, project, target, patch)
	}
	return nil
}
func (*stubHub) ChownContext(context.Context, string, string, uint32, uint32) error { return nil }
func (*stubHub) ChtimesExplicitContext(context.Context, string, string, *time.Time, *time.Time) error {
	return nil
}
func (*stubHub) ChtimesContext(context.Context, string, string, int64, int64) error {
	return nil
}
func (*stubHub) SymlinkContext(context.Context, string, string, string) (*meta.FileMeta, error) {
	return nil, nil
}
func (s *stubHub) ReadlinkContext(ctx context.Context, project, target string) (string, error) {
	if s.readlink != nil {
		return s.readlink(ctx, project, target)
	}
	return "", nil
}
func (*stubHub) LinkContext(context.Context, string, string, string) (*meta.FileMeta, error) {
	return nil, nil
}
func (s *stubHub) GetXAttrContext(ctx context.Context, project, target, attr string) ([]byte, error) {
	if s.getXAttr != nil {
		return s.getXAttr(ctx, project, target, attr)
	}
	return nil, nil
}
func (s *stubHub) SetXAttrContext(ctx context.Context, project, target, attr string, data []byte, mode ...shfs.XAttrMode) error {
	// Mirror posix.Service.SetXAttrContext: create/replace are enforced
	// atomically against current state. This is what lets the fuse test
	// prove the mode actually reaches the hub.
	var m shfs.XAttrMode
	for _, x := range mode {
		m |= x
	}
	if m != 0 {
		_, err := s.GetXAttrContext(ctx, project, target, attr)
		exists := err == nil
		if err != nil && !errors.Is(err, shfs.ErrXAttrNotFound) {
			return err
		}
		if m&shfs.XAttrCreate != 0 && exists {
			return syscall.EEXIST
		}
		if m&shfs.XAttrReplace != 0 && !exists {
			return shfs.ErrXAttrNotFound
		}
	}
	if s.setXAttr != nil {
		return s.setXAttr(ctx, project, target, attr, data)
	}
	return nil
}
func (s *stubHub) ListXAttrContext(ctx context.Context, project, target string) ([]string, error) {
	if s.listXAttr != nil {
		return s.listXAttr(ctx, project, target)
	}
	return nil, nil
}
func (s *stubHub) RemoveXAttrContext(ctx context.Context, project, target, attr string) error {
	if s.removeXAttr != nil {
		return s.removeXAttr(ctx, project, target, attr)
	}
	return nil
}
func (s *stubHub) DownloadFileContext(ctx context.Context, project, path, dest string) error {
	if s.downloadFile != nil {
		s.downloads++
		return s.downloadFile(ctx, project, path, dest)
	}
	return nil
}
func (s *stubHub) ReadFileAtContext(ctx context.Context, project, target string, off, length int64) ([]byte, error) {
	if s.readFileAt != nil {
		return s.readFileAt(ctx, project, target, off, length)
	}
	return []byte{}, nil
}
func (s *stubHub) PatchFileContext(ctx context.Context, project, target string, off, del int64, edit []byte, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	if s.patchFile != nil {
		return s.patchFile(ctx, project, target, off, del, edit)
	}
	return nil, nil
}
func (s *stubHub) ReplaceFileContext(ctx context.Context, project, target, inputPath string, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	if s.replaceFile != nil {
		return s.replaceFile(ctx, project, target, inputPath)
	}
	return nil, nil
}
func (s *stubHub) LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error) {
	if s.loadReadonly != nil {
		return s.loadReadonly(ctx, project)
	}
	clone := meta.NewRepoMetadata(project)
	clone.RebuildIndexes()
	return clone, "sha", nil
}
func (s *stubHub) ReadPinnedFileContext(ctx context.Context, project string, _ *meta.FileMeta, _ map[int64]meta.ChunkInfo, offset, length int64) ([]byte, error) {
	if s.readFileAt != nil {
		data, err := s.readFileAt(ctx, project, "stub", offset, length)
		return data, err
	}
	return []byte{}, nil
}
func (s *stubHub) UpdateRepoMetadataContext(ctx context.Context, project string, apply func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error) {
	if s.updateMeta != nil {
		return s.updateMeta(ctx, project, apply, message)
	}
	clone := meta.NewRepoMetadata(project)
	clone.RebuildIndexes()
	if err := apply(clone); err != nil {
		return nil, err
	}
	return clone, nil
}
func (s *stubHub) RewriteFileRangesWithMetadataContext(ctx context.Context, project, target, inputPath string, _ *meta.RepoMetadata, _ *meta.FileMeta, _ int64, _ []ByteRange) (*meta.FileMeta, error) {
	if s.rewriteFn != nil {
		return s.rewriteFn(ctx, project, target, inputPath)
	}
	return nil, nil
}

func (s *stubHub) PatchFileRangesContext(_ context.Context, _, _ string, edits []shfs.RangeEdit) (*meta.FileMeta, error) {
	if s.patchRanges == nil {
		return nil, nil
	}
	out, err := s.patchRanges(edits)
	if err != nil {
		return nil, err
	}
	s.patchRangeCalls++
	s.patchRangeEdits = append(s.patchRangeEdits, edits)
	// Mirror the storage contract: the returned entry carries the batched size.
	totalDelete, totalInsert := int64(0), int64(0)
	for _, edit := range edits {
		totalDelete += edit.DeleteSize
		totalInsert += edit.Len()
	}
	base := &meta.FileMeta{Size: 0}
	if out != nil {
		base = out
	}
	base.Size += totalInsert - totalDelete
	return base, nil
}
func (s *stubHub) DrainProjectContext(ctx context.Context, project string) error {
	s.drainCalls++
	if s.drainFn != nil {
		return s.drainFn(ctx, project)
	}
	return nil
}
func (s *stubHub) Now() int64 {
	if s.now == 0 {
		return time.Now().UnixNano()
	}
	return s.now
}
func (s *stubHub) ChunkSize() int64 {
	if s.chunkSize == 0 {
		return 4
	}
	return s.chunkSize
}

// spawnFlockHolder starts a foreign process holding an exclusive flock on
// lockPath for 30 seconds and returns once the kernel lock is confirmed
// held. Skipped where the util-linux flock tool is unavailable.
func spawnFlockHolder(t *testing.T, lockPath string) int {
	t.Helper()
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock(1) unavailable; cannot simulate a foreign live holder")
	}
	cmd := exec.Command("flock", lockPath, "sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn live flock holder: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if probe, err := os.Open(lockPath); err == nil {
			flockErr := syscall.Flock(int(probe.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			_ = probe.Close()
			if flockErr != nil {
				return cmd.Process.Pid
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("foreign holder never acquired the mount lock")
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// exitedProcessPid returns the pid of a spawned-and-reaped process, so the
// pid is definitively dead when the caller plants it as a stale claim.
func exitedProcessPid(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn stale holder process: %v", err)
	}
	_ = cmd.Wait()
	return cmd.Process.Pid
}

func (s *stubHub) RenameContext(ctx context.Context, project, oldPath, newPath string, _ ...shfs.MutateOption) error {
	if s.renameFn != nil {
		return s.renameFn(ctx, project, oldPath, newPath)
	}
	return syscall.ENOSYS
}

func (s *stubHub) CloneRange(ctx context.Context, project, src string, srcOff int64, dst string, dstOff int64, length int64, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	if s.cloneFn != nil {
		return s.cloneFn(ctx, project, src, srcOff, dst, dstOff, length)
	}
	return nil, syscall.ENOSYS
}

func newPatchTestHandle(fsys *Filesystem, inode uint64, state *inodeWriteState) *storhubHandle {
	h := &storhubHandle{fs: fsys, inode: inode, id: fsys.nextHandle.Add(1), flags: syscall.O_RDWR, path: state.path, writeState: state}
	fsys.mu.Lock()
	fsys.handles[h.id] = h
	fsys.mu.Unlock()
	return h
}
