package fusefs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestNewAppliesDefaultsAndCreatesCacheDir(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	fake := &stubHub{}
	fsys, err := New(fake, "demo-project", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	if fsys.Options().PageSize != DefaultOptions().PageSize {
		t.Fatalf("expected defaults to be applied: %+v", fsys.Options())
	}
	if fsys.RootNode() == nil || fsys.RootNode().inode != 1 {
		t.Fatal("expected root node")
	}
	if _, err := os.Stat(cacheDir); err != nil {
		t.Fatalf("expected cache dir to exist: %v", err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatalf("close should be idempotent: %v", err)
	}
	if _, err := New(fake, "bad/name", Options{CacheDir: cacheDir}); err == nil {
		t.Fatal("expected invalid project error")
	}
}

func TestReadCacheAndCleanupHelpers(t *testing.T) {
	var reads int
	fake := &stubHub{
		readFileAt: func(_ context.Context, _ string, _ string, offset, length int64) ([]byte, error) {
			reads++
			data := []byte("abcdefgh")
			if offset >= int64(len(data)) {
				return []byte{}, nil
			}
			end := offset + length
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			return append([]byte(nil), data[offset:end]...), nil
		},
	}
	cacheDir := filepath.Join(t.TempDir(), "cache")
	fsys, err := New(fake, "demo", Options{CacheDir: cacheDir, PageSize: 4, ReadAheadPages: 2, MaxCachedPages: 2, CacheTTL: time.Hour, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	fsys.rememberPath(7, "docs/file.txt")
	got, err := fsys.readCached(context.Background(), 7, 1, 5)
	if err != nil || string(got) != "bcdef" {
		t.Fatalf("unexpected cached read: %q %v", got, err)
	}
	if _, err := fsys.readCached(context.Background(), 7, 0, 4); err != nil {
		t.Fatalf("second cached read: %v", err)
	}
	if reads != 1 {
		t.Fatalf("expected cache reuse, got %d reads", reads)
	}
	fsys.mu.Lock()
	for _, elem := range fsys.pageCache {
		entry := elem.Value.(*pageCacheEntry)
		entry.expires = time.Now().Add(-time.Minute)
	}
	fsys.mu.Unlock()
	stale := filepath.Join(cacheDir, "stale.tmp")
	if err := os.WriteFile(stale, []byte("x"), 0o644); err != nil {
		t.Fatalf("write stale cache file: %v", err)
	}
	fsys.cleanupExpiredCache()
	if len(fsys.pageCache) != 0 {
		t.Fatal("expected expired cache to be cleared")
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("expected stale file cleanup, got %v", err)
	}
	fsys.evictInodeCache(7)
	if len(fsys.pageCache) != 0 {
		t.Fatal("expected inode cache eviction")
	}
}

func TestWriteStateAndRangeHelpers(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{chunkSize: 4}, "demo", Options{CacheDir: cacheDir, PageSize: 4, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	temp, err := os.CreateTemp(cacheDir, "inode-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	state := &inodeWriteState{fs: fsys, inode: 1, temp: temp, tempPath: temp.Name(), baseSize: 10, logicalSize: 10}
	state.markDirtyLocked(2, 5)
	state.markDirtyLocked(5, 8)
	if got := state.dirtyBytesLocked(); got != 6 {
		t.Fatalf("unexpected dirty bytes: %d", got)
	}
	baseTemp, err := os.CreateTemp(cacheDir, "inode-base-*")
	if err != nil {
		t.Fatalf("create base temp file: %v", err)
	}
	state.baseTemp = baseTemp
	state.baseTempPath = baseTemp.Name()
	if err := state.setSizeLocked(12); err != nil {
		t.Fatalf("grow file: %v", err)
	}
	if err := state.setSizeLocked(6); err != nil {
		t.Fatalf("shrink file: %v", err)
	}
	if state.baseSize != 10 {
		t.Fatalf("expected committed base size 10, got %d", state.baseSize)
	}
	if err := state.setSizeLocked(-1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("expected invalid size error, got %v", err)
	}
	planned := state.plannedRangesLocked()
	if len(planned) == 0 || planned[0].Start != 0 {
		t.Fatalf("unexpected planned ranges: %+v", planned)
	}
	state.dirtyRanges = []ByteRange{{Start: 0, End: 6}}
	if !state.shouldReplaceLocked([]ByteRange{{Start: 0, End: 6}}) {
		t.Fatal("expected replace heuristic to trigger")
	}
	if !state.shouldChunkRewriteLocked([]ByteRange{{Start: 0, End: 1}, {Start: 4, End: 5}}) {
		t.Fatal("expected chunk rewrite heuristic to trigger")
	}
	if merged := mergeByteRange([]ByteRange{{Start: 0, End: 2}}, ByteRange{Start: 2, End: 5}); len(merged) != 1 || merged[0].End != 5 {
		t.Fatalf("unexpected merged ranges: %+v", merged)
	}
	if total := totalByteRanges([]ByteRange{{Start: 0, End: 2}, {Start: 5, End: 7}}); total != 4 {
		t.Fatalf("unexpected total bytes: %d", total)
	}
	state.closeTemp()
	if _, err := os.Stat(temp.Name()); !os.IsNotExist(err) {
		t.Fatalf("expected temp cleanup, got %v", err)
	}
}

func TestRefreshBaseSnapshotLockedUpdatesCachedBase(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{chunkSize: 4}, "demo", Options{CacheDir: cacheDir, PageSize: 4, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	base, err := os.CreateTemp(cacheDir, "inode-base-*")
	if err != nil {
		t.Fatalf("create base temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("abXYef"), 0); err != nil {
		t.Fatalf("seed working temp: %v", err)
	}
	if _, err := base.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatalf("seed base temp: %v", err)
	}
	state := &inodeWriteState{
		fs:           fsys,
		inode:        1,
		temp:         working,
		tempPath:     working.Name(),
		baseTemp:     base,
		baseTempPath: base.Name(),
		baseSize:     6,
		logicalSize:  6,
		dirtyRanges:  []ByteRange{{Start: 2, End: 4}},
	}
	if err := state.refreshBaseSnapshotLocked(); err != nil {
		t.Fatalf("refresh base snapshot: %v", err)
	}
	got := make([]byte, 6)
	if _, err := state.baseTemp.ReadAt(got, 0); err != nil && err != io.EOF {
		t.Fatalf("read refreshed base temp: %v", err)
	}
	if string(got) != "abXYef" {
		t.Fatalf("unexpected refreshed base temp: %q", got)
	}
	state.closeTemp()
}

func TestCreateCommittedSnapshotUsesWorkingTempForFullyDirtyFile(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{chunkSize: 4}, "demo", Options{CacheDir: cacheDir, PageSize: 4, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatalf("seed working temp: %v", err)
	}
	state := &inodeWriteState{
		fs:          fsys,
		inode:       1,
		temp:        working,
		tempPath:    working.Name(),
		baseSize:    8,
		logicalSize: 6,
		dirtyRanges: []ByteRange{{Start: 0, End: 6}},
	}
	snapshotPath, err := state.createCommittedSnapshotLocked(context.Background())
	if err != nil {
		t.Fatalf("create committed snapshot: %v", err)
	}
	defer os.Remove(snapshotPath)
	got, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read committed snapshot: %v", err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("unexpected committed snapshot: %q", got)
	}
	state.closeTemp()
}

func TestCreateCommittedSnapshotUsesWorkingTempAfterTruncateToZero(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{chunkSize: 4}, "demo", Options{CacheDir: cacheDir, PageSize: 4, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatalf("seed working temp: %v", err)
	}
	state := &inodeWriteState{
		fs:                fsys,
		inode:             1,
		temp:              working,
		tempPath:          working.Name(),
		baseSize:          10,
		logicalSize:       6,
		dirtyRanges:       []ByteRange{{Start: 2, End: 6}},
		tempAuthoritative: true,
	}
	snapshotPath, err := state.createCommittedSnapshotLocked(context.Background())
	if err != nil {
		t.Fatalf("create committed snapshot after truncate: %v", err)
	}
	defer os.Remove(snapshotPath)
	got, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read committed snapshot: %v", err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("unexpected committed snapshot after truncate: %q", got)
	}
	state.closeTemp()
}

func TestCreateCommittedSnapshotZeroFillsSparseAuthoritativeTemp(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{chunkSize: 4}, "demo", Options{CacheDir: cacheDir, PageSize: 4, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("tail"), 4); err != nil {
		t.Fatalf("seed sparse working temp: %v", err)
	}
	state := &inodeWriteState{
		fs:                fsys,
		inode:             1,
		temp:              working,
		tempPath:          working.Name(),
		logicalSize:       8,
		tempAuthoritative: true,
	}
	snapshotPath, err := state.createCommittedSnapshotLocked(context.Background())
	if err != nil {
		t.Fatalf("create committed snapshot from sparse authoritative temp: %v", err)
	}
	defer os.Remove(snapshotPath)
	got, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read committed snapshot: %v", err)
	}
	want := append([]byte{0, 0, 0, 0}, []byte("tail")...)
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected sparse committed snapshot: %v", got)
	}
	state.closeTemp()
}

func TestReplaceInputPathLockedReusesWorkingTempForAuthoritativeData(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{chunkSize: 4}, "demo", Options{CacheDir: cacheDir, PageSize: 4, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	working, err := os.CreateTemp(cacheDir, "inode-working-*")
	if err != nil {
		t.Fatalf("create working temp: %v", err)
	}
	if _, err := working.WriteAt([]byte("abcdef"), 0); err != nil {
		t.Fatalf("seed working temp: %v", err)
	}
	state := &inodeWriteState{
		fs:                fsys,
		inode:             1,
		temp:              working,
		tempPath:          working.Name(),
		logicalSize:       6,
		tempAuthoritative: true,
	}
	inputPath, cleanup, err := state.replaceInputPathLocked(context.Background())
	if err != nil {
		t.Fatalf("replace input path: %v", err)
	}
	if cleanup {
		t.Fatal("expected working temp reuse without cleanup")
	}
	if inputPath != working.Name() {
		t.Fatalf("expected working temp path %q, got %q", working.Name(), inputPath)
	}
	state.closeTemp()
}

func TestSequentialReadHandleExpandsWindow(t *testing.T) {
	var lengths []int64
	fsys, err := New(&stubHub{
		chunkSize: 16,
		readFileAt: func(_ context.Context, _ string, _ string, off, length int64) ([]byte, error) {
			lengths = append(lengths, length)
			data := []byte("abcdefghijklmnop")
			if off >= int64(len(data)) {
				return []byte{}, io.EOF
			}
			end := off + length
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			return append([]byte(nil), data[off:end]...), nil
		},
	}, "demo", Options{CacheDir: t.TempDir(), PageSize: 4, ReadAheadPages: 1, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	h := &storhubHandle{fs: fsys, inode: 7, path: "demo.bin"}
	for _, off := range []int64{0, 4, 8} {
		res, errno := h.Read(context.Background(), make([]byte, 4), off)
		if errno != 0 {
			t.Fatalf("read offset %d: %v", off, errno)
		}
		buf, status := res.Bytes(make([]byte, 4))
		if status != fuse.OK || len(buf) != 4 {
			t.Fatalf("read result offset %d: %q %v", off, buf, status)
		}
	}
	if len(lengths) != 2 {
		t.Fatalf("expected 2 backend reads, got %v", lengths)
	}
	if lengths[1] != 16 {
		t.Fatalf("expected sequential window fetch of 16 bytes, got %d", lengths[1])
	}
}

func TestSequentialWriteCommitStreamsChunks(t *testing.T) {
	var uploads []meta.ChunkInfo
	var finalized bool
	fsys, err := New(&stubHub{
		chunkSize: 4,
		prepareReplace: func(_ context.Context, _ string, target string, required int) (string, string, error) {
			if target != "demo.bin" || required != 1 {
				t.Fatalf("unexpected prepare args target=%q required=%d", target, required)
			}
			return "v1", "upload", nil
		},
		uploadChunk: func(_ context.Context, _ string, releaseTag, uploadURL string, index int, offset int64, data []byte) (meta.ChunkInfo, error) {
			chunk := meta.ChunkInfo{Name: string(data), Index: index, Offset: offset, Size: int64(len(data)), Release: releaseTag, AssetID: int64(index + 1)}
			uploads = append(uploads, chunk)
			return chunk, nil
		},
		finalizeChunks: func(_ context.Context, _ string, target, releaseTag string, size int64, chunks []meta.ChunkInfo) (*meta.FileMetadata, error) {
			finalized = true
			if target != "demo.bin" || size != 10 || len(chunks) != 3 {
				t.Fatalf("unexpected finalize args target=%q size=%d chunks=%d", target, size, len(chunks))
			}
			return &meta.FileMetadata{Name: target, Size: size, Release: releaseTag, Chunks: chunks}, nil
		},
	}, "demo", Options{CacheDir: t.TempDir(), CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("abcdefghij"), 0); errno != 0 || n != 10 {
		t.Fatalf("stream write: n=%d errno=%v", n, errno)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release stream handle: %v", errno)
	}
	if !finalized {
		t.Fatal("expected streamed commit finalization")
	}
	if len(uploads) != 3 || uploads[0].Offset != 0 || uploads[1].Offset != 4 || uploads[2].Offset != 8 {
		t.Fatalf("unexpected uploaded chunks: %+v", uploads)
	}
}

func TestLockAndErrorHelpers(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir, CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	lock := fuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}
	if errno := fsys.setLock(1, 10, lock); errno != 0 {
		t.Fatalf("set first lock: %v", errno)
	}
	if errno := fsys.setLock(1, 11, fuse.FileLock{Start: 5, End: 12, Typ: syscall.F_RDLCK}); errno != syscall.EAGAIN {
		t.Fatalf("expected conflict, got %v", errno)
	}
	if !locksOverlap(lock, fuse.FileLock{Start: 9, End: 20}) {
		t.Fatal("expected lock overlap")
	}
	if segments := subtractLock(lock, fuse.FileLock{Start: 3, End: 5}); len(segments) != 2 {
		t.Fatalf("expected split lock segments, got %+v", segments)
	}
	if errno := fsys.setLock(1, 10, fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_UNLCK}); errno != 0 {
		t.Fatalf("unlock lock: %v", errno)
	}
	if !lockConflicts(lockRecord{owner: 1, lock: fuse.FileLock{Start: 0, End: 1, Typ: syscall.F_WRLCK}}, 2, fuse.FileLock{Start: 0, End: 1, Typ: syscall.F_RDLCK}) {
		t.Fatal("expected conflicting locks")
	}
	if errnoFromError(nil) != 0 || errnoFromError(context.Canceled) != syscall.EINTR || errnoFromError(context.DeadlineExceeded) != syscall.ETIMEDOUT {
		t.Fatal("unexpected context error mapping")
	}
	if errnoFromError(errors.New("already exists")) != syscall.EEXIST || errnoFromError(errors.New("xattr not found")) != syscall.ENODATA {
		t.Fatal("unexpected message-based errno mapping")
	}
	if errnoFromError(errors.New("some other issue")) != syscall.EIO {
		t.Fatal("expected default errno mapping")
	}
	if normalizedChunkSize(0) <= 0 || minInt64(1, 2) != 1 || maxInt64(1, 2) != 2 || durationPtr(time.Second) == nil {
		t.Fatal("unexpected helper values")
	}
	if err := validateProject(strings.Repeat("a", 101)); err == nil {
		t.Fatal("expected long project validation error")
	}
}

func TestFillAndNodeAttributeHelpers(t *testing.T) {
	now := time.Unix(20, 0).UTC()
	entry := &shfs.EntryInfo{Path: "docs/file.txt", Inode: 4, Size: 7, UID: 1, GID: 2, NLink: 3, Mode: 0o640, ModifiedAt: now, AccessedAt: now, ChangedAt: now, IsSymlink: true}
	var attr fuse.Attr
	fillAttr(&attr, entry)
	if attr.Ino != 4 || attr.Mode&syscall.S_IFLNK == 0 {
		t.Fatalf("unexpected filled attr: %+v", attr)
	}
	var out fuse.EntryOut
	fillEntryOut(&out, entry, DefaultOptions())
	if out.Attr.Ino != 4 {
		t.Fatalf("unexpected filled entry out: %+v", out)
	}
	file := &meta.FileMetadata{Name: "docs/file.txt", Kind: meta.NodeKindSymlink, Inode: 9, Size: 2, Mode: 0o777, UID: 1, GID: 2, NLink: 1, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now, SymlinkTarget: "target"}
	converted := entryInfoFromFile(file)
	if !converted.IsSymlink || converted.Path != file.Name {
		t.Fatalf("unexpected file conversion: %+v", converted)
	}

	var setCalls int
	fake := &stubHub{
		readDir: func(context.Context, string, string) ([]shfs.DirEntry, error) {
			return []shfs.DirEntry{{Name: "child", Path: "docs/child", Inode: 5, IsDir: true}}, nil
		},
		statFS: func(context.Context, string) (*shfs.FSStats, error) {
			return &shfs.FSStats{Inodes: 3, Bytes: 8192}, nil
		},
		getXAttr: func(context.Context, string, string, string) ([]byte, error) {
			if setCalls == 0 {
				return nil, errors.New("xattr not found")
			}
			return []byte("value"), nil
		},
		setXAttr: func(context.Context, string, string, string, []byte) error {
			setCalls++
			return nil
		},
		listXAttr: func(context.Context, string, string) ([]string, error) {
			return []string{"user.demo", "user.other"}, nil
		},
		removeXAttr: func(context.Context, string, string, string) error { return nil },
		readlink:    func(context.Context, string, string) (string, error) { return "target", nil },
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir(), CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	dirNode := &storhubNode{fs: fsys, inode: 1, isDir: true}
	stream, errno := dirNode.Readdir(context.Background())
	if errno != 0 || stream == nil {
		t.Fatalf("unexpected readdir result: %v %v", stream, errno)
	}
	var statOut fuse.StatfsOut
	if errno := dirNode.Statfs(context.Background(), &statOut); errno != 0 || statOut.Files != 3 {
		t.Fatalf("unexpected statfs output: %+v errno=%v", statOut, errno)
	}
	node := &storhubNode{fs: fsys, inode: 9, kind: meta.NodeKindSymlink}
	fsys.rememberPath(9, "docs/file.txt")
	if _, errno := node.Getxattr(context.Background(), "user.demo", nil); errno != syscall.ENODATA {
		t.Fatalf("expected missing xattr errno, got %v", errno)
	}
	if errno := node.Setxattr(context.Background(), "user.demo", []byte("value"), xattrReplace); errno != syscall.ENODATA {
		t.Fatalf("expected replace on missing attr to fail, got %v", errno)
	}
	if errno := node.Setxattr(context.Background(), "user.demo", []byte("value"), 0); errno != 0 {
		t.Fatalf("unexpected setxattr error: %v", errno)
	}
	if size, errno := node.Getxattr(context.Background(), "user.demo", nil); errno != 0 || size != uint32(len("value")) {
		t.Fatalf("unexpected getxattr size probe: %d errno=%v", size, errno)
	}
	buf := make([]byte, 8)
	if size, errno := node.Getxattr(context.Background(), "user.demo", buf); errno != 0 || string(buf[:size]) != "value" {
		t.Fatalf("unexpected getxattr payload: %q errno=%v", buf[:size], errno)
	}
	if _, errno := node.Getxattr(context.Background(), "user.demo", make([]byte, 2)); errno != syscall.ERANGE {
		t.Fatalf("expected small getxattr buffer error, got %v", errno)
	}
	if errno := node.Setxattr(context.Background(), "user.demo", []byte("value"), xattrCreate); errno != syscall.EEXIST {
		t.Fatalf("expected create on existing attr to fail, got %v", errno)
	}
	if size, errno := node.Listxattr(context.Background(), nil); errno != 0 || size == 0 {
		t.Fatalf("unexpected listxattr size: %d errno=%v", size, errno)
	}
	if _, errno := node.Listxattr(context.Background(), make([]byte, 2)); errno != syscall.ERANGE {
		t.Fatalf("expected small listxattr buffer error, got %v", errno)
	}
	listBuf := make([]byte, 32)
	if _, errno := node.Listxattr(context.Background(), listBuf); errno != 0 {
		t.Fatalf("unexpected listxattr error: %v", errno)
	}
	if errno := node.Removexattr(context.Background(), "user.demo"); errno != 0 {
		t.Fatalf("unexpected removexattr error: %v", errno)
	}
	if target, errno := node.Readlink(context.Background()); errno != 0 || string(target) != "target" {
		t.Fatalf("unexpected readlink: %q errno=%v", target, errno)
	}
}

func TestRenameWithReplaceFilePath(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	metaState := meta.NewRepoMetadata("demo")
	metaState.EnsureDirectory("docs", now)
	metaState.UpsertFile(meta.FileMetadata{Name: "docs/old.txt", Release: "v1", Size: 1, Chunks: []meta.ChunkInfo{{Index: 0, Offset: 0, Size: 1, Release: "v1", AssetID: 1}}}, now)
	fake := &stubHub{
		now: now,
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			file := metaState.FindFile(target)
			if file == nil {
				return nil, syscall.ENOENT
			}
			return entryInfoFromFile(file), nil
		},
		loadReadonly: func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
			clone := metaState.Clone()
			clone.RebuildIndexes()
			return &clone, "sha", nil
		},
		updateMeta: func(_ context.Context, _ string, apply func(*meta.RepoMetadata) error, _ string) (*meta.RepoMetadata, error) {
			clone := metaState.Clone()
			clone.RebuildIndexes()
			if err := apply(&clone); err != nil {
				return nil, err
			}
			metaState = &clone
			metaState.RebuildIndexes()
			return metaState, nil
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir(), CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	old := metaState.FindFile("docs/old.txt")
	fsys.rememberPath(old.Inode, old.Name)
	if err := fsys.renameWithReplace(context.Background(), "docs/old.txt", "docs/new.txt", 0); err != nil {
		t.Fatalf("rename file: %v", err)
	}
	if metaState.FindFile("docs/new.txt") == nil || metaState.FindFile("docs/old.txt") != nil {
		t.Fatalf("expected metadata rename, got %+v", metaState.AllFiles())
	}
	if got := fsys.pathForInode(old.Inode); got != "docs/new.txt" {
		t.Fatalf("expected inode path remap, got %q", got)
	}
	if err := fsys.renameWithReplace(context.Background(), "docs/new.txt", "docs/newer.txt", renameNoReplace); err != nil {
		t.Fatalf("rename into empty destination: %v", err)
	}
}

func TestCreateBootstrapsWritableHandleWithoutRestat(t *testing.T) {
	now := time.Unix(30, 0).UTC()
	var replacedPath string
	var replacedBytes []byte
	fake := &stubHub{
		createFile: func(_ context.Context, _ string, target string) (*meta.FileMetadata, error) {
			return &meta.FileMetadata{
				Name:       target,
				Kind:       meta.NodeKindFile,
				Inode:      8,
				Mode:       0o644,
				UID:        1000,
				GID:        1000,
				NLink:      1,
				UploadedAt: now,
				ModifiedAt: now,
				AccessedAt: now,
				ChangedAt:  now,
			}, nil
		},
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			switch target {
			case "docs":
				return &shfs.EntryInfo{Path: "docs", Inode: 2, IsDir: true, Mode: 0o755, UID: 1000, GID: 1000, NLink: 2, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			case "docs/new.txt":
				return nil, syscall.ENOENT
			default:
				return nil, syscall.ENOENT
			}
		},
		replaceFile: func(_ context.Context, _ string, target, inputPath string) (*meta.FileMetadata, error) {
			data, err := os.ReadFile(inputPath)
			if err != nil {
				return nil, err
			}
			replacedPath = target
			replacedBytes = data
			return &meta.FileMetadata{Name: target, Kind: meta.NodeKindFile, Inode: 8, Size: int64(len(data)), Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		uploadChunk: func(_ context.Context, _ string, releaseTag, _ string, index int, offset int64, data []byte) (meta.ChunkInfo, error) {
			return meta.ChunkInfo{Name: string(data), Index: index, Offset: offset, Size: int64(len(data)), Release: releaseTag}, nil
		},
		finalizeChunks: func(_ context.Context, _ string, target, _ string, size int64, chunks []meta.ChunkInfo) (*meta.FileMetadata, error) {
			replacedPath = target
			buf := make([]byte, size)
			for _, chunk := range chunks {
				copy(buf[chunk.Offset:], chunk.Name)
			}
			replacedBytes = buf
			return &meta.FileMetadata{Name: target, Kind: meta.NodeKindFile, Inode: 8, Size: size, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir(), CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	dirNode := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs", Inode: 2, IsDir: true, Mode: 0o755, UID: 1000, GID: 1000, NLink: 2, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	var out fuse.EntryOut
	_, handleAny, _, errno := dirNode.Create(context.Background(), "new.txt", syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL, 0o644, &out)
	if errno != 0 {
		t.Fatalf("create file: %v", errno)
	}
	handle := handleAny.(*storhubHandle)
	if written, errno := handle.Write(context.Background(), []byte("hello"), 0); errno != 0 || written != 5 {
		t.Fatalf("write created file: written=%d errno=%v", written, errno)
	}
	if errno := handle.Fsync(context.Background(), 0); errno != 0 {
		t.Fatalf("fsync created file: %v", errno)
	}
	if replacedPath != "docs/new.txt" {
		t.Fatalf("unexpected replace path: %q", replacedPath)
	}
	if string(replacedBytes) != "hello" {
		t.Fatalf("unexpected replace payload: %q", replacedBytes)
	}
	if errno := handle.Release(context.Background()); errno != 0 {
		t.Fatalf("release created file: %v", errno)
	}
}

func TestCreateIgnoresModeAdjustmentRoundTrip(t *testing.T) {
	now := time.Unix(31, 0).UTC()
	chmodCalled := false
	fake := &stubHub{
		createFile: func(_ context.Context, _ string, target string) (*meta.FileMetadata, error) {
			return &meta.FileMetadata{
				Name:       target,
				Kind:       meta.NodeKindFile,
				Inode:      9,
				Mode:       0o644,
				UID:        1000,
				GID:        1000,
				NLink:      1,
				UploadedAt: now,
				ModifiedAt: now,
				AccessedAt: now,
				ChangedAt:  now,
			}, nil
		},
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			switch target {
			case "docs":
				return &shfs.EntryInfo{Path: "docs", Inode: 2, IsDir: true, Mode: 0o755, UID: 1000, GID: 1000, NLink: 2, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			case "docs/new.txt":
				return &shfs.EntryInfo{Path: target, Inode: 9, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			default:
				return nil, syscall.ENOENT
			}
		},
		replaceFile: func(_ context.Context, _ string, target, inputPath string) (*meta.FileMetadata, error) {
			return &meta.FileMetadata{Name: target, Kind: meta.NodeKindFile, Inode: 9, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		chmod: func(_ context.Context, _ string, _ string, _ uint32) error {
			chmodCalled = true
			return nil
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir(), CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	dirNode := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs", Inode: 2, IsDir: true, Mode: 0o755, UID: 1000, GID: 1000, NLink: 2, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	var out fuse.EntryOut
	_, handleAny, _, errno := dirNode.Create(context.Background(), "new.txt", syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL, 0o664, &out)
	if errno != 0 {
		t.Fatalf("create file: %v", errno)
	}
	if chmodCalled {
		t.Fatal("expected create to skip chmod round-trip")
	}
	if errno := handleAny.(*storhubHandle).Release(context.Background()); errno != 0 {
		t.Fatalf("release created file: %v", errno)
	}
}

func TestSafeNotifyDeleteDoesNotBlockCaller(t *testing.T) {
	oldNotifyDelete := notifyDeleteFunc
	t.Cleanup(func() { notifyDeleteFunc = oldNotifyDelete })
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	notifyDeleteFunc = func(parent *storhubNode, name string, child *storhubNode) {
		started <- struct{}{}
		<-release
	}
	parent := &storhubNode{}
	child := &storhubNode{}
	done := make(chan struct{})
	go func() {
		safeNotifyDelete(parent, "swap", child)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("safeNotifyDelete blocked caller")
	}
	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("safeNotifyDelete did not dispatch notification")
	}
	close(release)
}

func TestSafeNotifyEntryDoesNotBlockCaller(t *testing.T) {
	oldNotifyEntry := notifyEntryFunc
	t.Cleanup(func() { notifyEntryFunc = oldNotifyEntry })
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	notifyEntryFunc = func(node *storhubNode, name string) {
		started <- struct{}{}
		<-release
	}
	node := &storhubNode{}
	done := make(chan struct{})
	go func() {
		safeNotifyEntry(node, "swap")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("safeNotifyEntry blocked caller")
	}
	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("safeNotifyEntry did not dispatch notification")
	}
	close(release)
}

func TestReadIntoLockedFailsOnZeroProgressBaseRead(t *testing.T) {
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir(), CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	base, err := os.CreateTemp(t.TempDir(), "base-*")
	if err != nil {
		t.Fatalf("create base temp: %v", err)
	}
	defer os.Remove(base.Name())
	state := &inodeWriteState{fs: fsys, inode: 1, baseTemp: base, baseTempPath: base.Name(), baseSize: 4, logicalSize: 4}
	buf := make([]byte, 4)
	if _, err := state.readIntoLocked(context.Background(), buf, 0); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("expected io.ErrNoProgress, got %v", err)
	}
}

func TestSetattrWithoutHandleUsesActiveWriteState(t *testing.T) {
	now := time.Unix(40, 0).UTC()
	var truncates int
	var replaced []byte
	backendSize := int64(len("abcdefghij"))
	fake := &stubHub{
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			if target != "docs/file.txt" {
				return nil, syscall.ENOENT
			}
			return &shfs.EntryInfo{Path: target, Inode: 7, Size: backendSize, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		readFileAt: func(_ context.Context, _ string, _ string, off, length int64) ([]byte, error) {
			data := []byte("abcdefghij")
			if off >= backendSize {
				return []byte{}, nil
			}
			end := off + length
			if end > backendSize {
				end = backendSize
			}
			return append([]byte(nil), data[off:end]...), nil
		},
		truncateFile: func(_ context.Context, _ string, _ string, size int64) (*meta.FileMetadata, error) {
			truncates++
			backendSize = size
			return &meta.FileMetadata{Name: "docs/file.txt", Kind: meta.NodeKindFile, Inode: 7, Size: size, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		replaceFile: func(_ context.Context, _ string, _ string, inputPath string) (*meta.FileMetadata, error) {
			data, err := os.ReadFile(inputPath)
			if err != nil {
				return nil, err
			}
			replaced = data
			backendSize = int64(len(data))
			return &meta.FileMetadata{Name: "docs/file.txt", Kind: meta.NodeKindFile, Inode: 7, Size: int64(len(data)), Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		uploadChunk: func(_ context.Context, _ string, releaseTag, _ string, index int, offset int64, data []byte) (meta.ChunkInfo, error) {
			return meta.ChunkInfo{Name: string(data), Index: index, Offset: offset, Size: int64(len(data)), Release: releaseTag}, nil
		},
		finalizeChunks: func(_ context.Context, _ string, _ string, _ string, size int64, chunks []meta.ChunkInfo) (*meta.FileMetadata, error) {
			buf := make([]byte, size)
			for _, chunk := range chunks {
				copy(buf[chunk.Offset:], chunk.Name)
			}
			replaced = buf
			backendSize = size
			return &meta.FileMetadata{Name: "docs/file.txt", Kind: meta.NodeKindFile, Inode: 7, Size: size, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir(), CleanupInterval: time.Hour})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer fsys.Close()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs/file.txt", Inode: 7, Size: backendSize, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	hAny, _, errno := node.Open(context.Background(), syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("open write handle: %v", errno)
	}
	h := hAny.(*storhubHandle)
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = 0
	var out fuse.AttrOut
	if errno := node.Setattr(context.Background(), nil, &attr, &out); errno != 0 {
		t.Fatalf("setattr without handle: %v", errno)
	}
	if truncates != 0 {
		t.Fatalf("expected setattr to avoid backend truncate, got %d calls", truncates)
	}
	if out.Attr.Size != 0 {
		t.Fatalf("expected local setattr size 0, got %d", out.Attr.Size)
	}
	if written, errno := h.Write(context.Background(), []byte("hello"), 0); errno != 0 || written != 5 {
		t.Fatalf("write after local truncate: written=%d errno=%v", written, errno)
	}
	if errno := h.Fsync(context.Background(), 0); errno != 0 {
		t.Fatalf("fsync rewritten file: %v", errno)
	}
	if string(replaced) != "hello" {
		t.Fatalf("unexpected replace payload: %q", replaced)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release rewritten file: %v", errno)
	}
	if backendSize != 5 {
		t.Fatalf("expected backend size to update after commit, got %d", backendSize)
	}
	var getattr fuse.AttrOut
	if errno := node.Getattr(context.Background(), nil, &getattr); errno != 0 {
		t.Fatalf("getattr after commit: %v", errno)
	}
	if getattr.Attr.Size != 5 {
		t.Fatalf("expected getattr size 5 after commit, got %d", getattr.Attr.Size)
	}
}

type stubHub struct {
	createFile     func(context.Context, string, string) (*meta.FileMetadata, error)
	statPath       func(context.Context, string, string) (*shfs.EntryInfo, error)
	readDir        func(context.Context, string, string) ([]shfs.DirEntry, error)
	statFS         func(context.Context, string) (*shfs.FSStats, error)
	readFileAt     func(context.Context, string, string, int64, int64) ([]byte, error)
	getXAttr       func(context.Context, string, string, string) ([]byte, error)
	setXAttr       func(context.Context, string, string, string, []byte) error
	listXAttr      func(context.Context, string, string) ([]string, error)
	removeXAttr    func(context.Context, string, string, string) error
	readlink       func(context.Context, string, string) (string, error)
	chmod          func(context.Context, string, string, uint32) error
	truncateFile   func(context.Context, string, string, int64) (*meta.FileMetadata, error)
	loadReadonly   func(context.Context, string) (*meta.RepoMetadata, string, error)
	updateMeta     func(context.Context, string, func(*meta.RepoMetadata) error, string) (*meta.RepoMetadata, error)
	replaceFile    func(context.Context, string, string, string) (*meta.FileMetadata, error)
	prepareReplace func(context.Context, string, string, int) (string, string, error)
	uploadChunk    func(context.Context, string, string, string, int, int64, []byte) (meta.ChunkInfo, error)
	finalizeChunks func(context.Context, string, string, string, int64, []meta.ChunkInfo) (*meta.FileMetadata, error)
	fillChunk      func(context.Context, string, meta.ChunkInfo, []byte) error
	now            time.Time
	chunkSize      int64
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

func (s *stubHub) CreateFileContext(ctx context.Context, project, target string) (*meta.FileMetadata, error) {
	if s.createFile != nil {
		return s.createFile(ctx, project, target)
	}
	return nil, io.EOF
}
func (*stubHub) MkdirContext(context.Context, string, string) error  { return nil }
func (*stubHub) UnlinkContext(context.Context, string, string) error { return nil }
func (*stubHub) RmdirContext(context.Context, string, string) error  { return nil }
func (s *stubHub) TruncateFileContext(ctx context.Context, project, target string, size int64) (*meta.FileMetadata, error) {
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
func (*stubHub) ChownContext(context.Context, string, string, uint32, uint32) error { return nil }
func (*stubHub) ChtimesContext(context.Context, string, string, time.Time, time.Time) error {
	return nil
}
func (*stubHub) SymlinkContext(context.Context, string, string, string) (*meta.FileMetadata, error) {
	return nil, nil
}
func (s *stubHub) ReadlinkContext(ctx context.Context, project, target string) (string, error) {
	if s.readlink != nil {
		return s.readlink(ctx, project, target)
	}
	return "", nil
}
func (*stubHub) LinkContext(context.Context, string, string, string) (*meta.FileMetadata, error) {
	return nil, nil
}
func (s *stubHub) GetXAttrContext(ctx context.Context, project, target, attr string) ([]byte, error) {
	if s.getXAttr != nil {
		return s.getXAttr(ctx, project, target, attr)
	}
	return nil, nil
}
func (s *stubHub) SetXAttrContext(ctx context.Context, project, target, attr string, data []byte) error {
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
func (*stubHub) DownloadFileContext(context.Context, string, string, string) error { return nil }
func (s *stubHub) ReadFileAtContext(ctx context.Context, project, target string, off, length int64) ([]byte, error) {
	if s.readFileAt != nil {
		return s.readFileAt(ctx, project, target, off, length)
	}
	return []byte{}, nil
}
func (s *stubHub) PrepareReplaceContext(ctx context.Context, project, target string, required int) (string, string, error) {
	if s.prepareReplace != nil {
		return s.prepareReplace(ctx, project, target, required)
	}
	return "v1", "upload", nil
}
func (s *stubHub) UploadChunkDataContext(ctx context.Context, project, releaseTag, uploadURL string, index int, offset int64, data []byte) (meta.ChunkInfo, error) {
	if s.uploadChunk != nil {
		return s.uploadChunk(ctx, project, releaseTag, uploadURL, index, offset, data)
	}
	return meta.ChunkInfo{Index: index, Offset: offset, Size: int64(len(data)), Release: releaseTag}, nil
}
func (s *stubHub) FinalizeReplaceChunksContext(ctx context.Context, project, target, releaseTag string, size int64, chunks []meta.ChunkInfo) (*meta.FileMetadata, error) {
	if s.finalizeChunks != nil {
		return s.finalizeChunks(ctx, project, target, releaseTag, size, chunks)
	}
	return &meta.FileMetadata{Name: target, Size: size, Release: releaseTag, Chunks: chunks}, nil
}
func (s *stubHub) FillChunkRangeContext(ctx context.Context, project string, chunk meta.ChunkInfo, dst []byte) error {
	if s.fillChunk != nil {
		return s.fillChunk(ctx, project, chunk, dst)
	}
	return nil
}
func (*stubHub) PatchFileContext(context.Context, string, string, int64, int64, []byte) (*meta.FileMetadata, error) {
	return nil, nil
}
func (s *stubHub) ReplaceFileContext(ctx context.Context, project, target, inputPath string) (*meta.FileMetadata, error) {
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
func (*stubHub) RewriteFileRangesWithMetadataContext(context.Context, string, string, string, *meta.RepoMetadata, *meta.FileMetadata, int64, []ByteRange) (*meta.FileMetadata, error) {
	return nil, nil
}
func (s *stubHub) Now() time.Time {
	if s.now.IsZero() {
		return time.Now().UTC()
	}
	return s.now
}
func (s *stubHub) ChunkSize() int64 {
	if s.chunkSize == 0 {
		return 4
	}
	return s.chunkSize
}
