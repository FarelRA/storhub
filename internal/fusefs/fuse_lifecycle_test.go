package fusefs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestNewAppliesDefaultsAndCreatesCacheDir(t *testing.T) {
	t.Parallel()
	cacheDir := filepath.Join(t.TempDir(), "cache")
	fake := &stubHub{}
	fsys, err := New(fake, "demo-project", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	if fsys.Options().OverlayBufferSize != DefaultOptions().OverlayBufferSize {
		t.Fatalf("expected defaults to be applied: %+v", fsys.Options())
	}
	if got := strings.Join(fsys.Options().ExtraMountOpts, ","); got != "noatime" {
		t.Fatalf("unexpected default mount opts: %q", got)
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

func TestReleaseQuarantinesOverlayWhenCommitFails(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{
		replaceFile: func(context.Context, string, string, string) (*meta.FileMeta, error) {
			return nil, errors.New("network down")
		},
	}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("abcdefghij"), 0); errno != 0 || n != 10 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.Release(context.Background()); errno == 0 {
		t.Fatal("expected release to report the commit failure")
	}
	recovery := filepath.Join(cacheDir, "recovery")
	entries, err := os.ReadDir(recovery)
	if err != nil {
		t.Fatalf("read recovery dir: %v", err)
	}
	// Manifest sidecars (.json) describe the quarantined payload; only
	// data files count here.
	var payload string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			payload = e.Name()
		}
	}
	if payload == "" {
		t.Fatalf("expected one quarantined overlay in %s, got %v (err=%v)", recovery, entries, err)
	}
	data, err := os.ReadFile(filepath.Join(recovery, payload))
	if err != nil {
		t.Fatalf("read quarantined overlay: %v", err)
	}
	if string(data) != "abcdefghij" {
		t.Fatalf("quarantined overlay lost data: %q", data)
	}
	rootEntries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	for _, entry := range rootEntries {
		if !entry.IsDir() && entry.Name() != mountLockFileName {
			t.Fatalf("stray temp left in cache root: %s", entry.Name())
		}
	}
}

func TestCloseQuarantinesDirtyWriteStates(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	var failOnce atomic.Bool
	failOnce.Store(true)
	fsys, err := New(&stubHub{
		replaceFile: func(context.Context, string, string, string) (*meta.FileMeta, error) {
			if failOnce.Load() {
				return nil, errors.New("network down")
			}
			return &meta.FileMeta{}, nil
		},
	}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if _, errno := h.Write(context.Background(), []byte("dirty"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	// Do not Release; Close() must quarantine the dirty overlay instead of
	// silently discarding acknowledged writes.
	if err := fsys.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	recovery := filepath.Join(cacheDir, "recovery")
	entries, err := os.ReadDir(recovery)
	if err != nil {
		t.Fatalf("read recovery dir: %v", err)
	}
	var payload string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			payload = e.Name()
		}
	}
	if payload == "" {
		t.Fatalf("expected quarantined overlay after close, got %v (err=%v)", entries, err)
	}
	data, err := os.ReadFile(filepath.Join(recovery, payload))
	if err != nil || string(data) != "dirty" {
		t.Fatalf("quarantined overlay content wrong: %q (err=%v)", data, err)
	}
}

// assertCacheDirClean enforces each temp family's owner contract: once the
// owning operation finished, the cache root holds only directories
// (recovery/) and the mount lockfile - no leftover temps, because nothing
// sweeps them anymore.
func assertCacheDirClean(t *testing.T, cacheDir string) {
	t.Helper()
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == mountLockFileName {
			continue
		}
		t.Errorf("temp left behind by its owner: %s", entry.Name())
	}
}

func newMountedStubFS(t *testing.T, cacheDir string, hub Hub) *Filesystem {
	t.Helper()
	fsys, err := New(hub, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	return fsys
}

// Owner contract for handle-* temps: displacing an open readonly handle
// (unlink) materializes a private snapshot that the handle itself removes
// on Release.
func TestDisplacedReadHandleCleansUpSnapshotOnRelease(t *testing.T) {
	t.Parallel()
	now := int64(60)
	cacheDir := t.TempDir()
	hub := &stubHub{}
	hub.loadReadonly = func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
		repo := meta.NewRepoMetadata("demo")
		repo.UpsertFile("docs/file.txt", meta.FileMeta{Inode: 7, Size: 6}, now)
		repo.RebuildIndexes()
		return repo, "sha-1", nil
	}
	hub.statPath = func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
		if target == "docs/file.txt" {
			return &shfs.EntryInfo{Path: target, Inode: 7, Size: 6, Mode: 0o644, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		}
		return nil, syscall.ENOENT
	}
	hub.downloadFile = func(_ context.Context, _, _, dest string) error {
		return os.WriteFile(dest, []byte("abcdef"), 0o644)
	}
	fsys := newMountedStubFS(t, cacheDir, hub)
	docsNode := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs", IsDir: true, Inode: 2, Mode: 0o755})
	fileNode := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs/file.txt", Inode: 7, Size: 6, Mode: 0o644})
	hAny, _, errno := fileNode.Open(context.Background(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("open: %v", errno)
	}
	h := hAny.(*storhubHandle)
	if errno := docsNode.Unlink(context.Background(), "file.txt"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if h.temp == nil || !strings.HasPrefix(filepath.Base(h.tempPath), "handle-") {
		t.Fatalf("displaced handle must materialize a handle-* snapshot, got %q", h.tempPath)
	}
	buf := make([]byte, 6)
	res, errno := h.Read(context.Background(), buf, 0)
	if errno != 0 {
		t.Fatalf("read from displaced handle: %v", errno)
	}
	got, _ := res.Bytes(buf)
	if string(got) != "abcdef" {
		t.Fatalf("snapshot read returned %q, want %q", got, "abcdef")
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	assertCacheDirClean(t, cacheDir)
}

// Owner contract for inode-base-* temps: materializing a base snapshot for
// a displaced write handle is owned by the write state and removed when
// the last reference releases.
func TestUnlinkMaterializedBaseSnapshotIsCleanedByOwner(t *testing.T) {
	t.Parallel()
	now := int64(61)
	cacheDir := t.TempDir()
	hub := &stubHub{}
	hub.loadReadonly = func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
		repo := meta.NewRepoMetadata("demo")
		repo.UpsertFile("docs/file.txt", meta.FileMeta{Inode: 7, Size: 6}, now)
		repo.RebuildIndexes()
		return repo, "sha-1", nil
	}
	hub.statPath = func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
		if target == "docs/file.txt" {
			return &shfs.EntryInfo{Path: target, Inode: 7, Size: 6, Mode: 0o644, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		}
		return nil, syscall.ENOENT
	}
	hub.downloadFile = func(_ context.Context, _, _, dest string) error {
		return os.WriteFile(dest, []byte("abcdef"), 0o644)
	}
	fsys := newMountedStubFS(t, cacheDir, hub)
	docsNode := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs", IsDir: true, Inode: 2, Mode: 0o755})
	fileNode := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs/file.txt", Inode: 7, Size: 6, Mode: 0o644})
	hAny, _, errno := fileNode.Open(context.Background(), syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("open write handle: %v", errno)
	}
	h := hAny.(*storhubHandle)
	if errno := docsNode.Unlink(context.Background(), "file.txt"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if h.writeState.baseTemp == nil || !strings.HasPrefix(filepath.Base(h.writeState.baseTempPath), "inode-base-") {
		t.Fatalf("unlink must materialize an inode-base-* snapshot, got %q", h.writeState.baseTempPath)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	assertCacheDirClean(t, cacheDir)
}

// Owner contract for inode-commit-* temps: a replace commit whose working
// temp does not cover the whole file stages a commit snapshot, uploads it,
// and removes it on every exit path of the commit frame.
func TestPartialReplaceCommitCleansCommitSnapshot(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var replaced []byte
	base := []byte("ABCDEFGHIJKLMNOP")
	hub := &stubHub{}
	hub.readFileAt = func(_ context.Context, _, _ string, off, length int64) ([]byte, error) {
		end := off + length
		if end > int64(len(base)) {
			end = int64(len(base))
		}
		return append([]byte(nil), base[off:end]...), nil
	}
	hub.replaceFile = func(_ context.Context, _, _ string, inputPath string) (*meta.FileMeta, error) {
		data, err := os.ReadFile(inputPath)
		mu.Lock()
		replaced = data
		mu.Unlock()
		return &meta.FileMeta{Size: int64(len(data))}, err
	}
	cacheDir := t.TempDir()
	fsys := newMountedStubFS(t, cacheDir, hub)
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 16})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("XXXXXXXX"), 4); errno != 0 || n != 8 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	mu.Lock()
	defer mu.Unlock()
	if string(replaced) != "ABCDXXXXXXXXMNOP" {
		t.Fatalf("unexpected replace payload %q", replaced)
	}
	assertCacheDirClean(t, cacheDir)
}

// Owner contract for inode-ranges-* temps: a chunk-rewrite commit stages a
// range snapshot, hands it to the backend, and removes it even though the
// commit frame released state.mu in between.
func TestChunkRewriteCommitCleansRangeSnapshot(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var rewrittenInput string
	hub := &stubHub{chunkSize: 4}
	hub.loadReadonly = func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
		repo := meta.NewRepoMetadata("demo")
		repo.UpsertFile("ranges.bin", meta.FileMeta{Inode: 31, Size: 32}, 62)
		repo.RebuildIndexes()
		return repo, "sha-1", nil
	}
	hub.rewriteFn = func(_ context.Context, _, _, inputPath string) (*meta.FileMeta, error) {
		mu.Lock()
		rewrittenInput = inputPath
		mu.Unlock()
		return &meta.FileMeta{}, nil
	}
	cacheDir := t.TempDir()
	fsys := newMountedStubFS(t, cacheDir, hub)

	const inode = uint64(31)
	state := &inodeWriteState{fs: fsys, inode: inode, path: "ranges.bin", refs: 1}
	fsys.mu.Lock()
	fsys.writeStates[inode] = state
	fsys.mu.Unlock()
	if err := state.materializeBootstrap(32); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := state.temp.WriteAt([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"), 0); err != nil {
		t.Fatalf("seed temp: %v", err)
	}
	state.mu.Lock()
	state.baseSize = 32
	state.logicalSize = 32
	state.markDirtyLocked(4, 8)
	state.markDirtyLocked(12, 16)
	planned := state.plannedRangesLocked()
	state.mu.Unlock()
	if len(planned) != 2 || !state.shouldChunkRewriteLocked(planned) {
		t.Fatalf("expected two planned ranges on the chunk-rewrite rung, got %+v", planned)
	}
	h := newPatchTestHandle(fsys, inode, state)
	// Caller contract: hold state.mu on entry; commitChunkRewrite releases
	// it on every return path.
	state.mu.Lock()
	errno := h.commitChunkRewrite(context.Background(), "ranges.bin", 32, append([]ByteRange(nil), planned...), shfs.MetadataPatch{}, &commitNotifies{})
	if errno != 0 {
		t.Fatalf("chunk-rewrite commit: %v", errno)
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.HasPrefix(filepath.Base(rewrittenInput), "inode-ranges-") {
		t.Fatalf("backend must receive an inode-ranges-* snapshot, got %q", rewrittenInput)
	}
	state.closeTemp()
	assertCacheDirClean(t, cacheDir)
}

func TestOnForgetEvictsNodeBookkeeping(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}

	dirs := &shfs.EntryInfo{Path: "docs", IsDir: true, Inode: 2, Mode: 0o755}
	files := &shfs.EntryInfo{Path: "docs/a.txt", Inode: 3, Mode: 0o644}
	_ = fsys.EnsureNodeForTest(context.Background(), dirs)
	file := fsys.EnsureNodeForTest(context.Background(), files)
	fsys.mu.Lock()
	fsys.lockTable[3] = []lockRecord{{owner: 1}}
	fsys.mu.Unlock()

	// Superseded incarnations are left alone.
	impostor := &storhubNode{fs: fsys, inode: 3}
	impostor.OnForget()
	if fsys.nodes[3] != file {
		t.Fatal("superseding OnForget must not evict the live node")
	}

	// A real forget clears path maps and unreferenced lock records.
	file.OnForget()
	if fsys.nodes[3] != nil || fsys.pathToInode["docs/a.txt"] != 0 {
		t.Fatal("forgotten node bookkeeping survived eviction")
	}
	if _, ok := fsys.lockTable[3]; ok {
		t.Fatal("lock records for forgotten node survived")
	}
	if fsys.pathToInode["docs"] != 2 {
		t.Fatal("unrelated directory bookkeeping was disturbed")
	}

	// The root never gets evicted.
	fsys.root.OnForget()
	if fsys.nodes[1] == nil {
		t.Fatal("root node was evicted")
	}
}

func TestLastHandleReleaseDropsInodeLocks(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	h := &storhubHandle{fs: fsys, inode: 7, id: fsys.nextHandle.Add(1)}
	fsys.mu.Lock()
	fsys.handles[h.id] = h
	fsys.lockTable[7] = []lockRecord{{owner: 42, lock: fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_WRLCK}}}
	fsys.mu.Unlock()

	// POSIX: once a file has no open descriptor, no locks remain - even
	// locks recorded by owners this handle never tracked.
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	fsys.mu.RLock()
	_, ok := fsys.lockTable[7]
	alive := fsys.handles[h.id] != nil
	fsys.mu.RUnlock()
	if ok {
		t.Fatal("lock records survived the last handle release")
	}
	if alive {
		t.Fatal("released handle stayed registered")
	}
}

// TestNewSweepsStaleOverlayTemps pins the mount-start sweep: every flat
// handle-* / inode-* temp family left by a crashed previous mount is
// garbage (construction owns all future temps), while recovery/ holds
// quarantined data and must survive untouched.
func TestNewSweepsStaleOverlayTemps(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	stale := []string{
		filepath.Join(cacheDir, "handle-1234"),
		filepath.Join(cacheDir, "inode-1234"),
		filepath.Join(cacheDir, "inode-base-99"),
		filepath.Join(cacheDir, "inode-commit-7"),
		filepath.Join(cacheDir, "inode-ranges-11"),
	}
	recoveryFile := filepath.Join(cacheDir, "recovery", "keep-me")
	for _, p := range append(stale, recoveryFile) {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	for _, p := range stale {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("crash leftover %s must be swept: %v", p, err)
		}
	}
	if _, err := os.Stat(recoveryFile); err != nil {
		t.Errorf("recovery/ quarantine must survive the sweep: %v", err)
	}
}
