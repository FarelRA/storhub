package fusefs

// Regression tests for FUSE recovery behaviors: flush writeback, commit
// crash ordering, O_TRUNC and read serialization, cache invalidation,
// errno mapping, and crash-safe quarantine.
// Commit-on-deleted returning 0 is pinned POSIX behavior and untouched.

import (
	"context"
	"encoding/json"
	"errors"
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

// Flush must push dirty overlay data (writeback cache is enabled), not
// return success while dropping bytes on the floor.
func TestRecoveryFlushCommitsDirtyOverlay(t *testing.T) {
	t.Parallel()
	var replaceCalls int
	var replaced []byte
	hub := &stubHub{
		replaceFile: func(_ context.Context, _, _ string, inputPath string) (*meta.FileMeta, error) {
			replaceCalls++
			data, err := os.ReadFile(inputPath)
			if err != nil {
				t.Errorf("read replace input: %v", err)
				return nil, err
			}
			replaced = append([]byte(nil), data...)
			return &meta.FileMeta{Size: int64(len(data))}, nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("hello"), 0); errno != 0 || n != 5 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.Flush(context.Background()); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	if replaceCalls != 1 {
		t.Fatalf("flush must commit the dirty overlay, replace calls=%d", replaceCalls)
	}
	if string(replaced) != "hello" {
		t.Fatalf("flush committed wrong bytes: %q", replaced)
	}
	// A second flush on the now-clean handle must be a no-op, and the
	// final Release must succeed without re-uploading.
	if errno := h.Flush(context.Background()); errno != 0 {
		t.Fatalf("second flush: %v", errno)
	}
	if replaceCalls != 1 {
		t.Fatalf("clean flush must not re-upload, replace calls=%d", replaceCalls)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
}

// Flush must surface backend failures instead of swallowing them.
func TestRecoveryFlushReportsCommitFailure(t *testing.T) {
	t.Parallel()
	hub := &stubHub{
		replaceFile: func(context.Context, string, string, string) (*meta.FileMeta, error) {
			return nil, errors.New("network down")
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("dirty"), 0); errno != 0 || n != 5 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.Flush(context.Background()); errno == 0 {
		t.Fatal("flush must report the commit failure")
	}
	_ = h.Release(context.Background())
}

// A commit is not complete until the size is reconciled. If the
// post-patch truncate fails, the dirty ranges must stay dirty so the retry
// replays patch+truncate instead of declaring victory on half-applied state.
func TestRecoveryCommitKeepsRangesDirtyUntilTruncateSucceeds(t *testing.T) {
	t.Parallel()
	truncateCalls := 0
	failTruncate := true
	hub := &stubHub{
		chunkSize: 4,
		readFileAt: func(_ context.Context, _, _ string, _, length int64) ([]byte, error) {
			return make([]byte, length), nil
		},
		truncateFile: func(_ context.Context, _, _ string, size int64) (*meta.FileMeta, error) {
			truncateCalls++
			if failTruncate {
				return nil, errors.New("no space left on device")
			}
			return &meta.FileMeta{Size: size}, nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	const inode = uint64(31)
	state := &inodeWriteState{fs: fsys, inode: inode, path: "shrink.bin", refs: 1}
	fsys.mu.Lock()
	fsys.writeStates[inode] = state
	fsys.mu.Unlock()
	if err := state.materializeBootstrap(12); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := state.temp.WriteAt([]byte("0123456789XY"), 0); err != nil {
		t.Fatalf("seed temp: %v", err)
	}
	state.mu.Lock()
	state.baseSize = 12
	state.logicalSize = 8
	state.tempAuthoritative = false
	state.markDirtyLocked(10, 12)
	state.mu.Unlock()
	h := newPatchTestHandle(fsys, inode, state)
	h.path = "shrink.bin"

	state.mu.Lock()
	errno := h.commitPatch(context.Background(), "shrink.bin", 12, 8, []ByteRange{{Start: 8, End: 12}}, shfs.MetadataPatch{})
	if errno == 0 {
		t.Fatal("expected the injected truncate failure to surface")
	}
	state.mu.Lock()
	dirtyLeft := append([]ByteRange(nil), state.dirtyRanges...)
	logical := state.logicalSize
	state.mu.Unlock()
	if len(dirtyLeft) == 0 {
		t.Fatal("failed post-patch truncate must leave ranges dirty for retry")
	}
	if logical != 8 {
		t.Fatalf("failure must not lose the logical size: %d", logical)
	}

	failTruncate = false
	state.mu.Lock()
	errno = h.commitPatch(context.Background(), "shrink.bin", 12, 8, append([]ByteRange(nil), dirtyLeft...), shfs.MetadataPatch{})
	if errno != 0 {
		t.Fatalf("retry failed: %v", errno)
	}
	state.mu.Lock()
	remaining := len(state.dirtyRanges)
	base := state.baseSize
	state.mu.Unlock()
	if remaining != 0 || base != 8 {
		t.Fatalf("retry must converge: dirty=%d base=%d", remaining, base)
	}
	if truncateCalls != 2 {
		t.Fatalf("retry must re-issue the truncate, calls=%d", truncateCalls)
	}
}

// Data operations must land before the metadata patch in every commit
// path, so a crash can never leave metadata pointing at data that never
// arrived.
func TestRecoveryCommitOrdersDataBeforeMetadata(t *testing.T) {
	t.Parallel()
	var order []string
	hub := &stubHub{
		chunkSize: 4,
		readFileAt: func(_ context.Context, _, _ string, _, length int64) ([]byte, error) {
			return make([]byte, length), nil
		},
		patchRanges: func([]shfs.RangeEdit) (*meta.FileMeta, error) {
			order = append(order, "data")
			return &meta.FileMeta{}, nil
		},
		truncateFile: func(_ context.Context, _, _ string, size int64) (*meta.FileMeta, error) {
			order = append(order, "truncate")
			return &meta.FileMeta{Size: size}, nil
		},
		applyPatch: func(_ context.Context, _, _ string, patch shfs.MetadataPatch) error {
			order = append(order, "metadata")
			if !patch.HasMode {
				return errors.New("expected pending mode patch")
			}
			return nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	const inode = uint64(32)
	state := &inodeWriteState{fs: fsys, inode: inode, path: "ordered.bin", refs: 1}
	fsys.mu.Lock()
	fsys.writeStates[inode] = state
	fsys.mu.Unlock()
	if err := state.materializeBootstrap(12); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := state.temp.WriteAt([]byte("0123456789XY"), 0); err != nil {
		t.Fatalf("seed temp: %v", err)
	}
	state.mu.Lock()
	state.baseSize = 12
	state.logicalSize = 8
	state.tempAuthoritative = false
	state.markDirtyLocked(10, 12)
	state.pending = shfs.MetadataPatch{HasMode: true, Mode: 0o644}
	state.mu.Unlock()
	h := newPatchTestHandle(fsys, inode, state)
	h.path = "ordered.bin"
	if errno := h.commit(context.Background()); errno != 0 {
		t.Fatalf("commit: %v", errno)
	}
	if len(order) != 3 || order[0] != "data" || order[1] != "truncate" || order[2] != "metadata" {
		t.Fatalf("commit must order data, truncate, metadata; got %v", order)
	}
}

// O_TRUNC must serialize on the inode operation lock, not slip in
// beside an in-flight commit that already captured its plan.
func TestRecoveryOTruncSerializesOnOpMu(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "trunc.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("data"), 0); errno != 0 || n != 4 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	ws := h.writeState
	ws.opMu.Lock()
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := fsys.newHandle(context.Background(), 7, "trunc.bin", syscall.O_WRONLY|syscall.O_TRUNC, nil)
		done <- err
	}()
	<-started
	select {
	case <-done:
		ws.opMu.Unlock()
		t.Fatal("O_TRUNC open ran without holding opMu; it can interleave with a commit")
	case <-time.After(20 * time.Millisecond):
	}
	ws.opMu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("truncating open: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("truncating open hung after opMu release")
	}
	ws.mu.Lock()
	size := ws.logicalSize
	ws.mu.Unlock()
	if size != 0 {
		t.Fatalf("O_TRUNC must empty the file, size=%d", size)
	}
}

// Reads against a committing inode must serialize on the operation
// lock; otherwise a read straddling the commit's unlocked network window
// observes half-old, half-new state.
func TestRecoveryReadHoldsOpMuAcrossCommit(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "read.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("0123456789"), 0); errno != 0 || n != 10 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	ws := h.writeState
	ws.opMu.Lock()
	done := make(chan struct{})
	started := make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		_, _ = h.Read(context.Background(), make([]byte, 10), 0)
	}()
	<-started
	select {
	case <-done:
		ws.opMu.Unlock()
		t.Fatal("read ran without holding opMu; it can tear against a concurrent commit")
	case <-time.After(20 * time.Millisecond):
	}
	ws.opMu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("read hung after opMu release")
	}
	res, errno := h.Read(context.Background(), make([]byte, 10), 0)
	if errno != 0 {
		t.Fatalf("read: %v", errno)
	}
	buf, status := res.Bytes(make([]byte, 10))
	if status != fuse.OK || string(buf) != "0123456789" {
		t.Fatalf("torn read: %q %v", buf, status)
	}
}

// Every mutation must invalidate the kernel's entry/attr caches,
// otherwise the 60s timeouts serve stale metadata for a full minute.
func TestRecoveryMutationsInvalidateKernelCaches(t *testing.T) {
	t.Parallel()
	now := int64(77)
	renames := 0
	hub := &stubHub{
		createFile: func(_ context.Context, _ string, target string) (*meta.FileMeta, error) {
			return &meta.FileMeta{Inode: 101, Size: 0, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			switch target {
			case "newdir", "victim.txt", "attr.txt":
				return &shfs.EntryInfo{Path: target, Inode: 102, Size: 3, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			}
			return nil, syscall.ENOENT
		},
		loadReadonly: func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
			repo := meta.NewRepoMetadata("demo")
			repo.RebuildIndexes()
			return repo, "sha-1", nil
		},
		renameFn: func(_ context.Context, _, _, _ string) error {
			renames++
			return nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	before := fsys.Invalidations()
	var entryOut fuse.EntryOut
	if _, _, _, errno := fsys.root.Create(context.Background(), "created.txt", syscall.O_WRONLY|syscall.O_CREAT, 0o644, &entryOut); errno != 0 {
		t.Fatalf("create: %v", errno)
	}
	if got := fsys.Invalidations() - before; got < 1 {
		t.Fatalf("create must invalidate entry caches, delta=%d", got)
	}

	before = fsys.Invalidations()
	if _, errno := fsys.root.Mkdir(context.Background(), "newdir", 0o755, &entryOut); errno != 0 {
		t.Fatalf("mkdir: %v", errno)
	}
	if got := fsys.Invalidations() - before; got < 1 {
		t.Fatalf("mkdir must invalidate entry caches, delta=%d", got)
	}

	before = fsys.Invalidations()
	if errno := fsys.root.Rename(context.Background(), "a.txt", fsys.root, "b.txt", 0); errno != 0 {
		t.Fatalf("rename: %v", errno)
	}
	if renames != 1 {
		t.Fatalf("rename must reach the hub, calls=%d", renames)
	}
	if got := fsys.Invalidations() - before; got < 2 {
		t.Fatalf("rename must invalidate both parents, delta=%d", got)
	}

	before = fsys.Invalidations()
	if errno := fsys.root.Unlink(context.Background(), "victim.txt"); errno != 0 {
		t.Fatalf("unlink: %v", errno)
	}
	if got := fsys.Invalidations() - before; got < 1 {
		t.Fatalf("unlink must invalidate entry caches, delta=%d", got)
	}

	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "attr.txt", Inode: 102, Size: 3, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	var sattr fuse.SetAttrIn
	sattr.Valid = fuse.FATTR_MODE
	sattr.Mode = 0o600
	var attrOut fuse.AttrOut
	before = fsys.Invalidations()
	if errno := node.Setattr(context.Background(), nil, &sattr, &attrOut); errno != 0 {
		t.Fatalf("setattr: %v", errno)
	}
	if got := fsys.Invalidations() - before; got < 1 {
		t.Fatalf("setattr must invalidate attr caches, delta=%d", got)
	}
}

// Errno mapping must not leak raw ECANCELED to the kernel, must
// recognize cancellations that lost their error chain, and must report
// structural corruption as EUCLEAN (via the build-tagged
// errCorruptedErrno constant, so this file also compiles under
// GOOS=darwin where syscall.EUCLEAN does not exist).
func TestRecoveryErrnoMappingGaps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{"nil", nil, 0},
		{"canceled", context.Canceled, syscall.EINTR},
		{"deadline", context.DeadlineExceeded, syscall.ETIMEDOUT},
		{"ecanceled-raw", syscall.ECANCELED, syscall.EINTR},
		{"canceled-unwrapped", errors.New("download: context canceled"), syscall.EINTR},
		{"deadline-unwrapped", errors.New("rpc: deadline exceeded"), syscall.ETIMEDOUT},
		{"corrupted", shfs.Corrupted("repo/meta"), errCorruptedErrno},
		{"corrupted-wrapped", errors.Join(errors.New("load"), shfs.Corrupted("x")), errCorruptedErrno},
		{"notfound", shfs.NotFound("a"), syscall.ENOENT},
		{"unknown", errors.New("boom"), syscall.EIO},
	}
	for _, tc := range cases {
		if got := errnoFromError(tc.err); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

// Quarantine must be crash-safe (fsync + manifest) and the next mount
// must replay the inventory instead of hiding it.
func TestRecoveryQuarantineWritesManifestAndSurvivesRestart(t *testing.T) {
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
	if err := fsys.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(cacheDir, "recovery"))
	if err != nil {
		t.Fatalf("read recovery: %v", err)
	}
	var dataFile, manifestFile string
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".json"):
			manifestFile = e.Name()
		default:
			dataFile = e.Name()
		}
	}
	if dataFile == "" || manifestFile == "" {
		t.Fatalf("quarantine must leave data + manifest, got %v", entries)
	}
	data, err := os.ReadFile(filepath.Join(cacheDir, "recovery", dataFile))
	if err != nil || string(data) != "abcdefghij" {
		t.Fatalf("quarantined data wrong: %q (err=%v)", data, err)
	}
	raw, err := os.ReadFile(filepath.Join(cacheDir, "recovery", manifestFile))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest RecoveryEntry
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("manifest must be valid JSON: %v", err)
	}
	if manifest.TargetPath != "demo.bin" || manifest.Size != 10 || manifest.Reason == "" {
		t.Fatalf("manifest must record target/size/reason: %+v", manifest)
	}

	// Restart: the sweep must preserve recovery/ and replay its inventory.
	fsys2, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("reopen filesystem: %v", err)
	}
	defer func() { _ = fsys2.Close() }()
	inventory, err := fsys2.RecoveryInventory()
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	found := false
	for _, e := range inventory {
		if e.TargetPath == "demo.bin" && e.Size == 10 {
			found = true
		}
	}
	if !found {
		t.Fatalf("restart must replay quarantined entry for demo.bin, got %+v", inventory)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "recovery", dataFile)); err != nil {
		t.Fatalf("restart must preserve quarantined data: %v", err)
	}
}

// Stale overlay temps from a crashed mount are quarantined with
// manifests so nothing is silently lost or untraceable.
func TestRecoveryStartupSweepWritesManifests(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	for name, content := range map[string]string{"inode-aaa": "dirty-inode", "handle-bbb": "dirty-handle"} {
		if err := os.WriteFile(filepath.Join(cacheDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	inventory, err := fsys.RecoveryInventory()
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if len(inventory) != 2 {
		t.Fatalf("sweep must quarantine both leftovers with manifests, got %+v", inventory)
	}
	seen := map[string]bool{}
	for _, e := range inventory {
		seen[e.Reason] = true
		data, err := os.ReadFile(e.SavedPath)
		if err != nil || len(data) == 0 {
			t.Fatalf("quarantined entry unreadable: %+v (err=%v)", e, err)
		}
	}
	if !seen["startup-sweep"] {
		t.Fatalf("sweep entries must carry their reason, got %+v", inventory)
	}
}
