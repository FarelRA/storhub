package fusefs

// Regression tests for the FUSE layer: write-path DAC, overlay Setattr
// delegation, quarantine poisoning, hardlink rebind, detached-node
// ESTALE, whitespace-name commit, and the smaller fuse-layer behaviors.

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func callerCtx(uid, gid uint32) context.Context {
	return fuse.NewContext(context.Background(), &fuse.Caller{Owner: fuse.Owner{Uid: uid, Gid: gid}, Pid: 4242})
}

// secretRepo serves a repo view with secret.txt owned by uid 1000, mode
// 0600, in a world-traversable root.
func secretRepo() *meta.RepoMetadata {
	repo := meta.NewRepoMetadata("demo")
	repo.UpsertFile("secret.txt", meta.FileMeta{Inode: 7, Size: 6, Mode: 0o600, UID: 1000, GID: 1000}, 100)
	repo.RebuildIndexes()
	return repo
}

func secretHub(extra ...func(*stubHub)) *stubHub {
	hub := &stubHub{
		loadReadonly: func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
			return secretRepo(), "sha1", nil
		},
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			if target == "secret.txt" {
				return &shfs.EntryInfo{Path: target, Inode: 7, Size: 6, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: 100, AccessedAt: 100, ChangedAt: 100}, nil
			}
			return nil, syscall.ENOENT
		},
	}
	for _, fn := range extra {
		fn(hub)
	}
	return hub
}

// With NullPermissions the server is the only DAC gate, so a
// write-open must carry write permission on the file.
func TestOpenWriteDeniedForNonOwner(t *testing.T) {
	t.Parallel()
	fsys, err := New(secretHub(), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "secret.txt", Inode: 7, Size: 6, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1})
	if _, _, errno := node.Open(callerCtx(1001, 1001), syscall.O_WRONLY); errno != syscall.EACCES {
		t.Fatalf("stranger write-open must be EACCES, got %v", errno)
	}
	if _, _, errno := node.Open(callerCtx(1001, 1001), syscall.O_RDWR); errno != syscall.EACCES {
		t.Fatalf("stranger rdwr write-open must be EACCES, got %v", errno)
	}
	// Read-opens are DAC-gated at Open (Open-ONLY gate): a stranger gets
	// EACCES on O_RDONLY without paying a per-read check, and the hub
	// read path stays fast on the pinned snapshot.
	if _, _, errno := node.Open(callerCtx(1001, 1001), syscall.O_RDONLY); errno != syscall.EACCES {
		t.Fatalf("stranger read-open must be EACCES, got %v", errno)
	}
	if _, _, errno := node.Open(callerCtx(1000, 1000), syscall.O_WRONLY); errno != 0 {
		t.Fatalf("owner write-open: %v", errno)
	}
}

// Second DAC gate: identity at Flush time may differ from open time,
// so commit re-checks write DAC under the flushing caller.
func TestCommitRechecksWriteDAC(t *testing.T) {
	t.Parallel()
	var replaced int
	hub := secretHub(func(s *stubHub) {
		s.replaceFile = func(context.Context, string, string, string) (*meta.FileMeta, error) {
			replaced++
			return &meta.FileMeta{Inode: 7, Size: 5, Mode: 0o600, UID: 1000, GID: 1000}, nil
		}
	})
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "secret.txt", Inode: 7, Size: 6, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1})
	hAny, _, errno := node.Open(callerCtx(1000, 1000), syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("owner open: %v", errno)
	}
	h := hAny.(*storhubHandle)
	if n, errno := h.Write(context.Background(), []byte("hello!"), 0); errno != 0 || n != 6 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.Flush(callerCtx(1001, 1001)); errno != syscall.EACCES {
		t.Fatalf("flush under a stranger identity must be EACCES, got %v", errno)
	}
	if replaced != 0 {
		t.Fatalf("denied commit must not reach the backend, replace calls=%d", replaced)
	}
	if errno := h.Flush(callerCtx(1000, 1000)); errno != 0 {
		t.Fatalf("owner flush: %v", errno)
	}
	if replaced != 1 {
		t.Fatalf("owner flush must commit, replace calls=%d", replaced)
	}
}

// A Setattr without an attached handle must NOT touch another
// writer's overlay; it delegates to the hub verbs, which enforce DAC.
func TestSetattrWithoutHandleDelegatesToHub(t *testing.T) {
	t.Parallel()
	var truncates int
	hub := secretHub(func(s *stubHub) {
		s.truncateFile = func(_ context.Context, _, _ string, _ int64) (*meta.FileMeta, error) {
			truncates++
			return &meta.FileMeta{Inode: 7, Size: 0}, nil
		}
	})
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "secret.txt", Inode: 7, Size: 6, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1})
	// A writer holds the overlay open.
	hAny, _, errno := node.Open(callerCtx(1000, 1000), syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("open: %v", errno)
	}
	h := hAny.(*storhubHandle)
	if _, errno := h.Write(context.Background(), []byte("dirty"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	// A path-based truncate (no fh) must go to the hub, not the overlay.
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = 0
	var out fuse.AttrOut
	if errno := node.Setattr(callerCtx(1001, 1001), nil, &attr, &out); errno != 0 {
		t.Fatalf("path setattr: %v", errno)
	}
	if truncates != 1 {
		t.Fatalf("path-based setattr must delegate to the hub verb, truncates=%d", truncates)
	}
	h.writeState.mu.Lock()
	dirty := len(h.writeState.dirtyRanges)
	h.writeState.mu.Unlock()
	if dirty == 0 {
		t.Fatal("stranger's path truncate wiped the owner's overlay")
	}
}

// Overlay metadata mutations are refused when the caller identity
// differs from the handle's opener.
func TestSetattrOverlayCallerMismatchRejected(t *testing.T) {
	t.Parallel()
	fsys, err := New(secretHub(), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(callerCtx(1000, 1000), 7, "secret.txt", syscall.O_WRONLY, &writeBootstrap{baseSize: 6})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "secret.txt", Inode: 7, Size: 6, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1})
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_MODE
	attr.Mode = 0o666
	var out fuse.AttrOut
	if errno := node.Setattr(callerCtx(1001, 1001), h, &attr, &out); errno != syscall.EPERM {
		t.Fatalf("overlay setattr by a foreign uid must be EPERM, got %v", errno)
	}
	h.writeState.mu.Lock()
	pendingMode := h.writeState.pending.HasMode
	h.writeState.mu.Unlock()
	if pendingMode {
		t.Fatal("rejected setattr must not arm the pending mode patch")
	}
}

// A quarantined write state is poisoned - later writes, reads, and
// commits fail EIO instead of uploading zeros over remote data - and it
// is unregistered so new opens get a fresh state.
func TestQuarantinedWriteStateIsPoisoned(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	const inode = uint64(41)
	state := &inodeWriteState{fs: fsys, inode: inode, path: "poison.bin", refs: 1}
	fsys.mu.Lock()
	fsys.writeStates[inode] = state
	fsys.mu.Unlock()
	if err := state.materializeBootstrap(0); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	h := newPatchTestHandle(fsys, inode, state)
	if _, errno := h.Write(context.Background(), []byte("dirty"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	state.quarantineTempsReason(quarantineReasonClose)
	fsys.mu.RLock()
	_, registered := fsys.writeStates[inode]
	fsys.mu.RUnlock()
	if registered {
		t.Fatal("quarantined state stayed registered in writeStates")
	}
	if _, errno := h.Write(context.Background(), []byte("more"), 5); errno != syscall.EIO {
		t.Fatalf("write to poisoned state must be EIO, got %v", errno)
	}
	if errno := h.commit(context.Background()); errno != syscall.EIO {
		t.Fatalf("commit of poisoned state must be EIO, got %v", errno)
	}
	if _, errno := h.Read(context.Background(), make([]byte, 5), 0); errno != syscall.EIO {
		t.Fatalf("read of poisoned state must be EIO, got %v", errno)
	}
}

// A quarantine landing inside commitPatch's network window must
// fail the commit with EIO, not panic on the nil temp.
func TestCommitPatchSurvivesConcurrentQuarantine(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	hub := &stubHub{chunkSize: 4}
	hub.patchRanges = func([]shfs.RangeEdit) (*meta.FileMeta, error) {
		close(entered)
		<-release
		return &meta.FileMeta{}, nil
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	const inode = uint64(42)
	state := &inodeWriteState{fs: fsys, inode: inode, path: "race.bin", refs: 1}
	fsys.mu.Lock()
	fsys.writeStates[inode] = state
	fsys.mu.Unlock()
	if err := state.materializeBootstrap(4); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := state.temp.WriteAt([]byte("ABCD"), 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	state.mu.Lock()
	state.baseSize = 4
	state.logicalSize = 8
	state.markDirtyLocked(4, 8)
	state.mu.Unlock()
	h := newPatchTestHandle(fsys, inode, state)

	done := make(chan syscall.Errno, 1)
	go func() {
		state.mu.Lock()
		done <- h.commitPatch(context.Background(), "race.bin", 4, 8, []ByteRange{{Start: 4, End: 8}}, shfs.MetadataPatch{}, &commitNotifies{})
	}()
	<-entered
	// Quarantine while commitPatch is inside its unlocked network window.
	state.opMu.Lock()
	state.quarantineTempsReason(quarantineReasonClose)
	state.opMu.Unlock()
	close(release)
	var errno syscall.Errno
	select {
	case errno = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("commitPatch hung after concurrent quarantine")
	}
	if errno != syscall.EIO {
		t.Fatalf("commit racing quarantine must fail EIO, got %v", errno)
	}
}

// Unlinking one hardlink must rebind the shared write state to the
// surviving name; the writer's data lands there instead of being
// silently discarded.
func TestHardlinkUnlinkRebindsWriteState(t *testing.T) {
	t.Parallel()
	var commits []string
	hub := &stubHub{
		replaceFile: func(_ context.Context, _, target, _ string) (*meta.FileMeta, error) {
			commits = append(commits, target)
			return &meta.FileMeta{Inode: 12, Size: 5}, nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	const inode = uint64(12)
	fsys.rememberPath(inode, "a/x")
	fsys.rememberPath(inode, "a/y")
	h, err := fsys.newHandle(context.Background(), inode, "a/x", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if _, errno := h.Write(context.Background(), []byte("hello"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	// Simulate the unlink bookkeeping the Unlink op performs for a/x.
	remaining := fsys.dropPath(inode, "a/x")
	fsys.rebindHandlesAfterPathChange(inode, "a/x", remaining)
	h.writeState.mu.Lock()
	statePath, deleted := h.writeState.path, h.writeState.deleted
	h.writeState.mu.Unlock()
	if deleted || statePath != "a/y" {
		t.Fatalf("write state must follow the surviving link a/y, got path=%q deleted=%v", statePath, deleted)
	}
	h.mu.Lock()
	handlePath, handleDeleted := h.path, h.deleted
	h.mu.Unlock()
	if handleDeleted || handlePath != "a/y" {
		t.Fatalf("handle must follow the surviving link, got path=%q deleted=%v", handlePath, handleDeleted)
	}
	if errno := h.Flush(context.Background()); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	if len(commits) != 1 || commits[0] != "a/y" {
		t.Fatalf("commit must land in the surviving link, got %v", commits)
	}
}

// A node whose paths are all gone must report ESTALE, never
// masquerade as the root directory.
func TestDetachedNodeReportsESTALE(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "gone.txt", Inode: 15, Mode: 0o644})
	fsys.dropPath(15, "gone.txt")
	if errno := node.Getattr(context.Background(), nil, &fuse.AttrOut{}); errno != syscall.ESTALE {
		t.Fatalf("getattr on detached node must be ESTALE, got %v", errno)
	}
	if _, errno := node.Readdir(context.Background()); errno != syscall.ESTALE {
		t.Fatalf("readdir on detached node must be ESTALE, got %v", errno)
	}
	if _, _, errno := node.Open(context.Background(), syscall.O_RDONLY); errno != syscall.ESTALE {
		t.Fatalf("open on detached node must be ESTALE, got %v", errno)
	}
	if _, errno := node.Lookup(context.Background(), "child", &fuse.EntryOut{}); errno != syscall.ESTALE {
		t.Fatalf("lookup under detached node must be ESTALE, got %v", errno)
	}
	// The root (inode 1, path "") keeps working.
	if _, errno := fsys.root.Readdir(context.Background()); errno != 0 {
		t.Fatalf("root readdir must succeed, got %v", errno)
	}
}

// A whitespace-only handle path must still commit; only the truly
// empty (detached) path discards.
func TestWhitespaceNameWritesCommit(t *testing.T) {
	t.Parallel()
	var replaceTargets []string
	hub := &stubHub{
		replaceFile: func(_ context.Context, _, target, _ string) (*meta.FileMeta, error) {
			replaceTargets = append(replaceTargets, target)
			return &meta.FileMeta{Size: 4}, nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 16, " ", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if _, errno := h.Write(context.Background(), []byte("data"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := h.Flush(context.Background()); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	if len(replaceTargets) != 1 || replaceTargets[0] != " " {
		t.Fatalf("writes to a whitespace-named file must commit, got %v", replaceTargets)
	}
}

// df must never report free space exceeding the filesystem total.
func TestStatfsAccountingConsistent(t *testing.T) {
	t.Parallel()
	hub := &stubHub{
		statFS: func(context.Context, string) (*shfs.FSStats, error) {
			return &shfs.FSStats{Inodes: 3, Bytes: 8192}, nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	var out fuse.StatfsOut
	if errno := fsys.root.Statfs(context.Background(), &out); errno != 0 {
		t.Fatalf("statfs: %v", errno)
	}
	if out.Bfree > out.Blocks {
		t.Fatalf("free (%d) exceeds total (%d): negative usage", out.Bfree, out.Blocks)
	}
	if out.Blocks < out.Bfree {
		t.Fatal("blocks below free")
	}
}

// A hub that returns a nil entry with success must not be
// dereferenced.
func TestLinkNilEntryGuard(t *testing.T) {
	t.Parallel()
	// The default stubHub.LinkContext returns (nil, nil) with success -
	// exactly the shape that used to panic.
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	target := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "dir", Inode: 17, IsDir: true, Mode: 0o755})
	if _, errno := fsys.root.Link(callerCtx(1000, 1000), target, "alias", &fuse.EntryOut{}); errno != syscall.EPERM {
		t.Fatalf("nil-entry link success must be guarded as EPERM, got %v", errno)
	}
}

// FUSE requests carry no umask, so the server applies a default
// mask to creation modes instead of minting 0666 files.
func TestCallerContextCarriesDefaultUmask(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	ctx := fsys.callerContext(callerCtx(1000, 1000))
	id := shfs.IdentityFromContext(ctx)
	if id.Umask != defaultCallerUmask {
		t.Fatalf("expected default umask %#o, got %#o", defaultCallerUmask, id.Umask)
	}
	if got := shfs.ApplyCreateMode(shfs.WithCreateMode(ctx, 0o666), 0o666); got != 0o644 {
		t.Fatalf("touch must create 0644 under the default mask, got %#o", got)
	}
}

// Cross-platform gate: the corruption errno mapping is asserted through
// the build-tagged constant, so this compiles and passes on every GOOS
// (EUCLEAN on linux, EIO elsewhere). Never reference syscall.EUCLEAN
// directly in an untagged file.
func TestErrnoCorruptedUsesPlatformConstant(t *testing.T) {
	t.Parallel()
	if got := errnoFromError(shfs.Corrupted("meta")); got != errCorruptedErrno {
		t.Fatalf("corrupted mapping: got %v want %v", got, errCorruptedErrno)
	}
	if errnoFromError(errors.Join(errors.New("load"), shfs.Corrupted("x"))) != errCorruptedErrno {
		t.Fatal("wrapped corrupted mapping must use the platform constant too")
	}
}
