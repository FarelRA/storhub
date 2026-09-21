package fusefs

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestCallerContextSuppressesAtime(t *testing.T) {
	t.Parallel()
	fake := &stubHub{}
	fsys, err := New(fake, "demoproject", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	ctx := fsys.callerContext(context.Background())
	if !shfs.AtimeSuppressed(ctx) {
		t.Fatal("expected FUSE caller context to suppress atime")
	}
	// Without a kernel caller, no identity is attached: the context fails
	// closed to a non-admin fallback, never to anonymous root. Pinned on
	// fixed UIDs only (never os.Getuid: the assertion must read identically
	// as root and in CI).
	if shfs.IdentityPresent(ctx) {
		t.Fatal("background caller context must not carry an attached identity")
	}
	if identity := shfs.IdentityFromContext(ctx); identity.Admin {
		t.Fatalf("fallback identity must never be admin: %+v", identity)
	}
}

func TestRenameDelegatesToHubAndRemapsPaths(t *testing.T) {
	t.Parallel()
	now := int64(10)
	metaState := meta.NewRepoMetadata("demo")
	metaState.EnsureDirectory("docs", now)
	metaState.Chunks()[1] = meta.ChunkInfo{Offset: 0, Size: 1, Release: "v1", AssetID: 1}
	if _, err := metaState.EnsureRelease("v1", now); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	metaState.UpsertFile("docs/old.txt", meta.FileMeta{Size: 1, Chunks: []int64{1}, Inode: 5, Mode: 0o644, UploadedAt: now}, now)
	var renamedOld, renamedNew string
	fake := &stubHub{
		now: now,
		renameFn: func(_ context.Context, _ string, oldPath, newPath string) error {
			file := metaState.FindFile(oldPath)
			if file == nil {
				return syscall.ENOENT
			}
			renamed := file.Clone()
			metaState.RemoveFile(oldPath)
			metaState.RemoveFile(newPath)
			metaState.UpsertFile(newPath, renamed, now)
			renamedOld, renamedNew = oldPath, newPath
			return nil
		},
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			if f := metaState.FindFile(target); f != nil {
				return &shfs.EntryInfo{Path: target, Inode: f.Inode, Size: f.Size, Mode: f.Mode}, nil
			}
			if metaState.HasDirectory(target) {
				return &shfs.EntryInfo{Path: target, IsDir: true, Mode: 0o755}, nil
			}
			return nil, syscall.ENOENT
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	oldFileMeta := metaState.FindFile("docs/old.txt")
	fsys.rememberPath(oldFileMeta.Inode, "docs/old.txt")

	dirEntry := &shfs.EntryInfo{Path: "docs", IsDir: true, Inode: 2, Mode: 0o755}
	docsNode := fsys.EnsureNodeForTest(context.Background(), dirEntry)
	if !docsNode.isDir {
		t.Fatal("expected directory node")
	}

	// Rename is invoked on the parent node with child names.
	if errno := docsNode.Rename(context.Background(), "old.txt", docsNode, "new.txt", 0); errno != 0 {
		t.Fatalf("rename: %v", errno)
	}
	if renamedOld != "docs/old.txt" || renamedNew != "docs/new.txt" {
		t.Fatalf("hub rename not called with resolved paths: %q -> %q", renamedOld, renamedNew)
	}
	if metaState.FindFile("docs/new.txt") == nil || metaState.FindFile("docs/old.txt") != nil {
		t.Fatalf("expected metadata rename, got %+v", metaState.AllFiles())
	}
	if got := fsys.pathForInode(oldFileMeta.Inode); got != "docs/new.txt" {
		t.Fatalf("expected inode path remap, got %q", got)
	}
	// RENAME_NOREPLACE onto an existing destination must fail before the
	// hub is consulted.
	if errno := docsNode.Rename(context.Background(), "new.txt", docsNode, "new.txt", renameNoReplace); errno != syscall.EEXIST {
		t.Fatalf("expected EEXIST for noreplace onto existing target, got %v", errno)
	}
}

func TestCreateBootstrapsWritableHandleWithoutRestat(t *testing.T) {
	t.Parallel()
	now := int64(30)
	var replacedPath string
	var replacedBytes []byte
	fake := &stubHub{
		createFile: func(_ context.Context, _ string, _ string) (*meta.FileMeta, error) {
			return &meta.FileMeta{
				Inode:      8,
				Mode:       0o644,
				UID:        1000,
				GID:        1000,
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
		replaceFile: func(_ context.Context, _ string, target, inputPath string) (*meta.FileMeta, error) {
			data, err := os.ReadFile(inputPath)
			if err != nil {
				return nil, err
			}
			replacedPath = target
			replacedBytes = data
			return &meta.FileMeta{Inode: 8, Size: int64(len(data)), Mode: 0o644, UID: 1000, GID: 1000, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
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
	t.Parallel()
	now := int64(31)
	chmodCalled := false
	fake := &stubHub{
		createFile: func(_ context.Context, _ string, _ string) (*meta.FileMeta, error) {
			return &meta.FileMeta{
				Inode:      9,
				Mode:       0o644,
				UID:        1000,
				GID:        1000,
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
				entry := mkEntry(target, 0, now)
				entry.Inode = 9
				return entry, nil
			default:
				return nil, syscall.ENOENT
			}
		},
		replaceFile: func(_ context.Context, _ string, _, _ string) (*meta.FileMeta, error) {
			return &meta.FileMeta{Inode: 9, Mode: 0o644, UID: 1000, GID: 1000, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		chmod: func(_ context.Context, _ string, _ string, _ uint32) error {
			chmodCalled = true
			return nil
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
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

func TestCreatePassesCallerIdentityAndRequestedMode(t *testing.T) {
	t.Parallel()
	now := int64(32)
	var seenIdentity shfs.Identity
	var seenMode uint32
	fake := &stubHub{
		createFile: func(ctx context.Context, _ string, _ string) (*meta.FileMeta, error) {
			seenIdentity = shfs.IdentityFromContext(ctx)
			seenMode, _ = shfs.CreateModeFromContext(ctx)
			return &meta.FileMeta{Inode: 9, Mode: seenMode, UID: seenIdentity.UID, GID: seenIdentity.GID, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			switch target {
			case "docs":
				return &shfs.EntryInfo{Path: "docs", Inode: 2, IsDir: true, Mode: 0o755, UID: 1000, GID: 1000, NLink: 2, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			case "docs/new.txt":
				return &shfs.EntryInfo{Path: target, Inode: 9, Mode: seenMode, UID: seenIdentity.UID, GID: seenIdentity.GID, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			default:
				return nil, syscall.ENOENT
			}
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	dirNode := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs", Inode: 2, IsDir: true, Mode: 0o755, UID: 1000, GID: 1000, NLink: 2, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	ctx := fuse.NewContext(context.Background(), &fuse.Caller{Owner: fuse.Owner{Uid: 123, Gid: 456}, Pid: 789})
	var out fuse.EntryOut
	_, handleAny, _, errno := dirNode.Create(ctx, "new.txt", syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL, 0o640, &out)
	if errno != 0 {
		t.Fatalf("create file: %v", errno)
	}
	if seenIdentity.UID != 123 || seenIdentity.GID != 456 || seenIdentity.PID != 789 {
		t.Fatalf("unexpected identity: %+v", seenIdentity)
	}
	if seenMode != 0o640 {
		t.Fatalf("unexpected create mode: %#o", seenMode)
	}
	if errno := handleAny.(*storhubHandle).Release(context.Background()); errno != 0 {
		t.Fatalf("release created file: %v", errno)
	}
}

func TestAccessChecksCallerPermissions(t *testing.T) {
	t.Parallel()
	now := int64(33)
	fake := &stubHub{
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			if target == "docs/file.txt" {
				return &shfs.EntryInfo{Path: target, Inode: 7, Mode: 0o640, UID: 10, GID: 20, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			}
			return nil, syscall.ENOENT
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs/file.txt", Inode: 7, Mode: 0o640, UID: 10, GID: 20, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	denied := fuse.NewContext(context.Background(), &fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 99}, Pid: 1})
	if errno := node.Access(denied, 0x4); errno != syscall.EACCES {
		t.Fatalf("expected read denial, got %v", errno)
	}
	allowed := fuse.NewContext(context.Background(), &fuse.Caller{Owner: fuse.Owner{Uid: 10, Gid: 20}, Pid: 1})
	if errno := node.Access(allowed, 0x4); errno != 0 {
		t.Fatalf("expected owner read success, got %v", errno)
	}
}

func TestMknodRejectsUnsupportedSpecialFiles(t *testing.T) {
	t.Parallel()
	now := int64(34)
	fake := &stubHub{
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			if target == "docs" {
				return &shfs.EntryInfo{Path: target, Inode: 2, IsDir: true, Mode: 0o755, UID: 1000, GID: 1000, NLink: 2, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			}
			return nil, syscall.ENOENT
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	dirNode := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs", Inode: 2, IsDir: true, Mode: 0o755, UID: 1000, GID: 1000, NLink: 2, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	var out fuse.EntryOut
	if _, errno := dirNode.Mknod(context.Background(), "pipe", syscall.S_IFIFO|0o644, 0, &out); errno != syscall.ENOTSUP {
		t.Fatalf("expected fifo mknod to be unsupported, got %v", errno)
	}
}

func TestSetattrOnWriteHandleDefersMetadataPatchUntilRelease(t *testing.T) {
	t.Parallel()
	now := int64(35)
	patchCalls := 0
	chmodCalls := 0
	fake := &stubHub{
		loadReadonly: func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
			repo := meta.NewRepoMetadata("demo")
			repo.UpsertFile("docs/file.txt", meta.FileMeta{Inode: 7, Size: 10}, now)
			repo.RebuildIndexes()
			return repo, "sha1", nil
		},
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			if target == "docs/file.txt" {
				return &shfs.EntryInfo{Path: target, Inode: 7, Size: 10, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			}
			return nil, syscall.ENOENT
		},
		chmod: func(context.Context, string, string, uint32) error {
			chmodCalls++
			return nil
		},
		applyPatch: func(_ context.Context, _ string, target string, patch shfs.MetadataPatch) error {
			patchCalls++
			if target != "docs/file.txt" || !patch.HasMode || patch.Mode != 0o644 || !patch.HasTimes {
				return fmt.Errorf("unexpected patch: %+v target=%s", patch, target)
			}
			return nil
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs/file.txt", Inode: 7, Size: 10, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	hAny, _, errno := node.Open(context.Background(), syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("open write handle: %v", errno)
	}
	h := hAny.(*storhubHandle)
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_MODE | fuse.FATTR_MTIME | fuse.FATTR_ATIME
	attr.Mode = 0o644
	attr.Atime = uint64(now - 3600)
	attr.Atimensec = 0
	attr.Mtime = uint64(now - 7200)
	attr.Mtimensec = 0
	var out fuse.AttrOut
	if errno := node.Setattr(context.Background(), h, &attr, &out); errno != 0 {
		t.Fatalf("setattr with handle: %v", errno)
	}
	if chmodCalls != 0 || patchCalls != 0 {
		t.Fatalf("expected no immediate backend metadata writes, got chmod=%d patch=%d", chmodCalls, patchCalls)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release handle: %v", errno)
	}
	if patchCalls != 1 {
		t.Fatalf("expected one deferred metadata patch, got %d", patchCalls)
	}
}

// TestOpenReturnsKernelCachedFlags pins that opens hand the kernel
// page-cached IO (zero FOPEN flags); direct IO would bypass our overlay.
func TestOpenReturnsKernelCachedFlags(t *testing.T) {
	t.Parallel()
	now := int64(36)
	fake := &stubHub{
		loadReadonly: func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
			repo := meta.NewRepoMetadata("demo")
			repo.UpsertFile("docs/file.txt", meta.FileMeta{Inode: 7, Size: 10}, now)
			repo.RebuildIndexes()
			return repo, "sha1", nil
		},
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			if target == "docs/file.txt" {
				return &shfs.EntryInfo{Path: target, Inode: 7, Size: 10, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
			}
			return nil, syscall.ENOENT
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs/file.txt", Inode: 7, Size: 10, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	h, flags, errno := node.Open(context.Background(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("open: %v", errno)
	}
	if flags != 0 {
		t.Fatalf("expected zero open flag, got %#x", flags)
	}
	if errno := h.(*storhubHandle).Release(context.Background()); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
}

// The overlay is honored only for a handle attached to the write
// state. A truncate through the open handle stays local until commit.
func TestSetattrWithAttachedHandleUsesActiveWriteState(t *testing.T) {
	t.Parallel()
	now := int64(40)
	var truncates int
	var replaced []byte
	backendSize := int64(len("abcdefghij"))
	fake := &stubHub{
		loadReadonly: func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
			repo := meta.NewRepoMetadata("demo")
			repo.UpsertFile("docs/file.txt", meta.FileMeta{Inode: 7, Size: backendSize}, now)
			repo.RebuildIndexes()
			return repo, "sha1", nil
		},
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
		truncateFile: func(_ context.Context, _ string, _ string, size int64) (*meta.FileMeta, error) {
			truncates++
			backendSize = size
			return &meta.FileMeta{Inode: 7, Size: size, Mode: 0o644, UID: 1000, GID: 1000, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
		replaceFile: func(_ context.Context, _ string, _ string, inputPath string) (*meta.FileMeta, error) {
			data, err := os.ReadFile(inputPath)
			if err != nil {
				return nil, err
			}
			replaced = data
			backendSize = int64(len(data))
			return &meta.FileMeta{Inode: 7, Size: int64(len(data)), Mode: 0o644, UID: 1000, GID: 1000, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
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
	if errno := node.Setattr(context.Background(), h, &attr, &out); errno != 0 {
		t.Fatalf("setattr with attached handle: %v", errno)
	}
	if truncates != 0 {
		t.Fatalf("expected attached-handle setattr to avoid backend truncate, got %d calls", truncates)
	}
	if out.Size != 0 {
		t.Fatalf("expected local setattr size 0, got %d", out.Size)
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
	if getattr.Size != 5 {
		t.Fatalf("expected getattr size 5 after commit, got %d", getattr.Size)
	}
}
