package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"syscall"
	"testing"

	storcfg "github.com/FarelRA/storhub/internal/config"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type testBackend struct {
	repo       *meta.RepoMetadata
	now        int64
	nextAsset  int64
	assetBytes map[int64][]byte
}

func newTestBackend(now int64) *testBackend {
	repo := meta.NewRepoMetadata("demo")
	repo.EnsureRelease("v1", now)
	return &testBackend{repo: repo, now: now, nextAsset: 1, assetBytes: map[int64][]byte{}}
}

func (b *testBackend) ValidateProjectName(project string) error {
	if project == "bad/project" {
		return errors.New("bad project")
	}
	return nil
}

func (b *testBackend) EnsureRepoContext(context.Context, string) error { return nil }

func (b *testBackend) LoadRepoMetadataContext(context.Context, string) (*meta.RepoMetadata, string, error) {
	clone := b.repo.Clone()
	clone.RebuildIndexes()
	return &clone, "sha", nil
}

func (b *testBackend) LoadRepoMetadataReadonlyContext(context.Context, string) (*meta.RepoMetadata, string, error) {
	return b.LoadRepoMetadataContext(context.Background(), "")
}

func (b *testBackend) UpdateRepoMetadataContext(_ context.Context, _ string, fn func(*meta.RepoMetadata) error, _ string) (*meta.RepoMetadata, error) {
	clone := b.repo.Clone()
	clone.RebuildIndexes()
	if err := fn(&clone); err != nil {
		return nil, err
	}
	clone.RebuildIndexes()
	b.repo = &clone
	return &clone, nil
}

func (b *testBackend) QueueAtimeUpdateContext(ctx context.Context, project, targetPath string, isDir bool, now int64) {
	_, _ = b.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if isDir {
			if targetPath == "" {
				repo.Root.AccessedAt = now
				return nil
			}
			if dir := repo.GetDirectory(targetPath); dir != nil {
				dir.AccessedAt = now
			}
			return nil
		}
		if file := repo.FindFile(targetPath); file != nil {
			file.AccessedAt = now
		}
		return nil
	}, "test atime")
}

func (b *testBackend) Logger() *slog.Logger { return nil }

func (b *testBackend) GetOrCreateUploadReleaseContext(_ context.Context, _ string, repoMeta *meta.RepoMetadata, _ int) (string, string, error) {
	repoMeta.EnsureRelease("v1", b.now)
	return "v1", "upload", nil
}

func (b *testBackend) PatchFileWithMetadataContext(_ context.Context, _ string, cleanName string, _ *meta.RepoMetadata, fileMeta *meta.FileMeta, offset, deleteSize int64, edit []byte) (*meta.FileMeta, error) {
	current, err := b.fileData(fileMeta)
	if err != nil {
		return nil, err
	}
	patched := append([]byte(nil), current[:offset]...)
	patched = append(patched, edit...)
	patched = append(patched, current[offset+deleteSize:]...)
	updated := fileMeta.Clone()
	updated.Size = int64(len(patched))
	updated.ModifiedAt = b.now
	updated.ChangedAt = b.now
	updated.AccessedAt = b.now
	b.storeFile(&updated, patched)
	if _, err := b.UpdateRepoMetadataContext(context.Background(), "", func(repo *meta.RepoMetadata) error {
		repo.UpsertFile(cleanName, updated, b.now)
		return nil
	}, "patch"); err != nil {
		return nil, err
	}
	return &updated, nil
}

func (b *testBackend) FillAssetRangeContext(_ context.Context, _ string, segment meta.ChunkInfo, dst []byte) error {
	data := b.assetBytes[segment.AssetID]
	start := int(segment.AssetOffset)
	end := start + len(dst)
	copy(dst, data[start:end])
	return nil
}

func (b *testBackend) Now() int64 { return b.now }

func (b *testBackend) FileNotFound(path string) error { return fmt.Errorf("not found: %s", path) }

func (b *testBackend) DefaultFileMode(kind meta.NodeKind) uint32 {
	if kind == meta.NodeKindSymlink {
		return 0o777
	}
	return 0o644
}

func (b *testBackend) DefaultOwnerIDs() (uint32, uint32) { return 1, 2 }

func (b *testBackend) seedDir(path string) {
	b.repo.EnsureDirectory(path, b.now)
}

func (b *testBackend) seedFile(path string, data []byte) *meta.FileMeta {
	file := meta.FileMeta{Mode: 0o644, UID: 1, GID: 2, UploadedAt: b.now, ModifiedAt: b.now, AccessedAt: b.now, ChangedAt: b.now}
	b.storeFile(&file, data)
	b.repo.UpsertFile(path, file, b.now)
	stored := b.repo.FindFile(path)
	clone := stored.Clone()
	return &clone
}

func (b *testBackend) storeFile(file *meta.FileMeta, data []byte) {
	assetID := b.nextAsset
	b.nextAsset++
	b.assetBytes[assetID] = append([]byte(nil), data...)
	file.Size = int64(len(data))
	if len(data) == 0 {
		file.Chunks = []int64{}
		return
	}
	release := "v1"
	if len(file.Chunks) > 0 {
		if c, ok := b.repo.Chunks[file.Chunks[0]]; ok {
			release = c.Release
		}
	}
	b.repo.Chunks[assetID] = meta.ChunkInfo{Offset: 0, Size: int64(len(data)), AssetID: assetID, Release: release}
	file.Chunks = []int64{assetID}
}

func (b *testBackend) fileData(file *meta.FileMeta) ([]byte, error) {
	if len(file.Chunks) == 0 {
		return nil, nil
	}
	chunkName := file.Chunks[0]
	chunk, ok := b.repo.Chunks[chunkName]
	if !ok {
		return nil, io.EOF
	}
	data, ok := b.assetBytes[chunk.AssetID]
	if !ok {
		return nil, io.EOF
	}
	return append([]byte(nil), data...), nil
}

func TestServiceWorkflowAndHelpers(t *testing.T) {
	t.Parallel()
	// The workflow runs as an explicitly identified root caller; absent
	// identities fail closed to the process user and own nothing here.
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
	now := int64(100)
	backend := newTestBackend(now)
	svc := NewService(backend)
	backend.seedDir("docs")
	seeded := backend.seedFile("docs/readme.txt", []byte("hello"))

	created, err := svc.CreateFileContext(ctx, "demo", "docs/empty.txt")
	if err != nil || created.Size != 0 {
		returnFail(t, "create file", err, created)
	}
	if err := svc.MkdirContext(ctx, "demo", "docs/sub"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := svc.AppendFileContext(ctx, "demo", "docs/readme.txt", []byte(" world")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := svc.WriteFileAtContext(ctx, "demo", "docs/readme.txt", 0, []byte("H")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := svc.TruncateFileContext(ctx, "demo", "docs/readme.txt", 5); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	data, err := svc.ReadFileAtContext(ctx, "demo", "docs/readme.txt", 0, 5)
	if err != nil || string(data) != "Hello" {
		t.Fatalf("read after writes: %q %v", data, err)
	}
	if err := svc.RenameContext(ctx, "demo", "docs/readme.txt", "docs/sub/final.txt"); err != nil {
		t.Fatalf("rename file: %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "docs/sub", "docs/archive"); err != nil {
		t.Fatalf("rename dir: %v", err)
	}
	entry, err := svc.StatPathContext(ctx, "demo", "docs/archive/final.txt")
	if err != nil || entry.Path != "docs/archive/final.txt" || entry.Size != 5 {
		t.Fatalf("stat path: %+v %v", entry, err)
	}
	dirs, err := svc.ReadDirContext(ctx, "demo", "docs")
	if err != nil || len(dirs) != 2 {
		t.Fatalf("readdir: %+v %v", dirs, err)
	}
	stats, err := svc.StatFSContext(ctx, "demo")
	if err != nil || stats.Files < 2 || stats.Directories < 2 {
		t.Fatalf("statfs: %+v %v", stats, err)
	}
	if err := svc.RmdirContext(ctx, "demo", "docs/archive"); err == nil {
		t.Fatal("expected non-empty rmdir failure")
	}
	if err := svc.RenameContext(ctx, "demo", "docs", "docs/archive/nested"); err == nil {
		t.Fatal("expected self-rename failure")
	}
	// Reads at/past EOF return zero bytes, not io.EOF (which the
	// FUSE layer mapped to EIO).
	if data, err := svc.ReadFileAtContext(ctx, "demo", "docs/archive/final.txt", 99, 1); err != nil || len(data) != 0 {
		t.Fatalf("expected empty read past EOF, got %q %v", data, err)
	}
	if info := EntryInfoFromFile(seeded, "docs/readme.txt", backend.repo.FileNLink("docs/readme.txt")); info.Path != "docs/readme.txt" || !EntryInfoFromDirectory(&meta.DirMeta{Inode: 2, Mode: 0o755}, "docs", 2).IsDir {
		t.Fatal("entry helper conversion failed")
	}
	if DirEntryFromFile(*seeded, "docs/readme.txt", backend.repo.FileNLink("docs/readme.txt")).Path != "docs/readme.txt" || !DirEntryFromDirectory(meta.DirMeta{}, "docs/sub", 2).IsDir {
		t.Fatal("dir entry helper conversion failed")
	}
	if CountUniqueInodes(backend.repo) == 0 || min(int64(1), int64(2)) != 1 || max(int64(1), int64(2)) != 2 {
		t.Fatal("helper counts/min/max failed")
	}
	if err := svc.RmdirContext(ctx, "demo", "docs/archive"); err == nil {
		t.Fatal("expected archive to remain non-empty")
	}
}

func TestServiceErrors(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(200)
	svc := NewService(backend)
	backend.seedDir("docs")
	backend.seedFile("docs/link", []byte("abc"))
	backend.repo.UpsertFile("docs/symlink", meta.FileMeta{Mode: 0o777, Symlink: "link", UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now}, backend.now)
	backend.repo.UpsertFile("docs/dangling", meta.FileMeta{Mode: 0o777, Symlink: "nowhere", UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now}, backend.now)

	if _, err := svc.CreateFileContext(context.Background(), "bad/project", "docs/x"); err == nil {
		t.Fatal("expected project validation error")
	}
	if _, err := svc.CreateFileContext(context.Background(), "demo", "../bad"); err == nil {
		t.Fatal("expected path normalization error")
	}
	if _, err := svc.WriteFileAtContext(context.Background(), "demo", "docs/missing", 0, []byte("x")); err == nil {
		t.Fatal("expected missing file error")
	}
	if _, err := svc.WriteFileAtContext(context.Background(), "demo", "docs/link", -1, []byte("x")); err == nil {
		t.Fatal("expected negative offset error")
	}
	// Read/append have open() semantics and follow the final
	// symlink to its target instead of failing on the link itself.
	rootCtx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
	if data, err := svc.ReadFileAtContext(context.Background(), "demo", "docs/symlink", 0, 3); err != nil || string(data) != "abc" {
		t.Fatalf("expected read through symlink to succeed, got %q err=%v", data, err)
	}
	if _, err := svc.ReadFileAtContext(context.Background(), "demo", "docs/dangling", 0, 1); err == nil {
		t.Fatal("expected dangling symlink read to fail with not-found")
	}
	if _, err := svc.AppendFileContext(rootCtx, "demo", "docs/symlink", []byte("!")); err != nil {
		t.Fatalf("expected append through symlink to succeed: %v", err)
	}
	if data, err := svc.ReadFileAtContext(context.Background(), "demo", "docs/link", 0, 4); err != nil || string(data) != "abc!" {
		t.Fatalf("append through symlink must land on the target, got %q err=%v", data, err)
	}
	if _, err := svc.TruncateFileContext(context.Background(), "demo", "docs/symlink", 1); err == nil {
		t.Fatal("expected symlink truncate failure")
	}
	if _, err := svc.StatPathContext(context.Background(), "demo", "docs/missing"); err == nil {
		t.Fatal("expected missing path error")
	}
	if _, err := svc.ReadDirContext(context.Background(), "demo", "docs/link"); err == nil {
		t.Fatal("expected not-a-directory error")
	}
	if err := svc.RmdirContext(context.Background(), "demo", ""); err == nil {
		t.Fatal("expected root rmdir failure")
	}
	if err := RequireParentDirectory(backend.repo, "missing/file"); err == nil {
		t.Fatal("expected missing parent failure")
	}
}

func TestServicePermissionEnforcement(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(250)
	backend.seedDir("private")
	file := backend.seedFile("private/note.txt", []byte("secret"))
	file.Mode = 0o640
	file.UID = 11
	file.GID = 22
	backend.repo.UpsertFile("private/note.txt", *file, backend.now)
	dir := backend.repo.GetDirectory("private")
	dir.Mode = 0o750
	dir.UID = 11
	dir.GID = 22
	backend.repo.Dirs["private"] = *dir
	backend.repo.RebuildIndexes()
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 30, GID: 40, Groups: []uint32{40}})
	if _, err := svc.ReadFileAtContext(ctx, "demo", "private/note.txt", 0, 1); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected read denial, got %v", err)
	}
	if _, err := svc.WriteFileAtContext(ctx, "demo", "private/note.txt", 0, []byte("x")); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected write denial, got %v", err)
	}
	if _, err := svc.ReadDirContext(ctx, "demo", "private"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected list denial, got %v", err)
	}
	ownerCtx := WithIdentity(context.Background(), Identity{UID: 11, GID: 22, Groups: []uint32{22}})
	if data, err := svc.ReadFileAtContext(ownerCtx, "demo", "private/note.txt", 0, 6); err != nil || string(data) != "secret" {
		t.Fatalf("expected owner read access, got %q %v", data, err)
	}
}

func TestCreateAndMkdirInheritSetgidAndTouchParent(t *testing.T) {
	t.Parallel()
	now := int64(260)
	backend := newTestBackend(now)
	backend.seedDir("shared")
	parent := backend.repo.GetDirectory("shared")
	parent.Mode = 0o2775
	parent.UID = 50
	parent.GID = 60
	parent.ModifiedAt = now - 3600
	parent.ChangedAt = now - 3600
	backend.repo.Dirs["shared"] = *parent
	backend.repo.RebuildIndexes()
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 70, GID: 80, Groups: []uint32{80, 60}})
	created, err := svc.CreateFileContext(ctx, "demo", "shared/file.txt")
	if err != nil {
		t.Fatalf("create with setgid parent: %v", err)
	}
	if created.GID != 60 {
		t.Fatalf("expected inherited gid 60, got %d", created.GID)
	}
	if err := svc.MkdirContext(ctx, "demo", "shared/subdir"); err != nil {
		t.Fatalf("mkdir with setgid parent: %v", err)
	}
	dir := backend.repo.GetDirectory("shared/subdir")
	if dir == nil || dir.GID != 60 || dir.Mode&0o2000 == 0 {
		t.Fatalf("expected setgid inheritance on directory, got %+v", dir)
	}
	parent = backend.repo.GetDirectory("shared")
	if parent.ModifiedAt != now || parent.ChangedAt != now {
		t.Fatalf("expected parent timestamps touched, got mtime=%v ctime=%v", parent.ModifiedAt, parent.ChangedAt)
	}
}

func TestCreateAndMkdirUseCallerOwnership(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(265)
	backend.seedDir("docs")
	dir := backend.repo.GetDirectory("docs")
	dir.Mode = 0o777
	backend.repo.Dirs["docs"] = *dir
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 986, GID: 986, Groups: []uint32{986}})
	file, err := svc.CreateFileContext(ctx, "demo", "docs/file.txt")
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if file.UID != 986 || file.GID != 986 {
		t.Fatalf("expected caller-owned file, got %d:%d", file.UID, file.GID)
	}
	if err := svc.MkdirContext(ctx, "demo", "docs/subdir"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	dir = backend.repo.GetDirectory("docs/subdir")
	if dir == nil || dir.UID != 986 || dir.GID != 986 {
		t.Fatalf("expected caller-owned dir, got %+v", dir)
	}
}

func (b *testBackend) AtimePolicy() storcfg.AtimePolicy { return storcfg.AtimeRelatime }

func returnFail(t *testing.T, label string, err error, value any) {
	t.Helper()
	t.Fatalf("%s: value=%+v err=%v", label, value, err)
}

func TestCreateFileRejectsExistingDirectory(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(100)
	svc := NewService(backend)
	if err := svc.MkdirContext(context.Background(), "demo", "sub"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := svc.CreateFileContext(context.Background(), "demo", "sub"); !errors.Is(err, ErrIsDirectory) {
		t.Fatalf("expected ErrIsDirectory, got %v", err)
	}
	if _, shadowed := backend.repo.Files["sub"]; shadowed {
		t.Fatal("directory was silently shadowed by a file entry")
	}
}

func TestRenamePOSIXReplaceSemantics(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(100)
	svc := NewService(backend)
	ctx := context.Background()
	if err := svc.MkdirContext(ctx, "demo", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := svc.CreateFileContext(ctx, "demo", "docs/a"); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := svc.CreateFileContext(ctx, "demo", "docs/b"); err != nil {
		t.Fatalf("create b: %v", err)
	}
	// File onto existing file replaces.
	if err := svc.RenameContext(ctx, "demo", "docs/a", "docs/b"); err != nil {
		t.Fatalf("rename onto existing file: %v", err)
	}
	if backend.repo.FindFile("docs/a") != nil || backend.repo.FindFile("docs/b") == nil {
		t.Fatal("file-over-file replace failed")
	}
	// File onto directory is EISDIR.
	if _, err := svc.CreateFileContext(ctx, "demo", "docs/c"); err != nil {
		t.Fatalf("create c: %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "docs/c", "docs"); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("expected EISDIR, got %v", err)
	}
	// Directory onto file is ENOTDIR.
	if err := svc.RenameContext(ctx, "demo", "docs", "docs/c"); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("expected ENOTDIR, got %v", err)
	}
	// rename(x, x) on missing path is ENOENT.
	if err := svc.RenameContext(ctx, "demo", "missing", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ENOENT for rename(x,x) missing, got %v", err)
	}
	// rename(x, x) on existing path succeeds.
	if err := svc.RenameContext(ctx, "demo", "docs/c", "docs/c"); err != nil {
		t.Fatalf("rename(x,x) existing: %v", err)
	}
	// Directory onto empty directory replaces.
	if err := svc.MkdirContext(ctx, "demo", "empty-dir"); err != nil {
		t.Fatalf("mkdir empty-dir: %v", err)
	}
	if err := svc.MkdirContext(ctx, "demo", "docs/nested"); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "docs", "empty-dir"); err != nil {
		t.Fatalf("dir onto empty dir: %v", err)
	}
	if !backend.repo.HasDirectory("empty-dir/nested") || backend.repo.HasDirectory("docs") {
		t.Fatal("dir-over-empty-dir replace failed")
	}
}

func TestWhitespaceNamesEndToEnd(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(100)
	svc := NewService(backend)
	ctx := context.Background()
	const proj = "demo"

	// A directory whose name contains spaces is one specific name,
	// distinct from any other spelling; children address it verbatim.
	if err := svc.MkdirContext(ctx, proj, "my docs"); err != nil {
		t.Fatalf("mkdir spaced: %v", err)
	}
	if _, err := svc.CreateFileContext(ctx, proj, "my docs/readme.txt "); err != nil {
		t.Fatalf("create inside spaced dir (trailing-space filename): %v", err)
	}
	if _, err := svc.StatPathContext(ctx, proj, "my doc"); err == nil {
		t.Fatal("near-miss name must not address spaced entry")
	}
	if _, err := svc.StatPathContext(ctx, proj, "my docs/readme.txt "); err != nil {
		t.Fatalf("stat trailing-space filename: %v", err)
	}

	// Rename into a padded target lands on exactly that key and stays
	// addressable and removable - regression for raw-key divergence.
	if err := svc.MkdirContext(ctx, proj, "plain"); err != nil {
		t.Fatalf("mkdir plain: %v", err)
	}
	if err := svc.RenameContext(ctx, proj, "plain", " moved "); err != nil {
		t.Fatalf("rename to padded target: %v", err)
	}
	if _, err := svc.StatPathContext(ctx, proj, " moved "); err != nil {
		t.Fatalf("stat renamed padded dir: %v", err)
	}
	if err := svc.RmdirContext(ctx, proj, " moved "); err != nil {
		t.Fatalf("rmdir padded dir (raw-key integrity): %v", err)
	}

	if err := svc.RmdirContext(ctx, proj, "my docs"); !errors.Is(err, ErrNotEmpty) {
		t.Fatalf("expected ENOTEMPTY for spaced dir with child, got %v", err)
	}
	if !backend.repo.RemoveFile("my docs/readme.txt ") {
		t.Fatal("spaced file key not found for removal")
	}
	if err := svc.RmdirContext(ctx, proj, "my docs"); err != nil {
		t.Fatalf("rmdir spaced dir after cleanup: %v", err)
	}
}

// A REST-supplied length near MaxInt64 must not overflow the
// offset+length addition into a make() panic.
func TestReadFileAtOverflowLengthClamped(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(700)
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
	backend.seedFile("big.bin", []byte("0123456789"))
	data, err := svc.ReadFileAtContext(ctx, "demo", "big.bin", 0, math.MaxInt64)
	if err != nil {
		t.Fatalf("max-int read: %v", err)
	}
	if string(data) != "0123456789" {
		t.Fatalf("expected clamped full read, got %q", data)
	}
	data, err = svc.ReadFileAtContext(ctx, "demo", "big.bin", 8, math.MaxInt64)
	if err != nil || string(data) != "89" {
		t.Fatalf("expected clamped tail read, got %q %v", data, err)
	}
}

// A whitespace-only name must never masquerade as the root
// directory. Until the metadata store stops collapsing such names to the
// root key (cross-file), the fs layer rejects them loudly.
func TestWhitespaceOnlyNameIsNotRoot(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(710)
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
	if _, err := svc.StatPathContext(ctx, "demo", " "); err == nil {
		t.Fatal("stat of a whitespace-only name must not return root")
	}
	if _, err := svc.CreateFileContext(ctx, "demo", " "); err == nil {
		t.Fatal("create of a whitespace-only name must not fabricate a root-keyed entry")
	}
	if backend.repo.FindFile("") != nil {
		t.Fatal("rejected create must not write a root-keyed phantom")
	}
	// Names with *significant* whitespace around real content stay legal.
	if _, err := svc.CreateFileContext(ctx, "demo", " pad "); err != nil {
		t.Fatalf("create padded name: %v", err)
	}
	if entry, err := svc.StatPathContext(ctx, "demo", " pad "); err != nil || entry.IsDir {
		t.Fatalf("stat padded name: %+v %v", entry, err)
	}
}

// Extending a file must stream fixed-size zero chunks instead of
// materializing the whole hole in RAM. The observable contract is the
// resulting content; the memory bound is structural (chunked loop).
func TestTruncateExtensionStreamsZeros(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(720)
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
	backend.seedFile("grow.bin", []byte("ab"))
	// Two MiB: more than one 1 MiB chunk, so the streaming loop iterates.
	if _, err := svc.TruncateFileContext(ctx, "demo", "grow.bin", 2<<20); err != nil {
		t.Fatalf("extend: %v", err)
	}
	data, err := svc.ReadFileAtContext(ctx, "demo", "grow.bin", 0, 2<<20)
	if err != nil || len(data) != 2<<20 {
		t.Fatalf("extended read: len=%d err=%v", len(data), err)
	}
	if string(data[:2]) != "ab" {
		t.Fatalf("head lost: %q", data[:2])
	}
	for _, b := range data[2:] {
		if b != 0 {
			t.Fatal("extension must be zeros")
		}
	}
	// Sparse write past EOF fills the hole the same way.
	if _, err := svc.WriteFileAtContext(ctx, "demo", "grow.bin", 3<<20, []byte("tail")); err != nil {
		t.Fatalf("sparse write: %v", err)
	}
	data, err = svc.ReadFileAtContext(ctx, "demo", "grow.bin", 3<<20, 4)
	if err != nil || string(data) != "tail" {
		t.Fatalf("sparse tail read: %q %v", data, err)
	}
}

// WithNoReplace must reject an existing destination inside the
// update transaction, not via a pre-stat the caller could race.
func TestRenameNoReplaceInTransaction(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(730)
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
	if _, err := svc.CreateFileContext(ctx, "demo", "src.txt"); err != nil {
		t.Fatalf("create src: %v", err)
	}
	if _, err := svc.CreateFileContext(ctx, "demo", "dst.txt"); err != nil {
		t.Fatalf("create dst: %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "src.txt", "dst.txt", WithNoReplace()); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected EEXIST from in-transaction no-replace, got %v", err)
	}
	if backend.repo.FindFile("src.txt") == nil || backend.repo.FindFile("dst.txt") == nil {
		t.Fatal("failed no-replace rename must not mutate the tree")
	}
	if err := svc.RenameContext(ctx, "demo", "src.txt", "fresh.txt", WithNoReplace()); err != nil {
		t.Fatalf("no-replace onto free name: %v", err)
	}
	// Without the option, replacement stays unconditional.
	if _, err := svc.CreateFileContext(ctx, "demo", "src.txt"); err != nil {
		t.Fatalf("recreate src: %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "src.txt", "fresh.txt"); err != nil {
		t.Fatalf("plain replace rename: %v", err)
	}
}

// CopyContext must check read access on the source, not just
// traversal, or unreadable 0600 files can be duplicated by strangers.
func TestCopyRequiresSourceReadAccess(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(740)
	backend.seedDir("mine")
	file := backend.seedFile("mine/secret.txt", []byte("hush"))
	file.Mode = 0o600
	file.UID = 11
	file.GID = 22
	backend.repo.UpsertFile("mine/secret.txt", *file, backend.now)
	dir := backend.repo.GetDirectory("mine")
	dir.Mode = 0o755
	backend.repo.Dirs["mine"] = *dir
	backend.seedDir("theirs")
	theirs := backend.repo.GetDirectory("theirs")
	theirs.Mode = 0o777
	backend.repo.Dirs["theirs"] = *theirs
	backend.repo.RebuildIndexes()
	svc := NewService(backend)
	attacker := WithIdentity(context.Background(), Identity{UID: 30, GID: 40, Groups: []uint32{40}})
	if err := svc.CopyContext(attacker, "demo", "mine/secret.txt", "theirs/copy.txt"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected EACCES copying unreadable source, got %v", err)
	}
	if backend.repo.FindFile("theirs/copy.txt") != nil {
		t.Fatal("denied copy must not create the destination")
	}
	owner := WithIdentity(context.Background(), Identity{UID: 11, GID: 22, Groups: []uint32{22}})
	if err := svc.CopyContext(owner, "demo", "mine/secret.txt", "theirs/copy.txt"); err != nil {
		t.Fatalf("owner copy: %v", err)
	}
}

// The file owner keeps the POSIX chgrp right (into a group they
// belong to) but may not hand the file away.
func TestCanChownOwnerRights(t *testing.T) {
	t.Parallel()
	entry := &EntryInfo{Path: "f", UID: 1000, GID: 100, Mode: 0o644}
	owner := WithIdentity(context.Background(), Identity{UID: 1000, GID: 100, Groups: []uint32{100, 200}})
	if err := CanChown(owner, entry, entry.UID, 200); err != nil {
		t.Fatalf("owner chgrp into member group: %v", err)
	}
	if err := CanChown(owner, entry, entry.UID, 999); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("owner chgrp into foreign group must be EPERM, got %v", err)
	}
	if err := CanChown(owner, entry, 1234, entry.GID); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("owner give-away must be EPERM, got %v", err)
	}
	stranger := WithIdentity(context.Background(), Identity{UID: 7, GID: 7, Groups: []uint32{7}})
	if err := CanChown(stranger, entry, entry.UID, 100); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("non-owner chown must be EPERM, got %v", err)
	}
	root := WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
	if err := CanChown(root, entry, 1234, 5678); err != nil {
		t.Fatalf("admin chown: %v", err)
	}
}

// Nits: a truncate to the current size must still bump mtime/ctime.
func TestTruncateSameSizeTouchesTimestamps(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(750)
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
	backend.seedFile("ts.bin", []byte("abc"))
	before := backend.repo.FindFile("ts.bin")
	beforeMtime, beforeCtime := before.ModifiedAt, before.ChangedAt
	backend.now = 760
	if _, err := svc.TruncateFileContext(ctx, "demo", "ts.bin", 3); err != nil {
		t.Fatalf("same-size truncate: %v", err)
	}
	after := backend.repo.FindFile("ts.bin")
	if after.ModifiedAt != 760 || after.ChangedAt != 760 {
		t.Fatalf("same-size truncate must update mtime/ctime: before=%d/%d after=%d/%d", beforeMtime, beforeCtime, after.ModifiedAt, after.ChangedAt)
	}
}
