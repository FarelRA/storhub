package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type testBackend struct {
	repo       *meta.RepoMetadata
	now        time.Time
	nextAsset  int64
	assetBytes map[int64][]byte
}

func newTestBackend(now time.Time) *testBackend {
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

func (b *testBackend) GetOrCreateUploadReleaseContext(_ context.Context, _ string, repoMeta *meta.RepoMetadata, _ int, _ string) (string, string, error) {
	repoMeta.EnsureRelease("v1", b.now)
	return "v1", "upload", nil
}

func (b *testBackend) PatchFileWithMetadataContext(_ context.Context, _ string, cleanName string, _ *meta.RepoMetadata, fileMeta *meta.FileMetadata, offset, deleteSize int64, edit []byte) (*meta.FileMetadata, error) {
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
		repo.UpsertFile(updated, b.now)
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

func (b *testBackend) Now() time.Time { return b.now }

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

func (b *testBackend) seedFile(path string, data []byte) *meta.FileMetadata {
	file := meta.FileMetadata{Name: path, Kind: meta.NodeKindFile, Release: "v1", Mode: 0o644, UID: 1, GID: 2, UploadedAt: b.now, ModifiedAt: b.now, AccessedAt: b.now, ChangedAt: b.now}
	b.storeFile(&file, data)
	b.repo.UpsertFile(file, b.now)
	stored := b.repo.FindFile(path)
	clone := stored.Clone()
	return &clone
}

func (b *testBackend) storeFile(file *meta.FileMetadata, data []byte) {
	assetID := b.nextAsset
	b.nextAsset++
	b.assetBytes[assetID] = append([]byte(nil), data...)
	file.Size = int64(len(data))
	file.Chunks = []meta.ChunkInfo{{Index: 0, Offset: 0, Size: int64(len(data)), AssetID: assetID, Release: file.Release}}
	if len(data) == 0 {
		file.Chunks = nil
	}
}

func (b *testBackend) fileData(file *meta.FileMetadata) ([]byte, error) {
	if len(file.Chunks) == 0 {
		return nil, nil
	}
	chunk := file.Chunks[0]
	data, ok := b.assetBytes[chunk.AssetID]
	if !ok {
		return nil, io.EOF
	}
	return append([]byte(nil), data...), nil
}

func TestServiceWorkflowAndHelpers(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	backend := newTestBackend(now)
	svc := NewService(backend)
	backend.seedDir("docs")
	seeded := backend.seedFile("docs/readme.txt", []byte("hello"))

	created, err := svc.CreateFileContext(context.Background(), "demo", "docs/empty.txt")
	if err != nil || created.Name != "docs/empty.txt" || created.Size != 0 {
		returnFail(t, "create file", err, created)
	}
	if err := svc.MkdirContext(context.Background(), "demo", "docs/sub"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := svc.AppendFileContext(context.Background(), "demo", "docs/readme.txt", []byte(" world")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := svc.WriteFileAtContext(context.Background(), "demo", "docs/readme.txt", 0, []byte("H")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := svc.TruncateFileContext(context.Background(), "demo", "docs/readme.txt", 5); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	data, err := svc.ReadFileAtContext(context.Background(), "demo", "docs/readme.txt", 0, 5)
	if err != nil || string(data) != "Hello" {
		t.Fatalf("read after writes: %q %v", data, err)
	}
	if err := svc.RenameContext(context.Background(), "demo", "docs/readme.txt", "docs/sub/final.txt"); err != nil {
		t.Fatalf("rename file: %v", err)
	}
	if err := svc.RenameContext(context.Background(), "demo", "docs/sub", "docs/archive"); err != nil {
		t.Fatalf("rename dir: %v", err)
	}
	entry, err := svc.StatPathContext(context.Background(), "demo", "docs/archive/final.txt")
	if err != nil || entry.Path != "docs/archive/final.txt" || entry.Size != 5 {
		t.Fatalf("stat path: %+v %v", entry, err)
	}
	dirs, err := svc.ReadDirContext(context.Background(), "demo", "docs")
	if err != nil || len(dirs) != 2 {
		t.Fatalf("readdir: %+v %v", dirs, err)
	}
	stats, err := svc.StatFSContext(context.Background(), "demo")
	if err != nil || stats.Files < 2 || stats.Directories < 2 {
		t.Fatalf("statfs: %+v %v", stats, err)
	}
	if err := svc.RmdirContext(context.Background(), "demo", "docs/archive"); err == nil {
		t.Fatal("expected non-empty rmdir failure")
	}
	if err := svc.RenameContext(context.Background(), "demo", "docs", "docs/archive/nested"); err == nil {
		t.Fatal("expected self-rename failure")
	}
	if _, err := svc.ReadFileAtContext(context.Background(), "demo", "docs/archive/final.txt", 99, 1); !errors.Is(err, io.EOF) {
		t.Fatalf("expected eof, got %v", err)
	}
	if info := EntryInfoFromFile(seeded); info.Path != seeded.Name || !EntryInfoFromDirectory(&meta.DirectoryMetadata{Path: "docs", Inode: 2, Mode: 0o755}).IsDir {
		t.Fatal("entry helper conversion failed")
	}
	if DirEntryFromFile(*seeded).Path != seeded.Name || !DirEntryFromDirectory(meta.DirectoryMetadata{Path: "docs/sub"}).IsDir {
		t.Fatal("dir entry helper conversion failed")
	}
	if CountUniqueInodes(backend.repo) == 0 || MinInt64(1, 2) != 1 || MaxInt64(1, 2) != 2 {
		t.Fatal("helper counts/min/max failed")
	}
	if err := svc.RmdirContext(context.Background(), "demo", "docs/archive"); err == nil {
		t.Fatal("expected archive to remain non-empty")
	}
}

func TestServiceErrors(t *testing.T) {
	backend := newTestBackend(time.Unix(200, 0).UTC())
	svc := NewService(backend)
	backend.seedDir("docs")
	backend.seedFile("docs/link", []byte("abc"))
	backend.repo.UpsertFile(meta.FileMetadata{Name: "docs/symlink", Kind: meta.NodeKindSymlink, Release: "v1", Mode: 0o777, SymlinkTarget: "docs/link", UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now}, backend.now)

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
	if _, err := svc.ReadFileAtContext(context.Background(), "demo", "docs/symlink", 0, 1); err == nil {
		t.Fatal("expected symlink read failure")
	}
	if _, err := svc.AppendFileContext(context.Background(), "demo", "docs/symlink", []byte("x")); err == nil {
		t.Fatal("expected symlink append failure")
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
	backend := newTestBackend(time.Unix(250, 0).UTC())
	backend.seedDir("private")
	file := backend.seedFile("private/note.txt", []byte("secret"))
	file.Mode = 0o640
	file.UID = 11
	file.GID = 22
	backend.repo.UpsertFile(*file, backend.now)
	dir := backend.repo.GetDirectory("private")
	dir.Mode = 0o750
	dir.UID = 11
	dir.GID = 22
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
	now := time.Unix(260, 0).UTC()
	backend := newTestBackend(now)
	backend.seedDir("shared")
	parent := backend.repo.GetDirectory("shared")
	parent.Mode = 0o2775
	parent.UID = 50
	parent.GID = 60
	parent.ModifiedAt = now.Add(-time.Hour)
	parent.ChangedAt = now.Add(-time.Hour)
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
	if !parent.ModifiedAt.Equal(now) || !parent.ChangedAt.Equal(now) {
		t.Fatalf("expected parent timestamps touched, got mtime=%v ctime=%v", parent.ModifiedAt, parent.ChangedAt)
	}
}

func TestCreateAndMkdirUseCallerOwnership(t *testing.T) {
	backend := newTestBackend(time.Unix(265, 0).UTC())
	backend.seedDir("docs")
	backend.repo.GetDirectory("docs").Mode = 0o777
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
	dir := backend.repo.GetDirectory("docs/subdir")
	if dir == nil || dir.UID != 986 || dir.GID != 986 {
		t.Fatalf("expected caller-owned dir, got %+v", dir)
	}
}

func (b *testBackend) AtimePolicy() storcfg.AtimePolicy { return storcfg.AtimeRelatime }

func returnFail(t *testing.T, label string, err error, value any) {
	t.Helper()
	t.Fatalf("%s: value=%+v err=%v", label, value, err)
}
