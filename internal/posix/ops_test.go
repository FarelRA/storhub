package posix

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"syscall"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type testBackend struct {
	repo *meta.RepoMetadata
	now  int64
}

func newTestBackend(now int64) *testBackend {
	repo := meta.NewRepoMetadata("demo")
	repo.EnsureRelease("v1", now)
	return &testBackend{repo: repo, now: now}
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

func (b *testBackend) Now() int64 { return b.now }

func (b *testBackend) AtimePolicy() storcfg.AtimePolicy { return storcfg.AtimeRelatime }

func (b *testBackend) FileNotFound(path string) error { return fmt.Errorf("not found: %s", path) }

func (b *testBackend) DefaultFileMode(kind meta.NodeKind) uint32 {
	if kind == meta.NodeKindSymlink {
		return 0o777
	}
	return 0o644
}

func (b *testBackend) DefaultOwnerIDs() (uint32, uint32) { return 1, 2 }

func (b *testBackend) PatchFileWithMetadataContext(context.Context, string, string, *meta.RepoMetadata, *meta.FileMeta, int64, int64, []byte) (*meta.FileMeta, error) {
	panic("patch not exercised by posix tests")
}

func (b *testBackend) FillAssetRangeContext(context.Context, string, meta.ChunkInfo, []byte) error {
	panic("asset fills not exercised by posix tests")
}

func (b *testBackend) seedDir(path string) {
	b.repo.EnsureDirectory(path, b.now)
}

func (b *testBackend) seedFile(path string) *meta.FileMeta {
	file := meta.FileMeta{Mode: 0o644, UID: 1, GID: 2, Chunks: []int64{}, UploadedAt: b.now, ModifiedAt: b.now, AccessedAt: b.now, ChangedAt: b.now}
	b.repo.UpsertFile(path, file, b.now)
	stored := b.repo.FindFile(path)
	clone := stored.Clone()
	return &clone
}

func TestServicePOSIXWorkflow(t *testing.T) {
	// The workflow runs as an explicitly identified root caller; absent
	// identities fail closed to the process user and own nothing here.
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0})
	now := int64(300)
	backend := newTestBackend(now)
	backend.seedDir("docs")
	base := backend.seedFile("docs/base.txt")
	svc := NewService(backend)

	linked, err := svc.LinkContext(ctx, "demo", "docs/base.txt", "docs/alias.txt")
	if err != nil || linked.Inode == 0 {
		t.Fatalf("link: %+v %v", linked, err)
	}
	if err := svc.ChmodContext(ctx, "demo", "docs/base.txt", 0o600); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := svc.ChownContext(ctx, "demo", "docs/base.txt", 9, 10); err != nil {
		t.Fatalf("chown: %v", err)
	}
	if err := svc.ChtimesContext(ctx, "demo", "docs/base.txt", int64(1), int64(2)); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := svc.SetXAttrContext(ctx, "demo", "docs/base.txt", "user.note", []byte("hello")); err != nil {
		t.Fatalf("setxattr: %v", err)
	}
	value, err := svc.GetXAttrContext(ctx, "demo", "docs/alias.txt", "user.note")
	if err != nil || string(value) != "hello" {
		t.Fatalf("getxattr family: %q %v", value, err)
	}
	attrs, err := svc.ListXAttrContext(ctx, "demo", "docs/base.txt")
	if err != nil || len(attrs) != 1 || attrs[0] != "user.note" {
		t.Fatalf("listxattr: %v %v", attrs, err)
	}
	if err := svc.RemoveXAttrContext(ctx, "demo", "docs/base.txt", "user.note"); err != nil {
		t.Fatalf("removexattr: %v", err)
	}
	attrs, err = svc.ListXAttrContext(ctx, "demo", "docs/base.txt")
	if err != nil || len(attrs) != 0 {
		t.Fatalf("listxattr after remove: %v %v", attrs, err)
	}
	symlink, err := svc.SymlinkContext(ctx, "demo", "docs/base.txt", "docs/base.link")
	if err != nil || symlink.Symlink != "docs/base.txt" {
		t.Fatalf("symlink: %+v %v", symlink, err)
	}
	target, err := svc.ReadlinkContext(ctx, "demo", "docs/base.link")
	if err != nil || target != "docs/base.txt" {
		t.Fatalf("readlink: %q %v", target, err)
	}
	info := backend.repo.FindFile("docs/alias.txt")
	if info.Mode != 0o600 || info.UID != 9 || info.GID != 10 || info.Inode != base.Inode {
		t.Fatalf("hardlink family metadata not propagated: %+v", info)
	}
	rootAttrsBefore, err := svc.ListXAttrContext(ctx, "demo", "")
	if err != nil || len(rootAttrsBefore) != 0 {
		t.Fatalf("expected empty root xattrs: %v %v", rootAttrsBefore, err)
	}
	if err := svc.SetXAttrContext(ctx, "demo", "", "user.root", []byte("ok")); err != nil {
		t.Fatalf("set root xattr: %v", err)
	}
	rootValue, err := svc.GetXAttrContext(ctx, "demo", "", "user.root")
	if err != nil || string(rootValue) != "ok" {
		t.Fatalf("get root xattr: %q %v", rootValue, err)
	}
	if err := svc.RemoveXAttrContext(ctx, "demo", "", "user.root"); err != nil {
		t.Fatalf("remove root xattr: %v", err)
	}
	if !shfs.EntryInfoFromFile(base, "docs/base.txt", backend.repo.FileNLink("docs/base.txt")).IsDir && shfs.EntryInfoFromFile(base, "docs/base.txt", backend.repo.FileNLink("docs/base.txt")).Path != "docs/base.txt" {
		t.Fatal("unexpected file entry conversion")
	}
}

func TestServicePOSIXErrorsAndHelpers(t *testing.T) {
	backend := newTestBackend(int64(400))
	backend.seedDir("docs")
	backend.seedFile("docs/base.txt")
	svc := NewService(backend)

	if _, err := svc.SymlinkContext(context.Background(), "bad/project", "target", "docs/link"); err == nil {
		t.Fatal("expected project validation error")
	}
	if _, err := svc.SymlinkContext(context.Background(), "demo", "target", ""); err == nil {
		t.Fatal("expected missing symlink path error")
	}
	if _, err := svc.LinkContext(context.Background(), "demo", "", "docs/link"); err == nil {
		t.Fatal("expected missing link paths error")
	}
	// POSIX: link(x, x) succeeds when x exists.
	if _, err := svc.LinkContext(context.Background(), "demo", "docs/base.txt", "docs/base.txt"); err != nil {
		t.Fatalf("expected same-path link to succeed on existing file, got %v", err)
	}
	if _, err := svc.LinkContext(context.Background(), "demo", "docs/missing.txt", "docs/missing.txt"); err == nil {
		t.Fatal("expected same-path link on missing path to fail")
	}
	if _, err := svc.ReadlinkContext(context.Background(), "demo", "docs/base.txt"); err == nil {
		t.Fatal("expected non-symlink readlink error")
	}
	if err := svc.SetXAttrContext(context.Background(), "demo", "docs/base.txt", "", []byte("x")); err == nil {
		t.Fatal("expected xattr name error")
	}
	if _, err := svc.GetXAttrContext(context.Background(), "demo", "docs/base.txt", "missing"); err == nil {
		t.Fatal("expected missing xattr error")
	}
	if _, err := svc.ListXAttrContext(context.Background(), "demo", "docs/missing"); err == nil {
		t.Fatal("expected missing path error")
	}
	if err := svc.RemoveXAttrContext(context.Background(), "demo", "docs/base.txt", "missing"); err == nil {
		t.Fatal("expected missing xattr removal error")
	}
	repo := backend.repo.Clone()
	if err := UpdateFileFamily(&repo, 999, func(*meta.FileMeta) {}); err == nil {
		t.Fatal("expected missing inode family error")
	}
	if err := TouchInodeFamilyChangedAt(&repo, 999, backend.now); err == nil {
		t.Fatal("expected missing inode touch error")
	}
	if ChooseNonZeroTime() != 0 || CloneStringMap(nil) != nil {
		t.Fatal("helper coverage failure")
	}
}

func TestServicePOSIXPermissionEnforcement(t *testing.T) {
	backend := newTestBackend(int64(500))
	backend.seedDir("docs")
	base := backend.seedFile("docs/base.txt")
	base.Mode = 0o640
	base.UID = 7
	base.GID = 8
	backend.repo.UpsertFile("docs/base.txt", *base, backend.now)
	dir := backend.repo.GetDirectory("docs")
	dir.Mode = 0o755
	dir.UID = 7
	dir.GID = 8
	backend.repo.RebuildIndexes()
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 9, GID: 10, Groups: []uint32{10}})
	if err := svc.ChmodContext(ctx, "demo", "docs/base.txt", 0o600); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("expected chmod denial, got %v", err)
	}
	if err := svc.SetXAttrContext(ctx, "demo", "docs/base.txt", "user.note", []byte("x")); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected xattr write denial, got %v", err)
	}
	ownerCtx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 7, GID: 8, Groups: []uint32{8}})
	if err := svc.ChmodContext(ownerCtx, "demo", "docs/base.txt", 0o600); err != nil {
		t.Fatalf("expected owner chmod success, got %v", err)
	}
}

func TestServicePOSIXDirectoryMetadataUpdatesPersist(t *testing.T) {
	backend := newTestBackend(int64(600))
	backend.seedDir("docs")
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})
	if err := svc.ChownContext(ctx, "demo", "docs", 986, 986); err != nil {
		t.Fatalf("chown dir: %v", err)
	}
	if err := svc.ChmodContext(ctx, "demo", "docs", 0o775); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	dir := backend.repo.GetDirectory("docs")
	if dir == nil {
		t.Fatal("expected docs directory")
	}
	if dir.UID != 986 || dir.GID != 986 {
		t.Fatalf("expected persisted dir ownership, got %d:%d", dir.UID, dir.GID)
	}
	if dir.Mode != 0o775 {
		t.Fatalf("expected persisted dir mode, got %#o", dir.Mode)
	}
}

func TestServicePOSIXSymlinkUsesCallerOwnershipAndHardlinkPreservesSourceOwner(t *testing.T) {
	backend := newTestBackend(int64(610))
	backend.seedDir("docs")
	dir := backend.repo.GetDirectory("docs")
	dir.Mode = 0o777
	backend.repo.Dirs["docs"] = *dir
	base := backend.seedFile("docs/base.txt")
	base.UID = 1000
	base.GID = 1000
	backend.repo.UpsertFile("docs/base.txt", *base, backend.now)
	backend.repo.RebuildIndexes()
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 986, GID: 986, Groups: []uint32{986}})
	symlink, err := svc.SymlinkContext(ctx, "demo", "docs/base.txt", "docs/link.txt")
	if err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if symlink.UID != 986 || symlink.GID != 986 {
		t.Fatalf("expected caller-owned symlink, got %d:%d", symlink.UID, symlink.GID)
	}
	hardlink, err := svc.LinkContext(ctx, "demo", "docs/base.txt", "docs/hard.txt")
	if err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	if hardlink.UID != 1000 || hardlink.GID != 1000 {
		t.Fatalf("expected hardlink to preserve source owner, got %d:%d", hardlink.UID, hardlink.GID)
	}
}

// TestXAttrBinaryValuesRoundTrip pins byte-fidelity for extended attributes:
// values are opaque bytes, so NULs, invalid UTF-8, and empty payloads must
// survive set/get/list unchanged.
func TestXAttrBinaryValuesRoundTrip(t *testing.T) {
	now := int64(300)
	backend := newTestBackend(now)
	backend.seedDir("docs")
	base := backend.seedFile("docs/base.txt")
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: base.UID, GID: base.GID})

	payloads := [][]byte{
		[]byte("plain"),
		{0x00, 0x01, 0xFF, 0xFE, 0x00},
		{},
		bytes.Repeat([]byte{0x80}, 256),
	}
	for i, payload := range payloads {
		name := fmt.Sprintf("user.bin%d", i)
		if err := svc.SetXAttrContext(ctx, "demo", "docs/base.txt", name, payload); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
		got, err := svc.GetXAttrContext(ctx, "demo", "docs/base.txt", name)
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("value %s corrupted: got %x want %x", name, got, payload)
		}
	}
	names, err := svc.ListXAttrContext(ctx, "demo", "docs/base.txt")
	if err != nil || len(names) != len(payloads) {
		t.Fatalf("list: %+v %v", names, err)
	}
}

// TestChownKeepOwnerSentinel pins POSIX chown(2) semantics: (uid_t)-1
// (all-ones) leaves that owner field unchanged instead of setting it to
// 4294967295.
func TestChownKeepOwnerSentinel(t *testing.T) {
	backend := newTestBackend(500)
	backend.seedDir("docs")
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0})

	if err := svc.ChownContext(ctx, "demo", "docs", 1000, 2000); err != nil {
		t.Fatalf("initial chown: %v", err)
	}
	assertOwners := func(label string, wantUID, wantGID uint32) {
		t.Helper()
		repo, _, _, dir, err := svc.lookupPath(ctx, "demo", "docs")
		if err != nil {
			t.Fatalf("%s: lookup: %v", label, err)
		}
		_ = repo
		if dir.UID != wantUID || dir.GID != wantGID {
			t.Fatalf("%s: uid=%d gid=%d, want %d/%d", label, dir.UID, dir.GID, wantUID, wantGID)
		}
	}
	assertOwners("precondition", 1000, 2000)

	const keep = ^uint32(0)
	if err := svc.ChownContext(ctx, "demo", "docs", keep, 3000); err != nil {
		t.Fatalf("chown keep-uid: %v", err)
	}
	assertOwners("keep-uid", 1000, 3000)

	if err := svc.ChownContext(ctx, "demo", "docs", 1500, keep); err != nil {
		t.Fatalf("chown keep-gid: %v", err)
	}
	assertOwners("keep-gid", 1500, 3000)
}

// TestChtimesExplicitSemantics pins utimensat trinary behavior: nil omits,
// non-nil sets exactly (epoch zero included and marked authoritative).
func TestChtimesExplicitSemantics(t *testing.T) {
	backend := newTestBackend(500)
	backend.seedFile("ts.txt")
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0})
	epoch := time.Unix(0, 0)
	if err := svc.ChtimesExplicitContext(ctx, "demo", "ts.txt", &epoch, nil); err != nil {
		t.Fatalf("explicit chtimes: %v", err)
	}
	entry, err := svc.lookupEntryForAccess(ctx, "demo", "ts.txt")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if entry.AccessedAt != 0 {
		t.Fatalf("explicit epoch atime rewritten to %d", entry.AccessedAt)
	}
	before := entry.ModifiedAt
	if err := svc.ChtimesExplicitContext(ctx, "demo", "ts.txt", nil, &epoch); err != nil {
		t.Fatalf("second explicit chtimes: %v", err)
	}
	entry2, _ := svc.lookupEntryForAccess(ctx, "demo", "ts.txt")
	if entry2.ModifiedAt != 0 || entry2.AccessedAt != 0 {
		t.Fatalf("omit must not touch fields: %+v", entry2)
	}
	_ = before
	// Legacy verb keeps omit-on-zero contract.
	if err := svc.ChtimesContext(ctx, "demo", "ts.txt", 0, 0); err != nil {
		t.Fatalf("legacy chtimes: %v", err)
	}
	entry3, _ := svc.lookupEntryForAccess(ctx, "demo", "ts.txt")
	if entry3.AccessedAt == 0 && entry3.AccessedAt != svc.backend.Now() && entry3.AccessedAt < 1_000_000_000 {
		t.Fatalf("legacy zero should map to now-ish, got %d", entry3.AccessedAt)
	}
}
