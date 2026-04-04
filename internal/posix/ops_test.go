package posix

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type testBackend struct {
	repo *meta.RepoMetadata
	now  time.Time
}

func newTestBackend(now time.Time) *testBackend {
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

func (b *testBackend) GetOrCreateUploadReleaseContext(_ context.Context, _ string, repoMeta *meta.RepoMetadata, _ int, _ string) (string, string, error) {
	repoMeta.EnsureRelease("v1", b.now)
	return "v1", "upload", nil
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

func (b *testBackend) seedFile(path string) *meta.FileMetadata {
	file := meta.FileMetadata{Name: path, Kind: meta.NodeKindFile, Release: "v1", Mode: 0o644, UID: 1, GID: 2, UploadedAt: b.now, ModifiedAt: b.now, AccessedAt: b.now, ChangedAt: b.now}
	b.repo.UpsertFile(file, b.now)
	stored := b.repo.FindFile(path)
	clone := stored.Clone()
	return &clone
}

func TestServicePOSIXWorkflow(t *testing.T) {
	now := time.Unix(300, 0).UTC()
	backend := newTestBackend(now)
	backend.seedDir("docs")
	base := backend.seedFile("docs/base.txt")
	svc := NewService(backend)

	linked, err := svc.LinkContext(context.Background(), "demo", "docs/base.txt", "docs/alias.txt")
	if err != nil || linked.Inode == 0 {
		t.Fatalf("link: %+v %v", linked, err)
	}
	if err := svc.ChmodContext(context.Background(), "demo", "docs/base.txt", 0o600); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := svc.ChownContext(context.Background(), "demo", "docs/base.txt", 9, 10); err != nil {
		t.Fatalf("chown: %v", err)
	}
	if err := svc.ChtimesContext(context.Background(), "demo", "docs/base.txt", time.Unix(1, 0), time.Unix(2, 0)); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := svc.SetXAttrContext(context.Background(), "demo", "docs/base.txt", "user.note", []byte("hello")); err != nil {
		t.Fatalf("setxattr: %v", err)
	}
	value, err := svc.GetXAttrContext(context.Background(), "demo", "docs/alias.txt", "user.note")
	if err != nil || string(value) != "hello" {
		t.Fatalf("getxattr family: %q %v", value, err)
	}
	attrs, err := svc.ListXAttrContext(context.Background(), "demo", "docs/base.txt")
	if err != nil || len(attrs) != 1 || attrs[0] != "user.note" {
		t.Fatalf("listxattr: %v %v", attrs, err)
	}
	if err := svc.RemoveXAttrContext(context.Background(), "demo", "docs/base.txt", "user.note"); err != nil {
		t.Fatalf("removexattr: %v", err)
	}
	attrs, err = svc.ListXAttrContext(context.Background(), "demo", "docs/base.txt")
	if err != nil || len(attrs) != 0 {
		t.Fatalf("listxattr after remove: %v %v", attrs, err)
	}
	symlink, err := svc.SymlinkContext(context.Background(), "demo", "docs/base.txt", "docs/base.link")
	if err != nil || symlink.Kind != meta.NodeKindSymlink {
		t.Fatalf("symlink: %+v %v", symlink, err)
	}
	target, err := svc.ReadlinkContext(context.Background(), "demo", "docs/base.link")
	if err != nil || target != "docs/base.txt" {
		t.Fatalf("readlink: %q %v", target, err)
	}
	info := backend.repo.FindFile("docs/alias.txt")
	if info.Mode != 0o600 || info.UID != 9 || info.GID != 10 || info.Inode != base.Inode {
		t.Fatalf("hardlink family metadata not propagated: %+v", info)
	}
	rootAttrsBefore, err := svc.ListXAttrContext(context.Background(), "demo", "")
	if err != nil || len(rootAttrsBefore) != 0 {
		t.Fatalf("expected empty root xattrs: %v %v", rootAttrsBefore, err)
	}
	if err := svc.SetXAttrContext(context.Background(), "demo", "", "user.root", []byte("ok")); err != nil {
		t.Fatalf("set root xattr: %v", err)
	}
	rootValue, err := svc.GetXAttrContext(context.Background(), "demo", "", "user.root")
	if err != nil || string(rootValue) != "ok" {
		t.Fatalf("get root xattr: %q %v", rootValue, err)
	}
	if err := svc.RemoveXAttrContext(context.Background(), "demo", "", "user.root"); err != nil {
		t.Fatalf("remove root xattr: %v", err)
	}
	if !shfs.EntryInfoFromFile(base).IsDir && shfs.EntryInfoFromFile(base).Path != base.Name {
		t.Fatal("unexpected file entry conversion")
	}
}

func TestServicePOSIXErrorsAndHelpers(t *testing.T) {
	backend := newTestBackend(time.Unix(400, 0).UTC())
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
	if _, err := svc.LinkContext(context.Background(), "demo", "docs/base.txt", "docs/base.txt"); err == nil {
		t.Fatal("expected same-path link error")
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
	if err := UpdateFileFamily(&repo, 999, func(*meta.FileMetadata) {}); err == nil {
		t.Fatal("expected missing inode family error")
	}
	if err := TouchInodeFamilyChangedAt(&repo, 999, backend.now); err == nil {
		t.Fatal("expected missing inode touch error")
	}
	if ChooseNonZeroTime() != (time.Time{}) || CloneStringMap(nil) != nil {
		t.Fatal("helper coverage failure")
	}
}
