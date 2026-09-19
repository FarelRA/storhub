package posix

import (
	"context"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Path-resolution conformance at the POSIX facade: metadata verbs follow the final
// symlink (chmod/setxattr address the TARGET reached through a symlinked
// directory), link-creating verbs resolve intermediate components, and
// readlink/symlink creation operate on the link itself.
//
// Single through-verb smoke: the resolver matrix itself lives in fs
// (path_conformance + symlink_physical); posix keeps one test proving the
// facade routes through it, plus the escape rejection.
func TestPosixConformanceSymlinkSmoke(t *testing.T) {
	t.Parallel()
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})
	svc, backend := posixConformanceService(t)
	// chmod follows the final symlink to the target.
	if err := svc.ChmodContext(ctx, "demo", "a/link/f.txt", 0o600); err != nil {
		t.Fatalf("chmod through symlinked dir: %v", err)
	}
	file := backend.repo.FindFile("a/b/c/f.txt")
	if file == nil || file.Mode != 0o600 {
		t.Fatalf("chmod must reach the resolved target: %+v", file)
	}
	// readlink resolves intermediate components but not the final one.
	target, err := svc.ReadlinkContext(ctx, "demo", "a/link/other")
	if err != nil || target != "f.txt" {
		t.Fatalf("readlink through symlinked dir: %q %v", target, err)
	}
	// symlink(2) creates at the resolved parent.
	if _, err := svc.SymlinkContext(ctx, "demo", "f.txt", "a/link/created"); err != nil {
		t.Fatalf("symlink through symlinked dir: %v", err)
	}
	link := backend.repo.FindFile("a/b/c/created")
	if link == nil || link.Symlink != "f.txt" {
		t.Fatalf("symlink must be created under the resolved directory: %+v", link)
	}
	// link(2) hard links the resolved source into the resolved directory.
	if _, err := svc.LinkContext(ctx, "demo", "a/link/f.txt", "a/link/hard"); err != nil {
		t.Fatalf("link through symlinked dir: %v", err)
	}
	hard := backend.repo.FindFile("a/b/c/hard")
	src := backend.repo.FindFile("a/b/c/f.txt")
	if hard == nil || src == nil || hard.Inode != src.Inode {
		t.Fatalf("hard link must share the resolved source inode: hard=%+v src=%+v", hard, src)
	}
	// setxattr through ..-via-symlink lands on the resolved target.
	if err := svc.SetXAttrContext(ctx, "demo", "a/link/../c/f.txt", "user.note", []byte("v")); err != nil {
		t.Fatalf("setxattr through ..-via-symlink: %v", err)
	}
	value, err := svc.GetXAttrContext(ctx, "demo", "a/b/c/f.txt", "user.note")
	if err != nil || string(value) != "v" {
		t.Fatalf("xattr must land on the resolved target: %q %v", value, err)
	}
	// Escapes stay rejected through the facade.
	if err := svc.ChmodContext(ctx, "demo", "a/link/../../../../x", 0o600); err == nil {
		t.Fatal("chmod escaping the root through a symlinked dir must fail")
	}
	if _, err := svc.ReadlinkContext(ctx, "demo", "../x"); err == nil {
		t.Fatal("readlink on an escaping path must fail")
	}
}
func posixConformanceService(t *testing.T) (*Service, *testBackend) {
	t.Helper()
	backend := newTestBackend(500)
	backend.seedDir("a")
	backend.seedDir("a/b")
	backend.seedDir("a/b/c")
	backend.seedFile("a/b/c/f.txt")
	backend.repo.UpsertFile("a/link", meta.FileMeta{Symlink: "b/c", Mode: 0o777, UID: 1, GID: 2, UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now}, backend.now)
	backend.repo.UpsertFile("a/b/c/other", meta.FileMeta{Symlink: "f.txt", Mode: 0o777, UID: 1, GID: 2, UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now}, backend.now)
	return NewService(backend), backend
}
