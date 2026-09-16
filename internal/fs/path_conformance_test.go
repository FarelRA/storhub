package fs

import (
	"context"
	"errors"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Path-resolution conformance: every path-taking operation must resolve user paths
// physically (symlink components spliced, ".." popping the resolved stack)
// before touching the repository, and the DAC checks must see the real
// traversal chain. These tests drive the public Service verbs, not the
// resolver directly.

func conformanceService(t *testing.T) (*Service, *testBackend) {
	t.Helper()
	backend := newTestBackend(100)
	backend.seedDir("a")
	backend.seedDir("a/b")
	backend.seedDir("a/b/c")
	backend.seedFile("a/b/c/f.txt", []byte("hello"))
	backend.repo.UpsertFile("a/link", meta.FileMeta{Symlink: "b/c", Mode: 0o777, UID: 1, GID: 2, UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now}, backend.now)
	return NewService(backend), backend
}

func rootTestCtx() context.Context {
	return WithIdentity(context.Background(), Identity{UID: 0, GID: 0})
}

func TestConformanceReadThroughSymlinkedDir(t *testing.T) {
	t.Parallel()
	svc, _ := conformanceService(t)
	data, err := svc.ReadFileAtContext(context.Background(), "demo", "a/link/f.txt", 0, 5)
	if err != nil || string(data) != "hello" {
		t.Fatalf("read through symlinked dir: %q %v", data, err)
	}
	// ".." through the symlink addresses the PHYSICAL parent a/b.
	entry, err := svc.StatPathContext(context.Background(), "demo", "a/link/..")
	if err != nil {
		t.Fatalf("stat .. through symlink: %v", err)
	}
	if entry.Path != "a/b" || !entry.IsDir {
		t.Fatalf("stat .. through symlink: got %+v, want dir a/b", entry)
	}
	// lstat semantics survive at the final component.
	linkEntry, err := svc.StatPathContext(context.Background(), "demo", "a/link")
	if err != nil {
		t.Fatalf("stat symlink: %v", err)
	}
	if !linkEntry.IsSymlink || linkEntry.SymlinkTarget != "b/c" {
		t.Fatalf("stat symlink must report the link itself: %+v", linkEntry)
	}
}

func TestConformanceWriteThroughSymlinkedDir(t *testing.T) {
	t.Parallel()
	ctx := rootTestCtx()
	svc, backend := conformanceService(t)
	if _, err := svc.WriteFileAtContext(ctx, "demo", "a/link/f.txt", 0, []byte("J")); err != nil {
		t.Fatalf("write through symlinked dir: %v", err)
	}
	file := backend.repo.FindFile("a/b/c/f.txt")
	if file == nil {
		t.Fatal("target vanished")
	}
	data, err := svc.ReadFileAtContext(context.Background(), "demo", "a/b/c/f.txt", 0, 5)
	if err != nil || string(data) != "Jello" {
		t.Fatalf("write through symlink must land on the target: %q %v", data, err)
	}
	if _, err := svc.CreateFileContext(ctx, "demo", "a/link/new.txt"); err != nil {
		t.Fatalf("create through symlinked dir: %v", err)
	}
	if backend.repo.FindFile("a/b/c/new.txt") == nil {
		t.Fatal("created file must live under the resolved directory")
	}
	if err := svc.MkdirContext(ctx, "demo", "a/link/sub"); err != nil {
		t.Fatalf("mkdir through symlinked dir: %v", err)
	}
	if !backend.repo.HasDirectory("a/b/c/sub") {
		t.Fatal("mkdir must create under the resolved directory")
	}
}

func TestConformanceReadDirThroughSymlinkedDir(t *testing.T) {
	t.Parallel()
	svc, _ := conformanceService(t)
	entries, err := svc.ReadDirContext(context.Background(), "demo", "a/link")
	if err != nil {
		t.Fatalf("readdir through symlinked dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "f.txt" {
		t.Fatalf("unexpected listing: %+v", entries)
	}
}

func TestConformanceUnlinkAndRmdirDoNotFollowFinalSymlink(t *testing.T) {
	t.Parallel()
	ctx := rootTestCtx()
	svc, backend := conformanceService(t)
	// rmdir of a symlink-to-dir must fail ENOTDIR, not remove the target.
	if err := svc.RmdirContext(ctx, "demo", "a/link"); err == nil {
		t.Fatal("rmdir on a symlink must fail")
	}
	if !backend.repo.HasDirectory("a/b/c") || backend.repo.FindFile("a/link") == nil {
		t.Fatal("failed rmdir must not disturb the link or its target")
	}
	// rename moves the link itself, never its target.
	if err := svc.RenameContext(ctx, "demo", "a/link", "a/moved"); err != nil {
		t.Fatalf("rename symlink: %v", err)
	}
	if backend.repo.FindFile("a/moved") == nil || backend.repo.FindFile("a/link") != nil {
		t.Fatal("rename must move the link entry itself")
	}
	if !backend.repo.HasDirectory("a/b/c") {
		t.Fatal("rename of a link must not move its target")
	}
}

func TestConformanceEscapeStillRejectedThroughOps(t *testing.T) {
	t.Parallel()
	svc, _ := conformanceService(t)
	for _, path := range []string{"../x", "a/link/../../../../x"} {
		if _, err := svc.ReadFileAtContext(context.Background(), "demo", path, 0, 1); err == nil {
			t.Fatalf("ReadFileAt(%q) must reject the escape", path)
		}
	}
}

func TestConformanceAbsoluteLinkParentChainStillGuarded(t *testing.T) {
	t.Parallel()
	// A 0700 directory holding an absolute link must not leak the target
	// to a caller without exec on it, through the migrated ops too.
	svc, backend := conformanceService(t)
	backend.seedDir("pub")
	backend.seedFile("pub/secret.txt", []byte("shh"))
	backend.repo.UpsertFile("a/abs", meta.FileMeta{Symlink: "/pub/secret.txt", Mode: 0o777, UID: 1, GID: 2, UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now}, backend.now)
	dir := backend.repo.GetDirectory("a")
	dir.Mode = 0o700
	// Pin the owner explicitly: EnsureDirectory inherits the process UID,
	// and CI runners are UID 1001, which would otherwise make the "attacker"
	// below the directory's own owner and silently pass the check.
	dir.UID = 0
	dir.GID = 0
	backend.repo.Dirs()["a"] = *dir
	attacker := WithIdentity(context.Background(), Identity{UID: 1001, GID: 1001, Groups: []uint32{1001}})
	if _, err := svc.ReadFileAtContext(attacker, "demo", "a/abs", 0, 3); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("read through absolute link inside a 0700 dir must be EACCES, got %v", err)
	}
	if _, err := svc.ReadFileAtContext(attacker, "demo", "a/abs/..", 0, 3); !errors.Is(err, syscall.EACCES) {
		t.Fatalf(".. through absolute link must still hit the EACCES chain, got %v", err)
	}
}
