package fs

import (
	"context"
	"errors"
	"syscall"
	"testing"
)

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

// WithNoReplace must reject an existing destination inside the
// update transaction, not via a pre-stat the caller could race.
func TestRenameNoReplaceInTransaction(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(730)
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0, Admin: true})
	if _, err := svc.CreateFileContext(ctx, "demo", "src.txt"); err != nil {
		t.Fatalf("create src: %v", err)
	}
	if _, err := svc.CreateFileContext(ctx, "demo", "dst.txt"); err != nil {
		t.Fatalf("create dst: %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "src.txt", "dst.txt", WithNoReplace()); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected EEXIST from in-transaction noreplace, got %v", err)
	}
	if backend.repo.FindFile("src.txt") == nil || backend.repo.FindFile("dst.txt") == nil {
		t.Fatal("failed noreplace rename must not mutate the tree")
	}
	if err := svc.RenameContext(ctx, "demo", "src.txt", "fresh.txt", WithNoReplace()); err != nil {
		t.Fatalf("noreplace onto free name: %v", err)
	}
	// Without the option, replacement stays unconditional.
	if _, err := svc.CreateFileContext(ctx, "demo", "src.txt"); err != nil {
		t.Fatalf("recreate src: %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "src.txt", "fresh.txt"); err != nil {
		t.Fatalf("plain replace rename: %v", err)
	}
}
