package fs

import (
	"errors"
	"reflect"
	"syscall"
	"testing"
)

// Single-form typed errors, checked canonicalizer, loud size validation.
// RED-first: these fail before the fix.

// One typed sentinel per failure, errno preserved for the FUSE/REST edges
// via errors.Is/As on both faces.
func TestErrorSingleForm(t *testing.T) {
	t.Parallel()
	svc, backend := conformanceService(t)
	ctx := rootTestCtx()

	if _, err := svc.TruncateFileContext(ctx, "demo", "a/b/c/f.txt", -1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative truncate must fail with ErrInvalidArgument, got %v", err)
	}
	if _, err := svc.TruncateFileContext(ctx, "demo", "a/b/c/f.txt", -1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative truncate must stay EINVAL at the edges, got %v", err)
	}
	if _, err := svc.WriteFileAtContext(ctx, "demo", "a/b/c/f.txt", -1, []byte("x")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative write offset must fail with ErrInvalidArgument, got %v", err)
	}
	if _, err := svc.ReadFileAtContext(ctx, "demo", "a/b/c/f.txt", -1, 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative read offset must fail with ErrInvalidArgument, got %v", err)
	}
	if _, err := svc.ReadFileAtContext(ctx, "demo", "a/b/c/f.txt", 0, -1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative read length must fail with ErrInvalidArgument, got %v", err)
	}
	if err := svc.RmdirContext(ctx, "demo", "/"); !errors.Is(err, ErrBusy) {
		t.Fatalf("rmdir root must fail with ErrBusy, got %v", err)
	}
	if err := svc.RmdirContext(ctx, "demo", "/"); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("rmdir root must stay EBUSY at the edges, got %v", err)
	}
	if _, err := svc.TruncateFileContext(ctx, "demo", "a/b/c/f.txt", 1<<30); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("over-cap grow must fail with ErrTooLarge, got %v", err)
	}
	if _, err := svc.TruncateFileContext(ctx, "demo", "a/b/c/f.txt", 1<<30); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("over-cap grow must stay EFBIG at the edges, got %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "a/b/c/f.txt", "a/b"); !errors.Is(err, ErrIsDirectory) {
		t.Fatalf("file onto dir must fail with ErrIsDirectory, got %v", err)
	}
	if err := svc.RenameContext(ctx, "demo", "a/b/c/f.txt", "a/b"); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("file onto dir must stay EISDIR at the edges, got %v", err)
	}
	if err := svc.CopyContext(ctx, "demo", "a/b", "a/b/c/f.txt"); !errors.Is(err, ErrNotDirectory) {
		t.Fatalf("dir onto file must fail with ErrNotDirectory, got %v", err)
	}
	if _, err := svc.StatPathContext(ctx, "demo", "a/nope.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing path must fail with ErrNotFound, got %v", err)
	}
	if _, err := svc.StatPathContext(ctx, "demo", "a/nope.txt"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("missing path must stay ENOENT at the edges, got %v", err)
	}
	if err := CheckListDirAccess(ctx, backend.repo, "a/nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("list missing dir must fail with ErrNotFound, got %v", err)
	}
	if err := CheckParentWrite(ctx, backend.repo, "a/nope/x"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("parent check under missing dir must stay ENOENT at the edges, got %v", err)
	}
	if _, err := NormalizePath("../escape"); !errors.Is(err, ErrEscapesRoot) {
		t.Fatalf("escaping path must fail with ErrEscapesRoot, got %v", err)
	}
	if _, err := svc.CreateFileContext(ctx, "demo", "/"); !errors.Is(err, ErrRequiredPath) {
		t.Fatalf("create at root must fail with ErrRequiredPath, got %v", err)
	}
	if _, err := NormalizePath(""); !errors.Is(err, ErrRequiredPath) {
		t.Fatalf("empty path must fail with ErrRequiredPath, got %v", err)
	}
}

// The checked canonicalizer is the single home: failure paths miss lookups,
// never masquerade as the root.
func TestNormalizerFailureMisses(t *testing.T) {
	t.Parallel()
	if got := normalizeStoredPath("   "); got == "" {
		t.Fatal("whitespace-only failure must miss, not masquerade as root")
	}
	if got := normalizeStoredPath("../x"); got == "" {
		t.Fatal("traversal failure must miss, not masquerade as root")
	}
}

// The root stat view routes through the single directory constructor.
func TestRootViaCtor(t *testing.T) {
	t.Parallel()
	svc, backend := conformanceService(t)
	got, err := svc.StatPathContext(rootTestCtx(), "demo", "")
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	want := EntryFromDirectory(&backend.repo.Root, "", backend.repo.DirNLink(""))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("root view must equal EntryFromDirectory: got %+v want %+v", got, want)
	}
}

// Negative sizes are recorded faithfully and rejected loud, never
// silently dropped.
func TestWithSizeLoud(t *testing.T) {
	t.Parallel()
	got, ok := ApplyMutateOptions([]MutateOption{WithSize(-3)}).ExpectedSize()
	if !ok || got != -3 {
		t.Fatalf("negative size must be recorded, got %d %v", got, ok)
	}
	if err := ApplyMutateOptions([]MutateOption{WithSize(-3)}).ValidateSize(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative size must fail loud, got %v", err)
	}
	if err := ApplyMutateOptions([]MutateOption{WithSize(10)}).ValidateSize(); err != nil {
		t.Fatalf("valid size must pass, got %v", err)
	}
}
