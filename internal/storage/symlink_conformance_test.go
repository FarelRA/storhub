package storage

import (
	"context"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// Symlink conformance through the real StorHub surface (mock GitHub): read,
// stat, write, and upload through a symlinked directory; ".." popping the
// physical stack; and unlink removing the link itself without following
// it.

func TestStorageConformanceSymlinkAndDotDotMatrix(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})
	const project = "project-symlink-conformance"

	for _, dir := range []string{"a", "a/b", "a/b/c"} {
		if err := hub.MkdirContext(ctx, project, dir); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	input := writeTempFile(t, t.TempDir(), "f.txt", []byte("hello"))
	if _, err := hub.UploadFileContext(ctx, project, "a/b/c/f.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if _, err := hub.SymlinkContext(ctx, project, "b/c", "a/link"); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// Read through the symlinked directory.
	data, err := hub.ReadFileAtContext(ctx, project, "a/link/f.txt", 0, 5)
	if err != nil || string(data) != "hello" {
		t.Fatalf("read through symlinked dir: %q %v", data, err)
	}
	// ".." through the symlink addresses the physical parent a/b.
	entry, err := hub.StatPathContext(ctx, project, "a/link/..")
	if err != nil {
		t.Fatalf("stat .. through symlink: %v", err)
	}
	if entry.Path != "a/b" || !entry.IsDir {
		t.Fatalf("stat .. through symlink: got %+v, want dir a/b", entry)
	}
	// Mixed spelling: a/link/../c/f.txt == a/b/c/f.txt.
	data, err = hub.ReadFileAtContext(ctx, project, "a/link/../c/f.txt", 0, 5)
	if err != nil || string(data) != "hello" {
		t.Fatalf("read via ..-then-down: %q %v", data, err)
	}
	// Write through the symlinked directory lands on the target.
	if _, err := hub.WriteFileAtContext(ctx, project, "a/link/f.txt", 0, []byte("J")); err != nil {
		t.Fatalf("write through symlinked dir: %v", err)
	}
	dl := writeTempFile(t, t.TempDir(), "out", nil)
	if err := hub.DownloadFileContext(ctx, project, "a/b/c/f.txt", dl); err != nil {
		t.Fatalf("download target: %v", err)
	}
	assertFileContent(t, dl, []byte("Jello"))
	// Upload through the symlinked directory creates under the target dir.
	input2 := writeTempFile(t, t.TempDir(), "up.txt", []byte("up"))
	if _, err := hub.UploadFileContext(ctx, project, "a/link/up.txt", input2); err != nil {
		t.Fatalf("upload through symlinked dir: %v", err)
	}
	if _, err := hub.StatPathContext(ctx, project, "a/b/c/up.txt"); err != nil {
		t.Fatalf("uploaded file must live at the resolved key: %v", err)
	}
	// Unlink of the symlink removes the link, never the target.
	if err := hub.UnlinkContext(ctx, project, "a/link"); err != nil {
		t.Fatalf("unlink symlink: %v", err)
	}
	if _, err := hub.ReadlinkContext(ctx, project, "a/link"); err == nil {
		t.Fatal("link must be gone after unlink")
	}
	if _, err := hub.StatPathContext(ctx, project, "a/b/c/f.txt"); err != nil {
		t.Fatalf("unlink of a symlink must not disturb the target: %v", err)
	}
	// Escapes are rejected through the real verbs.
	if _, err := hub.ReadFileAtContext(ctx, project, "a/b/c/../../../../x", 0, 1); err == nil {
		t.Fatal("escaping read must fail")
	}
}
