package storage

import (
	"context"
	"errors"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// TestDeleteCollapsesUnlink pins the single delete spelling: the unlink
// name removes exactly what the canonical delete removes, reports the same
// errors, and refuses directories under both spellings.
func TestDeleteCollapsesUnlink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectdeletealias"

	cloneSeedFile(t, hub, project, "gone.txt", []byte("bye"))
	if err := hub.UnlinkContext(ctx, project, "gone.txt"); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if repo.FindFile("gone.txt") != nil {
		t.Fatal("unlinked file still present")
	}

	unlinkErr := hub.UnlinkContext(ctx, project, "gone.txt")
	if !errors.Is(unlinkErr, shfs.ErrNotFound) {
		t.Fatalf("second unlink: want not-found, got %v", unlinkErr)
	}
	deleteErr := hub.DeleteFileContext(ctx, project, "gone.txt")
	if !errors.Is(deleteErr, shfs.ErrNotFound) {
		t.Fatalf("delete missing: want not-found, got %v", deleteErr)
	}

	if err := hub.MkdirContext(ctx, project, "adir"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.UnlinkContext(ctx, project, "adir"); !errors.Is(err, shfs.ErrIsDirectory) {
		t.Fatalf("unlink dir: want is-directory, got %v", err)
	}
	if err := hub.DeleteFileContext(ctx, project, "adir"); !errors.Is(err, shfs.ErrIsDirectory) {
		t.Fatalf("delete dir: want is-directory, got %v", err)
	}
}
