package storage

import (
	"context"
	"testing"
	"time"
)

// TestPatchPreservesAtime pins the read-stamp rule on content edits: a
// stored access time survives the patch, and only a missing stamp falls
// back to now.
func TestPatchPreservesAtime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectpatchatime"

	cloneSeedFile(t, hub, project, "noted.txt", []byte("0123456789ABCDEF"))

	fixed := time.Unix(1_700_000_100, 0).UTC()
	if err := hub.ChtimesExplicitContext(ctx, project, "noted.txt", &fixed, &fixed); err != nil {
		t.Fatalf("stamp fixture: %v", err)
	}
	if _, err := hub.PatchFileContext(ctx, project, "noted.txt", 0, 1, []byte("X")); err != nil {
		t.Fatalf("patch: %v", err)
	}
	entry, err := hub.StatPathContext(ctx, project, "noted.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if entry.AccessedAt != fixed.UnixNano() {
		t.Fatalf("patch moved atime to %d, want stored %d", entry.AccessedAt, fixed.UnixNano())
	}
	if entry.ModifiedAt == fixed.UnixNano() {
		t.Fatal("patch left mtime untouched")
	}
}
