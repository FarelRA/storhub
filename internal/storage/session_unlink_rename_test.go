package storage

// session_unlink_rename_test.go: a handle whose path is unlinked or
// renamed after open keeps POSIX open-file-description semantics:
// reads serve the pin plus staged writes, close-after-unlink discards
// with success, close-after-rename follows the surviving name, and sync
// after unlink retains staged bytes without publishing.

import (
	"context"
	"testing"
)

func TestSessionUnlinkReadCloseDiscard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-session-unlink-discard"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("keep"))

	id := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, id, 4, []byte("more")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := hub.DeleteFileContext(ctx, proj, "data.txt"); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if _, err := hub.StatPathContext(ctx, proj, "data.txt"); err == nil {
		t.Fatalf("stat after unlink: want not found, got entry")
	}
	got, err := hub.ReadSession(ctx, id, 0, 8)
	if err != nil {
		t.Fatalf("read on unlinked handle: %v", err)
	}
	if string(got) != "keepmore" {
		t.Fatalf("unlinked read: want %q, got %q", "keepmore", got)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close on unlinked handle (want discard-success): %v", err)
	}
	if _, err := hub.StatPathContext(ctx, proj, "data.txt"); err == nil {
		t.Fatalf("file resurrected by close: want not found")
	}
}

func TestSessionRenameCloseFollowsSurvivor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-session-rename-follow"
	setupSessionFile(ctx, t, hub, proj, "src.txt", []byte("keep"))

	id := mustOpenSession(ctx, t, hub, proj, "src.txt", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, id, 4, []byte("more")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := hub.RenameContext(ctx, proj, "src.txt", "dst.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	got, err := hub.ReadSession(ctx, id, 0, 8)
	if err != nil {
		t.Fatalf("read on renamed handle: %v", err)
	}
	if string(got) != "keepmore" {
		t.Fatalf("renamed read: want %q, got %q", "keepmore", got)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close on renamed handle: %v", err)
	}
	if _, err := hub.StatPathContext(ctx, proj, "src.txt"); err == nil {
		t.Fatalf("old name resurrected by close: want not found")
	}
	data := freshSessionBytes(t, hub, proj, "dst.txt")
	if string(data) != "keepmore" {
		t.Fatalf("survivor content: want %q, got %q", "keepmore", data)
	}
}

func TestSessionSyncAfterUnlinkRetains(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-session-sync-unlinked"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("keep"))

	id := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, id, 4, []byte("more")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := hub.DeleteFileContext(ctx, proj, "data.txt"); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	// fsync-equivalent on an unlinked fd succeeds without publishing.
	if err := hub.SyncSession(ctx, id); err != nil {
		t.Fatalf("sync on unlinked handle: %v", err)
	}
	got, err := hub.ReadSession(ctx, id, 0, 8)
	if err != nil {
		t.Fatalf("read after sync on unlinked handle: %v", err)
	}
	if string(got) != "keepmore" {
		t.Fatalf("retained content: want %q, got %q", "keepmore", got)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close on unlinked handle: %v", err)
	}
	if _, err := hub.StatPathContext(ctx, proj, "data.txt"); err == nil {
		t.Fatalf("file resurrected by sync+close: want not found")
	}
}
