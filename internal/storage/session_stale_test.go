package storage

import (
	"context"
	"testing"
)

// TestSessionStatStaleFreshThenRival pins the Stale contract, direction one
// then two: a freshly opened handle is not stale, and once the same hub
// commits a newer revision the pinned handle reports stale while still
// serving its snapshot.
func TestSessionStatStaleFreshThenRival(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionstalerival"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("version-one"))

	id := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadOnly)
	stat, err := hub.StatSession(ctx, id)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if stat.Stale {
		t.Fatal("freshly opened handle must not be stale")
	}

	seed := writeTempFile(t, t.TempDir(), "v2.bin", []byte("version-two"))
	if _, err := hub.ReplaceFileContext(ctx, proj, "data.txt", seed); err != nil {
		t.Fatalf("rival replace: %v", err)
	}
	if err := hub.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain rival: %v", err)
	}

	stat, err = hub.StatSession(ctx, id)
	if err != nil {
		t.Fatalf("stat after rival commit: %v", err)
	}
	if !stat.Stale {
		t.Fatal("handle pinned before a newer commit must report stale")
	}
	if stat.Project != proj || stat.Path != "data.txt" || stat.Size != 11 || stat.Dirty || stat.Mode != SessionReadOnly {
		t.Fatalf("stale stat must preserve the other fields: %+v", stat)
	}

	got, err := hub.ReadSession(ctx, id, 0, 64)
	if err != nil {
		t.Fatalf("pinned read: %v", err)
	}
	if string(got) != "version-one" {
		t.Fatalf("stale handle must still serve its snapshot, got %q", got)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestSessionStatStaleClearsOnSync pins the return trip: committing the
// handle's own staged state re-pins it to the newest revision, clearing
// the flag.
func TestSessionStatStaleClearsOnSync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionstalesync"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("base"))

	id := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadWrite)

	seed := writeTempFile(t, t.TempDir(), "rival.bin", []byte("rival-committed"))
	if _, err := hub.ReplaceFileContext(ctx, proj, "data.txt", seed); err != nil {
		t.Fatalf("rival replace: %v", err)
	}
	if err := hub.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain rival: %v", err)
	}
	stat, err := hub.StatSession(ctx, id)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !stat.Stale {
		t.Fatal("handle must be stale after a rival commit")
	}

	if _, err := hub.WriteSession(ctx, id, 0, []byte("mine")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := hub.SyncSession(ctx, id); err != nil {
		t.Fatalf("sync: %v", err)
	}
	stat, err = hub.StatSession(ctx, id)
	if err != nil {
		t.Fatalf("stat after sync: %v", err)
	}
	if stat.Stale {
		t.Fatal("sync must re-pin to the newest revision, clearing stale")
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}
