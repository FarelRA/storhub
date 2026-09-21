package storage

// Multi-path session link tests (prod POSIX gaps, item 1).
//
// A scratch handle links an ordered set of pending names: Link appends,
// Relink replaces the whole set, and Close/Sync pre-validate every pending
// name before publishing the staged bytes to each with create semantics.
// RED-first: these fail against the single-path link field.

import (
	"context"
	"errors"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// multilinkSetup opens a scratch handle with staged bytes on a project
// whose docs directory exists and is committed.
func multilinkSetup(t *testing.T, hub *StorHub, project string, data []byte) (context.Context, string) {
	t.Helper()
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.DrainProjectContext(ctx, project); err != nil {
		t.Fatalf("drain mkdir: %v", err)
	}
	id := mustOpenSession(ctx, t, hub, project, "", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, id, 0, data); err != nil {
		t.Fatalf("write scratch: %v", err)
	}
	return ctx, id
}

// multilinkFresh finds a file in backend truth through a fresh observer hub.
func multilinkFresh(t *testing.T, backend *mockGitHub, project, path string) *FileMeta {
	t.Helper()
	observer := backend.newClient(t, smallTransferTestConfig())
	meta, _, err := observer.loadRepoMetadataFresh(context.Background(), project)
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	return meta.FindFile(path)
}

func multilinkFreshBytes(t *testing.T, backend *mockGitHub, project, path string) []byte {
	t.Helper()
	entry := multilinkFresh(t, backend, project, path)
	if entry == nil {
		t.Fatalf("fresh read: %s not found", path)
	}
	observer := backend.newClient(t, smallTransferTestConfig())
	data, err := observer.ReadPinnedFileContext(context.Background(), project, entry, multilinkFreshChunks(t, backend, project), 0, entry.Size)
	if err != nil {
		t.Fatalf("fresh read %s: %v", path, err)
	}
	return data
}

func multilinkFreshChunks(t *testing.T, backend *mockGitHub, project string) map[int64]ChunkInfo {
	t.Helper()
	observer := backend.newClient(t, smallTransferTestConfig())
	meta, _, err := observer.loadRepoMetadataFresh(context.Background(), project)
	if err != nil {
		t.Fatalf("fresh load chunks: %v", err)
	}
	return meta.Chunks()
}

// Two linked names both publish the staged bytes on close.
func TestSessionMultiLinkPublishesBoth(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-session-multilink-two"

	ctx, id := multilinkSetup(t, hub, project, []byte("shared-payload"))
	if err := hub.LinkSession(ctx, id, "docs/a.txt"); err != nil {
		t.Fatalf("link a: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "docs/b.txt"); err != nil {
		t.Fatalf("link b: %v", err)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := multilinkFreshBytes(t, backend, project, "docs/a.txt"); string(got) != "shared-payload" {
		t.Fatalf("a.txt = %q, want %q", got, "shared-payload")
	}
	if got := multilinkFreshBytes(t, backend, project, "docs/b.txt"); string(got) != "shared-payload" {
		t.Fatalf("b.txt = %q, want %q", got, "shared-payload")
	}
}

// Linking an already-pending name fails with ErrSessionLinked and the
// handle still commits its single pending name.
func TestSessionLinkDuplicateFails(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-session-multilink-dup"

	ctx, id := multilinkSetup(t, hub, project, []byte("dup-payload"))
	if err := hub.LinkSession(ctx, id, "docs/a.txt"); err != nil {
		t.Fatalf("link a: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "docs/a.txt"); !errors.Is(err, ErrSessionLinked) {
		t.Fatalf("duplicate link must fail with ErrSessionLinked, got %v", err)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close after duplicate: %v", err)
	}
	if got := multilinkFreshBytes(t, backend, project, "docs/a.txt"); string(got) != "dup-payload" {
		t.Fatalf("a.txt = %q, want %q", got, "dup-payload")
	}
}

// A taken second name aborts the whole close with AlreadyExists,
// publishing nothing; the handle stays open for Relink.
func TestSessionMultiLinkTakenSecondAborts(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-session-multilink-taken"

	ctx, id := multilinkSetup(t, hub, project, []byte("staged-bytes"))
	if err := hub.LinkSession(ctx, id, "docs/a.txt"); err != nil {
		t.Fatalf("link a: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "docs/b.txt"); err != nil {
		t.Fatalf("link b: %v", err)
	}
	// A concurrent writer takes the second name before close.
	setupSessionFile(ctx, t, hub, project, "docs/b.txt", []byte("rival"))

	if err := hub.CloseSession(ctx, id); !errors.Is(err, shfs.ErrAlreadyExists) {
		t.Fatalf("close over a taken second name must fail with AlreadyExists, got %v", err)
	}
	// Nothing published: the first name must not exist.
	if entry := multilinkFresh(t, backend, project, "docs/a.txt"); entry != nil {
		t.Fatal("aborted close published docs/a.txt, want nothing published")
	}
	// Pre-validation runs before any verb: the first name must not even
	// be staged in the live tree.
	live, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("live load: %v", err)
	}
	if live.FindFile("docs/a.txt") != nil {
		t.Fatal("aborted close staged docs/a.txt locally, want pre-validation before any publish")
	}
	// The handle stays open with its staged bytes intact.
	if _, err := hub.StatSession(ctx, id); err != nil {
		t.Fatalf("stat after aborted close: %v", err)
	}
	got, err := hub.ReadSession(ctx, id, 0, 64)
	if err != nil {
		t.Fatalf("read after aborted close: %v", err)
	}
	if string(got) != "staged-bytes" {
		t.Fatalf("staged read = %q, want %q", got, "staged-bytes")
	}
	// Relink rescues to a free name; the rival keeps its content.
	if err := hub.RelinkSession(ctx, id, "docs/c.txt"); err != nil {
		t.Fatalf("relink: %v", err)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close after relink: %v", err)
	}
	if got := multilinkFreshBytes(t, backend, project, "docs/c.txt"); string(got) != "staged-bytes" {
		t.Fatalf("c.txt = %q, want %q", got, "staged-bytes")
	}
	if got := multilinkFreshBytes(t, backend, project, "docs/b.txt"); string(got) != "rival" {
		t.Fatalf("rival b.txt = %q, want %q", got, "rival")
	}
}

// Relink replaces the whole pending set: only the relinked name publishes.
func TestSessionRelinkReplacesPendingSet(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-session-multilink-relink"

	ctx, id := multilinkSetup(t, hub, project, []byte("relocated"))
	if err := hub.LinkSession(ctx, id, "docs/a.txt"); err != nil {
		t.Fatalf("link a: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "docs/b.txt"); err != nil {
		t.Fatalf("link b: %v", err)
	}
	if err := hub.RelinkSession(ctx, id, "docs/c.txt"); err != nil {
		t.Fatalf("relink: %v", err)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := multilinkFreshBytes(t, backend, project, "docs/c.txt"); string(got) != "relocated" {
		t.Fatalf("c.txt = %q, want %q", got, "relocated")
	}
	if entry := multilinkFresh(t, backend, project, "docs/a.txt"); entry != nil {
		t.Fatal("relink must drop docs/a.txt from the pending set")
	}
	if entry := multilinkFresh(t, backend, project, "docs/b.txt"); entry != nil {
		t.Fatal("relink must drop docs/b.txt from the pending set")
	}
}

// Sync pre-validates like close: a taken second name fails the sync with
// AlreadyExists and retains staged state.
func TestSessionMultiLinkSyncPrevalidates(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-session-multilink-sync"

	ctx, id := multilinkSetup(t, hub, project, []byte("sync-bytes"))
	if err := hub.LinkSession(ctx, id, "docs/a.txt"); err != nil {
		t.Fatalf("link a: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "docs/b.txt"); err != nil {
		t.Fatalf("link b: %v", err)
	}
	setupSessionFile(ctx, t, hub, project, "docs/b.txt", []byte("rival"))

	if err := hub.SyncSession(ctx, id); !errors.Is(err, shfs.ErrAlreadyExists) {
		t.Fatalf("sync over a taken second name must fail with AlreadyExists, got %v", err)
	}
	if entry := multilinkFresh(t, backend, project, "docs/a.txt"); entry != nil {
		t.Fatal("aborted sync published docs/a.txt, want nothing published")
	}
	// Pre-validation runs before any verb: the first name must not even
	// be staged in the live tree.
	live, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("live load: %v", err)
	}
	if live.FindFile("docs/a.txt") != nil {
		t.Fatal("aborted sync staged docs/a.txt locally, want pre-validation before any publish")
	}
	stat, err := hub.StatSession(ctx, id)
	if err != nil {
		t.Fatalf("stat after aborted sync: %v", err)
	}
	if !stat.Dirty {
		t.Fatal("aborted sync must retain staged state")
	}
}
