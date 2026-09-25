package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Explicit discard drops staged writes on a named handle without
// committing: the committed file keeps its old bytes and the handle answers
// stale afterwards.
func TestDiscardSessionDropsNamedWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessiondiscardnamed"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("kept"))

	id := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, id, 0, []byte("doomed")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := hub.DiscardSession(ctx, id); err != nil {
		t.Fatalf("discard: %v", err)
	}
	if _, err := hub.ReadSession(ctx, id, 0, 1); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("discarded handle must be stale, got %v", err)
	}
	if got := freshSessionBytes(t, hub, proj, "data.txt"); string(got) != "kept" {
		t.Fatalf("after discard committed = %q, want %q", got, "kept")
	}
}

// Explicit discard on a linked scratch handle publishes nothing.
func TestDiscardSessionDropsLinkedScratch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessiondiscardlinked"
	if err := hub.MkdirContext(ctx, proj, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain mkdir: %v", err)
	}

	id := mustOpenSession(ctx, t, hub, proj, "", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, id, 0, []byte("doomed")); err != nil {
		t.Fatalf("write scratch: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "docs/linked.txt"); err != nil {
		t.Fatalf("link: %v", err)
	}
	if err := hub.DiscardSession(ctx, id); err != nil {
		t.Fatalf("discard: %v", err)
	}
	meta, _, err := hub.loadRepoMetadataFresh(ctx, proj)
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	if meta.FindFile("docs/linked.txt") != nil {
		t.Fatal("discarded linked scratch must publish nothing")
	}
}

// Discarding an unknown handle answers the typed stale error.
func TestDiscardSessionUnknownHandleStale(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.DiscardSession(ctx, "no-such-handle"); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("discard unknown must be stale, got %v", err)
	}
}

// Stale errors carry the full handle id: only logs truncate.
func TestStaleErrorCarriesFullHandle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionstalefull"

	id := mustOpenSession(ctx, t, hub, proj, "", SessionReadWrite)
	if len(id) != 32 {
		t.Fatalf("handle id = %q, want 32 hex chars", id)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := hub.ReadSession(ctx, id, 0, 1)
	var stale *StaleSessionError
	if !errors.As(err, &stale) {
		t.Fatalf("want typed stale error, got %v", err)
	}
	if stale.HandleID != id {
		t.Fatalf("stale handle = %q, want %q", stale.HandleID, id)
	}
	if !strings.Contains(err.Error(), id) {
		t.Fatalf("stale error %q must carry the full handle id", err.Error())
	}
}

// Staging temps live under a prefix of their own so the quarantine scan
// never confuses a live temp with a quarantined one.
func TestSessionStagingPrefixSeparated(t *testing.T) {
	t.Parallel()
	tmp, err := newSessionTemp()
	if err != nil {
		t.Fatalf("staging temp: %v", err)
	}
	_ = tmp.File.Close()
	_ = os.Remove(tmp.Name)
	if base := filepath.Base(tmp.Name); !strings.HasPrefix(base, "scratch-") {
		t.Fatalf("staging temp %q must use the scratch prefix", base)
	}
}

// The quarantine sweep collects orphaned staging temps by prefix,
// including ones minted before the prefix split, and never touches
// quarantined recoveries.
func TestQuarantineSweepCollectsStagingPrefixes(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	base, err := spoolBase()
	if err != nil {
		t.Fatalf("spool base: %v", err)
	}
	mkold := func(pattern, content string) string {
		t.Helper()
		f, err := os.CreateTemp(base, pattern)
		if err != nil {
			t.Fatalf("temp: %v", err)
		}
		if _, err := f.WriteString(content); err != nil {
			t.Fatalf("write temp: %v", err)
		}
		name := f.Name()
		if err := f.Close(); err != nil {
			t.Fatalf("close temp: %v", err)
		}
		old := time.Unix(1700000000, 0).UTC().Add(-2 * time.Hour)
		if err := os.Chtimes(name, old, old); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
		return name
	}
	oldScratch := mkold("scratch-*", "orphan-scratch")
	oldLegacy := mkold("session-*", "orphan-legacy")
	moved, err := hub.QuarantineStaleSessionTemps(time.Hour)
	if err != nil {
		t.Fatalf("quarantine sweep: %v", err)
	}
	if moved < 2 {
		t.Fatalf("sweep moved %d, want at least 2", moved)
	}
	for _, name := range []string{oldScratch, oldLegacy} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("orphan %s must move aside: %v", name, os.Remove(name))
		}
	}
}
