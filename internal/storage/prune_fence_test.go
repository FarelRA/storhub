package storage

// Purge-wide prune fence tests (item 3): mutations admitted while a prune
// runs fail loud with *PruneConflictError instead of interleaving, a second
// concurrent prune is refused, and writes succeed once the prune finishes.
// RED-first: these fail while admitMutation ignores the prune fence.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// stallPruneDeletes blocks the mock on content-addressed object deletes so
// the prune holds its fence while the test drives concurrent mutations.
// It returns the disarm function.
func stallPruneDeletes(t *testing.T, backend *mockGitHub) func() {
	t.Helper()
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, ".storhub/objects") {
			time.Sleep(2 * time.Second)
		}
		return false
	})
	return func() {
		backend.intercept.Store((func(http.ResponseWriter, *http.Request) bool)(nil))
	}
}

// waitPruneFence polls until the hub reports a running prune for project.
func waitPruneFence(t *testing.T, hub *StorHub, project string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if hub.pruneFenceRunning(project) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the prune fence to engage")
}

// Writes admitted during a running prune fail loud; after the prune they
// succeed. The pruned orphan is still reclaimed exactly once.
func TestPruneFenceRefusesWritesDuringPrune(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "prunefence-writes"
	seedMeta(t, hub, project, "docs", "a.txt", 1)
	orphan := injectOrphanObject(t, hub, project)

	disarm := stallPruneDeletes(t, backend)
	defer disarm()

	pruneDone := make(chan error, 1)
	go func() {
		_, err := hub.Prune(ctx, project, PruneObjects, 0, false)
		pruneDone <- err
	}()
	waitPruneFence(t, hub, project)

	input := writeTempFile(t, t.TempDir(), "late.txt", []byte("late write"))
	if _, err := hub.UploadFileContext(ctx, project, "late.txt", input); err == nil {
		t.Fatal("upload during prune must fail loud, got nil")
	} else {
		var conflict *PruneConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("upload during prune must fail with *PruneConflictError, got %T: %v", err, err)
		}
		if conflict.Project != project {
			t.Fatalf("conflict project = %q, want %q", conflict.Project, project)
		}
	}
	if _, err := hub.Prune(ctx, project, PruneObjects, 0, true); err == nil {
		t.Fatal("second prune during a running prune must fail loud, got nil")
	} else {
		var conflict *PruneConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("second prune must fail with *PruneConflictError, got %T: %v", err, err)
		}
	}

	disarm()
	if err := <-pruneDone; err != nil {
		t.Fatalf("prune: %v", err)
	}
	if mockHas(backend, project, objectRepoPath(orphan)) {
		t.Fatal("orphan survived the fenced prune")
	}

	// The fence is released: writes admit again.
	input2 := writeTempFile(t, t.TempDir(), "after.txt", []byte("after prune"))
	if _, err := hub.UploadFileContext(ctx, project, "after.txt", input2); err != nil {
		t.Fatalf("upload after prune must succeed: %v", err)
	}
	if err := hub.DrainProjectContext(ctx, project); err != nil {
		t.Fatalf("drain after prune: %v", err)
	}
}

// The fence registry refuses double acquisition and honors generations:
// a stale release is a no-op, never dropping a newer holder.
func TestPruneFenceRegistryGenerations(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "prunefence-reg"

	if hub.pruneFenceRunning(project) {
		t.Fatal("fence must start free")
	}
	gen, ok := hub.pruneFenceAcquire(project)
	if !ok {
		t.Fatal("first acquire must succeed")
	}
	if !hub.pruneFenceRunning(project) {
		t.Fatal("fence must report running after acquire")
	}
	if _, ok := hub.pruneFenceAcquire(project); ok {
		t.Fatal("second acquire while held must fail")
	}
	hub.pruneFenceRelease(project, gen+999)
	if !hub.pruneFenceRunning(project) {
		t.Fatal("stale-generation release must not drop the fence")
	}
	hub.pruneFenceRelease(project, gen)
	if hub.pruneFenceRunning(project) {
		t.Fatal("fence must be free after a matching release")
	}
	if _, ok := hub.pruneFenceAcquire(project); !ok {
		t.Fatal("acquire after release must succeed")
	}
	hub.pruneFenceRelease(project, ^uint64(0))
}

// The typed refusal names the project for operators.
func TestPruneConflictErrorShape(t *testing.T) {
	t.Parallel()
	err := &PruneConflictError{Project: "demo"}
	if !strings.Contains(err.Error(), "demo") {
		t.Fatalf("conflict error must name the project, got: %v", err)
	}
	var target *PruneConflictError
	if !errors.As(err, &target) {
		t.Fatalf("conflict error must match errors.As, got %T", err)
	}
}
