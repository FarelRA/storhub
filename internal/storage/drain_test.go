package storage

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// DrainProjectContext blocks until everything published before the call
// has landed in the remote commit, or fails loudly. It is the shared
// durability primitive behind fsync, O_SYNC, and the REST/CLI sync
// opt-in: POSIX fsync-class semantics cover pre-call data only, so a
// concurrent writer's later mutations may land in the same commit (fine)
// or a later one (also fine), but pre-call data is always durable on
// success.
func TestDrainProjectLandsPublishedData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "project-drain-lands"

	first := writeTempFile(t, t.TempDir(), "f1.txt", []byte("one"))
	if _, err := hubA.UploadFile(proj, "f1.txt", first); err != nil {
		t.Fatalf("upload f1: %v", err)
	}
	// No flush: the data is published in memory but not yet committed.
	if err := hubA.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain: %v", err)
	}
	meta, _, err := hubB.loadRepoMetadataFresh(ctx, proj)
	if err != nil {
		t.Fatalf("hubB fresh load: %v", err)
	}
	if meta.FindFile("f1.txt") == nil {
		t.Fatal("drained data not visible to a second hub")
	}
	pm := hubA.getOrCreateProjectMeta(proj)
	pm.mu.RLock()
	dirty := pm.dirty
	pm.mu.RUnlock()
	if dirty {
		t.Fatal("project still dirty after successful drain")
	}
}

func TestDrainProjectCleanIsNoNetwork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-drain-clean"

	first := writeTempFile(t, t.TempDir(), "f1.txt", []byte("one"))
	if _, err := hub.UploadFile(proj, "f1.txt", first); err != nil {
		t.Fatalf("upload f1: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, proj); err != nil {
		t.Fatalf("flush f1: %v", err)
	}
	var apiCalls atomic.Int64
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		apiCalls.Add(1)
		return false
	})
	if err := hub.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain clean: %v", err)
	}
	if got := apiCalls.Load(); got != 0 {
		t.Fatalf("drain of clean project issued %d API calls, want 0", got)
	}
}

func TestDrainProjectReportsCommitFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-drain-fails"

	first := writeTempFile(t, t.TempDir(), "f1.txt", []byte("one"))
	if _, err := hub.UploadFile(proj, "f1.txt", first); err != nil {
		t.Fatalf("upload f1: %v", err)
	}
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"injected failure"}`))
			return true
		}
		return false
	})
	if err := hub.DrainProjectContext(ctx, proj); err == nil {
		t.Fatal("drain over failing remote: want error, got nil")
	} else if !strings.Contains(err.Error(), "drain "+proj) {
		t.Fatalf("drain error should name the project, got: %v", err)
	}
	pm := hub.getOrCreateProjectMeta(proj)
	pm.mu.RLock()
	dirty := pm.dirty
	pm.mu.RUnlock()
	if !dirty {
		t.Fatal("failed drain must retain dirty state for retry")
	}
}

func TestDrainProjectConvergesUnderConcurrentMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "project-drain-concurrent"

	write := func(name, body string) {
		t.Helper()
		seed := writeTempFile(t, t.TempDir(), name, []byte(body))
		if _, err := hubA.UploadFile(proj, name, seed); err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
	}
	visible := func(names ...string) {
		t.Helper()
		meta, _, err := hubB.loadRepoMetadataFresh(ctx, proj)
		if err != nil {
			t.Fatalf("hubB fresh load: %v", err)
		}
		for _, name := range names {
			if meta.FindFile(name) == nil {
				t.Fatalf("%s not visible after drain", name)
			}
		}
	}
	write("f1.txt", "one")
	write("f2.txt", "two")
	// A concurrent writer keeps publishing while the drain runs; the
	// drain guarantees pre-call data (f1, f2) and must converge rather
	// than chase the flood. f3 may or may not ride along.
	done := make(chan error, 1)
	seed3 := writeTempFile(t, t.TempDir(), "f3.txt", []byte("three"))
	go func() {
		_, err := hubA.UploadFile(proj, "f3.txt", seed3)
		done <- err
	}()
	if err := hubA.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("concurrent upload f3: %v", err)
	}
	visible("f1.txt", "f2.txt")
	if err := hubA.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("second drain: %v", err)
	}
	visible("f1.txt", "f2.txt", "f3.txt")
}
