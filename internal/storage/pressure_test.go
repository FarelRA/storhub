package storage

// Pressure-counter contract tests (item A1).
//
// Each test drives the real path and asserts the counter moves.
// RED-first: this file was written before pressure.go existed.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// pressureTestHub builds a hub with an isolated journal dir so
// appendOpLocked journal writes never touch shared state.
func pressureTestHub(t *testing.T, backend *mockGitHub) *StorHub {
	t.Helper()
	cfg := Config{
		ChunkSize:         64,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
		JournalDir:        t.TempDir(),
	}
	return backend.newClient(t, cfg)
}

// newDetachedPM returns a projectMetadata with no running commit
// loop, so direct commitProjectMetadata calls are deterministic:
// no background attempt can steal the commit or the counters.
func newDetachedPM(project string) *projectMetadata {
	return &projectMetadata{
		meta:      NewRepoMetadata(project),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
		triggerCh: make(chan struct{}, 1),
		hydrated:  true,
	}
}

// TestPressureCapCrossCountsCrossingAppend pins the cap-cross
// counter: the mutation that pushes the stack to
// maxPendingOpsPerProject must record exactly one cap-cross and
// one force-retry poke.
func TestPressureCapCrossCountsCrossingAppend(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := pressureTestHub(t, backend)
	project := "pressure-cap-cross"

	pm := newDetachedPM(project)
	for i := 0; i < maxPendingOpsPerProject-1; i++ {
		pm.opStack.append(Op{
			Type:      OpMkdir,
			Paths:     []string{fmt.Sprintf("dir-%d", i)},
			Cause:     "pressure-prefill",
			Timestamp: 1700000000,
		})
	}

	before := hub.PressureSnapshot()
	pm.mu.Lock()
	hub.appendOpLocked(project, pm, Op{
		Type:      OpMkdir,
		Paths:     []string{"dir-crossing"},
		Cause:     "pressure-crossing",
		Timestamp: 1700000000,
	})
	pm.mu.Unlock()
	after := hub.PressureSnapshot()

	if got := after.CapCrosses - before.CapCrosses; got != 1 {
		t.Fatalf("cap-cross delta = %d, want 1", got)
	}
	if got := after.ForceRetryPokes - before.ForceRetryPokes; got != 1 {
		t.Fatalf("force-retry delta = %d, want 1", got)
	}
}

// TestPressureCommitFailureStreakAndReset pins the consecutive
// failure streak: two failed commits raise it to 2, and the next
// success resets it to 0 while moving both commit counters.
func TestPressureCommitFailureStreakAndReset(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := pressureTestHub(t, backend)
	ctx := context.Background()
	project := "pressure-streak"

	// Seed through the real path so the mock repo and its upload
	// release exist: a detached pm cannot create them, and object
	// writes would 404. Counters are baselined after the seed.
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seed"))
	if _, err := hub.UploadFileContext(context.Background(), project, "seed.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("seed flush: %v", err)
	}

	pm := newDetachedPM(project)
	// Adopt the live committed state (tree + CAS token) so the
	// detached commit publishes cleanly: a fresh pm would take
	// the legacy-migration path and 422 on the missing CAS sha.
	// No loop is attached, so the sequence stays deterministic.
	live := hub.getOrCreateProjectMeta(project)
	live.mu.RLock()
	pm.meta, pm.sha = live.meta, live.sha
	live.mu.RUnlock()
	pm.mu.Lock()
	hub.appendOpLocked(project, pm, Op{
		Type:      OpMkdir,
		Paths:     []string{"d"},
		Cause:     "pressure-seed",
		Timestamp: 1700000000,
	})
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	var fail bool
	fail = true
	backend.intercept.Store(
		func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodPut &&
				strings.Contains(r.URL.Path, testIndexPath) &&
				fail {
				http.Error(w, "injected failure",
					http.StatusInternalServerError)
				return true
			}
			return false
		})
	t.Cleanup(func() {
		backend.intercept.Store(
			(func(http.ResponseWriter, *http.Request) bool)(nil))
	})

	before := hub.PressureSnapshot()
	for i := 0; i < 2; i++ {
		if err := hub.commitProjectMetadata(ctx, project, pm); err == nil {
			t.Fatalf("commit %d under intercepted PUT must fail", i)
		}
	}
	mid := hub.PressureSnapshot()
	if got := mid.CommitFailures - before.CommitFailures; got != 2 {
		t.Fatalf("commit-failure delta = %d, want 2", got)
	}
	if got := hub.PressureFailureStreak(project); got != 2 {
		t.Fatalf("failure streak = %d, want 2", got)
	}

	fail = false
	if err := hub.commitProjectMetadata(ctx, project, pm); err != nil {
		t.Fatalf("commit after disarm must succeed: %v", err)
	}
	after := hub.PressureSnapshot()
	if got := after.CommitSuccesses - before.CommitSuccesses; got != 1 {
		t.Fatalf("commit-success delta = %d, want 1", got)
	}
	if got := hub.PressureFailureStreak(project); got != 0 {
		t.Fatalf("streak after success = %d, want 0", got)
	}
}

// TestPressureRebaseCounted pins the rebase counter: a stale
// flush that converges via rebase-on-conflict records exactly
// one rebase. Follows the two-hub pattern of
// TestHardeningFlushMetadataRecoversFromConflict.
func TestPressureRebaseCounted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "pressure-rebase"

	first := writeTempFile(t, t.TempDir(), "f1.txt", []byte("one"))
	if _, err := hubA.UploadFileContext(context.Background(), proj, "f1.txt", first); err != nil {
		t.Fatalf("upload f1: %v", err)
	}
	if err := hubA.FlushProjectContext(ctx, proj); err != nil {
		t.Fatalf("flush f1: %v", err)
	}
	if _, _, err := hubB.loadRepoMetadata(ctx, proj); err != nil {
		t.Fatalf("hubB hydrate: %v", err)
	}
	second := writeTempFile(t, t.TempDir(), "f2.txt", []byte("two"))
	if _, err := hubA.UploadFileContext(context.Background(), proj, "f2.txt", second); err != nil {
		t.Fatalf("upload f2: %v", err)
	}
	if err := hubA.FlushProjectContext(ctx, proj); err != nil {
		t.Fatalf("flush f2: %v", err)
	}

	before := hubB.PressureSnapshot()
	pmB := hubB.getOrCreateProjectMeta(proj)
	pmB.mu.Lock()
	pmB.meta.UpsertFile("b.txt", FileMeta{Size: 0, Mode: 0o644}, 1700000000)
	entry := pmB.meta.FindFile("b.txt").Clone()
	pmB.opStack.append(Op{Type: OpPutFile, Paths: []string{"b.txt"},
		Cause: "pressure-test", Timestamp: 1700000000, File: &entry})
	markProjectDirtyLocked(pmB)
	pmB.mu.Unlock()
	if err := hubB.FlushMetadata(ctx); err != nil {
		t.Fatalf("stale flush must converge via rebase: %v", err)
	}
	after := hubB.PressureSnapshot()
	if got := after.Rebases - before.Rebases; got != 1 {
		t.Fatalf("rebase delta = %d, want 1", got)
	}
}

// TestPressurePendingDepth pins the live depth accessor: three
// appended ops read back as depth 3 without touching the journal
// or the commit trigger.
func TestPressurePendingDepth(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := pressureTestHub(t, backend)
	project := "pressure-depth"

	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	for i := 0; i < 3; i++ {
		pm.opStack.append(Op{
			Type:      OpMkdir,
			Paths:     []string{fmt.Sprintf("depth-%d", i)},
			Cause:     "pressure-depth",
			Timestamp: 1700000000,
		})
	}
	pm.mu.Unlock()

	if got := hub.PressurePendingDepth(project); got != 3 {
		t.Fatalf("pending depth = %d, want 3", got)
	}
	if got := hub.PressurePendingDepth("pressure-unknown"); got != 0 {
		t.Fatalf("unknown project depth = %d, want 0", got)
	}
}

// TestPressureSweepPokeCounted pins the backstop poke site: a
// dirty project over the op-count cap gets one sweep poke
// from sweepCachesOnce, counted apart from live crossing pokes.
func TestPressureSweepPokeCounted(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := pressureTestHub(t, backend)
	project := "pressure-sweep"

	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	for i := 0; i < maxPendingOpsPerProject; i++ {
		pm.opStack.append(Op{
			Type:      OpMkdir,
			Paths:     []string{fmt.Sprintf("sweep-%d", i)},
			Cause:     "pressure-sweep",
			Timestamp: 1700000000,
		})
	}
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	before := hub.PressureSnapshot()
	hub.sweepCachesOnce()
	after := hub.PressureSnapshot()
	if got := after.SweepRetryPokes - before.SweepRetryPokes; got != 1 {
		t.Fatalf("sweep retry delta = %d, want 1", got)
	}
	if got := after.ForceRetryPokes - before.ForceRetryPokes; got != 0 {
		t.Fatalf("live force-retry delta = %d, want 0", got)
	}
}

// TestPressureSnapshotIsolation pins copy semantics: mutating a
// returned snapshot must not affect later snapshots.
func TestPressureSnapshotIsolation(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := pressureTestHub(t, backend)

	first := hub.PressureSnapshot()
	if first.FailureStreaks == nil {
		t.Fatal("snapshot streak map must be non-nil for safe reads")
	}
	first.FailureStreaks["poison"] = 99
	second := hub.PressureSnapshot()
	if got := second.FailureStreaks["poison"]; got != 0 {
		t.Fatalf("snapshot aliasing: poison streak = %d, want 0", got)
	}
}
