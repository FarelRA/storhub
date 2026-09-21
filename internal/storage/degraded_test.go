package storage

// Degraded-mode policy tests (item A4).
//
// A project whose commits fail consecutively past
// MaxConsecutiveCommitFailures degrades: new mutations fail fast with a loud
// typed error while recovery verbs (drain, purge, rollback) keep working.
// Only an explicit ReEnableProject clears the latch; later commit successes
// must not.
//
// RED-first: this file was written before degraded.go existed.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

// degradedTestHub builds a hub with an explicit degraded-mode threshold so
// trip/boundary tests need only a handful of injected failures.
func degradedTestHub(t *testing.T, backend *mockGitHub, threshold int) *StorHub {
	t.Helper()
	cfg := Config{
		ChunkSize:                    64,
		BufferSize:                   testSingleBufferSize,
		MaxRetries:                   0,
		DisableGitBackend:            true,
		JournalDir:                   t.TempDir(),
		MaxConsecutiveCommitFailures: threshold,
	}
	return backend.newClient(t, cfg)
}

// degradedSeed commits one file through the real path so the mock repo
// exists and later detached commits publish cleanly.
func degradedSeed(t *testing.T, hub *StorHub, project string) {
	t.Helper()
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seed"))
	if _, err := hub.UploadFileContext(context.Background(), project, "seed.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(context.Background(), project); err != nil {
		t.Fatalf("seed flush: %v", err)
	}
	// The seed's trigger may still have the loop mid-commit; Flush holds
	// commitMu, so returning here means no commit is in flight and the
	// failure counts below are exact.
}

// driveDegradedFailures records exactly n consecutive commit failures for
// project through a detached pm (no commit loop attached, so no background
// attempt can add to or clear the streak).
func driveDegradedFailures(t *testing.T, hub *StorHub, backend *mockGitHub, project string, n int) {
	t.Helper()
	ctx := context.Background()
	live := hub.getOrCreateProjectMeta(project)
	pm := newDetachedPM(project)
	live.mu.RLock()
	pm.meta, pm.sha = live.meta, live.sha
	live.mu.RUnlock()
	pm.mu.Lock()
	hub.appendOpLocked(project, pm, Op{
		Type:      OpMkdir,
		Paths:     []string{"d"},
		Cause:     "degraded-test",
		Timestamp: 1700000000,
	})
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()
	backend.intercept.Store(
		func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodPut &&
				strings.Contains(r.URL.Path, testIndexPath) {
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
	for i := 0; i < n; i++ {
		if err := hub.commitProjectMetadata(ctx, project, pm); err == nil {
			t.Fatalf("commit %d under intercepted PUT must fail", i)
		}
	}
	// Disarm now (the Cleanup above is backstop only): post-failure steps
	// run against a healthy backend unless the test re-arms.
	backend.intercept.Store(
		(func(http.ResponseWriter, *http.Request) bool)(nil))
}

// dirtyRealPMSilent marks the hub's live pm dirty without poking the commit
// trigger, so the loop stays asleep and the next explicit flush or drain is
// the only commit attempt.
func dirtyRealPMSilent(t *testing.T, hub *StorHub, project, dir string) {
	t.Helper()
	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	hub.appendOpLocked(project, pm, Op{
		Type:      OpMkdir,
		Paths:     []string{dir},
		Cause:     "degraded-test",
		Timestamp: 1700000000,
	})
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()
}

// asDegraded unwraps the fail-fast refusal or fails the test.
func asDegraded(t *testing.T, err error) *DegradedProjectError {
	t.Helper()
	if err == nil {
		t.Fatal("degraded project must refuse new mutations, got nil error")
	}
	var deg *DegradedProjectError
	if !errors.As(err, &deg) {
		t.Fatalf("want *DegradedProjectError, got %T: %v", err, err)
	}
	return deg
}

// TestDegradedConfigDefaults pins the knob semantics: Default() ships a
// nonzero threshold, WithDefaults fills an unset zero, and a negative value
// fails Validate loudly instead of silently disabling the policy.
func TestDegradedConfigDefaults(t *testing.T) {
	t.Parallel()
	if got := DefaultConfig().MaxConsecutiveCommitFailures; got != 8 {
		t.Fatalf("default threshold = %d, want 8", got)
	}
	if got := (Config{}).WithDefaults().MaxConsecutiveCommitFailures; got != 8 {
		t.Fatalf("WithDefaults threshold = %d, want 8", got)
	}
	cfg := DefaultConfig()
	cfg.MaxConsecutiveCommitFailures = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("negative threshold must fail Validate, got nil")
	} else if !strings.Contains(err.Error(), "MaxConsecutiveCommitFailures") {
		t.Fatalf("Validate error must name the knob, got: %v", err)
	}
}

// TestDegradedGateAdmitsBelowThreshold pins the boundary: K-1 consecutive
// failures still admit mutations (transient blips must not degrade).
func TestDegradedGateAdmitsBelowThreshold(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := degradedTestHub(t, backend, 3)
	project := "degraded-admit"

	degradedSeed(t, hub, project)
	driveDegradedFailures(t, hub, backend, project, 2)

	if err := hub.admitMutation(project); err != nil {
		t.Fatalf("admission below threshold must succeed: %v", err)
	}
	if err := hub.MkdirContext(context.Background(), project, "still-fine"); err != nil {
		t.Fatalf("mutation below threshold must succeed: %v", err)
	}
}

// TestDegradedGateTripsAtThreshold pins the trip: at K consecutive failures
// the next mutation fails fast with a typed error naming the project and
// the streak, and the project becomes visible as degraded.
func TestDegradedGateTripsAtThreshold(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := degradedTestHub(t, backend, 3)
	project := "degraded-trip"

	degradedSeed(t, hub, project)
	driveDegradedFailures(t, hub, backend, project, 3)

	if got := hub.PressureFailureStreak(project); got != 3 {
		t.Fatalf("failure streak = %d, want 3", got)
	}
	if got := hub.PressureSnapshot().FailureStreaks[project]; got != 3 {
		t.Fatalf("snapshot streak = %d, want 3", got)
	}

	deg := asDegraded(t, hub.MkdirContext(context.Background(), project, "nope"))
	if deg.Project != project {
		t.Fatalf("degraded error project = %q, want %q", deg.Project, project)
	}
	if deg.Streak < 3 {
		t.Fatalf("degraded error streak = %d, want >= 3", deg.Streak)
	}
	if deg.Threshold != 3 {
		t.Fatalf("degraded error threshold = %d, want 3", deg.Threshold)
	}
	if !strings.Contains(deg.Error(), project) {
		t.Fatalf("degraded error must name the project, got: %v", deg)
	}

	found := false
	for _, name := range hub.DegradedProjects() {
		if name == project {
			found = true
		}
	}
	if !found {
		t.Fatalf("DegradedProjects() = %v, want it to contain %q", hub.DegradedProjects(), project)
	}
}

// TestDegradedUploadGateFirst pins gate placement: the refusal lands before
// any upload work, so even a mutation that could never start (missing input)
// reports the degraded refusal, not the downstream I/O error.
func TestDegradedUploadGateFirst(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := degradedTestHub(t, backend, 3)
	project := "degraded-gate-first"

	degradedSeed(t, hub, project)
	driveDegradedFailures(t, hub, backend, project, 3)

	_, err := hub.UploadFileContext(context.Background(), project, "x.txt", "/nonexistent/input-path")
	_ = asDegraded(t, err)
}

// TestDegradedDefaultThreshold pins the default trip point end to end: a
// zero knob means 8, so 7 failures still admit and the 8th trips the latch.
func TestDegradedDefaultThreshold(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := degradedTestHub(t, backend, 0)
	project := "degraded-default"

	degradedSeed(t, hub, project)
	driveDegradedFailures(t, hub, backend, project, 7)
	if err := hub.admitMutation(project); err != nil {
		t.Fatalf("7 failures must still admit (default threshold 8): %v", err)
	}
	driveDegradedFailures(t, hub, backend, project, 1)
	deg := asDegraded(t, hub.admitMutation(project))
	if deg.Threshold != DefaultConfig().MaxConsecutiveCommitFailures {
		t.Fatalf("threshold = %d, want default %d", deg.Threshold, DefaultConfig().MaxConsecutiveCommitFailures)
	}
}

// TestDegradedNoAutoClearAndReEnable pins the latch: a commit success after
// degrading clears the streak but NOT the refusal; only ReEnableProject
// re-admits. Re-enable is idempotent, and garbage names fail loudly.
func TestDegradedNoAutoClearAndReEnable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := degradedTestHub(t, backend, 3)
	project := "degraded-latch"

	degradedSeed(t, hub, project)
	driveDegradedFailures(t, hub, backend, project, 3)
	_ = asDegraded(t, hub.MkdirContext(ctx, project, "trip"))

	// A success on the rescue path clears the streak (pressure contract)
	// but must not clear the latch.
	dirtyRealPMSilent(t, hub, project, "heal-1")
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush after disarm must succeed: %v", err)
	}
	if got := hub.PressureFailureStreak(project); got != 0 {
		t.Fatalf("streak after success = %d, want 0", got)
	}
	_ = asDegraded(t, hub.MkdirContext(ctx, project, "still-no"))

	if err := hub.ReEnableProject(project); err != nil {
		t.Fatalf("explicit re-enable must succeed: %v", err)
	}
	if err := hub.MkdirContext(ctx, project, "back"); err != nil {
		t.Fatalf("mutation after re-enable must succeed: %v", err)
	}
	for _, name := range hub.DegradedProjects() {
		if name == project {
			t.Fatalf("DegradedProjects() still lists %q after re-enable", project)
		}
	}
	if err := hub.ReEnableProject(project); err != nil {
		t.Fatalf("re-enable of a healthy project must stay nil: %v", err)
	}
	if err := hub.ReEnableProject(""); err == nil {
		t.Fatal("re-enable of an empty project name must fail loudly")
	}
}

// TestDegradedRecoveryBypass pins the reserved-headroom principle: while a
// project is degraded, drain heals dirty state, purge runs, and rollback
// commits, none of them answering the degraded refusal.
func TestDegradedRecoveryBypass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := degradedTestHub(t, backend, 3)
	project := "degraded-bypass"

	degradedSeed(t, hub, project)
	driveDegradedFailures(t, hub, backend, project, 3)
	_ = asDegraded(t, hub.MkdirContext(ctx, project, "trip"))

	// Drain commits through the degraded gate and heals the dirty state.
	dirtyRealPMSilent(t, hub, project, "rescue-me")
	if err := hub.DrainProjectContext(ctx, project); err != nil {
		t.Fatalf("drain on a degraded project must bypass the gate: %v", err)
	}
	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.RLock()
	dirty := pm.dirty
	pm.mu.RUnlock()
	if dirty {
		t.Fatal("drain must have committed the dirty state")
	}

	// Purge and rollback likewise bypass: neither may answer degraded.
	if _, err := hub.PruneProject(project, "assets", 0, false); err != nil {
		var deg *DegradedProjectError
		if errors.As(err, &deg) {
			t.Fatalf("purge must bypass the degraded gate: %v", err)
		}
	}
	revisions, err := hub.ListMetadataRevisionsContext(ctx, project)
	if err != nil || len(revisions) == 0 {
		t.Fatalf("revisions: %v %d", err, len(revisions))
	}
	if err := hub.RollbackMetadataContext(ctx, project, revisions[0].CommitSHA); err != nil {
		var deg *DegradedProjectError
		if errors.As(err, &deg) {
			t.Fatalf("rollback must bypass the degraded gate: %v", err)
		} else {
			t.Fatalf("rollback on a degraded project: %v", err)
		}
	}

	// The rescue successes above must not have cleared the latch.
	_ = asDegraded(t, hub.MkdirContext(ctx, project, "still-no"))
}

// TestDegradedGateCoversDeleteReleaseAndReader pins the follow-up gates:
// delete paths, release deletes, and reader uploads refuse on a latched
// project exactly like the funnel verbs.
func TestDegradedGateCoversDeleteReleaseAndReader(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := degradedTestHub(t, backend, 3)
	project := "degraded-wide"

	degradedSeed(t, hub, project)
	driveDegradedFailures(t, hub, backend, project, 3)
	_ = asDegraded(t, hub.MkdirContext(context.Background(), project, "trip"))

	_ = asDegraded(t, hub.DeleteFileContext(context.Background(), project, "seed.txt"))
	_ = asDegraded(t, hub.DeleteReleaseContext(context.Background(), project, "v1"))
	_, rerr := hub.ReplaceFileFromReaderContext(context.Background(), project, "seed.txt", strings.NewReader("new"))
	_ = asDegraded(t, rerr)
}

// TestDegradedAtimeQueued pins the advisory rule: atime updates on a
// latched project are still queued through the cheap metadata-only path,
// so reads (which queue atime) keep working AND their stamps stay correct
// while degraded. Only the commit outcome stays loud.
func TestDegradedAtimeQueued(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := degradedTestHub(t, backend, 3)
	hub.config.AtimePolicy = "relatime"
	project := "degraded-atime"

	degradedSeed(t, hub, project)
	driveDegradedFailures(t, hub, backend, project, 3)
	_ = asDegraded(t, hub.MkdirContext(ctx, project, "trip"))

	// Keep the backend failing so the queued atime cannot commit
	// successfully behind our back and clear the streak mid-test.
	backend.intercept.Store(
		func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == http.MethodPut &&
				strings.Contains(r.URL.Path, testIndexPath) {
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

	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.RLock()
	before := len(pm.opStack.ops)
	pm.mu.RUnlock()
	// A stamp far ahead of the seed trips the relatime ladder.
	now := hub.config.Now().UnixNano() + 2*86400*1e9
	hub.QueueAtimeUpdateContext(ctx, project, "seed.txt", false, now)
	pm.mu.RLock()
	after := len(pm.opStack.ops)
	queued := pm.meta.FindFile("seed.txt")
	pm.mu.RUnlock()
	if after != before+1 {
		t.Fatalf("degraded atime must queue one op, stack %d -> %d", before, after)
	}
	if queued == nil || queued.AccessedAt != now {
		t.Fatalf("degraded atime must stamp the tree, got %+v", queued)
	}
	if _, err := hub.ReadFileAtContext(ctx, project, "seed.txt", 0, 4); err != nil {
		t.Fatalf("reads must keep working while degraded: %v", err)
	}
	if got := hub.PressureFailureStreak(project); got == 0 {
		t.Fatal("queued atime must not clear the streak")
	}
}
