package storage

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// syncBuffer is a goroutine-safe bytes.Buffer: multiple commit loops log
// into the captured output concurrently.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

const testMetadataPath = "/contents/.storhub/metadata.json"

// pollUntil spins until cond holds or the deadline passes; event-driven
// commits are async, so tests synchronize on observable effects instead
// of sleeping fixed intervals.
func pollUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func projectMeta(t *testing.T, hub *StorHub, project string) *projectMetadata {
	t.Helper()
	hub.metaMu.RLock()
	pm := hub.metaCache[project]
	hub.metaMu.RUnlock()
	if pm == nil {
		t.Fatalf("project %q missing from metadata cache", project)
	}
	return pm
}

func metaIsClean(pm *projectMetadata) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return !pm.dirty
}

// TestMetadataCommitsOnTriggerWithoutTicker pins the pure-event commit
// contract: a mutation publishes metadata with no periodic flush behind
// it - there is no interval left to rely on.
func TestMetadataCommitsOnTriggerWithoutTicker(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())
	var puts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testMetadataPath) {
			puts.Add(1)
		}
		return false
	})
	if err := hub.MkdirContext(context.Background(), "project-event-commit", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pollUntil(t, 3*time.Second, "triggered metadata commit", func() bool { return puts.Load() >= 1 })
}

// TestFailedMetadataCommitRetainsDirtyUntilRetrigger pins failure
// semantics without a timer: a failed push keeps dirty state and makes
// exactly one attempt per trigger; the next trigger retries; Shutdown
// performs the final drain attempt.
func TestFailedMetadataCommitRetainsDirtyUntilRetrigger(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())
	ctx := context.Background()
	project := "project-failed-push"

	var attempts atomic.Int32
	var fail atomic.Bool
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut || !strings.Contains(r.URL.Path, testMetadataPath) {
			return false
		}
		attempts.Add(1)
		if fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})

	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pollUntil(t, 3*time.Second, "clean baseline commit", func() bool { return attempts.Load() >= 1 })
	pm := projectMeta(t, hub, project)
	pollUntil(t, 3*time.Second, "baseline drain to clean", func() bool { return metaIsClean(pm) })

	fail.Store(true)
	if err := hub.MkdirContext(ctx, project, "late"); err != nil {
		t.Fatalf("mutate under failing push: %v", err)
	}
	pollUntil(t, 3*time.Second, "first failed attempt", func() bool { return attempts.Load() >= 2 })

	// No timer may retry behind the scenes: attempts freeze until the
	// next explicit trigger.
	frozen := attempts.Load()
	time.Sleep(150 * time.Millisecond)
	if got := attempts.Load(); got != frozen {
		t.Fatalf("failed push retried on its own: attempts went %d -> %d without a trigger", frozen, got)
	}
	if metaIsClean(pm) {
		t.Fatal("dirty state must survive a failed push")
	}

	fail.Store(false)
	if err := hub.MkdirContext(ctx, project, "late2"); err != nil {
		t.Fatalf("retriggering mutation: %v", err)
	}
	pollUntil(t, 3*time.Second, "retry after retrigger", func() bool { return attempts.Load() > frozen && metaIsClean(pm) })
	beforeShutdown := attempts.Load()

	fail.Store(true)
	if err := hub.MkdirContext(ctx, project, "late3"); err != nil {
		t.Fatalf("mutate for shutdown drain: %v", err)
	}
	pollUntil(t, 3*time.Second, "pre-shutdown failed attempt", func() bool { return attempts.Load() > beforeShutdown })
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if got := attempts.Load(); got != beforeShutdown+2 {
		t.Fatalf("shutdown must make exactly one final attempt: before=%d after=%d", beforeShutdown, got)
	}

	select {
	case <-pm.stoppedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("commit loop did not exit after shutdown")
	}
}

// TestQueueAtimeUpdatePokesCommitLoop proves atime dirtiness reaches the
// remote via the read's own trigger instead of waiting for a later
// mutation or shutdown.
func TestQueueAtimeUpdatePokesCommitLoop(t *testing.T) {
	backend := newMockGitHub(t)
	cfg := singleChunkTestConfig()
	cfg.AtimePolicy = storcfg.AtimeStrict
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	project := "project-atime-poke"

	seed := writeTempFile(t, t.TempDir(), "watched.txt", []byte("read me"))
	if _, err := hub.UploadFileContext(ctx, project, "watched.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}
	pm := projectMeta(t, hub, project)
	pollUntil(t, 3*time.Second, "seed commit drain", func() bool { return metaIsClean(pm) })

	var puts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testMetadataPath) {
			puts.Add(1)
		}
		return false
	})
	baseline := puts.Load()

	// The fixed test clock seeds AccessedAt; any later timestamp moves it.
	hub.QueueAtimeUpdateContext(ctx, project, "watched.txt", false, time.Unix(1700000000, 0).Add(time.Hour).Unix())
	pollUntil(t, 3*time.Second, "atime-triggered metadata commit", func() bool { return puts.Load() > baseline })
}

// TestInvalidateRepoMetadataStopsCommitLoop pins the leak fix: removing a
// project from the cache stops its commit loop instead of orphaning it.
func TestInvalidateRepoMetadataStopsCommitLoop(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())
	ctx := context.Background()
	project := "project-invalidate"

	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pm := projectMeta(t, hub, project)

	hub.invalidateRepoMetadata(project)

	select {
	case <-pm.stoppedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("invalidated project leaked a live commit loop")
	}
	hub.metaMu.RLock()
	_, cached := hub.metaCache[project]
	hub.metaMu.RUnlock()
	if cached {
		t.Fatal("invalidated entry still resident in cache")
	}

	// Re-creating the project must yield exactly one fresh loop: the old
	// one is provably gone (stoppedCh above), so no duplicate can tick.
	reborn := hub.getOrCreateProjectMeta(project)
	if reborn == pm {
		t.Fatal("re-created project reused the invalidated instance")
	}
	if reborn.stopped {
		t.Fatal("fresh instance must carry a live commit loop")
	}
	select {
	case <-pm.stoppedCh:
	default:
		t.Fatal("old loop resurrected alongside the fresh instance")
	}
}

// TestMaxTrackedProjectsCapEvictsOldestCleanKeepsDirty pins capped-insert
// eviction: growth is the eviction event, the least-recently-used clean
// entry goes first, dirty entries always survive, all-dirty pressure
// degrades honestly (unbounded insert + warn-once), and an evicted
// pointer still revives instead of stranding mutations.
func TestMaxTrackedProjectsCapEvictsOldestCleanKeepsDirty(t *testing.T) {
	backend := newMockGitHub(t)
	cfg := singleChunkTestConfig()
	cfg.MaxTrackedProjects = 3
	logBuf := &syncBuffer{}
	cfg.LogOutput = logBuf
	base := time.Unix(1700000000, 0)
	var step atomic.Int64
	cfg.Now = func() time.Time {
		return base.Add(time.Duration(step.Add(1)) * time.Millisecond)
	}
	hub := backend.newClient(t, cfg)

	markDirty := func(pm *projectMetadata) {
		pm.mu.Lock()
		markProjectDirtyLocked(pm)
		pm.mu.Unlock()
	}
	isStopped := func(pm *projectMetadata) bool {
		pm.mu.RLock()
		defer pm.mu.RUnlock()
		return pm.stopped
	}

	p1 := hub.getOrCreateProjectMeta("cap-a")
	markDirty(p1)
	p2 := hub.getOrCreateProjectMeta("cap-b")
	hub.getOrCreateProjectMeta("cap-c")

	// Insert #4 over the cap: cap-a is dirty and survives; cap-b is the
	// oldest clean entry and is evicted with its loop stopped.
	hub.getOrCreateProjectMeta("cap-d")
	hub.metaMu.RLock()
	_, bCached := hub.metaCache["cap-b"]
	hub.metaMu.RUnlock()
	if bCached {
		t.Fatal("oldest clean entry was not evicted at cap")
	}
	select {
	case <-p2.stoppedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("evicted entry leaked a live commit loop")
	}
	for _, name := range []string{"cap-a", "cap-c", "cap-d"} {
		hub.metaMu.RLock()
		_, ok := hub.metaCache[name]
		hub.metaMu.RUnlock()
		if !ok {
			t.Fatalf("survivor %s was evicted", name)
		}
	}
	if isStopped(p1) {
		t.Fatal("dirty entry must never be evicted")
	}

	// Stale-pointer revival across cap eviction: a long operation that
	// captured cap-b before its eviction still lands its mutation.
	p2.mu.Lock()
	p2.meta.EnsureDirectory("late", time.Now().Unix())
	hub.markProjectDirtyLiveLocked("cap-b", p2)
	p2.mu.Unlock()
	if isStopped(p2) {
		t.Fatal("mutation must revive the cap-evicted instance")
	}
	hub.metaMu.RLock()
	_, bCached = hub.metaCache["cap-b"]
	hub.metaMu.RUnlock()
	if !bCached {
		t.Fatal("revived entry was not re-inserted into the cache")
	}

	// All entries dirty now (a, b revived-dirty, c, d): pressure inserts
	// unbounded and flags the crossing exactly once per threshold.
	markDirty(hub.getOrCreateProjectMeta("cap-c"))
	markDirty(hub.getOrCreateProjectMeta("cap-d"))
	hub.getOrCreateProjectMeta("cap-e")
	markDirty(hub.getOrCreateProjectMeta("cap-e"))
	hub.getOrCreateProjectMeta("cap-f")
	hub.metaMu.RLock()
	resident := len(hub.metaCache)
	crossed := hub.capWarned
	hub.metaMu.RUnlock()
	if resident != 6 {
		t.Fatalf("all-dirty pressure must insert unbounded, resident=%d", resident)
	}
	if !crossed {
		t.Fatal("all-dirty overflow must flag the threshold crossing")
	}
	// Both overflow inserts crossed the cap, but exactly one warning may
	// fire per threshold crossing - honest degradation without spam.
	if warns := strings.Count(logBuf.String(), "tracked projects exceed cap"); warns != 1 {
		t.Fatalf("cap overflow warning fired %d times, want exactly 1 per crossing", warns)
	}

	// Re-arm proof: drop residency back under the cap by invalidating
	// entries (their repos were never created on the mock, so draining
	// through commits is not available here). The next insertion runs in
	// healthy territory and must clear the crossing flag, making a LATER
	// all-dirty crossing warn again instead of staying silent forever.
	for _, name := range []string{"cap-c", "cap-d", "cap-e", "cap-f"} {
		hub.invalidateRepoMetadata(name)
	}
	hub.getOrCreateProjectMeta("cap-g")
	hub.metaMu.RLock()
	rearmed := !hub.capWarned
	hub.metaMu.RUnlock()
	if !rearmed {
		t.Fatal("returning under the cap must re-arm the overflow warning")
	}
	markDirty(hub.getOrCreateProjectMeta("cap-a"))
	markDirty(hub.getOrCreateProjectMeta("cap-b"))
	markDirty(hub.getOrCreateProjectMeta("cap-g"))
	hub.getOrCreateProjectMeta("cap-h")
	if warns := strings.Count(logBuf.String(), "tracked projects exceed cap"); warns != 2 {
		t.Fatalf("second all-dirty crossing must warn again, total warnings=%d", warns)
	}
}
