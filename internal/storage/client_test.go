package storage

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// oversizeTestEntries is sized so a single directory serializes past the
// 8MiB contents ceiling: ~125 bytes of JSON per entry => ~10MB.
const oversizeTestEntries = 80000

func addBigDir(m *RepoMetadata, entries int) {
	m.EnsureDirectory("big", 1700000000)
	for i := 0; i < entries; i++ {
		name := "big/f" + strconv.Itoa(i) + ".txt"
		m.UpsertFile(name, FileMeta{Size: 0, Mode: 0o644, Inode: m.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}, 1700000000)
	}
}

// waitClean parks until the project's async commit loop has drained all
// dirty state, so a test can mark dirty without racing a buffered trigger.
func waitClean(t *testing.T, pm *projectMetadata) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		pm.mu.Lock()
		dirty := pm.dirty
		pm.mu.Unlock()
		if !dirty {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("metadata never drained")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestOversizeAdmissionRejectsFastWithoutLeakingState pins the oversize-admission contract:
// a mutation rejected at admission must leave the shared tree, the op
// stack, the dirty flag, and the journal exactly as they were; and once the
// ceiling is armed, further growth (transaction path AND direct mutation
// paths) fails fast while shrinks stay open.
func TestOversizeAdmissionRejectsFastWithoutLeakingState(t *testing.T) {
	if testing.Short() {
		t.Skip("oversize fixture is heavy")
	}
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	cfg.JournalDir = t.TempDir()
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	seedMeta(t, hub, "admit", "docs", "a.txt", 1)

	pm := hub.getOrCreateProjectMeta("admit")
	pm.mu.RLock()
	opsBefore, dirtyBefore := len(pm.opStack.ops), pm.dirty
	pm.mu.RUnlock()

	_, err := hub.UpdateRepoMetadataContext(ctx, "admit", func(m *RepoMetadata) error {
		addBigDir(m, oversizeTestEntries)
		return nil
	}, "storhub: oversize import")
	if err == nil || !strings.Contains(err.Error(), "one directory serializes past") {
		t.Fatalf("expected single-object oversize rejection, got %v", err)
	}

	pm.mu.RLock()
	opsAfter, dirtyAfter := len(pm.opStack.ops), pm.dirty
	leaked := pm.meta.FindFile("big/f0.txt") != nil
	capped := pm.sizeCapped
	pm.mu.RUnlock()
	if opsAfter != opsBefore {
		t.Fatalf("rejected mutation leaked ops into the shared stack: %d -> %d", opsBefore, opsAfter)
	}
	if dirtyAfter != dirtyBefore {
		t.Fatal("rejected mutation flipped the dirty flag")
	}
	if leaked {
		t.Fatal("rejected mutation touched the shared tree")
	}
	if _, statErr := os.Stat(filepath.Join(cfg.JournalDir, "admit.jsonl")); !os.IsNotExist(statErr) {
		t.Fatalf("rejected mutation journaled ops (stat err=%v)", statErr)
	}
	if !capped {
		t.Fatal("expected the size-ceiling marker armed after the rejection")
	}

	// Reproduce the livelock state - the shared tree itself is over
	// the ceiling (direct paths can put it there) and the marker is set.
	pm.mu.Lock()
	addBigDir(pm.meta, oversizeTestEntries)
	pm.sizeCapped = true
	pm.mu.Unlock()

	// Growth via the transaction path now fails fast.
	_, err = hub.UpdateRepoMetadataContext(ctx, "admit", func(m *RepoMetadata) error {
		m.EnsureDirectory("more", 1700000100)
		return nil
	}, "storhub: growth")
	if err == nil || !strings.Contains(err.Error(), "over size ceiling") {
		t.Fatalf("expected fail-fast growth rejection while capped, got %v", err)
	}

	// Direct growth paths (which bypass admission) are gated too.
	if _, err := hub.UploadFileContext(ctx, "admit", "late.txt", writeTempFile(t, t.TempDir(), "late", []byte("x"))); err == nil || !strings.Contains(err.Error(), "over size ceiling") {
		t.Fatalf("expected capped upload rejection, got %v", err)
	}

	// Shrinks stay open so the project can always fold back.
	if _, err := hub.UpdateRepoMetadataContext(ctx, "admit", func(m *RepoMetadata) error {
		m.RemoveFile("big/f0.txt")
		return nil
	}, "storhub: shrink"); err != nil {
		t.Fatalf("shrink must stay admitted while capped: %v", err)
	}
}

// TestMigrationClobberDetectedAndRebased pins the migration-clobber contract: a legacy->split migration
// publishes with an empty CAS token, which the contents API treats as "no
// conflict detection". A concurrent migrator that lands between our PUT and
// the next commit must be detected by the read-back verification and
// resolved by rebasing, not silently overwritten.
func TestMigrationClobberDetectedAndRebased(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	project := "migrate-clobber"
	seedLegacyBlob(t, hub, project, "docs", "legacy.txt", 1)

	// Build the rival's split state from the same legacy base (a second
	// mount that migrated first and added its own file).
	legacyData, _, err := hub.gh.GetFileContent(ctx, hub.owner, project, metadataFilePath, "")
	if err != nil {
		t.Fatalf("read legacy blob: %v", err)
	}
	rival := NewRepoMetadata(project)
	if err := rival.FromJSON(legacyData); err != nil {
		t.Fatalf("parse legacy: %v", err)
	}
	rival.EnsureDirectory("rivaldir", 1700000000)
	rival.UpsertFile("rivaldir/rival.txt", FileMeta{Size: 0, Mode: 0o644, Inode: rival.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}, 1700000000)
	rival.MarkSplit()
	rival.Normalize(project, 1700000000)
	rival.RecomputeStats()
	res, err := meta.BuildTree(rival)
	if err != nil {
		t.Fatalf("build rival tree: %v", err)
	}
	mb, err := meta.MarshalManifest(hub.buildManifest(project, rival, res, uint64(len(res.Objects))))
	if err != nil {
		t.Fatalf("marshal rival manifest: %v", err)
	}

	// Plant the rival manifest the first time our migration commit reads
	// index.json back (i.e. right after our unconditional PUT landed):
	// exactly the window where a concurrent migrator clobbers us.
	var sawPut atomic.Bool
	var planted atomic.Bool
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) {
			sawPut.Store(true)
			return false
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, testIndexPath) && sawPut.Load() && planted.CompareAndSwap(false, true) {
			backend.mu.Lock()
			defer backend.mu.Unlock()
			repo := backend.repos[project]
			for sha, data := range res.Objects {
				p := objectRepoPath(sha)
				repo.files[p] = &mockFile{path: p, sha: computeGitBlobSHA(data), data: append([]byte(nil), data...)}
			}
			repo.files[".storhub/index.json"] = &mockFile{path: ".storhub/index.json", sha: computeGitBlobSHA(mb), data: append([]byte(nil), mb...)}
		}
		return false
	})

	// Our migration commit: a mkdir on the legacy project, committed
	// synchronously so the rebase-retry converges inside the call.
	if _, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		m.EnsureDirectory("ours", 1700000000)
		return nil
	}, "storhub: mkdir ours"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("migration commit must converge after clobber detection: %v", err)
	}
	if !planted.Load() {
		t.Fatal("interceptor never observed the migration read-back; fixture broken")
	}

	hub2 := backend.newClient(t, smallTransferTestConfig())
	m, _, err := hub2.LoadRepoMetadataContext(ctx, project)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if m.FindFile("docs/legacy.txt") == nil {
		t.Fatal("legacy content lost across migration")
	}
	if m.FindFile("rivaldir/rival.txt") == nil {
		t.Fatal("concurrent migrator's manifest was clobbered: rival.txt missing")
	}
	if !m.HasDirectory("ours") {
		t.Fatal("our rebased mkdir must survive")
	}
}

// TestMigrationAfterConcurrentMigrateRebases pins the other clobber window: the
// rival's manifest already exists when our migration commit resolves its CAS
// token. CASing with the rival's token would "succeed" while publishing a
// tree that lacks the rival's changes, so the migration must detect that it
// was migrated underneath and rebase first.
func TestMigrationAfterConcurrentMigrateRebases(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	project := "migrate-seq"
	seedLegacyBlob(t, hub, project, "docs", "legacy.txt", 1)

	// Fail (not just delay) our async migration commit so the pending ops
	// are retained and the migration decision is re-resolved only at the
	// explicit flush, after the rival has landed.
	var rejected atomic.Int64
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
			rejected.Add(1)
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})

	if _, err := hub.ListFilesContext(ctx, project); err != nil {
		t.Fatalf("load legacy: %v", err)
	}
	if _, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		m.EnsureDirectory("ours", 1700000000)
		return nil
	}, "storhub: mkdir ours"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Wait until the async loop has attempted (and failed) the migration
	// commit, so it cannot race the rival's publish afterwards.
	deadline := time.Now().Add(5 * time.Second)
	for rejected.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("async migration commit never attempted")
		}
		time.Sleep(time.Millisecond)
	}

	// A rival migrates the same legacy project and commits first.
	rival := backend.newClient(t, smallTransferTestConfig())
	if _, err := rival.ListFilesContext(ctx, project); err != nil {
		t.Fatalf("rival load: %v", err)
	}
	if _, err := rival.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		m.EnsureDirectory("rivaldir", 1700000000)
		return nil
	}, "storhub: mkdir rivaldir"); err != nil {
		t.Fatalf("rival mkdir: %v", err)
	}
	fail.Store(false)
	if err := rival.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("rival flush: %v", err)
	}

	// Our migration commit must rebase onto the rival's split state.
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("our migration commit: %v", err)
	}
	hub2 := backend.newClient(t, smallTransferTestConfig())
	m, _, err := hub2.LoadRepoMetadataContext(ctx, project)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if m.FindFile("docs/legacy.txt") == nil {
		t.Fatal("legacy content lost")
	}
	if !m.HasDirectory("rivaldir") {
		t.Fatal("rival's migrated directory was clobbered in the sequential window")
	}
	if !m.HasDirectory("ours") {
		t.Fatal("our mkdir must survive the rebase")
	}
}

// TestColdCacheUploadHydratesBeforeCommitting pins the cold-cache hydration contract: a mutation that
// reaches a cold (never-hydrated) cache entry must hydrate from the remote
// first. Serving the empty tree to the pre-check and mutating it commits an
// empty tree over real remote state - acknowledged data loss.
func TestColdCacheUploadHydratesBeforeCommitting(t *testing.T) {
	backend := newMockGitHub(t)
	hub1 := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	seedMeta(t, hub1, "cold", "docs", "keep.txt", 1)

	hub2 := backend.newClient(t, smallTransferTestConfig())
	// Create the cache entry WITHOUT hydrating it: exactly the state a
	// concurrent hydration leaves behind while pm.mu is released.
	hub2.getOrCreateProjectMeta("cold")

	if _, err := hub2.UploadFileContext(ctx, "cold", "new.txt", writeTempFile(t, t.TempDir(), "new", []byte("hello"))); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub2.FlushProjectContext(ctx, "cold"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	hub3 := backend.newClient(t, smallTransferTestConfig())
	m, _, err := hub3.LoadRepoMetadataContext(ctx, "cold")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if m.FindFile("docs/keep.txt") == nil {
		t.Fatal("cold-cache upload committed an empty tree over remote state")
	}
	if m.FindFile("new.txt") == nil {
		t.Fatal("our upload must survive")
	}
}

// TestConflictRecoveryRetainsPendingOps pins the conflict-recovery contract: a plain 409 reaching
// recoverMetadataCommitFailure must retain the pending ops (the next
// trigger rebases them). The old branch reloaded remote truth and discarded
// acknowledged work - a path that, for rebase-capable commits, could not
// even be reached without destroying data.
func TestConflictRecoveryRetainsPendingOps(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "conf", "docs", "a.txt", 1)

	pm := hub.getOrCreateProjectMeta("conf")
	waitClean(t, pm)
	// Block commits: the manual marking below must not be consumed by the
	// async loop before the recovery call runs.
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	pm.mu.Lock()
	pm.meta.EnsureDirectory("pending", 1700000100)
	hub.appendOpLocked("conf", pm, Op{Type: OpMkdir, Paths: []string{"pending"}, Cause: "mkdir", Timestamp: 1700000100, Dir: &DirMeta{Inode: 99, Mode: 0o755, CreatedAt: 1700000100, ModifiedAt: 1700000100}})
	markProjectDirtyLocked(pm)
	version := pm.version
	pm.mu.Unlock()

	conflict := &commitError{
		err:     &ghapi.APIError{StatusCode: http.StatusConflict, Message: "sha does not match"},
		version: version,
	}
	hub.recoverMetadataCommitFailure("conf", conflict)

	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if !pm.dirty {
		t.Fatal("conflict recovery discarded the dirty flag")
	}
	if len(pm.opStack.ops) != 1 {
		t.Fatalf("conflict recovery discarded pending ops: %+v", pm.opStack.ops)
	}
	if !pm.meta.HasDirectory("pending") {
		t.Fatal("conflict recovery clobbered the local tree")
	}
	fail.Store(false)
}

// TestRevivalTimeoutRetainsAcknowledgedMutation pins the revival-timeout contract: when the evicted
// commit loop refuses to exit, the mutation must still be marked dirty and
// the entry kept resident - the old path returned without either, stranding
// acknowledged work on an entry outside the cache (survivable only via the
// journal, which is optional).
func TestRevivalTimeoutRetainsAcknowledgedMutation(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	// Fabricate an evicted instance whose loop never exits: stoppedCh stays
	// open, so markProjectDirtyLiveLocked takes the 5s timeout branch.
	pm := &projectMetadata{
		meta:      NewRepoMetadata("strand"),
		sha:       "tok",
		hydrated:  true,
		stopped:   true,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
		triggerCh: make(chan struct{}, 1),
	}
	pm.meta.Normalize("strand", 1700000000)

	done := make(chan struct{})
	go func() {
		defer close(done)
		pm.mu.Lock()
		pm.meta.EnsureDirectory("late", 1700000100)
		hub.markProjectDirtyLiveLocked("strand", pm)
		pm.mu.Unlock()
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("revival timeout never resolved")
	}

	hub.metaMu.RLock()
	resident := hub.metaCache["strand"] == pm
	hub.metaMu.RUnlock()
	if !resident {
		t.Fatal("timed-out revival dropped the entry: the mutation is stranded outside the cache")
	}
	pm.mu.RLock()
	dirty := pm.dirty
	pm.mu.RUnlock()
	if !dirty {
		t.Fatal("timed-out revival did not mark dirty: acknowledged mutation lost")
	}
	// De-escalate so cleanup's shutdown drain does not try to commit the
	// fabricated entry against the mock.
	pm.mu.Lock()
	pm.dirty = false
	pm.mu.Unlock()
}

// TestConcurrentRevivalSingleLoop exercises the concurrent-revival window: several stale
// pointers revive the same evicted instance concurrently while mutators
// read stopped/triggerCh under pm.mu. The channel/flag swap must be
// synchronized (run under -race in the authoritative gate).
func TestConcurrentRevivalSingleLoop(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	project := "revive-race"
	seedMeta(t, hub, project, "docs", "a.txt", 1)

	pm := hub.getOrCreateProjectMeta(project)
	waitClean(t, pm)
	hub.metaMu.Lock()
	pm.mu.Lock()
	pm.stopped = true
	close(pm.stopCh)
	delete(hub.metaCache, project)
	pm.mu.Unlock()
	hub.metaMu.Unlock()
	<-pm.stoppedCh // old loop fully exited

	const writers = 8
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pm.mu.Lock()
			pm.meta.EnsureDirectory("d"+strconv.Itoa(i), int64(1700000100+i))
			trigger := hub.markProjectDirtyLiveLocked(project, pm)
			pm.mu.Unlock()
			select {
			case trigger <- struct{}{}:
			default:
			}
		}(i)
	}
	wg.Wait()

	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush after concurrent revival: %v", err)
	}
	hub2 := backend.newClient(t, smallTransferTestConfig())
	m, _, err := hub2.LoadRepoMetadataContext(ctx, project)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	for i := 0; i < writers; i++ {
		if !m.HasDirectory("d" + strconv.Itoa(i)) {
			t.Fatalf("writer %d's mutation lost across revival", i)
		}
	}
}
