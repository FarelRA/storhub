package storage

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestJournalCrashReplay covers the cold-start recovery contract: a hub
// crashes (or is killed) with acknowledged-but-uncommitted mutations; a new
// hub on the same journal dir must replay them onto the remote state and
// commit them.
func TestJournalCrashReplay(t *testing.T) {
	backend := newMockGitHub(t)
	journalDir := t.TempDir()
	cfg := Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true, JournalDir: journalDir}
	hub := backend.newClient(t, cfg)
	ctx := context.Background()

	if _, err := hub.UploadFileContext(ctx, "proj", "seed.txt", writeTempFile(t, t.TempDir(), "seed", []byte(strings.Repeat("s", 32)))); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("seed flush: %v", err)
	}

	// Deterministic crash simulation: block every metadata commit with an
	// injected 500 so the following mutations stay pending (stack +
	// journal) exactly as they would in a process killed mid-flight.
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testMetadataPath) && fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	if err := hub.MkdirContext(ctx, "proj", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := hub.CreateFileContext(ctx, "proj", "docs/hello.txt"); err != nil {
		t.Fatalf("create: %v", err)
	}
	pm := hub.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	pending := len(pm.opStack.ops)
	pm.mu.RUnlock()
	if pending == 0 {
		t.Fatal("expected pending ops before the simulated crash")
	}
	// The abandoned hub never shuts down (no drain): the journal survives.
	backend.intercept.Store(func(http.ResponseWriter, *http.Request) bool { return false })

	// New hub, same journal dir: the journal must replay onto remote state.
	hub2 := backend.newClient(t, cfg)
	meta, _, err := hub2.LoadRepoMetadataContext(ctx, "proj")
	if err != nil {
		t.Fatalf("load after crash: %v", err)
	}
	if !meta.HasDirectory("docs") || meta.FindFile("docs/hello.txt") == nil {
		t.Fatalf("expected replayed mutations after cold start, got files %+v dirs %+v", meta.Files, meta.Dirs)
	}
	if err := hub2.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush replayed ops: %v", err)
	}
	// The journal is consumed by the commit.
	if _, err := os.Stat(filepath.Join(journalDir, "proj.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("expected journal cleared after commit, stat err=%v", err)
	}
	// A third hub sees the committed state with no journal involved.
	hub3 := backend.newClient(t, cfg)
	meta3, _, err := hub3.LoadRepoMetadataContext(ctx, "proj")
	if err != nil {
		t.Fatalf("load after replay commit: %v", err)
	}
	if meta3.FindFile("docs/hello.txt") == nil {
		t.Fatal("expected replayed mutation committed remotely")
	}
}

func TestJournalCorruptTailDropped(t *testing.T) {
	backend := newMockGitHub(t)
	journalDir := t.TempDir()
	cfg := Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true, JournalDir: journalDir}
	hub := backend.newClient(t, cfg)
	ctx := context.Background()

	if _, err := hub.UploadFileContext(ctx, "proj", "seed.txt", writeTempFile(t, t.TempDir(), "seed", []byte(strings.Repeat("s", 16)))); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("seed flush: %v", err)
	}
	// Block commits so the pending mkdir cannot be consumed (and its
	// journal cleared) by the async loop before we corrupt the file.
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testMetadataPath) && fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	if err := hub.MkdirContext(ctx, "proj", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Simulate a torn write: garbage appended after the valid lines.
	journal := filepath.Join(journalDir, "proj.jsonl")
	f, err := os.OpenFile(journal, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	if _, err := f.WriteString(`{"seq":99,"type":"pu`); err != nil {
		t.Fatalf("append garbage: %v", err)
	}
	_ = f.Close()
	backend.intercept.Store(func(http.ResponseWriter, *http.Request) bool { return false })

	hub2 := backend.newClient(t, cfg)
	meta, _, err := hub2.LoadRepoMetadataContext(ctx, "proj")
	if err != nil {
		t.Fatalf("load with corrupt journal: %v", err)
	}
	if !meta.HasDirectory("docs") {
		t.Fatal("expected valid journal prefix to replay despite corrupt tail")
	}
}

func TestCommitMessageRecordsOps(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()

	if err := hub.MkdirContext(ctx, "proj", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush mkdir: %v", err)
	}
	if _, err := hub.UploadFileContext(ctx, "proj", "docs/report.txt", writeTempFile(t, t.TempDir(), "report", []byte(strings.Repeat("r", 100)))); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := hub.DeleteFileContext(ctx, "proj", "docs/report.txt"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush delete: %v", err)
	}

	revisions, err := hub.ListMetadataRevisionsContext(ctx, "proj")
	if err != nil {
		t.Fatalf("revisions: %v", err)
	}
	if len(revisions) < 3 {
		t.Fatalf("expected at least 3 revisions, got %d", len(revisions))
	}
	var putMsg, delMsg string
	for _, rev := range revisions {
		if strings.Contains(rev.Message, "put docs/report.txt") && putMsg == "" {
			putMsg = rev.Message
		}
		if strings.Contains(rev.Message, "del docs/report.txt freed") && delMsg == "" {
			delMsg = rev.Message
		}
	}
	if putMsg == "" {
		t.Fatalf("expected a rich put message across revisions, got %+v", revisions)
	}
	if delMsg == "" {
		t.Fatalf("expected a rich delete message across revisions, got %+v", revisions)
	}
}

func TestStackClearedOnCommitRetainedOnFailure(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()

	if _, err := hub.UploadFileContext(ctx, "proj", "a.txt", writeTempFile(t, t.TempDir(), "a", []byte(strings.Repeat("a", 16)))); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	pm := hub.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	cleared := len(pm.opStack.ops) == 0 && !pm.dirty
	pm.mu.RUnlock()
	if !cleared {
		t.Fatal("expected op stack cleared after successful commit")
	}

	// Inject a 500 on the next metadata PUT: the commit fails and the
	// stack must be retained for the retry.
	var fail atomic.Bool
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testMetadataPath) && fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	fail.Store(true)
	if err := hub.MkdirContext(ctx, "proj", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err == nil {
		t.Fatal("expected injected failure to fail the flush")
	}
	pm.mu.RLock()
	retained := pm.dirty && len(pm.opStack.ops) > 0
	pm.mu.RUnlock()
	if !retained {
		t.Fatal("expected op stack retained after failed commit")
	}
	fail.Store(false)
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	pm.mu.RLock()
	clearedAfterRetry := len(pm.opStack.ops) == 0 && !pm.dirty
	pm.mu.RUnlock()
	if !clearedAfterRetry {
		t.Fatal("expected op stack cleared after successful retry")
	}
}

func TestConflictDiscardsStackPhaseA(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()

	if _, err := hub.UploadFileContext(ctx, "proj", "a.txt", writeTempFile(t, t.TempDir(), "a", []byte(strings.Repeat("a", 16)))); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// A rival writer advances the remote metadata behind our back.
	rival := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	if _, err := rival.UploadFileContext(ctx, "proj", "rival.txt", writeTempFile(t, t.TempDir(), "rival", []byte(strings.Repeat("v", 16)))); err != nil {
		t.Fatalf("rival upload: %v", err)
	}
	if err := rival.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("rival flush: %v", err)
	}

	// Our pending mutation now carries a stale SHA; the commit conflicts
	// and Phase A semantics discard the local uncommitted state. The D8
	// contract reports the error to the caller even after recovery, and
	// the async loop's own recovery converges the cache - poll for it.
	if err := hub.MkdirContext(ctx, "proj", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Logf("flush reported the conflict as designed: %v", err)
	}
	pm := hub.getOrCreateProjectMeta("proj")
	pollUntil(t, 5*time.Second, "conflict recovery converges clean", func() bool {
		pm.mu.RLock()
		defer pm.mu.RUnlock()
		return !pm.dirty && len(pm.opStack.ops) == 0
	})
	meta, _, err := hub.LoadRepoMetadataContext(ctx, "proj")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if meta.FindFile("rival.txt") == nil {
		t.Fatal("expected rival writer's file visible after conflict recovery")
	}
}

func TestAtimeOpEmitted(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true, AtimePolicy: "strictatime"})
	ctx := context.Background()

	if _, err := hub.UploadFileContext(ctx, "proj", "a.txt", writeTempFile(t, t.TempDir(), "a", []byte(strings.Repeat("a", 16)))); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	hub.QueueAtimeUpdateContext(ctx, "proj", "a.txt", false, 1700000100)
	pm := hub.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	ops := pm.opStack.snapshot()
	pm.mu.RUnlock()
	found := false
	for _, op := range ops {
		if op.Type == OpSetattr && op.Cause == "atime" && len(op.Paths) > 0 && op.Paths[0] == "a.txt" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected atime setattr op, got %+v", ops)
	}
}

// TestBulkImportStackPerformance guards the coalescing index: a large bulk
// import must synthesize ops without quadratic behavior.
func TestBulkImportStackPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("perf guard skipped in short mode")
	}
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()

	const files = 20000
	started := time.Now()
	_, err := hub.UpdateRepoMetadataContext(ctx, "proj", func(meta *RepoMetadata) error {
		for i := 0; i < files; i++ {
			path := "bulk/dir" + itoa(i%20) + "/f" + itoa(i) + ".txt"
			meta.EnsureDirectory(parentOf(path), 1700000000)
			meta.UpsertFile(path, FileMeta{Size: 1, Inode: meta.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}, 1700000000)
		}
		return nil
	}, "storhub: bulk import")
	if err != nil {
		t.Fatalf("bulk import: %v", err)
	}
	elapsed := time.Since(started)
	if elapsed > 30*time.Second {
		t.Fatalf("bulk op synthesis took %s; coalescing index regressed to quadratic?", elapsed)
	}
	pm := hub.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	n := len(pm.opStack.ops)
	pm.mu.RUnlock()
	if n < files {
		t.Fatalf("expected at least %d ops (one per file), got %d", files, n)
	}
}

func parentOf(path string) string {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[:i]
	}
	return ""
}
