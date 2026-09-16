package storage

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// TestJournalCrashReplay covers the cold-start recovery contract: a hub
// crashes (or is killed) with acknowledged-but-uncommitted mutations; a new
// hub on the same journal dir must replay them onto the remote state and
// commit them.
func TestJournalWrittenByMutations(t *testing.T) {
	t.Parallel()
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
	// Block commits so the async loop can never consume (and clear) the
	// journal: mutations stay pending exactly as in a process killed
	// mid-flight.
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
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

	data, err := os.ReadFile(filepath.Join(journalDir, "proj.jsonl"))
	if err != nil {
		t.Fatalf("journal not written: %v", err)
	}
	ops := hub.journalRead("proj")
	var mkdirs, puts int
	for _, op := range ops {
		switch op.Type {
		case OpMkdir:
			mkdirs++
		case OpPutFile:
			puts++
		}
	}
	if mkdirs != 1 || puts != 1 {
		t.Fatalf("expected journal to fold to 1 mkdir + 1 put, got mkdirs=%d puts=%d raw=%q", mkdirs, puts, string(data))
	}
}

func TestJournalReplayOnColdStart(t *testing.T) {
	t.Parallel()
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

	// Hand-write a journal as if a previous process crashed with these
	// acknowledged-but-uncommitted ops.
	dirEntry := DirMeta{Inode: 50, Mode: 0o755, CreatedAt: 1700000100, ModifiedAt: 1700000100}
	fileEntry := FileMeta{Size: 0, Mode: 0o644, Inode: 51, Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	ops := []Op{
		{Seq: 1, Type: OpMkdir, Paths: []string{"docs"}, Cause: "mkdir", Timestamp: 1700000100, Dir: &dirEntry},
		{Seq: 2, Type: OpPutFile, Paths: []string{"docs/hello.txt"}, Cause: "create", Timestamp: 1700000100, File: &fileEntry},
	}
	var sb strings.Builder
	for _, op := range ops {
		line, _ := json.Marshal(op)
		sb.Write(line)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(journalDir, "proj.jsonl"), []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write journal: %v", err)
	}

	// A fresh hub replays the journal onto remote state at load time. The
	// commit loop stays blocked (GETs are unaffected), so the replay is
	// observed purely in-memory.
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	hub2 := backend.newClient(t, cfg)
	meta, _, err := hub2.LoadRepoMetadataContext(ctx, "proj")
	if err != nil {
		t.Fatalf("load after crash: %v", err)
	}
	if !meta.HasDirectory("docs") || meta.FindFile("docs/hello.txt") == nil {
		t.Fatalf("expected replayed mutations after cold start, got files %+v dirs %+v", meta.Files, meta.Dirs)
	}
	// The replayed ops are pending (dirty) and the journal survives until
	// a commit lands them.
	pm := hub2.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	dirty, pending := pm.dirty, len(pm.opStack.ops)
	pm.mu.RUnlock()
	if !dirty || pending == 0 {
		t.Fatalf("expected replayed ops pending (dirty=%v ops=%d)", dirty, pending)
	}
	if _, err := os.Stat(filepath.Join(journalDir, "proj.jsonl")); err != nil {
		t.Fatalf("journal should survive until commit: %v", err)
	}
	// Unblock and commit: the journal is consumed.
	fail.Store(false)
	if err := hub2.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush replayed ops: %v", err)
	}
	if _, err := os.Stat(filepath.Join(journalDir, "proj.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("expected journal cleared after commit, stat err=%v", err)
	}
}

func TestJournalCorruptTailDropped(t *testing.T) {
	t.Parallel()
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
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
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
	t.Parallel()
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
	t.Parallel()
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
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
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

func TestConflictRebasesOps(t *testing.T) {
	t.Parallel()
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

	// Our pending mutation carries a stale SHA; the commit rebases the
	// ops onto upstream instead of discarding them, and both writers'
	// changes land.
	if err := hub.MkdirContext(ctx, "proj", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := hub.CreateFileContext(ctx, "proj", "docs/hello.txt"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush (expected rebase to converge): %v", err)
	}
	pm := hub.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	dirty := pm.dirty
	stacked := len(pm.opStack.ops)
	pm.mu.RUnlock()
	if dirty || stacked > 0 {
		t.Fatalf("expected rebase to converge clean (dirty=%v ops=%d)", dirty, stacked)
	}
	meta, _, err := hub.LoadRepoMetadataContext(ctx, "proj")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if meta.FindFile("rival.txt") == nil {
		t.Fatal("expected rival writer's file preserved after rebase")
	}
	if meta.FindFile("docs/hello.txt") == nil {
		t.Fatal("expected our rebased mutation committed")
	}
	revisions, err := hub.ListMetadataRevisionsContext(ctx, "proj")
	if err != nil {
		t.Fatalf("revisions: %v", err)
	}
	foundRebaseNote := false
	for _, rev := range revisions {
		if strings.Contains(rev.Message, "rebase:") {
			foundRebaseNote = true
		}
	}
	if !foundRebaseNote {
		t.Fatalf("expected a rebase note across revisions, got %+v", revisions)
	}
}

func TestConflictStrictModeRetainsStack(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true, StrictConflicts: true})
	ctx := context.Background()

	if _, err := hub.UploadFileContext(ctx, "proj", "a.txt", writeTempFile(t, t.TempDir(), "a", []byte(strings.Repeat("a", 16)))); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// The rival rewrites a.txt after our base: our pending put on the
	// same path will conflict at CAS time and the rebase would have to
	// resolve it - strict mode must fail the commit instead.
	rival := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	if _, err := rival.ReplaceFileContext(ctx, "proj", "a.txt", writeTempFile(t, t.TempDir(), "a2", []byte(strings.Repeat("b", 16)))); err != nil {
		t.Fatalf("rival replace: %v", err)
	}
	if err := rival.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("rival flush: %v", err)
	}
	if _, err := hub.ReplaceFileContext(ctx, "proj", "a.txt", writeTempFile(t, t.TempDir(), "a3", []byte(strings.Repeat("c", 16)))); err != nil {
		t.Fatalf("our replace: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err == nil {
		t.Fatal("expected strict mode to fail the conflicting commit")
	}
	pm := hub.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	retained := pm.dirty && len(pm.opStack.ops) > 0
	pm.mu.RUnlock()
	if !retained {
		t.Fatal("expected strict conflict to retain the pending ops")
	}
}

// TestFamilyPropagationCaptured guards the mid-commit replay contract: a
// direct-site mutation (patch/write) propagates identity across the hardlink
// family and touches the parent directory; every touched entry must land in
// the op stack, or a conflict replay would silently revert the propagation.
func TestFamilyPropagationCaptured(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()

	if err := hub.MkdirContext(ctx, "proj", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "base.txt", []byte("hello world"))
	if _, err := hub.UploadFileContext(ctx, "proj", "docs/base.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if _, err := hub.LinkContext(ctx, "proj", "docs/base.txt", "docs/alias.txt"); err != nil {
		t.Fatalf("link: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Write through the hardlink family (direct patch site). Block commits
	// so the async loop cannot consume the stack before we inspect it.
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	if _, err := hub.WriteFileAtContext(ctx, "proj", "docs/base.txt", 6, []byte("storhub")); err != nil {
		t.Fatalf("write-at: %v", err)
	}
	pm := hub.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	ops := pm.opStack.snapshot()
	pm.mu.RUnlock()
	seen := map[string]bool{}
	for _, op := range ops {
		if len(op.Paths) > 0 {
			seen[op.Paths[0]] = true
		}
	}
	if !seen["docs/base.txt"] {
		t.Fatalf("expected primary write op, got %+v", ops)
	}
	if !seen["docs/alias.txt"] {
		t.Fatalf("expected hardlink sibling op (family propagation must be captured), got %+v", ops)
	}
}

func TestAtimeOpEmitted(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
			meta.EnsureDirectory(shfs.ParentPath(path), 1700000000)
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
