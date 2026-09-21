package storage

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	shfs "github.com/FarelRA/storhub/internal/fs"
)

func smallRetryDisabledTestConfig() Config {
	return baseTestConfig()
}

func retryTestConfig() Config {
	return Config{
		MaxRetries:        2,
		BaseRetryDelay:    time.Millisecond,
		MaxRetryDelay:     5 * time.Millisecond,
		DisableGitBackend: true,
		RateMaxWait:       disableRateLimit,
		RatePointsPerMin:  testRatePointsPerMin,
		RateContentPerMin: testRateContentPerMin,
	}
}

const (
	testRatePointsPerMin  = int64(1) << 40
	testRateContentPerMin = int64(1) << 40
)

// baseTestConfig is the single funnel for mock-backed test hubs: small
// chunks, no retries, no atime, no git backend, fail-fast governor with an
// effectively infinite per-minute budget. Variants pass opts (withRetries)
// instead of pasting the literal block.
func baseTestConfig(opts ...func(*Config)) Config {
	cfg := Config{
		ChunkSize:         testSmallChunkSize,
		BufferSize:        testSmallBufferSize,
		MaxRetries:        0,
		AtimePolicy:       "noatime",
		DisableGitBackend: true,
		RateMaxWait:       disableRateLimit,
		RatePointsPerMin:  testRatePointsPerMin,
		RateContentPerMin: testRateContentPerMin,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// withRetries enables n retries with millisecond test-scale backoff.
func withRetries(n int) func(*Config) {
	return func(cfg *Config) {
		cfg.MaxRetries = n
		cfg.BaseRetryDelay = time.Millisecond
		cfg.MaxRetryDelay = 5 * time.Millisecond
	}
}

func rateLimitTestConfig(sleep func(context.Context, time.Duration) error) Config {
	return Config{
		MaxRetries:        1,
		BaseRetryDelay:    time.Millisecond,
		MaxRetryDelay:     2 * time.Millisecond,
		Sleep:             sleep,
		Now:               time.Now,
		DisableGitBackend: true,
		RateMaxWait:       15 * time.Minute,
		RatePointsPerMin:  1 << 40,
		RateContentPerMin: 1 << 40,
	}
}

// Zero MaxRetries uniformly means "no retries"; there is no ambiguous
// unset-vs-zero split. Negative values are rejected by Validate.
func TestConfigRetriesSemantics(t *testing.T) {
	t.Parallel()
	defaults := (Config{}).WithDefaults()
	if defaults.MaxRetries != 0 {
		t.Fatalf("zero MaxRetries must mean no retries, got %d", defaults.MaxRetries)
	}
	explicit := (Config{ChunkSize: chunking.DefaultChunkSize, MaxRetries: 3}).WithDefaults()
	if explicit.MaxRetries != 3 {
		t.Fatalf("expected explicit retries preserved, got %d", explicit.MaxRetries)
	}
	if err := (Config{MaxRetries: -1}).WithDefaults().Validate(); err == nil {
		t.Fatal("expected negative MaxRetries to be rejected")
	}
}

func TestRetryOnTransientServerError(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/repos/") && failures.Load() == 0 {
			failures.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upstream issue"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, retryTestConfig())
	input := writeTempFile(t, t.TempDir(), "retry.txt", []byte("retry payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectretry", "retry.txt", input); err != nil {
		t.Fatalf("expected retried idempotent request to succeed, got %v", err)
	}
	if failures.Load() != 1 {
		t.Fatalf("expected one transient failure, got %d", failures.Load())
	}
}

// Non-idempotent POSTs (repo creation, asset upload) must surface transient
// failures instead of blind-retrying them.
func TestNonIdempotentPostsDoNotRetry(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var repoAttempts atomic.Int32
	var uploadAttempts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/user/repos":
			repoAttempts.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upstream issue"}`))
			return true
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/"):
			uploadAttempts.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upload issue"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, retryTestConfig())
	input := writeTempFile(t, t.TempDir(), "noretry.txt", []byte("no retry payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectnoretry", "noretry.txt", input); err == nil {
		t.Fatal("expected non-idempotent POST failure to surface")
	}
	if repoAttempts.Load() != 1 || uploadAttempts.Load() != 0 {
		t.Fatalf("expected exactly one repo POST and no upload POST, got %d/%d", repoAttempts.Load(), uploadAttempts.Load())
	}
}

func TestConstructorDefersAuthentication(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var hits atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && r.URL.Path == "/user" {
			hits.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"bad credentials"}`))
			return true
		}
		return false
	})
	cfg := smallTransferTestConfig()
	cfg.APIBaseURL = backend.server.URL
	cfg.HTTPClient = backend.server.Client()
	cfg.Sleep = func(_ context.Context, _ time.Duration) error { return nil }
	hub, err := NewStorHubWithContext(context.Background(), backend.token, cfg)
	if err != nil {
		t.Fatalf("constructor should not authenticate eagerly: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("expected no auth requests during construction, got %d", hits.Load())
	}
	if _, err := hub.ListFilesContext(context.Background(), "projectauthlazy"); err == nil || !strings.Contains(err.Error(), "resolve authenticated user") {
		t.Fatalf("expected deferred auth failure, got %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected one auth request after first operation, got %d", hits.Load())
	}
}

func TestRateLimitAwareRetry(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var hits atomic.Int32
	var slept atomic.Int64
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && r.URL.Path == "/user" && hits.Load() == 0 {
			hits.Add(1)
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-RateLimit-Remaining", "0")
			// A wide reset window keeps the assertion stable under load:
			// recorded sleeps do not advance the wall clock, so the
			// measured wait must reflect until-reset, not backoff clamps.
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(30*time.Second).Unix(), 10))
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, rateLimitTestConfig(func(_ context.Context, d time.Duration) error {
		slept.Store(int64(d))
		return nil
	}))
	if hub.Owner() != "" {
		t.Fatalf("expected lazy owner resolution, got %s", hub.Owner())
	}
	if _, err := hub.ListFilesContext(context.Background(), "projectratelimitmiss"); err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
		t.Fatalf("expected projectnotfound error after owner resolution, got %v", err)
	}
	if hub.Owner() != backend.owner {
		t.Fatalf("unexpected owner after request: %s", hub.Owner())
	}
	if hits.Load() != 1 {
		t.Fatalf("expected one rate-limited response, got %d", hits.Load())
	}
	waited := time.Duration(slept.Load())
	// New doctrine: a primary rate-limit wait honors x-ratelimit-reset
	// (~30s away) instead of being clamped to MaxRetryDelay. The last
	// recorded sleep is the governor's pre-send wait for the retry.
	if waited < 5*time.Second || waited > 35*time.Second {
		t.Fatalf("rate-limit wait must honor the reset window, got %v", waited)
	}
}

func TestReadAPIsReturnProjectNotFound(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	checks := []struct {
		name string
		fn   func() error
	}{
		{name: "list files", fn: func() error { _, err := hub.ListFilesContext(context.Background(), "missing-project"); return err }},
		{name: "list releases", fn: func() error { _, err := hub.ListReleasesContext(context.Background(), "missing-project"); return err }},
		{name: "list revisions", fn: func() error {
			_, err := hub.ListMetadataRevisionsContext(context.Background(), "missing-project")
			return err
		}},
		{name: "read dir", fn: func() error { _, err := hub.ReadDirContext(context.Background(), "missing-project", ""); return err }},
		{name: "stat path", fn: func() error { _, err := hub.StatPathContext(context.Background(), "missing-project", ""); return err }},
		{name: "rollback", fn: func() error { return hub.RollbackMetadataContext(context.Background(), "missing-project", "commit-1") }},
	}
	for _, check := range checks {
		if err := check.fn(); err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
			t.Fatalf("expected projectnotfound for %s, got %v", check.name, err)
		}
	}
}

func TestListFilesUsesMetadataCache(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	seed := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cache.txt", []byte("cache payload"))
	if _, err := seed.UploadFileContext(context.Background(), "projectcache", "cache.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	var metadataGets atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/contents/.storhub/index.json") {
			metadataGets.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	if _, err := hub.ListFilesContext(context.Background(), "projectcache"); err != nil {
		t.Fatalf("first list files: %v", err)
	}
	if _, err := hub.ListFilesContext(context.Background(), "projectcache"); err != nil {
		t.Fatalf("second list files: %v", err)
	}
	if metadataGets.Load() != 1 {
		t.Fatalf("expected one metadata fetch, got %d", metadataGets.Load())
	}
}

func TestMetadataCacheInvalidatesAcrossMutationsAndDeleteProject(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cachemutate.txt", []byte("cache mutate payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectcachemutate", "cachemutate.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	var metadataGets atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/contents/.storhub/index.json") {
			metadataGets.Add(1)
		}
		return false
	})
	if _, err := hub.ListFilesContext(context.Background(), "projectcachemutate"); err != nil {
		t.Fatalf("list files from warm cache: %v", err)
	}
	if metadataGets.Load() != 0 {
		t.Fatalf("expected warm cache to avoid metadata fetch, got %d", metadataGets.Load())
	}
	if err := hub.DeleteFileContext(context.Background(), "projectcachemutate", "cachemutate.txt"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	beforeListAfterDelete := metadataGets.Load()
	files, err := hub.ListFilesContext(context.Background(), "projectcachemutate")
	if err != nil {
		t.Fatalf("list files after delete: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected deleted file to disappear, got %+v", files)
	}
	if metadataGets.Load() != beforeListAfterDelete {
		t.Fatalf("expected list after delete to reuse updated cache, got %d total fetches", metadataGets.Load())
	}
	if err := hub.DeleteProject("projectcachemutate"); err != nil {
		t.Fatalf("delete project: %v", err)
	}
	beforePostDeleteList := metadataGets.Load()
	if _, err := hub.ListFilesContext(context.Background(), "projectcachemutate"); err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
		t.Fatalf("expected projectnotfound after delete project, got %v", err)
	}
	if metadataGets.Load() <= beforePostDeleteList {
		t.Fatal("expected metadata fetch after cache invalidation from delete project")
	}
}

func TestValidateProjectRejectsInvalidNames(t *testing.T) {
	t.Parallel()
	invalid := []string{"", " ", ".", "..", "bad/name", "bad name", "bad*name", ".hidden", "trailing."}
	for _, name := range invalid {
		if err := validateProject(name); err == nil {
			t.Fatalf("expected invalid project name %q to be rejected", name)
		}
	}
	if err := validateProject("valid.project-name_123"); err != nil {
		t.Fatalf("expected valid project name, got %v", err)
	}
}

// A transient commit failure must NOT discard acknowledged writes: dirty
// state is retained and the commit loop retries it.
func TestTransientMetadataCommitFailureRetriesWithRetainedState(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var failed atomic.Bool
	backend.onContentsPUT(t, func(w http.ResponseWriter, _ *http.Request) bool {
		if !failed.Load() {
			failed.Store(true)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"transient failure"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "retry.txt", []byte("retry payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectretry", "retry.txt", input); err != nil {
		t.Fatalf("upload should succeed (metadata commit is async): %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		files, err := hub.ListFilesContext(context.Background(), "projectretry")
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		if len(files) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained dirty state was not retried after transient failure; files=%+v", files)
		}
		time.Sleep(5 * time.Millisecond)
	}
	repo := backend.repo("projectretry")
	if repo == nil || len(repo.assets) == 0 || repo.releasesByTag["v1"] == nil {
		t.Fatalf("expected immutable data to remain, repo=%+v", repo)
	}
}

func TestUploadRetriesMetadataConflictByReloading(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var conflicts atomic.Int32
	var commitCount atomic.Int32
	backend.onContentsPUT(t, func(w http.ResponseWriter, _ *http.Request) bool {
		commitCount.Add(1)
		if commitCount.Load() == 2 && conflicts.Load() == 0 {
			conflicts.Add(1)
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"sha does not match"}`))
			return true
		}
		return false
	})
	cfg := smallTransferTestConfig()
	cfg.MaxRetries = 2
	cfg.BaseRetryDelay = time.Millisecond
	cfg.MaxRetryDelay = 5 * time.Millisecond
	hub := backend.newClient(t, cfg)

	// First upload to create initial metadata
	input1 := writeTempFile(t, t.TempDir(), "initial.txt", []byte("initial"))
	if _, err := hub.UploadFileContext(context.Background(), "projectconflict", "initial.txt", input1); err != nil {
		t.Fatalf("upload initial file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush first metadata: %v", err)
	}

	// Second upload - its async commit will hit an injected conflict,
	// triggering the rebase: the pending op replays onto upstream and the
	// retry lands it. The observable contract is one conflict plus a
	// converged two-file state (the mutation SURVIVES the conflict now).
	input2 := writeTempFile(t, t.TempDir(), "conflict.txt", []byte("conflict payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectconflict", "conflict.txt", input2); err != nil {
		t.Fatalf("upload should succeed (commit is async): %v", err)
	}
	stateDeadline := time.Now().Add(time.Second)
	for {
		conflictsSeen := conflicts.Load() >= 1
		files, err := hub.ListFilesContext(context.Background(), "projectconflict")
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		if conflictsSeen && len(files) == 2 {
			break
		}
		if time.Now().After(stateDeadline) {
			t.Fatalf("conflict rebase did not converge: conflicts=%d files=%d",
				conflicts.Load(), len(files))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMetadataCommitRetriesTransientFailure(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") && failures.Load() == 0 {
			failures.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary metadata issue"}`))
			return true
		}
		return false
	})
	cfg := smallTransferTestConfig()
	cfg.MaxRetries = 2
	cfg.BaseRetryDelay = time.Millisecond
	cfg.MaxRetryDelay = 5 * time.Millisecond
	hub := backend.newClient(t, cfg)
	input := writeTempFile(t, t.TempDir(), "meta-retry.txt", []byte("meta retry payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectmetaretry", "meta-retry.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	// With batching, commit happens later. Poll (bounded) for the commit,
	// failure, and retry cycle instead of sleeping a fixed duration.
	failureDeadline := time.Now().Add(time.Second)
	for failures.Load() < 1 {
		if time.Now().After(failureDeadline) {
			t.Fatalf("expected one transient metadata failure, got %d", failures.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMetadataBatchesAndFlushes(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	// Upload multiple files - metadata commits happen asynchronously.
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("file-%d.txt", i)
		input := writeTempFile(t, t.TempDir(), name, []byte(name))
		if _, err := hub.UploadFileContext(context.Background(), "projectbatching", name, input); err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
	}

	// Wait for all background commit operations to complete
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	// Verify all 3 files are in the metadata
	files, err := hub.ListFilesContext(context.Background(), "projectbatching")
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 files in metadata, got %d", len(files))
	}
}

func TestListReleasesPaginates(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seed"))
	if _, err := hub.UploadFileContext(context.Background(), "projectreleasepages", "seed.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	for i := 0; i < 105; i++ {
		backend.addRelease(t, "projectreleasepages", fmt.Sprintf("extra-%03d", i))
	}
	releases, err := hub.listReleases(context.Background(), "projectreleasepages")
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 106 {
		t.Fatalf("expected 106 releases, got %d", len(releases))
	}
}

func TestRejectsInvalidMetadataSnapshots(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "invalid.bin", []byte("invalid metadata payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectinvalidmetadata", "invalid.bin", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	meta := mustLoadMetadata(t, backend.repo("projectinvalidmetadata"))
	// corrupt a chunk offset to create invalid metadata
	for _, file := range meta.Files() {
		if len(file.Chunks) > 0 {
			chunk := meta.Chunks()[file.Chunks[0]]
			chunk.Offset = 99
			meta.Chunks()[file.Chunks[0]] = chunk
			break
		}
	}
	backend.setMetadata(t, "projectinvalidmetadata", meta)
	hub = backend.newClient(t, smallTransferTestConfig())
	if _, err := hub.ListFilesContext(context.Background(), "projectinvalidmetadata"); err == nil {
		t.Fatal("expected invalid metadata to be rejected")
	}
}
