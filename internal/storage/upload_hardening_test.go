package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
)

const (
	hardeningFileCountBody   = `{"message":"Validation Failed","errors":[{"resource":"ReleaseAsset","code":"custom","field":"file_count","message":"file_count limited to 1000 assets per release"}]}`
	hardeningCollisionBody   = `{"message":"Validation Failed","errors":[{"resource":"ReleaseAsset","code":"already_exists","field":"name","message":"name already_exists: injected"}]}`
	hardeningInjectedFailure = `{"message":"injected failure"}`
)

func hardeningAssetCount(t *testing.T, backend *mockGitHub, project string) int {
	t.Helper()
	repo := backend.repo(project)
	if repo == nil {
		t.Fatalf("expected repo %s to exist", project)
	}
	return len(repo.assets)
}

// A single-edit patch that dies on its second chunk must not leak the
// first chunk's asset: only the seed asset may remain afterwards.
func TestUploadHardeningPatchCompensatesMidUploadFailure(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("12345678"))
	if _, err := hub.UploadFile("project-hardening-patch", "patch.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	var posts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			if posts.Add(1) == 2 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(hardeningInjectedFailure))
				return true
			}
		}
		return false
	})
	if _, err := hub.PatchFileContext(context.Background(), "project-hardening-patch", "patch.txt", 0, 0, []byte("ABCDEFGHI")); err == nil {
		t.Fatal("expected injected failure")
	}
	if got := hardeningAssetCount(t, backend, "project-hardening-patch"); got != 1 {
		t.Fatalf("leaked orphan assets after mid-patch failure: got %d assets, want 1 (seed only)", got)
	}
}

// A batched patch that dies during its second edit must not leak the
// first edit's assets either.
func TestUploadHardeningPatchBatchCompensatesMidUploadFailure(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("0123456789abcdef0123456789abcdef"))
	if _, err := hub.UploadFile("project-hardening-batch", "batch.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	var posts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			if posts.Add(1) == 3 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(hardeningInjectedFailure))
				return true
			}
		}
		return false
	})
	edits := []shfs.RangeEdit{
		{Start: 0, DeleteSize: 0, Data: []byte("ABCDEFGHI")},
		{Start: 16, DeleteSize: 0, Data: []byte("JKLMNOPQR")},
	}
	if _, err := hub.PatchFileRangesContext(context.Background(), "project-hardening-batch", "batch.txt", edits); err == nil {
		t.Fatal("expected injected failure")
	}
	if got := hardeningAssetCount(t, backend, "project-hardening-batch"); got != 4 {
		t.Fatalf("leaked orphan assets after mid-batch failure: got %d assets, want 4 (seed only)", got)
	}
}

// A rewrite that dies on its second dirty chunk must not leak the first.
func TestUploadHardeningRewriteCompensatesMidUploadFailure(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("0123456789abcdef01234567"))
	if _, err := hub.UploadFile("project-hardening-rewrite", "rewrite.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	repoMeta, _, err := hub.loadRepoMetadata(ctx, "project-hardening-rewrite")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	fileMeta := repoMeta.FindFile("rewrite.txt")
	if fileMeta == nil {
		t.Fatal("expected seeded file in metadata")
	}
	snapshot := writeTempFile(t, t.TempDir(), "snap.bin", []byte("ABCDEFGHIJKLmnopqrstuvwx"))
	var posts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			if posts.Add(1) == 2 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(hardeningInjectedFailure))
				return true
			}
		}
		return false
	})
	if _, _, err := hub.buildRewrittenChunks(ctx, "project-hardening-rewrite", repoMeta, *fileMeta, "rewrite.txt", snapshot, 24, []byteRange{{start: 0, end: 16}}); err == nil {
		t.Fatal("expected injected failure")
	}
	if got := hardeningAssetCount(t, backend, "project-hardening-rewrite"); got != 3 {
		t.Fatalf("leaked orphan assets after mid-rewrite failure: got %d assets, want 3 (seed only)", got)
	}
}

// Six consecutive release-full rotations must still upload: rotations
// must not consume the 5x name-collision budget.
func TestUploadHardeningRotationsDoNotConsumeNameBudget(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("12345678"))
	if _, err := hub.UploadFile("project-hardening-rotation", "seed.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	releases, err := hub.listReleases(ctx, "project-hardening-rotation")
	if err != nil || len(releases) == 0 {
		t.Fatalf("list releases: %v (releases=%d)", err, len(releases))
	}
	var posts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			if posts.Add(1) <= 6 {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = w.Write([]byte(hardeningFileCountBody))
				return true
			}
		}
		return false
	})
	sink := hub.newChunkSink(ctx, "project-hardening-rotation", releases[0].TagName, releases[0].UploadURL, 1,
		func(remaining int) (string, string, error) { return releases[0].TagName, releases[0].UploadURL, nil })
	if err := sink.put(bytes.NewReader([]byte("12345678")), 8, 0); err != nil {
		t.Fatalf("six rotations must not exhaust the name budget, got: %v", err)
	}
	if len(sink.results) != 1 {
		t.Fatalf("expected one uploaded chunk, got %d", len(sink.results))
	}
}

// Two rotations plus five collisions must fail as a name-budget
// exhaustion after exactly seven upload attempts: collisions keep the
// max-5 budget even when mixed with rotations.
func TestUploadHardeningNameCollisionBudgetKeptAcrossRotations(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("12345678"))
	if _, err := hub.UploadFile("project-hardening-mixed", "seed.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	releases, err := hub.listReleases(ctx, "project-hardening-mixed")
	if err != nil || len(releases) == 0 {
		t.Fatalf("list releases: %v (releases=%d)", err, len(releases))
	}
	var posts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			n := posts.Add(1)
			w.WriteHeader(http.StatusUnprocessableEntity)
			if n <= 2 {
				_, _ = w.Write([]byte(hardeningFileCountBody))
			} else {
				_, _ = w.Write([]byte(hardeningCollisionBody))
			}
			return true
		}
		return false
	})
	sink := hub.newChunkSink(ctx, "project-hardening-mixed", releases[0].TagName, releases[0].UploadURL, 1,
		func(remaining int) (string, string, error) { return releases[0].TagName, releases[0].UploadURL, nil })
	err = sink.put(bytes.NewReader([]byte("12345678")), 8, 0)
	if err == nil {
		t.Fatal("expected name-budget exhaustion")
	}
	if !strings.Contains(err.Error(), "upload chunk failed after 5 name retries") {
		t.Fatalf("expected name-retries error, got: %v", err)
	}
	if got := posts.Load(); got != 7 {
		t.Fatalf("expected 2 rotations + 5 collisions = 7 attempts, got %d", got)
	}
}

// Creating an already-existing repo must answer the live GitHub shape:
// 422 Validation Failed with an already_exists entry, so the client
// already_exists matcher recognizes it.
func TestUploadHardeningDuplicateRepoMatchesAlreadyExists(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("12345678"))
	if _, err := hub.UploadFile("project-hardening-dup", "seed.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	resp, err := backend.server.Client().Post(backend.server.URL+"/user/repos", "application/json",
		strings.NewReader(`{"name":"project-hardening-dup","private":true}`))
	if err != nil {
		t.Fatalf("duplicate create: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for duplicate repo, got %d (%s)", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode duplicate body: %v", err)
	}
	apiErr := &ghapi.APIError{StatusCode: resp.StatusCode, Message: payload.Message, Body: string(raw)}
	if !isAlreadyExists(apiErr) {
		t.Fatalf("duplicate repo body must match already_exists matcher, got: %s", strings.TrimSpace(string(raw)))
	}
}
