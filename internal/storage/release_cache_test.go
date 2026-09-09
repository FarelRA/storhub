package storage

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

func TestRegressionRequiredSlotsNoNewVar(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("a"))
	meta, err := hub.UploadFile("project-required-slots", "a.txt", input)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-required-slots")
	firstRelease := repoMeta.Chunks[meta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-required-slots", firstRelease, 998)
	hub.invalidateReleaseCache("project-required-slots")
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile("a.txt")
	tag1, _, err := hub.getOrCreateUploadRelease(context.Background(), "project-required-slots", &workingMeta, 1)
	if err != nil {
		t.Fatalf("getOrCreate 1 slot: %v", err)
	}
	if tag1 != firstRelease {
		t.Fatalf("expected reuse of %s for 1 slot, got %s", firstRelease, tag1)
	}
	tag2, _, err := hub.getOrCreateUploadRelease(context.Background(), "project-required-slots", &workingMeta, 2)
	if err != nil {
		t.Fatalf("getOrCreate 2 slots: %v", err)
	}
	if tag2 == firstRelease {
		t.Fatalf("expected new release for 2 slots, got same %s", tag2)
	}
	tag0, _, err := hub.getOrCreateUploadRelease(context.Background(), "project-required-slots", &workingMeta, 0)
	if err != nil {
		t.Fatalf("getOrCreate 0 slots: %v", err)
	}
	if tag0 == "" {
		t.Fatalf("expected any release for 0 slots, got empty")
	}
}

func TestRegressionEqualScanOrphaned(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("a"))
	if _, err := hub.UploadFile("project-equal-scan", "a.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-equal-scan")
	backend.addRelease(t, "project-equal-scan", "v999")
	hub.invalidateReleaseCache("project-equal-scan")
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile("a.txt")
	firstRelease := ""
	for tag := range repoMeta.Releases {
		firstRelease = tag
		break
	}
	backend.addAssetsToRelease(t, "project-equal-scan", firstRelease, 999)
	hub.invalidateReleaseCache("project-equal-scan")
	tag, _, err := hub.getOrCreateUploadRelease(context.Background(), "project-equal-scan", &workingMeta, 1)
	if err != nil {
		t.Fatalf("getOrCreate: %v", err)
	}
	if tag != "v999" {
		t.Fatalf("expected orphaned v999 reuse, got %s", tag)
	}
}

func TestRegression422Handling(t *testing.T) {
	if !isAlreadyExists(&ghapi.APIError{StatusCode: http.StatusUnprocessableEntity, Body: `{"message":"Validation Failed","errors":[{"code":"already_exists"}]}`}) {
		t.Fatal("expected already_exists detection")
	}
	if !isReleaseFull(&ghapi.APIError{StatusCode: http.StatusUnprocessableEntity, Body: `file_count limited to 1000`}) {
		t.Fatal("expected file_count detection")
	}
	if !isReleaseFull(&ghapi.APIError{StatusCode: http.StatusUnprocessableEntity, Message: "Validation Failed", Body: `too many assets`}) {
		t.Fatal("expected too many detection")
	}
	// Exact live probe body from storhub-web v18 (2026-09-09): top-level
	// message is bare "Validation Failed", the file_count signal lives in
	// errors[].field/message. Regression lock: this shape must rotate.
	if !isReleaseFull(&ghapi.APIError{StatusCode: http.StatusUnprocessableEntity, Message: "Validation Failed", Body: `{"message":"Validation Failed","request_id":"9870:3DBD4A:DC51C:167E39:6AA1054C","documentation_url":"https://docs.github.com/rest","errors":[{"resource":"ReleaseAsset","code":"custom","field":"file_count","message":"file_count limited to 1000 assets per release"}]}`}) {
		t.Fatal("expected live v18 file_count probe body detection")
	}
	if isAlreadyExists(&ghapi.APIError{StatusCode: http.StatusUnprocessableEntity, Body: `file_count`}) {
		t.Fatal("file_count should not be already_exists")
	}
	if isReleaseFull(&ghapi.APIError{StatusCode: http.StatusUnprocessableEntity, Body: `already_exists`}) {
		t.Fatal("already_exists should not be release full")
	}
	if isAlreadyExists(&ghapi.APIError{StatusCode: http.StatusConflict}) {
		t.Fatal("non-422 should not be already_exists")
	}
}

func TestRegressionReplaceRotatesWhenReleaseFillsMidUpload(t *testing.T) {
	// Reproduces storhub-web v18 (2026-09-09): the cached release list shows
	// space (prod: embedded 980) but the server is full (prod: true 1000).
	// The upload must rotate to a new release instead of failing.
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("a"))
	meta, err := hub.UploadFile("project-rotate-full", "a.txt", input)
	if err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(ctx, "project-rotate-full")
	firstRelease := repoMeta.Chunks[meta.Chunks[0]].Release
	// Fill the release server-side WITHOUT touching the client cache,
	// exactly like prod where embedded counts lag the true count.
	backend.addAssetsToRelease(t, "project-rotate-full", firstRelease, 999)
	input2 := writeTempFile(t, t.TempDir(), "b.txt", []byte("b"))
	meta2, err := hub.UploadFile("project-rotate-full", "b.txt", input2)
	if err != nil {
		t.Fatalf("upload must rotate to a new release, got: %v", err)
	}
	repoMeta2, _, _ := hub.loadRepoMetadata(ctx, "project-rotate-full")
	if got := repoMeta2.Chunks[meta2.Chunks[0]].Release; got == firstRelease {
		t.Fatalf("upload landed on full release %s, must rotate", got)
	}
}

func TestRegressionReleasePickerUsesTrueCountNearCeiling(t *testing.T) {
	// storhub-web v18 (2026-09-09): embedded list shows 980 while the true
	// count is 1000. The picker must resolve the true count inside the
	// danger band instead of trusting the truncated embedded list.
	ctx := context.Background()
	backend := newMockGitHub(t)
	backend.embedCap = 980
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("a"))
	meta, err := hub.UploadFile("project-true-count", "a.txt", input)
	if err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(ctx, "project-true-count")
	fullRelease := repoMeta.Chunks[meta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-true-count", fullRelease, 999)
	hub.invalidateReleaseCache("project-true-count")
	workingMeta := repoMeta.Clone()
	workingMeta.RemoveFile("a.txt")
	tag, _, err := hub.getOrCreateUploadRelease(ctx, "project-true-count", &workingMeta, 1)
	if err != nil {
		t.Fatalf("getOrCreate: %v", err)
	}
	if tag == fullRelease {
		t.Fatalf("picker trusted truncated embedded count and chose server-full %s", tag)
	}
}

func TestRegressionPutFileCompensatesMidUploadFailure(t *testing.T) {
	// A file that dies on its second chunk must not leak the first chunk's
	// asset: only the seed asset may remain afterwards.
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("12345678"))
	if _, err := hub.UploadFile("project-compensate", "seed.txt", seed); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	var posts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			if posts.Add(1) == 2 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"message":"injected failure"}`))
				return true
			}
		}
		return false
	})
	two := writeTempFile(t, t.TempDir(), "two.txt", []byte("123456789"))
	if _, err := hub.UploadFile("project-compensate", "two.txt", two); err == nil {
		t.Fatal("expected injected failure")
	}
	if got := len(backend.repo("project-compensate").assets); got != 1 {
		t.Fatalf("leaked orphan assets after mid-upload failure: got %d assets, want 1 (seed only)", got)
	}
}

func TestRegressionPreferredTagRemoved(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-preferred-removed")
	_, _, err := hub.getOrCreateUploadRelease(context.Background(), "project-preferred-removed", repoMeta, 1)
	if err != nil {
		t.Logf("getOrCreate without hint returned: %v", err)
	}
	_, _, _ = hub.GetOrCreateUploadReleaseContext(context.Background(), "project-preferred-removed", repoMeta, 1)
}

func TestRegressionReleaseCacheLifetime(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("a"))
	if _, err := hub.UploadFile("project-cache-lifetime", "a.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if _, ok := hub.getCachedReleases("project-cache-lifetime"); !ok {
		t.Fatal("expected cache to be populated after first list")
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-cache-lifetime")
	firstRelease := ""
	for tag := range repoMeta.Releases {
		firstRelease = tag
		break
	}
	backend.addAssetsToRelease(t, "project-cache-lifetime", firstRelease, 999)
	backend.addRelease(t, "project-cache-lifetime", "v999")
	workingMeta := repoMeta.Clone()
	tag, _, _ := hub.getOrCreateUploadRelease(context.Background(), "project-cache-lifetime", &workingMeta, 1)
	if tag == "v999" {
		t.Fatal("expected cached result (not v999) before invalidation, got v999")
	}
	hub.invalidateReleaseCache("project-cache-lifetime")
	tag2, _, _ := hub.getOrCreateUploadRelease(context.Background(), "project-cache-lifetime", &workingMeta, 1)
	if tag2 != "v999" {
		t.Fatalf("expected v999 after invalidation, got %s", tag2)
	}
}
