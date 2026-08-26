package storage

import (
	"context"
	"net/http"
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
