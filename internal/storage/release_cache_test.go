package storage

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
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

// A rival writer may create the next release between our list and our
// create (single suspected prod instance: v17/v18's shared timestamp).
// GitHub answers the loser's create with 422 already_exists; we must reuse
// the rival's release, not fail the upload.
func TestRegressionDoubleCreateReusesRivalRelease(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("a"))
	meta, err := hub.UploadFile("project-race", "a.txt", input)
	if err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-race")
	firstRelease := repoMeta.Chunks[meta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-race", firstRelease, 999)
	// Prime the release cache with only the (now full) first release, then
	// let the rival create v2 out-of-band: our cache is stale by design.
	if _, err := hub.listReleases(context.Background(), "project-race"); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	rival := backend.addRelease(t, "project-race", "v2")
	workingMeta := repoMeta.Clone()
	tag, _, err := hub.getOrCreateUploadRelease(context.Background(), "project-race", &workingMeta, 1)
	if err != nil {
		t.Fatalf("must reuse rival release instead of failing: %v", err)
	}
	if tag != rival.tag {
		t.Fatalf("expected rival release %s, got %s", rival.tag, tag)
	}
}

// The picker must prefer the oldest release with space (lowest v number):
// pack elders full before opening new headroom, and always consume the
// curated empty releases instead of stranding them. API order is string
// sort ("v10" < "v9"), so the preference is enforced prod-side and is
// independent of listing order.
func TestRegressionPickerPrefersOldestRelease(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("a"))
	meta, err := hub.UploadFile("project-oldest", "a.txt", input)
	if err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-oldest")
	firstRelease := repoMeta.Chunks[meta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-oldest", firstRelease, 999)
	backend.addRelease(t, "project-oldest", "v9")
	backend.addAssetToRelease(t, "project-oldest", "v9", "elder.bin", []byte("elder"))
	backend.addRelease(t, "project-oldest", "v10")
	input2 := writeTempFile(t, t.TempDir(), "b.txt", []byte("b"))
	meta2, err := hub.UploadFile("project-oldest", "b.txt", input2)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	repoMeta2, _, _ := hub.loadRepoMetadata(context.Background(), "project-oldest")
	if got := repoMeta2.Chunks[meta2.Chunks[0]].Release; got != "v9" {
		t.Fatalf("expected oldest-with-space v9, landed %s", got)
	}
}

// Purge reclaims orphaned storage; an empty release holds nothing, so it
// must never be dropped. Curated rotation targets (and freshly created
// releases awaiting their first upload) are exactly such empties.
func TestRegressionPurgeKeepsEmptyReleases(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "kept.txt", []byte("kept payload"))
	if _, err := hub.UploadFile("project-purge-empty", "kept.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	empty := backend.addRelease(t, "project-purge-empty", "v-empty")
	result, err := hub.PurgeUntracked("project-purge-empty")
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if result.DeletedReleases != 0 {
		t.Fatalf("purge must not drop empty releases, got %+v", result)
	}
	if backend.repo("project-purge-empty").releasesByTag[empty.tag] == nil {
		t.Fatal("empty release was deleted by purge")
	}
}

// A concurrent writer winning the same asset name surfaces as 422
// already_exists; the sink must retry with a fresh name and the file must
// land intact. collideNext injects exactly one such collision.
func TestRegressionAssetNameCollisionRetries(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	backend.collideNext = map[string]bool{"project-collide/v1": true}
	payload := bytes.Repeat([]byte("c"), int(testSmallChunkSize)) // exactly one chunk
	input := writeTempFile(t, t.TempDir(), "collide.txt", payload)
	if _, err := hub.UploadFile("project-collide", "collide.txt", input); err != nil {
		t.Fatalf("upload must survive one name collision: %v", err)
	}
	if n := len(backend.repo("project-collide").assets); n != 1 {
		t.Fatalf("expected exactly 1 stored asset after retry, got %d", n)
	}
	output := filepath.Join(t.TempDir(), "collide.out")
	if err := hub.DownloadFile("project-collide", "collide.txt", output); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("downloaded content differs after collision retry")
	}
}

// (2a) A multi-chunk file whose release fills mid-upload must spread the
// remaining chunks onto a fresh release with both tags registered, and the
// file must download intact. v1 sits at 999 true assets behind a truncated
// 950 embedded array, so the picker targets it and chunk 2 forces rotation.
func TestRegressionMultiChunkFileRotatesMidUpload(t *testing.T) {
	backend := newMockGitHub(t)
	backend.embedCap = 950
	hub := backend.newClient(t, smallTransferTestConfig())
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("s"))
	seedMeta, err := hub.UploadFile("project-spread", "seed.txt", seed)
	if err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-spread")
	firstRelease := repoMeta.Chunks[seedMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-spread", firstRelease, 998) // 999 true, 950 embedded
	payload := bytes.Repeat([]byte("m"), int(2*testSmallChunkSize+4))  // 3 chunks
	input := writeTempFile(t, t.TempDir(), "spread.bin", payload)
	meta, err := hub.UploadFile("project-spread", "spread.bin", input)
	if err != nil {
		t.Fatalf("spread upload: %v", err)
	}
	if len(meta.Chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(meta.Chunks))
	}
	metaState, _, _ := hub.loadRepoMetadata(context.Background(), "project-spread")
	seen := map[string]bool{}
	for _, id := range meta.Chunks {
		seen[metaState.Chunks[id].Release] = true
	}
	if len(seen) != 2 || !seen[firstRelease] {
		t.Fatalf("expected chunks spread across %s and a new release, got %v", firstRelease, seen)
	}
	for tag := range seen {
		if _, ok := metaState.Releases[tag]; !ok {
			t.Fatalf("rotated release %s missing from metadata catalog", tag)
		}
	}
	output := filepath.Join(t.TempDir(), "spread.out")
	if err := hub.DownloadFile("project-spread", "spread.bin", output); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("downloaded content differs after mid-file rotation")
	}
}
