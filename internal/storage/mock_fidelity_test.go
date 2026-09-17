package storage

// Mock-fidelity gaps: the in-memory GitHub mock must mirror the live API
// shapes the production client depends on (redirect/CDN downloads,
// pagination Link headers, auth enforcement, deterministic embed order)
// plus an opt-in rate-limit fault for retry tests.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
)

// Asset downloads must redirect to a signed CDN URL; range fetches then
// hit the CDN without an Authorization header.
func TestMockAssetDownloadRedirectShape(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	uploadFixture(t, backend, "project-cdn", "cdn.txt", []byte("cdn redirect payload"))
	ctx := context.Background()

	var apiAssetPath atomic.Value
	var apiSawAuth atomic.Bool
	backend.onAssetGET(t, func(w http.ResponseWriter, r *http.Request) bool {
		apiAssetPath.Store(r.URL.Path)
		apiSawAuth.Store(r.Header.Get("Authorization") != "")
		return false
	})

	// A fresh client has an empty signed-URL cache, forcing full resolution.
	reader := backend.newClient(t, smallTransferTestConfig())
	output := filepath.Join(t.TempDir(), "cdn.out")
	if err := reader.DownloadFileContext(ctx, "project-cdn", "cdn.txt", output); err != nil {
		t.Fatalf("download: %v", err)
	}
	assertFileContent(t, output, []byte("cdn redirect payload"))
	if !apiSawAuth.Load() {
		t.Fatal("API asset request must carry Authorization")
	}
	if backend.faults.cdnSawAuth.Load() {
		t.Fatal("CDN fetch must not carry Authorization (signed URL is bearer already)")
	}

	// Raw shape: octet-stream GET answers 302 with a CDN Location. The
	// default client follows redirects, so use one that surfaces them.
	assetPath, _ := apiAssetPath.Load().(string)
	if assetPath == "" {
		t.Fatal("expected an API asset request during download")
	}
	noRedirect := backend.server.Client()
	noRedirect.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, _ := http.NewRequest(http.MethodGet, backend.server.URL+assetPath, nil)
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+backend.token)
	req.Header.Set("Range", "bytes=0-3")
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatalf("raw asset get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302 redirect, got %d", resp.StatusCode)
	}
	location := resp.Header.Get("Location")
	if !strings.Contains(location, "/cdn/") {
		t.Fatalf("redirect Location must point at the CDN, got %q", location)
	}
	// The signed URL must carry both real expiries (front-door jwt and
	// backing SAS se): the client lapses its cache at the earlier one.
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse redirect location: %v", err)
	}
	if parsed.Query().Get("jwt") == "" || parsed.Query().Get("se") == "" {
		t.Fatalf("signed URL must mint ?jwt=&se= like prod, got %q", location)
	}
}

// The CDN URL works unauthenticated and honors ranges. The recorded
// asset is the last chunk touched (small chunk sizes split the file),
// so the assertion sizes itself off the asset: full fetch first, then
// its first byte, which must match.
func TestMockCDNRangeFetches(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	payload := []byte("cdn redirect payload")
	uploadFixture(t, backend, "project-cdn-range", "cdn.txt", payload)

	var apiAssetPath atomic.Value
	backend.onAssetGET(t, func(w http.ResponseWriter, r *http.Request) bool {
		apiAssetPath.Store(r.URL.Path)
		return false
	})
	reader := backend.newClient(t, smallTransferTestConfig())
	output := filepath.Join(t.TempDir(), "cdn.out")
	if err := reader.DownloadFileContext(context.Background(), "project-cdn-range", "cdn.txt", output); err != nil {
		t.Fatalf("download: %v", err)
	}
	assetPath, _ := apiAssetPath.Load().(string)
	if assetPath == "" {
		t.Fatal("expected an API asset request during download")
	}
	noRedirect := backend.server.Client()
	noRedirect.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, _ := http.NewRequest(http.MethodGet, backend.server.URL+assetPath, nil)
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+backend.token)
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatalf("raw asset get: %v", err)
	}
	location := resp.Header.Get("Location")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound || !strings.Contains(location, "/cdn/") {
		t.Fatalf("expected 302 to the CDN, got %d %q", resp.StatusCode, location)
	}

	fullReq, _ := http.NewRequest(http.MethodGet, location, nil)
	fullResp, err := backend.server.Client().Do(fullReq)
	if err != nil {
		t.Fatalf("cdn fetch: %v", err)
	}
	full, _ := io.ReadAll(fullResp.Body)
	_ = fullResp.Body.Close()
	if fullResp.StatusCode != http.StatusOK || len(full) == 0 {
		t.Fatalf("cdn full fetch: status %d, %d bytes", fullResp.StatusCode, len(full))
	}
	if !bytes.Contains(payload, full) {
		t.Fatalf("cdn asset %q is not a slice of the uploaded payload", full)
	}
	headReq, _ := http.NewRequest(http.MethodGet, location, nil)
	headReq.Header.Set("Range", "bytes=0-0")
	headResp, err := backend.server.Client().Do(headReq)
	if err != nil {
		t.Fatalf("cdn range fetch: %v", err)
	}
	head, _ := io.ReadAll(headResp.Body)
	_ = headResp.Body.Close()
	if headResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206 from CDN, got %d", headResp.StatusCode)
	}
	if string(head) != string(full[:1]) {
		t.Fatalf("unexpected CDN range body %q, want %q", head, full[:1])
	}
}

// Release listing must emit RFC 5988 Link headers and the client must
// page through them.
func TestMockListReleasesEmitsLinkHeaders(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "project-links"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	for i := 0; i < 205; i++ {
		backend.addRelease(t, "project-links", fmt.Sprintf("tag-%03d", i))
	}

	req, _ := http.NewRequest(http.MethodGet,
		backend.server.URL+"/repos/"+backend.owner+"/project-links/releases?per_page=100&page=1", nil)
	req.Header.Set("Authorization", "Bearer "+backend.token)
	resp, err := backend.server.Client().Do(req)
	if err != nil {
		t.Fatalf("raw list: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	link := resp.Header.Get("Link")
	if !strings.Contains(link, `rel="next"`) || !strings.Contains(link, `rel="last"`) {
		t.Fatalf("expected Link next+last headers, got %q", link)
	}

	releases, err := hub.gh.ListReleases(ctx, hub.Owner(), "project-links")
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 205 {
		t.Fatalf("expected 205 releases across pages, got %d", len(releases))
	}
}

// Edge case: when the total is an exact multiple of the page size the client
// must still fetch the terminating empty page instead of stopping early.
func TestMockListReleasesExactMultipleOfPageSize(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "project-exact100"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	for i := 0; i < 200; i++ {
		backend.addRelease(t, "project-exact100", fmt.Sprintf("tag-%03d", i))
	}
	var pages atomic.Int32
	var sawPage3 atomic.Bool
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/releases") {
			pages.Add(1)
			if r.URL.Query().Get("page") == "3" {
				sawPage3.Store(true)
			}
		}
		return false
	})
	releases, err := hub.gh.ListReleases(ctx, hub.Owner(), "project-exact100")
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 200 {
		t.Fatalf("expected 200 releases, got %d", len(releases))
	}
	if !sawPage3.Load() {
		t.Fatalf("exact multiple of page size must still fetch the terminating page (saw %d page fetches)", pages.Load())
	}
}

// Opt-in rate-limit fault — one 429 with Retry-After, then success.
// The fault is armed after the upload warms the owner/repo caches, so the
// next API call (the flush PUT) deterministically takes the 429 and the
// retry succeeds: served==1 proves the fault fired, puts>=2 proves the
// retry, and the observer proves remote truth converged.
func TestMockRateLimitRetryOptIn(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	// smallRetryTestConfig inherits RateMaxWait:-1 (fail-fast: any
	// rate-limit wait is refused, so no retry can happen). Retrying a 429
	// needs waiting to be allowed.
	throttleCfg := smallTransferTestConfig()
	throttleCfg.MaxRetries = 2
	throttleCfg.BaseRetryDelay = time.Millisecond
	throttleCfg.MaxRetryDelay = 5 * time.Millisecond
	throttleCfg.RateMaxWait = time.Minute
	hub := backend.newClient(t, throttleCfg)
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "throttle.txt", []byte("throttled payload"))
	if _, err := hub.UploadFileContext(ctx, "project-throttle", "throttle.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	// Settle the upload's metadata BEFORE arming: otherwise the async
	// commit loop races the arm and may leave nothing dirty, so the flush
	// PUT that is supposed to take the 429 never happens.
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("settle flush: %v", err)
	}
	var metaPuts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		// The v5 commit writes content-addressed objects then the manifest;
		// a 429 on any of them must trigger a retry, so count the whole
		// .storhub commit surface.
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/.storhub/") {
			metaPuts.Add(1)
		}
		return false
	})
	backend.armRateLimit()
	// A fresh mutation guarantees a dirty metadata commit, so a PUT must
	// follow the armed fault no matter whether the async loop or this
	// flush lands it first.
	if err := hub.MkdirContext(ctx, "project-throttle", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush after throttled fault: %v", err)
	}
	if backend.faults.rateLimitServed.Load() != 1 {
		t.Fatalf("expected exactly one served 429, got %d", backend.faults.rateLimitServed.Load())
	}
	if metaPuts.Load() < 2 {
		t.Fatalf("expected the 429 to trigger a metadata PUT retry, saw %d PUTs", metaPuts.Load())
	}
	observer := backend.newClient(t, smallTransferTestConfig())
	entry, err := observer.StatPathContext(ctx, "project-throttle", "throttle.txt")
	if err != nil {
		t.Fatalf("observer stat after throttled flush: %v", err)
	}
	if entry.Size != int64(len("throttled payload")) {
		t.Fatalf("unexpected entry: %+v", entry)
	}
}

// A one-shot 618 (front-door JWT died first) must re-resolve through the
// API instead of failing the download: the second resolution mints a fresh
// signed URL and the bytes converge.
func TestMockCDN618ForcesReResolution(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	payload := []byte("618 re-resolution payload")
	uploadFixture(t, backend, "project-618", "f.txt", payload)

	var apiAssetHits atomic.Int32
	backend.onAssetGET(t, func(w http.ResponseWriter, r *http.Request) bool {
		apiAssetHits.Add(1)
		return false
	})
	backend.faults.cdnFail618Once.Store(true)

	reader := backend.newClient(t, smallTransferTestConfig())
	out := filepath.Join(t.TempDir(), "f.out")
	if err := reader.DownloadFileContext(context.Background(), "project-618", "f.txt", out); err != nil {
		t.Fatalf("download past a 618 must re-resolve, got: %v", err)
	}
	assertFileContent(t, out, payload)
	if apiAssetHits.Load() < 2 {
		t.Fatalf("618 must force API re-resolution, saw %d asset hits", apiAssetHits.Load())
	}
}

// A slowed CDN stream (cdnDelay) must still converge: the client waits out
// the stall instead of amputating the transfer.
func TestMockCDNDelayStillConverges(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	payload := []byte("slow cdn payload")
	uploadFixture(t, backend, "project-delay", "f.txt", payload)
	backend.faults.cdnDelay.Store(int64(50 * time.Millisecond))

	reader := backend.newClient(t, smallTransferTestConfig())
	out := filepath.Join(t.TempDir(), "f.out")
	start := time.Now()
	if err := reader.DownloadFileContext(context.Background(), "project-delay", "f.txt", out); err != nil {
		t.Fatalf("download over a slow CDN must converge, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("cdnDelay never engaged: download took %s", elapsed)
	}
	assertFileContent(t, out, payload)
}

// mockNow pins the mock clock: with a frozen now and an armed TTL the
// minted jwt/se expiries are exactly now+TTL / now+TTL+10m, so expiry math
// is asserted without racing the wall clock.
func TestMockNowDrivesSignedExpiry(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	frozen := time.Unix(1700000000, 0).UTC()
	backend.faults.mockNow.Store(frozen.UnixNano())
	backend.faults.SetCDNTTL(time.Minute)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "project-now"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	backend.addRelease(t, "project-now", "v1")
	id := backend.addAssetToRelease(t, "project-now", "v1", "data.bin", []byte("x"))

	noRedirect := backend.server.Client()
	noRedirect.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, _ := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/repos/%s/project-now/releases/assets/%d", backend.server.URL, backend.owner, id), nil)
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+backend.token)
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatalf("raw asset get: %v", err)
	}
	location := resp.Header.Get("Location")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("expected 302, got %d", resp.StatusCode)
	}
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse location: %v", err)
	}
	jwtExp, ok := mockJWTExpiry(parsed.Query().Get("jwt"))
	if !ok {
		t.Fatalf("signed URL must carry a parseable jwt exp, got %q", location)
	}
	if want := frozen.Add(time.Minute); !jwtExp.Equal(want) {
		t.Fatalf("jwt exp = %v, want frozen now+TTL %v", jwtExp, want)
	}
	se, err := time.Parse(time.RFC3339, parsed.Query().Get("se"))
	if err != nil {
		t.Fatalf("signed URL must carry an RFC3339 se, got %q", location)
	}
	if want := jwtExp.Add(10 * time.Minute); !se.Equal(want) {
		t.Fatalf("se = %v, want jwt exp + 10m %v", se, want)
	}
}

// Production chunk sizes (MaxReleaseAssetSize) with a slow CDN must still
// converge: the scaled transfer deadline covers the stall instead of
// amputating at the 5-minute floor the 8B fixtures always take.
func TestMockMaxChunkSizeSlowCDNConverges(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	cfg.ChunkSize = chunking.MaxReleaseAssetSize
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	payload := []byte(strings.Repeat("s", 3000))
	input := writeTempFile(t, t.TempDir(), "big.txt", payload)
	fileMeta, err := hub.UploadFileContext(ctx, "project-maxchunk", "big.txt", input)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(fileMeta.Chunks) != 1 {
		t.Fatalf("3KB at MaxReleaseAssetSize must be one chunk, got %d", len(fileMeta.Chunks))
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	backend.faults.cdnDelay.Store(int64(20 * time.Millisecond))
	reader := backend.newClient(t, cfg)
	out := filepath.Join(t.TempDir(), "big.out")
	if err := reader.DownloadFileContext(ctx, "project-maxchunk", "big.txt", out); err != nil {
		t.Fatalf("slow-CDN download at max chunk size must converge, got: %v", err)
	}
	assertFileContent(t, out, payload)
}

// Opt-in exhausted-budget headers (200 + Remaining 0, Reset at mockNow+2s)
// must engage the governor's sustainable-pace wait on the following
// requests: the burst still succeeds, and the recorded sleep proves the
// throttle fired instead of being inferred.
func TestMockRateLimitHeadersThrottleGovernor(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	const frozen = int64(1700000000)
	backend.faults.mockNow.Store(frozen * 1e9)
	var nowUnix atomic.Int64
	nowUnix.Store(frozen)
	var slept atomic.Int32
	cfg := smallTransferTestConfig()
	cfg.Now = func() time.Time { return time.Unix(nowUnix.Load(), 0).UTC() }
	cfg.Sleep = func(_ context.Context, d time.Duration) error {
		slept.Add(1)
		// A recorded sleep advances test time so the wait converges
		// deterministically (same contract as the governor unit tests).
		nowUnix.Add(int64(d.Seconds()))
		return nil
	}
	cfg.RateMaxWait = time.Minute
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "project-headers"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	backend.faults.rateLimitHeadersOnce.Store(true)
	if _, err := hub.gh.ListReleases(ctx, hub.Owner(), "project-headers"); err != nil {
		t.Fatalf("first list (observes headers): %v", err)
	}
	if _, err := hub.gh.ListReleases(ctx, hub.Owner(), "project-headers"); err != nil {
		t.Fatalf("second list (throttled) must still succeed: %v", err)
	}
	if slept.Load() < 1 {
		t.Fatal("exhausted-budget headers must force at least one recorded governor sleep")
	}
}

// A missing per_page defaults to 30 like real GitHub: a client that forgets
// per_page pages instead of receiving the whole collection.
func TestMockPerPageDefaultsTo30(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "project-pp"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	for i := 0; i < 35; i++ {
		backend.addRelease(t, "project-pp", fmt.Sprintf("tag-%02d", i))
	}
	decode := func(path string) []map[string]any {
		t.Helper()
		resp := mockRaw(t, backend, http.MethodGet, path, nil)
		var got []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		return got
	}
	base := "/repos/" + backend.owner + "/project-pp/releases"
	if got := decode(base); len(got) != 30 {
		t.Fatalf("missing per_page must default to 30, got %d items", len(got))
	}
	if got := decode(base + "?page=2"); len(got) != 5 {
		t.Fatalf("page 2 must carry the remaining 5, got %d", len(got))
	}
	if got := decode(base + "?per_page=100"); len(got) != 35 {
		t.Fatalf("explicit per_page=100 must return all 35, got %d", len(got))
	}
}

// API routes require a bearer token; wrong or missing auth 401s.
func TestMockRequiresAuthorization(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	get := func(auth string) int {
		req, _ := http.NewRequest(http.MethodGet, backend.server.URL+"/user", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := backend.server.Client().Do(req)
		if err != nil {
			t.Fatalf("raw get: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.ReadAll(resp.Body)
		return resp.StatusCode
	}
	if got := get(""); got != http.StatusUnauthorized {
		t.Fatalf("missing auth must 401, got %d", got)
	}
	if got := get("Bearer wrong"); got != http.StatusUnauthorized {
		t.Fatalf("wrong token must 401, got %d", got)
	}
	if got := get("Bearer " + backend.token); got != http.StatusOK {
		t.Fatalf("correct token must 200, got %d", got)
	}

	// Uploads without auth are rejected before routing.
	upReq, _ := http.NewRequest(http.MethodPost, backend.server.URL+"/upload/x/y?name=z", strings.NewReader("data"))
	upResp, err := backend.server.Client().Do(upReq)
	if err != nil {
		t.Fatalf("raw upload: %v", err)
	}
	defer func() { _ = upResp.Body.Close() }()
	_, _ = io.ReadAll(upResp.Body)
	if upResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated upload must 401, got %d", upResp.StatusCode)
	}

	// The regular client (correct token) is unaffected.
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "auth.txt", []byte("auth ok"))
	if _, err := hub.UploadFileContext(context.Background(), "project-auth", "auth.txt", input); err != nil {
		t.Fatalf("authenticated upload: %v", err)
	}
}

// Embedded asset arrays must be deterministic (sorted by asset ID),
// not Go map-iteration order.
func TestMockEmbeddedAssetsDeterministicOrder(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "project-order"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	backend.addRelease(t, "project-order", "v1")
	for i := 0; i < 50; i++ {
		backend.addAssetToRelease(t, "project-order", "v1", fmt.Sprintf("asset-%02d.bin", i), []byte("x"))
	}
	previous := ""
	for round := 0; round < 10; round++ {
		release, err := hub.gh.GetReleaseByTag(ctx, hub.Owner(), "project-order", "v1")
		if err != nil {
			t.Fatalf("get release: %v", err)
		}
		if len(release.Assets) != 50 {
			t.Fatalf("expected 50 embedded assets, got %d", len(release.Assets))
		}
		ids := make([]string, 0, len(release.Assets))
		for i, asset := range release.Assets {
			ids = append(ids, strconv.FormatInt(asset.ID, 10))
			if i > 0 && asset.ID <= release.Assets[i-1].ID {
				t.Fatalf("embedded assets not sorted by ID: %v", ids)
			}
		}
		flat := strings.Join(ids, ",")
		if previous != "" && flat != previous {
			t.Fatalf("embedded order not deterministic:\n%s\n%s", previous, flat)
		}
		previous = flat
	}
}
