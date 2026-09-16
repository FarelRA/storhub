package storage

// Mock-fidelity gaps: the in-memory GitHub mock must mirror the live API
// shapes the production client depends on (redirect/CDN downloads,
// pagination Link headers, auth enforcement, deterministic embed order)
// plus an opt-in rate-limit fault for retry tests.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Asset downloads must redirect to a signed CDN URL; range fetches then
// hit the CDN without an Authorization header.
func TestMockAssetDownloadRedirectsToCDN(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	payload := []byte("cdn redirect payload")
	input := writeTempFile(t, t.TempDir(), "cdn.txt", payload)
	if _, err := hub.UploadFileContext(ctx, "project-cdn", "cdn.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var apiAssetPath atomic.Value
	var apiSawAuth atomic.Bool
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			apiAssetPath.Store(r.URL.Path)
			apiSawAuth.Store(r.Header.Get("Authorization") != "")
		}
		return false
	})

	// A fresh client has an empty signed-URL cache, forcing full resolution.
	reader := backend.newClient(t, smallTransferTestConfig())
	output := t.TempDir() + "/cdn.out"
	if err := reader.DownloadFileContext(ctx, "project-cdn", "cdn.txt", output); err != nil {
		t.Fatalf("download: %v", err)
	}
	assertFileContent(t, output, payload)
	if !apiSawAuth.Load() {
		t.Fatal("API asset request must carry Authorization")
	}
	if backend.cdnSawAuth.Load() {
		t.Fatal("CDN fetch must not carry Authorization (signed URL is bearer already)")
	}

	// Raw shape: octet-stream GET answers 302 with a CDN Location.
	assetPath, _ := apiAssetPath.Load().(string)
	if assetPath == "" {
		t.Fatal("expected an API asset request during download")
	}
	// Raw shape: octet-stream GET answers 302 with a CDN Location. The
	// default client follows redirects, so use one that surfaces them.
	noRedirect := backend.server.Client()
	noRedirect.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, _ := http.NewRequest(http.MethodGet, backend.server.URL+assetPath, nil)
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer token")
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

	// The CDN URL works unauthenticated and honors ranges. The recorded
	// asset is the last chunk touched (small chunk sizes split the file),
	// so size the assertion off the asset itself: full fetch first, then
	// its first byte, which must match.
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
	req.Header.Set("Authorization", "Bearer token")
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
	backend.rateLimitOnce.Store(true)
	// A fresh mutation guarantees a dirty metadata commit, so a PUT must
	// follow the armed fault no matter whether the async loop or this
	// flush lands it first.
	if err := hub.MkdirContext(ctx, "project-throttle", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush after throttled fault: %v", err)
	}
	if backend.rateLimitServed.Load() != 1 {
		t.Fatalf("expected exactly one served 429, got %d", backend.rateLimitServed.Load())
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
	if got := get("Bearer token"); got != http.StatusOK {
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
