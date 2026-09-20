package storage

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
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
)

// LoadCDNTTL returns the armed TTL (0 = immortal URLs).
func (f *mockFaults) LoadCDNTTL() time.Duration { return time.Duration(f.cdnTTL.Load()) }

// expireCDNOnce fast-forwards the CDN expiry clock once (event, not sleep).
func (m *mockGitHub) expireCDNOnce(d time.Duration) { m.faults.cdnTimeShift.Store(int64(d)) }

// mockTime is the mock's clock: faults.mockNow when pinned, wall time
// otherwise. CDN expiries and rate-limit resets are minted from it so tests
// can freeze time instead of racing the wall clock.
func (m *mockGitHub) mockTime() time.Time {
	if nanos := m.faults.mockNow.Load(); nanos != 0 {
		return time.Unix(0, nanos).UTC()
	}
	return time.Now().UTC()
}

// mockCommitSHA mints a 40-hex commit SHA like real GitHub (sha1 of the
// counter), never the old "commit-%d" shape: a future client-side hex check
// must fail loudly on the mock exactly as it would in prod.
func mockCommitSHA(counter int64) string {
	sum := sha1.Sum([]byte(fmt.Sprintf("commit-%d", counter)))
	return fmt.Sprintf("%x", sum)
}

// mockTestJWT mints an unsigned test JWT carrying only the exp claim,
// mirroring the front-door token shape in
// internal/github/client_test.go:testSignedAssetURL (payload exp is real;
// sig is never verified, by design on both sides).
func mockTestJWT(exp time.Time) string {
	enc := func(v any) string {
		raw, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return enc(map[string]string{"alg": "none", "typ": "JWT"}) + "." +
		enc(map[string]int64{"exp": exp.Unix()}) + ".testsig"
}

// mockSignedURLExpiry returns the earliest expiry carried by a signed CDN
// URL: the minimum of the front-door JWT exp and the backing SAS se
// (mirrors the client's signedURLExpiry min()+margin contract). Legacy
// ?exp= (unix nanos) URLs are honored for back-compat.
func mockSignedURLExpiry(query url.Values) (time.Time, bool) {
	var earliest time.Time
	found := false
	consider := func(t time.Time, ok bool) {
		if ok && (!found || t.Before(earliest)) {
			earliest, found = t, true
		}
	}
	consider(mockJWTExpiry(query.Get("jwt")))
	if se := query.Get("se"); se != "" {
		if exp, err := time.Parse(time.RFC3339, se); err == nil {
			consider(exp, true)
		}
	}
	if exp := query.Get("exp"); exp != "" {
		if nanos, err := strconv.ParseInt(exp, 10, 64); err == nil {
			consider(time.Unix(0, nanos).UTC(), true)
		}
	}
	return earliest, found
}

func (m *mockGitHub) mintSignedCDNURL(assetID int64) string {
	base := fmt.Sprintf("%s/cdn/%d", m.server.URL, assetID)
	ttl := m.faults.LoadCDNTTL()
	if ttl <= 0 {
		ttl = defaultMockCDNTTL
	}
	// JWT exp is whole-second precision: round the short TTL UP to a
	// strictly-future second so a 50ms TTL is not born expired by
	// truncation (a truncated-down exp lands in the past and the
	// first fetch 403s). The se backs it 10 minutes later.
	jwtExp := m.mockTime().Add(ttl).Truncate(time.Second)
	if !jwtExp.After(m.mockTime()) {
		jwtExp = jwtExp.Add(time.Second)
	}
	q := url.Values{}
	q.Set("jwt", mockTestJWT(jwtExp))
	q.Set("se", jwtExp.Add(10*time.Minute).UTC().Format(time.RFC3339))
	return base + "?" + q.Encode()
}

// serveFileContentLocked answers a contents GET (raw or JSON envelope)
// with the given bytes. Caller holds m.mu.
func (m *mockGitHub) serveFileContentLocked(w http.ResponseWriter, filePath string, data []byte, acceptRaw bool) {
	if acceptRaw {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}
	m.writeJSON(w, http.StatusOK, map[string]any{
		"name":     filepath.Base(filePath),
		"path":     filePath,
		"sha":      computeGitBlobSHA(data),
		"encoding": "base64",
		"type":     "file",
		"content":  base64.StdEncoding.EncodeToString(data),
	})
}

func (m *mockGitHub) handleListReleaseAssets(w http.ResponseWriter, r *http.Request, repo *mockRepo, rawID string) {
	releaseID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	release := repo.releasesByID[releaseID]
	if release == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	allAssets := m.releaseAssetsLocked(repo, release.tag)
	m.writePaginationLinks(w, r, len(allAssets))
	m.writeJSON(w, http.StatusOK, paginateSlice(allAssets, r.URL.Query()))
}

// embeddedAssetsLocked mirrors GitHub's truncated embedded asset array when
// embedCap is set; the dedicated list-assets endpoint always serves truth.
func (m *mockGitHub) embeddedAssetsLocked(repo *mockRepo, tag string) []map[string]any {
	assets := m.releaseAssetsLocked(repo, tag)
	if limit := m.faults.embedCap; limit > 0 && len(assets) > limit {
		assets = assets[:limit]
	}
	return assets
}

// tagAssetLocked adds asset to the per-tag index. Caller holds m.mu.
func tagAssetLocked(repo *mockRepo, asset *mockAsset) {
	if repo.assetsByTag == nil {
		repo.assetsByTag = make(map[string]map[int64]*mockAsset)
	}
	byTag := repo.assetsByTag[asset.releaseTag]
	if byTag == nil {
		byTag = make(map[int64]*mockAsset)
		repo.assetsByTag[asset.releaseTag] = byTag
	}
	byTag[asset.id] = asset
}

// untagAssetLocked removes id from the per-tag index. Caller holds m.mu.
func untagAssetLocked(repo *mockRepo, tag string, id int64) {
	byTag := repo.assetsByTag[tag]
	if byTag == nil {
		return
	}
	delete(byTag, id)
	if len(byTag) == 0 {
		delete(repo.assetsByTag, tag)
	}
}

// parsePaging extracts (perPage, page) from a query once for both the
// slicer and the Link-header writer. A missing/invalid per_page defaults
// to 30 like real GitHub (was: "return everything"), so a client that
// forgets per_page still pages instead of passing vacuously.
func parsePaging(query url.Values) (perPage, page int) {
	perPage, _ = strconv.Atoi(query.Get("per_page"))
	page, _ = strconv.Atoi(query.Get("page"))
	if perPage <= 0 {
		perPage = 30
	}
	if page <= 0 {
		page = 1
	}
	return perPage, page
}

// writePaginationLinks mirrors GitHub's RFC 5988 Link headers so paging is
// observable without relying on body-length heuristics.
func (m *mockGitHub) writePaginationLinks(w http.ResponseWriter, r *http.Request, total int) {
	perPage, page := parsePaging(r.URL.Query())
	last := (total + perPage - 1) / perPage
	if last < 1 {
		last = 1
	}
	if page >= last {
		return
	}
	setPage := func(p int) string {
		q := r.URL.Query()
		q.Set("per_page", strconv.Itoa(perPage))
		q.Set("page", strconv.Itoa(p))
		return fmt.Sprintf("http://%s%s?%s", r.Host, r.URL.Path, q.Encode())
	}
	w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="next", <%s>; rel="last"`, setPage(page+1), setPage(last)))
}

// writeAlreadyExists mirrors GitHub's 422 duplicate body (live shape: 422
// Validation Failed with an already_exists error entry).
func (m *mockGitHub) writeAlreadyExists(w http.ResponseWriter, resource, field, value string) {
	m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
		"message": "Validation Failed",
		"errors": []map[string]any{{
			"resource": resource,
			"code":     "already_exists",
			"field":    field,
			"message":  field + " already_exists: " + value,
		}},
	})
}

// TestMockBlobSHAMatchesGitVectors cross-pins the mock's local blob-SHA
// oracle against git's documented hashes: the empty blob must hash to
// e69de29bb2d1d6434b8b29ae775ad8c2e48c5391 (git hash-object -t blob
// /dev/null), and the digest must be sensitive to both content and length.
// If the production helper drifts from real git, this fails while the
// integration pins keep passing, isolating the oracle from the client.
func TestMockBlobSHAMatchesGitVectors(t *testing.T) {
	t.Parallel()
	if got := computeGitBlobSHA(nil); got != "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" {
		t.Fatalf("empty blob SHA = %q, want git's e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", got)
	}
	a := computeGitBlobSHA([]byte("hello"))
	b := computeGitBlobSHA([]byte("hello!"))
	c := computeGitBlobSHA([]byte("hello"))
	if len(a) != 40 {
		t.Fatalf("blob SHA must be 40-hex, got %q", a)
	}
	if a == b {
		t.Fatal("blob SHA must be content-sensitive")
	}
	if a != c {
		t.Fatal("blob SHA must be deterministic")
	}
	if computeGitBlobSHA([]byte("")) != computeGitBlobSHA(nil) {
		t.Fatal("empty slice and nil must hash identically (length-0 header)")
	}
}

func putRawStatus(t *testing.T, backend *mockGitHub, project, path string, body map[string]any) int {
	t.Helper()
	resp := mockRaw(t, backend, http.MethodPut, "/repos/"+backend.owner+"/"+project+"/contents/"+path, body)
	return resp.StatusCode
}

// A sha-less PUT onto an existing path is a create collision and
// must 422 with GitHub's "sha wasn't supplied" validation body - never a
// silent 200 overwrite. Creates answer 201, updates 200, missing message
// and invalid base64 answer 422.
func TestMockContentsPutCreateCollisionIs422(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "put422"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString([]byte("v1"))
	if got := putRawStatus(t, backend, "put422", "f.txt", map[string]any{"message": "create", "content": b64}); got != http.StatusCreated {
		t.Fatalf("create must 201, got %d", got)
	}
	resp := mockRaw(t, backend, http.MethodPut, "/repos/"+backend.owner+"/put422/contents/f.txt", map[string]any{"message": "collision", "content": b64})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("sha-less create onto existing path must 422, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "sha") || !strings.Contains(string(body), "Invalid request") {
		t.Fatalf("422 body must carry GitHub's sha-validation prose, got %q", body)
	}
	// A stale sha still 409s (update precondition), the current sha updates (200).
	if got := putRawStatus(t, backend, "put422", "f.txt", map[string]any{"message": "stale", "content": b64, "sha": "deadbeef"}); got != http.StatusConflict {
		t.Fatalf("stale sha must 409, got %d", got)
	}
	current := computeGitBlobSHA([]byte("v1"))
	if got := putRawStatus(t, backend, "put422", "f.txt", map[string]any{"message": "ok", "content": base64.StdEncoding.EncodeToString([]byte("v2")), "sha": current}); got != http.StatusOK {
		t.Fatalf("matching sha must update with 200, got %d", got)
	}
	if got := putRawStatus(t, backend, "put422", "g.txt", map[string]any{"content": b64}); got != http.StatusUnprocessableEntity {
		t.Fatalf("missing message must 422, got %d", got)
	}
	if got := putRawStatus(t, backend, "put422", "h.txt", map[string]any{"message": "bad b64", "content": "!!!not-base64!!!"}); got != http.StatusUnprocessableEntity {
		t.Fatalf("invalid base64 must 422, got %d", got)
	}
}

// ?ref= must resolve like GitHub - exact commit, any valid commit
// (content at-or-before), 404 for unknown refs, 404 for a path deleted at
// that ref, and HEAD/branch names resolving to current state.
func TestMockUnknownRefIs404(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "ref404"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	get := func(path, ref string) int {
		p := "/repos/" + backend.owner + "/ref404/contents/" + path
		if ref != "" {
			p += "?ref=" + url.QueryEscape(ref)
		}
		return mockRaw(t, backend, http.MethodGet, p, nil).StatusCode
	}
	putRawStatus(t, backend, "ref404", "a.txt", map[string]any{"message": "a1", "content": base64.StdEncoding.EncodeToString([]byte("a1"))})
	// commit SHAs are 40-hex like prod; resolve them dynamically instead of
	// pinning the old counter shape.
	revsA, err := hub.gh.ListFileCommits(ctx, hub.Owner(), "ref404", "a.txt")
	if err != nil || len(revsA) != 1 {
		t.Fatalf("list a.txt commits: %v %+v", err, revsA)
	}
	commitA := revsA[0].SHA
	if len(commitA) != 40 {
		t.Fatalf("mock commit SHAs must be 40-hex like prod, got %q", commitA)
	}
	// commit-1 created a.txt. An unknown SHA must 404, not serve HEAD.
	if got := get("a.txt", "0123456789abcdef"); got != http.StatusNotFound {
		t.Fatalf("unknown ref must 404, got %d", got)
	}
	// A commit that touched a DIFFERENT path still resolves: b.txt's
	// create commit sees a.txt at its commit-1 content.
	putRawStatus(t, backend, "ref404", "b.txt", map[string]any{"message": "b1", "content": base64.StdEncoding.EncodeToString([]byte("b1"))})
	revsB, err := hub.gh.ListFileCommits(ctx, hub.Owner(), "ref404", "b.txt")
	if err != nil || len(revsB) != 1 {
		t.Fatalf("list b.txt commits: %v %+v", err, revsB)
	}
	commitB := revsB[0].SHA
	resp := mockRaw(t, backend, http.MethodGet, "/repos/"+backend.owner+"/ref404/contents/a.txt?ref="+commitB, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid cross-path ref must resolve, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), base64.StdEncoding.EncodeToString([]byte("a1"))) {
		t.Fatalf("ref read must serve the at-or-before content, got %q", body)
	}
	// b.txt did not exist at commit-1: 404 Not Found (not HEAD!).
	if got := get("b.txt", commitA); got != http.StatusNotFound {
		t.Fatalf("path created after ref must 404, got %d", got)
	}
	// HEAD and the default branch resolve to current state.
	if got := get("a.txt", "HEAD"); got != http.StatusOK {
		t.Fatalf("HEAD ref must resolve, got %d", got)
	}
	if got := get("a.txt", defaultBranch); got != http.StatusOK {
		t.Fatalf("branch ref must resolve, got %d", got)
	}
	// Deleting a.txt records a delete commit; reading the path AT that
	// commit 404s while the commit appears in the path's history.
	del := mockRaw(t, backend, http.MethodDelete, "/repos/"+backend.owner+"/ref404/contents/a.txt", map[string]any{"message": "bye", "sha": computeGitBlobSHA([]byte("a1"))})
	if del.StatusCode != http.StatusOK {
		t.Fatalf("delete with matching sha must 200, got %d", del.StatusCode)
	}
	delBody, _ := io.ReadAll(del.Body)
	var parsed struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := json.Unmarshal(delBody, &parsed); err != nil || parsed.Commit.SHA == "" {
		t.Fatalf("delete must report its commit: %v %s", err, delBody)
	}
	if got := get("a.txt", parsed.Commit.SHA); got != http.StatusNotFound {
		t.Fatalf("read at the delete commit must 404, got %d", got)
	}
	revs, err := hub.gh.ListFileCommits(ctx, hub.Owner(), "ref404", "a.txt")
	if err != nil {
		t.Fatalf("list commits: %v", err)
	}
	if len(revs) != 2 || revs[0].SHA != parsed.Commit.SHA {
		t.Fatalf("delete commit must head the path history, got %+v", revs)
	}
}

// A directory listing over GitHub's 1000-entry cap must 403 "too
// large", exactly like the live API, so prune's oversized-repo fallback
// branch is reachable in tests.
func TestMockDirListingOverCapIs403(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "cap403"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	repo := backend.repo("cap403")
	backend.mu.Lock()
	for i := 0; i <= contentsListingCap; i++ {
		name := fmt.Sprintf("big/f%05d", i)
		backend.putMockFileLocked(repo, name, []byte(name))
	}
	backend.mu.Unlock()
	resp := mockRaw(t, backend, http.MethodGet, "/repos/"+backend.owner+"/cap403/contents/big", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("dir over the cap must 403, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "too large") {
		t.Fatalf("403 body must carry GitHub's too-large message, got %q", body)
	}
	// At the cap the listing still answers.
	backend.mu.Lock()
	backend.deleteMockFileLocked(repo, "big/f00000")
	backend.mu.Unlock()
	if got := mockRaw(t, backend, http.MethodGet, "/repos/"+backend.owner+"/cap403/contents/big", nil).StatusCode; got != http.StatusOK {
		t.Fatalf("listing at the cap must 200, got %d", got)
	}
}

// Contents DELETE without a sha must 422 (GitHub requires the blob
// sha), never delete silently.
func TestMockDeleteRequiresSHA(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "del422"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	putRawStatus(t, backend, "del422", "k.txt", map[string]any{"message": "keep", "content": base64.StdEncoding.EncodeToString([]byte("k"))})
	resp := mockRaw(t, backend, http.MethodDelete, "/repos/"+backend.owner+"/del422/contents/k.txt", map[string]any{"message": "wipe"})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("sha-less delete must 422, got %d", resp.StatusCode)
	}
	if backend.repo("del422").files["k.txt"] == nil {
		t.Fatal("refused delete must not remove the file")
	}
}

// DELETE on an unknown repo 404s (the router already refuses unknown
// repos; pin it so the client's 404-is-success tolerance stays exercised).
func TestMockDeleteUnknownRepoIs404(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	if got := mockRaw(t, backend, http.MethodDelete, "/repos/"+backend.owner+"/never-existed", nil).StatusCode; got != http.StatusNotFound {
		t.Fatalf("unknown repo delete must 404, got %d", got)
	}
}

// Legacy setMetadata must store the REAL git blob sha, so a hub that
// reads the seeded file and CAS-writes it back (token computed locally
// from bytes) is not 409ed by a precondition GitHub would pass.
func TestMockLegacySetMetadataStoresRealBlobSHA(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "legacy6"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	md := NewRepoMetadata("legacy6")
	md.EnsureDirectory("docs", 1700000000)
	backend.setMetadata(t, "legacy6", md)
	repo := backend.repo("legacy6")
	backend.mu.Lock()
	file := repo.files[metadataFilePath]
	want := computeGitBlobSHA(file.data)
	backend.mu.Unlock()
	if file.sha != want {
		t.Fatalf("legacy seed must store the real blob sha, got %s want %s", file.sha, want)
	}
	// The read-modify-write the client performs must pass the mock's CAS.
	_, sha, err := hub.gh.GetFileContent(ctx, hub.Owner(), "legacy6", metadataFilePath, "")
	if err != nil {
		t.Fatalf("read legacy: %v", err)
	}
	if _, _, err := hub.gh.PutFileContent(ctx, hub.Owner(), "legacy6", metadataFilePath, file.data, sha, "rewrite"); err != nil {
		t.Fatalf("CAS rewrite of seeded legacy file must succeed: %v", err)
	}
}

// The releases list must be ordered newest-created first, like GitHub
// (created_at desc), not by tag string.
func TestMockReleasesListedNewestFirst(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "relorder"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	for _, tag := range []string{"v9", "v10", "v2"} {
		backend.addRelease(t, "relorder", tag)
	}
	releases, err := hub.gh.ListReleases(ctx, hub.Owner(), "relorder")
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 3 || releases[0].TagName != "v2" || releases[1].TagName != "v10" || releases[2].TagName != "v9" {
		t.Fatalf("releases must come back created-desc (v2, v10, v9), got %+v", releases)
	}
}

// The opt-in rate fault may only be spent on an authenticated API
// route - never on a CDN fetch.
func TestMockRateLimitFaultScopedToAPIRoutes(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	backend.armRateLimit()
	req, _ := http.NewRequest(http.MethodGet, backend.server.URL+"/cdn/404110", nil)
	resp, err := backend.server.Client().Do(req)
	if err != nil {
		t.Fatalf("cdn probe: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Fatal("CDN fetch must not consume the rate fault")
	}
	if got := mockRaw(t, backend, http.MethodGet, "/user", nil).StatusCode; got != http.StatusTooManyRequests {
		t.Fatalf("API route must take the armed fault, got %d", got)
	}
	if backend.faults.rateLimitServed.Load() != 1 {
		t.Fatalf("fault must fire exactly once, served=%d", backend.faults.rateLimitServed.Load())
	}
}

// With cdnTTL armed, signed CDN URLs expire (403) and the client's
// SAS-rejection path re-resolves through the API instead of failing.
func TestMockCDNExpiryForcesReResolution(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	backend.faults.SetCDNTTL(50 * time.Millisecond)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	payload := []byte("expiring cdn payload")
	input := writeTempFile(t, t.TempDir(), "exp.txt", payload)
	if _, err := hub.UploadFileContext(ctx, "cdnexp", "exp.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var apiAssetHits atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			apiAssetHits.Add(1)
		}
		return false
	})
	out := filepath.Join(t.TempDir(), "exp.out")
	if err := hub.DownloadFileContext(ctx, "cdnexp", "exp.txt", out); err != nil {
		t.Fatalf("first download: %v", err)
	}
	// Event, not wall clock: shift the CDN's expiry clock past the TTL
	// once, so the cached signed URL is provably stale for the next fetch.
	// The shift (2s, virtual - zero wall cost) must exceed the minted
	// whole-second JWT exp (up to ~1s out: 50ms TTL rounded up), or the
	// cached URL would still verify and no re-resolution would fire.
	backend.expireCDNOnce(2 * time.Second)
	if err := hub.DownloadFileContext(ctx, "cdnexp", "exp.txt", out); err != nil {
		t.Fatalf("download with expired cached URL must re-resolve, got: %v", err)
	}
	assertFileContent(t, out, payload)
	if apiAssetHits.Load() < 2 {
		t.Fatalf("expired URL must force at least two API resolutions, got %d", apiAssetHits.Load())
	}
}

// An asset GET with a JSON Accept answers with the asset's metadata,
// never the bytes.
func TestMockAssetJSONAcceptReturnsMetadata(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "assetjson"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	backend.addRelease(t, "assetjson", "v1")
	id := backend.addAssetToRelease(t, "assetjson", "v1", "data.bin", []byte("raw bytes should not stream here"))
	resp := mockRaw(t, backend, http.MethodGet, fmt.Sprintf("/repos/%s/assetjson/releases/assets/%d", backend.owner, id), nil)
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("JSON-accept asset GET must answer metadata JSON, got content-type %q", ct)
	}
	var meta map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatalf("decode asset metadata: %v", err)
	}
	if meta["name"] != "data.bin" || meta["id"] != float64(id) {
		t.Fatalf("unexpected asset metadata: %+v", meta)
	}
}

// --- git_repo sabotage checks ------------------------------------------
