package storage

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

const (
	testSmallChunkSize   int64 = 8
	testSmallBufferSize        = 16
	testSingleChunkSize  int64 = 1024
	testSingleBufferSize       = 32
	testLargeChunkSize   int64 = 1 << 30
	testLargeBufferSize        = 4 << 20
	// NOTE: the 1000-assets-per-release ceiling lives in non-test source
	// as releaseAssetCap; tests reference that const, never a literal.
)

func singleChunkTestConfig() Config {
	return Config{
		ChunkSize:         testSingleChunkSize,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	}
}

func smallTransferTestConfig() Config {
	return baseTestConfig()
}

func smallRetryTestConfig() Config {
	return baseTestConfig(withRetries(2))
}

// disableRateLimit is the RateMaxWait sentinel meaning "fail fast, never
// wait on the governor" (was: a bare -1 pasted across builders).
const disableRateLimit = -1

// onContentsPUT arms a one-shot intercept filtered to contents-API PUTs,
// replacing the pasted Store(Contains(..."/contents/...")) closures with a
// named hook. t.Cleanup auto-disarms back to pass-through.
func (m *mockGitHub) onContentsPUT(t *testing.T, fn func(w http.ResponseWriter, r *http.Request) bool) {
	t.Helper()
	m.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			return fn(w, r)
		}
		return false
	})
	t.Cleanup(func() {
		m.intercept.Store((func(http.ResponseWriter, *http.Request) bool)(nil))
	})
}

// onAssetGET arms a one-shot intercept filtered to release-asset GETs
// (both the /releases/assets/<id> resolve and the /releases/<id>/assets
// list endpoint).
func (m *mockGitHub) onAssetGET(t *testing.T, fn func(w http.ResponseWriter, r *http.Request) bool) {
	t.Helper()
	m.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/assets") {
			return fn(w, r)
		}
		return false
	})
	t.Cleanup(func() {
		m.intercept.Store((func(http.ResponseWriter, *http.Request) bool)(nil))
	})
}

// uploadFixture uploads payload to project/path and flushes, returning the
// flushed hub. Shared by fidelity/contract tests instead of pasting the
// writeTempFile+Upload+Flush triple.
func uploadFixture(t *testing.T, backend *mockGitHub, project, name string, payload []byte) *StorHub {
	t.Helper()
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), name, payload)
	if _, err := hub.UploadFileContext(ctx, project, name, input); err != nil {
		t.Fatalf("upload fixture: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush fixture: %v", err)
	}
	return hub
}

// assertNamerShape pins the asset-name contract for n generated names:
// lowercase words+extensions shape, no derivation from source file names,
// no duplicates. Shared by the helper smoke test and the dictionary
// regression test (which adds only the diversity assertion).
func assertNamerShape(t *testing.T, namer *assetNamer, n int) []string {
	t.Helper()
	nameRe := regexp.MustCompile(`^[a-z]+(?:[-_]?[a-z]+){0,4}(?:\.[a-z]+){1,5}$`)
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name, err := namer.Next()
		if err != nil {
			t.Fatalf("generate asset name: %v", err)
		}
		if !nameRe.MatchString(name) {
			t.Fatalf("unexpected asset name format: %q", name)
		}
		if strings.Contains(name, "file") || strings.Contains(name, "txt") || strings.Contains(name, filepath.Base("docs/file.txt")) {
			t.Fatalf("asset name should not derive from source file name: %q", name)
		}
		if _, ok := seen[name]; ok {
			t.Fatalf("duplicate asset name generated: %q", name)
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func liveSmokeConfig() Config {
	return Config{
		ChunkSize:         testLargeChunkSize,
		BufferSize:        testLargeBufferSize,
		MaxRetries:        4,
		RateMaxWait:       -1,
		RatePointsPerMin:  1 << 40,
		RateContentPerMin: 1 << 40,
	}
}

func liveLargeSmokeConfig(client *http.Client) Config {
	cfg := liveSmokeConfig()
	cfg.HTTPClient = client
	cfg.ChunkSize = chunking.MaxReleaseAssetSize
	return cfg
}

func largeValidationConfig() Config {
	return defaultTestConfig()
}

func expectedChunkCount(size, chunkSize int64) int {
	if chunkSize <= 0 {
		chunkSize = chunking.DefaultChunkSize
	}
	if chunkSize > chunking.MaxReleaseAssetSize {
		chunkSize = chunking.MaxReleaseAssetSize
	}
	chunks := int((size + chunkSize - 1) / chunkSize)
	if chunks == 0 {
		return 1
	}
	return chunks
}

func newLiveHub(t *testing.T, token string, cfg Config) *StorHub {
	t.Helper()
	hub, err := NewStorHubWithConfig(token, cfg)
	if err != nil {
		t.Fatalf("new storhub client: %v", err)
	}
	return hub
}

type mockGitHub struct {
	t      *testing.T
	server *mockServerRef
	mu     sync.Mutex
	owner  string
	repos  map[string]*mockRepo
	// intercept is a per-test hook consulted before routing (fault
	// injection, request counting). Reset in t.Cleanup via
	// resetMockMutables; prefer the named onContentsPUT/onAssetGET
	// helpers for new tests over raw path-substring pastes.
	intercept atomic.Value
	// faults groups every fault-injection switch (opt-in 429s, CDN
	// expiry/delay/618, header injector, truncation, collisions) away
	// from server state. All are per-test and reset in t.Cleanup.
	faults mockFaults
	// token is the bearer token expected on API routes (unique per test;
	// the shared server dispatches on it). The CDN host is exempt:
	// signed-URL fetches carry no Authorization header.
	token string
}

// mockFaults holds the mock's fault-injection switches. Each fault is
// opt-in per test and reset by resetMockMutables, so arming one never
// leaks into another test sharing the server socket.
type mockFaults struct {
	// embedCap simulates GitHub truncating the asset array embedded in
	// release objects. When > 0, list/get-release responses carry at most
	// embedCap embedded assets while the true count stays higher. The
	// dedicated list-assets endpoint always serves the true set.
	embedCap int
	// collideNext forces the next upload to repo/tag to fail once with a
	// 422 already_exists body, simulating a concurrent writer winning the
	// same asset name. Keyed "repo/tag". Test-only fault injection.
	collideNext map[string]bool
	// rateLimitOnce makes the next API response a 429 with Retry-After,
	// letting retry tests opt into rate-limit simulation without arming
	// the governor for the whole suite.
	rateLimitOnce atomic.Bool
	// rateLimitHeadersOnce makes the next API response carry exhausted
	// X-RateLimit-* headers (Remaining 0, Reset at mockNow+2s) instead of
	// a 429, exercising the governor's sustainable-pace wait on a 200.
	// NOTE: the governor paces against the client's cfg.Now clock, so
	// tests using this fault must pin faults.mockNow to that same clock.
	rateLimitHeadersOnce atomic.Bool
	// cdnSawAuth records whether any CDN fetch carried an Authorization
	// header (it must not: signed URLs are bearer credentials already).
	cdnSawAuth atomic.Bool
	// rateLimitServed counts opt-in 429s served, so retry tests can prove
	// the fault actually fired instead of inferring it from request counts.
	rateLimitServed atomic.Int32
	// cdnTTL (nanoseconds) makes signed CDN URLs expire: when > 0, the
	// octet-stream redirect carries jwt/se expiries and the CDN 403s
	// expired fetches, exercising the client's SAS-rejection and
	// re-resolution path (real GitHub: Azure SAS URLs are short-lived).
	// Zero (default) keeps URLs immortal so unrelated tests never trip.
	// Use SetCDNTTL/LoadCDNTTL (time.Duration) instead of raw nanos.
	cdnTTL atomic.Int64
	// cdnTimeShift (nanoseconds) fast-forwards the CDN's expiry clock
	// exactly once (consumed by the next signed-URL check), so tests can
	// force "the cached URL is now stale" as an event instead of sleeping
	// past the TTL. The re-resolved URL must then serve normally, so the
	// shift must not linger.
	cdnTimeShift atomic.Int64
	// cdnFail618Once makes the next CDN fetch answer 618
	// (StatusSignedURLExpired: the front-door JWT died) once, exercising
	// the client's 618 re-resolution path. Consumed on fire.
	cdnFail618Once atomic.Bool
	// cdnDelay (nanoseconds) stalls every CDN body by the given duration,
	// simulating a slow stream without real latency elsewhere.
	cdnDelay atomic.Int64
	// mockNow (unix nanos) overrides the mock's clock; 0 means wall time.
	// Pin it to the client's frozen cfg.Now when a test needs TTL/reset
	// headers interpreted against test time instead of wall time.
	mockNow atomic.Int64
}

// SetCDNTTL arms signed-URL expiry; LoadCDNTTL reads it back.
func (f *mockFaults) SetCDNTTL(d time.Duration) { f.cdnTTL.Store(int64(d)) }

// armRateLimit arms one opt-in 429 for the next API call.
func (m *mockGitHub) armRateLimit() { m.faults.rateLimitOnce.Store(true) }

type mockRepo struct {
	name          string
	private       bool
	nextReleaseID int64
	nextBlobID    int64
	nextCommitID  int64
	releasesByTag map[string]*mockRelease
	releasesByID  map[int64]*mockRelease
	assets        map[int64]*mockAsset
	// assetsByTag indexes assets per release tag (name uniqueness,
	// per-tag counts, and per-tag listings all read here instead of
	// scanning every asset). Maintained on upload/delete paths.
	assetsByTag map[string]map[int64]*mockAsset
	files       map[string]*mockFile
	// dirChildren indexes immediate children per directory path:
	// dirChildren[dir][name] = isDir. Maintained by putMockFileLocked /
	// deleteMockFileLocked so ListDir never scans every file.
	dirChildren   map[string]map[string]bool
	commitsByPath map[string][]mockCommit
	// commitTime indexes commit SHA -> commit time across every path, so
	// ?ref= resolution never scans every path's history.
	commitTime map[string]time.Time
}

type mockRelease struct {
	id        int64
	tag       string
	name      string
	uploadURL string
}

type mockAsset struct {
	id         int64
	name       string
	releaseTag string
	data       []byte
}

type mockFile struct {
	path string
	sha  string
	data []byte
}

type mockCommit struct {
	sha     string
	message string
	path    string
	data    []byte
	when    time.Time
	// deleted marks a contents-DELETE commit: the path existed in every
	// earlier commit and is gone from this one onward (GitHub records the
	// delete and 404s file reads at that ref).
	deleted bool
}

func newMockGitHub(t *testing.T) *mockGitHub {
	t.Helper()
	token := fmt.Sprintf("token-%d", sharedMockSeq.Add(1))
	backend := &mockGitHub{
		t:     t,
		owner: "storhub-tester",
		repos: make(map[string]*mockRepo),
		token: token,
	}
	backend.faults.collideNext = make(map[string]bool)
	backend.server = &mockServerRef{URL: sharedMockBaseURL(t), token: token}
	registerMockBackend(token, backend)
	t.Cleanup(func() {
		backend.resetMockMutables()
		backend.server.Close()
	})
	return backend
}

func (m *mockGitHub) newClient(t *testing.T, cfg Config) *StorHub {
	t.Helper()
	cfg.APIBaseURL = m.server.URL
	cfg.HTTPClient = m.server.Client()
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	}
	if cfg.Sleep == nil {
		cfg.Sleep = func(_ context.Context, _ time.Duration) error { return nil }
	}
	hub, err := NewStorHubWithContext(context.Background(), m.token, cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() {
		_ = hub.Shutdown(context.Background())
	})
	return hub
}

// computeGitBlobSHA is intentionally implemented LOCALLY (sha1 over the
// "blob <len>\0" header + bytes), byte-identical to the production client's
// computation in internal/github/client.go:710. It must NOT be imported or
// re-exported from the github package: the mock has to stay an independent
// oracle, so a regression in the shared helper fails the cross-pin test
// below instead of passing vacuously on both sides.
func computeGitBlobSHA(data []byte) string {
	header := fmt.Sprintf("blob %d\x00", len(data))
	h := sha1.New()
	h.Write([]byte(header))
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (m *mockGitHub) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if fn, ok := m.intercept.Load().(func(http.ResponseWriter, *http.Request) bool); ok && fn != nil && fn(w, r) {
		return
	}
	// Mirror GitHub: every API route requires a bearer token. The CDN
	// host is exempt: signed-URL fetches carry no Authorization header.
	if !strings.HasPrefix(r.URL.Path, "/cdn/") && r.Header.Get("Authorization") != "Bearer "+m.authToken() {
		m.writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "Requires authentication"})
		return
	}
	// Opt-in rate-limit fault for retry tests. One 429 with
	// Retry-After, then normal service resumes. Scoped to authenticated
	// API routes: a CDN fetch or an unauthenticated probe must never
	// spend the fault armed for the next API call.
	if !strings.HasPrefix(r.URL.Path, "/cdn/") && m.faults.rateLimitOnce.CompareAndSwap(true, false) {
		m.faults.rateLimitServed.Add(1)
		w.Header().Set("Retry-After", "1")
		m.writeJSON(w, http.StatusTooManyRequests, map[string]any{"message": "API rate limit exceeded for user ID"})
		return
	}
	// Opt-in exhausted-budget headers for governor tests. The next API
	// response carries Remaining 0 with Reset at mockNow+2s (a 200, not a
	// 429), so the governor's sustainable-pace wait engages on the
	// FOLLOWING requests. Scoped to API routes like the 429 fault; the
	// CDN never sends rate-limit headers (its authority has no budget).
	// NOTE: the governor paces against the client's cfg.Now clock, so pin
	// faults.mockNow to that clock or the reset lands years off.
	if !strings.HasPrefix(r.URL.Path, "/cdn/") && m.faults.rateLimitHeadersOnce.CompareAndSwap(true, false) {
		w.Header().Set("X-RateLimit-Limit", "60")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(m.mockTime().Add(2*time.Second).Unix(), 10))
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/user":
		m.writeJSON(w, http.StatusOK, map[string]any{"login": m.owner})
	case r.Method == http.MethodPost && r.URL.Path == "/user/repos":
		m.handleCreateRepo(w, r)
	case strings.HasPrefix(r.URL.Path, "/cdn/"):
		m.handleCDN(w, r)
	case strings.HasPrefix(r.URL.Path, "/repos/"):
		m.handleRepos(w, r)
	case strings.HasPrefix(r.URL.Path, "/upload/"):
		m.handleUpload(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (m *mockGitHub) authToken() string {
	if m.token == "" {
		return "token"
	}
	return m.token
}

// mockJWTExpiry decodes the exp claim from a compact JWS without verifying
// the signature (mirrors the client's jwtExpiry: expiry is public metadata).
func mockJWTExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp <= 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Exp, 0).UTC(), true
}

// mintSignedCDNURL builds the redirect target for an octet-stream asset GET:
// a /cdn/<id> URL carrying BOTH real expiries (front-door jwt with a short
// TTL, backing SAS se 10 minutes later), exactly the shape the production
// client parses via signedURLExpiry. The JWT governs: the client's cache
// lapses at the earlier expiry minus margin, never at the SAS alone.
// defaultMockCDNTTL is the signed-URL lifetime minted when a test does not
// arm cdnTTL: URLs always carry both real expiries (front-door jwt, backing
// SAS se) like production, so the client's proactive min(jwt,se)-30s cache
// path is exercised on every download, not just TTL-pinned tests. An hour
// keeps test traffic far from expiry without wall-clock dependence.
const defaultMockCDNTTL = time.Hour

// findAssetLocked resolves a global asset ID to its record. Caller holds
// m.mu. Asset IDs are globally unique (globalMockAssetID), so no repo scan
// ambiguity exists; the shared-server CDN router uses this per backend.
func (m *mockGitHub) findAssetLocked(id int64) (*mockAsset, bool) {
	for _, repo := range m.repos {
		if asset := repo.assets[id]; asset != nil {
			return asset, true
		}
	}
	return nil, false
}

func (m *mockGitHub) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Name    string `json:"name"`
		Private bool   `json:"private"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.repos[payload.Name]; exists {
		m.writeAlreadyExists(w, "Repository", "name", payload.Name)
		return
	}
	m.repos[payload.Name] = &mockRepo{
		name:          payload.Name,
		private:       payload.Private,
		nextReleaseID: 1,
		nextBlobID:    1,
		nextCommitID:  1,
		releasesByTag: make(map[string]*mockRelease),
		releasesByID:  make(map[int64]*mockRelease),
		assets:        make(map[int64]*mockAsset),
		assetsByTag:   make(map[string]map[int64]*mockAsset),
		files:         make(map[string]*mockFile),
		dirChildren:   make(map[string]map[string]bool),
		commitsByPath: make(map[string][]mockCommit),
		commitTime:    make(map[string]time.Time),
	}
	m.writeJSON(w, http.StatusCreated, map[string]any{"name": payload.Name})
}

// putMockFileLocked stores file bytes under the real git blob SHA and
// maintains the dirChildren index. Caller holds m.mu. Every repo.files
// write must go through here (or deleteMockFileLocked) so the directory
// index never drifts from the file map.
func (m *mockGitHub) putMockFileLocked(repo *mockRepo, filePath string, data []byte) *mockFile {
	_ = m
	file := repo.files[filePath]
	if file == nil {
		file = &mockFile{path: filePath}
		repo.files[filePath] = file
	}
	file.sha = computeGitBlobSHA(data)
	file.data = append([]byte(nil), data...)
	indexMockDirChild(repo, filePath)
	return file
}

// deleteMockFileLocked removes a file and prunes now-empty ancestor index
// entries. Caller holds m.mu.
func (m *mockGitHub) deleteMockFileLocked(repo *mockRepo, filePath string) {
	delete(repo.files, filePath)
	unindexMockDirChild(repo, filePath)
}

// indexMockDirChild records filePath's leaf and every ancestor directory in
// dirChildren: dirChildren[dir][name] reports whether name is a directory.
// A directory entry always wins over a file entry of the same name.
func indexMockDirChild(repo *mockRepo, filePath string) {
	segments := strings.Split(filePath, "/")
	for i := range segments {
		dir := strings.Join(segments[:i], "/")
		child := segments[i]
		isDir := i < len(segments)-1
		if repo.dirChildren == nil {
			repo.dirChildren = make(map[string]map[string]bool)
		}
		kids := repo.dirChildren[dir]
		if kids == nil {
			kids = make(map[string]bool)
			repo.dirChildren[dir] = kids
		}
		kids[child] = kids[child] || isDir
	}
}

// unindexMockDirChild drops filePath's leaf, then prunes ancestors left
// childless (a directory with no children does not exist on GitHub).
func unindexMockDirChild(repo *mockRepo, filePath string) {
	segments := strings.Split(filePath, "/")
	if len(segments) == 0 {
		return
	}
	dir := strings.Join(segments[:len(segments)-1], "/")
	if kids := repo.dirChildren[dir]; kids != nil {
		delete(kids, segments[len(segments)-1])
	}
	// Walk up: a directory whose index entry is now empty vanishes, which
	// may empty its own parent in turn.
	for d := dir; d != ""; {
		if kids := repo.dirChildren[d]; kids != nil && len(kids) == 0 {
			delete(repo.dirChildren, d)
		} else {
			break
		}
		parent, base := parentDirBase(d)
		if kids := repo.dirChildren[parent]; kids != nil {
			delete(kids, base)
		}
		d = parent
	}
}

func parentDirBase(dir string) (parent, base string) {
	if i := strings.LastIndex(dir, "/"); i >= 0 {
		return dir[:i], dir[i+1:]
	}
	return "", dir
}

// recordMockCommitLocked appends a newest-first commit on filePath's history
// with a 40-hex SHA and maintains the commitTime index. Caller holds m.mu.
func (m *mockGitHub) recordMockCommitLocked(repo *mockRepo, filePath, message string, data []byte, deleted bool) string {
	_ = m
	commitSHA := mockCommitSHA(repo.nextCommitID)
	repo.nextCommitID++
	when := time.Unix(1700000000+repo.nextCommitID, 0).UTC()
	repo.commitsByPath[filePath] = append([]mockCommit{{
		sha: commitSHA, message: message, path: filePath,
		data: append([]byte(nil), data...), when: when, deleted: deleted,
	}}, repo.commitsByPath[filePath]...)
	if repo.commitTime == nil {
		repo.commitTime = make(map[string]time.Time)
	}
	repo.commitTime[commitSHA] = when
	return commitSHA
}

func (m *mockGitHub) handleRepos(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "repos" || parts[1] != m.owner {
		http.NotFound(w, r)
		return
	}
	repo := m.repo(parts[2])
	if repo == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "repo not found"})
		return
	}

	switch {
	case len(parts) == 3 && r.Method == http.MethodGet:
		m.writeJSON(w, http.StatusOK, map[string]any{"name": repo.name})
	case len(parts) >= 5 && parts[3] == "contents":
		m.handleContents(w, r, repo, strings.Join(parts[4:], "/"))
	case len(parts) == 4 && parts[3] == "commits" && r.Method == http.MethodGet:
		m.handleListCommits(w, r, repo)
	case len(parts) == 4 && parts[3] == "releases" && r.Method == http.MethodPost:
		m.handleCreateRelease(w, r, repo)
	case len(parts) == 4 && parts[3] == "releases" && r.Method == http.MethodGet:
		m.handleListReleases(w, r, repo)
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "tags" && r.Method == http.MethodGet:
		m.handleGetReleaseByTag(w, repo, parts[5])
	case len(parts) == 5 && parts[3] == "releases" && r.Method == http.MethodDelete:
		m.handleDeleteRelease(w, repo, parts[4])
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "assets" && r.Method == http.MethodGet:
		m.handleDownloadAsset(w, r, repo, parts[5])
	case len(parts) == 6 && parts[3] == "releases" && parts[5] == "assets" && r.Method == http.MethodGet:
		m.handleListReleaseAssets(w, r, repo, parts[4])
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "assets" && r.Method == http.MethodDelete:
		m.handleDeleteAsset(w, repo, parts[5])
	case len(parts) == 3 && r.Method == http.MethodDelete:
		m.handleDeleteRepo(w, parts[2])
	default:
		http.NotFound(w, r)
	}
}

func (m *mockGitHub) handleContents(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	cleanPath := strings.TrimPrefix(filePath, "/")
	switch r.Method {
	case http.MethodGet:
		m.handleGetContent(w, r, repo, cleanPath)
	case http.MethodPut:
		m.handlePutContent(w, r, repo, cleanPath)
	case http.MethodDelete:
		m.handleDeleteContent(w, r, repo, cleanPath)
	default:
		http.NotFound(w, r)
	}
}

// handleDeleteContent removes a file, verifying the caller's blob sha matches
// (GitHub's optimistic-concurrency precondition for deletes).
func (m *mockGitHub) handleDeleteContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	var payload struct {
		Message string `json:"message"`
		SHA     string `json:"sha"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	// Mirror GitHub: contents DELETE requires both a commit message and
	// the current blob sha; either missing is a 422, never a silent delete.
	if payload.Message == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"message\" wasn't supplied."})
		return
	}
	if payload.SHA == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"sha\" wasn't supplied."})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := repo.files[filePath]
	if current == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	if payload.SHA != current.sha {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "sha does not match"})
		return
	}
	delete(repo.files, filePath)
	unindexMockDirChild(repo, filePath)
	// Mirror GitHub: the delete is itself a commit on the path's history
	// (revision lists after a delete must show it), and the path is gone
	// from that commit onward.
	commitSHA := m.recordMockCommitLocked(repo, filePath, payload.Message, nil, true)
	m.writeJSON(w, http.StatusOK, map[string]any{"commit": map[string]any{"sha": commitSHA}})
}

func (m *mockGitHub) handleGetContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ref := r.URL.Query().Get("ref")

	// Check if client wants raw content
	acceptRaw := r.Header.Get("Accept") == "application/vnd.github.raw"

	if ref != "" && ref != "HEAD" && ref != defaultBranch {
		// Real GitHub resolves ?ref= against the commit graph and 404s a
		// ref it does not know; it never silently serves HEAD bytes for an
		// unknown/stale SHA. "HEAD" and the default branch name
		// are valid refs and resolve to current state below.
		data, known, present := m.contentAtRefLocked(repo, filePath, ref)
		switch {
		case !known:
			m.writeJSON(w, http.StatusNotFound, map[string]any{"message": fmt.Sprintf("No commit found for SHA: %s", ref)})
			return
		case present:
			m.serveFileContentLocked(w, filePath, data, acceptRaw)
			return
		case repo.files[filePath] != nil:
			// A file now (or again) at HEAD that did not exist at ref:
			// GitHub 404s the historical read.
			m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
			return
			// Otherwise the path may be a directory prefix: fall through
			// to the HEAD listing (the mock keeps no per-commit trees).
		}
	}
	file := repo.files[filePath]
	if file == nil {
		// Not an exact file: it may be a directory prefix. GitHub's contents
		// API returns an array of entries for a directory. Prune's object
		// enumeration relies on this, so mirror it - including GitHub's
		// hard 1000-entry ceiling, past which the API 403s "too large"
		// rather than truncating.
		if entries, ok := m.dirEntriesLocked(repo, filePath); ok {
			if len(entries) > contentsListingCap {
				m.writeJSON(w, http.StatusForbidden, map[string]any{"message": "too large"})
				return
			}
			m.writeJSON(w, http.StatusOK, entries)
			return
		}
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	m.serveFileContentLocked(w, filePath, file.data, acceptRaw)
}

// contentAtRefLocked resolves filePath at commit ref the way GitHub does:
// the bytes the path carried in that commit's tree. known=false means ref
// is not a commit the mock knows at all (GitHub: 404 "No commit found for
// SHA" - the ref is resolved across EVERY path's history, since GitHub
// accepts any valid SHA, not just commits that touched this file).
// known=true, present=false means the path did not exist (or was deleted)
// at that commit (GitHub: 404 "Not Found").
func (m *mockGitHub) contentAtRefLocked(repo *mockRepo, filePath, ref string) (data []byte, known, present bool) {
	// O(1) ref resolution via the commitTime index (was: a scan of every
	// path's history per ?ref= GET).
	refWhen, known := repo.commitTime[ref]
	if !known {
		return nil, false, false
	}
	commits := repo.commitsByPath[filePath]
	if len(commits) == 0 {
		// No recorded history for this path: it predates every commit the
		// mock knows (seeded state), so HEAD bytes are its content at any
		// valid ref.
		if f := repo.files[filePath]; f != nil {
			return f.data, true, true
		}
		return nil, true, false
	}
	// commitsByPath is newest-first; the content at ref is the newest
	// entry at or before ref's timestamp.
	for _, c := range commits {
		if !c.when.After(refWhen) {
			if c.deleted {
				return nil, true, false
			}
			return c.data, true, true
		}
	}
	// Every change to this path postdates ref: it did not exist then.
	return nil, true, false
}

// dirEntriesLocked returns the immediate children of a directory prefix
// from the dirChildren index (was: a scan of every file per ListDir).
// ok=false when nothing lives under it. Caller holds m.mu.
func (m *mockGitHub) dirEntriesLocked(repo *mockRepo, dirPath string) ([]map[string]any, bool) {
	// Preserve the historical contract: the root (""/"/") never lists.
	// GitHub has no root-contents listing on this route and prune's
	// enumeration always passes a real directory prefix.
	if strings.Trim(dirPath, "/") == "" {
		return nil, false
	}
	dir := strings.TrimSuffix(dirPath, "/")
	kids, ok := repo.dirChildren[dir]
	if !ok || len(kids) == 0 {
		return nil, false
	}
	names := make([]string, 0, len(kids))
	for name := range kids {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]map[string]any, 0, len(names))
	for _, name := range names {
		if kids[name] {
			out = append(out, map[string]any{"name": name, "path": dir + "/" + name, "type": "dir"})
			continue
		}
		p := dir + "/" + name
		sha := ""
		if f := repo.files[p]; f != nil {
			sha = f.sha
		}
		out = append(out, map[string]any{"name": name, "path": p, "type": "file", "sha": sha})
	}
	return out, true
}

func (m *mockGitHub) handlePutContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	var payload struct {
		Message string `json:"message"`
		Content string `json:"content"`
		SHA     string `json:"sha"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	// Mirror GitHub's request validation: a contents PUT
	// without a commit message 422s, and invalid base64 is a 422 - not
	// the 400/200 the old mock answered with.
	if payload.Message == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"message\" wasn't supplied."})
		return
	}
	data, err := base64.StdEncoding.DecodeString(payload.Content)
	if err != nil {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"content\" is not valid base64-encoded data."})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := repo.files[filePath]
	// Mirror GitHub: a sha-less PUT onto an EXISTING path is not a silent
	// overwrite, it is a create collision - 422 with the "sha wasn't
	// supplied" request-validation body. The old 200 made the
	// production benign-concurrent-create branch unreachable in tests.
	if current != nil && payload.SHA == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"sha\" wasn't supplied."})
		return
	}
	if current != nil && payload.SHA != "" && payload.SHA != current.sha {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "sha does not match"})
		return
	}
	if current == nil && payload.SHA != "" {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "file does not exist"})
		return
	}
	// Compute real git blob SHA instead of fake counter-based SHA
	blobSHA := computeGitBlobSHA(data)
	m.putMockFileLocked(repo, filePath, data)
	commitSHA := m.recordMockCommitLocked(repo, filePath, payload.Message, data, false)
	// Mirror GitHub: creating a new file answers 201, updating 200.
	status := http.StatusOK
	if current == nil {
		status = http.StatusCreated
	}
	m.writeJSON(w, status, map[string]any{
		"content": map[string]any{
			"name": filepath.Base(filePath),
			"path": filePath,
			"sha":  blobSHA,
		},
		"commit": map[string]any{
			"sha": commitSHA,
		},
	})
}

func (m *mockGitHub) handleListCommits(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	filePath := r.URL.Query().Get("path")
	m.mu.Lock()
	defer m.mu.Unlock()
	commits := repo.commitsByPath[filePath]
	m.writePaginationLinks(w, r, len(commits))
	pageItems := paginateSlice(commits, r.URL.Query())
	response := make([]map[string]any, 0, len(pageItems))
	for _, commit := range pageItems {
		response = append(response, map[string]any{
			"sha": commit.sha,
			"commit": map[string]any{
				"message": commit.message,
				"author":  map[string]any{"date": commit.when.Format(time.RFC3339)},
			},
		})
	}
	m.writeJSON(w, http.StatusOK, response)
}

func (m *mockGitHub) handleCreateRelease(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	var payload struct {
		TagName string `json:"tag_name"`
		Name    string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: creating a tag that already exists 422s instead of
	// overwriting. The production double-create race depends on this.
	if existing := repo.releasesByTag[payload.TagName]; existing != nil {
		m.writeAlreadyExists(w, "Release", "tag_name", payload.TagName)
		return
	}
	release := &mockRelease{
		id:        repo.nextReleaseID,
		tag:       payload.TagName,
		name:      payload.Name,
		uploadURL: fmt.Sprintf("%s/upload/%s/%s{?name}", m.server.URL, repo.name, payload.TagName),
	}
	repo.nextReleaseID++
	repo.releasesByTag[release.tag] = release
	repo.releasesByID[release.id] = release
	m.writeJSON(w, http.StatusCreated, map[string]any{
		"id":         release.id,
		"tag_name":   release.tag,
		"name":       release.name,
		"upload_url": release.uploadURL,
		"draft":      false,
	})
}

func (m *mockGitHub) handleGetReleaseByTag(w http.ResponseWriter, repo *mockRepo, tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	release := repo.releasesByTag[tag]
	if release == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	m.writeJSON(w, http.StatusOK, map[string]any{
		"id":         release.id,
		"tag_name":   release.tag,
		"name":       release.name,
		"upload_url": release.uploadURL,
		"draft":      false,
		"assets":     m.embeddedAssetsLocked(repo, release.tag),
	})
}

func (m *mockGitHub) handleListReleases(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	releases := make([]*mockRelease, 0, len(repo.releasesByTag))
	for _, release := range repo.releasesByTag {
		releases = append(releases, release)
	}
	// Mirror GitHub: the releases list is ordered by created_at DESC, not
	// by tag. IDs are minted in creation order, so descending
	// ID is the same sequence; tag-string order ("v10" < "v9") is not.
	sort.Slice(releases, func(i, j int) bool { return releases[i].id > releases[j].id })
	m.writePaginationLinks(w, r, len(releases))
	pageReleases := paginateSlice(releases, r.URL.Query())
	response := make([]map[string]any, 0, len(pageReleases))
	for _, release := range pageReleases {
		assets := m.embeddedAssetsLocked(repo, release.tag)
		response = append(response, map[string]any{
			"id":         release.id,
			"tag_name":   release.tag,
			"name":       release.name,
			"upload_url": release.uploadURL,
			"draft":      false,
			"assets":     assets,
		})
	}
	m.writeJSON(w, http.StatusOK, response)
}

func (m *mockGitHub) releaseAssetsLocked(repo *mockRepo, tag string) []map[string]any {
	// O(tag assets) via the per-tag index (was: a scan of every asset in
	// the repo per call, O(R*A) per releases page when embedded).
	byTag := repo.assetsByTag[tag]
	rows := make([]*mockAsset, 0, len(byTag))
	for _, asset := range byTag {
		rows = append(rows, asset)
	}
	// Deterministic embed order: GitHub lists assets by ascending
	// ID; Go map iteration is random.
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	assets := make([]map[string]any, 0, len(rows))
	for _, asset := range rows {
		assets = append(assets, map[string]any{"id": asset.id, "name": asset.name, "size": len(asset.data)})
	}
	return assets
}

func (m *mockGitHub) handleDeleteRelease(w http.ResponseWriter, repo *mockRepo, rawID string) {
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
	delete(repo.releasesByID, releaseID)
	delete(repo.releasesByTag, release.tag)
	delete(repo.assetsByTag, release.tag)
	for id, asset := range repo.assets {
		if asset.releaseTag == release.tag {
			delete(repo.assets, id)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) handleUpload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	repo := m.repo(parts[1])
	if repo == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "repo not found"})
		return
	}
	name := r.URL.Query().Get("name")
	data, err := io.ReadAll(r.Body)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: uploads target a release that must exist.
	if repo.releasesByTag[parts[2]] == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	// Fault injection: one forced name collision for the sink retry test.
	if m.faults.collideNext[parts[1]+"/"+parts[2]] {
		delete(m.faults.collideNext, parts[1]+"/"+parts[2])
		m.writeAlreadyExists(w, "ReleaseAsset", "name", name)
		return
	}
	// Mirror GitHub: asset names are unique per release; a duplicate 422s.
	// The per-tag index answers both the dup check and the count below in
	// one pass (was: two full asset scans per upload, O(N^2) to fill).
	byTag := repo.assetsByTag[parts[2]]
	for _, asset := range byTag {
		if asset.name == name {
			m.writeAlreadyExists(w, "ReleaseAsset", "name", name)
			return
		}
	}
	// Mirror GitHub: a release holds at most releaseAssetCap assets;
	// further uploads 422 with a file_count body (live shape from
	// storhub-web v18).
	if len(byTag) >= releaseAssetCap {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"message": "Validation Failed",
			"errors": []map[string]any{{
				"resource": "ReleaseAsset",
				"code":     "custom",
				"field":    "file_count",
				"message":  "file_count limited to 1000 assets per release",
			}},
		})
		return
	}
	asset := &mockAsset{id: globalMockAssetID.Add(1), name: name, releaseTag: parts[2], data: append([]byte(nil), data...)}
	repo.assets[asset.id] = asset
	tagAssetLocked(repo, asset)
	m.writeJSON(w, http.StatusCreated, map[string]any{"id": asset.id, "name": asset.name})
}

func (m *mockGitHub) handleDownloadAsset(w http.ResponseWriter, r *http.Request, repo *mockRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	asset, ok := repo.assets[assetID]
	if !ok {
		m.mu.Unlock()
		// Include the ID: under concurrent append storms a 404 without
		// it is undebuggable (which upload's asset went missing?).
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": fmt.Sprintf("asset not found: %d", assetID)})
		return
	}
	name, size := asset.name, len(asset.data)
	m.mu.Unlock()
	// Mirror GitHub: only the octet-stream GET is a redirect to a
	// short-lived signed CDN URL (range fetches then hit the CDN directly
	// without auth headers). Any other Accept answers with the asset's
	// metadata JSON - never the bytes.
	if r.Header.Get("Accept") != "application/octet-stream" {
		m.writeJSON(w, http.StatusOK, map[string]any{
			"id":                   assetID,
			"name":                 name,
			"size":                 size,
			"content_type":         "application/octet-stream",
			"browser_download_url": fmt.Sprintf("%s/cdn/%d", m.server.URL, assetID),
		})
		return
	}
	w.Header().Set("Location", m.mintSignedCDNURL(assetID))
	w.WriteHeader(http.StatusFound)
}

// handleCDN serves signed-URL range fetches: no auth required (the URL is
// the bearer credential), Range honored, Accept-Ranges advertised. When
// the URL carries expiries (cdnTTL armed) an expired fetch 403s like
// GitHub's expired Azure SAS does, forcing the client to re-resolve
// through the API. A one-shot 618 (cdnFail618Once) mirrors the front-door
// JWT dying first, which the client also re-resolves.
func (m *mockGitHub) handleCDN(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "" {
		m.faults.cdnSawAuth.Store(true)
	}
	if m.faults.cdnFail618Once.CompareAndSwap(true, false) {
		m.writeJSON(w, ghapi.StatusSignedURLExpired, map[string]any{"message": "jwt:expired"})
		return
	}
	if exp, ok := mockSignedURLExpiry(r.URL.Query()); ok {
		shift := m.faults.cdnTimeShift.Swap(0)
		if m.mockTime().Add(time.Duration(shift)).After(exp) {
			m.writeJSON(w, http.StatusForbidden, map[string]any{"message": "Signature is not valid on this request"})
			return
		}
	}
	assetID, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/cdn/"), 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	asset, ok := m.findAssetLocked(assetID)
	var data []byte
	if ok {
		data = append([]byte(nil), asset.data...)
	}
	m.mu.Unlock()
	if !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	if delay := m.faults.cdnDelay.Load(); delay > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(delay)):
		}
	}
	start, end, partial, err := resolveByteRange(r.Header.Get("Range"), int64(len(data)))
	if err != nil {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	body := data
	status := http.StatusOK
	if partial {
		body = data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		status = http.StatusPartialContent
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (m *mockGitHub) handleDeleteAsset(w http.ResponseWriter, repo *mockRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	asset, ok := repo.assets[assetID]
	if !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	untagAssetLocked(repo, asset.releaseTag, assetID)
	delete(repo.assets, assetID)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) handleDeleteRepo(w http.ResponseWriter, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: deleting an unknown repo 404s. The client
	// treats 404 on delete as success; the old unconditional 204 hid the
	// lost-response-retry divergence.
	if _, ok := m.repos[name]; !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	delete(m.repos, name)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) repo(name string) *mockRepo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.repos[name]
}

func (m *mockGitHub) addRelease(t *testing.T, project, tag string) *mockRelease {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	release := &mockRelease{id: repo.nextReleaseID, tag: tag, name: "manual " + tag, uploadURL: fmt.Sprintf("%s/upload/%s/%s{?name}", m.server.URL, repo.name, tag)}
	repo.nextReleaseID++
	repo.releasesByTag[tag] = release
	repo.releasesByID[release.id] = release
	return release
}

func (m *mockGitHub) addAssetToRelease(t *testing.T, project, tag, name string, data []byte) int64 {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	asset := &mockAsset{id: globalMockAssetID.Add(1), name: name, releaseTag: tag, data: append([]byte(nil), data...)}
	repo.assets[asset.id] = asset
	tagAssetLocked(repo, asset)
	return asset.id
}

func (m *mockGitHub) addAssetsToRelease(t *testing.T, project, tag string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		m.addAssetToRelease(t, project, tag, fmt.Sprintf("extra-%03d.bin", i), []byte("x"))
	}
}

func (m *mockGitHub) removeAsset(t *testing.T, project string, assetID int64) {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if asset, ok := repo.assets[assetID]; ok {
		untagAssetLocked(repo, asset.releaseTag, assetID)
	}
	delete(repo.assets, assetID)
}

func (m *mockGitHub) setMetadata(t *testing.T, project string, md *RepoMetadata) {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Match the project's current layout: a split (version-5) project stores
	// a manifest plus objects; a legacy project a single blob.
	if repo.files[indexFilePath] != nil {
		res, err := meta.BuildTree(md)
		if err != nil {
			t.Fatalf("build tree: %v", err)
		}
		for sha, data := range res.Objects {
			m.putMockFileLocked(repo, objectRepoPath(sha), data)
		}
		mf := &meta.Manifest{
			Version: meta.CurrentVersion, Project: md.Project, TreeRoot: res.RootSHA,
			ChunkBuckets: res.ChunkBuckets, Releases: res.ReleasesSHA,
			NextInode: md.NextInode, NextChunkID: md.NextChunkID,
			Stats: meta.ManifestStats{Files: md.TotalFiles, Bytes: md.TotalSize}, LastMod: md.LastMod,
		}
		mb, err := meta.MarshalManifest(mf)
		if err != nil {
			t.Fatalf("marshal manifest: %v", err)
		}
		m.putMockFileLocked(repo, indexFilePath, mb)
		return
	}
	payload, err := md.ToJSON()
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	// Mirror GitHub's stored blob sha: the client computes its
	// CAS token locally from raw bytes, so a fake counter sha would make
	// a read-modify-write of seeded legacy state 409 on a precondition
	// that passes against the real API.
	m.putMockFileLocked(repo, metadataFilePath, payload)
}

func mustLoadMetadata(t *testing.T, repo *mockRepo) *RepoMetadata {
	t.Helper()
	// Layout-aware: a split (version-5) project stores its index as a
	// manifest plus content-addressed objects; a legacy project as one blob.
	if idx := repo.files[indexFilePath]; idx != nil {
		manifest, err := meta.ParseManifest(idx.data)
		if err != nil {
			t.Fatalf("parse manifest: %v", err)
		}
		get := func(sha string) ([]byte, error) {
			f := repo.files[objectRepoPath(sha)]
			if f == nil {
				return nil, fmt.Errorf("missing object %s", sha)
			}
			return f.data, nil
		}
		m, err := meta.LoadTree(manifest, get)
		if err != nil {
			t.Fatalf("load split index: %v", err)
		}
		m.Normalize(manifest.Project, m.LastMod)
		return m
	}
	file := repo.files[metadataFilePath]
	if file == nil {
		return &RepoMetadata{}
	}
	legacy := &RepoMetadata{}
	if err := legacy.FromJSON(file.data); err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	return legacy
}

func (m *mockGitHub) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeTempFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

func paginateSlice[T any](items []T, query url.Values) []T {
	if query == nil {
		return items
	}
	perPage, page := parsePaging(query)
	start := (page - 1) * perPage
	if start >= len(items) {
		return nil
	}
	end := start + perPage
	if end > len(items) {
		end = len(items)
	}
	return items[start:end]
}

func resolveByteRange(header string, size int64) (int64, int64, bool, error) {
	if strings.TrimSpace(header) == "" {
		if size == 0 {
			return 0, -1, false, nil
		}
		return 0, size - 1, false, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, fmt.Errorf("unsupported range header: %s", header)
	}
	parts := strings.SplitN(strings.TrimPrefix(header, "bytes="), "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid range header: %s", header)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false, err
	}
	if start < 0 || end < start || end >= size {
		return 0, 0, false, fmt.Errorf("range %d-%d out of bounds for size %d", start, end, size)
	}
	return start, end, true, nil
}

func assertFileContent(t *testing.T, path string, expected []byte) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	if !bytes.Equal(data, expected) {
		t.Fatalf("unexpected file content for %s", path)
	}
}

func mustMetadataRevision(t *testing.T, hub *StorHub, project, message string) MetadataRevision {
	t.Helper()
	revisions, err := hub.ListMetadataRevisionsContext(context.Background(), project)
	if err != nil {
		t.Fatalf("list metadata revisions: %v", err)
	}
	for _, revision := range revisions {
		if revision.Message == message {
			return revision
		}
	}
	t.Fatalf("metadata revision %q not found", message)
	return MetadataRevision{}
}

func mockRaw(t *testing.T, backend *mockGitHub, method, path string, body any) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal raw body: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, backend.server.URL+path, rdr)
	if err != nil {
		t.Fatalf("build raw request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+backend.token)
	resp, err := backend.server.Client().Do(req)
	if err != nil {
		t.Fatalf("raw %s %s: %v", method, path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}
