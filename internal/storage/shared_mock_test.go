package storage

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Shared mock-GitHub HTTP server.
//
// The mock used to bind one httptest.Server per test (~170 servers across
// this package: bind + listen + close each, serially under -p=1). A single
// server is now shared per package run and requests are dispatched to the
// owning per-test backend by bearer token: newMockGitHub mints a unique
// token per test, registers the backend, and unregisters it in t.Cleanup.
// Per-test isolation is preserved (each backend owns its repos map, fault
// switches, and intercept hook); only the socket is shared.
//
// Per-test StorHub instances and seedMeta seeding are unchanged: every test
// still builds its own hub(s) against its own backend.
//
// /cdn/ requests carry no Authorization header (signed URLs are bearer
// credentials already), so they dispatch by asset ID instead: asset IDs are
// minted from a package-global counter, unique across backends like real
// GitHub, and the CDN handler scans registered backends for the owner.

var (
	sharedMockOnce     sync.Once
	sharedMockServer   *httptest.Server
	sharedMockBackends sync.Map // token string -> *mockGitHub
	sharedMockSeq      atomic.Int64
	globalMockAssetID  atomic.Int64
)

// mockServerRef is the per-test handle to the shared server. It mirrors the
// httptest.Server surface the tests use (URL field, Client, Close) so call
// sites are unchanged; Close only unregisters the backend, never the socket.
type mockServerRef struct {
	URL   string
	token string
}

// Client returns a fresh client per call (plain HTTP; no special transport
// needed). Freshness matters: some tests tune CheckRedirect on the returned
// client, which must not leak into other tests sharing the server.
func (s *mockServerRef) Client() *http.Client { return &http.Client{} }

// Close unregisters the owning backend from shared dispatch.
func (s *mockServerRef) Close() { sharedMockBackends.Delete(s.token) }

func sharedMockBaseURL(t *testing.T) string {
	t.Helper()
	sharedMockOnce.Do(func() {
		sharedMockServer = httptest.NewServer(http.HandlerFunc(sharedMockServeHTTP))
	})
	if sharedMockServer == nil {
		t.Fatal("shared mock server failed to start")
	}
	return sharedMockServer.URL
}

// shutdownSharedMockServer releases the package-wide socket. Called from
// TestMain after the run; process exit would close it anyway.
func shutdownSharedMockServer() {
	if sharedMockServer != nil {
		sharedMockServer.Close()
	}
}

func registerMockBackend(token string, backend *mockGitHub) {
	sharedMockBackends.Store(token, backend)
}

// resetMockMutables returns every per-test fault switch and hook to its
// default so a backend never leaks armed faults past its test. Backends are
// per-test already; this is belt-and-braces for the shared socket.
func (m *mockGitHub) resetMockMutables() {
	m.intercept.Store((func(http.ResponseWriter, *http.Request) bool)(nil))
	m.faults.rateLimitOnce.Store(false)
	m.faults.rateLimitHeadersOnce.Store(false)
	m.faults.cdnFail618Once.Store(false)
	m.faults.cdnTTL.Store(0)
	m.faults.cdnTimeShift.Store(0)
	m.faults.cdnDelay.Store(0)
	m.faults.mockNow.Store(0)
	m.mu.Lock()
	m.faults.collideNext = make(map[string]bool)
	m.faults.embedCap = 0
	m.mu.Unlock()
}

func sharedMockServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/cdn/") {
		serveSharedMockCDN(w, r)
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if v, ok := sharedMockBackends.Load(token); ok {
		v.(*mockGitHub).serveHTTP(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{"message": "Requires authentication"})
}

// serveSharedMockCDN routes an unauthenticated signed-URL fetch to the
// backend that owns the asset ID. Unknown IDs 404 like GitHub.
func serveSharedMockCDN(w http.ResponseWriter, r *http.Request) {
	raw := strings.TrimPrefix(r.URL.Path, "/cdn/")
	if i := strings.Index(raw, "/"); i >= 0 {
		raw = raw[:i]
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": "invalid asset id"})
		return
	}
	var owner *mockGitHub
	sharedMockBackends.Range(func(_, v any) bool {
		backend := v.(*mockGitHub)
		backend.mu.Lock()
		_, ok := backend.findAssetLocked(id)
		backend.mu.Unlock()
		if ok {
			owner = backend
			return false
		}
		return true
	})
	if owner == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": "asset not found"})
		return
	}
	// Preserve the intercept contract across the shared socket: per-test
	// hooks observe and may short-circuit CDN traffic exactly as they do
	// API traffic in serveHTTP. Without this, range/download accounting
	// tests go blind to every byte past the first touch (which the client
	// fetches direct-to-CDN after the initial API-path redirect).
	if fn, ok := owner.intercept.Load().(func(http.ResponseWriter, *http.Request) bool); ok && fn != nil && fn(w, r) {
		return
	}
	owner.handleCDN(w, r)
}
