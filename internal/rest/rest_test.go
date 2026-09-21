package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func mustJSONRequest(t *testing.T, handler http.Handler, method, target string, payload any, wantStatus int) *http.Response {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return mustRequest(t, handler, method, target, bytes.NewReader(body), map[string]string{"Content-Type": "application/json"}, wantStatus)
}

func mustRequest(t *testing.T, handler http.Handler, method, target string, body io.Reader, headers map[string]string, wantStatus int) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	if resp.StatusCode != wantStatus {
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("unexpected status for %s %s: got %d want %d body=%s", method, target, resp.StatusCode, wantStatus, strings.TrimSpace(string(data)))
	}
	return resp
}

func decodeJSONBody(t *testing.T, resp *http.Response, dst any) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		t.Fatalf("decode body: %v", err)
	}
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return data
}

func assertErrorCode(t *testing.T, resp *http.Response, want string) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var payload restError
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if payload.Error.Code != want {
		t.Fatalf("unexpected error code: got %q want %q", payload.Error.Code, want)
	}
}

type fakeRESTClient struct {
	mu                    sync.Mutex
	nextInode             uint64
	projects              map[string]*fakeRESTProject
	deleted               map[string]bool
	now                   int64
	rollbacks             []string
	revertPaths           []string
	readCalls             []readCall
	drainCalls            []string
	drainErr              error
	seenIdentities        []shfs.Identity
	failReplaceFromReader error
	revision              string
	optErr                error
	// Session emulation (rest_sessions_test.go): live handles plus policy
	// knobs. Zero knobs mean the storage defaults.
	sessions       map[string]*fakeSession
	nextSession    int
	sessionTTL     time.Duration
	maxSessProject int
	maxSessUser    int
}

// recordIdentityLocked captures the caller identity the storage layer would
// see for this request (tests assert share redemption runs as nobody).
// Callers must hold c.mu.
func (c *fakeRESTClient) recordIdentityLocked(ctx context.Context) {
	c.seenIdentities = append(c.seenIdentities, shfs.IdentityFromContext(ctx))
}

func (c *fakeRESTClient) takeSeenIdentities() []shfs.Identity {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := append([]shfs.Identity(nil), c.seenIdentities...)
	c.seenIdentities = nil
	return ids
}

// SetRevision seeds the fake's metadata revision (used by precondition
// tests). Mutations carrying a stale WithExpectedRevision fail with
// fs.ErrPreconditionFailed.
func (c *fakeRESTClient) SetRevision(rev string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.revision = rev
	c.optErr = nil
}

// RevisionContext implements the Client contract.
func (c *fakeRESTClient) RevisionContext(context.Context, string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.revision == "" {
		return "rev-0", nil
	}
	return c.revision, nil
}

func (c *fakeRESTClient) consumeOptErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.optErr
	c.optErr = nil
	return err
}

func (c *fakeRESTClient) failNextReplace(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failReplaceFromReader = err
}

// setDrainErr seeds a persistent drain failure (naming the project, like
// the real DrainProjectContext) so tests can assert the 500 mapping.
func (c *fakeRESTClient) setDrainErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drainErr = err
}

// DrainProjectContext implements the Client contract: it records the call
// so sync tests can assert draining happened (or did not).
func (c *fakeRESTClient) DrainProjectContext(_ context.Context, project string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drainCalls = append(c.drainCalls, project)
	return c.drainErr
}

type readCall struct {
	path   string
	offset int64
	length int64
}

type fakeRESTProject struct {
	dirs      map[string]*fakeRESTNode
	files     map[string]*fakeRESTNode
	revisions []MetadataRevision
}

type fakeRESTNode struct {
	entry *EntryInfo
	xattr map[string][]byte
	data  *fakeRESTData
}

type fakeRESTData struct {
	bytes  []byte
	nlink  uint32
	kind   NodeKind
	target string
}

// recordOpts captures mutate options so tests can assert precondition
// threading; when an expected revision is declared it is ENFORCED against
// the fake's current revision, mirroring storage behavior.
func (c *fakeRESTClient) recordOpts(opts []shfs.MutateOption) {
	cfg := shfs.ApplyMutateOptions(opts)
	c.mu.Lock()
	defer c.mu.Unlock()
	if cfg.ExpectedRevision() != "" && c.revision != "" && cfg.ExpectedRevision() != c.revision {
		c.optErr = shfs.ErrPreconditionFailed
	}
}

func newFakeRESTClient() *fakeRESTClient {
	return &fakeRESTClient{
		nextInode: 2,
		projects:  make(map[string]*fakeRESTProject),
		deleted:   make(map[string]bool),
		// Fake clock in Unix nanoseconds: every entry stamp minted from
		// tick() is ns, matching the storage contract.
		now: 1700000000000000000,
	}
}

// --- content mutations (ops/mkdir, content PUT/PATCH/DELETE) ---
