package github

// Fix pins for the GitHub client: contents writes draw from the
// governor's content-creation window, rate-limited Retry-After is
// honored on the client clock, caller deadlines and DNS NXDOMAIN are
// permanent, DeleteRepo treats GitHub's 404 as success,
// FindAssetIDByName paginates past the truncated embed, and a canceled
// slot wait rolls its committed reservation back.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// A contents-API PUT/DELETE is a content-generating request (GitHub's
// ~80/min secondary window counts metadata commits, not just asset
// uploads). With the content window at 1 and fail-fast maxWait, the
// second contents write must be refused by the governor while a plain GET
// still passes.
func TestContentsWritesDrawFromContentWindow(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			_, _ = w.Write([]byte(`{"content":{"sha":"abc"},"commit":{"sha":"c1"}}`))
			return
		}
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"login":"x"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	cfg := retryTaxonomyConfig(server, nil)
	cfg.MaxRetries = 0
	cfg.RateContentPerMin = 1
	cfg.RateMaxWait = -1
	c := NewClient("t", cfg)
	ctx := context.Background()
	if _, _, err := c.PutFileContent(ctx, "o", "p", "f1", []byte("x"), "sha1", "m"); err != nil {
		t.Fatalf("first contents PUT must pass: %v", err)
	}
	_, _, err := c.PutFileContent(ctx, "o", "p", "f2", []byte("x"), "sha1", "m")
	if err == nil || !strings.Contains(err.Error(), "content creation budget exhausted") {
		t.Fatalf("second contents PUT must hit the content window, got %v", err)
	}
	if _, err := c.GetAuthenticatedUser(ctx); err != nil {
		t.Fatalf("reads must not draw from the content window: %v", err)
	}
}

// Counterpart to the contents-write pin above: asset uploads keep
// drawing from the content window too.
func TestAssetUploadStillDrawsFromContentWindow(t *testing.T) {
	t.Parallel()
	var posts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts++
		_, _ = w.Write([]byte(`{"id":1}`))
	}))
	defer server.Close()
	cfg := retryTaxonomyConfig(server, nil)
	cfg.MaxRetries = 0
	cfg.RateContentPerMin = 1
	cfg.RateMaxWait = -1
	c := NewClient("t", cfg)
	ctx := context.Background()
	if _, err := c.UploadAsset(ctx, server.URL+"/upload", "a.bin", strings.NewReader("x"), 1); err != nil {
		t.Fatalf("first upload: %v", err)
	}
	if _, err := c.UploadAsset(ctx, server.URL+"/upload", "b.bin", strings.NewReader("x"), 1); err == nil {
		t.Fatal("second upload must hit the content window")
	}
}

// The reset wait must run on the client's single (injectable) clock.
// With a frozen fake clock, a reset 5s ahead of the FAKE now means a 5s
// wait; wall-clock time.Until would compute a wildly different number and
// fake-clock tests would diverge from the code they pin.
func TestResetWaitUsesClientClock(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	var mu sync.Mutex
	now := time.Unix(1700000000, 0).UTC()
	cfg := retryTaxonomyConfig(server, nil)
	cfg.Now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	c := NewClient("t", cfg)
	apiErr := &APIError{
		StatusCode:     http.StatusForbidden,
		RateLimited:    true,
		Primary:        true,
		RateLimitReset: now.Add(5 * time.Second),
	}
	if got := c.retryDelay(0, apiErr); got != 5*time.Second {
		t.Fatalf("reset wait must be measured on the client clock: got %v, want 5s", got)
	}
}

// A caller deadline is permanent (never retried), while http.Client's
// own timeout stays retryable, and DNS NXDOMAIN is permanent.
func TestPermanentTransportFailuresNotRetryable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	if isRetryableNetworkError(fmt.Errorf("perform request: %w", ctx.Err())) {
		t.Fatal("caller deadline exceeded must not be retried")
	}
	dns := &net.DNSError{Err: "no such host", Name: "nowhere.invalid", IsNotFound: true}
	if isRetryableNetworkError(&url.Error{Op: "Get", URL: "https://nowhere.invalid/x", Err: dns}) {
		t.Fatal("DNS NXDOMAIN must not be retried")
	}
	if isRetryableNetworkError(&net.OpError{Op: "dial", Err: dns}) {
		t.Fatal("wrapped DNS NXDOMAIN must not be retried")
	}
	// A transient DNS failure (timeout) IS still retryable.
	dnsTimeout := &net.DNSError{Err: "i/o timeout", Name: "slow.invalid"}
	if !isRetryableNetworkError(dnsTimeout) {
		t.Fatal("transient DNS failure must stay retryable")
	}
}

// GitHub 404s a DELETE for an unknown repo; after a lost-response
// retry the repo is already gone, and gone IS success.
func TestDeleteRepoTreatsNotFoundAsSuccess(t *testing.T) {
	t.Parallel()
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	}))
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))
	if err := c.DeleteRepo(context.Background(), "o", "gone"); err != nil {
		t.Fatalf("404 on delete must be treated as success, got %v", err)
	}
	if hits != 1 {
		t.Fatalf("expected one attempt, got %d", hits)
	}
	// Other failures still surface.
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	})
	if err := c.DeleteRepo(context.Background(), "o", "p"); err == nil {
		t.Fatal("5xx on delete must still surface")
	}
}

// FindAssetIDByName must paginate the dedicated list-assets endpoint;
// the array embedded in the release object truncates near 1000, so a scan
// of it silently misses real assets.
func TestFindAssetIDByNamePaginates(t *testing.T) {
	t.Parallel()
	const total = 150
	target := 120
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/p/releases/tags/v1", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":7,"tag_name":"v1","assets":[]}`))
	})
	mux.HandleFunc("/repos/o/p/releases/7/assets", func(w http.ResponseWriter, r *http.Request) {
		start := 0
		switch r.URL.Query().Get("page") {
		case "2":
			start = 100
		case "3":
			start = 200
		}
		n := total - start
		if n > 100 {
			n = 100
		}
		assets := make([]map[string]any, 0, n)
		for i := start; i < start+n; i++ {
			assets = append(assets, map[string]any{"id": int64(1000 + i), "name": fmt.Sprintf("asset-%03d.bin", i), "size": 1})
		}
		_ = json.NewEncoder(w).Encode(assets)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))
	id, err := c.FindAssetIDByName(context.Background(), "o", "p", "v1", fmt.Sprintf("asset-%03d.bin", target))
	if err != nil {
		t.Fatalf("asset past the embedded-array ceiling must be found via pagination: %v", err)
	}
	if id != int64(1000+target) {
		t.Fatalf("wrong asset id %d, want %d", id, 1000+target)
	}
}

// When the concurrency-slot select loses to ctx.Done AFTER reserve()
// committed a zero-wait reservation, the accounting must roll back - a
// request that was never sent must not spend the windows.
func TestAcquireCtxDoneRollsBackReservation(t *testing.T) {
	t.Parallel()
	cfg := storcfg.Default()
	cfg.MaxConcurrentRequests = 1
	g := newRateGovernor(cfg, nil, func(context.Context, time.Duration) error { return nil })
	rel, err := g.acquire(context.Background(), 5, true, false)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer rel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.acquire(ctx, 5, true, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("slot-blocked acquire on canceled ctx must fail canceled, got %v", err)
	}
	if got := g.winPoints; got != 5 {
		t.Fatalf("rolled-back reservation must leave winPoints=5, got %d", got)
	}
	if got := g.winContent; got != 1 {
		t.Fatalf("rolled-back reservation must leave winContent=1, got %d", got)
	}
}
