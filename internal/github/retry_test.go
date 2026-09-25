package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

func retryTaxonomyConfig(server *httptest.Server, sleeps *[]time.Duration) storcfg.Config {
	return storcfg.Config{
		APIBaseURL:     server.URL,
		HTTPClient:     server.Client(),
		MaxRetries:     2,
		BaseRetryDelay: time.Millisecond,
		MaxRetryDelay:  5 * time.Millisecond,
		Sleep: func(_ context.Context, d time.Duration) error {
			if sleeps != nil {
				*sleeps = append(*sleeps, d)
			}
			return nil
		},
		Logger: nil,
	}
}

func TestRetryTaxonomy(t *testing.T) {
	t.Parallel()
	t.Run("idempotent GET retries then succeeds", func(t *testing.T) {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if hits.Add(1) <= 2 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"message":"boom"}`))
				return
			}
			_, _ = w.Write([]byte(`{"login":"tester"}`))
		}))
		defer server.Close()
		c := NewClient("t", retryTaxonomyConfig(server, nil))
		var user struct {
			Login string `json:"login"`
		}
		if err := c.getJSON(context.Background(), c.apiURL("/user"), &user); err != nil {
			t.Fatalf("expected retry success, got %v", err)
		}
		if hits.Load() != 3 {
			t.Fatalf("expected 3 attempts, got %d", hits.Load())
		}
	})

	t.Run("POST is never retried", func(t *testing.T) {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				hits.Add(1)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()
		c := NewClient("t", retryTaxonomyConfig(server, nil))
		if err := c.CreateRepo(context.Background(), "demo", "d", true, true); err == nil {
			t.Fatal("expected POST failure to surface")
		}
		if hits.Load() != 1 {
			t.Fatalf("POST must not be retried, got %d attempts", hits.Load())
		}
	})

	t.Run("huge Retry-After is clamped", func(t *testing.T) {
		var hits atomic.Int32
		var sleeps []time.Duration
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if hits.Add(1) == 1 {
				w.Header().Set("Retry-After", "3600")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()
		c := NewClient("t", retryTaxonomyConfig(server, &sleeps))
		_, _, _ = c.GetFileContent(context.Background(), "o", "p", "f", "")
		if len(sleeps) == 0 {
			t.Fatal("expected a retry sleep")
		}
		if sleeps[0] > 5*time.Millisecond {
			t.Fatalf("Retry-After must be clamped to MaxRetryDelay, got %v", sleeps[0])
		}
	})

	t.Run("canceled context is not retried", func(t *testing.T) {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()
		c := NewClient("t", retryTaxonomyConfig(server, nil))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := c.RepoExists(ctx, "o", "p"); err == nil {
			t.Fatal("expected canceled request to fail")
		}
		if hits.Load() != 0 {
			t.Fatalf("canceled request must not reach the server, got %d hits", hits.Load())
		}
	})
}

func TestDownloadAssetStreamRejectsInvalidRange(t *testing.T) {
	t.Parallel()
	var unexpected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		unexpected.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))
	if _, _, err := c.DownloadAssetStream(context.Background(), "o", "p", 1, 5, -1); err == nil {
		t.Fatal("expected invalid range error")
	}
	if _, _, err := c.DownloadAssetStream(context.Background(), "o", "p", 1, -1, -1); err == nil {
		t.Fatal("expected invalid range error for inverted range")
	}
	if n := unexpected.Load(); n != 0 {
		t.Fatalf("no request should be made for an invalid range, got %d", n)
	}
}

// TestSharedRetryDelayCoversEveryWaitBranch pins the exported retry-delay
// math other layers adopt: server-dictated waits honored exactly against
// the injected clock, client-computed waits bounded and jittered.
func TestSharedRetryDelayCoversEveryWaitBranch(t *testing.T) {
	t.Parallel()
	now := time.Now()
	base := 10 * time.Millisecond
	limit := 160 * time.Millisecond

	reset := &APIError{StatusCode: 429, RateLimited: true, RateLimitReset: now.Add(90 * time.Second)}
	if got := RetryDelay(0, reset, base, limit, now); got != 90*time.Second {
		t.Fatalf("reset wait must be honored exactly, got %v", got)
	}

	hint := &APIError{StatusCode: 429, RateLimited: true, RetryAfter: 5 * time.Second}
	if got := RetryDelay(0, hint, base, limit, now); got != 5*time.Second {
		t.Fatalf("rate-limited hint must be honored exactly, got %v", got)
	}

	bare := &APIError{StatusCode: 429, RateLimited: true}
	if got := RetryDelay(0, bare, base, limit, now); got < time.Minute || got > 75*time.Second {
		t.Fatalf("bare secondary wait must sit in [60s,75s], got %v", got)
	}

	plain := &APIError{StatusCode: 503, RetryAfter: time.Hour}
	if got := RetryDelay(0, plain, base, limit, now); got != limit {
		t.Fatalf("plain hint must be capped at max, got %v", got)
	}

	if got := RetryDelay(3, nil, base, limit, now); got < 80*time.Millisecond || got > 200*time.Millisecond {
		t.Fatalf("plain backoff attempt 3 must sit in [80ms,200ms], got %v", got)
	}

	if got := BoundedWait(time.Hour, limit); got != limit {
		t.Fatalf("BoundedWait must cap at max, got %v", got)
	}
	if got := BoundedWait(-time.Second, limit); got != 0 {
		t.Fatalf("BoundedWait must floor negative waits, got %v", got)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	c := NewClient("token", retryTaxonomyConfig(server, nil))
	for attempt := 0; attempt < 4; attempt++ {
		// Jitter draws differ per call, so method and export must agree
		// on bounds, not bits: both compute the same secondary backoff.
		floor := 60 * time.Second << attempt
		ceiling := floor + floor/4
		for _, got := range []time.Duration{
			c.retryDelay(attempt, bare),
			RetryDelay(attempt, bare, c.baseRetryDelay, c.maxRetryDelay, c.now()),
		} {
			if got < floor || got > ceiling {
				t.Fatalf("secondary backoff attempt %d must sit in [%v,%v], got %v", attempt, floor, ceiling, got)
			}
		}
	}
}
