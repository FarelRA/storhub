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
	t.Run("idempotent GET retries then succeeds", func(t *testing.T) {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	var unexpected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
