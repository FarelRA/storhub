package github

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestHardenedRetryAfterCappedByMaxRetryDelay pins that a hostile or
// misconfigured Retry-After (even on a rate-limited rejection) can never
// stall callers past maxRetryDelay.
func TestHardenedRetryAfterCappedByMaxRetryDelay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil)) // MaxRetryDelay 5ms
	apiErr := &APIError{StatusCode: http.StatusTooManyRequests, RateLimited: true, RetryAfter: time.Hour}
	if got := c.retryDelay(0, apiErr); got > c.maxRetryDelay {
		t.Fatalf("rate-limited Retry-After must be capped at %v, got %v", c.maxRetryDelay, got)
	}
	plain := &APIError{StatusCode: http.StatusServiceUnavailable, RetryAfter: 30 * time.Second}
	if got := c.retryDelay(1, plain); got > c.maxRetryDelay {
		t.Fatalf("plain Retry-After must be capped at %v, got %v", c.maxRetryDelay, got)
	}
}

// TestHardenedSecondaryBackoffFloorAndCeiling pins GitHub's secondary-limit
// guidance: at least ~60s of patience with growth, never past the 15m
// ceiling (plus bounded jitter).
func TestHardenedSecondaryBackoffFloorAndCeiling(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))
	secondary := &APIError{StatusCode: http.StatusForbidden, RateLimited: true}
	if got := c.retryDelay(0, secondary); got < 60*time.Second {
		t.Fatalf("secondary backoff must wait at least 60s, got %v", got)
	}
	if got := c.retryDelay(10, secondary); got > 20*time.Minute {
		t.Fatalf("secondary backoff must stay bounded, got %v", got)
	}
}

// TestHardenedRedirectStatusesResolve pins that every redirect status the
// API/CDN may emit carries the signed URL like 302 does.
func TestHardenedRedirectStatusesResolve(t *testing.T) {
	for _, status := range []int{
		http.StatusMovedPermanently,  // 301
		http.StatusFound,             // 302
		http.StatusSeeOther,          // 303
		http.StatusTemporaryRedirect, // 307
		http.StatusPermanentRedirect, // 308
	} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/repos/o/p/releases/assets/7", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "/cdn/7", status)
			})
			mux.HandleFunc("/cdn/7", func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				_, _ = w.Write([]byte("hello"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			c := NewClient("t", retryTaxonomyConfig(server, nil))
			body, _, err := c.DownloadAssetStream(context.Background(), "o", "p", 7, 0, 4)
			if err != nil {
				t.Fatalf("redirect %d must resolve: %v", status, err)
			}
			data, _ := io.ReadAll(body)
			_ = body.Close()
			if string(data) != "hello" {
				t.Fatalf("redirect %d body=%q, want %q", status, data, "hello")
			}
		})
	}
}

type hardenedTimeoutError struct{}

func (hardenedTimeoutError) Error() string   { return "i/o timeout" }
func (hardenedTimeoutError) Timeout() bool   { return true }
func (hardenedTimeoutError) Temporary() bool { return true }

// TestHardenedNetworkErrorsUnified pins one retryable-network-error
// semantic: timeouts, torn reads and reset/aborted connections retry;
// user cancellation never does.
func TestHardenedNetworkErrorsUnified(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"timeout", hardenedTimeoutError{}, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"conn reset", syscall.ECONNRESET, true},
		{"conn aborted", syscall.ECONNABORTED, true},
		{"op-wrapped reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, true},
		{"canceled", context.Canceled, false},
		{"plain", fmt.Errorf("boom"), false},
	}
	for _, tc := range cases {
		if got := isRetryableNetworkError(tc.err); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestHardenedUploadRetryRewindsAndSucceeds pins the region around the
// per-attempt transfer context: the request carries its own attempt
// deadline (derived from the caller's context, canceled explicitly after
// the attempt), the body factory rewinds for every attempt, and a failed
// first attempt is followed by a successful retry.
func TestHardenedUploadRetryRewindsAndSucceeds(t *testing.T) {
	var posts atomic.Int32
	var lastBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if posts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		data, _ := io.ReadAll(r.Body)
		lastBody = string(data)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":222}`))
	}))
	defer server.Close()
	cfg := retryTaxonomyConfig(server, nil)
	cfg.MaxRetries = 2
	c := NewClient("t", cfg)
	id, err := c.UploadAsset(context.Background(), "o", "p", "tag", server.URL+"/upload", "chunk.bin", strings.NewReader("payload-bytes"), int64(len("payload-bytes")))
	if err != nil {
		t.Fatalf("upload should succeed on retry: %v (posts=%d)", err, posts.Load())
	}
	if id != 222 || posts.Load() != 2 {
		t.Fatalf("asset=%d posts=%d, want 222/2", id, posts.Load())
	}
	if lastBody != "payload-bytes" {
		t.Fatalf("retried upload did not rewind the reader: body=%q", lastBody)
	}
}

// TestHardenedTransferDeadlineScalesWithSize pins that the per-attempt
// upload deadline grows with payload size and never drops below the
// general request timeout.
func TestHardenedTransferDeadlineScalesWithSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))
	if got := c.transferDeadline(0); got != defaultRequestTimeout {
		t.Fatalf("deadline(0)=%v, want %v", got, defaultRequestTimeout)
	}
	small, big := c.transferDeadline(1<<20), c.transferDeadline(1<<30)
	if big <= small {
		t.Fatalf("deadline must grow with size: 1MiB=%v 1GiB=%v", small, big)
	}
	if small < defaultRequestTimeout {
		t.Fatalf("deadline must never drop below the request timeout: %v", small)
	}
}
