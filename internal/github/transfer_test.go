package github

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

func TestTransferDeadlineScalesWithSize(t *testing.T) {
	c := NewClient("t", storcfg.Config{TransferThroughput: 1 << 20}) // 1 MiB/s
	cases := []struct {
		name string
		size int64
		want time.Duration
	}{
		{"empty payload takes the floor", 0, defaultRequestTimeout},
		{"small payload takes the floor", 1024, defaultRequestTimeout},
		{"exactly the floor boundary", int64(5<<20) - 2_000_000, defaultRequestTimeout}, // ~3s scaled < floor
		{"one gigabyte at 1MiB/s", 1 << 30, time.Duration(1<<30/(1<<20))*time.Second + 2*time.Second},
		{"the observed capped-link chunk", 1_716_214_866, ((1716214866 / (1 << 20)) * time.Second) + 2*time.Second},
	}
	for _, tc := range cases {
		if got := c.transferDeadline(tc.size); got != tc.want {
			t.Errorf("%s: transferDeadline(%d)=%v, want %v", tc.name, tc.size, got, tc.want)
		}
	}
}

func TestTransferDeadlineHonorsConfiguredThroughput(t *testing.T) {
	fast := NewClient("t", storcfg.Config{TransferThroughput: 8 << 20}) // 8 MiB/s
	slow := NewClient("t", storcfg.Config{TransferThroughput: 1 << 20})
	big := int64(4 << 30)
	if fast.transferDeadline(big) >= slow.transferDeadline(big) {
		t.Fatalf("higher assumed throughput must shorten the deadline: %v vs %v",
			fast.transferDeadline(big), slow.transferDeadline(big))
	}
	unconfigured := NewClient("t", storcfg.Config{})
	if unconfigured.transferDeadline(big) != slow.transferDeadline(big) {
		t.Fatal("zero throughput must take the library default")
	}
}

func TestUploadCallerDeadlineSurfacesWithoutSpin(t *testing.T) {
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		time.Sleep(500 * time.Millisecond)
	}))
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.UploadAsset(ctx, "o", "p", "tag", server.URL+"/upload", "chunk.bin", strings.NewReader("x"), 1)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expired deadline must surface")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("dead context must stop the retry loop promptly, took %v", elapsed)
	}
	if posts.Load() != 1 {
		t.Fatalf("expired ctx must block further sends, posts=%d", posts.Load())
	}
}

func TestCanceledContextIsNeverRetriedAsTimeout(t *testing.T) {
	canceled := context.Canceled
	if isRetryableNetworkError(canceled) {
		t.Fatal("cancellation must not be retryable")
	}
	wrapped := &net.OpError{Op: "read", Err: canceled}
	if isRetryableNetworkError(wrapped) && !wrapped.Timeout() {
		t.Log("non-timeout op errors fall through to EOF check")
	}
	// A genuine client timeout IS retryable.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()
	client := &http.Client{Timeout: 20 * time.Millisecond}
	_, err := client.Get(srv.URL)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !isRetryableNetworkError(err) {
		t.Fatalf("client timeout must be retryable: %v", err)
	}
}
