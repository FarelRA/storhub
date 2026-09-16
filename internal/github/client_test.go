package github

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// testClock is the single clock source shared by the client's sleeps and
// the governor: recording a sleep advances time, so wait-until-reset
// loops converge deterministically instead of spinning.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock { return &testClock{t: time.Now()} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func clockedTaxonomyConfig(server *httptest.Server, clock *testClock, sleeps *[]time.Duration) storcfg.Config {
	cfg := retryTaxonomyConfig(server, nil)
	cfg.Now = clock.Now
	cfg.Sleep = func(_ context.Context, d time.Duration) error {
		if sleeps != nil {
			*sleeps = append(*sleeps, d)
		}
		clock.Advance(d)
		return nil
	}
	return cfg
}

func rateLimitResponse(w http.ResponseWriter, resetIn time.Duration, withHeaders bool) {
	if withHeaders {
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(resetIn).Unix()))
	}
	w.WriteHeader(http.StatusForbidden)
	_, _ = w.Write([]byte(`{"message":"API rate limit exceeded for user ID 69133683."}`))
}

func TestDecodeAPIErrorClassification(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		headers     http.Header
		body        string
		rateLimited bool
		primary     bool
		wantRetry   bool
	}{
		{
			name:   "primary from zeroed headers",
			status: http.StatusForbidden,
			// Keys must be canonical MIME form; Header.Get would miss
			// anything else.
			headers: http.Header{
				"X-Ratelimit-Remaining": []string{"0"},
				"X-Ratelimit-Reset":     []string{fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix())},
			},
			rateLimited: true,
			primary:     true,
			wantRetry:   true,
		},
		{
			name:        "upload endpoint marker without any headers",
			status:      http.StatusForbidden,
			body:        `{"message":"API rate limit exceeded for user ID 69133683. If you reach out to GitHub Support..."}`,
			rateLimited: true,
			primary:     false,
			wantRetry:   true,
		},
		{
			name:        "secondary marker without headers",
			status:      http.StatusForbidden,
			body:        `{"message":"You have exceeded a secondary rate limit"}`,
			rateLimited: true,
			primary:     false,
			wantRetry:   true,
		},
		{
			name:        "plain 429",
			status:      http.StatusTooManyRequests,
			body:        `{"message":"slow down"}`,
			rateLimited: true,
			primary:     false,
			wantRetry:   true,
		},
		{
			name:      "plain server error is not rate limiting",
			status:    http.StatusInternalServerError,
			body:      `{"message":"boom"}`,
			wantRetry: true, // transient 5xx stays retryable, just not rate-limited
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: tc.status,
				Header:     tc.headers,
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			got := decodeAPIError(resp)
			if got == nil {
				t.Fatal("expected an APIError")
			}
			if got.RateLimited != tc.rateLimited {
				t.Errorf("rate_limited=%v, want %v", got.RateLimited, tc.rateLimited)
			}
			if got.Primary != tc.primary {
				t.Errorf("primary=%v, want %v", got.Primary, tc.primary)
			}
			if got.IsRetryable() != tc.wantRetry {
				t.Errorf("retryable=%v, want %v", got.IsRetryable(), tc.wantRetry)
			}
		})
	}
}

func TestUploadAssetRetriesAfterRateLimit(t *testing.T) {
	var posts atomic.Int32
	var lastBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch posts.Add(1) {
		case 1:
			rateLimitResponse(w, 2*time.Second, true)
		default:
			data, _ := io.ReadAll(r.Body)
			lastBody = string(data)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":42}`))
		}
	}))
	defer server.Close()
	clock := newTestClock()
	cfg := clockedTaxonomyConfig(server, clock, nil)
	cfg.RateMaxWait = time.Minute
	c := NewClient("t", cfg)

	assetID, err := c.UploadAsset(context.Background(), "o", "p", "tag", server.URL+"/upload", "chunk.bin", strings.NewReader("payload-bytes"), int64(len("payload-bytes")))
	if err != nil {
		t.Fatalf("upload should succeed on retry: %v", err)
	}
	if assetID != 42 {
		t.Fatalf("asset id=%d, want 42", assetID)
	}
	if posts.Load() != 2 {
		t.Fatalf("expected exactly one retry, got %d attempts", posts.Load())
	}
	if lastBody != "payload-bytes" {
		t.Fatalf("retried upload did not rewind the reader: body=%q", lastBody)
	}
}

func TestUploadAssetFailsFastOnDistantReset(t *testing.T) {
	var posts atomic.Int32
	var sleeps []time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		rateLimitResponse(w, 43*time.Minute, true)
	}))
	defer server.Close()
	cfg := retryTaxonomyConfig(server, &sleeps)
	cfg.RateMaxWait = 0
	c := NewClient("t", cfg)

	_, err := c.UploadAsset(context.Background(), "o", "p", "tag", server.URL+"/upload", "chunk.bin", strings.NewReader("x"), 1)
	apiErr := &APIError{}
	if !errors.As(err, &apiErr) || !apiErr.Primary {
		t.Fatalf("expected primary rate-limit error, got %v", err)
	}
	if posts.Load() != 1 {
		t.Fatalf("fail-fast must not re-fire requests, got %d", posts.Load())
	}
	if len(sleeps) != 0 {
		t.Fatalf("fail-fast must not sleep, got sleeps %v", sleeps)
	}
}

func TestUploadAssetRetriesOnHeaderlessRateLimit(t *testing.T) {
	// Regression for the observed uploads.github.com behavior: a 403
	// whose only signal is the message text, no x-ratelimit headers at
	// all. The upload must be classified as rate limited and retried.
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if posts.Add(1) == 1 {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded for user ID 69133683."}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":7}`))
	}))
	defer server.Close()
	cfg := clockedTaxonomyConfig(server, newTestClock(), nil)
	cfg.RateMaxWait = 2 * time.Minute
	c := NewClient("t", cfg)

	assetID, err := c.UploadAsset(context.Background(), "o", "p", "tag", server.URL+"/upload", "chunk.bin", strings.NewReader("x"), 1)
	if err != nil {
		t.Fatalf("header-less rate-limit rejection must be retried: %v", err)
	}
	if assetID != 7 || posts.Load() != 2 {
		t.Fatalf("asset=%d attempts=%d, want 7/2", assetID, posts.Load())
	}
}

func TestUploadAssetBurstSurvivesLowHourlySnapshot(t *testing.T) {
	// Regression for the 2026-09-05 FUSE incident: uploads.github.com sends
	// no X-RateLimit headers, so the governor paces bursts against a stale
	// low core snapshot and throttles them client-side while the real
	// budget is healthy. Asset uploads must not draw from the hourly bucket,
	// so a burst against a low snapshot must fire with zero throttle sleeps.
	// NOTE: the clock-advancing sleep is load-bearing — a recording no-op
	// sleep sends acquire() into an infinite WARN-spinning loop (2026-09-09:
	// 8.7M lines in 17s, OOM-killed the go process twice). Never use a
	// frozen clock with throttle-asserting tests.
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":1}`))
	}))
	defer server.Close()
	var sleeps []time.Duration
	c := NewClient("t", clockedTaxonomyConfig(server, newTestClock(), &sleeps))
	low := http.Header{}
	low.Set("X-RateLimit-Limit", "5000")
	low.Set("X-RateLimit-Remaining", "30")
	low.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(time.Hour).Unix()))
	c.governor.observe(low)
	for i := 0; i < 20; i++ {
		if _, err := c.UploadAsset(context.Background(), "o", "p", "tag", server.URL+"/upload", fmt.Sprintf("chunk-%d.bin", i), strings.NewReader("x"), 1); err != nil {
			t.Fatalf("upload %d failed while server healthy (posts=%d): %v", i, posts.Load(), err)
		}
	}
	if posts.Load() != 20 {
		t.Fatalf("posts=%d, want 20", posts.Load())
	}
	if len(sleeps) != 0 {
		t.Fatalf("burst throttled %d times against a stale hourly snapshot (first wait %v); asset uploads must skip hourly pacing", len(sleeps), sleeps[0])
	}
}

func TestDownloadAssetStreamCachesSignedURL(t *testing.T) {
	var apiHits, cdnHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/p/releases/assets/7", func(w http.ResponseWriter, r *http.Request) {
		apiHits.Add(1)
		http.Redirect(w, r, fmt.Sprintf("/cdn/7?sp=r&se=%s&sig=testsig", time.Now().Add(5*time.Minute).UTC().Format(time.RFC3339)), http.StatusFound)
	})
	mux.HandleFunc("/cdn/7", func(w http.ResponseWriter, r *http.Request) {
		cdnHits.Add(1)
		if r.Header.Get("Authorization") != "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var start int64
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start); err != nil || start < 0 || start >= 11 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		end := start + 4
		if end > 10 {
			end = 10
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/11", start, end))
		_, _ = w.Write([]byte("hello-world"[start : end+1]))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))

	for i, tc := range []struct {
		start, end int64
		want       string
	}{{0, 4, "hello"}, {5, 9, "-worl"}, {6, 10, "world"}} {
		body, _, err := c.DownloadAssetStream(context.Background(), "o", "p", 7, tc.start, tc.end)
		if err != nil {
			t.Fatalf("download %d: %v", i, err)
		}
		data, _ := io.ReadAll(body)
		_ = body.Close()
		if string(data) != tc.want {
			t.Fatalf("download %d returned %q, want %q", i, data, tc.want)
		}
	}
	if apiHits.Load() != 1 {
		t.Fatalf("signed URL must be reused: api hits=%d", apiHits.Load())
	}
	if cdnHits.Load() != 3 {
		t.Fatalf("range fetches must go to the cdn: hits=%d", cdnHits.Load())
	}
}

func TestDownloadAssetStreamReresolvesRejectedURL(t *testing.T) {
	var apiHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/p/releases/assets/7", func(w http.ResponseWriter, r *http.Request) {
		if apiHits.Add(1) == 1 {
			http.Redirect(w, r, "/expired/7", http.StatusFound)
			return
		}
		http.Redirect(w, r, "/fresh/7", http.StatusFound)
	})
	mux.HandleFunc("/expired/7", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	mux.HandleFunc("/fresh/7", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("fresh"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))

	body, _, err := c.DownloadAssetStream(context.Background(), "o", "p", 7, 0, 4)
	if err != nil {
		t.Fatalf("download after cdn rejection: %v", err)
	}
	data, _ := io.ReadAll(body)
	_ = body.Close()
	if string(data) != "fresh" {
		t.Fatalf("body=%q", data)
	}
	if apiHits.Load() != 2 {
		t.Fatalf("rejected url must force exactly one re-resolve, api hits=%d", apiHits.Load())
	}
}

// TestDownloadAssetStreamReresolvesOn618 proves the reactive recovery for
// GitHub's non-standard 618 "jwt:expired": a cached signed URL whose front-door
// token has lapsed must be dropped and re-resolved through the API, then the
// range served from the fresh URL - not retried against the dead one.
func TestDownloadAssetStreamReresolvesOn618(t *testing.T) {
	var apiHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/p/releases/assets/7", func(w http.ResponseWriter, r *http.Request) {
		if apiHits.Add(1) == 1 {
			http.Redirect(w, r, "/jwt-expired/7", http.StatusFound)
			return
		}
		http.Redirect(w, r, "/fresh/7", http.StatusFound)
	})
	mux.HandleFunc("/jwt-expired/7", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(StatusSignedURLExpired)
	})
	mux.HandleFunc("/fresh/7", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("fresh"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))

	body, _, err := c.DownloadAssetStream(context.Background(), "o", "p", 7, 0, 4)
	if err != nil {
		t.Fatalf("download after 618: %v", err)
	}
	data, _ := io.ReadAll(body)
	_ = body.Close()
	if string(data) != "fresh" {
		t.Fatalf("body=%q, want fresh", data)
	}
	if apiHits.Load() != 2 {
		t.Fatalf("618 must force exactly one re-resolve, api hits=%d", apiHits.Load())
	}
}

func TestDownloadAssetStreamDirect200Legacy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=0-4" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("12345"))
	}))
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))
	body, _, err := c.DownloadAssetStream(context.Background(), "o", "p", 7, 0, 4)
	if err != nil {
		t.Fatalf("legacy direct download: %v", err)
	}
	data, _ := io.ReadAll(body)
	_ = body.Close()
	if string(data) != "12345" {
		t.Fatalf("body=%q", data)
	}
}

func TestPrimaryRateLimitWaitHonorsResetOnce(t *testing.T) {
	var gets atomic.Int32
	var sleeps []time.Duration
	resetAt := time.Now().Add(5 * time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gets.Add(1) == 1 {
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", resetAt.Unix()))
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded"}`))
			return
		}
		_, _ = w.Write([]byte(`{"login":"ok"}`))
	}))
	defer server.Close()
	clock := newTestClock()
	cfg := clockedTaxonomyConfig(server, clock, &sleeps)
	cfg.RateMaxWait = 15 * time.Minute
	c := NewClient("t", cfg)

	var user struct {
		Login string `json:"login"`
	}
	if err := c.getJSON(context.Background(), c.apiURL("/user"), &user); err != nil {
		t.Fatalf("wait-until-reset then success expected: %v", err)
	}
	if gets.Load() != 2 {
		t.Fatalf("expected exactly one wait-and-retry, attempts=%d", gets.Load())
	}
	if len(sleeps) == 0 || sleeps[0] <= time.Second {
		t.Fatalf("the wait must honor the reset window, got %v", sleeps)
	}
	if user.Login != "ok" {
		t.Fatalf("unexpected login %q", user.Login)
	}
}

// TestStoreAssetURLParsesAzureSASExpiry pins expiry extraction for
// GitHub's current release-asset redirect format: an Azure blob SAS URL
// carrying the lifetime in the RFC3339 'se' parameter. Before this was
// parsed, every URL fell back to a fixed 60s residency and got
// re-resolved through the API roughly sixty times more often than its
// real ~1h validity required.
func TestStoreAssetURLParsesAzureSASExpiry(t *testing.T) {
	c := NewClient("t", storcfg.Default())
	// Relative to the test run: a hardcoded past date would make
	// cachedAssetURL rightly refuse the entry and turn this into a
	// time bomb on any machine whose clock has moved past it.
	expiry := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	c.storeAssetURL(9, fmt.Sprintf("https://release-assets.githubusercontent.com/github-production-release-asset/42/abc?sp=r&sv=2018-11-09&sr=b&se=%s&sig=secret&jwt=token", url.QueryEscape(expiry.Format(time.RFC3339))))
	got, ok := c.cachedAssetURL(9)
	if !ok {
		t.Fatal("stored URL must still be cached")
	}
	if want := expiry.Add(-30 * time.Second); !got.expires.Equal(want) {
		t.Fatalf("expiry %v, want %v (real SAS lifetime minus margin)", got.expires, want)
	}
}

// TestStoreAssetURLFallbackKeepsUnknownSchemesCovered pins the safe
// default: a signed URL whose scheme we cannot read gets a conservative
// short residency; rejection-triggered re-resolution covers any guess
// that runs long.
func TestStoreAssetURLFallbackKeepsUnknownSchemesCovered(t *testing.T) {
	c := NewClient("t", storcfg.Default())
	c.storeAssetURL(11, "https://cdn.example.com/x?sig=opaque&nonsense=1")
	_, ok := c.cachedAssetURL(11)
	if !ok {
		t.Fatal("unknown-scheme URL must stay cached under the fallback")
	}
}

// testJWT builds an unsigned compact JWS whose payload carries exp, so the
// client's expiry parser sees a realistic front-door token. Only the payload
// matters; the signature is a placeholder (never verified).
func testJWT(t *testing.T, exp time.Time) string {
	t.Helper()
	enc := func(obj any) string {
		b, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]string{"alg": "none", "typ": "JWT"}) + "." +
		enc(map[string]int64{"exp": exp.Unix()}) + ".sig"
}

// testSignedAssetURL assembles a release-assets URL carrying whichever of the
// two real expiries (front-door JWT 'jwt', backing SAS 'se') the caller sets.
func testSignedAssetURL(t *testing.T, jwtExp, sasExp time.Time) string {
	t.Helper()
	q := url.Values{}
	if !jwtExp.IsZero() {
		q.Set("jwt", testJWT(t, jwtExp))
	}
	if !sasExp.IsZero() {
		q.Set("se", sasExp.UTC().Format(time.RFC3339))
	}
	return "https://release-assets.githubusercontent.com/github-production-release-asset/42/abc?" + q.Encode()
}

// TestStoreAssetURLHonorsEarliestExpiry is the regression for the 618 storm:
// a release-assets URL carries BOTH a ~30min front-door JWT and a longer
// backing SAS 'se'. The JWT governs usability (exceeding it yields 618), so
// the cache must lapse at the EARLIER expiry, not the SAS. Trusting 'se'
// alone kept a JWT-dead URL cached for ~10 minutes, guaranteeing 618s.
func TestStoreAssetURLHonorsEarliestExpiry(t *testing.T) {
	c := NewClient("t", storcfg.Default())
	jwtExp := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Second)
	sasExp := jwtExp.Add(10 * time.Minute) // SAS outlives the JWT by 10 min
	c.storeAssetURL(21, testSignedAssetURL(t, jwtExp, sasExp))
	got, ok := c.cachedAssetURL(21)
	if !ok {
		t.Fatal("URL must stay cached")
	}
	if want := jwtExp.Add(-30 * time.Second); !got.expires.Equal(want) {
		t.Fatalf("expiry %v, want JWT %v minus margin (must NOT trust the later SAS %v)", got.expires, jwtExp, sasExp)
	}
}

// TestStoreAssetURLHonorsSASWhenEarlier pins the min() symmetry: when the
// backing SAS expires before the JWT, the SAS is the binding constraint.
func TestStoreAssetURLHonorsSASWhenEarlier(t *testing.T) {
	c := NewClient("t", storcfg.Default())
	sasExp := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Second)
	jwtExp := sasExp.Add(10 * time.Minute)
	c.storeAssetURL(22, testSignedAssetURL(t, jwtExp, sasExp))
	got, ok := c.cachedAssetURL(22)
	if !ok {
		t.Fatal("URL must stay cached")
	}
	if want := sasExp.Add(-30 * time.Second); !got.expires.Equal(want) {
		t.Fatalf("expiry %v, want SAS %v minus margin", got.expires, sasExp)
	}
}

// TestStoreAssetURLMalformedJWTFallsBackToSAS proves a non-decodable token
// never poisons the cache: the SAS expiry is still honored, no panic.
func TestStoreAssetURLMalformedJWTFallsBackToSAS(t *testing.T) {
	c := NewClient("t", storcfg.Default())
	sasExp := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Second)
	raw := "https://release-assets.githubusercontent.com/x?jwt=aaa.bbb.ccc&se=" +
		url.QueryEscape(sasExp.Format(time.RFC3339))
	c.storeAssetURL(23, raw)
	got, ok := c.cachedAssetURL(23)
	if !ok {
		t.Fatal("URL must stay cached")
	}
	if want := sasExp.Add(-30 * time.Second); !got.expires.Equal(want) {
		t.Fatalf("expiry %v, want SAS %v minus margin (malformed JWT must be ignored)", got.expires, sasExp)
	}
}

// TestStoreAssetURLFallbackIsThirtyMinutes pins the no-expiry default: assume
// GitHub's ~30min front-door token and re-resolve 30s before it lapses.
func TestStoreAssetURLFallbackIsThirtyMinutes(t *testing.T) {
	c := NewClient("t", storcfg.Default())
	c.storeAssetURL(24, "https://cdn.example.com/x?sig=opaque")
	got, ok := c.cachedAssetURL(24)
	if !ok {
		t.Fatal("URL must stay cached")
	}
	want := time.Now().Add(30*time.Minute - 30*time.Second)
	if d := got.expires.Sub(want); d > 5*time.Second || d < -5*time.Second {
		t.Fatalf("fallback expiry %v, want ~%v (30min - 30s)", got.expires, want)
	}
}

// TestIsCDNRejectionIncludesSignedURLExpiry pins that GitHub's non-standard
// 618 (jwt:expired) routes to re-resolution like the other dead-URL statuses.
func TestIsCDNRejectionIncludesSignedURLExpiry(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusBadRequest, http.StatusGone, StatusSignedURLExpired} {
		if !isCDNRejection(status) {
			t.Errorf("status %d must trigger re-resolution", status)
		}
	}
	for _, status := range []int{http.StatusInternalServerError, http.StatusTooManyRequests, 600} {
		if isCDNRejection(status) {
			t.Errorf("status %d must NOT be treated as a dead signed URL", status)
		}
	}
}

// TestDownloadAssetStreamBodySurvivesReturn pins the streaming contract:
// the returned body stays fully readable long after DownloadAssetStream
// has returned. The CDN fetch bounds its request with a context deadline,
// and that deadline's cancel must live as long as the body - a deferred
// cancel amputated every large transfer mid-read ("context canceled"
// after whatever bytes had already buffered), while tiny buffered test
// bodies kept CI green. Large + slow is what makes this deterministic.
func TestDownloadAssetStreamBodySurvivesReturn(t *testing.T) {
	payload := make([]byte, 512<<10)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/p/releases/assets/7", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, fmt.Sprintf("/cdn/7?sp=r&se=%s&sig=testsig", time.Now().Add(5*time.Minute).UTC().Format(time.RFC3339)), http.StatusFound)
	})
	mux.HandleFunc("/cdn/7", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		flusher := w.(http.Flusher)
		for written := 0; written < len(payload); {
			n, err := w.Write(payload[written : written+8<<10])
			if err != nil {
				return
			}
			written += n
			flusher.Flush()
			time.Sleep(2 * time.Millisecond)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	cfg := retryTaxonomyConfig(server, nil)
	cfg.TransferThroughput = 1 << 20
	c := NewClient("t", cfg)

	body, size, err := c.DownloadAssetStream(context.Background(), "o", "p", 7, 0, int64(len(payload)-1))
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil {
		t.Fatalf("reading the returned body failed; the fetch deadline must outlive the call: %v (got %d/%d bytes)", readErr, len(got), len(payload))
	}
	if closeErr != nil {
		t.Fatalf("close body: %v", closeErr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes intact, want %d", len(got), len(payload))
	}
	if size != int64(len(payload)) {
		t.Fatalf("content length %d, want %d", size, len(payload))
	}
}
