package github

// Regression tests for the github-client leftover findings B1, B7, B8,
// B9, B10. Each test fails against the pre-fix code (RED) and passes
// after the fix (GREEN).

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// B1: body-text rate-limit markers must only classify on the statuses
// GitHub actually uses for rate/abuse rejections (403/429). A 404, 401,
// 422 or 5xx whose body prose happens to mention "secondary rate limit"
// must not be misclassified as rate-limited (wrong retry path).
func TestDecodeRateMarkersRequireAbuseStatuses(t *testing.T) {
	markerBody := `{"message":"You have exceeded a secondary rate limit, please slow down"}`
	for _, status := range []int{
		http.StatusNotFound,
		http.StatusUnauthorized,
		http.StatusUnprocessableEntity,
		http.StatusInternalServerError,
		http.StatusBadGateway,
	} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			resp := &http.Response{
				StatusCode: status,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(markerBody)),
			}
			got := decodeAPIError(resp)
			if got == nil {
				t.Fatal("expected an APIError")
			}
			if got.RateLimited {
				t.Fatalf("status %d with marker prose must not be rate-limited", status)
			}
		})
	}
}

// B1: GitHub 422 bodies carry structured errors[].code entries. Matching
// must use status + structured code, never bare body substrings such as
// "1000" or "too many" (false positives on message variants) and never
// on non-422 statuses.
func TestDecodeParsesStructuredValidationCodes(t *testing.T) {
	body := `{"message":"Validation Failed","errors":[` +
		`{"resource":"Release","code":"already_exists","field":"tag_name","message":"tag exists"},` +
		`{"resource":"ReleaseAsset","code":"custom","field":"file_count","message":"file_count limited to 1000 assets per release"}]}`

	newResp := func(status int, b string) *http.Response {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(b))}
	}

	got := decodeAPIError(newResp(http.StatusUnprocessableEntity, body))
	if got == nil {
		t.Fatal("expected an APIError")
	}
	if len(got.Details) != 2 {
		t.Fatalf("structured errors not parsed: %+v", got.Details)
	}
	for _, tc := range []struct {
		code, field string
		want        bool
	}{
		{"already_exists", "tag_name", true},
		{"already_exists", "", true}, // field is a narrowing filter, not required
		{"custom", "file_count", true},
		{"Already_Exists", "", true}, // codes match case-insensitively
		{"missing_code", "", false},
		{"custom", "name", false}, // right code, wrong field
	} {
		if out := got.IsValidationIssue(tc.code, tc.field); out != tc.want {
			t.Errorf("IsValidationIssue(%q,%q)=%v, want %v", tc.code, tc.field, out, tc.want)
		}
	}

	// Same structured body on a non-422 status is not a validation issue.
	other := decodeAPIError(newResp(http.StatusNotFound, body))
	if other.IsValidationIssue("already_exists", "") {
		t.Error("non-422 status must never report a validation issue")
	}

	// Message variants carrying "1000"/"too many" prose but no structured
	// code must not match: bare-substring matching is exactly the bug.
	prose := `{"message":"file_count report: 1000 files listed, too many cooks in the kitchen"}`
	proseErr := decodeAPIError(newResp(http.StatusUnprocessableEntity, prose))
	if proseErr.IsValidationIssue("custom", "file_count") {
		t.Error("prose-only body must not match a structured validation code")
	}
}

// B7: the asset-URL cache must run on one clock. Storing with the wall
// clock while checking with the governor clock breaks TTL under an
// injected test clock (and skews under any clock discipline change).
func TestAssetURLCacheUsesGovernorClock(t *testing.T) {
	var mu sync.Mutex
	// Start the governor clock well behind the wall clock so the two
	// clocks observably diverge: a wall-clock store stays valid far
	// longer than the governor-clock TTL under test.
	now := time.Now().Add(-10 * time.Minute)
	cfg := storcfg.Default()
	cfg.Now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	c := NewClient("t", cfg)

	c.storeAssetURL(11, "https://cdn.example.com/x?sig=opaque&nonsense=1")
	if _, ok := c.cachedAssetURL(11); !ok {
		t.Fatal("stored URL must be cached before its TTL elapses")
	}
	mu.Lock()
	now = now.Add(61 * time.Second)
	mu.Unlock()
	if _, ok := c.cachedAssetURL(11); ok {
		t.Fatal("fallback entry must expire 60s after store on the governor clock")
	}
}

// B8: acquire() must not park the concurrency slot while sleeping out a
// throttle wait. A throttled waiter holding a slot head-of-line-blocks
// cheap requests behind it.
func TestAcquireDoesNotHoldSlotWhileThrottled(t *testing.T) {
	var mu sync.Mutex
	now := time.Now()
	var sleeps []time.Duration
	cfg := storcfg.Default()
	cfg.MaxConcurrentRequests = 1
	cfg.RatePointsPerMin = 1
	cfg.RateMaxWait = time.Hour
	cfg.Now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	g := newRateGovernor(cfg, nil, func(_ context.Context, d time.Duration) error {
		mu.Lock()
		defer mu.Unlock()
		sleeps = append(sleeps, d)
		now = now.Add(d)
		return nil
	})

	// Fill the per-minute point window BEFORE anyone holds the slot, so
	// the next acquire must throttle. (With concurrency 1, filling
	// while a holder parks the slot would deadlock the filler.)
	rel, err := g.acquire(context.Background(), 1, false, false)
	if err != nil {
		t.Fatalf("window-fill acquire: %v", err)
	}
	rel()

	// Holder takes the only slot and keeps it. Its own throttle sleep
	// is the baseline: the waiter must add one more while parked.
	relA, err := g.acquire(context.Background(), 1, false, false)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	var relOnce sync.Once
	releaseA := func() { relOnce.Do(relA) }
	defer releaseA()
	mu.Lock()
	baseline := len(sleeps)
	mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := g.acquire(ctx, 1, false, false)
		done <- err
	}()

	// The throttled waiter must record its throttle sleep while the slot
	// is still held by A. Old code blocks on the slot first: no sleep.
	deadline := time.Now().Add(3 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(sleeps)
		mu.Unlock()
		if n > baseline {
			seen = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	releaseA()
	if !seen {
		t.Fatal("throttled acquire held the concurrency slot during its wait (no throttle sleep recorded while slot was held)")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second acquire after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second acquire never completed after the slot was released")
	}
}

// B9: observe() must not let a stale (late-arriving) snapshot regress the
// budget: an older window, or a higher remaining count for the same
// window, is strictly older information and must be ignored. Adopting a
// newer window re-arms the one-shot budget warnings.
func TestObserveIgnoresStaleSnapshots(t *testing.T) {
	h := newGovHarness(nil)
	reset := h.t.Add(30 * time.Minute).Unix()
	hdr := func(remaining int64, resetUnix int64) http.Header {
		hh := http.Header{}
		hh.Set("X-RateLimit-Limit", "5000")
		hh.Set("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
		hh.Set("X-RateLimit-Reset", fmt.Sprintf("%d", resetUnix))
		return hh
	}

	h.g.observe(hdr(90, reset))
	if got := h.g.snapshot().remaining; got != 90 {
		t.Fatalf("remaining=%d, want 90", got)
	}

	// Same window, higher remaining: older information, must be ignored.
	h.g.observe(hdr(95, reset))
	if got := h.g.snapshot().remaining; got != 90 {
		t.Fatalf("stale snapshot regressed budget: remaining=%d, want 90", got)
	}

	// Same window, lower remaining: normal progress, must be adopted.
	h.g.observe(hdr(88, reset))
	if got := h.g.snapshot().remaining; got != 88 {
		t.Fatalf("fresh snapshot not adopted: remaining=%d, want 88", got)
	}

	// Older window entirely: must be ignored.
	h.g.observe(hdr(4000, reset-3600))
	snap := h.g.snapshot()
	if snap.remaining != 88 || snap.resetAt.Unix() != reset {
		t.Fatalf("older-window snapshot regressed budget: %+v", snap)
	}

	// Drive the low-budget warning, then roll to a newer window: the
	// warning must re-arm so the next hour still warns.
	h.g.observe(hdr(10, reset))
	if !h.g.warnedLow {
		t.Fatal("low budget should arm the low warning")
	}
	newReset := reset + 3600
	h.g.observe(hdr(4999, newReset))
	snap = h.g.snapshot()
	if snap.remaining != 4999 || snap.resetAt.Unix() != newReset {
		t.Fatalf("newer window not adopted: %+v", snap)
	}
	if h.g.warnedLow {
		t.Fatal("new window must re-arm the one-shot low warning")
	}
	h.g.observe(hdr(9, newReset))
	if !h.g.warnedLow {
		t.Fatal("low budget in the new window should warn again")
	}
}

// B10: content paths normalize (clean "." / ".." / empty segments,
// clamped at the repo root) instead of being rejected or leaking ".."
// into the request URL.
func TestEscapeContentPathNormalizes(t *testing.T) {
	cases := []struct{ in, want string }{
		{"a/b/c", "a/b/c"},
		{"a//b/./c", "a/b/c"},
		{"a/b/../c", "a/c"},
		{"../x", "x"},
		{"a/../../b", "b"},
		{"../../..", ""},
		{"", ""},
		{".", ""},
		{"/", ""},
		{"/leading/slash", "leading/slash"},
		{"sp ace/pl+us", "sp%20ace/pl+us"},
		{"raw#hash?mark", "raw%23hash%3Fmark"},
		{"100%/x", "100%25/x"},
	}
	for _, tc := range cases {
		if got := escapeContentPath(tc.in); got != tc.want {
			t.Errorf("escapeContentPath(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}
}
