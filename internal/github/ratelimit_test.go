package github

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

type govHarness struct {
	g      *rateGovernor
	t      time.Time
	sleeps []time.Duration
}

func newGovHarness(mutate func(*storcfg.Config)) *govHarness {
	cfg := storcfg.Default()
	if mutate != nil {
		mutate(&cfg)
	}
	h := &govHarness{t: time.Now()}
	g := newRateGovernor(cfg, nil, func(_ context.Context, d time.Duration) error {
		h.sleeps = append(h.sleeps, d)
		h.t = h.t.Add(d)
		return nil
	})
	g.now = func() time.Time { return h.t }
	h.g = g
	return h
}

func (h *govHarness) observe(limit, remaining int64, resetIn time.Duration) {
	header := http.Header{}
	header.Set("X-RateLimit-Limit", itoa(limit))
	header.Set("X-RateLimit-Remaining", itoa(remaining))
	header.Set("X-RateLimit-Reset", itoa(h.t.Add(resetIn).Unix()))
	h.g.observe(header)
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}

func TestMethodCostValues(t *testing.T) {
	readMethods := []string{http.MethodGet, http.MethodHead, http.MethodOptions}
	writeMethods := []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}
	for _, m := range readMethods {
		if got := methodCost(m); got != 1 {
			t.Errorf("method %s cost=%d, want 1", m, got)
		}
	}
	for _, m := range writeMethods {
		if got := methodCost(m); got != 5 {
			t.Errorf("method %s cost=%d, want 5", m, got)
		}
	}
}

func TestGovernorObserveAndLocalAccounting(t *testing.T) {
	h := newGovHarness(nil)
	h.observe(5000, 100, 30*time.Minute)
	snap := h.g.snapshot()
	if !snap.seen || snap.limit != 5000 || snap.remaining != 100 {
		t.Fatalf("snapshot after observe: %+v", snap)
	}
	release, err := h.g.acquire(context.Background(), methodCost(http.MethodGet), false)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	release()
	release, err = h.g.acquire(context.Background(), methodCost(http.MethodGet), false)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	release()
	release, _ = h.g.acquire(context.Background(), methodCost(http.MethodGet), false)
	release()
	if left := h.g.snapshot().remaining; left != 97 {
		t.Fatalf("local accounting drift: remaining=%d, want 97", left)
	}
}

func TestGovernorReserveFloorDeniesWhenFailFast(t *testing.T) {
	h := newGovHarness(func(c *storcfg.Config) { c.RateMaxWait = 0 })
	h.observe(5000, 10, 40*time.Minute)
	_, err := h.g.acquire(context.Background(), 1, false)
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected *APIError, got %v", err)
	}
	if !apiErr.RateLimited || !apiErr.Primary {
		t.Fatalf("expected primary rate-limit error, got %+v", apiErr)
	}
	if apiErr.RateLimitReset.IsZero() {
		t.Fatal("denial must carry the reset time")
	}
}

func TestGovernorReserveFloorWaitsUntilReset(t *testing.T) {
	h := newGovHarness(func(c *storcfg.Config) { c.RateMaxWait = 15 * time.Minute })
	resetIn := 5 * time.Minute
	h.observe(5000, 10, resetIn)
	release, err := h.g.acquire(context.Background(), 1, false)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	release()
	if len(h.sleeps) != 1 || h.sleeps[0] < resetIn {
		t.Fatalf("expected one wait of at least %s, got %v", resetIn, h.sleeps)
	}
}

func TestGovernorPointsWindowThrottlesWrites(t *testing.T) {
	h := newGovHarness(func(c *storcfg.Config) {
		c.RatePointsPerMin = 6
		c.RateMaxWait = time.Hour
	})
	release, err := h.g.acquire(context.Background(), 5, false)
	if err != nil || release == nil {
		t.Fatalf("first write should pass: %v", err)
	}
	release()
	release, err = h.g.acquire(context.Background(), 5, false)
	if err != nil {
		t.Fatalf("second write should wait, not fail: %v", err)
	}
	release()
	if len(h.sleeps) != 1 || h.sleeps[0] <= 0 || h.sleeps[0] > secondaryWindow {
		t.Fatalf("expected one sub-minute wait, got %v", h.sleeps)
	}
}

func TestGovernorContentWindowCapsUploadsOnly(t *testing.T) {
	h := newGovHarness(func(c *storcfg.Config) {
		c.RateContentPerMin = 1
		c.RateMaxWait = time.Hour
	})
	release, err := h.g.acquire(context.Background(), 5, true)
	if err != nil {
		t.Fatalf("first upload: %v", err)
	}
	release()
	release, err = h.g.acquire(context.Background(), 1, false)
	if err != nil || len(h.sleeps) != 0 {
		t.Fatalf("reads must not draw from the content window: err=%v sleeps=%v", err, h.sleeps)
	}
	release()
	release, err = h.g.acquire(context.Background(), 5, true)
	if err != nil {
		t.Fatalf("second upload must wait, not fail: %v", err)
	}
	release()
	if len(h.sleeps) != 1 {
		t.Fatalf("second upload must have waited once, got %v", h.sleeps)
	}
}

func TestGovernorDormantWithoutServerBudget(t *testing.T) {
	h := newGovHarness(nil)
	for i := 0; i < 20; i++ {
		release, err := h.g.acquire(context.Background(), 1, false)
		if err != nil {
			t.Fatalf("request %d failed without budget data: %v", i, err)
		}
		release()
	}
	if len(h.sleeps) != 0 {
		t.Fatalf("hourly gating must stay dormant until headers arrive, got sleeps %v", h.sleeps)
	}
}
