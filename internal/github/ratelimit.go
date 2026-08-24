package github

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"log/slog"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/logging"
)

// Documented GitHub limits (docs.github.com/en/rest/using-the-rest-api/
// rate-limits-for-the-rest-api): 5,000 core requests per hour per token,
// 900 secondary points per minute (GET=1, POST/PUT/PATCH/DELETE=5), and
// no more than 80 content-generating requests per minute. The defaults
// below sit under those ceilings with margin so steady use never meets
// the enforcement side.
const (
	defaultRateReserve       = 25
	defaultRateMaxWait       = 15 * time.Minute
	defaultRatePointsPerMin  = 720
	defaultRateContentPerMin = 60
	defaultRateConcurrency   = 16

	secondaryWindow   = time.Minute
	warnRemainingHigh = 200
	warnRemainingLow  = 50
)

type rateConfig struct {
	reserve       int64
	maxWait       time.Duration
	pointsPerMin  int64
	contentPerMin int64
	concurrency   int64
}

func resolveRateConfig(cfg storcfg.Config) rateConfig {
	rc := rateConfig{
		reserve:       cfg.RateReserve,
		maxWait:       cfg.RateMaxWait,
		pointsPerMin:  cfg.RatePointsPerMin,
		contentPerMin: cfg.RateContentPerMin,
		concurrency:   cfg.MaxConcurrentRequests,
	}
	if rc.reserve < 0 {
		rc.reserve = defaultRateReserve
	}
	// Zero means "not configured" and takes the library default; an
	// explicitly negative max wait opts into fail-fast behavior.
	if rc.maxWait == 0 {
		rc.maxWait = defaultRateMaxWait
	}
	if rc.pointsPerMin <= 0 {
		rc.pointsPerMin = defaultRatePointsPerMin
	}
	if rc.contentPerMin <= 0 {
		rc.contentPerMin = defaultRateContentPerMin
	}
	if rc.concurrency <= 0 {
		rc.concurrency = defaultRateConcurrency
	}
	return rc
}

// budgetState mirrors the server's x-ratelimit-* headers for the hourly
// core budget. seen flips true on the first response carrying them.
type budgetState struct {
	limit     int64
	remaining int64
	resetAt   time.Time
	seen      bool
}

// rateGovernor turns documented limits into admission control: nothing is
// sent unless the hourly budget, the per-minute point window, the
// content-creation window, and the concurrency ceiling all agree. Between
// server updates it accounts requests locally so bursts of parallel
// callers cannot overspend before the next header arrives.
//
// The hourly budget paces as a token bucket refilled at the sustainable
// rate (spendable / time-until-reset) with a small burst pool, so short
// bursts stay fast while sustained load converges to exactly reserve
// requests left at reset. maxWait bounds every wait this governor may
// demand; zero means fail fast instead of waiting (one-shot CLI).
type rateGovernor struct {
	cfg      rateConfig
	logger   *slog.Logger
	sleep    func(context.Context, time.Duration) error
	now      func() time.Time
	inflight chan struct{}

	mu         sync.Mutex
	budget     budgetState
	tokens     float64
	lastRefill time.Time
	winStart   time.Time
	winPoints  int64
	winContent int64
	warnedHigh bool
	warnedLow  bool
}

func newRateGovernor(cfg storcfg.Config, logger *slog.Logger, sleep func(context.Context, time.Duration) error) *rateGovernor {
	g := &rateGovernor{
		cfg:    resolveRateConfig(cfg),
		logger: logging.WithComponent(logger, "ratelimit"),
		sleep:  sleep,
		now:    time.Now,
	}
	// One clock source for the whole client: tests inject cfg.Now so a
	// recorded sleep can advance time and waits converge deterministically.
	if cfg.Now != nil {
		g.now = cfg.Now
	}
	g.inflight = make(chan struct{}, g.cfg.concurrency)
	return g
}

// methodCost maps a request onto the documented secondary point values:
// reads cost 1, everything that generates content costs 5.
func methodCost(method string) int64 {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return 1
	default:
		return 5
	}
}

// acquire blocks until one request of the given cost may be sent and
// returns a release func for the concurrency slot. It fails with an
// *APIError when the required wait exceeds maxWait - honest refusal beats
// silently stalling a command past its usefulness.
func (g *rateGovernor) acquire(ctx context.Context, cost int64, content bool) (func(), error) {
	select {
	case g.inflight <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() { <-g.inflight }
	for {
		wait, apiErr := g.reserve(cost, content)
		if apiErr != nil {
			release()
			return nil, apiErr
		}
		if wait == 0 {
			return release, nil
		}
		logging.Warn(g.logger, "rate limit throttle", "wait", wait.Round(time.Millisecond), "cost", cost, "content", content)
		if err := g.sleep(ctx, wait); err != nil {
			release()
			return nil, err
		}
	}
}

// reserve computes the wait before sending; commit happens only when the
// caller accepts a zero wait.
func (g *rateGovernor) reserve(cost int64, content bool) (time.Duration, *APIError) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()

	var wait time.Duration
	deny := func(primary bool, reason string) *APIError {
		err := &APIError{
			StatusCode:     http.StatusTooManyRequests,
			Message:        fmt.Sprintf("rate limit governor: %s", reason),
			RateLimited:    true,
			RateLimitReset: g.budget.resetAt,
			Primary:        primary,
		}
		return err
	}
	// Only positive waits can exceed the ceiling; a zero wait must never
	// be denied, which matters because maxWait may be the negative
	// fail-fast sentinel.
	tooLong := func(d time.Duration) bool { return d > 0 && d > g.cfg.maxWait }

	// Hourly budget floor: once remaining dips into the reserve only the
	// reset helps, so wait for it or refuse. Until any response carries
	// rate-limit headers there is no budget to pace against, so hourly
	// gating stays dormant; the per-minute windows below still apply.
	if g.budget.seen && now.Before(g.budget.resetAt) && g.budget.remaining <= g.cfg.reserve+cost {
		untilReset := g.budget.resetAt.Sub(now)
		if tooLong(untilReset) {
			return 0, deny(true, fmt.Sprintf("hourly budget at reserve (%d left), resets in %s", g.budget.remaining, untilReset.Round(time.Second)))
		}
		wait = untilReset + time.Second
	}

	// Sustainable pace with burst tolerance: refill tokens at spendable /
	// window per second since the last admission, capped by a 30s burst
	// pool (+10 slack so the very first requests never wait).
	refill := 0.0
	if g.budget.seen && now.Before(g.budget.resetAt) {
		spendable := float64(g.budget.remaining - g.cfg.reserve)
		window := g.budget.resetAt.Sub(now).Seconds()
		if spendable > 0 && window > 0 {
			refill = spendable / window
		}
	}
	if g.lastRefill.IsZero() {
		g.lastRefill = now
		g.tokens = float64(cost) + 10
	}
	elapsed := now.Sub(g.lastRefill).Seconds()
	tokens := minf(g.tokens+refill*elapsed, refill*30+10)
	if tokens < float64(cost) {
		var paceWait time.Duration
		switch {
		case refill > 0:
			paceWait = time.Duration((float64(cost) - tokens) / refill * float64(time.Second))
		case !g.budget.resetAt.IsZero() && now.Before(g.budget.resetAt):
			paceWait = g.budget.resetAt.Sub(now)
		}
		if tooLong(paceWait) {
			return 0, deny(true, "sustainable request pace exhausted")
		}
		wait = maxDuration(wait, paceWait)
	}

	// Per-minute secondary windows.
	if now.Sub(g.winStart) >= secondaryWindow {
		g.winStart = now
		g.winPoints = 0
		g.winContent = 0
	}
	if g.winPoints+cost > g.cfg.pointsPerMin {
		pointWait := secondaryWindow - now.Sub(g.winStart)
		if tooLong(pointWait) {
			return 0, deny(false, "per-minute point budget exhausted")
		}
		wait = maxDuration(wait, pointWait)
	}
	if content && g.winContent >= g.cfg.contentPerMin {
		contentWait := secondaryWindow - now.Sub(g.winStart)
		if tooLong(contentWait) {
			return 0, deny(false, "content creation budget exhausted")
		}
		wait = maxDuration(wait, contentWait)
	}

	// Commit the reservation when no wait is needed.
	if wait == 0 {
		g.lastRefill = now
		g.tokens = tokens - float64(cost)
		g.winPoints += cost
		if content {
			g.winContent++
		}
		if g.budget.seen && g.budget.remaining > 0 {
			g.budget.remaining--
		}
	}
	return wait, nil
}

// observe folds x-ratelimit-* headers from any response into the budget
// snapshot. Responses without them (the upload endpoint) leave local
// accounting untouched.
func (g *rateGovernor) observe(header http.Header) {
	limit, hasLimit := parseIntHeader(header.Get("X-RateLimit-Limit"))
	remaining, hasRemaining := parseIntHeader(header.Get("X-RateLimit-Remaining"))
	resetAt, hasReset := parseUnixTime(header.Get("X-RateLimit-Reset"))
	if !hasLimit || !hasRemaining || !hasReset {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.budget = budgetState{limit: limit, remaining: remaining, resetAt: resetAt, seen: true}
	g.lastRefill = g.now()
	switch {
	case remaining <= warnRemainingLow:
		if !g.warnedLow {
			g.warnedLow = true
			logging.Warn(g.logger, "rate budget low", "remaining", remaining, "resets_at", resetAt)
		}
	case remaining <= warnRemainingHigh:
		if !g.warnedHigh {
			g.warnedHigh = true
			logging.Warn(g.logger, "rate budget depleting", "remaining", remaining, "resets_at", resetAt)
		}
	}
}

// snapshot returns the latest known budget so errors from endpoints that
// omit rate-limit headers can still carry an accurate reset time.
func (g *rateGovernor) snapshot() budgetState {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.budget
}

func parseIntHeader(v string) (int64, bool) {
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil
}

func minf(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
