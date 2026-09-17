package github

import (
	"context"
	"fmt"
	"math/rand"
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

// Sentinel convention (one table for every knob; zero is NOT uniformly
// "unset"):
//   - reserve:       <0 → library default (25); 0 → LIVE-ZERO, a real
//     setting meaning "no hourly floor, pace against the full budget"
//     (distinct from unset — zero-fuel headroom is a legitimate choice
//     for tests and one-shot CLIs); >0 → keep that many requests back.
//   - maxWait:        0 → library default (15m, long-running processes);
//     <0 → fail-fast (any positive wait is refused up front — one-shot
//     CLI); >0 → refuse waits beyond it.
//   - pointsPerMin / contentPerMin / concurrency:
//     <=0 → library default (720 / 60 / 16); >0 → that value.
//
// reserve==0 staying live-zero matters because tooLong only denies
// positive waits: a zero wait (budget healthy) must never be denied,
// which is also why a negative fail-fast maxWait still admits it.
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

// requestClass selects which governor windows a request draws from.
// Exactly three values exist — no fourth: the point cost still comes
// from methodCost, while the class decides window membership.
type requestClass int

const (
	// requestRead is a plain API exchange that creates no content (reads
	// plus non-content writes such as repo/release creation): it spends
	// the per-minute point window only.
	requestRead requestClass = iota
	// requestContent generates repository content (contents-API PUT/DELETE
	// and release-asset uploads): it spends the point window AND the
	// stricter content-creation window.
	requestContent
	// requestUpload is a release-asset POST to uploads.github.com: it
	// spends both per-minute windows but skips hourly pacing and hourly
	// budget accounting entirely (that endpoint sends no X-RateLimit
	// headers and draws from no hourly core budget).
	requestUpload
)

func (k requestClass) String() string {
	switch k {
	case requestRead:
		return "read"
	case requestContent:
		return "content"
	case requestUpload:
		return "upload"
	default:
		return "unknown"
	}
}

// content reports whether the class draws from the content-creation
// window (contents writes and asset uploads both do).
func (k requestClass) content() bool { return k == requestContent || k == requestUpload }

// assetUpload reports whether the class targets the header-less upload
// endpoint and therefore skips hourly pacing and budget accounting.
func (k requestClass) assetUpload() bool { return k == requestUpload }

// classifyFlags maps a legacy (content, assetUpload) flag pair onto a
// class. It exists only for the compat acquire wrapper below; new code
// classifies once via classifyRequest (client.go) and calls acquireClass.
func classifyFlags(content, assetUpload bool) requestClass {
	if assetUpload {
		return requestUpload
	}
	if content {
		return requestContent
	}
	return requestRead
}

// acquire is the compat entry point: the (cost, content, assetUpload)
// triple is how the pre-enum governor tests drive admission. New code
// calls acquireClass with a classified requestClass instead.
func (g *rateGovernor) acquire(ctx context.Context, cost int64, content, assetUpload bool) (func(), error) {
	return g.acquireClass(ctx, cost, classifyFlags(content, assetUpload))
}

// acquireClass blocks until one request of the given class may be sent
// and returns a release func for the concurrency slot. It fails with an
// *APIError when the required wait exceeds maxWait - honest refusal beats
// silently stalling a command past its usefulness.
//
// The concurrency slot is taken AFTER the throttle wait, never across
// it: a throttled waiter holding a slot head-of-line-blocks cheap
// requests behind it while it sleeps. Local budget accounting commits in
// reserve() at zero-wait exactly as before; the slot only gates sending.
//
// requestUpload marks POSTs to uploads.github.com, a separate authority
// that sends no X-RateLimit headers and draws from no hourly core budget.
// Such requests skip hourly pacing (floor + sustainable pace + local
// budget accounting) but still honor the per-minute windows and
// concurrency cap.
func (g *rateGovernor) acquireClass(ctx context.Context, cost int64, class requestClass) (func(), error) {
	for {
		wait, apiErr := g.reserve(cost, class)
		if apiErr != nil {
			return nil, apiErr
		}
		if wait == 0 {
			break
		}
		logging.Warn(g.logger, "rate limit throttle", "wait", wait.Round(time.Millisecond), "cost", cost, "class", class.String())
		if err := g.sleep(ctx, throttleJitter(wait)); err != nil {
			return nil, err
		}
	}
	select {
	case g.inflight <- struct{}{}:
	case <-ctx.Done():
		// The zero-wait reservation was already committed in reserve();
		// the request will never be sent, so undo the accounting instead
		// of over-counting a phantom request.
		g.rollback(cost, class)
		return nil, ctx.Err()
	}
	return func() { <-g.inflight }, nil
}

// throttleJitter spreads client-side pacing waits to keep fleets of
// StorHub processes from waking in lockstep against the same budget
// reset: +0-25%, additive only. Additive (never subtractive) so a wait
// never dips below what the budget accounting requires — pacing slower
// is always safe, pacing faster is not. Server-dictated waits (hourly
// reset, rate-limit Retry-After/Reset) stay exact: the server owns the
// resume instant, and fuzzing it would either arrive early (wasted call)
// or sleep past maxWait incorrectly. maxWait denial is evaluated on the
// unjittered wait inside reserve; the jittered sleep may overshoot
// maxWait by up to 25%, which is bounded and ctx-cancellable. Zero-safe.
func throttleJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration(rand.Int63n(int64(d)/4+1))
}

// rollback undoes a committed zero-wait reservation whose request never
// reached the wire (the concurrency slot select failed on ctx.Done). It
// clamps at zero because the accounting window may already have rolled
// over between commit and rollback.
func (g *rateGovernor) rollback(cost int64, class requestClass) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.winPoints >= cost {
		g.winPoints -= cost
	} else {
		g.winPoints = 0
	}
	if class.content() && g.winContent > 0 {
		g.winContent--
	}
	if !class.assetUpload() {
		g.tokens += float64(cost)
		if g.budget.seen && g.budget.remaining < g.budget.limit {
			g.budget.remaining++
		}
	}
}

// reserve computes the wait before sending; commit happens only when the
// caller accepts a zero wait. The policy lives in four small helpers so
// each window reads alone; reserve itself only sequences them and
// commits. Callers must not hold g.mu (reserve takes it); the helpers
// below require it held.
func (g *rateGovernor) reserve(cost int64, class requestClass) (time.Duration, *APIError) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()

	var wait time.Duration
	if w, err := g.hourlyWaitLocked(now, cost, class); err != nil {
		return 0, err
	} else {
		wait = maxDuration(wait, w)
	}
	tokens, w, err := g.pacingWaitLocked(now, cost, class)
	if err != nil {
		return 0, err
	}
	wait = maxDuration(wait, w)
	if w, err := g.windowWaitLocked(now, cost, class); err != nil {
		return 0, err
	} else {
		wait = maxDuration(wait, w)
	}
	if wait == 0 {
		g.commitReservationLocked(now, cost, class, tokens)
	}
	return wait, nil
}

// denyLocked builds a governor refusal carrying the current reset time.
// Callers must hold g.mu.
func (g *rateGovernor) denyLocked(primary bool, reason string) *APIError {
	return &APIError{
		StatusCode:     http.StatusTooManyRequests,
		Message:        fmt.Sprintf("rate limit governor: %s", reason),
		RateLimited:    true,
		RateLimitReset: g.budget.resetAt,
		Primary:        primary,
	}
}

// tooLongLocked reports whether a positive wait exceeds the ceiling.
// Only positive waits can exceed it; a zero wait must never be denied,
// which matters because maxWait may be the negative fail-fast sentinel.
// Callers must hold g.mu.
func (g *rateGovernor) tooLongLocked(d time.Duration) bool { return d > 0 && d > g.cfg.maxWait }

// hourlyWaitLocked returns the wait imposed by the hourly budget floor:
// once remaining dips into the reserve only the reset helps. Until any
// response carries rate-limit headers there is no budget to pace against,
// so hourly gating stays dormant; the per-minute windows still apply.
// Asset uploads skip this entirely (no hourly budget on their endpoint).
// A refusal (wait beyond maxWait) returns a primary denial error.
// Callers must hold g.mu.
func (g *rateGovernor) hourlyWaitLocked(now time.Time, cost int64, class requestClass) (time.Duration, *APIError) {
	if class.assetUpload() || !g.budget.seen || !now.Before(g.budget.resetAt) || g.budget.remaining > g.cfg.reserve+cost {
		return 0, nil
	}
	untilReset := g.budget.resetAt.Sub(now)
	if g.tooLongLocked(untilReset) {
		return 0, g.denyLocked(true, fmt.Sprintf("hourly budget at reserve (%d left), resets in %s", g.budget.remaining, untilReset.Round(time.Second)))
	}
	return untilReset + time.Second, nil
}

// pacingWaitLocked refills the sustainable-pace token bucket and returns
// the wait it imposes plus the refilled token balance for a later
// commit. Refill runs at spendable/time-until-reset per second, capped by
// a 30s burst pool (+10 slack so the very first requests never wait).
// Asset uploads skip this: the upload endpoint never reports a budget,
// so pacing bursts against a stale core snapshot only manufactures
// denials for a server that would accept the traffic (returns zero
// tokens, zero wait, nil error — commit ignores the balance for uploads).
// Callers must hold g.mu.
func (g *rateGovernor) pacingWaitLocked(now time.Time, cost int64, class requestClass) (float64, time.Duration, *APIError) {
	if class.assetUpload() {
		return 0, 0, nil
	}
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
		if g.tooLongLocked(paceWait) {
			return 0, 0, g.denyLocked(true, "sustainable request pace exhausted")
		}
		return tokens, paceWait, nil
	}
	return tokens, 0, nil
}

// windowWaitLocked rolls the per-minute secondary windows over when due
// and returns the wait they impose (point budget, then content-creation
// budget). The rollover mutates even when a wait or denial follows —
// that matches the pre-split behavior exactly. Callers must hold g.mu.
func (g *rateGovernor) windowWaitLocked(now time.Time, cost int64, class requestClass) (time.Duration, *APIError) {
	if now.Sub(g.winStart) >= secondaryWindow {
		g.winStart = now
		g.winPoints = 0
		g.winContent = 0
	}
	var wait time.Duration
	if g.winPoints+cost > g.cfg.pointsPerMin {
		pointWait := secondaryWindow - now.Sub(g.winStart)
		if g.tooLongLocked(pointWait) {
			return 0, g.denyLocked(false, "per-minute point budget exhausted")
		}
		wait = maxDuration(wait, pointWait)
	}
	if class.content() && g.winContent >= g.cfg.contentPerMin {
		contentWait := secondaryWindow - now.Sub(g.winStart)
		if g.tooLongLocked(contentWait) {
			return 0, g.denyLocked(false, "content creation budget exhausted")
		}
		wait = maxDuration(wait, contentWait)
	}
	return wait, nil
}

// commitReservationLocked applies zero-wait accounting: hourly tokens and
// budget for core requests only (asset uploads neither draw from nor
// replenish the hourly bucket — their endpoint reports nothing), plus
// the per-minute windows for every class. Callers must hold g.mu and
// must call it only when the computed wait is zero.
func (g *rateGovernor) commitReservationLocked(now time.Time, cost int64, class requestClass, tokens float64) {
	if !class.assetUpload() {
		g.lastRefill = now
		g.tokens = tokens - float64(cost)
		if g.budget.seen && g.budget.remaining > 0 {
			g.budget.remaining--
		}
	}
	g.winPoints += cost
	if class.content() {
		g.winContent++
	}
}

// observe folds x-ratelimit-* headers from any response into the budget
// snapshot. Responses without them (the upload endpoint) leave local
// accounting untouched. Late-arriving snapshots never regress the
// budget: an older window, or a higher remaining count for the current
// window, is strictly older information (remaining only decreases within
// a window for one token) and is ignored. Adopting a newer window
// re-arms the one-shot budget warnings so every hour warns again.
func (g *rateGovernor) observe(header http.Header) {
	limit, hasLimit := parseIntHeader(header.Get("X-RateLimit-Limit"))
	remaining, hasRemaining := parseIntHeader(header.Get("X-RateLimit-Remaining"))
	resetAt, hasReset := parseUnixTime(header.Get("X-RateLimit-Reset"))
	if !hasLimit || !hasRemaining || !hasReset {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.budget.seen {
		if resetAt.Before(g.budget.resetAt) {
			return
		}
		if resetAt.Equal(g.budget.resetAt) && remaining > g.budget.remaining {
			return
		}
		if resetAt.After(g.budget.resetAt) {
			g.warnedHigh = false
			g.warnedLow = false
		}
	}
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
