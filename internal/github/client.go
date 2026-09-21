package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/logging"
)

const (
	maxAPIErrorBodyBytes = 64 << 10
	pageSize             = 100
	// defaultRequestTimeout bounds small API exchanges; sized transfers
	// use transferDeadline instead, which scales with bytes.
	defaultRequestTimeout = 60 * storcfg.PatienceUnit
	defaultBaseRetryDelay = 10 * storcfg.TickUnit
	defaultMaxRetryDelay  = 160 * storcfg.TickUnit
	// Conservative upstream/downstream throughput assumption for sizing
	// transfer deadlines; capped links are the target environment.
	defaultTransferThroughput = int64(1 << 20) // 1 MiB/s
)

// transferDeadline converts a payload size into a request deadline:
// size divided by an assumed throughput floor, never below the general
// request timeout. A 1.7 GiB chunk over a capped 5 MiB/s link needs
// ~5.5 minutes of pure transfer, so deadlines must scale instead of
// amputating every large transfer at a fixed wall.
func (c *Client) transferDeadline(size int64) time.Duration {
	tp := c.transferThroughput
	if tp <= 0 {
		tp = defaultTransferThroughput
	}
	if size <= 0 {
		return defaultRequestTimeout
	}
	deadline := time.Duration(size/tp)*time.Second + 40*storcfg.TickUnit
	if deadline < defaultRequestTimeout {
		deadline = defaultRequestTimeout
	}
	return deadline
}

// Client is the GitHub API client with retry and rate-limit handling.
type Client struct {
	token              string
	apiBaseURL         string
	apiVersion         string
	client             *http.Client
	noFollow           *http.Client
	cdn                *http.Client
	maxRetries         int
	baseRetryDelay     time.Duration
	maxRetryDelay      time.Duration
	transferThroughput int64
	sleep              func(context.Context, time.Duration) error
	logger             *slog.Logger
	governor           *rateGovernor

	// assetMu guards assetURLs. Reads take the shared lock: the lookup
	// runs on every FUSE range read, and an exclusive lock there would
	// serialize all mounted reads through one convoy.
	assetMu   sync.RWMutex
	assetURLs map[int64]cachedAssetURL
}

// NewClient builds a Client for the token with the given config.
func NewClient(token string, cfg storcfg.Config) *Client {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultRequestTimeout}
	}
	sleep := cfg.Sleep
	if sleep == nil {
		sleep = storcfg.SleepWithContext
	}
	baseDelay := cfg.BaseRetryDelay
	if baseDelay <= 0 {
		baseDelay = defaultBaseRetryDelay
	}
	maxDelay := cfg.MaxRetryDelay
	if maxDelay <= 0 {
		maxDelay = defaultMaxRetryDelay
	}
	uploadThroughput := cfg.TransferThroughput
	if uploadThroughput <= 0 {
		uploadThroughput = defaultTransferThroughput
	}
	return &Client{
		token:              token,
		apiBaseURL:         strings.TrimRight(cfg.APIBaseURL, "/"),
		apiVersion:         cfg.APIVersion,
		client:             client,
		maxRetries:         cfg.MaxRetries,
		baseRetryDelay:     baseDelay,
		maxRetryDelay:      maxDelay,
		transferThroughput: uploadThroughput,
		sleep:              sleep,
		logger:             logging.WithComponent(cfg.Logger, "github"),
		governor:           newRateGovernor(cfg, cfg.Logger, sleep),
		assetURLs:          make(map[int64]cachedAssetURL),
		noFollow:           noRedirectClient(client),
		cdn:                bareCDNClient(client),
	}
}

// noRedirectClient returns a copy of client that surfaces redirect
// responses instead of following them.
func noRedirectClient(client *http.Client) *http.Client {
	noFollow := *client
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &noFollow
}

// bareCDNClient returns a copy of client without the request timeout for
// streaming range fetches against signed CDN URLs. No auth header is ever
// attached to these requests; cancellation is context-driven.
func bareCDNClient(client *http.Client) *http.Client {
	cdn := *client
	cdn.Timeout = 0
	return &cdn
}

// paginateGET follows per_page/page pagination until a short (or empty)
// page terminates the listing, appending every item. The endpoint closure
// receives the 1-based page number. Exact multiples still terminate:
// the API answers the first past-the-end page with an empty batch.
func paginateGET[T any](ctx context.Context, c *Client, endpoint func(page int) string) ([]T, error) {
	out := make([]T, 0)
	for page := 1; ; page++ {
		var batch []T
		if err := c.getJSON(ctx, endpoint(page), &batch); err != nil {
			return nil, err
		}
		out = append(out, batch...)
		if len(batch) < pageSize {
			return out, nil
		}
	}
}

// now is the client's single clock: the governor clock (injectable via
// cfg.Now) when present, wall time otherwise. Asset-cache TTLs are both
// stored and checked on this clock so injected test time moves them
// together.
func (c *Client) now() time.Time {
	if c.governor != nil && c.governor.now != nil {
		return c.governor.now()
	}
	return time.Now()
}

func (c *Client) doJSON(ctx context.Context, method, endpoint string, body any) (*http.Response, error) {
	return c.doJSONWithRetryable(ctx, method, endpoint, body, isRetrySafeMethod(method))
}

func (c *Client) doJSONWithRetryable(ctx context.Context, method, endpoint string, body any, retryable bool) (*http.Response, error) {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
	}
	// The JSON family never uploads assets, so classification reduces to
	// the method+endpoint rule (contents writes draw from the content
	// window); the old inline OR-expression lives in classifyRequest now.
	class := classifyRequest(method, endpoint, false)
	return c.doRequest(ctx, method, endpoint, func() (io.Reader, error) {
		if payload == nil {
			return nil, nil
		}
		return bytes.NewReader(payload), nil
	}, class, retryable, "application/json", "application/vnd.github+json", "", int64(len(payload)), false)
}

// doCDNResolve performs an API GET that surfaces redirect responses
// instead of following them, so the caller can capture the signed asset
// CDN URL from Location. It also serves plain raw-accept contents GETs
// (GetFileContent): the contents API never redirects, so surfacing
// redirects is immaterial there, and the timeout-free transport matches
// the sized-transfer doctrine (a large raw read must not be amputated by
// the 5-minute client timeout either — the same bug class fixed for
// uploads).
func (c *Client) doCDNResolve(ctx context.Context, endpoint, accept, rangeHeader string) (*http.Response, error) {
	return c.doRequest(ctx, http.MethodGet, endpoint, func() (io.Reader, error) {
		return nil, nil
	}, requestRead, true, "", accept, rangeHeader, 0, true)
}

func (c *Client) getJSON(ctx context.Context, endpoint string, out any) error {
	resp, err := c.doJSON(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// doRequest is the shared retry loop: it runs governed attempts until
// classifyAndWait reports success or a terminal error. The request
// profile arrives as explicit scalars — class selects the governor
// windows, retryable gates retries, the header strings and contentSize
// are pure data, and noFollow selects the redirect-surfacing transport
// for CDN resolution. Only the three explicit entry points above (plus
// deleteByURL) construct profiles; there is no flag bundle.
func (c *Client) doRequest(ctx context.Context, method, endpoint string, bodyFactory func() (io.Reader, error), class requestClass, retryable bool, contentType, accept, rangeHeader string, contentSize int64, noFollow bool) (*http.Response, error) {
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		resp, sendErr := c.sendOnce(ctx, method, endpoint, bodyFactory, class, contentType, accept, rangeHeader, contentSize, noFollow)
		out, retry, err := c.classifyAndWait(ctx, method, endpoint, attempt, resp, sendErr, retryable)
		if err != nil {
			return nil, err
		}
		if !retry {
			return out, nil
		}
	}
	return nil, fmt.Errorf("github request %s %s: retry loop exhausted (maxRetries=%d)", method, endpoint, c.maxRetries)
}

// sendOnce performs a single governed HTTP exchange: governor admission,
// request build, transport, and header observation. It returns the
// response on transport success (whatever the status — classification
// belongs to classifyAndWait) or an error that is either a governor
// admission refusal (*APIError, subject to the same rate-limit wait
// discipline as server rejections) or a transport failure.
func (c *Client) sendOnce(ctx context.Context, method, endpoint string, bodyFactory func() (io.Reader, error), class requestClass, contentType, accept, rangeHeader string, contentSize int64, noFollow bool) (*http.Response, error) {
	release, err := c.governor.acquireClass(ctx, methodCost(method), class)
	if err != nil {
		return nil, err
	}
	reader, err := bodyFactory()
	if err != nil {
		release()
		return nil, err
	}
	if contentSize == 0 && reader != nil {
		reader = http.NoBody
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		release()
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.applyHeaders(req, contentType, accept, rangeHeader)
	if contentSize >= 0 {
		req.ContentLength = contentSize
	}
	client := c.client
	if noFollow {
		client = c.noFollow
	}
	if class == requestUpload || noFollow {
		// Sized transfers are bounded by transferDeadline below, not
		// by the client-wide timeout that amputates large payloads.
		// Applies on top of noFollow too: the legacy direct-200
		// streaming path (no redirect) must not stay bounded by the
		// 5-minute client timeout either.
		noTimeout := *client
		noTimeout.Timeout = 0
		client = &noTimeout
	}
	// Per-attempt transfer context: derived from the caller's ctx (never
	// from a previous attempt's) and canceled explicitly once this
	// attempt's response arrives, so retries always get a full deadline
	// and timers never pile up on a loop-shared defer.
	cancel := context.CancelFunc(func() {})
	if class == requestUpload && contentSize > 0 {
		var reqCtx context.Context
		reqCtx, cancel = context.WithTimeout(ctx, c.transferDeadline(contentSize))
		req = req.WithContext(reqCtx)
	}
	resp, err := client.Do(req)
	cancel()
	release()
	if err != nil {
		return nil, fmt.Errorf("perform request: %w", err)
	}
	c.governor.observe(resp.Header)
	return resp, nil
}

// classifyAndWait decodes one attempt's outcome and either returns the
// successful response, sleeps the computed wait and asks for another
// attempt, or returns the terminal error.
//
// Log discipline: Warn fires ONLY when actually sleeping before a retry
// (actionable: someone waits); the final failure — terminal rejection,
// exhausted retries, refused wait, or failed sleep — logs at Debug with
// the status and body snippet attached. Per-attempt start/complete
// chatter is gone: the hot read path no longer spams the log to prove
// it is working.
func (c *Client) classifyAndWait(ctx context.Context, method, endpoint string, attempt int, resp *http.Response, sendErr error, retryable bool) (*http.Response, bool, error) {
	// sleepOrRetry applies the terminal-or-sleep decision for an *APIError
	// from any source (governor refusal or decoded response). It returns
	// (true, nil) after sleeping, or (false, err) for terminal outcomes.
	sleepOrRetry := func(apiErr *APIError) (bool, error) {
		if attempt >= c.maxRetries || !retryable || !apiErr.IsRetryable() {
			logging.Debug(c.logger, "http request failed", "method", method, "url", endpoint, "attempt", attempt+1, "status", apiErr.StatusCode, "rate_limited", apiErr.RateLimited, "primary", apiErr.Primary, "retry_after", apiErr.RetryAfter, "rate_reset", apiErr.RateLimitReset, "body", apiErr.BodySnippet(), "err", apiErr)
			return false, apiErr
		}
		delay := c.retryDelay(attempt, apiErr)
		// Primary exhaustion is not a backoff situation: the budget
		// is gone until reset. Waiting longer than maxWait allows is
		// refused up front instead of pretending an 8s retry helps.
		if apiErr.RateLimited && delay > c.governor.cfg.maxWait {
			logging.Debug(c.logger, "http request failed", "method", method, "url", endpoint, "attempt", attempt+1, "status", apiErr.StatusCode, "delay", delay, "max_wait", c.governor.cfg.maxWait, "body", apiErr.BodySnippet(), "err", apiErr)
			return false, apiErr
		}
		logging.Warn(c.logger, "http retry sleep", "method", method, "url", endpoint, "attempt", attempt+1, "delay", delay, "status", apiErr.StatusCode)
		if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
			return false, sleepErr
		}
		return true, nil
	}

	if sendErr != nil {
		var denied *APIError
		if errors.As(sendErr, &denied) {
			// Governor admission refusal: same rate-limit wait
			// discipline as a server rejection.
			retry, err := sleepOrRetry(denied)
			return nil, retry, err
		}
		if attempt >= c.maxRetries || !retryable || !isRetryableNetworkError(sendErr) {
			logging.Debug(c.logger, "http request failed", "method", method, "url", endpoint, "attempt", attempt+1, "retryable", false, "err", sendErr)
			return nil, false, sendErr
		}
		delay := c.retryDelay(attempt, nil)
		logging.Warn(c.logger, "http retry sleep", "method", method, "url", endpoint, "attempt", attempt+1, "delay", delay)
		if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
			return nil, false, sleepErr
		}
		return nil, true, nil
	}
	apiErr := c.decodeAPIError(resp)
	if apiErr == nil {
		return resp, false, nil
	}
	_ = resp.Body.Close()
	// Endpoints without rate-limit headers (uploads) still get an
	// accurate reset time from the governor's last snapshot.
	if apiErr.RateLimited && apiErr.RateLimitReset.IsZero() {
		if snap := c.governor.snapshot(); snap.seen && c.governor.now().Before(snap.resetAt) {
			apiErr.RateLimitReset = snap.resetAt
			if snap.remaining <= c.governor.cfg.reserve {
				apiErr.Primary = true
			}
		}
	}
	retry, err := sleepOrRetry(apiErr)
	return nil, retry, err
}

func (c *Client) applyHeaders(req *http.Request, contentType, accept, rangeHeader string) {
	if accept == "" {
		accept = "application/vnd.github+json"
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-GitHub-Api-Version", c.apiVersion)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
}

// rateLimitMarkers are substrings GitHub uses when rejecting requests on
// rate-limit grounds. The API endpoint advertises this via headers, but
// the upload endpoint (uploads.github.com) sends none at all - the body
// is the only signal there.
var rateLimitMarkers = []string{
	"api rate limit exceeded",
	"secondary rate limit",
	"abuse detection mechanism",
}

// decodeAPIError classifies one HTTP error response on the client's
// single (injectable) clock, so HTTP-date Retry-After values compute
// against the same now() that reset waits in rateWait use. Fake-clock
// tests that freeze time must not see the wall clock leak in through
// this branch (the numeric-seconds Retry-After path is clock-free).
func (c *Client) decodeAPIError(resp *http.Response) *APIError {
	return decodeAPIErrorAt(resp, c.now())
}

func decodeAPIError(resp *http.Response) *APIError {
	return decodeAPIErrorAt(resp, time.Now())
}

func decodeAPIErrorAt(resp *http.Response, now time.Time) *APIError {
	if resp.StatusCode < http.StatusBadRequest {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxAPIErrorBodyBytes))
	var payload struct {
		Message string           `json:"message"`
		Errors  []APIErrorDetail `json:"errors"`
	}
	_ = json.Unmarshal(body, &payload)
	err := &APIError{StatusCode: resp.StatusCode, Message: payload.Message, Body: string(body), Headers: resp.Header.Clone(), Details: payload.Errors}
	if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), now); retryAfter > 0 {
		err.RetryAfter = retryAfter
	}
	marker := containsAny(strings.ToLower(payload.Message+" "+string(body)), rateLimitMarkers)
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		err.RateLimited = true
		if reset, ok := parseUnixTime(resp.Header.Get("X-RateLimit-Reset")); ok {
			err.RateLimitReset = reset
			err.Primary = true
		}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		err.RateLimited = true
	}
	// Header-less rejections (uploads.github.com): the body text is the
	// only evidence of rate limiting. These are not provably primary -
	// the honest classification is secondary-style pacing.
	//
	// The marker only counts on the statuses GitHub uses for rate/abuse
	// rejections (403/429): the same prose quoted inside a 404, 422 or
	// 5xx body (docs links, echoed text) is not a rejection and must not
	// take the rate-limit retry path.
	if !err.RateLimited && marker &&
		(resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests) {
		err.RateLimited = true
	}
	return err
}

func containsAny(haystack string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(haystack, needle) {
			return true
		}
	}
	return false
}

// Secondary-limit wait guidance: a bare secondary rejection (no reset,
// no Retry-After) waits at least secondaryBackoffBase with exponential
// growth, never past secondaryBackoffCap; the shift is clamped at
// secondaryBackoffMaxShift so wild attempt counts cannot overflow it.
// Every branch is jittered (see addJitter) so a synchronized fleet does
// not thunder-herd. Values preserved from the pre-split retryDelay.
const (
	secondaryBackoffBase     = 12 * storcfg.PatienceUnit
	secondaryBackoffCap      = 180 * storcfg.PatienceUnit
	secondaryBackoffMaxShift = 10
)

// retryDelay dispatches to the rate-limit vs generic wait calculators.
// It is kept (same name, same values) because governor and client tests
// pin it directly; new code may call rateWait/backoffWait explicitly.
func (c *Client) retryDelay(attempt int, apiErr *APIError) time.Duration {
	if apiErr != nil && apiErr.RateLimited {
		return c.rateWait(attempt, apiErr)
	}
	return c.backoffWait(attempt, apiErr)
}

// rateWait computes the wait before the next attempt for rate-limit
// rejections, following GitHub's documented guidance instead of the
// generic exponential backoff: primary exhaustion means waiting for
// x-ratelimit-reset (the doRequest caller refuses waits beyond maxWait),
// a rate-limited Retry-After is honored exactly (GitHub's secondary-limit
// hints run 30-120s; truncating them to maxRetryDelay manufactures repeat
// rejections and burns the point window - the maxWait ceiling is what
// refuses an excessive wait, mirroring the reset branch), and bare
// secondary rejections wait at least one minute with exponential growth.
// Every branch is bounded and jittered so a hostile header or a
// synchronized fleet cannot stall or thunder-herd callers.
func (c *Client) rateWait(attempt int, apiErr *APIError) time.Duration {
	if !apiErr.RateLimitReset.IsZero() {
		// Wait for the documented reset exactly: the server dictates the
		// resume instant, so jitter/caps here only overshoot it. Pinned
		// by TestRateLimitAwareRetry — the floor only prevents a hot
		// loop when the clock has already passed reset. The client's
		// single (injectable) clock keeps fake-clock tests honest.
		return nonNegativeDelay(apiErr.RateLimitReset.Sub(c.now()))
	}
	if apiErr.RetryAfter > 0 {
		return nonNegativeDelay(apiErr.RetryAfter)
	}
	if attempt > secondaryBackoffMaxShift {
		attempt = secondaryBackoffMaxShift // keep the shift below from overflowing on wild input
	}
	return addJitter(minDuration(secondaryBackoffBase<<attempt, secondaryBackoffCap))
}

// backoffWait computes the wait for non-rate-limit retries: a plain
// Retry-After hint bounded by maxRetryDelay (so a hostile or
// misconfigured header cannot stall callers invisibly for minutes),
// otherwise exponential backoff with jitter. Rate-limit waits do not
// pass through here: they are honored exactly and refused up front by
// the maxWait ceiling in doRequest.
func (c *Client) backoffWait(attempt int, apiErr *APIError) time.Duration {
	if apiErr != nil && apiErr.RetryAfter > 0 {
		return c.boundedWait(nonNegativeDelay(apiErr.RetryAfter))
	}
	base := float64(c.baseRetryDelay)
	delay := time.Duration(base * math.Pow(2, float64(attempt)))
	if delay > c.maxRetryDelay {
		delay = c.maxRetryDelay
	}
	if delay <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int63n(int64(delay/4 + 1)))
	return delay + jitter
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (c *Client) apiURL(path string) string {
	return c.apiBaseURL + path
}

func isRetrySafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

// classifyRequest is the single landing for governor window
// classification. GitHub's ~80/min content-generation secondary window
// counts every request that creates repository content: asset uploads
// AND contents-API PUT/DELETE (metadata commits). Classifying only
// assetUpload here once let a metadata-chatty mount burst past the
// window and eat real secondary penalties, so the contents-write rule
// lives here alongside the upload rule — never as an OR-expression at a
// call site.
func classifyRequest(method, endpoint string, assetUpload bool) requestClass {
	if assetUpload {
		return requestUpload
	}
	if isContentsWrite(method, endpoint) {
		return requestContent
	}
	return requestRead
}

// isContentsWrite reports whether a request mutates repository content
// through the contents API - the requests GitHub's content-generation
// secondary window counts (besides asset uploads).
func isContentsWrite(method, endpoint string) bool {
	if method != http.MethodPut && method != http.MethodDelete {
		return false
	}
	return strings.Contains(endpoint, "/contents/")
}

func nonNegativeDelay(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	return delay
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if strings.TrimSpace(value) == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		return when.Sub(now)
	}
	return 0
}

func parseUnixTime(value string) (time.Time, bool) {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}

// addJitter spreads a computed wait by up to +25% so retries from a fleet
// of synchronized callers do not arrive as one thundering herd. The floor
// of the input is always preserved: guidance minimums (60s secondary
// patience, reset waits) stay intact.
func addJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	quarter := int64(d / 4)
	if quarter <= 0 {
		return d
	}
	return d + time.Duration(rand.Int63n(quarter+1))
}

// IsTimeout reports whether err represents a client-side transfer
// timeout (as opposed to the caller's own deadline or cancellation).
// It is nil-safe and checks, in order:
//
//  1. Primary: net.Error.Timeout() (including *url.Error, whose Timeout
//     delegates to the wrapped error) — a genuine stalled transfer.
//  2. Fallback: the "Client.Timeout exceeded" substring http.Client
//     embeds when its own timeout fires.
//
// A bare context.DeadlineExceeded WITHOUT that marker is the caller's
// own deadline (context.WithTimeout/WithDeadline) and is NOT a timeout
// by this definition: retrying it burns the governor's budget against a
// decision the caller already made. context.Canceled is never a timeout.
//
// CONTRACT for the storage pipeline agent: internal/storage/retry.go's
// isRetryableNetworkError duplicates the DeadlineExceeded-vs-timeout
// distinction with its own strings.Contains check. Replace that arm with
// ghapi.IsTimeout (import already present as ghapi): inside the
// errors.Is(err, context.DeadlineExceeded) branch, return
// ghapi.IsTimeout(err). Keep the surrounding Canceled/DNS/net.Error
// structure identical.
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		// http.Client's own timeout surfaces as the same wrapped
		// context.DeadlineExceeded but carries the marker — a stalled
		// transfer is exactly what retries exist to absorb. A bare
		// DeadlineExceeded is the caller's deadline: not a timeout.
		return strings.Contains(err.Error(), "Client.Timeout exceeded")
	}
	var urlErr *url.Error
	if errorAs(err, &urlErr) && urlErr.Timeout() {
		return true
	}
	var netErr net.Error
	if errorAs(err, &netErr) && netErr.Timeout() {
		return true
	}
	return strings.Contains(err.Error(), "Client.Timeout exceeded")
}

// isRetryableNetworkError reports whether a transport failure is worth
// another attempt. User cancellation and caller deadlines are never
// retried; timeouts, torn connections and reset/aborted connections are,
// because a release-asset PUT is atomic (no partial asset on a dropped
// connection) and range GETs are read-only - the only cost of a spurious
// retry is bandwidth, while refusing to retry turns every capped-link
// stall into a failed commit. Permanent name failures (DNS NXDOMAIN) are
// refused too: no retry resolves a name that does not exist.
//
// The semantic is shared with the storage layer's isRetryableNetworkError;
// keep the two identical (see the IsTimeout contract above).
func isRetryableNetworkError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	// A *url.Error always satisfies net.Error, so the deadline/timeout
	// check must come first: a caller deadline is the caller's decision
	// and must not burn the governor's budget on retries. The timeout
	// verdict itself lives in IsTimeout (single definition).
	if errors.Is(err, context.DeadlineExceeded) {
		return IsTimeout(err)
	}
	var dnsErr *net.DNSError
	if errorAs(err, &dnsErr) && dnsErr.IsNotFound {
		return false
	}
	var netErr net.Error
	if errorAs(err, &netErr) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// Bare syscall unwrapping: a connection reset can surface without a
	// *net.OpError wrapper depending on where the transport fails, and
	// killing the read on one dropped connection is exactly the failure
	// mode retries exist to absorb.
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED)
}

func errorAs(err error, target any) bool {
	return err != nil && errors.As(err, target)
}

// boundedWait caps non-rate-limit server-provided wait hints (a plain
// Retry-After on a 5xx) at maxRetryDelay so a hostile or misconfigured
// header cannot stall callers invisibly for minutes. Rate-limit waits do
// not pass through here: they are honored exactly and refused up front by
// the maxWait ceiling in doRequest.
func (c *Client) boundedWait(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	if c.maxRetryDelay > 0 && d > c.maxRetryDelay {
		return c.maxRetryDelay
	}
	return d
}
