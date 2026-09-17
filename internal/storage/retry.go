package storage

import (
	"context"
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"strings"
	"syscall"
	"time"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

func (h *StorHub) retryDelay(attempt int, apiErr *ghapi.APIError) time.Duration {
	if apiErr != nil && apiErr.RateLimited {
		// Mirror the github client's branches (client.go:1125-1145):
		// a rate-limited reset is honored EXACTLY (uncapped) — the
		// server dictates the resume instant, so jitter/caps only
		// overshoot it — as is a rate-limited Retry-After. Only
		// non-rate-limit Retry-After hints are bounded by
		// MaxRetryDelay. This purposefully diverges from the old
		// storage behavior that capped rate-limit waits: truncating
		// them manufactures repeat rejections and burns the point
		// window. The multiplicative bulk-read/purge shape (audit 33)
		// is bounded by design and ctx-cancellable; the governor's
		// maxWait ceiling (not MaxRetryDelay) is what refuses an
		// excessive wait.
		if !apiErr.RateLimitReset.IsZero() {
			return nonNegativeDelay(time.Until(apiErr.RateLimitReset))
		}
		if apiErr.RetryAfter > 0 {
			return nonNegativeDelay(apiErr.RetryAfter)
		}
		if attempt > 10 {
			attempt = 10 // keep the shift below from overflowing on wild input
		}
		return addJitter(minDuration(60*time.Second<<attempt, 15*time.Minute))
	}
	if apiErr != nil && apiErr.RetryAfter > 0 {
		return h.boundedWait(apiErr.RetryAfter)
	}
	base := float64(h.config.BaseRetryDelay)
	delay := time.Duration(base * math.Pow(2, float64(attempt)))
	if delay > h.config.MaxRetryDelay {
		delay = h.config.MaxRetryDelay
	}
	if delay <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int63n(int64(delay/4 + 1)))
	return delay + jitter
}

// boundedWait caps a NON-rate-limit server-provided wait hint
// (Retry-After) at MaxRetryDelay so one bad header cannot stall callers.
// Rate-limit waits (RateLimitReset, rate-limited Retry-After) are
// intentionally NOT capped here — see retryDelay: they are honored
// exactly per the purge contract ("always honor the advertised window").
func (h *StorHub) boundedWait(d time.Duration) time.Duration {
	d = nonNegativeDelay(d)
	if h.config.MaxRetryDelay > 0 && d > h.config.MaxRetryDelay {
		return h.config.MaxRetryDelay
	}
	return d
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func addJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int63n(int64(d/4 + 1)))
	return d + jitter
}

// withRetry is the single home of the backoff/sleep retry shape. It
// replaces the three duplicated loops (purgeRetry in cleanup.go,
// downloadChunkWithRetry and withAssetRangeReader in transfer.go /
// workflows.go): same sleep-via-config, same retryDelay, different caps
// supplied by the caller. isRetryable decides per-error; maxAttempts is
// the total attempt count (including the first try). APIErrors sleep via
// retryDelay; non-API retryable errors use exponential backoff.
func (h *StorHub) withRetry(ctx context.Context, op string, maxAttempts int, isRetryable func(error) bool, fn func() error) error {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !isRetryable(err) || attempt == maxAttempts-1 {
			return err
		}
		delay := h.retryDelay(attempt, extractAPIError(err))
		if delay < 0 {
			delay = 0
		}
		h.debugf("%s retry project op=%s attempt=%d delay=%s err=%v", op, op, attempt+1, delay, err)
		if sleepErr := h.config.Sleep(ctx, delay); sleepErr != nil {
			return sleepErr
		}
		lastErr = err
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("retry exhausted for " + op)
}

func nonNegativeDelay(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	return delay
}

// isRetryableNetworkError reports whether a transport failure is worth
// another attempt. User cancellation is never retried; everything else
// transport-shaped (timeouts, torn connections, truncated reads) is.
//
// The semantic is shared with the github layer's isRetryableNetworkError;
// keep the two identical.
func isRetryableNetworkError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	// A *url.Error always satisfies net.Error, so the deadline check must
	// come first: a caller deadline is the caller's decision and must not
	// burn retries. http.Client's own timeout surfaces as the same wrapped
	// context.DeadlineExceeded but carries the "Client.Timeout exceeded"
	// marker - a stalled transfer is exactly what retries exist to absorb.
	if errors.Is(err, context.DeadlineExceeded) {
		return strings.Contains(err.Error(), "Client.Timeout exceeded")
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
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
