package storage

import (
	"context"
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"syscall"
	"time"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

func (h *StorHub) retryDelay(attempt int, apiErr *ghapi.APIError) time.Duration {
	if apiErr != nil {
		// Primary rate-limit waits honor the server's reset window
		// uncapped (pinned doctrine: TestRateLimitAwareRetry) — only
		// generic Retry-After hints are bounded so one bad header
		// cannot stall callers past MaxRetryDelay.
		if apiErr.RetryAfter > 0 {
			return h.boundedWait(apiErr.RetryAfter)
		}
		if apiErr.RateLimited && !apiErr.RateLimitReset.IsZero() {
			return nonNegativeDelay(time.Until(apiErr.RateLimitReset))
		}
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

// boundedWait caps a server-provided wait hint (Retry-After, rate-limit
// reset) at MaxRetryDelay so one bad header cannot stall callers.
func (h *StorHub) boundedWait(d time.Duration) time.Duration {
	d = nonNegativeDelay(d)
	if h.config.MaxRetryDelay > 0 && d > h.config.MaxRetryDelay {
		return h.config.MaxRetryDelay
	}
	return d
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
