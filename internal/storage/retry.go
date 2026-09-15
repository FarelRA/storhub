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
