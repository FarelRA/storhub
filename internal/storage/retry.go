package storage

import (
	"errors"
	"io"
	"math"
	"math/rand"
	"net"
	"time"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

var ErrProjectNotFound = errors.New("project not found")

func (h *StorHub) retryDelay(attempt int, apiErr *ghapi.APIError) time.Duration {
	if apiErr != nil {
		if apiErr.RetryAfter > 0 {
			return nonNegativeDelay(apiErr.RetryAfter)
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

func nonNegativeDelay(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	return delay
}

func isRetryableNetworkError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF)
}
