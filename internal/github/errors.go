package github

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

type APIError struct {
	StatusCode     int
	Message        string
	Body           string
	Headers        http.Header
	RetryAfter     time.Duration
	RateLimitReset time.Time
	RateLimited    bool
	// Primary marks a primary rate-limit rejection: the hourly request
	// budget is exhausted (x-ratelimit-remaining: 0) and the only honest
	// recovery is waiting for x-ratelimit-reset. Secondary limits carry
	// their own shorter waits.
	Primary bool
}

func (e *APIError) Error() string {
	message := e.Message
	if message == "" {
		message = strings.TrimSpace(e.Body)
	}
	if message == "" {
		message = http.StatusText(e.StatusCode)
	}
	return fmt.Sprintf("github API error (%d): %s", e.StatusCode, message)
}

func (e *APIError) NotFound() bool { return e != nil && e.StatusCode == http.StatusNotFound }

// CDNError reports a failed range fetch against a signed asset URL. The
// status is carried structurally so callers can distinguish transient
// server trouble (retryable) from permanent conditions without parsing
// error strings.
type CDNError struct {
	StatusCode int
}

func (e *CDNError) Error() string {
	return fmt.Sprintf("cdn range fetch: unexpected status %d", e.StatusCode)
}

// Transient reports whether the CDN rejection is worth retrying: server
// trouble and throttling clear on their own, while 4xx client errors
// (expired signature aside, which triggers re-resolution instead) will
// repeat identically.
func (e *CDNError) Transient() bool {
	return e != nil && (e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500)
}

// IsPrimaryRateLimit reports whether err is a confirmed primary rate-limit
// rejection: the hourly budget is gone and the request must wait for the
// documented reset time instead of being retried on backoff.
func IsPrimaryRateLimit(err error) (*APIError, bool) {
	apiErr, ok := err.(*APIError)
	if !ok || apiErr == nil || !apiErr.RateLimited || !apiErr.Primary {
		return nil, false
	}
	return apiErr, true
}

func (e *APIError) IsRetryable() bool {
	if e == nil {
		return false
	}
	if e.RateLimited {
		return true
	}
	switch e.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	case http.StatusForbidden:
		return e.RateLimited
	case http.StatusInternalServerError:
		return true
	default:
		return false
	}
}
