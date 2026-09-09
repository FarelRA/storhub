package github

import (
	"errors"
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

// BodySnippet returns up to the first kilobyte of the response body for
// logs: enough to carry GitHub's errors[] detail (e.g. file_count), small
// enough to keep failure logs readable. The v18 incident cost a manual
// reconstruction precisely because only Message ("Validation Failed")
// was logged while the signal sat in Body.
func (e *APIError) BodySnippet() string {
	if e == nil || e.Body == "" {
		return ""
	}
	if len(e.Body) > 1024 {
		return e.Body[:1024] + "..."
	}
	return e.Body
}

// uploadErrorBody extracts the response-body snippet from a (possibly
// wrapped) upload failure for logs. Returns "" for non-API errors.
func uploadErrorBody(err error) string {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return ""
	}
	return apiErr.BodySnippet()
}

func (e *APIError) IsRetryable() bool {
	if e == nil {
		return false
	}
	if e.StatusCode == http.StatusTooManyRequests {
		return true
	}
	if e.RateLimited {
		return true
	}
	switch e.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	case http.StatusForbidden:
		return e.RateLimited
	case http.StatusInternalServerError:
		return true
	default:
		return false
	}
}
