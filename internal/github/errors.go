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
	// Details carries GitHub's structured errors[] array verbatim. It is
	// the only honest basis for fine-grained classification (e.g. 422
	// "already_exists" vs capacity "custom"/file_count): matching message
	// prose with bare substrings ("1000", "too many") false-positives on
	// unrelated variants and false-negatives on rewordings.
	Details []APIErrorDetail
}

// APIErrorDetail is one entry of GitHub's structured errors[] payload:
// a machine-readable (resource, code, field) triple that survives
// message rewording.
type APIErrorDetail struct {
	Resource string `json:"resource"`
	Code     string `json:"code"`
	Field    string `json:"field"`
	Message  string `json:"message"`
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

// IsValidationIssue matches status + structured code: a 422 whose
// errors[] array carries the given code (case-insensitive), narrowed by
// field when field != "". Prose-only bodies never match, so message
// variants ("file_count limited to 1000...", "too many ...") classify
// only through the code GitHub actually assigned.
func (e *APIError) IsValidationIssue(code, field string) bool {
	if e == nil || e.StatusCode != http.StatusUnprocessableEntity {
		return false
	}
	for _, d := range e.Details {
		if strings.EqualFold(d.Code, code) && (field == "" || strings.EqualFold(d.Field, field)) {
			return true
		}
	}
	return false
}

// StatusSignedURLExpired is GitHub's non-standard status from
// release-assets.githubusercontent.com meaning the front-door JWT on a signed
// asset URL has expired ("618 jwt:expired"). It is a credential event, not
// server trouble: the correct recovery is to re-resolve a fresh signed URL
// (see isCDNRejection), never to blind-retry the same dead URL.
const StatusSignedURLExpired = 618

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

// Transient reports whether the CDN rejection is worth another attempt after
// backoff. Throttling and genuine 5xx server trouble clear on their own. A 618
// is recoverable only by re-resolution, which the transport layer already
// performs; if one still escapes (a freshly minted URL rejected at once, e.g.
// clock skew), a backed-off retry is a reasonable last resort. Other 4xx and
// unknown 6xx statuses are terminal.
func (e *CDNError) Transient() bool {
	if e == nil {
		return false
	}
	switch e.StatusCode {
	case http.StatusTooManyRequests, StatusSignedURLExpired:
		return true
	default:
		return e.StatusCode >= 500 && e.StatusCode <= 599
	}
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
