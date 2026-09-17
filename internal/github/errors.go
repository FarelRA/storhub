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
//
// Empty code and/or field act as wildcards, giving full 3x3 matrix
// semantics on the SAME detail entry: code "" matches any code,
// field "" matches any field. ("already_exists","") narrows by code
// only; ("","file_count") narrows by field only (the storage
// file-count path calls with an empty code); ("","") matches any 422
// that carries at least one detail. A 422 with zero details never
// matches, and non-422 statuses never match.
func (e *APIError) IsValidationIssue(code, field string) bool {
	if e == nil || e.StatusCode != http.StatusUnprocessableEntity {
		return false
	}
	for _, d := range e.Details {
		if (code == "" || strings.EqualFold(d.Code, code)) && (field == "" || strings.EqualFold(d.Field, field)) {
			return true
		}
	}
	return false
}

// IsSHANotSupplied matches the contents-API 422 for a sha-less create onto
// an existing path: `Invalid request. "sha" wasn't supplied.` GitHub sends
// NO structured errors[] entry for this case (unlike already_exists), so
// the status + exact message sentence IS the structural signature —
// matched on the full quoted sentence, never a bare keyword. A concurrent
// writer having created the path first is success for idempotent writes:
// callers verify upstream content and adopt it.
func (e *APIError) IsSHANotSupplied() bool {
	return e != nil && e.StatusCode == http.StatusUnprocessableEntity &&
		strings.Contains(e.Message, `"sha" wasn't supplied`)
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

// IsRetryable reports whether the failed request is worth another
// attempt after backoff.
//
// Uniform retry doctrine (shared with CDNError.Transient, which uses the
// same 5xx range):
//   - 429 and any RateLimited rejection: retryable. Rate waits are honored
//     exactly; the maxWait ceiling refuses excessive waits upstream.
//   - 5xx (500-599, all of them): retryable transient server trouble.
//     Earlier code listed only 500/502/503/504, which made 501/505-511
//     terminal on the API path while retryable on the CDN path.
//   - 403: retryable ONLY when classified RateLimited (secondary-limit
//     prose); a bare 403 is a permission denial and is terminal.
//   - 404/409/422/400/401 and everything else: terminal. The caller must
//     resolve the condition (re-read, rebase, rename) instead of
//     re-sending the same doomed request.
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
	if e.StatusCode >= 500 && e.StatusCode <= 599 {
		return true
	}
	switch e.StatusCode {
	case http.StatusForbidden:
		// Reached only when !RateLimited (handled above): a bare 403 is
		// a permission denial, never a throttle.
		return e.RateLimited
	default:
		return false
	}
}
