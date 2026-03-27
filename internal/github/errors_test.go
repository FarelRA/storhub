package github

import (
	"net/http"
	"testing"
	"time"
)

func TestAPIErrorErrorFormattingAndHelpers(t *testing.T) {
	tests := []struct {
		name string
		err  *APIError
		want string
	}{
		{name: "message wins", err: &APIError{StatusCode: http.StatusBadGateway, Message: "upstream"}, want: "github API error (502): upstream"},
		{name: "body fallback", err: &APIError{StatusCode: http.StatusForbidden, Body: " rate limited \n"}, want: "github API error (403): rate limited"},
		{name: "status fallback", err: &APIError{StatusCode: http.StatusNotFound}, want: "github API error (404): Not Found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("unexpected error string: %q", got)
			}
		})
	}
	if (&APIError{StatusCode: http.StatusNotFound}).NotFound() != true {
		t.Fatal("expected 404 helper to report not found")
	}
	if (*APIError)(nil).NotFound() {
		t.Fatal("expected nil NotFound to be false")
	}
}

func TestAPIErrorRetryability(t *testing.T) {
	reset := time.Unix(10, 0)
	tests := []struct {
		name string
		err  *APIError
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "rate limited flag", err: &APIError{StatusCode: http.StatusForbidden, RateLimited: true, RetryAfter: time.Second, RateLimitReset: reset}, want: true},
		{name: "too many requests", err: &APIError{StatusCode: http.StatusTooManyRequests}, want: true},
		{name: "bad gateway", err: &APIError{StatusCode: http.StatusBadGateway}, want: true},
		{name: "service unavailable", err: &APIError{StatusCode: http.StatusServiceUnavailable}, want: true},
		{name: "gateway timeout", err: &APIError{StatusCode: http.StatusGatewayTimeout}, want: true},
		{name: "internal server error", err: &APIError{StatusCode: http.StatusInternalServerError}, want: true},
		{name: "forbidden without rate limit", err: &APIError{StatusCode: http.StatusForbidden}, want: false},
		{name: "other status", err: &APIError{StatusCode: http.StatusBadRequest}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.IsRetryable(); got != tt.want {
				t.Fatalf("unexpected retryable value: %v", got)
			}
		})
	}
}
