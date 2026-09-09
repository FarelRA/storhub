package github

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
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

func TestBodySnippetCarriesFileCountDetail(t *testing.T) {
	full := `{"message":"Validation Failed","errors":[{"resource":"ReleaseAsset","code":"custom","field":"file_count","message":"file_count limited to 1000 assets per release"}]}`
	err := &APIError{StatusCode: http.StatusUnprocessableEntity, Message: "Validation Failed", Body: full}
	if got := err.BodySnippet(); got != full {
		t.Fatalf("short body must pass through, got %q", got)
	}
	if got := uploadErrorBody(fmt.Errorf("upload asset: %w", err)); !strings.Contains(got, "file_count") {
		t.Fatalf("wrapped upload error must expose file_count, got %q", got)
	}
	long := strings.Repeat("x", 2000)
	if got := (&APIError{StatusCode: http.StatusBadRequest, Body: long}).BodySnippet(); len(got) != 1027 || !strings.HasSuffix(got, "...") {
		t.Fatalf("long body must truncate to 1024+marker, got len %d", len(got))
	}
	if got := uploadErrorBody(errors.New("boom")); got != "" {
		t.Fatalf("non-API error must yield empty snippet, got %q", got)
	}
	var nilErr *APIError
	if got := nilErr.BodySnippet(); got != "" {
		t.Fatalf("nil error must yield empty snippet, got %q", got)
	}
}
