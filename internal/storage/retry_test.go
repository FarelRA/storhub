package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

func TestIsRetryableDownloadError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"cdn throttle", &ghapi.CDNError{StatusCode: 429}, true},
		{"cdn server error", &ghapi.CDNError{StatusCode: 503}, true},
		{"cdn bad gateway", &ghapi.CDNError{StatusCode: 502}, true},
		{"cdn forbidden", &ghapi.CDNError{StatusCode: 403}, false},
		{"cdn not found", &ghapi.CDNError{StatusCode: 404}, false},
		{"cdn signed url expired", &ghapi.CDNError{StatusCode: ghapi.StatusSignedURLExpired}, true},
		{"cdn unknown 6xx is terminal", &ghapi.CDNError{StatusCode: 600}, false},
		{"wrapped cdn transient", fmt.Errorf("download asset 5: %w", &ghapi.CDNError{StatusCode: 500}), true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"connection reset", syscall.ECONNRESET, true},
		{"op-wrapped reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, true},
		{"connection aborted", syscall.ECONNABORTED, true},
		{"not found api", &ghapi.APIError{StatusCode: 404}, false},
		{"plain error", errors.New("asset range read exhausted retries"), false},
	}
	for _, tc := range tests {
		if got := isRetryableDownloadError(tc.err); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIsRetryableNetworkErrorTimeoutClassification(t *testing.T) {
	t.Parallel()
	stalled := fmt.Errorf("stalled: %w (Client.Timeout exceeded while awaiting headers)", context.DeadlineExceeded)
	bare := fmt.Errorf("slow: %w", context.DeadlineExceeded)
	canceled := fmt.Errorf("gone: %w", context.Canceled)
	canceledStalled := fmt.Errorf("race: %w: %w (Client.Timeout exceeded)", context.Canceled, context.DeadlineExceeded)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"client timeout marker retries", stalled, true},
		{"bare caller deadline never retries", bare, false},
		{"canceled never retries", canceled, false},
		{"canceled wins over timeout marker", canceledStalled, false},
	}
	for _, tc := range tests {
		if got := isRetryableNetworkError(tc.err); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
