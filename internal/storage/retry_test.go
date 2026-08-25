package storage

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

func TestIsRetryableDownloadError(t *testing.T) {
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
