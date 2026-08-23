package cli

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type flakyMount struct {
	failures int
	calls    int
}

func (f *flakyMount) Mount(string) error { return nil }
func (f *flakyMount) Unmount() error {
	f.calls++
	if f.calls <= f.failures {
		return errors.New("device busy")
	}
	return nil
}
func (f *flakyMount) Wait()        {}
func (f *flakyMount) Close() error { return nil }

func TestUnmountWithRetryRecoversFromBusy(t *testing.T) {
	oldDelay, oldBudget := unmountRetryBaseDelay, unmountRetryBudget
	unmountRetryBaseDelay = time.Millisecond
	unmountRetryBudget = 5 * time.Second
	t.Cleanup(func() { unmountRetryBaseDelay, unmountRetryBudget = oldDelay, oldBudget })

	var report bytes.Buffer
	mount := &flakyMount{failures: 2}
	unmountWithRetry(mount, "/tmp/mnt", &report)
	if mount.calls != 3 {
		t.Fatalf("expected 3 unmount attempts, got %d", mount.calls)
	}
	out := report.String()
	if !strings.Contains(out, "unmount failed") || !strings.Contains(out, "unmounted /tmp/mnt") {
		t.Fatalf("missing retry narrative: %q", out)
	}
}

func TestUnmountWithRetryGivesUpAfterBudget(t *testing.T) {
	oldDelay, oldBudget := unmountRetryBaseDelay, unmountRetryBudget
	unmountRetryBaseDelay = time.Millisecond
	unmountRetryBudget = 15 * time.Millisecond
	t.Cleanup(func() { unmountRetryBaseDelay, unmountRetryBudget = oldDelay, oldBudget })

	var report bytes.Buffer
	mount := &flakyMount{failures: 1 << 30}
	unmountWithRetry(mount, "/tmp/mnt", &report)
	if !strings.Contains(report.String(), "giving up") {
		t.Fatalf("expected give-up message, got %q", report.String())
	}
}

func TestLoggingMiddlewareRedactsTokens(t *testing.T) {
	app, _, stderr := newTestApp(t)
	handler := app.loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest("GET", "/shares/sig-capability-token/download?path=/a.txt&sig=secret", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	logs := stderr()
	if strings.Contains(logs, "sig-capability-token") || strings.Contains(logs, "secret") {
		t.Fatalf("credentials leaked into logs: %q", logs)
	}
	if !strings.Contains(logs, "path=/a.txt") && !strings.Contains(logs, "path=%2Fa.txt") {
		t.Fatalf("safe query value lost: %q", logs)
	}
}
