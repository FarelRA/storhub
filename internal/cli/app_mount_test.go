package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	shlog "github.com/FarelRA/storhub/internal/logging"
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

func TestRedactRequestURIRedactsTokens(t *testing.T) {
	// The CLI no longer wraps the REST handler in its own logging
	// middleware (single log layer: rest.requestLogging); this pins the
	// surviving redaction helper the request logs flow through instead.
	got := shlog.RedactRequestURI("/shares/sigcapabilitytoken/download?path=/a.txt&sig=secret")
	if strings.Contains(got, "sigcapabilitytoken") || strings.Contains(got, "secret") {
		t.Fatalf("credentials leaked into redacted URI: %q", got)
	}
	if !strings.Contains(got, "path=/a.txt") && !strings.Contains(got, "path=%2Fa.txt") {
		t.Fatalf("safe query value lost: %q", got)
	}
}
