package cli

import (
	"context"
	"fmt"
	storcfg "github.com/FarelRA/storhub/internal/config"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// shutdownHub is the SINGLE owner of hub.Shutdown: the one and only
// place that drains the asynchronous metadata writer. Commands must
// never call Shutdown themselves - not rest, not mount, not serve - so
// there is exactly one drain point to reason about. Shutdown is part of
// the hubClient contract, so nothing reachable here can lack it. The
// writer is asynchronous, meaning a CLI mutation that exits without this
// loses data; that is precisely what the released-binary smoke test caught.
// A failed drain is returned, not printed-and-forgotten: a failed commit
// point must change the exit code.
func (a *App) shutdownHub() error {
	if a.hub == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), hubShutdownTimeout)
	defer cancel()
	if err := a.hub.Shutdown(ctx); err != nil {
		return fmt.Errorf("metadata flush failed: %w", err)
	}
	return nil
}

// Hub constructors record the client so Run can always flush it on exit.

// withSignalContext arms SIGINT/SIGTERM handling: a Ctrl+C cancels the
// returned context between units of work instead of killing the process
// mid-loop. Every long-running command shares this one arm point.
func withSignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// mayStillBeMounted renders the shared teardown suffix so mount, serve,
// and the unmount retry loop never drift apart on wording.
func mayStillBeMounted(target string) string {
	return fmt.Sprintf("%s may still be mounted", target)
}

// joinWithin waits for done to close, giving up after timeout. It reports
// whether the join completed. Teardown joins must never be unbounded: a
// wedged FUSE server or listener goroutine would otherwise turn a failed
// unmount into a hung process.
func joinWithin(done <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// unmountWithRetry retries the unmount until it succeeds or its retry budget
// runs out. An unmount fails with EBUSY while any file on the mount is still
// open, so holders get a grace period instead of either hanging forever or
// silently leaking the mount. Only the first failure prints the advisory;
// later attempts are silent (the outcome line always prints), so teardown
// of a busy mount does not spam. A Ctrl+C during the backoff sleep aborts
// the wait early via a signal-scoped context: the callers already consumed
// their signal context to reach teardown, so the loop arms its own SIGINT
// watch (SIGTERM keeps the default kill disposition).
func unmountWithRetry(fsys fuseMount, target string, report io.Writer) {
	sigCtx, sigStop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer sigStop()
	delay := unmountRetryBaseDelay
	deadline := time.Now().Add(unmountRetryBudget)
	for attempt := 1; ; attempt++ {
		err := fsys.Unmount()
		if err == nil {
			if attempt > 1 {
				_, _ = fmt.Fprintf(report, "unmounted %s\n", target)
			}
			return
		}
		if attempt == 1 {
			_, _ = fmt.Fprintf(report, "unmount failed (%v); close programs using %s and wait, or press Ctrl+C again to quit\n", err, target)
		}
		if time.Now().After(deadline) {
			_, _ = fmt.Fprintf(report, "giving up on unmount after %d attempts; %s\n", attempt, mayStillBeMounted(target))
			return
		}
		if err := storcfg.SleepWithContext(sigCtx, delay); err != nil {
			_, _ = fmt.Fprintf(report, "unmount interrupted; %s\n", mayStillBeMounted(target))
			return
		}
		if delay < unmountBackoffCap {
			delay *= 2
		}
	}
}
