package fusefs

import (
	"sync/atomic"
	"testing"
	"time"
)

// A recovered upcall panic must not silently drop its invalidation: the
// pending mark clears before fn runs, so without a re-drive the kernel
// keeps the stale entry until timeout (the fan-out flake mechanism).
func TestNotifyAsyncRetriesPanickingDelivery(t *testing.T) {
	fs := newTestFilesystem()
	var calls atomic.Int32
	done := make(chan struct{})
	fs.notifyAsync(notifyKey{kind: notifyKindEntry, name: "f"}, "TestNotify", func() {
		if calls.Add(1) == 1 {
			panic("simulated upcall panic")
		}
		close(done)
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("panicking notification was never re-driven")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("delivery attempts: want 2, got %d", got)
	}
}
