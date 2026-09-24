package github

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// Slot-release broadcast: a parked content admission must wake immediately on
// slot release via broadcast, never via the old 10ms sleep-poll tick.
// Channel-asserted, budget-guarded, no sleeps.
func TestSlotReleaseWakesWaiterImmediately(t *testing.T) {
	t.Parallel()
	cfg := storcfg.Default()
	cfg.RatePointsPerMin = 100000
	cfg.RateContentPerMin = 100000
	cfg.RateMaxWait = time.Hour
	cfg.MaxConcurrentRequests = 4 // content cap = 4 - readSlotReserve(1) = 3
	var tickSleeps atomic.Int64
	g := newRateGovernor(cfg, nil, func(_ context.Context, d time.Duration) error {
		if d == 10*time.Millisecond {
			tickSleeps.Add(1)
		}
		return nil
	})
	ctx := context.Background()
	releases := make([]func(), 0, 3)
	for i := 0; i < 3; i++ {
		rel, err := g.acquireClass(ctx, 1, requestContent)
		if err != nil {
			t.Fatalf("fill slot %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	defer func() {
		for _, rel := range releases {
			rel()
		}
	}()
	started := make(chan struct{})
	type slotResult struct {
		rel func()
		err error
	}
	done := make(chan slotResult, 1)
	go func() {
		close(started)
		rel, err := g.acquireClass(ctx, 1, requestContent)
		done <- slotResult{rel: rel, err: err}
	}()
	<-started
	// Wait until the waiter is provably parked in the slot wait. Spin
	// on Gosched only (no sleeps), deadline-guarded.
	parkDeadline := time.Now().Add(5 * time.Second)
	for {
		g.mu.Lock()
		parked := g.slotWaits
		g.mu.Unlock()
		if parked >= 1 {
			break
		}
		if time.Now().After(parkDeadline) {
			t.Fatal("content waiter never parked in slot wait")
		}
		runtime.Gosched()
	}
	releases[0]()
	releases = releases[1:]
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("parked waiter denied after slot release: %v", res.err)
		}
		if res.rel != nil {
			releases = append(releases, res.rel)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parked waiter not woken by slot release (budget exceeded)")
	}
	if n := tickSleeps.Load(); n != 0 {
		t.Fatalf("slot wait slept %d times on the 10ms tick, must wake via broadcast", n)
	}
}
