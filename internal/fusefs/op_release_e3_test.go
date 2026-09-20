package fusefs

// Phase E3 (F4): commit-release broadcast plus Close-during-commit
// acceptance. All cross-goroutine coordination is channel-driven and
// budget-guarded; no sleeps.

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// The broadcast fires exactly once per signal: the generation bumps and
// the previous channel closes while the new one stays open.
func TestOpReleaseBroadcastFiresOnSignal(t *testing.T) {
	t.Parallel()
	// A waiter must be registered: releases skip the channel swap when
	// nobody waits (zero-alloc uncontended path), so the close assertion
	// below only holds with a waiter present.
	opReleaseWaiters.Add(1)
	defer opReleaseWaiters.Add(-1)
	ch0, gen0 := opReleaseWait()
	signalOpRelease()
	ch1, gen1 := opReleaseWait()
	if gen1 != gen0+1 {
		t.Fatalf("generation must bump once per signal, got %d -> %d", gen0, gen1)
	}
	select {
	case <-ch0:
	default:
		t.Fatal("previous broadcast channel was not closed by the signal")
	}
	select {
	case <-ch1:
		t.Fatal("new broadcast channel must stay open until the next signal")
	default:
	}
}

// Without a registered waiter the release skips the channel swap (the
// zero-alloc uncontended path the benchmark budgets gate): the
// generation still bumps, but the channel identity is unchanged and no
// close fires.
func TestSignalOpReleaseSkipsSwapWithoutWaiters(t *testing.T) {
	// No t.Parallel: the assertion needs a waiter-free global. Other
	// tests register briefly, so wait (budget-guarded) for them to drain
	// instead of skipping.
	deadline := time.Now().Add(10 * time.Second)
	for opReleaseWaiters.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("waiter count never drained, got %d", opReleaseWaiters.Load())
		}
		runtime.Gosched()
	}
	ch0, gen0 := opReleaseWait()
	signalOpRelease()
	ch1, gen1 := opReleaseWait()
	if gen1 != gen0+1 {
		t.Fatalf("generation must bump once per signal, got %d -> %d", gen0, gen1)
	}
	select {
	case <-ch0:
		t.Fatal("no waiter: previous channel must not be closed by the signal")
	default:
	}
	select {
	case <-ch1:
		t.Fatal("no waiter: channel must stay open until a waiter registers and a signal fires")
	default:
	}
}

// installParkHook reports every waitOpMuBounded park on the returned
// channel (non-blocking). Only the two hook tests below install it and
// both are sequential (no t.Parallel), so no two installers overlap;
// parallel suite tests never touch the global. The hook is cleared at
// test end.
func installParkHook(t *testing.T) chan struct{} {
	t.Helper()
	parked := make(chan struct{}, 16)
	fn := func() {
		select {
		case parked <- struct{}{}:
		default:
		}
	}
	waitOpMuParkedHook.Store(&fn)
	t.Cleanup(func() { waitOpMuParkedHook.Store(nil) })
	return parked
}

// awaitParked blocks until a waiter parks (budget-guarded, no sleeps).
func awaitParked(t *testing.T, parked chan struct{}) {
	t.Helper()
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("waiter never parked on the broadcast")
	}
}

// A waiter parked in waitOpMuBounded must acquire at commit end (the
// release plus signal), not at the timeout. Deterministic: the parked
// hook proves the waiter is in its select before the fake committer
// releases, so release-before-park luck cannot fake the pass.
func TestWaitOpMuBoundedWakesOnCommitRelease(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	parked := installParkHook(t)
	acquired := make(chan bool, 1)
	go func() { acquired <- waitOpMuBounded(&mu, closeOpMuTimeout) }()
	awaitParked(t, parked)
	mu.Unlock()
	signalOpRelease() // every commit-path opMu release signals
	releasedAt := time.Now()
	select {
	case ok := <-acquired:
		if !ok {
			t.Fatal("waiter failed to acquire a released lock")
		}
		if d := time.Since(releasedAt); d > 2*time.Second {
			t.Fatalf("waiter woke %v after release; must track the signal, not the timeout", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("waiter did not wake on commit release; fell back to the timeout")
	}
}

// The 5s closeOpMuTimeout stays as the loud backstop: a lock nobody
// releases still fails CLOSED (false) at the budget, never parks forever.
func TestWaitOpMuBoundedKeepsLoudBackstop(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	const budget = 100 * time.Millisecond
	start := time.Now()
	if waitOpMuBounded(&mu, budget) {
		t.Fatal("acquired a lock that was never released")
	}
	if d := time.Since(start); d < budget {
		t.Fatalf("backstop returned before its budget: %v < %v", d, budget)
	}
}

// Close blocked on a committer's opMu must return at commit end. The
// test goroutine itself plays the committer: it holds the state lock
// across the window, waits (via the parked hook) until Close is proven
// parked in waitOpMuBounded, then releases plus signals exactly like the
// commit-path unlockOpMu. Deterministic: no release-before-park luck,
// no sleeps, budget-guarded.
func TestCloseCompletesAtCommitEnd(t *testing.T) {
	fsys := newBareFilesystem(nil, "demo", Options{}, t.TempDir(), nil)
	state := &inodeWriteState{fs: fsys, inode: 7, path: "sync.bin"}
	fsys.mu.Lock()
	fsys.writeStates[7] = state
	fsys.mu.Unlock()
	state.opMu.Lock() // fake committer holds opMu across its window
	parked := installParkHook(t)
	closed := make(chan error, 1)
	go func() { closed <- fsys.Close() }()
	awaitParked(t, parked)
	state.opMu.Unlock()
	signalOpRelease()
	releasedAt := time.Now()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
		if d := time.Since(releasedAt); d > 2*time.Second {
			t.Fatalf("close lagged commit end by %v; must track the release, not the timeout", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not complete after commit end")
	}
}
