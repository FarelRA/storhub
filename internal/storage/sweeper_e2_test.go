package storage

import (
	"fmt"
	"testing"
	"time"
)

// TestOverCapAppendPokesCommitTrigger pins the Phase E2 force-retry leg:
// the mutation that pushes the pending op stack over maxPendingOpsPerProject
// pokes the live commit trigger synchronously under pm.mu. No sweeper tick,
// no sleep: the trigger lands (or not) before appendOpLocked returns.
//
// The pm is detached (no commit loop consumes the channel), so a plain
// channel assert observes the poke deterministically.
func TestOverCapAppendPokesCommitTrigger(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())
	project := "projectovercappoke"

	pm := &projectMetadata{
		meta:      NewRepoMetadata(project),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
		triggerCh: make(chan struct{}, 1),
		hydrated:  true,
	}

	// Pre-fill to one below the cap in memory (appendOpLocked would pay
	// one journal write per op; only the crossing append must go through
	// the real admission path). Distinct paths defeat coalescing, so the
	// count is exact.
	for i := 0; i < maxPendingOpsPerProject-1; i++ {
		pm.opStack.append(Op{
			Type:      OpMkdir,
			Paths:     []string{fmt.Sprintf("dir-%d", i)},
			Cause:     "e2-prefill",
			Timestamp: 1700000000,
		})
	}
	if got := len(pm.opStack.ops); got != maxPendingOpsPerProject-1 {
		t.Fatalf("prefill landed %d ops, want %d", got, maxPendingOpsPerProject-1)
	}
	select {
	case <-pm.triggerCh:
		t.Fatal("trigger already armed before the cap crossing")
	default:
	}

	// The crossing append runs under pm.mu exactly like every prod caller.
	pm.mu.Lock()
	hub.appendOpLocked(project, pm, Op{
		Type:      OpMkdir,
		Paths:     []string{"dir-crossing"},
		Cause:     "e2-crossing",
		Timestamp: 1700000000,
	})
	pm.mu.Unlock()

	select {
	case <-pm.triggerCh:
	default:
		t.Fatalf("crossing the op cap (%d ops) did not poke the commit trigger; the over-cap retry would wait on a sweeper tick that no longer exists", maxPendingOpsPerProject)
	}
}

// TestIdleExpiredGetPathEvictsCleanEntry pins the Phase E2 idle leg: a
// get-path hit on a clean entry idle past metaCacheIdleTTL evicts it and
// inserts a fresh one, with no timer involved. Touched and dirty entries
// are served live. Deterministic via the injected clock, no sleeps.
func TestIdleExpiredGetPathEvictsCleanEntry(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	base := time.Unix(1700000000, 0).UTC()
	now := base
	cfg := singleChunkTestConfig()
	cfg.Now = func() time.Time { return now }
	hub := backend.newClient(t, cfg)

	old := hub.getOrCreateProjectMeta("idle-victim")

	// A hit inside the TTL serves the live entry and bumps the stamp.
	now = base.Add(metaCacheIdleTTL - time.Minute)
	if got := hub.getOrCreateProjectMeta("idle-victim"); got != old {
		t.Fatal("get inside the TTL must serve the live entry")
	}

	// Past the TTL the clean entry is evicted and a fresh one inserted.
	// (The in-TTL hit above refreshed the stamp, so the clock moves a
	// full TTL past that touch.)
	now = base.Add(2 * metaCacheIdleTTL)
	got := hub.getOrCreateProjectMeta("idle-victim")
	if got == old {
		t.Fatal("idle-past-TTL get must evict the clean entry and insert fresh")
	}
	old.mu.RLock()
	stopped := old.stopped
	old.mu.RUnlock()
	if !stopped {
		t.Fatal("evicted idle entry must be marked stopped")
	}
	hub.metaMu.RLock()
	resident := hub.metaCache["idle-victim"]
	hub.metaMu.RUnlock()
	if resident != got {
		t.Fatal("cache must hold the fresh entry after idle eviction")
	}

	// A dirty entry survives idle expiry: eviction never drops unpushed work.
	got.mu.Lock()
	markProjectDirtyLocked(got)
	got.mu.Unlock()
	now = now.Add(metaCacheIdleTTL + time.Minute)
	if again := hub.getOrCreateProjectMeta("idle-victim"); again != got {
		t.Fatal("dirty entry must survive idle expiry")
	}
}
