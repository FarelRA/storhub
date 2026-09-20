package storage

import (
	"context"
	"testing"
	"time"
)

// Zero-value hubs (as stubbed by CLI seam tests) cannot serve publish
// subscriptions. Construction-time subscribe must degrade to a no-op
// instead of panicking, with timeout expiry as the fallback.
func TestSubscribeProjectPublishesZeroHubDegrades(t *testing.T) {
	t.Parallel()
	var hub StorHub
	cursor, unsubscribe := hub.SubscribeProjectPublishes("demo", func() {
		t.Error("degraded subscription must never poke")
	})
	if cursor != 0 {
		t.Fatalf("degraded cursor: want 0, got %d", cursor)
	}
	unsubscribe()
	unsubscribe()
}

// Shutdown on a zero-value hub must skip the manual sweep (nothing
// resident) instead of panicking, matching the subscribe degradation.
func TestShutdownZeroHubSkipsSweep(t *testing.T) {
	t.Parallel()
	var hub StorHub
	if err := hub.Shutdown(context.Background()); err != nil {
		t.Fatalf("zero-hub shutdown: %v", err)
	}
}

// Shutdown runs the manual sweep: a clean entry idle past the TTL is
// evicted during shutdown, so process exit does not retain untouched
// projects. Deterministic via the injected clock, no sleeps.
func TestShutdownSweepsIdleEntries(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	base := time.Unix(1700000000, 0).UTC()
	now := base
	cfg := singleChunkTestConfig()
	cfg.Now = func() time.Time { return now }
	hub := backend.newClient(t, cfg)

	old := hub.getOrCreateProjectMeta("shutdown-victim")
	now = base.Add(2 * metaCacheIdleTTL)
	if err := hub.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	old.mu.RLock()
	stopped := old.stopped
	old.mu.RUnlock()
	if !stopped {
		t.Fatal("shutdown sweep must evict the idle-past-TTL entry")
	}
	hub.metaMu.RLock()
	_, resident := hub.metaCache["shutdown-victim"]
	hub.metaMu.RUnlock()
	if resident {
		t.Fatal("shutdown sweep must not leave the idle entry resident")
	}
}
