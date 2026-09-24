package github

import (
	"context"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// Read-lane reservation: reserved read share lets interactive reads
// proceed under bulk load.
func TestReadLaneReservation(t *testing.T) {
	t.Parallel()
	cfg := storcfg.Default()
	cfg.RatePointsPerMin = 10
	cfg.RateContentPerMin = 100
	cfg.RateMaxWait = time.Hour
	cfg.MaxConcurrentRequests = 16
	g := newRateGovernor(cfg, nil, func(_ context.Context, _ time.Duration) error { return nil })
	ctx := context.Background()
	// Bulk content burst consumes the content-visible share (10 minus
	// reserve). With points=10 the reserve is 1, so content caps at 9.
	releases := []func(){}
	for i := 0; i < 9; i++ {
		rel, err := g.acquireClass(ctx, 1, requestContent)
		if err != nil {
			t.Fatalf("bulk content %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	// A tenth content request must wait (share exhausted).
	h := &govHarness{g: g, t: g.now()}
	_ = h
	wait, apiErr := g.reserve(1, requestContent)
	if apiErr != nil {
		t.Fatalf("tenth content reserve denied unexpectedly: %v", apiErr)
	}
	if wait == 0 {
		t.Fatalf("tenth bulk content must wait past the reserved read share")
	}
	// An interactive read at the same ledger state must still admit.
	wait, apiErr = g.reserve(1, requestRead)
	if apiErr != nil {
		t.Fatalf("interactive read denied under bulk load: %v", apiErr)
	}
	if wait != 0 {
		t.Fatalf("interactive read must proceed under bulk load, wait=%v", wait)
	}
	for _, rel := range releases {
		rel()
	}
	rw, cw, _, _ := g.GovernorStats()
	_ = rw
	_ = cw
}
