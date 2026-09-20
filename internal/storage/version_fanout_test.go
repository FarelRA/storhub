package storage

import (
	"context"
	"testing"
)

// TestProjectVersionAdvancesOnPublish pins the fan-out contract: every
// hub-side mutation that swaps shared truth must advance the per-project
// version counter, so a cross-surface subscriber (FUSE) can observe the
// change without polling content. Unknown projects report ok=false.
func TestProjectVersionAdvancesOnPublish(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	if _, ok := hub.ProjectVersion("fanout-ver"); ok {
		t.Fatal("unknown project must report no version")
	}

	seedMeta(t, hub, "fanout-ver", "docs", "a.txt", 1)
	v1, ok := hub.ProjectVersion("fanout-ver")
	if !ok {
		t.Fatal("known project must report a version")
	}

	seedMeta(t, hub, "fanout-ver", "docs", "b.txt", 2)
	v2, ok := hub.ProjectVersion("fanout-ver")
	if !ok {
		t.Fatal("known project must report a version")
	}
	if v2 <= v1 {
		t.Fatalf("second publish must advance the version: %d -> %d", v1, v2)
	}
}

// TestProjectVersionSurvivesRemoteSwap pins the other half: a remote-truth
// swap on a clean entry (rollback/rebase/load path) must also advance the
// counter, or a subscriber watching it misses the change.
func TestProjectVersionSurvivesRemoteSwap(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "fanout-swap", "docs", "a.txt", 1)

	before, ok := hub.ProjectVersion("fanout-swap")
	if !ok {
		t.Fatal("known project must report a version")
	}

	remote := NewRepoMetadata("fanout-swap")
	remote.EnsureDirectory("docs", 1700000000)
	remote.Normalize("fanout-swap", 1700000000)
	hub.storeRepoMetadata("fanout-swap", remote, "swap-token", nil, 0)

	after, ok := hub.ProjectVersion("fanout-swap")
	if !ok {
		t.Fatal("known project must report a version")
	}
	if after <= before {
		t.Fatalf("remote-truth swap must advance the version: %d -> %d", before, after)
	}
}

func TestPublishedPathsSinceExactScopes(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "fanout-paths"

	if _, unknown, _ := hub.PublishedPathsSince(project, 0); !unknown {
		t.Fatal("unknown project must report unknown scope")
	}
	seedMeta(t, hub, project, "docs", "a.txt", 1)
	// Baseline after seeding: the initial cold load records unknown
	// scope (a fresh load can move any entry), so exactness is asserted
	// only for publishes after the baseline.
	_, _, cur := hub.PublishedPathsSince(project, 0)
	seedMeta(t, hub, project, "docs", "b.txt", 2)
	paths, unknown, cur := hub.PublishedPathsSince(project, cur)
	if unknown {
		t.Fatal("fresh publishes must not report unknown scope")
	}
	found := map[string]bool{}
	for _, p := range paths {
		found[p] = true
	}
	if !found["docs/b.txt"] {
		t.Fatalf("seed publish must list docs/b.txt, got %q", paths)
	}
	again, unknown, cur2 := hub.PublishedPathsSince(project, cur)
	if unknown || len(again) != 0 {
		t.Fatalf("no new publishes: want empty exact window, got %q unknown=%v", again, unknown)
	}
	if cur2 != cur {
		t.Fatalf("cursor must not advance without publishes: %d -> %d", cur, cur2)
	}
}

func TestPublishedPathsSinceOverflowIsUnknown(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "fanout-overflow"
	seedMeta(t, hub, project, "docs", "a.txt", 1)

	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	for i := 0; i < maxRecentPaths+10; i++ {
		notePublishedPathsLocked(pm, []string{"docs/a.txt"})
	}
	pm.mu.Unlock()

	if _, unknown, _ := hub.PublishedPathsSince(project, 0); !unknown {
		t.Fatal("baseline predating the ring must report unknown scope")
	}
}

func TestPublishedPathsSinceSwapIsUnknown(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "fanout-swap-scope"
	seedMeta(t, hub, project, "docs", "a.txt", 1)

	_, _, cur := hub.PublishedPathsSince(project, 0)
	remote := NewRepoMetadata(project)
	remote.EnsureDirectory("docs", 1700000000)
	remote.Normalize(project, 1700000000)
	hub.storeRepoMetadata(project, remote, "swap-token", nil, 0)

	if _, unknown, _ := hub.PublishedPathsSince(project, cur); !unknown {
		t.Fatal("remote-truth swap must report unknown scope")
	}
}

// TestPublishedPathsSinceAdoptSwapStaysExact pins the S1 residual as a
// contract: a version-match commit (no mid-commit mutation) adopts the
// normalized working tree and bumps the version WITHOUT a ring entry.
// The swap carries no new namespace content beyond the mutation's own
// publish (already rung exact), so the window after it must stay exact:
// no unknown scope (a nil entry would force it) and no double-recorded
// paths. A subscriber polling the fan-out cursor therefore misses
// nothing observable, by construction rather than by luck.
func TestPublishedPathsSinceAdoptSwapStaysExact(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "fanout-adopt"

	seedMeta(t, hub, project, "docs", "a.txt", 1)
	_, _, cur := hub.PublishedPathsSince(project, 0)
	seedMeta(t, hub, project, "docs", "b.txt", 2)
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush: %v", err)
	}
	paths, unknown, cur2 := hub.PublishedPathsSince(project, cur)
	if unknown {
		t.Fatal("adopt swap must not force unknown scope")
	}
	seen := map[string]int{}
	for _, p := range paths {
		seen[p]++
	}
	if seen["docs/b.txt"] != 1 {
		t.Fatalf("mutation paths must appear exactly once, got %q", paths)
	}
	again, unknown, _ := hub.PublishedPathsSince(project, cur2)
	if unknown || len(again) != 0 {
		t.Fatalf("window must drain clean, got %q unknown=%v", again, unknown)
	}
}
