package storage

import (
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
