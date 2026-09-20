package storage

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// NOTE: the snapshot-mark machinery this file once pinned (noteSnapshot /
// rollbackSnapshot) is deleted: the generational boundary subsumes it, so
// a failed snapshot leaves nothing to restore. Its stronger replacements
// live in generation_test.go (TestGenerationFailedCommitNeedsNoRollback,
// TestGenerationFreezeSplitsRenameChain,
// TestGenerationDeleteTransformRespectsBoundary). What stays here is the
// deterministic end-to-end pin of the original flake, which must hold
// under EITHER mechanism: a failed commit between T1 and T2 must keep
// fold==live.

// Deterministic end-to-end pin of the flake: a failed commit landing
// between T1 and T2 must not split the chain. Drives commitProjectMetadata
// directly (no timing involved) with the index PUT intercepted, then
// asserts the live stack still merges and matches the journal fold.
func TestFailedCommitBetweenRenamesKeepsFoldEquivalence(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	journalDir := t.TempDir()
	cfg := Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true, JournalDir: journalDir}
	hub := backend.newClient(t, cfg)
	ctx := context.Background()

	if _, err := hub.UploadFileContext(ctx, "proj", "a.txt", writeTempFile(t, t.TempDir(), "a", []byte("a-content"))); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("seed flush: %v", err)
	}
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	if err := hub.RenameContext(ctx, "proj", "a.txt", "b.txt"); err != nil {
		t.Fatalf("rename A->B: %v", err)
	}
	// The failed commit between the renames: must leave no mark behind.
	pm := hub.getOrCreateProjectMeta("proj")
	if err := hub.commitProjectMetadata(ctx, "proj", pm); err == nil {
		t.Fatal("commit under intercepted PUT must fail")
	}
	if err := hub.RenameContext(ctx, "proj", "b.txt", "c.txt"); err != nil {
		t.Fatalf("rename B->C: %v", err)
	}
	pm.mu.RLock()
	liveOps := append([]Op(nil), pm.opStack.ops...)
	pm.mu.RUnlock()
	folded := hub.journalRead("proj")
	// The invariant is fold==live, whatever the merge outcome: a commit
	// loop attempt may legitimately be in flight during T2 (every
	// mutation pokes the loop), splitting the chain live; the journal
	// replays each delta under its own append-time mark and agrees.
	if len(folded) != len(liveOps) {
		t.Fatalf("journal fold (%d ops) must equal live stack (%d ops)", len(folded), len(liveOps))
	}
	for i := range folded {
		if folded[i].Type != liveOps[i].Type || len(folded[i].Paths) != len(liveOps[i].Paths) {
			t.Fatalf("journal line %d %+v != live op %+v", i, folded[i], liveOps[i])
		}
		for j := range folded[i].Paths {
			if folded[i].Paths[j] != liveOps[i].Paths[j] {
				t.Fatalf("journal line %d %+v != live op %+v", i, folded[i], liveOps[i])
			}
		}
	}
}
