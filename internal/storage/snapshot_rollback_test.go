package storage

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// A failed commit snapshot must not poison later coalescing. The rename
// chain A->B, B->C collapses to A->C only when the predecessor postdates
// the last snapshot mark; a snapshot whose publish failed published
// nothing, so its mark has to roll back. Without the rollback the live
// stack stays split while the journal fold merges, breaking fold/stack
// equivalence (TestJournalFoldEquivalenceAcrossRenameChain flake).
func TestFailedSnapshotRollbackKeepsRenameMerge(t *testing.T) {
	t.Parallel()
	var stack opStack
	stack.appendWithDelta(Op{Type: OpRename, Paths: []string{"a.txt", "b.txt"}})
	prev := stack.noteSnapshot(stack.maxSeq())
	stack.rollbackSnapshot(stack.maxSeq(), prev)
	stack.appendWithDelta(Op{Type: OpRename, Paths: []string{"b.txt", "c.txt"}})
	if len(stack.ops) != 1 {
		t.Fatalf("chain must merge after failed-snapshot rollback, got %d ops", len(stack.ops))
	}
	got := stack.ops[0]
	if len(got.Paths) != 2 || got.Paths[0] != "a.txt" || got.Paths[1] != "c.txt" {
		t.Fatalf("merged chain must be a.txt->c.txt, got %+v", got.Paths)
	}
}

// Without the rollback the mark stays and the chain stays split: pins
// the guard the rollback exists to lift.
func TestSnapshotMarkBlocksMergeWithoutRollback(t *testing.T) {
	t.Parallel()
	var stack opStack
	stack.appendWithDelta(Op{Type: OpRename, Paths: []string{"a.txt", "b.txt"}})
	stack.noteSnapshot(stack.maxSeq())
	stack.appendWithDelta(Op{Type: OpRename, Paths: []string{"b.txt", "c.txt"}})
	if len(stack.ops) != 2 {
		t.Fatalf("unrolled snapshot must keep the chain split, got %d ops", len(stack.ops))
	}
}

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
