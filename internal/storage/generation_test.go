package storage

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// Generational op stack (JBD2 shape): a commit snapshot freezes the open
// generation and publishes exactly it; appends after the freeze land in a
// strictly newer generation and never merge across the boundary. The old
// Seq-comparison guards (lastSnapshotSeq), noteSnapshot/rollbackSnapshot,
// and per-delta SnapSeq replay are gone; these tests pin the replacement
// invariant: merge iff same generation, fold==live by construction.

// A freeze between the two renames keeps the chain split: the commit
// publishes A->B, so B->C must survive as its own op for the next replay.
func TestGenerationFreezeSplitsRenameChain(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	_, frozen := stack.freeze()
	stack.append(Op{
		Type: OpRename, Paths: []string{"b.txt", "c.txt"}, Cause: "mv",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	if len(stack.ops) != 2 {
		t.Fatalf("chain must stay split across the generation boundary, got %+v", stack.ops)
	}
	if stack.ops[0].Gen != frozen || stack.ops[1].Gen != frozen+1 {
		t.Fatalf("gens must be %d then %d, got %d then %d", frozen, frozen+1, stack.ops[0].Gen, stack.ops[1].Gen)
	}

	// Replay onto the committed tree (file at B) must end at C with no B.
	meta := newTestMeta("p")
	meta.UpsertFile("b.txt", file, 100)
	if err := applyOps(meta, stack.ops); err != nil {
		t.Fatalf("applyOps: %v", err)
	}
	if meta.FindFile("b.txt") != nil {
		t.Fatal("phantom intermediate path b.txt survived replay")
	}
	if meta.FindFile("c.txt") == nil {
		t.Fatal("expected final path c.txt after replay")
	}
}

// Same-generation chains still collapse: the boundary only blocks merges
// across it, never within the open generation.
func TestGenerationSameGenChainStillMerges(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	stack.append(Op{
		Type: OpRename, Paths: []string{"b.txt", "c.txt"}, Cause: "mv",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	if len(stack.ops) != 1 {
		t.Fatalf("same-generation chain must merge, got %+v", stack.ops)
	}
	if got := stack.ops[0].Paths; len(got) != 2 || got[0] != "a.txt" || got[1] != "c.txt" {
		t.Fatalf("merged chain must be a.txt->c.txt, got %+v", got)
	}
}

// Rename-then-delete honors the boundary: with A->B frozen, deleting B
// survives as "delete B" for the next replay, not "delete A".
func TestGenerationDeleteTransformRespectsBoundary(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	stack.freeze()
	stack.append(Op{Type: OpDeleteFile, Paths: []string{"b.txt"}, Cause: "unlink", Timestamp: 200})
	if len(stack.ops) != 2 {
		t.Fatalf("delete must stay separate across the boundary, got %+v", stack.ops)
	}
	if stack.ops[1].Type != OpDeleteFile || stack.ops[1].Paths[0] != "b.txt" {
		t.Fatalf("expected del b.txt, got %+v", stack.ops[1])
	}

	meta := newTestMeta("p")
	meta.UpsertFile("b.txt", file, 100)
	if err := applyOps(meta, stack.ops); err != nil {
		t.Fatalf("applyOps: %v", err)
	}
	if meta.FindFile("b.txt") != nil {
		t.Fatal("expected committed b.txt to be deleted by the surviving op")
	}
}

// A failed publish needs no rollback: the freeze is structural, so a
// post-failure append lands in a newer generation and the chain still
// cannot merge. The next freeze seals every pending generation at once.
func TestGenerationFailedCommitNeedsNoRollback(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	batch, frozen := stack.freeze()
	if len(batch) != 1 {
		t.Fatalf("frozen batch must carry exactly generation %d, got %+v", frozen, batch)
	}
	// Publish fails: no rollback call exists. The rename after the failed
	// commit must still refuse to merge into the sealed op.
	stack.append(Op{
		Type: OpRename, Paths: []string{"b.txt", "c.txt"}, Cause: "mv",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	if len(stack.ops) != 2 {
		t.Fatalf("chain must stay split after a failed publish with no rollback, got %+v", stack.ops)
	}
	batch2, _ := stack.freeze()
	if len(batch2) != 2 {
		t.Fatalf("retry freeze must seal every pending generation, got %+v", batch2)
	}
}

// foldOps over gen-tagged deltas reproduces the live stack exactly,
// including a freeze-split chain: the fold never spans a boundary because
// append tracks each line's generation and merges only within it.
func TestGenerationFoldMatchesLiveAcrossFreeze(t *testing.T) {
	t.Parallel()
	var stack opStack
	var deltas []Op
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	deltas = append(deltas, stack.appendWithDelta(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	}))
	stack.freeze()
	deltas = append(deltas, stack.appendWithDelta(Op{
		Type: OpRename, Paths: []string{"b.txt", "c.txt"}, Cause: "mv",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{},
	}))
	folded := foldOps(deltas)
	if len(folded) != len(stack.ops) {
		t.Fatalf("fold (%d ops %+v) must equal live (%d ops %+v)", len(folded), folded, len(stack.ops), stack.ops)
	}
	for i := range folded {
		if folded[i].Type != stack.ops[i].Type || !equalStringSlices(folded[i].Paths, stack.ops[i].Paths) || folded[i].Gen != stack.ops[i].Gen {
			t.Fatalf("fold line %d %+v != live op %+v", i, folded[i], stack.ops[i])
		}
	}
}

// Old journals (no gen key, no snap key) still read: every line lands in
// one legacy generation and merges freely, the pre-mark behavior.
func TestJournalGenCompatOldLinesRead(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	journalDir := t.TempDir()
	cfg := Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true, JournalDir: journalDir}
	hub := backend.newClient(t, cfg)

	fileEntry := FileMeta{Size: 1, Mode: 0o644, Inode: 7, Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	var sb strings.Builder
	for i := 0; i < 3; i++ {
		line, err := json.Marshal(Op{Seq: uint64(i + 1), Type: OpPutFile, Paths: []string{"f.txt"}, Cause: "upload", Timestamp: 1700000100, File: &fileEntry})
		if err != nil {
			t.Fatalf("marshal op: %v", err)
		}
		sb.Write(line)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(journalDir, "proj.jsonl"), []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write journal: %v", err)
	}
	ops := hub.journalRead("proj")
	if len(ops) != 1 || ops[0].Type != OpPutFile || ops[0].Paths[0] != "f.txt" {
		t.Fatalf("legacy same-path puts must fold to one put, got %+v", ops)
	}
}

// Old journals with snapshot marks (snap>0) mid-chain now fold merged
// instead of split. Shape diverges from what the old binary folded, but
// the replayed tree converges: both orders end at C with no B phantom.
// This is the soundness proof for deleting the per-delta SnapSeq replay.
func TestJournalGenCompatSnapMarkedLinesConverge(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	journalDir := t.TempDir()
	cfg := Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true, JournalDir: journalDir}
	hub := backend.newClient(t, cfg)

	fileEntry := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	lines := []Op{
		{Seq: 1, Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv", Timestamp: 1700000100, File: &fileEntry, SnapSeq: 0},
		{Seq: 2, Type: OpRename, Paths: []string{"b.txt", "c.txt"}, Cause: "mv", Timestamp: 1700000200, File: &fileEntry, SnapSeq: 1},
	}
	var sb strings.Builder
	for _, op := range lines {
		line, err := json.Marshal(op)
		if err != nil {
			t.Fatalf("marshal op: %v", err)
		}
		sb.Write(line)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(journalDir, "proj.jsonl"), []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write journal: %v", err)
	}
	folded := hub.journalRead("proj")
	if len(folded) != 1 {
		t.Fatalf("snap-marked legacy chain folds merged under generations, got %+v", folded)
	}
	base := newTestMeta("proj")
	base.UpsertFile("a.txt", fileEntry, 1700000000)
	for name, batch := range map[string][]Op{
		"folded": folded,
		"split":  lines,
	} {
		replayed := base.Clone()
		if err := applyOps(replayed, batch); err != nil {
			t.Fatalf("%s replay: %v", name, err)
		}
		if replayed.FindFile("b.txt") != nil {
			t.Fatalf("%s replay left a B phantom", name)
		}
		if replayed.FindFile("c.txt") == nil {
			t.Fatalf("%s replay lost the final path", name)
		}
	}
}

// Clearing the stack resets the open generation: a later hydrate must
// fast-forward from the journal's generations, not restamp them into a
// stale open one. Without the reset, prior commit activity (gen 3 open)
// would stamp both replayed gens to 3 and merge a split chain.
func TestGenerationClearResetsForHydrate(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{Type: OpPutFile, Paths: []string{"x.txt"}, Cause: "upload", Timestamp: 50, File: &file})
	stack.freeze()
	stack.append(Op{Type: OpPutFile, Paths: []string{"y.txt"}, Cause: "upload", Timestamp: 60, File: &file})
	stack.freeze()
	stack.clear()
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{}, Gen: 1,
	})
	stack.append(Op{
		Type: OpRename, Paths: []string{"b.txt", "c.txt"}, Cause: "mv",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{}, Gen: 2,
	})
	if len(stack.ops) != 2 {
		t.Fatalf("hydrate after clear must preserve the journal split, got %+v", stack.ops)
	}
	if stack.ops[0].Gen != 1 || stack.ops[1].Gen != 2 {
		t.Fatalf("hydrate must keep journal gens, got %d and %d", stack.ops[0].Gen, stack.ops[1].Gen)
	}
}

// Crash replay preserves generations: a cold load over a gen-tagged
// journal hydrates the same split the fold computed, so live==fold after
// recovery too. Drives the public cold-start path (fresh hub, same dir).
func TestGenerationHydratePreservesSplit(t *testing.T) {
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
	pm := hub.getOrCreateProjectMeta("proj")
	if err := hub.commitProjectMetadata(ctx, "proj", pm); err == nil {
		t.Fatal("commit under intercepted PUT must fail")
	}
	// The intercept stays armed through the second rename and the cold
	// load: no commit may land anywhere in this test, or the async loop
	// would publish A->B first and the chain would hydrate merged. Loads
	// are GETs, unaffected by the PUT block.
	if err := hub.RenameContext(ctx, "proj", "b.txt", "c.txt"); err != nil {
		t.Fatalf("rename B->C: %v", err)
	}

	// Crash: a fresh hub on the same journal dir replays and hydrates.
	hub2 := backend.newClient(t, cfg)
	meta, _, err := hub2.LoadRepoMetadataContext(ctx, "proj")
	if err != nil {
		t.Fatalf("load after crash: %v", err)
	}
	if meta.FindFile("c.txt") == nil {
		t.Fatalf("expected replayed chain at c.txt, got %+v", meta.Files())
	}
	pm2 := hub2.getOrCreateProjectMeta("proj")
	pm2.mu.RLock()
	liveOps := append([]Op(nil), pm2.opStack.ops...)
	pm2.mu.RUnlock()
	folded := hub2.journalRead("proj")
	if len(folded) != len(liveOps) {
		t.Fatalf("hydrated live (%d ops %+v) must equal fold (%d ops %+v)", len(liveOps), liveOps, len(folded), folded)
	}
	for i := range folded {
		if folded[i].Type != liveOps[i].Type || !equalStringSlices(folded[i].Paths, liveOps[i].Paths) || folded[i].Gen != liveOps[i].Gen {
			t.Fatalf("hydrated line %d %+v != live op %+v", i, folded[i], liveOps[i])
		}
	}
	if len(liveOps) != 2 {
		t.Fatalf("freeze-split chain must hydrate split, got %+v", liveOps)
	}
}
