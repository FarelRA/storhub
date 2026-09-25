package storage

// Item A10 chunk GC: RED-first contract tests.
//
// Roots: live files, pending opStack (journal mirror), open sessions.
// Never deletes: file-referenced chunks, op-referenced chunks, anything
// while a live session exists for the project, release assets (catalog
// records only).

import (
	"context"
	"errors"
	"testing"
)

func chunkGCTestHub(t *testing.T, backend *mockGitHub) *StorHub {
	t.Helper()
	cfg := Config{
		ChunkSize:         64,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
		JournalDir:        t.TempDir(),
	}
	return backend.newClient(t, cfg)
}

// seedChunkGCFile creates one live file with chunk 1 through a real
// transaction + flush, so the tree is committed truth.
func seedChunkGCFile(t *testing.T, hub *StorHub, project string) {
	t.Helper()
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, project); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	_, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		m.EnsureDirectory("docs", 1700000000)
		m.Chunks()[1] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11}
		m.UpsertFile("docs/a.txt", FileMeta{Size: 4, Mode: 0o644, UploadedAt: 1700000000, ModifiedAt: 1700000000, Chunks: []int64{1}}, 1700000000)
		if _, err := m.EnsureRelease("v1", 1700000000); err != nil {
			return err
		}
		return nil
	}, "storhub: seed chunkgc")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// plantOrphanChunk inserts a catalog record no file references, then
// flushes so the commit loop is idle and the stack is empty: later
// assertions observe exactly the planted state, not a racing commit.
func plantOrphanChunk(t *testing.T, hub *StorHub, project string, id int64, size int64) {
	t.Helper()
	_, err := hub.UpdateRepoMetadataContext(context.Background(), project, func(m *RepoMetadata) error {
		m.Chunks()[id] = ChunkInfo{Size: size, Offset: 0, Release: "v1", AssetID: 100 + id}
		return nil
	}, "storhub: plant orphan")
	if err != nil {
		t.Fatalf("plant orphan %d: %v", id, err)
	}
	if err := hub.FlushProjectContext(context.Background(), project); err != nil {
		t.Fatalf("flush after plant %d: %v", id, err)
	}
}

func liveChunkIDs(t *testing.T, hub *StorHub, project string) map[int64]bool {
	t.Helper()
	meta, _, err := hub.loadRepoMetadataReadonly(context.Background(), project)
	if err != nil {
		t.Fatalf("load readonly: %v", err)
	}
	out := map[int64]bool{}
	for id := range meta.Chunks() {
		out[id] = true
	}
	return out
}

// TestChunkGCScanReportsPlantedOrphan pins phase-1 metrics: the scan is
// read-only and the pressure gauges move.
func TestChunkGCScanReportsPlantedOrphan(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := chunkGCTestHub(t, backend)
	project := "chunkgc-scan"
	seedChunkGCFile(t, hub, project)
	plantOrphanChunk(t, hub, project, 2, 9)

	before := hub.PressureSnapshot()
	res, err := hub.ScanChunkGC(context.Background(), project)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if res.OrphanChunks != 1 {
		t.Fatalf("orphans = %d, want 1", res.OrphanChunks)
	}
	if res.OrphanBytes != 9 {
		t.Fatalf("orphan bytes = %d, want 9", res.OrphanBytes)
	}
	// Read-only: the orphan is still there.
	if !liveChunkIDs(t, hub, project)[2] {
		t.Fatal("scan deleted the orphan (must be read-only)")
	}
	after := hub.PressureSnapshot()
	if got := after.ChunkGCScans - before.ChunkGCScans; got != 1 {
		t.Fatalf("scan counter delta = %d, want 1", got)
	}
	if after.OrphanChunksLast != 1 || after.OrphanBytesLast != 9 {
		t.Fatalf("gauges = (%d, %d), want (1, 9)", after.OrphanChunksLast, after.OrphanBytesLast)
	}
}

// TestChunkGCCollectsPlantedOrphanKeepsLive pins the keep-vs-collect
// decision: orphans go, manifest-referenced chunks stay.
func TestChunkGCCollectsPlantedOrphanKeepsLive(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := chunkGCTestHub(t, backend)
	project := "chunkgc-collect"
	seedChunkGCFile(t, hub, project)
	plantOrphanChunk(t, hub, project, 2, 9)

	dry, err := hub.CompactOrphanChunks(context.Background(), project, true)
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if dry.OrphanChunks != 1 || dry.CollectedChunks != 0 {
		t.Fatalf("dry-run = orphans %d collected %d, want 1/0", dry.OrphanChunks, dry.CollectedChunks)
	}
	if !liveChunkIDs(t, hub, project)[2] {
		t.Fatal("dry-run deleted the orphan")
	}

	before := hub.PressureSnapshot()
	res, err := hub.CompactOrphanChunks(context.Background(), project, false)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if res.CollectedChunks != 1 || res.CollectedBytes != 9 {
		t.Fatalf("collected = (%d, %d), want (1, 9)", res.CollectedChunks, res.CollectedBytes)
	}
	ids := liveChunkIDs(t, hub, project)
	if ids[2] {
		t.Fatal("orphan survived compaction")
	}
	if !ids[1] {
		t.Fatal("live chunk 1 was collected (manifest-referenced must survive)")
	}
	after := hub.PressureSnapshot()
	if got := after.OrphanChunksCollected - before.OrphanChunksCollected; got != 1 {
		t.Fatalf("collected counter delta = %d, want 1", got)
	}
	if got := after.OrphanBytesReclaimed - before.OrphanBytesReclaimed; got != 9 {
		t.Fatalf("reclaimed bytes delta = %d, want 9", got)
	}
}

// TestChunkGCKeepsJournalReferenced pins the journal root: a chunk
// referenced only by a pending op (journal mirror) must survive.
func TestChunkGCKeepsJournalReferenced(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := chunkGCTestHub(t, backend)
	project := "chunkgc-journal"
	seedChunkGCFile(t, hub, project)
	plantOrphanChunk(t, hub, project, 2, 9)
	// Catalog record 3 exists but is referenced only by a pending op,
	// never by a live file.
	plantOrphanChunk(t, hub, project, 3, 5)

	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	jit := FileMeta{Size: 5, Mode: 0o644, UploadedAt: 1700000000, Chunks: []int64{3}}
	hub.appendOpLocked(project, pm, Op{
		Type:      OpPutFile,
		Paths:     []string{"docs/pending.txt"},
		Cause:     "chunkgc-test",
		Timestamp: 1700000000,
		File:      &jit,
		Chunks:    map[int64]ChunkInfo{3: {Size: 5, Offset: 0, Release: "v1", AssetID: 103}},
	})
	pm.mu.Unlock()

	// The journal root must still be pending (not committed into a
	// file) when compaction runs; otherwise the keep proves nothing
	// about op coverage.
	pm.mu.RLock()
	pending := len(pm.opStack.ops)
	pm.mu.RUnlock()
	if pending != 1 {
		t.Fatalf("pending ops = %d, want 1 (journal root must be in flight)", pending)
	}

	res, err := hub.CompactOrphanChunks(context.Background(), project, false)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	ids := liveChunkIDs(t, hub, project)
	if !ids[3] {
		t.Fatal("journal-referenced chunk 3 was collected (pending ops are roots)")
	}
	if ids[2] {
		t.Fatal("true orphan 2 survived compaction")
	}
	// The keep must come from the pending op, not from a racing
	// commit publishing the file first.
	meta, _, err := hub.loadRepoMetadataReadonly(context.Background(), project)
	if err != nil {
		t.Fatalf("load readonly: %v", err)
	}
	if meta.FindFile("docs/pending.txt") != nil {
		t.Fatal("commit loop published pending.txt before compaction; journal-root path unproven")
	}
	if res.CollectedChunks != 1 {
		t.Fatalf("collected = %d, want 1 (only the true orphan)", res.CollectedChunks)
	}
}

// TestChunkGCRefusesLiveSession pins the pinned-session gate: an open
// handle blocks compaction loudly and deletes nothing.
func TestChunkGCRefusesLiveSession(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := chunkGCTestHub(t, backend)
	project := "chunkgc-session"
	seedChunkGCFile(t, hub, project)
	plantOrphanChunk(t, hub, project, 2, 9)

	sh := hub.sessionHub()
	sh.mu.Lock()
	if sh.byID == nil {
		sh.byID = map[string]*openSession{}
	}
	sh.byID["test-handle-1"] = &openSession{handleID: "test-handle-1", project: project}
	sh.mu.Unlock()
	t.Cleanup(func() {
		sh.mu.Lock()
		delete(sh.byID, "test-handle-1")
		sh.mu.Unlock()
	})

	_, err := hub.CompactOrphanChunks(context.Background(), project, false)
	if err == nil {
		t.Fatal("compaction with a live session must fail loud")
	}
	var refused *ChunkGCRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("error type = %T, want *ChunkGCRefusedError", err)
	}
	if !liveChunkIDs(t, hub, project)[2] {
		t.Fatal("refused compaction deleted the orphan")
	}

	// A read-only scan still works under a live session.
	if _, err := hub.ScanChunkGC(context.Background(), project); err != nil {
		t.Fatalf("scan under session: %v", err)
	}
}
