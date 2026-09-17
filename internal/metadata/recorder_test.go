package metadata

import (
	"encoding/json"
	"testing"
)

func TestIntentRecorderFirstTouchWins(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("p")
	old := FileMeta{Size: 1, Inode: 2, Chunks: []int64{7}, Mode: 0o644}
	m.UpsertFile("a.txt", old, 100) // pre-transaction state
	rec := NewIntentRecorder()
	m.AttachIntentRecorder(rec)

	// First touch inside the transaction pins the pre-transaction state.
	m.UpsertFile("a.txt", FileMeta{Size: 9, Inode: 3, Chunks: []int64{}, Mode: 0o600}, 200)
	// Later touches of the same path must not overwrite the pinned original.
	m.RemoveFile("a.txt")
	m.UpsertFile("a.txt", FileMeta{Size: 12, Inode: 4, Chunks: []int64{}, Mode: 0o600}, 300)

	intent, ok := rec.FileIntents()["a.txt"]
	if !ok {
		t.Fatal("expected a file intent for a.txt")
	}
	if !intent.Existed || intent.Old.Size != 1 || intent.Old.Inode != 2 {
		t.Fatalf("first touch must pin the pre-transaction state, got %+v", intent)
	}
	// The pinned state must be a snapshot: mutating the live entry later
	// must not reach into it.
	live := m.FindFile("a.txt")
	live.Size = 42
	m.UpsertFile("a.txt", *live, 400)
	if rec.FileIntents()["a.txt"].Old.Size != 1 {
		t.Fatal("recorded original was aliased by a later write")
	}
}

func TestIntentRecorderNilIsNoOp(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("p")
	// No recorder attached: every mutator must run without recording and
	// without panicking.
	m.UpsertFile("a.txt", FileMeta{Size: 1, Inode: 2, Chunks: []int64{}, Mode: 0o644}, 100)
	m.RemoveFile("a.txt")
	m.EnsureDirectory("d", 100)
	m.RemoveDirectory("d")
	if err := m.PutChunk(1, ChunkInfo{}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	m.DeleteChunk(1)
	if _, err := m.EnsureRelease("v1", 100); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	// A zero CreatedAt is rejected loudly (validation), never stored and
	// never panicking: the no-recorder contract covers error returns too.
	if err := m.PutRelease("v1", ReleaseRef{}); err == nil {
		t.Fatal("expected zero-CreatedAt PutRelease to fail")
	}
	m.RemoveRelease("v1")
	m.PruneUnreferencedChunks()
}

func TestIntentRecorderCloneDropsIt(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("p")
	rec := NewIntentRecorder()
	m.AttachIntentRecorder(rec)
	clone := m.Clone()
	// Mutating the clone must not record into the original's recorder.
	clone.EnsureDirectory("from-clone", 100)
	clone.UpsertFile("from-clone/f.txt", FileMeta{Size: 1, Inode: 5, Chunks: []int64{}, Mode: 0o644}, 100)
	if len(rec.DirIntents()) != 0 || len(rec.FileIntents()) != 0 {
		t.Fatalf("clone mutations leaked into the source recorder: %+v %+v", rec.DirIntents(), rec.FileIntents())
	}
	// And the clone takes its own recorder without affecting the source.
	rec2 := NewIntentRecorder()
	clone.AttachIntentRecorder(rec2)
	m.EnsureDirectory("from-source", 100)
	if len(rec2.DirIntents()) != 0 {
		t.Fatal("source mutations leaked into the clone recorder")
	}
	m.DetachIntentRecorder()
	clone.DetachIntentRecorder()
}

func TestIntentRecorderNotSerialized(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("p")
	m.AttachIntentRecorder(NewIntentRecorder())
	m.EnsureDirectory("d", 100)
	data, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	var shadow map[string]any
	if err := json.Unmarshal(data, &shadow); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for key := range shadow {
		if key == "recorder" {
			t.Fatal("recorder leaked into the JSON shadow")
		}
	}
}

func TestIntentRecorderChunkAndReleaseKinds(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("p")
	// Pre-transaction state: chunks 5 and 6 exist, release v1 does not.
	if err := m.PutChunk(5, ChunkInfo{Size: 1}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	if err := m.PutChunk(6, ChunkInfo{Size: 1}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	rec := NewIntentRecorder()
	m.AttachIntentRecorder(rec)

	if err := m.PutChunk(6, ChunkInfo{Size: 2}); err != nil { // overwrite
		t.Fatalf("seed chunk: %v", err)
	}
	m.DeleteChunk(6)                                          // removed again: net absent, existed before
	if err := m.PutChunk(7, ChunkInfo{Size: 1}); err != nil { // new
		t.Fatalf("seed chunk: %v", err)
	}
	m.DeleteChunk(7)                                      // created then removed: net absent, did not exist before
	if _, err := m.EnsureRelease("v1", 100); err != nil { // new
		t.Fatalf("seed release: %v", err)
	}
	if err := m.PutRelease("v1", ReleaseRef{CreatedAt: 200}); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	m.RemoveRelease("v1") // removed again: net absent, existed before

	if got := rec.ChunkIntents()[5]; got.Existed {
		t.Fatalf("chunk 5 was never touched and must have no intent: %+v", got)
	}
	if got := rec.ChunkIntents()[6]; !got.Existed {
		t.Fatalf("chunk 6 pre-transaction existence wrong: %+v", got)
	}
	if got := rec.ChunkIntents()[7]; got.Existed {
		t.Fatalf("chunk 7 did not exist pre-transaction: %+v", got)
	}
	if got := rec.ReleaseIntents()["v1"]; got.Existed {
		// v1 is created inside the transaction: the intent must pin it as
		// previously absent.
		t.Fatalf("release v1 intent wrong: %+v", got)
	}
}

func TestIntentRecorderSortFileChunksIntent(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("p")
	if err := m.PutChunk(1, ChunkInfo{Size: 4, Offset: 4, Release: "v1", AssetID: 11}); err != nil {
		t.Fatal(err)
	}
	if err := m.PutChunk(2, ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 12}); err != nil {
		t.Fatal(err)
	}
	m.UpsertFile("a.txt", FileMeta{Size: 8, Mode: 0o644, Chunks: []int64{1, 2}}, 100)
	rec := NewIntentRecorder()
	m.AttachIntentRecorder(rec)
	m.SortFileChunks("a.txt")
	intent, ok := rec.FileIntents()["a.txt"]
	if !ok {
		t.Fatal("SortFileChunks must record a file intent (the reorder is a mutation)")
	}
	if !intent.Existed || len(intent.Old.Chunks) != 2 || intent.Old.Chunks[0] != 1 {
		t.Fatalf("sort intent must pin the pre-sort order, got %+v", intent)
	}
	if got := m.FindFile("a.txt"); len(got.Chunks) != 2 || got.Chunks[0] != 2 {
		t.Fatalf("chunks not sorted by offset: %+v", got.Chunks)
	}
}

func TestIntentRecorderRemoveReleaseKeepsOld(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("p")
	if _, err := m.EnsureRelease("v1", 100); err != nil {
		t.Fatal(err)
	}
	rec := NewIntentRecorder()
	m.AttachIntentRecorder(rec)
	m.RemoveRelease("v1")
	intent, ok := rec.ReleaseIntents()["v1"]
	if !ok {
		t.Fatal("RemoveRelease must record a release intent")
	}
	if !intent.Existed || intent.Old.CreatedAt != 100 {
		t.Fatalf("remove intent must pin the removed ref, got %+v", intent)
	}
}

func TestIntentRecorderAtimeIntents(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("p")
	m.EnsureDirectory("d", 100)
	m.UpsertFile("d/a.txt", FileMeta{Size: 1, Mode: 0o644, Chunks: []int64{}}, 100)
	rec := NewIntentRecorder()
	m.AttachIntentRecorder(rec)
	if !m.SetFileAtime("d/a.txt", 200) {
		t.Fatal("set file atime")
	}
	if !m.SetDirAtime("d", 200) {
		t.Fatal("set dir atime")
	}
	fintent, ok := rec.FileIntents()["d/a.txt"]
	if !ok || !fintent.Existed {
		t.Fatalf("file atime must record a file intent, got %+v", rec.FileIntents())
	}
	dintent, ok := rec.DirIntents()["d"]
	if !ok || !dintent.Existed {
		t.Fatalf("dir atime must record a dir intent, got %+v", rec.DirIntents())
	}
}

func TestIntentRecorderRevertSubtreeIntents(t *testing.T) {
	t.Parallel()
	hist := buildTree(t, func(m *RepoMetadata) {
		m.EnsureDirectory("docs", 100)
		putTestChunk(t, m, 1, ChunkInfo{Size: 5, Offset: 0, Release: "v1", AssetID: 11})
		m.UpsertFile("docs/a.txt", FileMeta{Size: 5, Mode: 0o644, UploadedAt: 100, ModifiedAt: 100, Chunks: []int64{1}}, 100)
		if _, err := m.EnsureRelease("v1", 100); err != nil {
			t.Fatal(err)
		}
	})
	cur := clonePtr(hist)
	// Diverge first (setup, outside the transaction): the release, its
	// chunk, and the file are all gone from dst, so the revert genuinely
	// restores catalog state, not just bytes.
	cur.RemoveFile("docs/a.txt")
	cur.DeleteChunk(1)
	cur.RemoveRelease("v1")
	rec := NewIntentRecorder()
	cur.AttachIntentRecorder(rec)
	if err := RevertSubtree(cur, hist, "docs/a.txt", 300); err != nil {
		t.Fatalf("revert: %v", err)
	}
	if _, ok := rec.ChunkIntents()[1]; !ok {
		t.Fatalf("revert must record the restored chunk, got %+v", rec.ChunkIntents())
	}
	if _, ok := rec.ReleaseIntents()["v1"]; !ok {
		t.Fatalf("revert must record the restored release, got %+v", rec.ReleaseIntents())
	}
	if _, ok := rec.FileIntents()["docs/a.txt"]; !ok {
		t.Fatalf("revert must record the restored file, got %+v", rec.FileIntents())
	}
	if err := cur.Validate(); err != nil {
		t.Fatalf("reverted tree invalid: %v", err)
	}
}
