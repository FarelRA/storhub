package metadata

import (
	"encoding/json"
	"testing"
)

func TestIntentRecorderFirstTouchWins(t *testing.T) {
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
	m := NewRepoMetadata("p")
	// No recorder attached: every mutator must run without recording and
	// without panicking.
	m.UpsertFile("a.txt", FileMeta{Size: 1, Inode: 2, Chunks: []int64{}, Mode: 0o644}, 100)
	m.RemoveFile("a.txt")
	m.EnsureDirectory("d", 100)
	m.RemoveDirectory("d")
	m.PutChunk(1, ChunkInfo{})
	m.DeleteChunk(1)
	m.EnsureRelease("v1", 100)
	m.PutRelease("v1", ReleaseRef{})
	m.RemoveRelease("v1")
	m.PruneUnreferencedChunks()
}

func TestIntentRecorderCloneDropsIt(t *testing.T) {
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
	m := NewRepoMetadata("p")
	// Pre-transaction state: chunks 5 and 6 exist, release v1 does not.
	m.PutChunk(5, ChunkInfo{Size: 1})
	m.PutChunk(6, ChunkInfo{Size: 1})
	rec := NewIntentRecorder()
	m.AttachIntentRecorder(rec)

	m.PutChunk(6, ChunkInfo{Size: 2}) // overwrite
	m.DeleteChunk(6)                  // removed again: net absent, existed before
	m.PutChunk(7, ChunkInfo{Size: 1}) // new
	m.DeleteChunk(7)                  // created then removed: net absent, did not exist before
	m.EnsureRelease("v1", 100)        // new
	m.PutRelease("v1", ReleaseRef{CreatedAt: 200})
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
