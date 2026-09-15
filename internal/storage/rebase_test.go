package storage

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestRebasePreservesBothWriters is the core rebase contract: a rival
// writer commits while our ops are pending; the commit rebases instead of
// discarding, and BOTH writers' changes land.
func TestRebasePreservesBothWriters(t *testing.T) {
	base := newTestMeta("p")
	basePaths := hashPaths(base)

	// Rival advanced upstream: a new file we never saw.
	upstream := base.Clone()
	rivalFile := FileMeta{Size: 0, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	upstream.UpsertFile("rival.txt", rivalFile, 1700000100)

	// Our pending ops (built on base): mkdir + create.
	ops := []Op{
		{Type: OpMkdir, Paths: []string{"docs"}, Cause: "mkdir", Timestamp: 1700000200, Dir: &DirMeta{Inode: 50, Mode: 0o755, CreatedAt: 1700000200, ModifiedAt: 1700000200}},
		{Type: OpPutFile, Paths: []string{"docs/hello.txt"}, Cause: "create", Timestamp: 1700000200, File: &FileMeta{Size: 0, Mode: 0o644, Inode: 51, Chunks: []int64{}}},
	}

	rebased, resolutions, err := rebaseWorkingTree(&upstream, ops, basePaths, false)
	if err != nil {
		t.Fatalf("rebase: %v", err)
	}
	if rebased.FindFile("rival.txt") == nil {
		t.Fatal("expected rival writer's file preserved after rebase")
	}
	if rebased.FindFile("docs/hello.txt") == nil {
		t.Fatal("expected our op replayed after rebase")
	}
	if len(resolutions) != 0 {
		t.Fatalf("expected clean rebase (no path overlap), got %+v", resolutions)
	}
	if err := rebased.Validate(); err != nil {
		t.Fatalf("rebased tree invalid: %v", err)
	}
}

func TestRebaseLWWOnSamePath(t *testing.T) {
	base := newTestMeta("p")
	seed := FileMeta{Size: 4, Mode: 0o644, Inode: base.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	base.UpsertFile("a.txt", seed, 1700000000)
	basePaths := hashPaths(base)

	// Rival rewrote a.txt at t=100 with a DISTINCT state (size 0, fresh
	// inode) so the winner is unambiguous.
	upstream := base.Clone()
	rival := FileMeta{Size: 0, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	upstream.UpsertFile("a.txt", rival, 1700000100)

	// Our LATER put (t=200 > rival's t=100) wins, and it must be
	// identifiable as ours: distinct size, the seed's inode, and its own
	// chunk record.
	ourChunk := int64(77)
	ours := FileMeta{Size: 9, Mode: 0o644, Inode: seed.Inode, Chunks: []int64{ourChunk}, UploadedAt: 1700000200, ModifiedAt: 1700000200, AccessedAt: 1700000200, ChangedAt: 1700000200}
	ops := []Op{
		{Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload", Timestamp: 1700000200, File: &ours,
			Chunks: map[int64]ChunkInfo{ourChunk: {Size: 9, Offset: 0, Release: "v1", AssetID: 9}}},
	}

	rebased, resolutions, err := rebaseWorkingTree(&upstream, ops, basePaths, false)
	if err != nil {
		t.Fatalf("rebase: %v", err)
	}
	got := rebased.FindFile("a.txt")
	if got == nil {
		t.Fatal("expected a.txt after rebase")
	}
	if got.Size != 9 || got.Inode != seed.Inode || got.ChangedAt != 1700000200 {
		t.Fatalf("expected OUR put to win (size 9, seed inode, ts 200), got %+v", got)
	}
	if len(resolutions) != 1 || !strings.Contains(resolutions[0].Note, "LWW") {
		t.Fatalf("expected recorded LWW resolution, got %+v", resolutions)
	}
}

// TestRebaseOlderLocalWriteLosesToNewerUpstream pins the last-writer-wins rule: "we commit later"
// is not "we wrote later". A slow client whose op predates the upstream
// entry's change must NOT clobber the newer upstream write.
func TestRebaseOlderLocalWriteLosesToNewerUpstream(t *testing.T) {
	base := newTestMeta("p")
	seed := FileMeta{Size: 4, Mode: 0o644, Inode: base.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	base.UpsertFile("a.txt", seed, 1700000000)
	basePaths := hashPaths(base)

	// Rival wrote v2 at t=300 and committed first.
	upstream := base.Clone()
	rivalChunk := upstream.AllocateChunkID()
	upstream.Chunks[rivalChunk] = ChunkInfo{Size: 42, Offset: 0, Release: "v1", AssetID: 42}
	rival := FileMeta{Size: 42, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{rivalChunk}, UploadedAt: 1700000300, ModifiedAt: 1700000300, AccessedAt: 1700000300, ChangedAt: 1700000300}
	upstream.UpsertFile("a.txt", rival, 1700000300)

	// Our put carries the OLDER write time (t=200): it must be dropped.
	ourChunk := int64(77)
	ours := FileMeta{Size: 7, Mode: 0o644, Inode: seed.Inode, Chunks: []int64{ourChunk}, UploadedAt: 1700000200, ModifiedAt: 1700000200, AccessedAt: 1700000200, ChangedAt: 1700000200}
	ops := []Op{
		{Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload", Timestamp: 1700000200, File: &ours,
			Chunks: map[int64]ChunkInfo{ourChunk: {Size: 7, Offset: 0, Release: "v1", AssetID: 7}}},
	}

	rebased, resolutions, err := rebaseWorkingTree(&upstream, ops, basePaths, false)
	if err != nil {
		t.Fatalf("rebase: %v", err)
	}
	got := rebased.FindFile("a.txt")
	if got == nil || got.Size != 42 || got.ChangedAt != 1700000300 {
		t.Fatalf("expected newer upstream write to survive, got %+v", got)
	}
	if len(resolutions) != 1 || !strings.Contains(resolutions[0].Note, "upstream newer") {
		t.Fatalf("expected recorded upstream-newer resolution, got %+v", resolutions)
	}
}

func TestRebaseDeleteVsPutPutWins(t *testing.T) {
	base := newTestMeta("p")
	seed := FileMeta{Size: 4, Mode: 0o644, Inode: base.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	base.UpsertFile("keep.txt", seed, 1700000000)
	basePaths := hashPaths(base)

	// Rival re-created keep.txt with new content after our base.
	upstream := base.Clone()
	rival := FileMeta{Size: 0, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	upstream.UpsertFile("keep.txt", rival, 1700000100)

	// Our pending delete loses to the upstream put (data preservation).
	ops := []Op{
		{Type: OpDeleteFile, Paths: []string{"keep.txt"}, Cause: "unlink", Timestamp: 1700000200, FreedChunks: 1},
	}

	rebased, resolutions, err := rebaseWorkingTree(&upstream, ops, basePaths, false)
	if err != nil {
		t.Fatalf("rebase: %v", err)
	}
	if rebased.FindFile("keep.txt") == nil {
		t.Fatal("expected upstream put to win over our delete (data preservation)")
	}
	if len(resolutions) != 1 || !strings.Contains(resolutions[0].Note, "put wins") {
		t.Fatalf("expected recorded put-wins resolution, got %+v", resolutions)
	}
}

func TestRebaseStrictModeFails(t *testing.T) {
	base := newTestMeta("p")
	seed := FileMeta{Size: 4, Mode: 0o644, Inode: base.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	base.UpsertFile("a.txt", seed, 1700000000)
	basePaths := hashPaths(base)

	upstream := base.Clone()
	rival := FileMeta{Size: 0, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	upstream.UpsertFile("a.txt", rival, 1700000100)

	ops := []Op{
		{Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload", Timestamp: 1700000200, File: &FileMeta{Size: 0, Mode: 0o644, Inode: seed.Inode, Chunks: []int64{}}},
	}

	_, _, err := rebaseWorkingTree(&upstream, ops, basePaths, true)
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected strict mode to fail loudly on conflict, got %v", err)
	}
}

func TestRebaseChunkIDCollisionRemapped(t *testing.T) {
	base := newTestMeta("p")
	basePaths := hashPaths(base)

	// Both writers allocated chunk id 1 for DIFFERENT records (divergent
	// counters after a split brain). The rebase must remap ours instead of
	// corrupting either file's data.
	upstream := base.Clone()
	rivalChunk := upstream.AllocateChunkID() // id 1
	upstream.Chunks[rivalChunk] = ChunkInfo{Size: 5, Offset: 0, Release: "v1", AssetID: 100}
	rivalFile := FileMeta{Size: 5, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{rivalChunk}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	upstream.UpsertFile("rival.txt", rivalFile, 1700000100)

	ourChunkID := int64(1) // same id, different record
	ourRecord := ChunkInfo{Size: 7, Offset: 0, Release: "v1", AssetID: 200}
	ops := []Op{
		{Type: OpPutFile, Paths: []string{"ours.txt"}, Cause: "upload", Timestamp: 1700000200,
			File:   &FileMeta{Size: 7, Mode: 0o644, Inode: 60, Chunks: []int64{ourChunkID}},
			Chunks: map[int64]ChunkInfo{ourChunkID: ourRecord}},
	}

	rebased, resolutions, err := rebaseWorkingTree(&upstream, ops, basePaths, false)
	if err != nil {
		t.Fatalf("rebase: %v", err)
	}
	ours := rebased.FindFile("ours.txt")
	if ours == nil || len(ours.Chunks) != 1 {
		t.Fatalf("expected our file replayed with one chunk, got %+v", ours)
	}
	ourID := ours.Chunks[0]
	if ourID == rivalChunk {
		t.Fatal("expected our colliding chunk id to be remapped")
	}
	rec, ok := rebased.Chunks[ourID]
	if !ok || rec.AssetID != 200 {
		t.Fatalf("expected our chunk record preserved under the remapped id, got %+v", rec)
	}
	if rec := rebased.Chunks[rivalChunk]; rec.AssetID != 100 {
		t.Fatalf("expected rival chunk record untouched, got %+v", rec)
	}
	if rival := rebased.FindFile("rival.txt"); rival == nil || len(rival.Chunks) != 1 || rival.Chunks[0] != rivalChunk {
		t.Fatalf("expected rival file intact, got %+v", rival)
	}
	if len(resolutions) == 0 {
		t.Fatal("expected the chunk-id remap to be recorded as a resolution")
	}
	if err := rebased.Validate(); err != nil {
		t.Fatalf("rebased tree invalid: %v", err)
	}
}

func TestRebaseInodeDirCollisionRemapped(t *testing.T) {
	base := newTestMeta("p")
	basePaths := hashPaths(base)

	// Upstream allocated inode 7 to a directory; our pending file also
	// carries inode 7 (divergent counters). Validate forbids file/dir
	// inode collisions, so the rebase must remap ours.
	upstream := base.Clone()
	upstream.EnsureDirectory("rivaldir", 1700000100)
	dir := upstream.GetDirectory("rivaldir")
	dir.Inode = 7
	upstream.Dirs["rivaldir"] = *dir
	// Keep the counter honest.
	for upstream.NextInode <= 7 {
		upstream.AllocateInode()
	}

	ops := []Op{
		{Type: OpPutFile, Paths: []string{"ours.txt"}, Cause: "upload", Timestamp: 1700000200,
			File: &FileMeta{Size: 0, Mode: 0o644, Inode: 7, Chunks: []int64{}}},
	}

	rebased, _, err := rebaseWorkingTree(&upstream, ops, basePaths, false)
	if err != nil {
		t.Fatalf("rebase: %v", err)
	}
	ours := rebased.FindFile("ours.txt")
	if ours == nil || ours.Inode == 7 {
		t.Fatalf("expected our file's colliding inode remapped, got %+v", ours)
	}
	if err := rebased.Validate(); err != nil {
		t.Fatalf("rebased tree invalid: %v", err)
	}
}

func TestChangedPathsDetection(t *testing.T) {
	base := newTestMeta("p")
	f := FileMeta{Size: 4, Mode: 0o644, Inode: base.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	base.UpsertFile("same.txt", f, 1700000000)
	basePaths := hashPaths(base)

	upstream := base.Clone()
	changed := FileMeta{Size: 9, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	upstream.UpsertFile("same.txt", changed, 1700000100)
	upstream.RemoveFile("gone.txt") // not in base either; no-op
	gone := FileMeta{Size: 1, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	upstream.UpsertFile("added.txt", gone, 1700000100)

	changes := changedPaths(basePaths, &upstream)
	if !changes["f:same.txt"] {
		t.Fatalf("expected same.txt detected as changed, got %v", changes)
	}
	if !changes["f:added.txt"] {
		t.Fatalf("expected added.txt detected as changed, got %v", changes)
	}
	if changes["f:gone.txt"] {
		t.Fatalf("expected absent-on-both-sides path not flagged, got %v", changes)
	}
}

func TestRebaseMessageNote(t *testing.T) {
	ops := []Op{
		{Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload", Timestamp: 1700000200, File: &FileMeta{Size: 1, Mode: 0o644, Inode: 2, Chunks: []int64{}}},
	}
	resolutions := []ConflictResolution{{Seq: ops[0].Seq, Path: "a.txt", Note: "LWW a.txt (our put wins)"}}
	note := rebaseMessageNote(resolutions, "f00dfeed1234")
	if !strings.Contains(note, "rebased onto f00dfeed1234") || !strings.Contains(note, "1 resolved") {
		t.Fatalf("unexpected rebase note: %q", note)
	}
}

// TestRebaseMessageNoteTruncatesOnRuneBoundary pins the nit: the 300-byte
// cap must not split a UTF-8 sequence and emit invalid bytes into a commit
// message.
func TestRebaseMessageNoteTruncatesOnRuneBoundary(t *testing.T) {
	long := strings.Repeat("a", maxRebaseNoteBytes-1) + "é" // last rune straddles the cap
	resolutions := []ConflictResolution{{Seq: 1, Path: "x", Note: long}}
	note := rebaseMessageNote(resolutions, "f00dfeed1234")
	if !utf8.ValidString(note) {
		t.Fatalf("rebase note carries invalid UTF-8: %q", note)
	}
	if !strings.HasSuffix(note, ")") {
		t.Fatalf("expected the note to stay well-formed, got %q", note)
	}
}
