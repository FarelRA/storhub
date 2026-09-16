package storage

import (
	"strings"
	"testing"
)

func newTestMeta(project string) *RepoMetadata {
	meta := NewRepoMetadata(project)
	meta.Normalize(project, 1700000000)
	return meta
}

func TestOpStackPutCoalescing(t *testing.T) {
	t.Parallel()
	stack := &opStack{}

	file := FileMeta{Size: 10, Mode: 0o644, Inode: 2, Chunks: []int64{}}
	stack.append(Op{
		Type: OpPutFile, Paths: []string{"docs/a.txt"}, Cause: "upload",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	file.Size = 20
	stack.append(Op{
		Type: OpPutFile, Paths: []string{"docs/a.txt"}, Cause: "upload",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{},
	})

	if len(stack.ops) != 1 {
		t.Fatalf("expected 1 coalesced op, got %d: %+v", len(stack.ops), stack.ops)
	}
	op := stack.ops[0]
	if op.Times != 2 {
		t.Fatalf("expected times=2, got %d", op.Times)
	}
	if op.Timestamp != 200 {
		t.Fatalf("expected latest timestamp 200, got %d", op.Timestamp)
	}
	if op.File == nil || op.File.Size != 20 {
		t.Fatalf("expected latest full state, got %+v", op.File)
	}
}

func TestOpStackDeleteThenPutCoalescesToPut(t *testing.T) {
	t.Parallel()
	stack := &opStack{}

	stack.append(Op{Type: OpDeleteFile, Paths: []string{"a.txt"}, Cause: "unlink", Timestamp: 100})
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{},
	})

	if len(stack.ops) != 1 {
		t.Fatalf("expected single put op after delete+put, got %d: %+v", len(stack.ops), stack.ops)
	}
	if stack.ops[0].Type != OpPutFile {
		t.Fatalf("expected put to win, got %s", stack.ops[0].Type)
	}
}

func TestOpStackPutThenDeleteCoalescesToDelete(t *testing.T) {
	t.Parallel()
	stack := &opStack{}

	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{7}}
	stack.append(Op{
		Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{7: {Size: 5}},
	})
	stack.append(Op{Type: OpDeleteFile, Paths: []string{"a.txt"}, Cause: "unlink", Timestamp: 200, FreedChunks: 1})

	if len(stack.ops) != 1 {
		t.Fatalf("expected single del op after put+del, got %d: %+v", len(stack.ops), stack.ops)
	}
	if stack.ops[0].Type != OpDeleteFile {
		t.Fatalf("expected del to win, got %s", stack.ops[0].Type)
	}
}

func TestOpStackRenameChain(t *testing.T) {
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
		t.Fatalf("expected chained rename to collapse, got %d: %+v", len(stack.ops), stack.ops)
	}
	op := stack.ops[0]
	if op.Paths[0] != "a.txt" || op.Paths[1] != "c.txt" {
		t.Fatalf("expected a.txt->c.txt, got %v", op.Paths)
	}
}

// TestOpStackSnapshotNotAliasedByRenameCoalesce pins the snapshot-aliasing contract: snapshot() hands an
// in-flight commit a copy that must never observe later coalescing. The
// rename-chain merge used to write Paths[1] through the shared backing array,
// so a commit mid-flight would publish A->C after having announced A->B (and
// rebase onto the wrong target).
func TestOpStackSnapshotNotAliasedByRenameCoalesce(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	snap := stack.snapshot()
	if len(snap) != 1 || snap[0].Paths[1] != "b.txt" {
		t.Fatalf("snapshot precondition broken: %+v", snap)
	}
	// Coalesce B->C into the (still in-flight) A->B.
	stack.append(Op{
		Type: OpRename, Paths: []string{"b.txt", "c.txt"}, Cause: "mv",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	if len(stack.ops) != 1 || stack.ops[0].Paths[1] != "c.txt" {
		t.Fatalf("expected live stack to coalesce to a.txt->c.txt, got %+v", stack.ops)
	}
	if snap[0].Paths[1] != "b.txt" {
		t.Fatalf("snapshot aliased: in-flight commit now reads %v, want [a.txt b.txt]", snap[0].Paths)
	}
}

// TestOpStackRenameChainAcrossSnapshotStaysSplit pins the snapshot-boundary rule: a rename whose
// predecessor is inside the in-flight commit snapshot must NOT collapse into
// it. The commit publishes A->B; a merged A->C in the surviving stack would
// replay as "remove A (absent), write C" and leave B as a phantom duplicate.
func TestOpStackRenameChainAcrossSnapshotStaysSplit(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	stack.noteSnapshot(stack.maxSeq()) // commit snapshots [A->B] and publishes it

	stack.append(Op{
		Type: OpRename, Paths: []string{"b.txt", "c.txt"}, Cause: "mv",
		Timestamp: 200, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	if len(stack.ops) != 2 {
		t.Fatalf("expected the chain to stay split across the snapshot boundary, got %+v", stack.ops)
	}
	if stack.ops[0].Paths[1] != "b.txt" || stack.ops[1].Paths[0] != "b.txt" || stack.ops[1].Paths[1] != "c.txt" {
		t.Fatalf("expected [a->b, b->c], got %+v", stack.ops)
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

// TestOpStackRenameThenDeleteAcrossSnapshotStaysSplit pins the snapshot boundary
// for the rename-then-delete collapse: with A->B in flight, deleting B must
// survive as "delete B" (the next replay removes the committed B), not
// collapse to "delete A" (a no-op that strands B).
func TestOpStackRenameThenDeleteAcrossSnapshotStaysSplit(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	stack.noteSnapshot(stack.maxSeq())

	stack.append(Op{Type: OpDeleteFile, Paths: []string{"b.txt"}, Cause: "unlink", Timestamp: 200})
	if len(stack.ops) != 2 {
		t.Fatalf("expected delete to stay separate across the snapshot boundary, got %+v", stack.ops)
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

func TestOpStackRenameThenDeleteTarget(t *testing.T) {
	t.Parallel()
	stack := &opStack{}

	file := FileMeta{Size: 5, Mode: 0o644, Inode: 3, Chunks: []int64{}}
	stack.append(Op{
		Type: OpRename, Paths: []string{"a.txt", "b.txt"}, Cause: "mv",
		Timestamp: 100, File: &file, Chunks: map[int64]ChunkInfo{},
	})
	stack.append(Op{Type: OpDeleteFile, Paths: []string{"b.txt"}, Cause: "unlink", Timestamp: 200})

	if len(stack.ops) != 1 {
		t.Fatalf("expected rename+delete to collapse, got %d: %+v", len(stack.ops), stack.ops)
	}
	if stack.ops[0].Type != OpDeleteFile || stack.ops[0].Paths[0] != "a.txt" {
		t.Fatalf("expected net del a.txt, got %s %v", stack.ops[0].Type, stack.ops[0].Paths)
	}
}

func TestApplyOpsRoundTrip(t *testing.T) {
	t.Parallel()
	base := newTestMeta("p")
	base.EnsureDirectory("docs", 1700000000)
	chunkID := base.AllocateChunkID()
	base.Chunks()[chunkID] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11}
	file := FileMeta{Size: 4, Mode: 0o644, Inode: base.AllocateInode(), Chunks: []int64{chunkID}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	base.UpsertFile("docs/a.txt", file, 1700000000)

	stack := &opStack{}
	// put a new file with a chunk record
	newChunk := base.AllocateChunkID()
	base.Chunks()[newChunk] = ChunkInfo{Size: 3, Offset: 0, Release: "v1", AssetID: 12}
	newFile := FileMeta{Size: 3, Mode: 0o600, Inode: base.AllocateInode(), Chunks: []int64{newChunk}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	stack.append(Op{
		Type: OpPutFile, Paths: []string{"docs/b.txt"}, Cause: "upload", Timestamp: 1700000100,
		File: &newFile, Chunks: map[int64]ChunkInfo{newChunk: base.Chunks()[newChunk]},
	})
	// delete the first file
	stack.append(Op{Type: OpDeleteFile, Paths: []string{"docs/a.txt"}, Cause: "unlink", Timestamp: 1700000200, FreedChunks: 1})
	// mkdir
	stack.append(Op{Type: OpMkdir, Paths: []string{"tmp"}, Cause: "mkdir", Timestamp: 1700000300, Dir: &DirMeta{Inode: 50, Mode: 0o755, CreatedAt: 1700000300, ModifiedAt: 1700000300}})
	// rename the directory (b.txt moves with it)
	stack.append(Op{Type: OpRename, Paths: []string{"docs", "archive"}, Cause: "mv", Timestamp: 1700000400, Dir: &DirMeta{Inode: 20, Mode: 0o755, CreatedAt: 1700000000, ModifiedAt: 1700000000}})

	replayed := base.Clone()
	if err := applyOps(replayed, stack.ops); err != nil {
		t.Fatalf("applyOps: %v", err)
	}
	replayed.Normalize("p", 1700000500)

	if replayed.FindFile("docs/a.txt") != nil {
		t.Fatal("expected docs/a.txt deleted after replay")
	}
	if replayed.FindFile("docs/b.txt") != nil {
		t.Fatal("expected docs/b.txt moved out of docs by the rename replay")
	}
	got := replayed.FindFile("archive/b.txt")
	if got == nil {
		t.Fatal("expected archive/b.txt after replay")
	}
	if got.Size != 3 || got.Mode != 0o600 {
		t.Fatalf("unexpected replayed file state: %+v", got)
	}
	if _, ok := replayed.Chunks()[newChunk]; !ok {
		t.Fatal("expected replayed chunk record present")
	}
	if !replayed.HasDirectory("archive") {
		t.Fatal("expected docs renamed to archive")
	}
	if replayed.HasDirectory("docs") {
		t.Fatal("expected docs gone after rename replay")
	}
	if !replayed.HasDirectory("tmp") {
		t.Fatal("expected tmp dir created")
	}
	if err := replayed.Validate(); err != nil {
		t.Fatalf("replayed metadata invalid: %v", err)
	}
}

func TestApplyOpsRmdirSkipsNonEmptyUpstream(t *testing.T) {
	t.Parallel()
	base := newTestMeta("p")
	// Upstream (rebase target) gained a child under tmp after our base.
	upstream := base.Clone()
	upstream.EnsureDirectory("tmp", 1700000000)
	child := FileMeta{Size: 1, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000001, ModifiedAt: 1700000001, AccessedAt: 1700000001, ChangedAt: 1700000001}
	upstream.UpsertFile("tmp/keep.txt", child, 1700000001)

	resolutions := []ConflictResolution{}
	err := applyOpsWithResolutions(upstream, []Op{
		{Type: OpRmdir, Paths: []string{"tmp"}, Cause: "rmdir", Timestamp: 1700000100},
	}, &resolutions)
	if err != nil {
		t.Fatalf("applyOps: %v", err)
	}
	if !upstream.HasDirectory("tmp") {
		t.Fatal("expected data-preserving skip: upstream children must survive")
	}
	if upstream.FindFile("tmp/keep.txt") == nil {
		t.Fatal("expected upstream child preserved")
	}
	if len(resolutions) != 1 || !strings.Contains(resolutions[0].Note, "non-empty") {
		t.Fatalf("expected a recorded resolution note, got %+v", resolutions)
	}
}

func TestBuildCommitMessage(t *testing.T) {
	t.Parallel()
	stack := &opStack{}

	file := FileMeta{Size: 2400000, Mode: 0o644, Inode: 2, Chunks: []int64{1, 2}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	stack.append(Op{
		Type: OpPutFile, Paths: []string{"docs/report.pdf"}, Cause: "upload", Timestamp: 1700000000,
		File: &file, Chunks: map[int64]ChunkInfo{1: {Release: "v5"}, 2: {Release: "v5"}},
	})
	stack.append(Op{Type: OpDeleteFile, Paths: []string{"tmp/scratch.bin"}, Cause: "unlink", Timestamp: 1700000001, FreedChunks: 2})
	notes := FileMeta{Size: 12, Mode: 0o644, Inode: 5, Chunks: []int64{}, UploadedAt: 1700000002, ModifiedAt: 1700000002, AccessedAt: 1700000002, ChangedAt: 1700000002}
	stack.append(Op{Type: OpSetattr, Paths: []string{"docs/notes.txt"}, Cause: "chmod", Timestamp: 1700000002, File: &notes})

	msg := buildCommitMessage(stack.ops, "a1b2c3d4e5f60789")
	lines := strings.Split(msg, "\n")
	if lines[0] != "storhub: 3 ops (2 put, 1 del) on top of a1b2c3d4e5f6" {
		t.Fatalf("unexpected summary line: %q", lines[0])
	}
	if !strings.Contains(msg, "put docs/report.pdf 2.2MiB 2 chunks (v5) mode 0644 [upload]") {
		t.Fatalf("missing put line in:\n%s", msg)
	}
	if !strings.Contains(msg, "del tmp/scratch.bin freed 2 chunks [unlink]") {
		t.Fatalf("missing del line in:\n%s", msg)
	}
	if !strings.Contains(msg, "setattr docs/notes.txt mode 0644 [chmod]") {
		t.Fatalf("missing setattr line in:\n%s", msg)
	}
}

func TestBuildCommitMessageCapsBody(t *testing.T) {
	t.Parallel()
	stack := &opStack{}
	for i := 0; i < 150; i++ {
		file := FileMeta{Size: 1, Mode: 0o644, Inode: uint64(i + 2), Chunks: []int64{}}
		path := string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".txt"
		stack.append(Op{
			Type: OpPutFile, Paths: []string{path}, Cause: "upload",
			Timestamp: 1700000000, File: &file, Chunks: map[int64]ChunkInfo{},
		})
	}
	msg := buildCommitMessage(stack.ops, "")
	if got := strings.Count(msg, "\n"); got > 101 {
		t.Fatalf("expected body capped near 100 lines, got %d newlines", got)
	}
	if !strings.Contains(msg, "+ 50 more") {
		t.Fatalf("expected a '+ 50 more' truncation note in:\n%s", msg)
	}
}

func TestBuildCommitMessageFallback(t *testing.T) {
	t.Parallel()
	msg := buildCommitMessage(nil, "")
	if msg != "storhub: update metadata" {
		t.Fatalf("expected fallback message, got %q", msg)
	}
}

func TestOpSummaryCounts(t *testing.T) {
	t.Parallel()
	counts := opSummaryCounts([]Op{
		{Type: OpPutFile}, {Type: OpPutFile}, {Type: OpDeleteFile}, {Type: OpMkdir},
	})
	if counts != "2 put, 1 del, 1 mkdir" {
		t.Fatalf("unexpected summary counts: %q", counts)
	}
}

func TestHumanizeBytes(t *testing.T) {
	t.Parallel()
	cases := map[int64]string{
		0:          "0B",
		512:        "512B",
		2048:       "2.0KiB",
		2400000:    "2.2MiB",
		1610612736: "1.5GiB",
	}
	for in, want := range cases {
		if got := humanizeBytes(in); got != want {
			t.Fatalf("humanizeBytes(%d) = %q, want %q", in, got, want)
		}
	}
}
