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

func TestOpStackRenameThenDeleteTarget(t *testing.T) {
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
	base := newTestMeta("p")
	base.EnsureDirectory("docs", 1700000000)
	chunkID := base.AllocateChunkID()
	base.Chunks[chunkID] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11}
	file := FileMeta{Size: 4, Mode: 0o644, Inode: base.AllocateInode(), Chunks: []int64{chunkID}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	base.UpsertFile("docs/a.txt", file, 1700000000)

	stack := &opStack{}
	// put a new file with a chunk record
	newChunk := base.AllocateChunkID()
	base.Chunks[newChunk] = ChunkInfo{Size: 3, Offset: 0, Release: "v1", AssetID: 12}
	newFile := FileMeta{Size: 3, Mode: 0o600, Inode: base.AllocateInode(), Chunks: []int64{newChunk}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	stack.append(Op{
		Type: OpPutFile, Paths: []string{"docs/b.txt"}, Cause: "upload", Timestamp: 1700000100,
		File: &newFile, Chunks: map[int64]ChunkInfo{newChunk: base.Chunks[newChunk]},
	})
	// delete the first file
	stack.append(Op{Type: OpDeleteFile, Paths: []string{"docs/a.txt"}, Cause: "unlink", Timestamp: 1700000200, FreedChunks: 1})
	// mkdir
	stack.append(Op{Type: OpMkdir, Paths: []string{"tmp"}, Cause: "mkdir", Timestamp: 1700000300, Dir: &DirMeta{Inode: 50, Mode: 0o755, CreatedAt: 1700000300, ModifiedAt: 1700000300}})
	// rename the directory (b.txt moves with it)
	stack.append(Op{Type: OpRename, Paths: []string{"docs", "archive"}, Cause: "mv", Timestamp: 1700000400, Dir: &DirMeta{Inode: 20, Mode: 0o755, CreatedAt: 1700000000, ModifiedAt: 1700000000}})

	replayed := base.Clone()
	if err := applyOps(&replayed, stack.ops); err != nil {
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
	if _, ok := replayed.Chunks[newChunk]; !ok {
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
	base := newTestMeta("p")
	// Upstream (rebase target) gained a child under tmp after our base.
	upstream := base.Clone()
	upstream.EnsureDirectory("tmp", 1700000000)
	child := FileMeta{Size: 1, Mode: 0o644, Inode: upstream.AllocateInode(), Chunks: []int64{}, UploadedAt: 1700000001, ModifiedAt: 1700000001, AccessedAt: 1700000001, ChangedAt: 1700000001}
	upstream.UpsertFile("tmp/keep.txt", child, 1700000001)

	resolutions := []ConflictResolution{}
	err := applyOpsWithResolutions(&upstream, []Op{
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
	msg := buildCommitMessage(nil, "")
	if msg != "storhub: update metadata" {
		t.Fatalf("expected fallback message, got %q", msg)
	}
}

func TestSynthesizeDiffOps(t *testing.T) {
	before := newTestMeta("p")
	before.EnsureDirectory("docs", 1700000000)
	cid := before.AllocateChunkID()
	before.Chunks[cid] = ChunkInfo{Size: 2, Offset: 0, Release: "v1", AssetID: 9}
	src := FileMeta{Size: 2, Mode: 0o644, Inode: before.AllocateInode(), Chunks: []int64{cid}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	before.UpsertFile("docs/new.txt", src, 1700000000)

	after := before.Clone()
	// mkdir
	after.EnsureDirectory("tmp", 1700000100)
	// create another file
	cid2 := after.AllocateChunkID()
	after.Chunks[cid2] = ChunkInfo{Size: 5, Offset: 0, Release: "v1", AssetID: 10}
	other := FileMeta{Size: 5, Mode: 0o644, Inode: after.AllocateInode(), Chunks: []int64{cid2}, UploadedAt: 1700000100, ModifiedAt: 1700000100, AccessedAt: 1700000100, ChangedAt: 1700000100}
	after.UpsertFile("docs/other.txt", other, 1700000100)
	// chmod existing dir
	dir := after.GetDirectory("docs")
	dir.Mode = 0o700
	after.Dirs["docs"] = *dir
	// rename new.txt -> renamed.txt (same entry identity)
	renamed := after.FindFile("docs/new.txt").Clone()
	after.RemoveFile("docs/new.txt")
	after.UpsertFile("docs/renamed.txt", renamed, 1700000200)

	ops := synthesizeOpsFromDiff(before, &after, "test", 1700000200)
	counts := map[OpType]int{}
	for _, op := range ops {
		counts[op.Type]++
	}
	if counts[OpMkdir] != 1 {
		t.Fatalf("expected 1 mkdir op, got %+v", ops)
	}
	if counts[OpSetattr] != 1 {
		t.Fatalf("expected 1 setattr op (dir chmod), got %+v", ops)
	}
	if counts[OpRename] != 1 {
		t.Fatalf("expected rename detection for identical entry move, got %+v", ops)
	}
	if counts[OpPutFile] != 1 {
		t.Fatalf("expected 1 put op, got %+v", ops)
	}
	for _, op := range ops {
		if op.Type == OpRename && (op.Paths[0] != "docs/new.txt" || op.Paths[1] != "docs/renamed.txt") {
			t.Fatalf("unexpected rename pair: %v", op.Paths)
		}
		if op.Type == OpPutFile && (op.Paths[0] != "docs/other.txt" || len(op.Chunks) != 1) {
			t.Fatalf("expected put of other.txt with its chunk record, got %+v", op)
		}
	}
}

func TestSynthesizeDiffOpsDelete(t *testing.T) {
	before := newTestMeta("p")
	cid := before.AllocateChunkID()
	before.Chunks[cid] = ChunkInfo{Size: 2, Offset: 0, Release: "v1", AssetID: 9}
	f := FileMeta{Size: 2, Mode: 0o644, Inode: before.AllocateInode(), Chunks: []int64{cid}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	before.UpsertFile("gone.txt", f, 1700000000)

	after := before.Clone()
	after.RemoveFile("gone.txt")

	ops := synthesizeOpsFromDiff(before, &after, "test", 1700000100)
	if len(ops) != 1 || ops[0].Type != OpDeleteFile {
		t.Fatalf("expected single del op, got %+v", ops)
	}
	if ops[0].FreedChunks != 1 {
		t.Fatalf("expected freed chunk count 1, got %d", ops[0].FreedChunks)
	}
}

func TestSynthesizeDiffOpsDirRename(t *testing.T) {
	before := newTestMeta("p")
	before.EnsureDirectory("olddir", 1700000000)
	before.EnsureDirectory("olddir/sub", 1700000000)
	cid := before.AllocateChunkID()
	before.Chunks[cid] = ChunkInfo{Size: 2, Offset: 0, Release: "v1", AssetID: 9}
	f := FileMeta{Size: 2, Mode: 0o644, Inode: before.AllocateInode(), Chunks: []int64{cid}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}
	before.UpsertFile("olddir/sub/f.txt", f, 1700000000)

	after := before.Clone()
	// Mirror fs.RenameContext: remap the subtree, bump dir/file times.
	remapSubtree(&after, "olddir", "newdir")
	for path, dir := range after.Dirs {
		dir.ModifiedAt = 1700000500
		dir.ChangedAt = 1700000500
		after.Dirs[path] = dir
	}
	file := after.Files["newdir/sub/f.txt"]
	file.ChangedAt = 1700000500
	after.Files["newdir/sub/f.txt"] = file

	ops := synthesizeOpsFromDiff(before, &after, "rename", 1700000500)
	renames := 0
	for _, op := range ops {
		if op.Type != OpRename {
			t.Fatalf("expected only rename ops for a pure subtree rename, got %+v", op)
		}
		renames++
	}
	if renames != 3 {
		t.Fatalf("expected 3 renames (sub, f.txt, top), got %d: %+v", renames, ops)
	}
	// Children must be emitted before the parent directory.
	order := map[string]int{}
	for i, op := range ops {
		order[op.Paths[0]] = i
	}
	if order["olddir/sub"] > order["olddir"] || order["olddir/sub/f.txt"] > order["olddir"] {
		t.Fatalf("expected children-first rename order, got %+v", ops)
	}

	// Replay must land the whole subtree at the new location.
	replayed := before.Clone()
	if err := applyOps(&replayed, ops); err != nil {
		t.Fatalf("applyOps: %v", err)
	}
	replayed.Normalize("p", 1700000600)
	if replayed.FindFile("newdir/sub/f.txt") == nil || replayed.FindFile("olddir/sub/f.txt") != nil {
		t.Fatalf("expected subtree moved, got files %+v", replayed.Files)
	}
	if err := replayed.Validate(); err != nil {
		t.Fatalf("replayed metadata invalid: %v", err)
	}
}

func TestOpSummaryCounts(t *testing.T) {
	counts := opSummaryCounts([]Op{
		{Type: OpPutFile}, {Type: OpPutFile}, {Type: OpDeleteFile}, {Type: OpMkdir},
	})
	if counts != "2 put, 1 del, 1 mkdir" {
		t.Fatalf("unexpected summary counts: %q", counts)
	}
}

func TestHumanizeBytes(t *testing.T) {
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
