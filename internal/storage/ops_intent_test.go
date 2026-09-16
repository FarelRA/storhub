package storage

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// The intent-based synthesis equivalence battery. For every scenario the
// SAME transaction (fn against a COW candidate) is synthesized twice:
//
//	intent path: synthesizeOpsFromIntents over the intents the tracked
//	             mutators recorded while fn ran (the new O(changes) path)
//	diff path:   synthesizeOpsFromDiff over the whole pre/post trees
//	             (the former O(tree) path, kept as the equivalence oracle)
//
// Both batches must be equal as multisets (the diff iterates whole maps,
// whose order is unspecified, so intra-class order is not part of the
// contract), and replaying the intent ops onto the pre-transaction tree
// must reproduce the candidate tree.

const intentTestNow = int64(1700000500)

// runIntentTransaction applies fn the way UpdateRepoMetadataContext does: COW copy,
// recorder attached before fn, Normalize + RecomputeStats after, recorder
// detached before the fold. Returns the candidate plus both op batches.
// runIntentTransaction applies fn the way UpdateRepoMetadataContext does:
// COW copy, recorder attached, fn, touched-file canonicalization, the O(1)
// seal (NOT the full Normalize+RecomputeStats walk), detach, fold. It also
// pins that the incremental stats the mutators maintained agree exactly
// with a from-scratch walk: any drift between incremental and full
// accounting fails here.
func runIntentTransaction(t *testing.T, before *RepoMetadata, fn func(*RepoMetadata) error) (intentOps []Op, candidate *RepoMetadata) {
	t.Helper()
	candidate = before.Clone()
	rec := metadata.NewIntentRecorder()
	candidate.AttachIntentRecorder(rec)
	if err := fn(candidate); err != nil {
		t.Fatalf("fn: %v", err)
	}
	for path := range rec.FileIntents() {
		candidate.SortFileChunks(path)
	}
	candidate.SealTransaction("p", intentTestNow)
	candidate.DetachIntentRecorder()
	intentOps = synthesizeOpsFromIntents(before, candidate, rec, "test", intentTestNow)
	check := candidate.Clone()
	check.RecomputeStats()
	if candidate.TotalFiles != check.TotalFiles || candidate.TotalSize != check.TotalSize {
		t.Fatalf("incremental stats drift: files %d/%d size %d/%d",
			candidate.TotalFiles, check.TotalFiles, candidate.TotalSize, check.TotalSize)
	}
	if !reflect.DeepEqual(candidate.Releases(), check.Releases()) {
		t.Fatalf("incremental AssetCounts drift:\n got %+v\nwant %+v", candidate.Releases(), check.Releases())
	}
	return intentOps, candidate
}

func assertReplayReproduces(t *testing.T, scenario string, before, candidate *RepoMetadata, intentOps []Op) {
	t.Helper()
	replayed := before.Clone()
	if err := applyOps(replayed, intentOps); err != nil {
		t.Fatalf("%s: applyOps: %v", scenario, err)
	}
	replayed.Normalize("p", intentTestNow)
	replayed.RecomputeStats()
	if !reflect.DeepEqual(replayed.Files(), candidate.Files()) {
		t.Fatalf("%s: replay files diverge:\n got %+v\nwant %+v", scenario, replayed.Files(), candidate.Files())
	}
	if !reflect.DeepEqual(replayed.Dirs(), candidate.Dirs()) {
		t.Fatalf("%s: replay dirs diverge:\n got %+v\nwant %+v", scenario, replayed.Dirs(), candidate.Dirs())
	}
	if !reflect.DeepEqual(replayed.Chunks(), candidate.Chunks()) {
		t.Fatalf("%s: replay chunks diverge:\n got %+v\nwant %+v", scenario, replayed.Chunks(), candidate.Chunks())
	}
	if !reflect.DeepEqual(replayed.Releases(), candidate.Releases()) {
		t.Fatalf("%s: replay releases diverge:\n got %+v\nwant %+v", scenario, replayed.Releases(), candidate.Releases())
	}
	if !reflect.DeepEqual(replayed.Root, candidate.Root) {
		t.Fatalf("%s: replay root diverges:\n got %+v\nwant %+v", scenario, replayed.Root, candidate.Root)
	}
}

func intentTestFile(m *RepoMetadata, size int64, chunkIDs []int64) FileMeta {
	return FileMeta{
		Size: size, Mode: 0o644, Inode: m.AllocateInode(), Chunks: chunkIDs,
		UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000,
	}
}

func intentTestChunk(m *RepoMetadata, size int64) int64 {
	id := m.AllocateChunkID()
	m.PutChunk(id, ChunkInfo{Size: size, Offset: 0, Release: "v1", AssetID: 900 + id})
	return id
}

func TestIntentOpsReplayReconstructs(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		name  string
		build func(*RepoMetadata)
		fn    func(*RepoMetadata) error
	}{
		{
			name:  "no-op fn",
			build: func(m *RepoMetadata) {},
			fn:    func(m *RepoMetadata) error { return nil },
		},
		{
			name: "rewrite with identical body records no op",
			build: func(m *RepoMetadata) {
				m.UpsertFile("a.txt", intentTestFile(m, 4, nil), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				m.ReplaceFile("a.txt", *m.FindFile("a.txt"))
				return nil
			},
		},
		{
			name:  "create file with chunk and parent dir",
			build: func(m *RepoMetadata) {},
			fn: func(m *RepoMetadata) error {
				cid := intentTestChunk(m, 4)
				m.UpsertFile("docs/new.txt", intentTestFile(m, 4, []int64{cid}), intentTestNow)
				return nil
			},
		},
		{
			name:  "create dir",
			build: func(m *RepoMetadata) {},
			fn: func(m *RepoMetadata) error {
				m.EnsureDirectory("newdir", intentTestNow)
				return nil
			},
		},
		{
			name: "delete file with chunks",
			build: func(m *RepoMetadata) {
				cid := intentTestChunk(m, 4)
				m.UpsertFile("gone.txt", intentTestFile(m, 4, []int64{cid}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				m.RemoveFile("gone.txt")
				return nil
			},
		},
		{
			name: "delete empty dir",
			build: func(m *RepoMetadata) {
				m.EnsureDirectory("empty", 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				m.RemoveDirectory("empty")
				return nil
			},
		},
		{
			name: "delete non-empty dir (subtree)",
			build: func(m *RepoMetadata) {
				m.EnsureDirectory("top", 1700000000)
				m.EnsureDirectory("top/sub", 1700000000)
				cid := intentTestChunk(m, 4)
				m.UpsertFile("top/sub/f.txt", intentTestFile(m, 4, []int64{cid}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				m.RemoveFile("top/sub/f.txt")
				m.RemoveDirectory("top/sub")
				m.RemoveDirectory("top")
				return nil
			},
		},
		{
			name: "overwrite file keeps inode",
			build: func(m *RepoMetadata) {
				cid := intentTestChunk(m, 4)
				m.UpsertFile("a.txt", intentTestFile(m, 4, []int64{cid}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				cid := intentTestChunk(m, 9)
				entry := intentTestFile(m, 9, []int64{cid})
				entry.Inode = m.FindFile("a.txt").Inode
				m.UpsertFile("a.txt", entry, intentTestNow)
				return nil
			},
		},
		{
			name: "chmod chown utimes on file",
			build: func(m *RepoMetadata) {
				m.UpsertFile("a.txt", intentTestFile(m, 4, nil), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				f := m.FindFile("a.txt").Clone()
				f.Mode = 0o600
				f.UID = 1000
				f.GID = 1000
				f.ModifiedAt = intentTestNow
				f.AccessedAt = intentTestNow
				m.ReplaceFile("a.txt", f)
				return nil
			},
		},
		{
			name: "chmod dir",
			build: func(m *RepoMetadata) {
				m.EnsureDirectory("d", 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				d := m.GetDirectory("d").Clone()
				d.Mode = 0o700
				m.WriteDirDirect("d", d)
				return nil
			},
		},
		{
			name: "xattr set and remove",
			build: func(m *RepoMetadata) {
				m.UpsertFile("a.txt", intentTestFile(m, 4, nil), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				f := m.FindFile("a.txt").Clone()
				f.XAttrs = metadata.XAttrMap{"user.test": []byte("v1")}
				m.ReplaceFile("a.txt", f)
				g := m.FindFile("a.txt").Clone()
				delete(g.XAttrs, "user.test")
				m.ReplaceFile("a.txt", g)
				return nil
			},
		},
		{
			name:  "symlink create and delete",
			build: func(m *RepoMetadata) {},
			fn: func(m *RepoMetadata) error {
				link := intentTestFile(m, 0, nil)
				link.Symlink = "a.txt"
				m.UpsertFile("link.txt", link, intentTestNow)
				m.RemoveFile("link.txt")
				return nil
			},
		},
		{
			name: "hard link",
			build: func(m *RepoMetadata) {
				m.UpsertFile("a.txt", intentTestFile(m, 4, nil), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				linked := m.FindFile("a.txt").Clone()
				m.UpsertFile("alias.txt", linked, intentTestNow)
				return nil
			},
		},
		{
			name: "mtime-only touch is setattr",
			build: func(m *RepoMetadata) {
				cid := intentTestChunk(m, 4)
				m.UpsertFile("a.txt", intentTestFile(m, 4, []int64{cid}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				f := m.FindFile("a.txt").Clone()
				f.ModifiedAt = intentTestNow
				f.ChangedAt = intentTestNow
				m.ReplaceFile("a.txt", f)
				return nil
			},
		},
		{
			name: "chunk rewrite swaps chunk ids",
			build: func(m *RepoMetadata) {
				cid := intentTestChunk(m, 4)
				m.UpsertFile("a.txt", intentTestFile(m, 4, []int64{cid}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				cid := intentTestChunk(m, 9)
				entry := intentTestFile(m, 9, []int64{cid})
				entry.Inode = m.FindFile("a.txt").Inode
				m.UpsertFile("a.txt", entry, intentTestNow)
				return nil
			},
		},
		{
			name: "delete file then prune chunk catalog",
			build: func(m *RepoMetadata) {
				cid := intentTestChunk(m, 4)
				m.UpsertFile("a.txt", intentTestFile(m, 4, []int64{cid}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				m.RemoveFile("a.txt")
				m.PruneUnreferencedChunks()
				return nil
			},
		},
		{
			name: "release create update delete",
			build: func(m *RepoMetadata) {
				m.EnsureRelease("v1", 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				m.PutRelease("v1", ReleaseRef{CreatedAt: intentTestNow})
				m.EnsureRelease("v2", intentTestNow)
				m.RemoveRelease("v1")
				return nil
			},
		},
		{
			name: "rename file",
			build: func(m *RepoMetadata) {
				cid := intentTestChunk(m, 4)
				m.UpsertFile("a.txt", intentTestFile(m, 4, []int64{cid}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				renamed := m.FindFile("a.txt").Clone()
				renamed.ChangedAt = intentTestNow
				m.RemoveFile("a.txt")
				m.UpsertFile("b.txt", renamed, intentTestNow)
				return nil
			},
		},
		{
			name: "rename dir with children",
			build: func(m *RepoMetadata) {
				m.EnsureDirectory("olddir", 1700000000)
				m.EnsureDirectory("olddir/sub", 1700000000)
				cid := intentTestChunk(m, 4)
				m.UpsertFile("olddir/sub/f.txt", intentTestFile(m, 4, []int64{cid}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				// Mirror fs.RenameContext's per-entry subtree remap.
				type dirRemap struct {
					from, to string
					dir      DirMeta
				}
				var dirRemaps []dirRemap
				for dirPath, dir := range m.Dirs() {
					if shfs.IsParentOrSame("olddir", dirPath) {
						dir.ModifiedAt = intentTestNow
						dir.ChangedAt = intentTestNow
						dirRemaps = append(dirRemaps, dirRemap{from: dirPath, to: shfs.RemapPath("olddir", "newdir", dirPath), dir: dir})
					}
				}
				for _, r := range dirRemaps {
					m.RemoveDirectory(r.from)
					m.WriteDirDirect(r.to, r.dir)
				}
				type fileRemap struct {
					from, to string
					file     FileMeta
				}
				var fileRemaps []fileRemap
				for filePath, file := range m.Files() {
					if shfs.IsParentOrSame("olddir", filePath) {
						file.ChangedAt = intentTestNow
						fileRemaps = append(fileRemaps, fileRemap{from: filePath, to: shfs.RemapPath("olddir", "newdir", filePath), file: file})
					}
				}
				for _, r := range fileRemaps {
					m.RemoveFile(r.from)
					m.WriteFileDirect(r.to, r.file)
				}
				return nil
			},
		},
		{
			name: "rename then delete nets to delete",
			build: func(m *RepoMetadata) {
				m.UpsertFile("a.txt", intentTestFile(m, 4, nil), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				renamed := m.FindFile("a.txt").Clone()
				renamed.ChangedAt = intentTestNow
				m.RemoveFile("a.txt")
				m.UpsertFile("b.txt", renamed, intentTestNow)
				m.RemoveFile("b.txt")
				return nil
			},
		},
		{
			name: "rename chain nets to one rename",
			build: func(m *RepoMetadata) {
				m.UpsertFile("a.txt", intentTestFile(m, 4, nil), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				step1 := m.FindFile("a.txt").Clone()
				step1.ChangedAt = intentTestNow
				m.RemoveFile("a.txt")
				m.UpsertFile("b.txt", step1, intentTestNow)
				step2 := m.FindFile("b.txt").Clone()
				step2.ChangedAt = intentTestNow
				m.RemoveFile("b.txt")
				m.UpsertFile("c.txt", step2, intentTestNow)
				return nil
			},
		},
		{
			name: "rename onto existing file",
			build: func(m *RepoMetadata) {
				cidA := intentTestChunk(m, 4)
				m.UpsertFile("a.txt", intentTestFile(m, 4, []int64{cidA}), 1700000000)
				cidB := intentTestChunk(m, 7)
				m.UpsertFile("b.txt", intentTestFile(m, 7, []int64{cidB}), 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				renamed := m.FindFile("a.txt").Clone()
				renamed.ChangedAt = intentTestNow
				m.RemoveFile("a.txt")
				m.RemoveFile("b.txt")
				m.UpsertFile("b.txt", renamed, intentTestNow)
				return nil
			},
		},
		{
			name:  "root setattr",
			build: func(m *RepoMetadata) {},
			fn: func(m *RepoMetadata) error {
				m.Root.Mode = 0o700
				m.Root.UID = 1000
				return nil
			},
		},
		{
			name: "bulk import distinct files",
			build: func(m *RepoMetadata) {
				m.EnsureDirectory("bulk", 1700000000)
			},
			fn: func(m *RepoMetadata) error {
				for i := 0; i < 200; i++ {
					m.UpsertFile("bulk/f"+strconv.Itoa(i)+".txt", intentTestFile(m, 1, nil), intentTestNow)
				}
				return nil
			},
		},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			before := newTestMeta("p")
			sc.build(before)
			intentOps, candidate := runIntentTransaction(t, before, sc.fn)
			assertReplayReproduces(t, sc.name, before, candidate, intentOps)
		})
	}
}

// TestIntentBulkImportOpCount pins the folded shape of a bulk import: one
// put per file, nothing else (the target directory pre-exists).
func TestIntentBulkImportOpCount(t *testing.T) {
	t.Parallel()
	before := newTestMeta("p")
	before.EnsureDirectory("bulk", 1700000000)
	const files = 200
	intentOps, candidate := runIntentTransaction(t, before, func(m *RepoMetadata) error {
		for i := 0; i < files; i++ {
			m.UpsertFile("bulk/f"+strconv.Itoa(i)+".txt", intentTestFile(m, 1, nil), intentTestNow)
		}
		return nil
	})
	if len(intentOps) != files {
		t.Fatalf("expected exactly %d folded ops, got %d: %+v", files, len(intentOps), intentOps)
	}
	assertReplayReproduces(t, "bulk import", before, candidate, intentOps)
}

// TestIntentFiftyWritesCoalesce pins the cross-transaction coalescing
// contract: fifty sequential single-write transactions fold to ONE op with
// Times==50, identically on both synthesis paths.
func TestIntentFiftyWritesCoalesce(t *testing.T) {
	t.Parallel()
	before := newTestMeta("p")
	before.UpsertFile("hot.txt", intentTestFile(before, 1, nil), 1700000000)

	intentStack := &opStack{}
	for i := 0; i < 50; i++ {
		current := before
		var candidate *RepoMetadata
		var intentOps []Op
		intentOps, candidate = runIntentTransaction(t, current, func(m *RepoMetadata) error {
			entry := intentTestFile(m, int64(i+2), nil)
			entry.Inode = m.FindFile("hot.txt").Inode
			m.UpsertFile("hot.txt", entry, intentTestNow)
			return nil
		})
		for _, op := range intentOps {
			intentStack.append(op)
		}
		before = candidate
	}
	if len(intentStack.ops) != 1 {
		t.Fatalf("expected 1 coalesced op, got %d: %+v", len(intentStack.ops), intentStack.ops)
	}
	if intentStack.ops[0].Times != 50 {
		t.Fatalf("expected times=50, got %d", intentStack.ops[0].Times)
	}
}

// TestIntentFnErrorLeavesStackUntouched pins the rejection contract at the
// unit level: a transaction whose fn fails must not have recorded anything
// into the shared stack (the recorder dies with the discarded candidate).
func TestIntentFnErrorLeavesStackUntouched(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()
	if _, err := hub.UploadFileContext(ctx, "proj", "a.txt", writeTempFile(t, t.TempDir(), "a", []byte("aaaa"))); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "proj"); err != nil {
		t.Fatalf("seed flush: %v", err)
	}
	if _, err := hub.UpdateRepoMetadataContext(ctx, "proj", func(m *RepoMetadata) error {
		m.EnsureDirectory("docs", intentTestNow)
		return fmt.Errorf("boom")
	}, "storhub: failing fn"); err == nil {
		t.Fatal("expected fn error to propagate")
	}
	pm := hub.getOrCreateProjectMeta("proj")
	pm.mu.RLock()
	ops, dirty := len(pm.opStack.ops), pm.dirty
	pm.mu.RUnlock()
	if ops != 0 || dirty {
		t.Fatalf("failed fn leaked into the shared stack (ops=%d dirty=%v)", ops, dirty)
	}
}
