package metadata

import (
	"bytes"
	"slices"
	"testing"
)

// --- incremental index == full rebuild ------------------------------------

// compareDerived asserts that two trees agree on every index-visible
// observation: child lists, per-inode families, and all nlink forms.
func compareDerived(t *testing.T, step string, a, b *RepoMetadata) {
	t.Helper()
	seen := map[string]bool{"": true}
	for p := range a.dirs {
		seen[p] = true
	}
	for p := range b.dirs {
		seen[p] = true
	}
	for p := range seen {
		ad, af := a.DirectoryChildren(p)
		bd, bf := b.DirectoryChildren(p)
		if !slices.Equal(ad, bd) || !slices.Equal(af, bf) {
			t.Fatalf("%s: DirectoryChildren(%q): got %v/%v, want %v/%v", step, p, ad, af, bd, bf)
		}
		if an, bn := a.DirNLink(p), b.DirNLink(p); an != bn {
			t.Fatalf("%s: DirNLink(%q): got %d, want %d", step, p, an, bn)
		}
	}
	inodes := map[uint64]bool{}
	for _, f := range a.files {
		inodes[f.Inode] = true
	}
	for _, f := range b.files {
		inodes[f.Inode] = true
	}
	for ino := range inodes {
		if !slices.Equal(a.FindFilesByInode(ino), b.FindFilesByInode(ino)) {
			t.Fatalf("%s: FindFilesByInode(%d): got %v, want %v", step, ino, a.FindFilesByInode(ino), b.FindFilesByInode(ino))
		}
		if an, bn := a.NLink(ino), b.NLink(ino); an != bn {
			t.Fatalf("%s: NLink(%d): got %d, want %d", step, ino, an, bn)
		}
	}
	names := map[string]bool{}
	for n := range a.files {
		names[n] = true
	}
	for n := range b.files {
		names[n] = true
	}
	for n := range names {
		if an, bn := a.FileNLink(n), b.FileNLink(n); an != bn {
			t.Fatalf("%s: FileNLink(%q): got %d, want %d", step, n, an, bn)
		}
	}
}

// TestIncrementalIndexesMatchFullRebuild drives one identical mutation
// script through two trees: `inc` relies on incremental index maintenance,
// while `oracle` is force-invalidated after every step so its next read
// performs a full RebuildIndexes. A freshness assertion proves `inc` never
// silently fell back to a rebuild.
func TestIncrementalIndexesMatchFullRebuild(t *testing.T) {
	t.Parallel()
	now := int64(1000000000000)
	inc := NewRepoMetadata("equiv")
	oracle := NewRepoMetadata("equiv")
	inc.RebuildIndexes()

	apply := func(step string, fn func(m *RepoMetadata)) {
		t.Helper()
		fn(inc)
		fn(oracle)
		oracle.invalidateIndexes()
		if !inc.indexFresh() {
			t.Fatalf("%s: incremental index went stale (a full rebuild happened)", step)
		}
		compareDerived(t, step, inc, oracle)
	}

	apply("mkdir nested", func(m *RepoMetadata) {
		m.EnsureDirectory("a/b/c", now)
		m.EnsureDirectory("docs", now)
	})
	apply("upsert files", func(m *RepoMetadata) {
		m.UpsertFile("a/f1.txt", FileMeta{Size: 5, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1}}, now)
		m.UpsertFile("a/f2.txt", FileMeta{Size: 6, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{2}}, now)
		m.UpsertFile("a/b/f3.txt", FileMeta{Size: 7, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{3}}, now)
	})
	apply("overwrite keeps identity", func(m *RepoMetadata) {
		m.UpsertFile("a/f1.txt", FileMeta{Size: 9, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1}}, now+1)
	})
	apply("type change reallocates identity", func(m *RepoMetadata) {
		m.UpsertFile("a/f2.txt", FileMeta{Symlink: "target", Mode: 0o777, UploadedAt: now, ModifiedAt: now}, now+2)
	})
	apply("hardlink via WriteFileDirect", func(m *RepoMetadata) {
		f := *m.FindFile("a/f1.txt")
		m.WriteFileDirect("a/hard", f)
	})
	apply("inode rewrite via ReplaceFile", func(m *RepoMetadata) {
		h := *m.FindFile("a/hard")
		h.Inode = m.allocateInode()
		m.ReplaceFile("a/hard", h)
	})
	apply("remove file", func(m *RepoMetadata) {
		m.RemoveFile("a/b/f3.txt")
	})
	apply("rename simulation", func(m *RepoMetadata) {
		f := *m.FindFile("a/f1.txt")
		m.RemoveFile("a/f1.txt")
		m.UpsertFile("z/f1.txt", f, now+3)
	})
	apply("remove directory", func(m *RepoMetadata) {
		m.RemoveDirectory("a/b/c")
	})
	apply("root-level churn", func(m *RepoMetadata) {
		m.UpsertFile("top.txt", FileMeta{Size: 1, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{9}}, now)
	})
	apply("root-level removal", func(m *RepoMetadata) {
		m.RemoveFile("top.txt")
	})
	apply("releases do not disturb the index", func(m *RepoMetadata) {
		if _, err := m.EnsureRelease("v1", now); err != nil {
			t.Fatalf("seed release: %v", err)
		}
		m.RemoveRelease("v1")
	})
	apply("empty-inode direct write", func(m *RepoMetadata) {
		m.WriteFileDirect("a/zero", FileMeta{Size: 2, Mode: 0o644, UploadedAt: now, ModifiedAt: now})
	})
	apply("normalize", func(m *RepoMetadata) {
		m.Normalize("equiv", now+10)
	})

	// The incremental tree must still be fresh after the whole script
	// (Normalize rebuilt, which resets sharing/dirtiness cleanly).
	if !inc.indexFresh() {
		t.Fatal("index must be fresh after normalize")
	}
	compareDerived(t, "final", inc, oracle)
}

// --- cheap clone / shared index -------------------------------------------

func TestCloneSharesIndexAndIsolatesMutations(t *testing.T) {
	t.Parallel()
	now := int64(10000000000)
	m := NewRepoMetadata("share")
	m.EnsureDirectory("d", now)
	m.UpsertFile("d/a", FileMeta{Size: 1, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{5}}, now)
	m.RebuildIndexes()
	if !m.indexFresh() {
		t.Fatal("warm tree should have a fresh index")
	}

	c := m.Clone()
	if !m.derived.mapsShared.Load() {
		t.Fatal("Clone must mark the source's index maps shared")
	}
	if !c.indexFresh() {
		t.Fatal("clone reads must hit the shared index without a rebuild")
	}
	if _, files := c.DirectoryChildren("d"); len(files) != 1 {
		t.Fatalf("clone children = %v, want one file", files)
	}
	// Immutable entry values are shared, not deep-copied.
	sa := m.files["d/a"].Chunks
	sb := c.files["d/a"].Chunks
	if len(sa) == 0 || len(sb) == 0 || &sa[0] != &sb[0] {
		t.Fatal("clone should share immutable Chunks backing arrays")
	}

	// Mutating the clone must drop it to a private dirty state and leave
	// the source's (still clean, now-shared) index untouched.
	c.UpsertFile("d/b", FileMeta{Size: 2, Mode: 0o644, UploadedAt: now, ModifiedAt: now}, now)
	if c.indexFresh() {
		t.Fatal("clone mutation must invalidate the shared index")
	}
	if !m.indexFresh() {
		t.Fatal("source index must survive the clone's mutation")
	}
	if _, files := m.DirectoryChildren("d"); len(files) != 1 {
		t.Fatalf("source children = %v, want one file", files)
	}
	if _, files := c.DirectoryChildren("d"); len(files) != 2 {
		t.Fatalf("clone children = %v, want two files", files)
	}

	// Mutating the source afterwards detaches the source (its state is
	// marked shared by the earlier Clone) and never disturbs the clone.
	m.RemoveFile("d/a")
	if m.indexFresh() {
		t.Fatal("source mutation must invalidate its (shared) index")
	}
	if _, files := m.DirectoryChildren("d"); len(files) != 0 {
		t.Fatalf("source children after removal = %v, want none", files)
	}
	if _, files := c.DirectoryChildren("d"); len(files) != 2 {
		t.Fatalf("clone children = %v, must be unaffected by source mutation", files)
	}
}

// --- streaming BuildTree parity -------------------------------------------

func TestBuildTreeStreamParity(t *testing.T) {
	t.Parallel()
	m := sampleTree(t)
	res, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build tree: %v", err)
	}
	collected := map[string][]byte{}
	refs, err := BuildTreeStream(m, nil, nil, func(sha string, data []byte) error {
		collected[sha] = append([]byte(nil), data...)
		return nil
	})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if refs.RootSHA != res.RootSHA || refs.ReleasesSHA != res.ReleasesSHA {
		t.Fatalf("refs mismatch: got %s/%s, want %s/%s", refs.RootSHA, refs.ReleasesSHA, res.RootSHA, res.ReleasesSHA)
	}
	if !slices.Equal(refs.ChunkBuckets, res.ChunkBuckets) {
		t.Fatalf("bucket refs mismatch: %v vs %v", refs.ChunkBuckets, res.ChunkBuckets)
	}
	if len(collected) != len(res.Objects) {
		t.Fatalf("object count mismatch: %d vs %d", len(collected), len(res.Objects))
	}
	for sha, data := range res.Objects {
		if !bytes.Equal(collected[sha], data) {
			t.Fatalf("object %s bytes differ between BuildTree and BuildTreeStream", sha[:12])
		}
	}
}

func TestBuildTreeStreamCacheReuseAndKnown(t *testing.T) {
	t.Parallel()
	m := sampleTree(t)
	res1, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build 1: %v", err)
	}
	before := map[string]string{}
	collectNodeShas(t, m, res1, before)

	cache := NewTreeCache()
	emitted := map[string][]byte{}
	refs1, err := BuildTreeStream(m, cache, nil, func(sha string, data []byte) error {
		emitted[sha] = append([]byte(nil), data...)
		return nil
	})
	if err != nil {
		t.Fatalf("stream 1: %v", err)
	}
	if refs1.RootSHA != res1.RootSHA || len(emitted) != len(res1.Objects) {
		t.Fatalf("cold cache must emit every object: %d vs %d (root %s vs %s)", len(emitted), len(res1.Objects), refs1.RootSHA, res1.RootSHA)
	}

	// Change one deep file; the photos subtree and the release catalog stay
	// identical.
	f := *m.FindFile("docs/2024/jan.txt")
	f.Size = 7
	m.UpsertFile("docs/2024/jan.txt", f, m.LastMod+5)
	res2, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build 2: %v", err)
	}
	emitted2 := map[string][]byte{}
	refs2, err := BuildTreeStream(m, cache, nil, func(sha string, data []byte) error {
		emitted2[sha] = append([]byte(nil), data...)
		return nil
	})
	if err != nil {
		t.Fatalf("stream 2: %v", err)
	}
	if refs2.RootSHA != res2.RootSHA {
		t.Fatal("warm-cache root sha must match a cold full build")
	}
	if _, ok := emitted2[before["photos"]]; ok {
		t.Fatal("unchanged photos subtree must not be re-marshalled/re-emitted")
	}
	if _, ok := emitted2[res2.ReleasesSHA]; ok {
		t.Fatal("unchanged releases object must not be re-emitted")
	}
	if _, ok := emitted2[refs2.RootSHA]; !ok {
		t.Fatal("changed root chain must be emitted")
	}
	// The union of both emissions is exactly the full object set of build 2.
	for sha, data := range res2.Objects {
		got, ok := emitted2[sha]
		if !ok {
			got, ok = emitted[sha]
		}
		if !ok || !bytes.Equal(got, data) {
			t.Fatalf("object %s missing from streamed union", sha[:12])
		}
	}
	for sha := range emitted2 {
		if _, ok := res2.Objects[sha]; !ok {
			t.Fatalf("stream emitted object %s absent from the full build", sha[:12])
		}
	}

	// known(): when every sha is already stored, nothing is emitted but the
	// refs are still exact.
	count := 0
	refs3, err := BuildTreeStream(m, nil, func(string) bool { return true }, func(string, []byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("stream known: %v", err)
	}
	if count != 0 || refs3.RootSHA != refs2.RootSHA || refs3.ReleasesSHA != refs2.ReleasesSHA {
		t.Fatalf("known-sha stream emitted %d objects, refs %+v", count, refs3)
	}
}

// --- size counter == ToJSON length -----------------------------------------

func TestSerializedSizeMatchesToJSON(t *testing.T) {
	t.Parallel()
	now := int64(500000000000)
	m := NewRepoMetadata("size")
	check := func(step string) {
		t.Helper()
		want, err := m.ToJSON()
		if err != nil {
			t.Fatalf("%s: ToJSON: %v", step, err)
		}
		got, err := m.SerializedSize()
		if err != nil {
			t.Fatalf("%s: SerializedSize: %v", step, err)
		}
		if got != len(want) {
			t.Fatalf("%s: SerializedSize()=%d, ToJSON() length=%d", step, got, len(want))
		}
	}

	check("empty")
	m.EnsureDirectory("d", now)
	check("mkdir")
	if !m.derived.sections[secDirs].ok {
		t.Fatal("tracked dir insert must keep the dirs section fresh")
	}
	m.UpsertFile("d/f", FileMeta{Size: 3, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1}, XAttrs: XAttrMap{"user.a": []byte("v")}}, now)
	check("upsert")
	if !m.derived.sections[secFiles].ok {
		t.Fatal("tracked file upsert must keep the files section fresh")
	}
	if err := m.PutChunk(1, ChunkInfo{Size: 3, Offset: 0, AssetID: 11}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	check("put chunk")
	m.SetFileAtime("d/f", now+7)
	check("set atime")
	m.UpsertFile("d/f", FileMeta{Size: 9, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1, 2}}, now+1)
	check("overwrite")
	if err := m.PutChunk(2, ChunkInfo{Size: 6, Offset: 3, AssetID: 12}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	m.UpsertFile(`d/we"ird\ü<>&`, FileMeta{Size: 1, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{3}}, now)
	if err := m.PutChunk(3, ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 13}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	check("escaped path")
	m.UpsertFile("d/link", FileMeta{Symlink: "../f", Mode: 0o777, UploadedAt: now, ModifiedAt: now}, now)
	check("symlink")
	if _, err := m.EnsureRelease("v1", now); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	check("release add")
	m.WriteFileDirect("d/f", FileMeta{Inode: 4242, Size: 2, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{2}})
	check("inode rewrite")
	m.RemoveFile("d/link")
	check("file remove")
	m.RemoveRelease("v1")
	check("release remove")
	m.DeleteChunk(3)
	check("chunk delete")
	m.Normalize("size", now+10)
	check("normalize")
	m.RecomputeStats()
	check("recompute stats")
	m.PruneUnreferencedChunks()
	check("prune")

	// Wholesale map swap (the external rename pattern): the fingerprint
	// forces a recompute and the answer stays exact.
	swapped := make(map[string]FileMeta, len(m.files))
	for k, v := range m.files {
		swapped[k+".moved"] = v
	}
	m.files = swapped
	check("wholesale swap")

	// A clone shares the size state and answers exactly.
	c := m.Clone()
	want, err := c.ToJSON()
	if err != nil {
		t.Fatalf("clone ToJSON: %v", err)
	}
	got, err := c.SerializedSize()
	if err != nil {
		t.Fatalf("clone SerializedSize: %v", err)
	}
	if got != len(want) {
		t.Fatalf("clone: SerializedSize()=%d, ToJSON() length=%d", got, len(want))
	}

	// A zero tree with nil maps serializes as null sections.
	var zero RepoMetadata
	zwant, err := zero.ToJSON()
	if err != nil {
		t.Fatalf("zero ToJSON: %v", err)
	}
	zgot, err := zero.SerializedSize()
	if err != nil {
		t.Fatalf("zero SerializedSize: %v", err)
	}
	if zgot != len(zwant) {
		t.Fatalf("zero tree: SerializedSize()=%d, ToJSON() length=%d", zgot, len(zwant))
	}
}

// TestValidateScratchMapStillDetectsDuplicates pins the per-file duplicate
// detection after the scratch-map reuse.
func TestValidateScratchMapStillDetectsDuplicates(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("dup")
	m.EnsureDirectory("d", 1000000000)
	m.UpsertFile("d/a", FileMeta{Size: 2, Mode: 0o644, UploadedAt: 1000000000, ModifiedAt: 1000000000, Chunks: []int64{1}}, 1000000000)
	m.UpsertFile("d/b", FileMeta{Size: 2, Mode: 0o644, UploadedAt: 1000000000, ModifiedAt: 1000000000, Chunks: []int64{1}}, 1000000000)
	if err := m.PutChunk(1, ChunkInfo{Size: 2, Offset: 0, AssetID: 1}); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	m.RecomputeStats()
	// The fixture hand-forges chunk 1 instead of minting it; lift the
	// counter past the live max for the counter-floor check.
	m.NextChunkID = 2
	if err := m.Validate(); err != nil {
		t.Fatalf("cross-file shared chunk must validate: %v", err)
	}
	dup := *m.FindFile("d/b")
	dup.Chunks = []int64{1, 1}
	m.WriteFileDirect("d/b", dup)
	if err := m.Validate(); err == nil || !bytes.Contains([]byte(err.Error()), []byte("duplicate chunk reference")) {
		t.Fatalf("intra-file duplicate must be rejected, got %v", err)
	}
	// And the next clean file must not inherit the previous file's seen set.
	fixed := dup
	fixed.Chunks = []int64{1}
	m.WriteFileDirect("d/b", fixed)
	if err := m.Validate(); err != nil {
		t.Fatalf("scratch map leaked across files: %v", err)
	}
}

// TestCloneMutationDoesNotCorruptSourceIndex pins the sharing handshake: a
// Clone shares the derived index maps read-only (mapsShared on both sides),
// so when the clone diverges its Files map and mutates through tracked
// methods, the mutation must never be applied in-place to the index maps
// the original tree still reads. A raw struct copy (t := *m) would be a vet
// copylocks error; Clone is the only sanctioned sharing path.
func TestCloneMutationDoesNotCorruptSourceIndex(t *testing.T) {
	t.Parallel()
	now := int64(10000000000)
	m := NewRepoMetadata("valuecopy")
	m.EnsureDirectory("d", now)
	m.UpsertFile("d/a", FileMeta{Size: 1, Mode: 0o644, UploadedAt: now, ModifiedAt: now}, now)
	m.UpsertFile("d/b", FileMeta{Size: 2, Mode: 0o644, UploadedAt: now, ModifiedAt: now}, now)
	m.RebuildIndexes()

	copied := m.Clone()
	diverged := make(map[string]FileMeta, len(m.files))
	for k, v := range m.files {
		diverged[k] = v
	}
	f := diverged["d/a"]
	f.Inode = 777
	diverged["d/a"] = f
	copied.files = diverged
	copied.UpsertFile("d/a", f, now+1) // tracked mutation on the diverged copy

	// The copy's inode must not leak into the source's index.
	if n := m.NLink(777); n != 0 {
		t.Fatalf("source NLink(777) = %d, want 0 (copy mutation leaked into shared index)", n)
	}
	if _, files := m.DirectoryChildren("d"); len(files) != 2 {
		t.Fatalf("source children = %v, want two files", files)
	}
	if !m.indexFresh() {
		t.Fatal("source index must survive the clone mutation")
	}
	if n := copied.NLink(777); n != 1 {
		t.Fatalf("copy NLink(777) = %d, want 1", n)
	}
}

// TestDerivedIndexesSurviveJSONRoundTrip pins that marshalling never touches
// the derived state and decoding starts from a clean slate.
func TestDerivedIndexesSurviveJSONRoundTrip(t *testing.T) {
	t.Parallel()
	m := sampleTree(t)
	data, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	var decoded RepoMetadata
	if err := decoded.FromJSON(data); err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if decoded.derived != nil {
		t.Fatal("decoded tree must not carry derived state")
	}
	decoded.RebuildIndexes()
	aD, aF := m.DirectoryChildren("docs")
	bD, bF := decoded.DirectoryChildren("docs")
	if !slices.Equal(aD, bD) || !slices.Equal(aF, bF) {
		t.Fatalf("round-trip children differ: %v/%v vs %v/%v", aD, aF, bD, bF)
	}
}
