package metadata

import (
	"strings"
	"testing"
)

// Validate must reject map keys the split round-trip would canonicalize
// (mutating or clobbering entries on the way).
func TestValidateRejectsNonCanonicalKeys(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"a/", "a//b", "a/./b", "./a"} {
		m := NewRepoMetadata("demo")
		m.EnsureDirectory("a", 1000000000)
		m.EnsureDirectory("a/b", 1000000000)
		m.files[key] = FileMeta{Inode: 50, Size: 0}
		m.TotalFiles = 1
		if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "canonical") {
			t.Fatalf("key %q not rejected as non-canonical: %v", key, err)
		}
	}
	// Canonical keys (including whitespace-significant names) still pass.
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("a", 1000000000)
	m.UpsertFile("a/b.txt", FileMeta{Size: 0, Inode: 50}, 1000000000)
	m.UpsertFile(" spaced ", FileMeta{Size: 0, Inode: 51}, 1000000000)
	m.Normalize("demo", 1000000000)
	if err := m.Validate(); err != nil {
		t.Fatalf("canonical keys rejected: %v", err)
	}
}

// The chunk-bounds check must be overflow-free and reject overlapping
// (or duplicate-offset) chunk ranges, which make offset binary search
// ambiguous.
func TestValidateRejectsOverflowingAndOverlappingChunks(t *testing.T) {
	t.Parallel()
	// Overflow: Offset+Size wraps int64 negative and slips past a naive
	// `offset+size > fileSize` comparison.
	over := NewRepoMetadata("demo")
	over.chunks[1] = ChunkInfo{Offset: 1 << 62, Size: 1 << 62}
	over.files["f"] = FileMeta{Inode: 2, Size: 1 << 62, Chunks: []int64{1}}
	over.TotalFiles = 1
	over.TotalSize = 1 << 62
	if err := over.Validate(); err == nil || !strings.Contains(err.Error(), "beyond file size") {
		t.Fatalf("overflowing chunk bounds accepted: %v", err)
	}

	// Overlap: A [0,100) and B [50,60) share bytes.
	dup := NewRepoMetadata("demo")
	dup.chunks[1] = ChunkInfo{Offset: 0, Size: 100}
	dup.chunks[2] = ChunkInfo{Offset: 50, Size: 10}
	dup.files["f"] = FileMeta{Inode: 2, Size: 100, Chunks: []int64{1, 2}}
	dup.TotalFiles = 1
	dup.TotalSize = 100
	if err := dup.Validate(); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("overlapping chunks accepted: %v", err)
	}

	// Same start offset, both nonzero size: ambiguous reader target.
	same := NewRepoMetadata("demo")
	same.chunks[1] = ChunkInfo{Offset: 0, Size: 10}
	same.chunks[2] = ChunkInfo{Offset: 0, Size: 5}
	same.files["f"] = FileMeta{Inode: 2, Size: 10, Chunks: []int64{1, 2}}
	same.TotalFiles = 1
	same.TotalSize = 10
	if err := same.Validate(); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("duplicate-offset chunks accepted: %v", err)
	}

	// Disjoint ranges (including a zero-size chunk at a shared boundary)
	// stay legal.
	ok := NewRepoMetadata("demo")
	ok.chunks[1] = ChunkInfo{Offset: 0, Size: 10}
	ok.chunks[2] = ChunkInfo{Offset: 10, Size: 0}
	ok.chunks[3] = ChunkInfo{Offset: 10, Size: 5}
	ok.files["f"] = FileMeta{Inode: 2, Size: 15, Chunks: []int64{1, 2, 3}}
	ok.TotalFiles = 1
	ok.TotalSize = 15
	// Counters must clear the live max under the counter-floor check (the
	// fixture hand-forges entries instead of minting them).
	ok.NextInode = 3
	ok.NextChunkID = 4
	if err := ok.Validate(); err != nil {
		t.Fatalf("disjoint chunks rejected: %v", err)
	}
}

// One path must name one node; a file and a directory sharing a key make
// the FS view ambiguous even though the maps round-trip.
func TestValidateRejectsFileDirPathCollision(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("d", 1000000000)
	m.dirs["x"] = DirMeta{Inode: 10, CreatedAt: 1000000000, ModifiedAt: 1000000000}
	m.files["x"] = FileMeta{Inode: 11, Size: 0}
	m.TotalFiles = 1
	if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "both") {
		t.Fatalf("file/dir path collision accepted: %v", err)
	}
}

// Symlinks must default to the symlink mode, not the regular-file mode.
// Defaults apply on the creation path (UpsertFile); Normalize itself
// preserves stored modes verbatim, including an explicit 000.
func TestSymlinkModeDefaults(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.UpsertFile("lnk", FileMeta{Symlink: "target"}, 100000000000)
	got := m.FindFile("lnk")
	if got == nil || got.Mode != defaultFileMode(NodeKindSymlink) {
		t.Fatalf("symlink creation default = %o, want %o", got.Mode, defaultFileMode(NodeKindSymlink))
	}
	// Explicit modes (including deny-all) survive normalization: 0 is a
	// value, not a gap.
	f := &FileMeta{Symlink: "t"}
	f.Normalize()
	if f.Mode != 0 {
		t.Fatalf("explicit 000 must survive normalize, got %o", f.Mode)
	}
	f.Mode = defaultFileMode(NodeKindSymlink)
	f.Normalize()
	if f.Mode != defaultFileMode(NodeKindSymlink) {
		t.Fatalf("symlink mode changed by normalize: %o", f.Mode)
	}
	// Regular files keep the regular default on creation, and an explicit
	// 000 survives the verbatim store path end to end.
	r := &FileMeta{Size: 1, Chunks: []int64{}}
	InitializeNewFileIdentityFields(r, 200000000000)
	if r.Mode != defaultFileMode(NodeKindFile) {
		t.Fatalf("regular default = %o, want %o", r.Mode, defaultFileMode(NodeKindFile))
	}
	m.UpsertFile("plain.txt", FileMeta{Size: 1}, 200000000000)
	if stored := m.FindFile("plain.txt"); stored == nil || stored.Mode != defaultFileMode(NodeKindFile) {
		t.Fatalf("regular creation default broken: %+v", stored)
	}
	denied := FileMeta{Size: 1, Mode: 0o000, Inode: 4242, UploadedAt: 200000000000, ModifiedAt: 200000000000, AccessedAt: 200000000000, ChangedAt: 200000000000, Chunks: []int64{}}
	m.WriteFileDirect("denied.txt", denied)
	if stored := m.FindFile("denied.txt"); stored == nil || stored.Mode != 0 {
		t.Fatalf("explicit 000 widened on store: %+v", stored)
	}
	// Creation through UpsertFile still defaults an absent mode.
	m.UpsertFile("fresh.txt", FileMeta{Size: 1}, 200000000000)
	if stored := m.FindFile("fresh.txt"); stored == nil || stored.Mode != defaultFileMode(NodeKindFile) {
		t.Fatalf("creation default broken: %+v", stored)
	}
}

// Read-side lookups must canonicalize keys like the write path does.
func TestReadLookupsNormalizeKeys(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("a/b", 1000000000)
	m.UpsertFile("a/b/f.txt", FileMeta{Size: 0, Inode: 9}, 1000000000)
	m.RebuildIndexes()
	if !m.HasDirectory("/a") {
		t.Fatal(`HasDirectory("/a") missed`)
	}
	if m.GetDirectory("a//b") == nil {
		t.Fatal(`GetDirectory("a//b") missed`)
	}
	if got := m.DirNLink("/a"); got != 3 {
		t.Fatalf(`DirNLink("/a") = %d, want 3`, got)
	}
	if got := m.FileNLink("a/b/f.txt/"); got != 1 {
		t.Fatalf(`FileNLink("a/b/f.txt/") = %d, want 1`, got)
	}
}

// FileMeta.Normalize must not reorder chunks by ID - the stored-order
// invariant is offset order, enforced by RepoMetadata.Normalize.
func TestFileMetaNormalizeKeepsChunkOrder(t *testing.T) {
	t.Parallel()
	f := FileMeta{Size: 6, Chunks: []int64{2, 1}}
	f.Normalize()
	if len(f.Chunks) != 2 || f.Chunks[0] != 2 || f.Chunks[1] != 1 {
		t.Fatalf("FileMeta.Normalize reordered chunks by id: %v", f.Chunks)
	}
}

func TestValidationFailuresAndIdentityHelpers(t *testing.T) {
	t.Parallel()
	now := int64(300000000000)
	repo := NewRepoMetadata("validate")
	initializeNewFileIdentity(repo, &FileMeta{}, now)
	existing := &FileMeta{Inode: 42, Mode: 0o777, UID: 7, GID: 9, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now, XAttrs: XAttrMap{"user.demo": []byte("1")}}
	incoming := &FileMeta{}
	preserveFileIdentity(incoming, existing, now+60)
	if incoming.Inode != existing.Inode || string(incoming.XAttrs["user.demo"]) != "1" {
		t.Fatalf("unexpected preserved identity: %+v", incoming)
	}
	// A regular file replacing a symlink is a type change: the old target
	// must never leak onto the incoming file.
	symExisting := &FileMeta{Inode: 43, Mode: 0o777, Symlink: "target", UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}
	symIncoming := &FileMeta{}
	preserveFileIdentity(symIncoming, symExisting, now+60)
	if symIncoming.Symlink != "" {
		t.Fatalf("symlink target leaked onto regular file: %+v", symIncoming)
	}
	badRepo := RepoMetadata{}
	if err := badRepo.Validate(); err == nil || !strings.Contains(err.Error(), "project") {
		t.Fatalf("expected project validation error, got %v", err)
	}
	_ = chooseNonEmpty("", "  ", "x")
}

func TestNormalizeSortsFileChunksByOffsetAndValidateEnforcesOrder(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	if _, err := m.EnsureRelease("v1", 1000000000); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	m.chunks[2] = ChunkInfo{Size: 2, Offset: 4, Release: "v1"}
	m.chunks[1] = ChunkInfo{Size: 4, Offset: 0, Release: "v1"}
	m.UpsertFile("f.bin", FileMeta{Size: 6, Chunks: []int64{2, 1}, Inode: 9}, 1000000000)
	m.Normalize("demo", 1000000000)
	if err := m.Validate(); err != nil {
		t.Fatalf("validate after normalize: %v", err)
	}
	got := m.FindFile("f.bin")
	if got.Chunks[0] != 1 || got.Chunks[1] != 2 {
		t.Fatalf("chunks not sorted by offset: %v", got.Chunks)
	}

	// Out-of-order stored chunks must fail validation loudly.
	bad := NewRepoMetadata("demo")
	if _, err := bad.EnsureRelease("v1", 1000000000); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	bad.chunks[1] = ChunkInfo{Size: 4, Offset: 4, Release: "v1"}
	bad.chunks[2] = ChunkInfo{Size: 4, Offset: 0, Release: "v1"}
	bad.UpsertFile("g.bin", FileMeta{Size: 8, Chunks: []int64{1, 2}, Inode: 9}, 1000000000)
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "offset order") {
		t.Fatalf("expected offset-order violation, got %v", err)
	}
}

func TestDirNLinkCountsEachSubdirectoryOnce(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("docs/a", 1000000000)
	m.EnsureDirectory("docs/b", 1000000000)
	m.RebuildIndexes()
	if got := m.DirNLink("docs"); got != 4 {
		t.Fatalf("indexed DirNLink = %d, want 4", got)
	}
	m.invalidateIndexes()
	if got := m.DirNLink("docs"); got != 4 {
		t.Fatalf("unindexed DirNLink = %d, want 4 (index-state independent)", got)
	}
	if got := m.DirNLink(""); got != 3 {
		t.Fatalf("root DirNLink = %d, want 3 (one child dir)", got)
	}
}

func TestZeroOwnerSurvivesNormalize(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("rooted", 1000000000)
	dir := m.dirs["rooted"]
	dir.UID = 0
	dir.GID = 0
	m.dirs["rooted"] = dir
	m.Normalize("demo", 1000000000)
	if m.dirs["rooted"].UID != 0 || m.dirs["rooted"].GID != 0 {
		t.Fatalf("zero owner was clobbered by normalize: %+v", m.dirs["rooted"])
	}
}

// TestNormalizePreservesAuthoritativeZeros pins the v5 contract: Normalize
// never repairs timestamps - persisted values are complete and exact, and
// zero is a real epoch value, not a gap. Legacy completion belongs to the
// stacked migrator (migrate.go), not to the parser or normalizer.
func TestNormalizePreservesAuthoritativeZeros(t *testing.T) {
	t.Parallel()
	f := &FileMeta{}
	f.Normalize()
	if f.UploadedAt != 0 || f.ModifiedAt != 0 || f.AccessedAt != 0 || f.ChangedAt != 0 {
		t.Fatalf("normalize backfilled zeros: %+v", f)
	}
	d := &DirMeta{}
	d.Normalize()
	if d.CreatedAt != 0 || d.ModifiedAt != 0 {
		t.Fatalf("dir normalize backfilled zeros: %+v", d)
	}

	// Creation-time stamping stays: UpsertFile materializes identity.
	m := NewRepoMetadata("demo")
	m.UpsertFile("n.txt", FileMeta{Size: 1}, 1700000000000000000)
	stored := m.FindFile("n.txt")
	if stored.UploadedAt != 1700000000000000000 || stored.ChangedAt != 1700000000000000000 {
		t.Fatalf("creation stamping broken: %+v", stored)
	}
}

func TestPathNormalizerConformance(t *testing.T) {
	t.Parallel()
	fsNormalize := func(v string) string {
		cleaned, err := fsNormalizeForTest(v)
		if err != nil {
			return "<error:" + err.Error() + ">"
		}
		return cleaned
	}
	for _, in := range []string{
		"", " ", "   ", "/", "//", "docs", "/docs", " docs ", "  docs",
		"docs/", "a/b/c", "./a", "a/./b", "a//b", "/a/b/../c",
		" /x/../y ", "..", "../esc", "a/../..", " .hidden ", "...",
	} {
		want := fsNormalize(in)
		got := normalizeStoredPath(in)
		switch {
		case strings.HasPrefix(want, "<error:"):
			if want == "<error:path is required>" && got != "" {
				t.Errorf("input %q: fs rejects as required, metadata gave %q", in, got)
			}
			if strings.Contains(want, "escapes root") {
				// metadata stays total; Validate rejects escaping keys.
				continue
			}
		default:
			if got != want {
				t.Errorf("input %q: metadata %q != fs %q", in, got, want)
			}
		}
	}
}

func TestSchemaV4KeysRoundTrip(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("d", 1000000000)
	m.UpsertFile("d/f.txt", FileMeta{Size: 1, Chunks: []int64{7}, UploadedAt: 5000000000, ModifiedAt: 6000000000, AccessedAt: 7000000000, ChangedAt: 8000000000, Inode: 9}, 1000000000)
	blob, err := m.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	s := string(blob)
	for _, legacy := range []string{`"ca"`, `"cha"`} {
		if strings.Contains(s, legacy) {
			t.Fatalf("legacy key %s must not be written by v5", legacy)
		}
	}
	for _, want := range []string{`"cr":`, `"ch":`} {
		if !strings.Contains(s, want) {
			t.Fatalf("v5 key %s missing from output", want)
		}
	}
	var back RepoMetadata
	if err := back.FromJSON(blob); err != nil {
		t.Fatal(err)
	}
	f := back.FindFile("d/f.txt")
	if f == nil {
		t.Fatal("file missing after round trip")
	}
	if f.ChangedAt != 8000000000 || f.ModifiedAt != 6000000000 || f.AccessedAt != 7000000000 {
		t.Fatalf("file timestamps lost in round trip: %+v", f)
	}
	if d, ok := back.dirs["d"]; !ok || d.CreatedAt <= 0 || d.ChangedAt <= 0 {
		t.Fatalf("dir timestamps lost in round trip: %+v", d)
	}
}
