package metadata

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestHelpersAndPOSIXUtilities(t *testing.T) {
	if normalizeStoredPath(" /docs/specs/../guide.txt ") != "docs/guide.txt" {
		t.Fatal("unexpected normalized path")
	}
	if normalizeStoredPath("../escape") != "../escape" {
		t.Fatal("expected escaping path to stay trimmed")
	}
	if parentPath("docs/guide.txt") != "docs" || parentPath("guide.txt") != "" {
		t.Fatal("unexpected parent paths")
	}
	if defaultFileMode(NodeKindFile) != 0o644 || defaultFileMode(NodeKindSymlink) != 0o777 || defaultDirMode() != 0o755 {
		t.Fatal("unexpected mode defaults")
	}
	if normalizeXAttrs(XAttrMap{"": []byte("x")}) != nil {
		t.Fatal("expected nil map normalization")
	}
	attrs := normalizeXAttrs(XAttrMap{"user.a": []byte("1"), "": []byte("skip")})
	if len(attrs) != 1 || string(attrs["user.a"]) != "1" {
		t.Fatalf("unexpected xattrs: %+v", attrs)
	}
	when := chooseNonZeroTime(0, 5)
	if when == 0 {
		t.Fatalf("unexpected chosen time: %v", when)
	}
	if got, ok := ParseNumericReleaseTag("v12"); !ok || got != 12 {
		t.Fatalf("unexpected release tag parse: %d %v", got, ok)
	}
	if _, ok := ParseNumericReleaseTag("feature"); ok {
		t.Fatal("expected non-numeric tag parse failure")
	}
}

func TestRepoMetadataNormalizeCloneAndIndexes(t *testing.T) {
	now := int64(100)
	repo := NewRepoMetadata("demo")

	repo.EnsureDirectory("docs", now)
	repo.EnsureDirectory("docs/sub", now)
	repo.Dirs["docs"] = DirMeta{Inode: 2, CreatedAt: now, ModifiedAt: now}
	repo.Dirs["docs/sub"] = DirMeta{Inode: 3, XAttrs: XAttrMap{"user.dir": []byte("1")}, CreatedAt: now, ModifiedAt: now}

	repo.EnsureRelease("v2", now)
	repo.UpsertFile("docs/sub/file.txt", FileMeta{Size: 3, Inode: 5, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{2, 1}}, now)
	repo.Chunks[1] = ChunkInfo{Offset: 0, Size: 2, AssetID: 1}
	repo.Chunks[2] = ChunkInfo{Offset: 2, Size: 1, AssetID: 2}
	repo.UpsertFile("docs/sub/link", FileMeta{Inode: 4, Mode: 0o777, Symlink: "target", UploadedAt: now, ModifiedAt: now}, now)

	repo.Normalize("demo", now)
	if err := repo.Validate(); err != nil {
		t.Fatalf("validate normalized repo: %v", err)
	}
	if repo.Version != maxMetadataVersion || repo.Project != "demo" {
		t.Fatalf("unexpected normalized repo: %+v", repo)
	}
	if repo.Root.Inode == 0 {
		t.Fatal("expected root inode to be set")
	}
	if !repo.HasDirectory("docs") || !repo.HasDirectory("docs/sub") {
		t.Fatalf("expected directories to exist")
	}
	file := repo.FindFile("docs/sub/file.txt")
	if file == nil || len(file.Chunks) != 2 || file.Chunks[0] != 1 {
		t.Fatalf("unexpected file metadata: %+v", file)
	}
	if link := repo.FindFile("docs/sub/link"); link == nil || link.Symlink != "target" || link.Size != int64(len("target")) || len(link.Chunks) != 0 {
		t.Fatalf("unexpected symlink metadata: %+v", link)
	}
	dirs, files := repo.DirectoryChildren("docs/sub")
	if len(dirs) != 0 || len(files) != 2 {
		t.Fatalf("unexpected children: dirs=%+v files=%+v", dirs, files)
	}
	byInode := repo.FindFilesByInode(file.Inode)
	if len(byInode) != 1 || byInode[0] != "docs/sub/file.txt" {
		t.Fatalf("unexpected inode lookup: %+v", byInode)
	}
	repo.Root.XAttrs = XAttrMap{"user.root": []byte("1")}
	clone := repo.Clone()
	clonedRoot := &clone.Root
	origRoot := &repo.Root
	clonedRoot.XAttrs["user.root"] = []byte("mutated")
	if string(origRoot.XAttrs["user.root"]) != "1" {
		t.Fatal("expected deep clone of root attrs (mutation must not alias)")
	}
	allFiles := repo.AllFiles()
	allFiles[0].Mode = 0
	if repo.AllFiles()[0].Mode == 0 {
		t.Fatal("expected all-files copy")
	}
	encoded, err := repo.ToJSON()
	if err != nil {
		t.Fatalf("to json: %v", err)
	}
	var decoded RepoMetadata
	if err := decoded.FromJSON(encoded); err != nil {
		t.Fatalf("from json: %v", err)
	}
	decoded.Normalize("demo", now)
	if err := decoded.Validate(); err != nil {
		t.Fatalf("validate decoded repo: %v", err)
	}
	if _, err := json.Marshal(repo); err != nil {
		t.Fatalf("marshal normalized repo: %v", err)
	}
}

func TestRepoMetadataMutationFlows(t *testing.T) {
	now := int64(200)
	repo := NewRepoMetadata("mutations")
	repo.EnsureDirectory("docs/specs", now)
	if !repo.HasDirectory("docs") || !repo.HasDirectory("docs/specs") {
		t.Fatalf("expected ensured directories")
	}
	release := repo.EnsureRelease("v1", now)
	if release == nil || release.AssetCount != 0 {
		t.Fatalf("unexpected release: %+v", release)
	}
	release = repo.EnsureRelease("v1", now)
	if release == nil || release.AssetCount != 0 {
		t.Fatalf("unexpected release: %+v", release)
	}
	fileMeta := FileMeta{Size: 3, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1}}
	repo.Chunks[1] = ChunkInfo{Offset: 0, Size: 3, AssetID: 1}
	repo.UpsertFile("docs/specs/readme.txt", fileMeta, now)
	first := repo.FindFile("docs/specs/readme.txt")
	if first == nil || first.Inode == 0 {
		t.Fatalf("unexpected inserted file: %+v", first)
	}
	if n := repo.FileNLink("docs/specs/readme.txt"); n != 1 {
		t.Fatalf("expected nlink=1, got %d", n)
	}
	repo.UpsertFile("docs/specs/readme.txt", FileMeta{Size: 4, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{2}}, now+60)
	repo.Chunks[2] = ChunkInfo{Offset: 0, Size: 4, AssetID: 2}
	updated := repo.FindFile("docs/specs/readme.txt")
	if updated == nil || updated.Inode != first.Inode || updated.Size != 4 {
		t.Fatalf("expected identity preserved on upsert: first=%+v updated=%+v", first, updated)
	}
	if !repo.RemoveFile("docs/specs/readme.txt") || repo.FindFile("docs/specs/readme.txt") != nil {
		t.Fatal("expected file removal")
	}
	if !repo.RemoveDirectory("docs/specs") || repo.HasDirectory("docs/specs") {
		t.Fatal("expected directory removal")
	}
	if !repo.RemoveRelease("v1") {
		t.Fatal("expected release removal")
	}
	if repo.RemoveFile("missing") || repo.RemoveDirectory("missing") || repo.RemoveRelease("missing") {
		t.Fatal("expected missing removals to return false")
	}
	before := repo.AllocateInode()
	after := repo.AllocateInode()
	if after <= before {
		t.Fatalf("expected increasing inode allocation: %d %d", before, after)
	}
	repo.RebuildIndexes()
}

func TestValidationFailuresAndIdentityHelpers(t *testing.T) {
	now := int64(300)
	repo := NewRepoMetadata("validate")
	InitializeNewFileIdentity(repo, &FileMeta{}, now)
	existing := &FileMeta{Inode: 42, Mode: 0o777, UID: 7, GID: 9, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now, XAttrs: XAttrMap{"user.demo": []byte("1")}}
	incoming := &FileMeta{}
	PreserveFileIdentity(incoming, existing, now+60)
	if incoming.Inode != existing.Inode || string(incoming.XAttrs["user.demo"]) != "1" {
		t.Fatalf("unexpected preserved identity: %+v", incoming)
	}
	// A regular file replacing a symlink is a type change: the old target
	// must never leak onto the incoming file.
	symExisting := &FileMeta{Inode: 43, Mode: 0o777, Symlink: "target", UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}
	symIncoming := &FileMeta{}
	PreserveFileIdentity(symIncoming, symExisting, now+60)
	if symIncoming.Symlink != "" {
		t.Fatalf("symlink target leaked onto regular file: %+v", symIncoming)
	}
	badRepo := RepoMetadata{}
	if err := badRepo.Validate(); err == nil || !strings.Contains(err.Error(), "project") {
		t.Fatalf("expected project validation error, got %v", err)
	}
	_ = chooseNonEmpty("", "  ", "x")
}

func TestMigrateV1ChunkNameCollision(t *testing.T) {
	v1JSON := `{
		"version": 1,
		"project": "demo",
		"next_inode": 10,
		"total_files": 2,
		"total_size": 100,
		"last_modified": "2024-01-01T00:00:00Z",
		"root": {
			"inode": 1, "mode": 755, "uid": 1000, "gid": 1000,
			"nlink": 2, "created_at": "2024-01-01T00:00:00Z",
			"modified_at": "2024-01-01T00:00:00Z"
		},
		"directories": [],
		"releases": [
			{
				"tag": "v1",
				"asset_count": 1,
				"created_at": "2024-01-01T00:00:00Z",
				"files": [
					{
						"name": "bigfile.bin",
						"kind": "file",
						"size": 100,
						"mode": 644, "uid": 1000, "gid": 1000,
						"inode": 5, "nlink": 1,
						"uploaded_at": "2024-01-01T00:00:00Z",
						"chunks": [
							{"name": "shared.chunk", "size": 50, "index": 0, "offset": 0, "release": "v1", "asset_offset": 0, "asset_id": 100},
							{"name": "bigfile.unique", "size": 50, "index": 1, "offset": 50, "release": "v1", "asset_offset": 0, "asset_id": 101}
						]
					}
				]
			},
			{
				"tag": "v2",
				"asset_count": 1,
				"created_at": "2024-02-01T00:00:00Z",
				"files": [
					{
						"name": "small.txt",
						"kind": "file",
						"size": 30,
						"mode": 644, "uid": 1000, "gid": 1000,
						"inode": 6, "nlink": 1,
						"uploaded_at": "2024-02-01T00:00:00Z",
						"chunks": [
							{"name": "shared.chunk", "size": 30, "index": 0, "offset": 0, "release": "v2", "asset_offset": 0, "asset_id": 200}
						]
					}
				]
			}
		]
	}`

	var meta RepoMetadata
	if err := meta.FromJSON([]byte(v1JSON)); err != nil {
		t.Fatalf("FromJSON (migrateV1): %v", err)
	}

	meta.Normalize("demo", 100)
	if err := meta.Validate(); err != nil {
		t.Fatalf("Validate after migration+normalize: %v", err)
	}

	bigFile := meta.FindFile("bigfile.bin")
	if bigFile == nil {
		t.Fatal("bigfile.bin not found after migration")
	}
	smallFile := meta.FindFile("small.txt")
	if smallFile == nil {
		t.Fatal("small.txt not found after migration")
	}

	if len(bigFile.Chunks) != 2 || bigFile.Chunks[0] != 1 || bigFile.Chunks[1] != 2 {
		t.Fatalf("unexpected bigfile.bin chunks: %v", bigFile.Chunks)
	}
	if len(smallFile.Chunks) != 1 || smallFile.Chunks[0] != 3 {
		t.Fatalf("unexpected small.txt chunks: %v", smallFile.Chunks)
	}

	bigShared, ok := meta.Chunks[1]
	if !ok {
		t.Fatal("expected chunk 1 in meta.Chunks")
	}
	if bigShared.AssetID != 100 || bigShared.Offset != 0 || bigShared.Size != 50 {
		t.Fatalf("bigfile shared chunk has wrong data: %+v", bigShared)
	}

	bigUnique, ok := meta.Chunks[2]
	if !ok {
		t.Fatal("expected chunk 2 in meta.Chunks")
	}
	if bigUnique.AssetID != 101 || bigUnique.Offset != 50 || bigUnique.Size != 50 {
		t.Fatalf("bigfile unique chunk has wrong data: %+v", bigUnique)
	}

	smallChunk, ok := meta.Chunks[3]
	if !ok {
		t.Fatal("expected chunk 3 in meta.Chunks")
	}
	if smallChunk.AssetID != 200 || smallChunk.Offset != 0 || smallChunk.Size != 30 {
		t.Fatalf("small.txt chunk has wrong data: %+v", smallChunk)
	}

	if meta.Chunks[1].AssetID == meta.Chunks[3].AssetID {
		t.Fatal("expected distinct asset IDs for same-named chunks")
	}

	encoded, err := meta.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	var decoded RepoMetadata
	if err := decoded.FromJSON(encoded); err != nil {
		t.Fatalf("FromJSON round-trip: %v", err)
	}
	decoded.Normalize("demo", 100)
	if err := decoded.Validate(); err != nil {
		t.Fatalf("Validate after round-trip: %v", err)
	}
}

func TestUpsertRegularFileOverSymlinkReplacesNode(t *testing.T) {
	m := NewRepoMetadata("demo")
	link := FileMeta{Symlink: "target", Size: 6, Inode: 7, Mode: 0o120777, UID: 1, GID: 1}
	m.UpsertFile("link", link, 100)
	m.EnsureRelease("v1", 100)
	m.Chunks[1] = ChunkInfo{Size: 4, Offset: 0, Release: "v1"}

	file := FileMeta{Size: 4, Chunks: []int64{1}, Mode: 0o100644}
	m.UpsertFile("link", file, 200)

	got := m.FindFile("link")
	if got == nil {
		t.Fatal("file missing after replace")
	}
	if got.Symlink != "" {
		t.Fatalf("stale symlink target survived replace: %+v", got)
	}
	if got.Size != 4 || len(got.Chunks) != 1 {
		t.Fatalf("new content destroyed by symlink identity leak: %+v", got)
	}
	if got.Inode == 7 || got.Inode == 0 {
		t.Fatalf("expected fresh inode for replaced node, got %d", got.Inode)
	}
	m.Normalize("demo", 300)
	if err := m.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestUpsertSymlinkRefreshKeepsIdentity(t *testing.T) {
	m := NewRepoMetadata("demo")
	link := FileMeta{Symlink: "old-target", Inode: 9, Mode: 0o120777, UID: 5, GID: 5, UploadedAt: 100, ModifiedAt: 100, AccessedAt: 100, ChangedAt: 100}
	m.UpsertFile("lnk", link, 100)

	refresh := FileMeta{Symlink: "new-target"}
	m.UpsertFile("lnk", refresh, 200)

	got := m.FindFile("lnk")
	if got == nil || got.Symlink != "new-target" {
		t.Fatalf("symlink refresh failed: %+v", got)
	}
	if got.Inode != 9 || got.UID != 5 {
		t.Fatalf("identity not preserved on symlink refresh: %+v", got)
	}
}

func TestFromJSONRejectsCorruptAndFutureVersions(t *testing.T) {
	var corrupt RepoMetadata
	if err := corrupt.FromJSON([]byte(`{"p":"demo"}`)); err == nil || !strings.Contains(err.Error(), "no version") {
		t.Fatalf("expected loud rejection of versionless payload, got %v", err)
	}
	var future RepoMetadata
	if err := future.FromJSON([]byte(`{"v":99,"p":"demo"}`)); err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("expected rejection of future version, got %v", err)
	}
}

func TestV2PayloadMigratesStringXAttrsToBytes(t *testing.T) {
	v2JSON := `{"v":2,"p":"demo","f":{"a.txt":{"s":0,"i":5,"ua":100,"x":{"user.bin":"aGVsbG8=","user.txt":"plain"}}}}`
	var m RepoMetadata
	if err := m.FromJSON([]byte(v2JSON)); err != nil {
		t.Fatalf("migrate v2: %v", err)
	}
	if m.Version != maxMetadataVersion {
		t.Fatalf("expected version %d after migration, got %d", maxMetadataVersion, m.Version)
	}
	file := m.FindFile("a.txt")
	if file == nil {
		t.Fatal("file missing after migration")
	}
	if string(file.XAttrs["user.bin"]) != "hello" || string(file.XAttrs["user.txt"]) != "plain" {
		t.Fatalf("xattr migration lost data: %+v", file.XAttrs)
	}
}

func TestCountersPersistAcrossReload(t *testing.T) {
	m := NewRepoMetadata("demo")
	m.EnsureRelease("v1", 1)
	m.Chunks[1] = ChunkInfo{Size: 1, Release: "v1"}
	m.UpsertFile("a.txt", FileMeta{Size: 1, Chunks: []int64{1}}, 1)
	m.Normalize("demo", 1)
	nextChunk := m.NextChunkID
	nextInode := m.NextInode

	encoded, err := m.ToJSON()
	if err != nil {
		t.Fatalf("tojson: %v", err)
	}
	// Drop the highest entries, reload, and verify identifiers are not reused.
	delete(m.Files, "a.txt")
	delete(m.Chunks, 1)
	var reloaded RepoMetadata
	if err := reloaded.FromJSON(encoded); err != nil {
		t.Fatalf("fromjson: %v", err)
	}
	reloaded.Normalize("demo", 2)
	if reloaded.NextChunkID < nextChunk {
		t.Fatalf("chunk counter rolled back: %d < %d", reloaded.NextChunkID, nextChunk)
	}
	if reloaded.AllocateChunkID() != nextChunk {
		t.Fatalf("chunk id reused after reload")
	}
	if reloaded.AllocateInode() != nextInode {
		t.Fatalf("inode reused after reload")
	}
}

func TestNormalizeSortsFileChunksByOffsetAndValidateEnforcesOrder(t *testing.T) {
	m := NewRepoMetadata("demo")
	m.EnsureRelease("v1", 1)
	m.Chunks[2] = ChunkInfo{Size: 2, Offset: 4, Release: "v1"}
	m.Chunks[1] = ChunkInfo{Size: 4, Offset: 0, Release: "v1"}
	m.UpsertFile("f.bin", FileMeta{Size: 6, Chunks: []int64{2, 1}, Inode: 9}, 1)
	m.Normalize("demo", 1)
	if err := m.Validate(); err != nil {
		t.Fatalf("validate after normalize: %v", err)
	}
	got := m.FindFile("f.bin")
	if got.Chunks[0] != 1 || got.Chunks[1] != 2 {
		t.Fatalf("chunks not sorted by offset: %v", got.Chunks)
	}

	// Out-of-order stored chunks must fail validation loudly.
	bad := NewRepoMetadata("demo")
	bad.EnsureRelease("v1", 1)
	bad.Chunks[1] = ChunkInfo{Size: 4, Offset: 4, Release: "v1"}
	bad.Chunks[2] = ChunkInfo{Size: 4, Offset: 0, Release: "v1"}
	bad.UpsertFile("g.bin", FileMeta{Size: 8, Chunks: []int64{1, 2}, Inode: 9}, 1)
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "offset order") {
		t.Fatalf("expected offset-order violation, got %v", err)
	}
}

func TestDirNLinkCountsEachSubdirectoryOnce(t *testing.T) {
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("docs/a", 1)
	m.EnsureDirectory("docs/b", 1)
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
	m := NewRepoMetadata("demo")
	m.EnsureDirectory("rooted", 1)
	dir := m.Dirs["rooted"]
	dir.UID = 0
	dir.GID = 0
	m.Dirs["rooted"] = dir
	m.Normalize("demo", 1)
	if m.Dirs["rooted"].UID != 0 || m.Dirs["rooted"].GID != 0 {
		t.Fatalf("zero owner was clobbered by normalize: %+v", m.Dirs["rooted"])
	}
}

func TestPruneUnreferencedChunks(t *testing.T) {
	m := NewRepoMetadata("demo")
	m.UpsertFile("a.txt", FileMeta{Size: 4, Inode: 1, Chunks: []int64{10, 11}}, 100)
	m.Chunks[10] = ChunkInfo{Offset: 0, Size: 4}
	m.Chunks[11] = ChunkInfo{Offset: 4, Size: 4}
	// Stale entries from an overwrite and a deleted file.
	m.Chunks[12] = ChunkInfo{Offset: 0, Size: 8}

	if removed := m.PruneUnreferencedChunks(); removed != 1 {
		t.Fatalf("expected 1 pruned chunk, got %d", removed)
	}
	if _, ok := m.Chunks[12]; ok {
		t.Fatal("unreferenced chunk survived pruning")
	}
	for _, id := range []int64{10, 11} {
		if _, ok := m.Chunks[id]; !ok {
			t.Fatalf("referenced chunk %d was pruned", id)
		}
	}

	// Pruning is idempotent and a no-op when everything is referenced.
	if again := m.PruneUnreferencedChunks(); again != 0 {
		t.Fatalf("expected idempotent prune, got %d", again)
	}

	// Deleting the last file frees every chunk.
	m.Files = map[string]FileMeta{}
	if removed := m.PruneUnreferencedChunks(); removed != 2 {
		t.Fatalf("expected all chunks pruned after file deletion, got %d", removed)
	}
	if len(m.Chunks) != 0 {
		t.Fatalf("expected empty chunk catalog, got %d entries", len(m.Chunks))
	}
}
