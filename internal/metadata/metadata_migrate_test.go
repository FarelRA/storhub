package metadata

import (
	"encoding/json"
	"strings"
	"testing"
)

// A v6 document without a non-empty tree root is a truncated/corrupt
// manifest, never a blob. The blob side must reject it loudly instead of
// decoding an empty v6 tree that a later commit would publish over the real
// index (silent total data loss).
func TestV6DocumentWithoutTreeRootIsRejected(t *testing.T) {
	t.Parallel()
	probe := []byte(`{"v":6,"p":"proj","d":{},"f":{},"c":{},"r":{}}`)
	if IsManifest(probe) {
		t.Fatal("probe: shape detector must not call a tr-less document a manifest")
	}
	var direct RepoMetadata
	if err := json.Unmarshal(probe, &direct); err == nil {
		t.Fatal("UnmarshalJSON accepted a v6 blob: v6 documents are manifests, blobs are v<=5")
	} else if !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("rejection must name the manifest contract: %v", err)
	}
	if _, _, err := Migrate(probe); err == nil {
		t.Fatal("Migrate passed a tr-less v6 document through as a blob")
	}
	var viaFromJSON RepoMetadata
	if err := viaFromJSON.FromJSON(probe); err == nil {
		t.Fatal("FromJSON accepted a tr-less v6 document")
	}
	// A current blob (v5, no tree root) still decodes as a blob.
	blobProbe := []byte(`{"v":5,"p":"proj","tf":0,"ts":0,"lm":1700000000000000000,"rt":{"cr":1700000000000000000,"ma":1700000000000000000,"i":1},"ni":2,"nc":1}`)
	var asCurrent RepoMetadata
	if err := json.Unmarshal(blobProbe, &asCurrent); err != nil {
		t.Fatalf("current v5 blob must decode as a blob: %v", err)
	}
	// A real manifest (tr present) keeps its dedicated rejection.
	m := NewRepoMetadata("demo")
	m.Normalize("demo", 1000000000)
	res, err := BuildTree(m)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	mb, err := MarshalManifest(&Manifest{Version: CurrentVersion, Project: "demo", TreeRoot: res.RootSHA, Releases: res.ReleasesSHA})
	if err != nil {
		t.Fatal(err)
	}
	var asBlob RepoMetadata
	if err := json.Unmarshal(mb, &asBlob); err == nil {
		t.Fatal("manifest must not decode as a blob")
	}
}

// The blob side of the version axis: ToJSON serializes the single-blob
// layout, which tops out at maxBlobVersion; a version-6 tree migrates to the
// split layout on write, so a v6 blob document can never legitimately exist.
func TestToJSONEmitsBlobVersion(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.UpsertFile("f.txt", FileMeta{Size: 1}, 123000000000)
	blob, err := m.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(blob), `"v":5`) {
		t.Fatalf("blob document must carry the blob version: %s", blob)
	}
	var back RepoMetadata
	if err := back.FromJSON(blob); err != nil {
		t.Fatalf("round-trip of a new tree must stay loadable: %v", err)
	}
	if back.Version != maxBlobVersion {
		t.Fatalf("loaded blob version = %d, want %d", back.Version, maxBlobVersion)
	}
	if back.FindFile("f.txt") == nil {
		t.Fatal("round-trip lost the file")
	}
}

func TestMigrateV1ChunkNameCollision(t *testing.T) {
	t.Parallel()
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

	meta.Normalize("demo", 100000000000)
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

	bigShared, ok := meta.chunks[1]
	if !ok {
		t.Fatal("expected chunk 1 in meta.Chunks")
	}
	if bigShared.AssetID != 100 || bigShared.Offset != 0 || bigShared.Size != 50 {
		t.Fatalf("bigfile shared chunk has wrong data: %+v", bigShared)
	}

	bigUnique, ok := meta.chunks[2]
	if !ok {
		t.Fatal("expected chunk 2 in meta.Chunks")
	}
	if bigUnique.AssetID != 101 || bigUnique.Offset != 50 || bigUnique.Size != 50 {
		t.Fatalf("bigfile unique chunk has wrong data: %+v", bigUnique)
	}

	smallChunk, ok := meta.chunks[3]
	if !ok {
		t.Fatal("expected chunk 3 in meta.Chunks")
	}
	if smallChunk.AssetID != 200 || smallChunk.Offset != 0 || smallChunk.Size != 30 {
		t.Fatalf("small.txt chunk has wrong data: %+v", smallChunk)
	}

	if meta.chunks[1].AssetID == meta.chunks[3].AssetID {
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
	decoded.Normalize("demo", 100000000000)
	if err := decoded.Validate(); err != nil {
		t.Fatalf("Validate after round-trip: %v", err)
	}
}

func TestUpsertRegularFileOverSymlinkReplacesNode(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	link := FileMeta{Symlink: "target", Size: 6, Inode: 7, Mode: 0o120777, UID: 1, GID: 1}
	m.UpsertFile("link", link, 100000000000)
	if _, err := m.EnsureRelease("v1", 100000000000); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	m.chunks[1] = ChunkInfo{Size: 4, Offset: 0, Release: "v1"}

	file := FileMeta{Size: 4, Chunks: []int64{1}, Mode: 0o100644}
	m.UpsertFile("link", file, 200000000000)

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
	m.Normalize("demo", 300000000000)
	if err := m.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestUpsertSymlinkRefreshKeepsIdentity(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	link := FileMeta{Symlink: "old-target", Inode: 9, Mode: 0o120777, UID: 5, GID: 5, UploadedAt: 100000000000, ModifiedAt: 100000000000, AccessedAt: 100000000000, ChangedAt: 100000000000}
	m.UpsertFile("lnk", link, 100000000000)

	refresh := FileMeta{Symlink: "new-target"}
	m.UpsertFile("lnk", refresh, 200000000000)

	got := m.FindFile("lnk")
	if got == nil || got.Symlink != "new-target" {
		t.Fatalf("symlink refresh failed: %+v", got)
	}
	if got.Inode != 9 || got.UID != 5 {
		t.Fatalf("identity not preserved on symlink refresh: %+v", got)
	}
}

func TestFromJSONRejectsCorruptAndFutureVersions(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	v2JSON := `{"v":2,"p":"demo","f":{"a.txt":{"s":0,"i":5,"ua":100,"x":{"user.bin":"aGVsbG8=","user.txt":"cGxhaW4="}}}}`
	var m RepoMetadata
	if err := m.FromJSON([]byte(v2JSON)); err != nil {
		t.Fatalf("migrate v2: %v", err)
	}
	if m.Version != maxBlobVersion {
		t.Fatalf("expected version %d after migration, got %d", maxBlobVersion, m.Version)
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
	t.Parallel()
	m := NewRepoMetadata("demo")
	if _, err := m.EnsureRelease("v1", 1000000000); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	m.chunks[1] = ChunkInfo{Size: 1, Release: "v1"}
	m.UpsertFile("a.txt", FileMeta{Size: 1, Chunks: []int64{1}}, 1000000000)
	m.Normalize("demo", 1000000000)
	nextChunk := m.NextChunkID
	nextInode := m.NextInode

	encoded, err := m.ToJSON()
	if err != nil {
		t.Fatalf("tojson: %v", err)
	}
	// Drop the highest entries, reload, and verify identifiers are not reused.
	delete(m.files, "a.txt")
	delete(m.chunks, 1)
	var reloaded RepoMetadata
	if err := reloaded.FromJSON(encoded); err != nil {
		t.Fatalf("fromjson: %v", err)
	}
	reloaded.Normalize("demo", 2000000000)
	if reloaded.NextChunkID < nextChunk {
		t.Fatalf("chunk counter rolled back: %d < %d", reloaded.NextChunkID, nextChunk)
	}
	if reloaded.allocateChunkID() != nextChunk {
		t.Fatalf("chunk id reused after reload")
	}
	if reloaded.allocateInode() != nextInode {
		t.Fatalf("inode reused after reload")
	}
}

func TestPruneUnreferencedChunks(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.UpsertFile("a.txt", FileMeta{Size: 4, Inode: 1, Chunks: []int64{10, 11}}, 100000000000)
	m.chunks[10] = ChunkInfo{Offset: 0, Size: 4}
	m.chunks[11] = ChunkInfo{Offset: 4, Size: 4}
	// Stale entries from an overwrite and a deleted file.
	m.chunks[12] = ChunkInfo{Offset: 0, Size: 8}

	if removed := m.PruneUnreferencedChunks(); removed != 1 {
		t.Fatalf("expected 1 pruned chunk, got %d", removed)
	}
	if _, ok := m.chunks[12]; ok {
		t.Fatal("unreferenced chunk survived pruning")
	}
	for _, id := range []int64{10, 11} {
		if _, ok := m.chunks[id]; !ok {
			t.Fatalf("referenced chunk %d was pruned", id)
		}
	}

	// Pruning is idempotent and a no-op when everything is referenced.
	if again := m.PruneUnreferencedChunks(); again != 0 {
		t.Fatalf("expected idempotent prune, got %d", again)
	}

	// Deleting the last file frees every chunk.
	m.files = map[string]FileMeta{}
	if removed := m.PruneUnreferencedChunks(); removed != 2 {
		t.Fatalf("expected all chunks pruned after file deletion, got %d", removed)
	}
	if len(m.chunks) != 0 {
		t.Fatalf("expected empty chunk catalog, got %d entries", len(m.chunks))
	}
}

func TestMigrateV1RejectsDuplicatePathsAndSynthesizesParents(t *testing.T) {
	t.Parallel()
	// Duplicate file entries are corruption: refuse to guess.
	dup := []byte(`{"version":1,"project":"demo","last_modified":"2024-01-01T00:00:00Z",
		"root":{"inode":1,"mode":493,"created_at":"2024-01-01T00:00:00Z","modified_at":"2024-01-01T00:00:00Z"},
		"directories":[],"releases":[{"tag":"v1","asset_count":1,"created_at":"2024-01-01T00:00:00Z",
		"files":[
			{"name":"a.txt","size":1,"release":"v1","uploaded_at":"2024-01-01T00:00:00Z"},
			{"name":"a.txt","size":2,"release":"v1","uploaded_at":"2024-01-01T00:00:00Z"}
		]}]}`)
	m := NewRepoMetadata("demo")
	if err := m.migrateV1(dup); err == nil {
		t.Fatal("expected duplicate-path migration to fail")
	}

	// A file under an undeclared directory chain gets its parents created.
	orphan := []byte(`{"version":1,"project":"demo","last_modified":"2024-01-01T00:00:00Z",
		"root":{"inode":1,"mode":493,"created_at":"2024-01-01T00:00:00Z","modified_at":"2024-01-01T00:00:00Z"},
		"directories":[],"releases":[{"tag":"v1","asset_count":1,"created_at":"2024-01-01T00:00:00Z",
		"files":[{"name":"deeply/nested/file.txt","size":4,"release":"v1","uploaded_at":"2024-01-01T00:00:00Z",
		"chunks":[{"name":"file.txt.part001","size":4,"index":0,"offset":0,"release":"v1","asset_offset":0,"asset_id":7}]}]}]}`)
	m = NewRepoMetadata("demo")
	if err := m.migrateV1(orphan); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, dir := range []string{"deeply", "deeply/nested"} {
		if _, ok := m.dirs[dir]; !ok {
			t.Fatalf("parent %q was not synthesized", dir)
		}
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("migrated metadata must validate: %v", err)
	}
}

func TestParseNumericReleaseTagRejectsSigns(t *testing.T) {
	t.Parallel()
	if _, ok := parseNumericReleaseTag("v-1"); ok {
		t.Fatal("v-1 must not parse as numeric tag")
	}
	if _, ok := parseNumericReleaseTag("v+2"); ok {
		t.Fatal("v+2 must not parse as numeric tag")
	}
	if n, ok := parseNumericReleaseTag("V42"); !ok || n != 42 {
		t.Fatalf("V42 should parse as 42, got %d %v", n, ok)
	}
	if _, ok := parseNumericReleaseTag("release-9"); ok {
		t.Fatal("non-numeric tag must not parse")
	}
}

func TestV3PayloadMigratesTimestamps(t *testing.T) {
	t.Parallel()
	v3 := `{
	  "v":3,"p":"demo","tf":1,"ts":1,"lm":10,
	  "rt":{"ca":100,"ma":101,"aa":102,"cha":103,"i":1},
	  "d":{"olddir":{"ca":200,"ma":201,"cha":202}},
	  "f":{"old.txt":{"s":1,"cs":[1],"ua":300,"ma":301,"aa":302,"ca":303,"i":5}},
	  "r":{"v1":{"ac":1,"ca":400}},
	  "ni":6,"nc":2
	}`
	var m RepoMetadata
	if err := m.FromJSON([]byte(v3)); err != nil {
		t.Fatalf("migrate v3: %v", err)
	}
	if m.Version != maxBlobVersion {
		t.Fatalf("version not advanced: %d", m.Version)
	}
	if m.Root.CreatedAt != 100000000000 || m.Root.ChangedAt != 103000000000 {
		t.Fatalf("root legacy keys unmapped: %+v", m.Root)
	}
	d := m.dirs["olddir"]
	if d.CreatedAt != 200000000000 || d.ChangedAt != 202000000000 {
		t.Fatalf("dir legacy keys unmapped: %+v", d)
	}
	f, ok := m.files["old.txt"]
	if !ok || f.ChangedAt != 303000000000 || f.ModifiedAt != 301000000000 {
		t.Fatalf("file legacy ca (ChangedAt) unmapped: %+v", f)
	}
	r := m.releases["v1"]
	if r.CreatedAt != 400000000000 {
		t.Fatalf("release legacy key unmapped: %+v", r)
	}
	// Re-serialization must emit v5 spellings only.
	blob, err := m.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	out := string(blob)
	if strings.Contains(out, `"ca"`) || strings.Contains(out, `"cha"`) {
		t.Fatal("migrated metadata still carries legacy keys")
	}
}
