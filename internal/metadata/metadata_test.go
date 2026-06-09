package metadata

import (
	"encoding/json"
	"reflect"
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
	if cloneStringMap(nil) != nil || normalizeXAttrs(map[string]string{"": "x"}) != nil {
		t.Fatal("expected nil map normalization")
	}
	attrs := normalizeXAttrs(map[string]string{"user.a": "1", "": "skip"})
	if len(attrs) != 1 || attrs["user.a"] != "1" {
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
	repo.Dirs["docs/sub"] = DirMeta{Inode: 3, XAttrs: map[string]string{"user.dir": "1"}, CreatedAt: now, ModifiedAt: now}

	repo.EnsureRelease("v2", now)
	repo.UpsertFile("docs/sub/file.txt", FileMeta{Size: 3, Inode: 5, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []string{"chunk1", "chunk0"}}, now)
	repo.Chunks["chunk0"] = ChunkInfo{Offset: 0, Size: 2, AssetID: 1}
	repo.Chunks["chunk1"] = ChunkInfo{Offset: 2, Size: 1, AssetID: 2}
	repo.UpsertFile("docs/sub/link", FileMeta{Inode: 4, Mode: 0o777, Symlink: "target", UploadedAt: now, ModifiedAt: now}, now)

	repo.Normalize("demo", now)
	if err := repo.Validate(); err != nil {
		t.Fatalf("validate normalized repo: %v", err)
	}
	if repo.Version != 2 || repo.Project != "demo" {
		t.Fatalf("unexpected normalized repo: %+v", repo)
	}
	if repo.Root.Inode == 0 {
		t.Fatal("expected root inode to be set")
	}
	if !repo.HasDirectory("docs") || !repo.HasDirectory("docs/sub") {
		t.Fatalf("expected directories to exist")
	}
	file := repo.FindFile("docs/sub/file.txt")
	if file == nil || len(file.Chunks) != 2 || file.Chunks[0] != "chunk0" {
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
	clone := repo.Clone()
	clonedRoot := &clone.Root
	origRoot := &repo.Root
	clonedRoot.XAttrs = map[string]string{"mutated": "1"}
	if reflect.DeepEqual(clonedRoot.XAttrs, origRoot.XAttrs) && clonedRoot.XAttrs != nil {
		t.Fatal("expected deep clone of root attrs")
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
	fileMeta := FileMeta{Size: 3, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []string{"c1"}}
	repo.Chunks["c1"] = ChunkInfo{Offset: 0, Size: 3, AssetID: 1}
	repo.UpsertFile("docs/specs/readme.txt", fileMeta, now)
	first := repo.FindFile("docs/specs/readme.txt")
	if first == nil || first.Inode == 0 {
		t.Fatalf("unexpected inserted file: %+v", first)
	}
	if n := repo.FileNLink("docs/specs/readme.txt"); n != 1 {
		t.Fatalf("expected nlink=1, got %d", n)
	}
	repo.UpsertFile("docs/specs/readme.txt", FileMeta{Size: 4, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []string{"c2"}}, now+60)
	repo.Chunks["c2"] = ChunkInfo{Offset: 0, Size: 4, AssetID: 2}
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
	existing := &FileMeta{Inode: 42, Mode: 0o777, UID: 7, GID: 9, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now, Symlink: "target", XAttrs: map[string]string{"user.demo": "1"}}
	incoming := &FileMeta{}
	PreserveFileIdentity(incoming, existing, now+60)
	if incoming.Inode != existing.Inode || incoming.Symlink != "target" || incoming.XAttrs["user.demo"] != "1" {
		t.Fatalf("unexpected preserved identity: %+v", incoming)
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

	sharedChunkName := ""
	for _, cn := range bigFile.Chunks {
		if cn != "bigfile.unique" {
			sharedChunkName = cn
			break
		}
	}
	if sharedChunkName == "" {
		t.Fatal("expected shared.chunk variant in bigfile.bin")
	}

	bigChunk, ok := meta.Chunks[sharedChunkName]
	if !ok {
		t.Fatalf("bigfile's shared chunk %q not in meta.Chunks", sharedChunkName)
	}
	if bigChunk.AssetID != 100 || bigChunk.Offset != 0 || bigChunk.Size != 50 {
		t.Fatalf("bigfile shared chunk has wrong data: %+v", bigChunk)
	}

	smallChunkName := smallFile.Chunks[0]
	_ = smallChunkName
	smallChunk, ok := meta.Chunks[smallChunkName]
	if !ok {
		t.Fatalf("small.txt chunk %q not in meta.Chunks", smallChunkName)
	}
	if smallChunk.AssetID != 200 || smallChunk.Offset != 0 || smallChunk.Size != 30 {
		t.Fatalf("small.txt chunk has wrong data: %+v", smallChunk)
	}

	if sharedChunkName == smallChunkName {
		t.Fatalf("expected disambiguated chunk names, got same: %q", sharedChunkName)
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
