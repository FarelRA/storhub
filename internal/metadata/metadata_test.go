package metadata

import (
	"encoding/json"
	"errors"
	"path"
	"strings"
	"testing"
)

// fsNormalizeForTest mirrors internal/fs.NormalizePath so the conformance
// test can compare the two implementations without an import cycle
// (internal/fs imports this package).

func fsNormalizeForTest(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", errPathRequired
	}
	trimmed := strings.TrimLeft(value, "/")
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", errEscapesRoot
	}
	return cleaned, nil
}

var (
	errPathRequired = errors.New("path is required")
	errEscapesRoot  = errors.New("path escapes root")
)

func TestHelpersAndPOSIXUtilities(t *testing.T) {
	t.Parallel()
	// Whitespace is significant: the leading-space component is part of
	// the name (mirrors internal/fs, codified there since round 1).
	if normalizeStoredPath(" /docs/specs/../guide.txt ") != " /docs/guide.txt " {
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
	if got, ok := parseNumericReleaseTag("v12"); !ok || got != 12 {
		t.Fatalf("unexpected release tag parse: %d %v", got, ok)
	}
	if _, ok := parseNumericReleaseTag("feature"); ok {
		t.Fatal("expected non-numeric tag parse failure")
	}
}

func TestRepoMetadataNormalizeCloneAndIndexes(t *testing.T) {
	t.Parallel()
	now := int64(100000000000)
	repo := NewRepoMetadata("demo")

	repo.EnsureDirectory("docs", now)
	repo.EnsureDirectory("docs/sub", now)
	repo.dirs["docs"] = DirMeta{Inode: 2, CreatedAt: now, ModifiedAt: now}
	repo.dirs["docs/sub"] = DirMeta{Inode: 3, XAttrs: XAttrMap{"user.dir": []byte("1")}, CreatedAt: now, ModifiedAt: now}

	if _, err := repo.EnsureRelease("v2", now); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	repo.UpsertFile("docs/sub/file.txt", FileMeta{Size: 3, Inode: 5, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{2, 1}}, now)
	repo.chunks[1] = ChunkInfo{Offset: 0, Size: 2, AssetID: 1}
	repo.chunks[2] = ChunkInfo{Offset: 2, Size: 1, AssetID: 2}
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
	t.Parallel()
	now := int64(200000000000)
	repo := NewRepoMetadata("mutations")
	repo.EnsureDirectory("docs/specs", now)
	if !repo.HasDirectory("docs") || !repo.HasDirectory("docs/specs") {
		t.Fatalf("expected ensured directories")
	}
	release, err := repo.EnsureRelease("v1", now)
	if err != nil {
		t.Fatalf("ensure release: %v", err)
	}
	if release == nil || release.AssetCount != 0 {
		t.Fatalf("unexpected release: %+v", release)
	}
	release, err = repo.EnsureRelease("v1", now)
	if err != nil {
		t.Fatalf("ensure existing release: %v", err)
	}
	if release == nil || release.AssetCount != 0 {
		t.Fatalf("unexpected release: %+v", release)
	}
	fileMeta := FileMeta{Size: 3, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1}}
	repo.chunks[1] = ChunkInfo{Offset: 0, Size: 3, AssetID: 1}
	repo.UpsertFile("docs/specs/readme.txt", fileMeta, now)
	first := repo.FindFile("docs/specs/readme.txt")
	if first == nil || first.Inode == 0 {
		t.Fatalf("unexpected inserted file: %+v", first)
	}
	if n := repo.FileNLink("docs/specs/readme.txt"); n != 1 {
		t.Fatalf("expected nlink=1, got %d", n)
	}
	repo.UpsertFile("docs/specs/readme.txt", FileMeta{Size: 4, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{2}}, now+60)
	repo.chunks[2] = ChunkInfo{Offset: 0, Size: 4, AssetID: 2}
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
	before := repo.allocateInode()
	after := repo.allocateInode()
	if after <= before {
		t.Fatalf("expected increasing inode allocation: %d %d", before, after)
	}
	repo.RebuildIndexes()
}
