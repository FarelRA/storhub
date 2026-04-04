package metadata

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
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
	when := chooseNonZeroTime(time.Time{}, time.Unix(5, 0))
	if when.IsZero() || when.Location() != time.UTC {
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
	now := time.Unix(100, 0).UTC()
	repo := &RepoMetadata{
		Project:     "demo",
		Directories: []DirectoryMetadata{{Path: "docs", Inode: 2}, {Path: " docs/sub ", Inode: 3, XAttrs: map[string]string{"user.dir": "1"}}},
		Releases:    []ReleaseMetadata{{Tag: "v2", Files: []FileMetadata{{Name: "docs/sub/link", Kind: NodeKindSymlink, Release: "", Inode: 4, SymlinkTarget: "target"}, {Name: "docs/sub/file.txt", Release: "v2", Size: 3, Inode: 5, Chunks: []ChunkInfo{{Index: 1, Offset: 2, Size: 1, Release: "", AssetID: 2}, {Index: 0, Offset: 0, Size: 2, Release: "", AssetID: 1}}}}}},
	}
	repo.Normalize("demo", now)
	if err := repo.Validate(); err != nil {
		t.Fatalf("validate normalized repo: %v", err)
	}
	if repo.Version != 1 || repo.Project != "demo" || repo.Root.Inode == 0 {
		t.Fatalf("unexpected normalized repo: %+v", repo)
	}
	if !repo.HasDirectory("docs") || !repo.HasDirectory("docs/sub") {
		t.Fatalf("expected directories to exist: %+v", repo.Directories)
	}
	file := repo.FindFile("docs/sub/file.txt")
	if file == nil || file.Release != "v2" || len(file.Chunks) != 2 || file.Chunks[0].Offset != 0 {
		t.Fatalf("unexpected file metadata: %+v", file)
	}
	if link := repo.FindFile("docs/sub/link"); link == nil || link.Kind != NodeKindSymlink || link.Size != int64(len("target")) || len(link.Chunks) != 0 {
		t.Fatalf("unexpected symlink metadata: %+v", link)
	}
	dirs, files := repo.DirectoryChildren("docs/sub")
	if len(dirs) != 0 || len(files) != 2 {
		t.Fatalf("unexpected children: dirs=%+v files=%+v", dirs, files)
	}
	byInode := repo.FindFilesByInode(file.Inode)
	if len(byInode) != 1 || byInode[0].Name != file.Name {
		t.Fatalf("unexpected inode lookup: %+v", byInode)
	}
	clone := repo.Clone()
	clone.Root.XAttrs = map[string]string{"mutated": "1"}
	if reflect.DeepEqual(clone.Root.XAttrs, repo.Root.XAttrs) && clone.Root.XAttrs != nil {
		t.Fatal("expected deep clone of root attrs")
	}
	allFiles := repo.AllFiles()
	allFiles[0].Name = "mutated"
	if repo.AllFiles()[0].Name == "mutated" {
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
	now := time.Unix(200, 0).UTC()
	repo := NewRepoMetadata("mutations")
	repo.EnsureDirectory("docs/specs", now)
	if !repo.HasDirectory("docs") || !repo.HasDirectory("docs/specs") {
		t.Fatalf("expected ensured directories: %+v", repo.Directories)
	}
	release := repo.EnsureRelease("v1", now)
	if release == nil || release.Tag != "v1" {
		t.Fatalf("unexpected release: %+v", release)
	}
	file := FileMetadata{Name: "docs/specs/readme.txt", Release: "v1", Size: 3, Chunks: []ChunkInfo{{Index: 0, Offset: 0, Size: 3, Release: "v1", AssetID: 1}}}
	repo.UpsertFile(file, now)
	first := repo.FindFile(file.Name)
	if first == nil || first.Inode == 0 || first.NLink != 1 {
		t.Fatalf("unexpected inserted file: %+v", first)
	}
	repo.UpsertFile(FileMetadata{Name: file.Name, Release: "v1", Size: 4, Chunks: []ChunkInfo{{Index: 0, Offset: 0, Size: 4, Release: "v1", AssetID: 2}}}, now.Add(time.Minute))
	updated := repo.FindFile(file.Name)
	if updated == nil || updated.Inode != first.Inode || updated.Size != 4 {
		t.Fatalf("expected identity preserved on upsert: first=%+v updated=%+v", first, updated)
	}
	if !repo.RemoveFile(file.Name) || repo.FindFile(file.Name) != nil {
		t.Fatal("expected file removal")
	}
	if !repo.RemoveDirectory("docs/specs") || repo.HasDirectory("docs/specs") {
		t.Fatal("expected directory removal")
	}
	if repo.RemoveRelease("v1") {
		t.Fatal("expected empty release to have already been pruned")
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
	now := time.Unix(300, 0).UTC()
	repo := NewRepoMetadata("validate")
	InitializeNewFileIdentity(repo, &FileMetadata{}, now)
	existing := &FileMetadata{Name: "file", Kind: NodeKindSymlink, Inode: 42, Mode: 0o777, UID: 7, GID: 9, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now, SymlinkTarget: "target", XAttrs: map[string]string{"user.demo": "1"}}
	incoming := &FileMetadata{}
	PreserveFileIdentity(incoming, existing, now.Add(time.Minute))
	if incoming.Inode != existing.Inode || incoming.SymlinkTarget != "target" || incoming.XAttrs["user.demo"] != "1" {
		t.Fatalf("unexpected preserved identity: %+v", incoming)
	}
	badRepo := RepoMetadata{Root: RootMetadata{Inode: 1}}
	if err := badRepo.Validate(); err == nil || !strings.Contains(err.Error(), "project") {
		t.Fatalf("expected project validation error, got %v", err)
	}
	dupDirs := NewRepoMetadata("dupdirs")
	dupDirs.Directories = []DirectoryMetadata{{Path: "docs", Inode: 2}, {Path: "docs", Inode: 3}}
	dupDirs.RecomputeStats()
	if err := dupDirs.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate directory") {
		t.Fatalf("expected duplicate directory error, got %v", err)
	}
	dupRelease := NewRepoMetadata("duprel")
	dupRelease.Directories = []DirectoryMetadata{{Path: "docs", Inode: 2}}
	file := FileMetadata{Name: "docs/a.txt", Release: "v1", Size: 1, Inode: 3, Chunks: []ChunkInfo{{Index: 0, Offset: 0, Size: 1, Release: "v1", AssetID: 1}}}
	dupRelease.Releases = []ReleaseMetadata{{Tag: "v1", Files: []FileMetadata{file}}, {Tag: "v1", Files: []FileMetadata{file}}}
	dupRelease.RecomputeStats()
	dupRelease.RebuildIndexes()
	if err := dupRelease.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate release") {
		t.Fatalf("expected duplicate release error, got %v", err)
	}
	badFile := FileMetadata{Name: "docs/a.txt", Release: "v1", Size: 2, Inode: 3, Chunks: []ChunkInfo{{Index: 0, Offset: 1, Size: 2, Release: "v1", AssetID: 1}}}
	if err := badFile.Validate("v1"); err == nil || !strings.Contains(err.Error(), "offset mismatch") {
		t.Fatalf("expected offset mismatch, got %v", err)
	}
	badLink := FileMetadata{Name: "link", Kind: NodeKindSymlink, Release: "v1", Inode: 4}
	if err := badLink.Validate("v1"); err == nil || !strings.Contains(err.Error(), "symlink target") {
		t.Fatalf("expected symlink validation error, got %v", err)
	}
	if err := (DirectoryMetadata{}).Validate(); err == nil {
		t.Fatal("expected directory validation error")
	}
	if err := (&ReleaseMetadata{}).Validate(); err == nil {
		t.Fatal("expected release validation error")
	}
	if chooseNonEmpty("", "  ", "x") != "x" || containsRelease([]ReleaseMetadata{{Tag: "v1"}}, "v2") {
		t.Fatal("unexpected helper result")
	}
	files := []FileMetadata{{Name: "b"}, {Name: "a"}}
	stableSortFiles(files)
	if files[0].Name != "a" {
		t.Fatalf("unexpected file sort: %+v", files)
	}
	dirs := []DirectoryMetadata{{Path: "b"}, {Path: "a"}}
	stableSortDirectories(dirs)
	if dirs[0].Path != "a" {
		t.Fatalf("unexpected dir sort: %+v", dirs)
	}
	releases := []ReleaseMetadata{{Tag: "v10"}, {Tag: "v2"}, {Tag: "alpha"}}
	stableSortReleases(releases)
	if releases[0].Tag != "alpha" || releases[1].Tag != "v2" || releases[2].Tag != "v10" {
		t.Fatalf("unexpected release sort: %+v", releases)
	}
	chunks := []ChunkInfo{{Index: 2, Offset: 5}, {Index: 0, Offset: 0}, {Index: 1, Offset: 5}}
	stableSortChunks(chunks)
	if !reflect.DeepEqual([]int{chunks[0].Index, chunks[1].Index, chunks[2].Index}, []int{0, 1, 2}) {
		t.Fatalf("unexpected chunk sort: %+v", chunks)
	}
}
