package posix

import (
	"testing"
	"time"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestApplyUploadAndUpdateIdentity(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	repo := meta.NewRepoMetadata("demo")
	file := &meta.FileMeta{Chunks: []string{}}
	ApplyUploadIdentity(repo, "docs/file.txt", nil, file, now)
	if file.Inode == 0 || file.Mode == 0 {
		t.Fatalf("unexpected initialized file identity: %+v", file)
	}
	existing := &meta.FileMeta{Inode: 9, Mode: 0o777, UID: 7, GID: 8, AccessedAt: now, UploadedAt: now, ModifiedAt: now, ChangedAt: now, Symlink: "target", XAttrs: map[string]string{"user.demo": "1"}}
	updated := &meta.FileMeta{Chunks: []string{}}
	ApplyUpdatedFileIdentity("", updated, existing, now.Add(time.Minute))
	if updated.Inode != 9 || updated.Symlink != "" {
		t.Fatalf("unexpected updated identity: %+v", updated)
	}
	if updated.ModifiedAt.IsZero() || updated.ChangedAt.IsZero() || updated.AccessedAt.IsZero() {
		t.Fatalf("expected timestamps to be set: %+v", updated)
	}
}

func TestReplaceInodeFamilyAndHelpers(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	repo := meta.NewRepoMetadata("demo")
	repo.EnsureDirectory("docs", now)

	chunk1 := meta.ChunkInfo{Offset: 0, Size: 1, Release: "v1", AssetID: 1}
	repo.Chunks["chunk_1"] = chunk1
	base := meta.FileMeta{Size: 1, Chunks: []string{"chunk_1"}}
	repo.UpsertFile("docs/a.txt", base, now)
	first := repo.FindFile("docs/a.txt")
	clone := first.Clone()
	repo.UpsertFile("docs/b.txt", clone, now)

	chunk2 := meta.ChunkInfo{Offset: 0, Size: 1, Release: "v2", AssetID: 2}
	repo.Chunks["chunk_2"] = chunk2
	updated := first.Clone()
	updated.Chunks = []string{"chunk_2"}
	ReplaceInodeFamily(repo, "docs/a.txt", first, updated, now.Add(time.Minute))
	got := repo.FindFilesByInode(first.Inode)
	if len(got) != 2 {
		t.Fatalf("expected 2 files, got %d: %+v", len(got), got)
	}
	for _, name := range got {
		f := repo.FindFile(name)
		if f == nil || len(f.Chunks) != 1 || f.Chunks[0] != "chunk_2" {
			t.Fatalf("expected chunk_2 on %s, got %+v", name, f)
		}
	}

	missing := &meta.FileMeta{Inode: 99}
	updated2 := first.Clone()
	updated2.Chunks = []string{"chunk_2"}
	ReplaceInodeFamily(repo, "docs/missing.txt", missing, updated2, now)
	if repo.FindFile("docs/missing.txt") == nil {
		t.Fatal("expected missing inode family to fall back to upsert")
	}
	if ChooseNonZeroTime(time.Time{}, now).IsZero() {
		t.Fatal("expected non-zero time selection")
	}
	if CloneStringMap(nil) != nil || CloneStringMap(map[string]string{"a": "b"})["a"] != "b" {
		t.Fatal("unexpected map clone")
	}
	if uid, gid := DefaultOwnerIDs(); uid == 0 && gid == 0 {
		// Valid on some systems; just exercise the helper.
	}
}
