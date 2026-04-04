package posix

import (
	"testing"
	"time"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestApplyUploadAndUpdateIdentity(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	repo := meta.NewRepoMetadata("demo")
	file := &meta.FileMetadata{Name: "docs/file.txt", Release: "v1"}
	ApplyUploadIdentity(repo, nil, file, now)
	if file.Inode == 0 || file.Kind != meta.NodeKindFile || file.Mode == 0 {
		t.Fatalf("unexpected initialized file identity: %+v", file)
	}
	existing := &meta.FileMetadata{Inode: 9, Kind: meta.NodeKindSymlink, Mode: 0o777, UID: 7, GID: 8, AccessedAt: now, UploadedAt: now, ModifiedAt: now, ChangedAt: now, SymlinkTarget: "target", XAttrs: map[string]string{"user.demo": "1"}}
	updated := &meta.FileMetadata{Release: "v1"}
	ApplyUpdatedFileIdentity(updated, existing, now.Add(time.Minute))
	if updated.Inode != 9 || updated.Kind != meta.NodeKindFile || updated.SymlinkTarget != "" {
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
	base := meta.FileMetadata{Name: "docs/a.txt", Release: "v1", Size: 1, Chunks: []meta.ChunkInfo{{Index: 0, Offset: 0, Size: 1, Release: "v1", AssetID: 1}}}
	repo.UpsertFile(base, now)
	first := repo.FindFile("docs/a.txt")
	clone := first.Clone()
	clone.Name = "docs/b.txt"
	repo.UpsertFile(clone, now)
	updated := first.Clone()
	updated.Release = "v2"
	updated.Chunks = []meta.ChunkInfo{{Index: 0, Offset: 0, Size: 1, Release: "v2", AssetID: 2}}
	ReplaceInodeFamily(repo, first, updated, now.Add(time.Minute))
	if got := repo.FindFilesByInode(first.Inode); len(got) != 2 || got[0].Release != "v2" || got[1].Release != "v2" {
		t.Fatalf("expected inode family replacement, got %+v", got)
	}
	missing := &meta.FileMetadata{Name: "docs/missing.txt", Inode: 99}
	updated.Name = "docs/missing.txt"
	ReplaceInodeFamily(repo, missing, updated, now)
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
