package posix

import (
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestApplyUploadAndUpdateIdentity(t *testing.T) {
	t.Parallel()
	now := int64(100)
	file := &meta.FileMeta{Chunks: []int64{}}
	ApplyUploadIdentity("docs/file.txt", nil, file, now)
	// ApplyUploadIdentity stamps creation fields but deliberately leaves
	// the inode at 0: minting happens against the authoritative metadata
	// (InitializeNewFileIdentity), never against a staged snapshot.
	// Owner IDs stay 0 here too: 0 legitimately means root, so the
	// caller provisions them explicitly (OwnerIDsForCreate) instead of
	// this helper stamping the process user (regression: root-owned
	// entries used to become process-owned, passing or failing tests
	// depending on the runner's UID).
	if file.Inode != 0 || file.Mode == 0 || file.UID != 0 || file.GID != 0 || file.UploadedAt != now {
		t.Fatalf("unexpected staged file identity: %+v", file)
	}
	existing := &meta.FileMeta{Inode: 9, Mode: 0o777, UID: 7, GID: 8, AccessedAt: now, UploadedAt: now, ModifiedAt: now, ChangedAt: now, XAttrs: meta.XAttrMap{"user.demo": []byte("1")}}
	updated := &meta.FileMeta{Chunks: []int64{}}
	ApplyUpdatedFileIdentity("", updated, existing, now+60)
	if updated.Inode != 9 || updated.Symlink != "" {
		t.Fatalf("unexpected updated identity: %+v", updated)
	}
	// Replacing a symlink with a regular file must not inherit the target.
	symExisting := &meta.FileMeta{Inode: 11, Mode: 0o777, Symlink: "target", AccessedAt: now, UploadedAt: now, ModifiedAt: now, ChangedAt: now}
	symUpdated := &meta.FileMeta{Chunks: []int64{}}
	ApplyUpdatedFileIdentity("", symUpdated, symExisting, now+60)
	if symUpdated.Symlink != "" || symUpdated.Inode == 11 {
		t.Fatalf("symlink identity leaked onto regular file: %+v", symUpdated)
	}
	if updated.ModifiedAt == 0 || updated.ChangedAt == 0 || updated.AccessedAt == 0 {
		t.Fatalf("expected timestamps to be set: %+v", updated)
	}
}

func TestReplaceInodeFamilyAndHelpers(t *testing.T) {
	t.Parallel()
	now := int64(200)
	repo := meta.NewRepoMetadata("demo")
	repo.EnsureDirectory("docs", now)

	chunk1 := meta.ChunkInfo{Offset: 0, Size: 1, Release: "v1", AssetID: 1}
	repo.Chunks()[1] = chunk1
	base := meta.FileMeta{Size: 1, Chunks: []int64{1}}
	repo.UpsertFile("docs/a.txt", base, now)
	first := repo.FindFile("docs/a.txt")
	clone := first.Clone()
	repo.UpsertFile("docs/b.txt", clone, now)

	chunk2 := meta.ChunkInfo{Offset: 0, Size: 1, Release: "v2", AssetID: 2}
	repo.Chunks()[2] = chunk2
	updated := first.Clone()
	updated.Chunks = []int64{2}
	ReplaceInodeFamily(repo, "docs/a.txt", first, updated, now+60)
	got := repo.FindFilesByInode(first.Inode)
	if len(got) != 2 {
		t.Fatalf("expected 2 files, got %d: %+v", len(got), got)
	}
	for _, name := range got {
		f := repo.FindFile(name)
		if f == nil || len(f.Chunks) != 1 || f.Chunks[0] != 2 {
			t.Fatalf("expected chunk_2 on %s, got %+v", name, f)
		}
	}

	missing := &meta.FileMeta{Inode: 99}
	updated2 := first.Clone()
	updated2.Chunks = []int64{2}
	ReplaceInodeFamily(repo, "docs/missing.txt", missing, updated2, now)
	if repo.FindFile("docs/missing.txt") == nil {
		t.Fatal("expected missing inode family to fall back to upsert")
	}
	if got := repo.FindFile("docs/missing.txt"); got.Inode != first.Inode {
		t.Fatalf("fallback must keep the staged inode, got %d want %d", got.Inode, first.Inode)
	}
}
