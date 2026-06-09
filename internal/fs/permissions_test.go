package fs

import (
	"context"
	"errors"
	"syscall"
	"testing"

	storcfg "github.com/FarelRA/storhub/internal/config"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestCheckStickyDelete(t *testing.T) {
	now := int64(280)
	repo := meta.NewRepoMetadata("demo")
	repo.EnsureRelease("v1", now)
	repo.EnsureDirectory("tmp", now)
	dir := repo.GetDirectory("tmp")
	dir.Mode = 0o1777
	dir.UID = 1
	dir.GID = 2
	repo.Dirs["tmp"] = *dir
	file := meta.FileMeta{Mode: 0o644, UID: 11, GID: 12, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}
	repo.UpsertFile("tmp/note.txt", file, now)
	ctx := WithIdentity(context.Background(), Identity{UID: 22, GID: 22, Groups: []uint32{22}})
	if err := CheckStickyDelete(ctx, repo, "tmp", "tmp/note.txt"); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("expected sticky delete denial, got %v", err)
	}
	ownerCtx := WithIdentity(context.Background(), Identity{UID: 11, GID: 12, Groups: []uint32{12}})
	if err := CheckStickyDelete(ownerCtx, repo, "tmp", "tmp/note.txt"); err != nil {
		t.Fatalf("expected file owner delete success, got %v", err)
	}
}

func TestShouldUpdateAtimePolicy(t *testing.T) {
	now := int64(1000)
	old := now - 172800
	recent := now - 3600
	if ShouldUpdateAtime(storcfg.AtimeNo, old, old, old, now) {
		t.Fatal("expected noatime to skip updates")
	}
	if !ShouldUpdateAtime(storcfg.AtimeStrict, recent, now, now, now) {
		t.Fatal("expected strictatime to always update")
	}
	if ShouldUpdateAtime(storcfg.AtimeRelatime, recent, old, old, now) {
		t.Fatal("expected relatime to skip recent atime")
	}
	if !ShouldUpdateAtime(storcfg.AtimeRelatime, old, old, old, now) {
		t.Fatal("expected relatime to update stale atime")
	}
}
