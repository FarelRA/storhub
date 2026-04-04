package fs

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestCheckStickyDelete(t *testing.T) {
	now := time.Unix(280, 0).UTC()
	repo := meta.NewRepoMetadata("demo")
	repo.EnsureRelease("v1", now)
	repo.EnsureDirectory("tmp", now)
	dir := repo.GetDirectory("tmp")
	dir.Mode = 0o1777
	dir.UID = 1
	dir.GID = 2
	file := meta.FileMetadata{Name: "tmp/note.txt", Kind: meta.NodeKindFile, Release: "v1", Mode: 0o644, UID: 11, GID: 12, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}
	repo.UpsertFile(file, now)
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
	now := time.Unix(1000, 0).UTC()
	old := now.Add(-48 * time.Hour)
	recent := now.Add(-time.Hour)
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
