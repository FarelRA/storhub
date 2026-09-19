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
	t.Parallel()
	now := int64(280)
	repo := meta.NewRepoMetadata("demo")
	if _, err := repo.EnsureRelease("v1", now); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	repo.EnsureDirectory("tmp", now)
	dir := repo.GetDirectory("tmp")
	dir.Mode = 0o1777
	dir.UID = 1
	dir.GID = 2
	repo.Dirs()["tmp"] = *dir
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
	t.Parallel()
	now := int64(1_000_000_000_000)
	old := now - 172800*1e9
	recent := now - 3600*1e9
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

func TestIdentityFromContextFailsClosed(t *testing.T) {
	t.Parallel()
	// An absent identity must never masquerade as an admin: the fallback
	// carries no Admin even when the daemon runs as root (the process uid
	// is not a caller assertion). Pinned without os.Getuid so the test
	// reads identically as root and as CI's UID 1001.
	id := IdentityFromContext(context.Background())
	if id.Admin {
		t.Fatalf("absent identity must not inherit Admin: %+v", id)
	}
	if IdentityPresent(context.Background()) {
		t.Fatal("absent identity must not count as present")
	}

	root := IdentityFromContext(WithIdentity(context.Background(), Identity{UID: 1000}))
	if root.Admin {
		t.Fatalf("explicit non-root identity must stay unprivileged: %+v", root)
	}

	// Admin is an explicit assertion only: a UID-0 identity without
	// Admin:true stays unprivileged (DAC still keys off the UID), and an
	// explicit Admin:true survives the assertion round trip.
	if IdentityFromContext(WithIdentity(context.Background(), Identity{UID: 0})).Admin {
		t.Fatal("uid 0 without an explicit Admin flag must not promote to Admin")
	}
	if !IdentityFromContext(WithIdentity(context.Background(), Identity{UID: 0, Admin: true})).Admin {
		t.Fatal("explicit uid-0 Admin must survive the assertion round trip")
	}
}
