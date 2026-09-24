package posix

import (
	"context"
	"errors"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// swapBackend simulates a concurrent rename/replace of a symlink component
// in the window between a verb's pre-transaction snapshot load and its
// update transaction.
type swapBackend struct {
	*testBackend
	linkPath  string
	newTarget string
	swapped   bool
}

func (b *swapBackend) UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error) {
	if !b.swapped {
		b.swapped = true
		if f := b.repo.FindFile(b.linkPath); f != nil {
			clone := f.Clone()
			clone.Symlink = b.newTarget
			clone.Size = int64(len(b.newTarget))
			b.repo.WriteFileDirect(b.linkPath, clone)
		}
	}
	return b.testBackend.UpdateRepoMetadataContext(ctx, project, fn, message)
}

// TestLinkReResolvesLiveState pins the stale-chain close for hardlinks: when
// a/link flips from pub to vault mid-op, the transaction must authorize the
// live walk (EACCES on vault) instead of the stale one.
func TestLinkReResolvesLiveState(t *testing.T) {
	base := newTestBackend(100)
	base.seedDir("a")
	base.seedDir("pub")
	base.seedDir("vault")
	setDir := func(path string, mode uint32, uid, gid uint32) {
		d := base.repo.GetDirectory(path)
		clone := d.Clone()
		clone.Mode, clone.UID, clone.GID = mode, uid, gid
		base.repo.WriteDirDirect(path, clone)
	}
	setDir("a", 0o755, 1000, 1000)
	setDir("pub", 0o777, 1, 1)
	setDir("vault", 0o700, 1, 1)
	base.repo.UpsertFile("a/link", meta.FileMeta{Symlink: "/pub", Mode: 0o777, UID: 1, GID: 2}, base.now)
	base.seedFile("pub/victim")
	base.seedFile("vault/victim")
	backend := &swapBackend{testBackend: base, linkPath: "a/link", newTarget: "/vault"}
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 1000, GID: 1000})
	if _, err := svc.LinkContext(ctx, "demo", "a/link/victim", "a/dst"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("link through a swapped component must fail closed, got %v", err)
	}
	if base.repo.FindFile("a/dst") != nil {
		t.Fatal("failed link must not create the destination")
	}
}

// TestReadlinkLeavesAtimeAlone pins the stat-consistent contract: readlink
// returns the target without queueing an atime update.
func TestReadlinkLeavesAtimeAlone(t *testing.T) {
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})
	backend := newTestBackend(400)
	backend.seedDir("docs")
	if _, err := NewService(backend).SymlinkContext(ctx, "demo", "docs/base.txt", "docs/base.link"); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	parked := backend.repo.FindFile("docs/base.link").Clone()
	parked.AccessedAt = 1
	backend.repo.WriteFileDirect("docs/base.link", parked)
	svc := NewService(backend)
	target, err := svc.ReadlinkContext(ctx, "demo", "docs/base.link")
	if err != nil || target != "docs/base.txt" {
		t.Fatalf("readlink: %q %v", target, err)
	}
	if got := backend.repo.FindFile("docs/base.link").AccessedAt; got != 1 {
		t.Fatalf("readlink bumped atime to %d, want untouched 1", got)
	}
}
