package fs

import (
	"context"
	"errors"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// TestRemapPathBoundary pins the slash-boundary assert: a target that merely
// shares a string prefix with the old base must pass through unchanged,
// not remap to a nonsense key.
func TestRemapPathBoundary(t *testing.T) {
	t.Parallel()
	if got := RemapPath("a", "x", "a"); got != "x" {
		t.Fatalf("exact base: got %q, want x", got)
	}
	if got := RemapPath("a", "x", "a/b/c"); got != "x/b/c" {
		t.Fatalf("child: got %q, want x/b/c", got)
	}
	if got := RemapPath("a", "x", "ab/c"); got != "ab/c" {
		t.Fatalf("prefix-only target must pass through, got %q", got)
	}
}

// swapBackend simulates a concurrent rename/replace of a symlink component
// in the window between a verb's pre-transaction snapshot load and its
// update transaction: the first Update call retargets one link before the
// transaction body runs.
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

// seedSwapTree builds: attacker-owned "a" holding a link to world-readable
// "pub", plus an owner-only "vault". Both dirs hold a "victim" file so the
// live resolution finds a node and the verdict comes from DAC, not ENOENT.
func seedSwapTree(backend *testBackend) {
	backend.seedDir("a")
	backend.seedDir("pub")
	backend.seedDir("vault")
	setDir := func(path string, mode uint32, uid, gid uint32) {
		d := backend.repo.GetDirectory(path)
		clone := d.Clone()
		clone.Mode, clone.UID, clone.GID = mode, uid, gid
		backend.repo.WriteDirDirect(path, clone)
	}
	setDir("a", 0o755, 1000, 1000)
	setDir("pub", 0o777, 1, 1)
	setDir("vault", 0o700, 1, 1)
	backend.repo.UpsertFile("a/link", meta.FileMeta{Symlink: "/pub", Mode: 0o777, UID: 1, GID: 2}, backend.now)
	backend.seedFile("pub/victim", []byte("x"))
	backend.seedFile("vault/victim", []byte("x"))
}

func swapAttackerCtx() context.Context {
	return WithIdentity(context.Background(), Identity{UID: 1000, GID: 1000})
}

// TestRenameReResolvesLiveState pins the stale-chain close: when a/link flips
// from pub to vault mid-op, the transaction must authorize the live walk
// (EACCES on vault) instead of the stale one.
func TestRenameReResolvesLiveState(t *testing.T) {
	base := newTestBackend(100)
	seedSwapTree(base)
	backend := &swapBackend{testBackend: base, linkPath: "a/link", newTarget: "/vault"}
	svc := NewService(backend)
	if err := svc.RenameContext(swapAttackerCtx(), "demo", "a/link/victim", "a/dst"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("rename through a swapped link must fail closed, got %v", err)
	}
	if backend.repo.FindFile("pub/victim") == nil {
		t.Fatal("failed rename must not move the source")
	}
}

// TestCopyReResolvesLiveState pins the same close for copies: a swapped
// link must deny the read-side DAC on the live walk.
func TestCopyReResolvesLiveState(t *testing.T) {
	base := newTestBackend(100)
	seedSwapTree(base)
	backend := &swapBackend{testBackend: base, linkPath: "a/link", newTarget: "/vault"}
	svc := NewService(backend)
	if err := svc.CopyContext(swapAttackerCtx(), "demo", "a/link/victim", "a/dst"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("copy through a swapped link must fail closed, got %v", err)
	}
	if backend.repo.FindFile("a/dst") != nil {
		t.Fatal("failed copy must not create the destination")
	}
}

// chmodRacerBackend simulates a concurrent chmod racing a no-op truncate:
// the transaction body runs against a tree whose mode already changed.
type chmodRacerBackend struct {
	*testBackend
	target string
	mode   uint32
	raced  bool
}

func (b *chmodRacerBackend) UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error) {
	if !b.raced {
		b.raced = true
		if f := b.repo.FindFile(b.target); f != nil {
			clone := f.Clone()
			clone.Mode = b.mode
			b.repo.WriteFileDirect(b.target, clone)
		}
	}
	return b.testBackend.UpdateRepoMetadataContext(ctx, project, fn, message)
}

// TestTruncateNoOpReturnsLiveMode pins the live-clone return: a chmod
// landing between the pre-transaction snapshot and the touch transaction
// must be reflected in the returned entry, not just the stored one.
func TestTruncateNoOpReturnsLiveMode(t *testing.T) {
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0, Admin: true})
	base := newTestBackend(100)
	backend := &chmodRacerBackend{testBackend: base, target: "docs/f.txt", mode: 0o600}
	backend.seedDir("docs")
	seeded := backend.seedFile("docs/f.txt", []byte("hello"))
	svc := NewService(backend)
	got, err := svc.TruncateFileContext(ctx, "demo", "docs/f.txt", seeded.Size)
	if err != nil {
		t.Fatalf("no-op truncate: %v", err)
	}
	if got.Mode != 0o600 {
		t.Fatalf("returned mode = %o, want live 600", got.Mode)
	}
	if live := backend.repo.FindFile("docs/f.txt"); live == nil || live.Mode != 0o600 {
		t.Fatalf("stored mode diverged from returned mode: %+v", live)
	}
}
