package storage

import (
	"context"
	"errors"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// TestCloneRangeRechecksParentInTransaction pins the in-transaction parent
// recheck: CloneRange authorizes against a readonly snapshot, so a
// concurrent rmdir landing before the transaction must fail the clone with
// NotFound instead of letting UpsertFile silently recreate a process-owned
// parent chain. The transaction is driven directly with a stale pre-check
// decision to deterministically model the interleave.
func TestCloneRangeRechecksParentInTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectcloneparentrecheck"

	if err := hub.MkdirContext(ctx, project, "src"); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	cloneSeedFile(t, hub, project, "src/a.bin", []byte("0123456789ABCDEF"))
	if err := hub.MkdirContext(ctx, project, "dst"); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}

	// Capture the pre-check decision while dst/ still exists.
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	srcFile := repo.FindFile("src/a.bin")
	if srcFile == nil {
		t.Fatal("seed file missing")
	}
	snapSrc := cloneFileSnapshot{size: srcFile.Size, chunks: append([]int64(nil), srcFile.Chunks...)}
	snapDst := cloneFileSnapshot{existed: false}
	now := hub.config.Now().Unix()

	// The concurrent rmdir lands between the pre-check and the transaction.
	if err := hub.RmdirContext(ctx, project, "dst"); err != nil {
		t.Fatalf("rmdir dst: %v", err)
	}

	_, err = hub.UpdateRepoMetadataContext(ctx, project, func(tree *RepoMetadata) error {
		return hub.applyCloneRange(ctx, tree, "src/a.bin", "dst/b.bin", 0, 0, 8, snapSrc, snapDst, now)
	}, "storhub: clone-range src/a.bin to dst/b.bin")
	if err == nil {
		t.Fatal("clone into a concurrently removed parent must fail (NotFound-or-caller-owned)")
	}
	if !errors.Is(err, shfs.ErrNotFound) {
		t.Fatalf("clone into removed parent: want NotFound, got %v", err)
	}
	live, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if live.FindFile("dst/b.bin") != nil {
		t.Fatal("rejected clone created the destination (not all-or-nothing)")
	}
	if live.HasDirectory("dst") {
		t.Fatal("rejected clone resurrected the removed parent directory")
	}
}
