package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func uploadFile(t *testing.T, hub *StorHub, project, remote, content string) {
	t.Helper()
	in := writeTempFile(t, t.TempDir(), filepath.Base(remote), []byte(content))
	if _, err := hub.UploadFileContext(context.Background(), project, remote, in); err != nil {
		t.Fatalf("upload %s: %v", remote, err)
	}
	if err := hub.FlushProjectContext(context.Background(), project); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func TestRevertPathRestoresOlderFileLeavingOthers(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.MkdirContext(ctx, "rp", "docs"); err != nil {
		t.Fatal(err)
	}
	uploadFile(t, hub, "rp", "docs/keep.txt", "keep")
	revs, err := hub.ListMetadataRevisionsContext(ctx, "rp")
	if err != nil || len(revs) == 0 {
		t.Fatalf("revisions: %v", err)
	}
	firstSHA := revs[0].CommitSHA
	uploadFile(t, hub, "rp", "docs/later.txt", "later")

	// Revert only docs/later.txt to the revision before it existed: it is
	// removed, while docs/keep.txt survives.
	if err := hub.RevertPathContext(ctx, "rp", "docs/later.txt", firstSHA); err != nil {
		t.Fatalf("revert path: %v", err)
	}
	hub2 := backend.newClient(t, smallTransferTestConfig())
	m, _, err := hub2.loadRepoMetadataFresh(ctx, "rp")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if m.FindFile("docs/keep.txt") == nil {
		t.Fatal("revert of one path must not touch another")
	}
	if m.FindFile("docs/later.txt") != nil {
		t.Fatal("revert did not remove the newer file")
	}
	// Still on the split layout (a revert is a new commit, not a downgrade).
	if !mockHas(backend, "rp", ".storhub/index.json") {
		t.Fatal("revert must keep the split layout")
	}
}

func TestRevertPathRestoresPriorContent(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	uploadFile(t, hub, "rc", "f.txt", "version-one")
	revs, err := hub.ListMetadataRevisionsContext(ctx, "rc")
	if err != nil || len(revs) == 0 {
		t.Fatalf("revisions: %v", err)
	}
	firstSHA := revs[0].CommitSHA
	// Replace (not upload) to change existing content.
	in2 := writeTempFile(t, t.TempDir(), "f.txt", []byte("version-two-longer"))
	if _, err := hub.ReplaceFileContext(ctx, "rc", "f.txt", in2); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "rc"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := hub.RevertPathContext(ctx, "rc", "f.txt", firstSHA); err != nil {
		t.Fatalf("revert: %v", err)
	}
	out := filepath.Join(t.TempDir(), "f.txt")
	if err := hub.DownloadFile("rc", "f.txt", out); err != nil {
		t.Fatalf("download: %v", err)
	}
	data, _ := os.ReadFile(out)
	if string(data) != "version-one" {
		t.Fatalf("revert did not restore prior content: %q", data)
	}
}

func TestRevertPathRejectsBranchAndRoot(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	uploadFile(t, hub, "rr", "f.txt", "x")
	if err := hub.RevertPathContext(ctx, "rr", "f.txt", "main"); err == nil {
		t.Fatal("revert must reject a branch name as a revision")
	}
	if err := hub.RevertPathContext(ctx, "rr", "", "deadbeef"); err == nil {
		t.Fatal("revert must reject the root path")
	}
}
