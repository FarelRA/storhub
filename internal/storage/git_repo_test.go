package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// seedBareMetadataRepo builds a local bare repository holding two commits
// of .storhub/metadata.json on main and returns its URL, exercising the
// full clone/commit/push/list/squash machinery without GitHub.
func seedBareMetadataRepo(t *testing.T) string {
	t.Helper()
	bareDir := filepath.Join(t.TempDir(), "demo.git")
	bare, err := git.PlainInit(bareDir, true)
	if err != nil {
		t.Fatalf("init bare: %v", err)
	}
	if err := bare.Storer.SetReference(plumbNewHead()); err != nil {
		t.Fatalf("set HEAD: %v", err)
	}

	work := filepath.Join(t.TempDir(), "work")
	repo, err := git.PlainInit(work, false)
	if err != nil {
		t.Fatalf("init work: %v", err)
	}
	// PlainInit defaults to master; pin main before any commit lands.
	if err := repo.Storer.SetReference(plumbNewHead()); err != nil {
		t.Fatalf("set work HEAD: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	mustCommit := func(content string, msg string) {
		t.Helper()
		p := filepath.Join(work, metadataFilePath)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := wt.Add(metadataFilePath); err != nil {
			t.Fatal(err)
		}
		sig := &object.Signature{Name: "test", Email: "t@e.st", When: time.Now()}
		if _, err := wt.Commit(msg, &git.CommitOptions{Author: sig, Committer: sig, AllowEmptyCommits: true}); err != nil {
			t.Fatalf("commit %q: %v", msg, err)
		}
	}
	mustCommit(`{"v":4,"p":"demo"}`, "seed v1")
	mustCommit(`{"v":4,"p":"demo","tf":1}`, "seed v2")

	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"file://" + bareDir}}); err != nil {
		t.Fatalf("remote: %v", err)
	}
	if err := repo.PushContext(context.Background(), &git.PushOptions{RemoteName: "origin", RefSpecs: pushMainRefSpecs()}); err != nil {
		t.Fatalf("push: %v", err)
	}
	return "file://" + bareDir
}

func TestGitRepoLocalHarnessLifecycle(t *testing.T) {
	url := seedBareMetadataRepo(t)
	r := newGitRepo(t.TempDir(), "owner", "demo", "")
	r.remoteBase = url
	ctx := context.Background()

	if err := r.ensure(ctx); err != nil {
		t.Fatalf("clone from local bare: %v", err)
	}
	head := r.headCommitSHA()
	if head == "" {
		t.Fatal("expected HEAD sha")
	}

	// Write a third revision and read it back through the ref API.
	blob, _ := meta.NewRepoMetadata("demo").ToJSON()
	sha, _, err := r.writeCommitPush(ctx, metadataFilePath, blob, "third")
	if err != nil {
		t.Fatalf("write commit push: %v", err)
	}
	if sha == "" || sha == head {
		t.Fatalf("expected a fresh commit, got %q (head %q)", sha, head)
	}

	revs, err := r.listFileCommits(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("list revisions: %v", err)
	}
	if len(revs) < 3 {
		t.Fatalf("expected full history (3 commits), got %d", len(revs))
	}
	if revs[0].Message == "" || revs[0].CommitSHA == "" {
		t.Fatalf("revisions must carry sha+message, got %+v", revs[0])
	}

	data, err := r.readFileRef(ctx, "HEAD", metadataFilePath)
	if err != nil {
		t.Fatalf("read ref: %v", err)
	}
	if !strings.Contains(string(data), `"p":"demo"`) {
		t.Fatalf("unexpected metadata payload: %s", data)
	}

	// Squash collapses history for the file while keeping content.
	before := len(revs)
	if err := r.squashHistory(ctx, metadataFilePath, "squashed"); err != nil {
		t.Fatalf("squash: %v", err)
	}
	after, err := r.listFileCommits(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("list after squash: %v", err)
	}
	if len(after) >= before {
		t.Fatalf("squash must shrink history: before=%d after=%d", before, len(after))
	}
	data2, err := r.readFileContentsNoLock(ctx, metadataFilePath)
	if err != nil || len(data2) == 0 {
		t.Fatalf("content lost after squash: %v", err)
	}
}

func plumbNewHead() *plumbing.Reference {
	return plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(defaultBranch))
}

func pushMainRefSpecs() []config.RefSpec {
	return []config.RefSpec{config.RefSpec("refs/heads/main:refs/heads/main")}
}
