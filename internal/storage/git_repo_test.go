package storage

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// The go-git fixture is expensive to build (2 inits + commits + push) and
// cheap to copy, so the package seeds ONE two-commit bare template behind a
// sync.Once and every test gets its own copy of it. Isolation is preserved
// (each test pushes into its private copy), while the seed cost is paid at
// most once per package run. TestMain removes the template afterwards.
var (
	gitSeedOnce  sync.Once
	gitSeedRoot  string
	gitSeedBare  string
	gitSeedError error
)

func TestMain(m *testing.M) {
	code := m.Run()
	shutdownSharedMockServer()
	if gitSeedRoot != "" {
		_ = os.RemoveAll(gitSeedRoot)
	}
	os.Exit(code)
}

// seedBareMetadataRepo returns the URL of a private copy of the shared
// bare repository holding two commits of .storhub/metadata.json on main,
// exercising the full clone/commit/push/list/squash machinery without
// GitHub.
func seedBareMetadataRepo(t *testing.T) string {
	t.Helper()
	gitSeedOnce.Do(func() { gitSeedRoot, gitSeedBare, gitSeedError = buildBareMetadataTemplate() })
	if gitSeedError != nil {
		t.Fatalf("seed bare template: %v", gitSeedError)
	}
	dest := filepath.Join(t.TempDir(), "demo.git")
	if err := copyTree(gitSeedBare, dest); err != nil {
		t.Fatalf("copy seeded repo: %v", err)
	}
	return "file://" + dest
}

func buildBareMetadataTemplate() (root, bareDir string, err error) {
	root, err = os.MkdirTemp("", "storhub-git-seed-*")
	if err != nil {
		return "", "", fmt.Errorf("seed temp: %w", err)
	}
	fail := func(format string, args ...any) (string, string, error) {
		return "", "", fmt.Errorf(format, args...)
	}
	bareDir = filepath.Join(root, "demo.git")
	bare, err := git.PlainInit(bareDir, true)
	if err != nil {
		return fail("init bare: %v", err)
	}
	if err := bare.Storer.SetReference(plumbNewHead()); err != nil {
		return fail("set HEAD: %v", err)
	}

	work := filepath.Join(root, "work")
	repo, err := git.PlainInit(work, false)
	if err != nil {
		return fail("init work: %v", err)
	}
	// PlainInit defaults to master; pin main before any commit lands.
	if err := repo.Storer.SetReference(plumbNewHead()); err != nil {
		return fail("set work HEAD: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		return fail("worktree: %v", err)
	}
	mustCommit := func(content string, msg string) error {
		p := filepath.Join(work, metadataFilePath)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			return err
		}
		if _, err := wt.Add(metadataFilePath); err != nil {
			return err
		}
		sig := &object.Signature{Name: "test", Email: "t@e.st", When: time.Now()}
		_, err := wt.Commit(msg, &git.CommitOptions{Author: sig, Committer: sig, AllowEmptyCommits: true})
		return err
	}
	if err := mustCommit(`{"v":4,"p":"demo"}`, "seed v1"); err != nil {
		return fail("commit seed v1: %v", err)
	}
	if err := mustCommit(`{"v":4,"p":"demo","tf":1}`, "seed v2"); err != nil {
		return fail("commit seed v2: %v", err)
	}

	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"file://" + bareDir}}); err != nil {
		return fail("remote: %v", err)
	}
	if err := repo.PushContext(context.Background(), &git.PushOptions{RemoteName: "origin", RefSpecs: pushMainRefSpecs()}); err != nil {
		return fail("push: %v", err)
	}
	return root, bareDir, nil
}

// copyTree recursively copies src (a small bare git dir) into dst.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}

func TestGitRepoLocalHarnessLifecycle(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
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
