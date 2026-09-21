package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// TestExpectedRevisionCAS pins the compare-and-swap primitive: a mutation
// carrying WithExpectedRevision succeeds only while the remote metadata
// revision still matches the declared one.
func TestExpectedRevisionCAS(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := Config{
		ChunkSize:         32 << 20,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	}
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	project := "projectcas"

	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	rev, err := hub.RevisionContext(ctx, project)
	if err != nil || rev == "" {
		t.Fatalf("revision: %q %v", rev, err)
	}

	seed := writeTempFile(t, t.TempDir(), "cas.txt", []byte("guarded payload"))
	if _, err := hub.UploadFileContext(ctx, project, "docs/cas.txt", seed); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	// Capture AFTER all seeding so the expectation matches current HEAD.
	guardedRev, err := hub.RevisionContext(ctx, project)
	if err != nil {
		t.Fatalf("guarded revision: %v", err)
	}
	if _, err := hub.AppendFileContext(ctx, project, "docs/cas.txt", []byte("x"), shfs.WithExpectedRevision(guardedRev)); err != nil {
		t.Fatalf("mutation under matching revision must succeed: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush guarded change: %v", err)
	}

	// Advance the remote behind the first client's back.
	other := backend.newClient(t, cfg)
	if err := other.MkdirContext(ctx, project, "competing"); err != nil {
		t.Fatalf("competing mkdir: %v", err)
	}
	if err := other.FlushMetadata(ctx); err != nil {
		t.Fatalf("competing flush: %v", err)
	}

	staleRev := rev // captured before the competing writer advanced HEAD
	err = hub.DeleteFileContext(ctx, project, "docs/cas.txt", shfs.WithExpectedRevision(staleRev))
	if !errors.Is(err, shfs.ErrPreconditionFailed) {
		t.Fatalf("stale revision must fail with ErrPreconditionFailed, got %v", err)
	}

	// Re-reading and retrying converges.
	freshRev, err := hub.RevisionContext(ctx, project)
	if err != nil {
		t.Fatalf("fresh revision: %v", err)
	}
	if freshRev == staleRev {
		t.Fatal("remote revision should have advanced")
	}
	observer := backend.newClient(t, cfg)
	files, _ := observer.ListFilesContext(context.Background(), project)
	t.Logf("remote files after competing write: %v", files)
	if _, err := hub.StatPathContext(ctx, project, "docs/cas.txt"); err != nil {
		t.Fatalf("precondition debug: cas.txt visibility: %v", err)
	}
	if err := hub.DeleteFileContext(ctx, project, "docs/cas.txt", shfs.WithExpectedRevision(freshRev)); err != nil {
		t.Fatalf("mutation under fresh revision must succeed: %+v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush delete: %v", err)
	}
}

// casConflict must classify go-git's real lease failures as 409 and
// must NOT misread "release" prose (hooks, URLs) as a conflict - the old
// bare "lease" substring did.
func TestCasConflictWordBoundary(t *testing.T) {
	t.Parallel()
	base := plumbing.ZeroHash
	if err := casConflict(errors.New("remote: hook declined push for releases/v9"), base); err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
			t.Fatal("error text containing 'release' must not be misread as a lease conflict")
		}
	}
	conflict := casConflict(errors.New("non-fast-forward update: refs/heads/main"), base)
	var apiErr *ghapi.APIError
	if !errors.As(conflict, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("non-fast-forward must map to 409, got %v", conflict)
	}
	conflict = casConflict(errors.New("! [rejected] main -> main (stale info)"), base)
	if !errors.As(conflict, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("stale info must map to 409, got %v", conflict)
	}
}

// listFileCommits must count only commits that CHANGE the path
// (REST commits?path= semantics), not every commit whose tree contains it.
func TestListFileCommitsOnlyTouchingCommits(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	bareDir := filepath.Join(t.TempDir(), "touch.git")
	bare, err := git.PlainInit(bareDir, true)
	if err != nil {
		t.Fatalf("init bare: %v", err)
	}
	if err := bare.Storer.SetReference(plumbNewHead()); err != nil {
		t.Fatalf("set HEAD: %v", err)
	}
	work := filepath.Join(t.TempDir(), "touchwork")
	repo, err := git.PlainInit(work, false)
	if err != nil {
		t.Fatalf("init work: %v", err)
	}
	if err := repo.Storer.SetReference(plumbNewHead()); err != nil {
		t.Fatalf("set work HEAD: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	commit := func(name, content, msg string) {
		t.Helper()
		p := filepath.Join(work, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := wt.Add(name); err != nil {
			t.Fatal(err)
		}
		sig := &object.Signature{Name: "test", Email: "t@e.st", When: time.Now()}
		if _, err := wt.Commit(msg, &git.CommitOptions{Author: sig, Committer: sig}); err != nil {
			t.Fatalf("commit %q: %v", msg, err)
		}
	}
	commit(metadataFilePath, "one", "index: first")
	commit("other.txt", "x", "other only") // must NOT count for metadataFilePath
	commit(metadataFilePath, "two", "index: second")
	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"file://" + bareDir}}); err != nil {
		t.Fatalf("remote: %v", err)
	}
	if err := repo.PushContext(context.Background(), &git.PushOptions{RemoteName: "origin", RefSpecs: pushMainRefSpecs()}); err != nil {
		t.Fatalf("push: %v", err)
	}
	r := newGitRepo(t.TempDir(), "owner", "touch", "")
	r.remoteBase = "file://" + bareDir
	revs, err := r.listFileCommits(context.Background(), metadataFilePath)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(revs) != 2 {
		t.Fatalf("only commits that change the path may count: got %d (%+v)", len(revs), revs)
	}
	if revs[0].Message != "index: second" || revs[1].Message != "index: first" {
		t.Fatalf("unexpected revision list %+v", revs)
	}
}

// readFileHead must answer from the HEAD tree. A file left untracked
// by a write canceled before Commit survives every HardReset; serving the
// worktree filesystem would present that ghost as HEAD truth.
func TestReadFileHeadIgnoresUntrackedGhost(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	r := newGitRepo(t.TempDir(), "owner", "ghost", "")
	r.remoteBase = url
	ctx := context.Background()
	if err := r.ensure(ctx); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := r.sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	ghost := filepath.Join(r.dir, ".storhub", "ghost.txt")
	if err := os.WriteFile(ghost, []byte("never committed"), 0o644); err != nil {
		t.Fatalf("plant ghost: %v", err)
	}
	if _, err := r.readFileHead(ctx, ".storhub/ghost.txt"); err == nil {
		t.Fatal("readFileHead must not serve an untracked worktree ghost as HEAD truth")
	}
	// Tracked reads still work.
	if data, err := r.readFileHead(ctx, metadataFilePath); err != nil || len(data) == 0 {
		t.Fatalf("tracked HEAD read broken: %v", err)
	}
}

// headCommitSHA runs off-lock against a repo that release() nils. The
// race detector must see no unsynchronized access while it is called
// concurrently with mutations.
func TestHeadCommitSHALockedAgainstRelease(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	r := newGitRepo(t.TempDir(), "owner", "race", "")
	r.remoteBase = url
	ctx := context.Background()
	if err := r.ensure(ctx); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Two spinners + five push cycles + a yield per spin: the race
	// invariant is proven by ANY overlap of headCommitSHA with the
	// mutation/release path, not by millions of instrumented iterations.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = r.headCommitSHA()
					runtime.Gosched()
				}
			}
		}()
	}
	for i := 0; i < 5; i++ {
		if _, _, err := r.writeCommitPush(ctx, metadataFilePath, []byte(fmt.Sprintf(`{"v":4,"p":"race","i":%d}`, i)), fmt.Sprintf("cycle %d", i)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	if err := r.release(true); err != nil {
		t.Fatalf("release: %v", err)
	}
	for i := 0; i < 100; i++ {
		if got := r.headCommitSHA(); got != "" {
			t.Fatalf("headCommitSHA after release must report empty, got %q", got)
		}
	}
}
