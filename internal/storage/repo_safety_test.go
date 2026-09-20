package storage

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// TestUpdateRepoMetadataReturnsSharedSnapshot pins the read-only sharing
// contract: the returned metadata is the live tree, not a copy. Readers get
// O(1) snapshots with zero copying; writers MUST go through
// UpdateRepoMetadataContext - mutating the returned value directly is a
// contract violation (COW shares backing storage with live state).
func TestUpdateRepoMetadataReturnsClone(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{DisableGitBackend: true})
	ctx := context.Background()

	first, err := hub.UpdateRepoMetadataContext(ctx, "clone-probe", func(m *meta.RepoMetadata) error {
		m.UpsertFile("real.txt", meta.FileMeta{Chunks: []int64{}, Size: 1, Mode: 0o644}, 1700000000)
		return nil
	}, "storhub: add real")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if first.FindFile("real.txt") == nil {
		t.Fatal("expected real.txt in returned snapshot")
	}

	// A no-op transaction must observe the committed state, and the
	// returned value must be the shared live tree itself: pointer
	// identity with the reader cache proves zero-copy sharing (the old
	// Clone contract returned a detached copy here).
	live, _, ok := hub.cachedRepoMetadata("clone-probe")
	if !ok {
		t.Fatal("expected cached metadata after update")
	}
	if live != first {
		t.Fatal("UpdateRepoMetadataContext must return the shared live tree, not a Clone")
	}
	second, err := hub.UpdateRepoMetadataContext(ctx, "clone-probe", func(_ *meta.RepoMetadata) error {
		return nil
	}, "storhub: noop")
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
	if second.FindFile("real.txt") == nil {
		t.Fatal("committed file real.txt missing from second snapshot")
	}
}

// TestCommitRepoMetadataGitPathRejectsStalePreviousSHA proves compare-and-swap
// on the git path: a write carrying a stale version token must fail with a
// 409 conflict (like the REST path) instead of silently overwriting.
func TestCommitRepoMetadataGitPathRejectsStalePreviousSHA(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{GitCacheDir: t.TempDir()})
	hub.getGitRepo("cas-probe").remoteBase = url
	ctx := context.Background()

	m := meta.NewRepoMetadata("cas-probe")
	_, _, err := hub.commitRepoMetadata(ctx, "cas-probe", m, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "storhub: stale write")
	if err == nil {
		t.Fatal("expected conflict for stale previousSHA on git path, got nil")
	}
	var apiErr *ghapi.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 conflict, got %v", err)
	}
}

// TestSquashHistorySyncsFromRemote proves ordering: squash must observe the
// latest remote content, not the locally cached HEAD.
func TestSquashHistorySyncsFromRemote(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	ctx := context.Background()
	a := newGitRepo(t.TempDir(), "owner", "demo", "")
	a.remoteBase = url
	b := newGitRepo(t.TempDir(), "owner", "demo", "")
	b.remoteBase = url

	if err := a.ensure(ctx); err != nil {
		t.Fatalf("clone A: %v", err)
	}
	marker := `{"v":4,"p":"demo","concurrent":1}`
	if _, _, err := b.writeCommitPush(ctx, metadataFilePath, []byte(marker), "concurrent"); err != nil {
		t.Fatalf("concurrent write: %v", err)
	}
	if err := a.squashHistory(ctx, metadataFilePath, "squashed"); err != nil {
		t.Fatalf("squash: %v", err)
	}
	data, err := a.readFileContentsNoLock(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("read after squash: %v", err)
	}
	if !strings.Contains(string(data), `"concurrent":1`) {
		t.Fatalf("squash kept stale local HEAD, losing remote write: %s", data)
	}
}

// TestSquashHistoryCASRejectsStaleBase proves the push is a compare-and-swap,
// not a blind force-push: a stale expected-old-OID aborts with 409 before
// mutating anything, while a fresh base succeeds.
func TestSquashHistoryCASRejectsStaleBase(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	ctx := context.Background()
	a := newGitRepo(t.TempDir(), "owner", "demo", "")
	a.remoteBase = url
	b := newGitRepo(t.TempDir(), "owner", "demo", "")
	b.remoteBase = url

	if err := a.ensure(ctx); err != nil {
		t.Fatalf("clone A: %v", err)
	}
	stale := a.headCommitSHA()
	if stale == "" {
		t.Fatal("expected HEAD sha")
	}
	before, err := a.listFileCommits(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("list before: %v", err)
	}
	if _, _, err := b.writeCommitPush(ctx, metadataFilePath, []byte(`{"v":4,"p":"demo","moved":1}`), "concurrent"); err != nil {
		t.Fatalf("concurrent write: %v", err)
	}
	if err := a.squashHistoryCAS(ctx, metadataFilePath, "stale squash", stale); err == nil {
		t.Fatal("expected conflict for stale squash base, got nil")
	} else {
		var apiErr *ghapi.APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
			t.Fatalf("expected 409 conflict, got %v", err)
		}
	}
	after, err := a.listFileCommits(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("list after: %v", err)
	}
	if len(after) != len(before)+1 {
		t.Fatalf("failed squash must not mutate history: before=%d after=%d", len(before), len(after))
	}
	// Fresh base succeeds and preserves the concurrent write.
	if err := a.squashHistory(ctx, metadataFilePath, "fresh squash"); err != nil {
		t.Fatalf("fresh squash: %v", err)
	}
	data, err := a.readFileContentsNoLock(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("read after squash: %v", err)
	}
	if !strings.Contains(string(data), `"moved":1`) {
		t.Fatalf("fresh squash lost concurrent write: %s", data)
	}
}

// TestRollbackGitPathHappyPath guards the Rollback region edit: a rollback
// with a fresh token on the git path must still succeed end to end.
func TestRollbackGitPathHappyPath(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	backend := newMockGitHub(t)
	// Same guard as TestEnsureRepoUsesExistenceCheckBeforeCreate: fixture
	// writes must hold the backend lock against shared-server readers.
	backend.mu.Lock()
	backend.repos["demo"] = &mockRepo{
		name:          "demo",
		nextReleaseID: 1,
		nextBlobID:    1,
		nextCommitID:  1,
		releasesByTag: make(map[string]*mockRelease),
		releasesByID:  make(map[int64]*mockRelease),
		assets:        make(map[int64]*mockAsset),
		files:         make(map[string]*mockFile),
		commitsByPath: make(map[string][]mockCommit),
	}
	backend.mu.Unlock()
	hub := backend.newClient(t, Config{GitCacheDir: t.TempDir()})
	hub.getGitRepo("demo").remoteBase = url
	ctx := context.Background()

	revs, err := hub.listMetadataRevisions(ctx, "demo")
	if err != nil {
		t.Fatalf("list revisions: %v", err)
	}
	if len(revs) < 2 {
		t.Fatalf("expected seeded history, got %d", len(revs))
	}
	oldest := revs[len(revs)-1].CommitSHA
	if err := hub.RollbackMetadataContext(ctx, "demo", oldest); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	// The rollback republished the index on the split layout (version 5):
	// HEAD now carries a manifest, and the rolled-back tree loads.
	if _, err := hub.getGitRepo("demo").readFileRef(ctx, "HEAD", indexFilePath); err != nil {
		t.Fatalf("rollback did not publish a v5 manifest on the git path: %v", err)
	}
	m, _, err := hub.loadRepoMetadataFresh(ctx, "demo")
	if err != nil {
		t.Fatalf("read rolled-back tree: %v", err)
	}
	if m.TotalFiles != 0 {
		t.Fatalf("expected rollback to the empty oldest revision, got %d files", m.TotalFiles)
	}
}
