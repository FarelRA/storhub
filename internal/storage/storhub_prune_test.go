package storage

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestDeleteReleaseHidesCatalogOnly(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallRetryDisabledTestConfig())

	input := writeTempFile(t, t.TempDir(), "release.txt", []byte("release payload"))
	fileMeta, err := hub.UploadFileContext(context.Background(), "projectrelease", "release.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "projectrelease")
	firstRelease := repoMeta.Chunks()[fileMeta.Chunks[0]].Release
	if err := hub.DeleteRelease("projectrelease", firstRelease); err != nil {
		t.Fatalf("delete release: %v", err)
	}
	releases, err := hub.ListReleasesContext(context.Background(), "projectrelease")
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 0 {
		t.Fatalf("expected no catalog releases, got %+v", releases)
	}
	repo := backend.repo("projectrelease")
	if repo == nil || repo.releasesByTag[firstRelease] == nil {
		t.Fatalf("expected immutable release to remain")
	}
}

func TestDeleteProject(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, defaultTestConfig())
	input := writeTempFile(t, t.TempDir(), "file.txt", []byte("payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectdelete", "file.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := hub.DeleteProject("projectdelete"); err != nil {
		t.Fatalf("delete project: %v", err)
	}
	if backend.repo("projectdelete") != nil {
		t.Fatal("expected repo to be deleted")
	}
}

func TestEnsureRepoUsesExistenceCheckBeforeCreate(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	// Guarded by the backend lock: the shared mock server reads repos
	// under mu on other tests' HTTP goroutines, so an unguarded fixture
	// write races parallel CDN reads (race detector, CI race-arm64).
	backend.mu.Lock()
	backend.repos["existing-project"] = &mockRepo{
		name:          "existing-project",
		private:       true,
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
	createCalls := 0
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Path == "/user/repos" {
			createCalls++
		}
		return false
	})
	hub := backend.newClient(t, defaultTestConfig())
	if err := hub.ensureRepo(context.Background(), "existing-project"); err != nil {
		t.Fatalf("ensure existing repo: %v", err)
	}
	if createCalls != 0 {
		t.Fatalf("expected ensureRepo to skip create for existing repo, got %d create calls", createCalls)
	}
}

func TestPruneAssetsScopeRemovesOrphanedAssetsAndReleases(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	inputA := writeTempFile(t, t.TempDir(), "tracked.txt", []byte("tracked payload"))
	tracked, err := hub.UploadFileContext(context.Background(), "projectpurge", "tracked.txt", inputA)
	if err != nil {
		t.Fatalf("upload tracked file: %v", err)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata after first upload: %v", err)
	}

	inputB := writeTempFile(t, t.TempDir(), "orphan.txt", []byte("orphan payload"))
	orphan, err := hub.UploadFileContext(context.Background(), "projectpurge", "orphan.txt", inputB)
	if err != nil {
		t.Fatalf("upload orphan file: %v", err)
	}

	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "projectpurge")
	trackedRelease := repoMeta.Chunks()[tracked.Chunks[0]].Release
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata after second upload: %v", err)
	}

	if err := hub.DeleteFileContext(context.Background(), "projectpurge", "orphan.txt"); err != nil {
		t.Fatalf("hide orphan file: %v", err)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata after delete: %v", err)
	}

	repo := backend.repo("projectpurge")
	if repo == nil {
		t.Fatal("expected repo to exist")
	}
	manualRelease := backend.addRelease(t, "projectpurge", "v999")
	backend.addAssetToRelease(t, "projectpurge", manualRelease.tag, "manual.bin", []byte("manual orphan"))
	backend.addAssetToRelease(t, "projectpurge", trackedRelease, "extra.bin", []byte("extra orphan"))

	result, err := hub.PruneProject("projectpurge", "assets", 0, false)
	if err != nil {
		t.Fatalf("purge untracked: %v", err)
	}
	if result.DeletedReleases != 1 {
		t.Fatalf("expected 1 deleted release, got %+v", result)
	}
	if result.DeletedAssets != len(orphan.Chunks)+1 {
		t.Fatalf("expected hidden-file assets plus one explicitly orphaned tracked-release asset to be deleted, got %+v", result)
	}

	repo = backend.repo("projectpurge")
	if repo.releasesByTag[manualRelease.tag] != nil {
		t.Fatalf("expected manual release %s to be deleted", manualRelease.tag)
	}
	for _, asset := range repo.assets {
		if asset.name == "extra.bin" {
			t.Fatal("expected extra orphan asset to be deleted")
		}
	}
	if repo.releasesByTag[trackedRelease] == nil {
		t.Fatalf("expected tracked release to remain")
	}
	// Find a revision that still contained orphan.txt (whose assets the
	// purge destroyed) to prove rollback after purge fails destructively.
	revisions, err := hub.ListMetadataRevisionsContext(context.Background(), "projectpurge")
	if err != nil {
		t.Fatalf("list metadata revisions: %v", err)
	}
	var orphanRevision string
	for _, rev := range revisions {
		snap, err := hub.getMetadataRevision(context.Background(), "projectpurge", rev.CommitSHA)
		if err != nil {
			t.Fatalf("fetch revision %s: %v", rev.CommitSHA, err)
		}
		if _, ok := snap.Files()["orphan.txt"]; ok {
			orphanRevision = rev.CommitSHA
			break
		}
	}
	if orphanRevision == "" {
		t.Fatal("expected a metadata revision containing orphan.txt")
	}
	if err := hub.RollbackMetadataContext(context.Background(), "projectpurge", orphanRevision); err == nil {
		t.Fatal("expected rollback after purge to fail because purge is destructive")
	}
}

func TestCleanupProjectSkipsNoopCommit(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cleanup.txt", []byte("cleanup payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectcleanup", "cleanup.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	backend.mu.Lock()
	repo := backend.repos["projectcleanup"]
	before := len(repo.commitsByPath[metadataFilePath])
	backend.mu.Unlock()
	if err := hub.CleanupProject("projectcleanup"); err != nil {
		t.Fatalf("cleanup project: %v", err)
	}
	backend.mu.Lock()
	after := len(repo.commitsByPath[metadataFilePath])
	backend.mu.Unlock()
	if after != before {
		t.Fatalf("expected cleanup noop commit count to stay %d, got %d", before, after)
	}
}

// Regression test for the purge data-destruction chain: a patch that spills
// into a new release must register that release in the metadata catalog so
// The assets purge cannot delete it (GitHub cascade-deletes its assets).
func TestPruneAssetsScopeKeepsReleaseAfterPatchSpill(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "spill.txt", []byte("abcdefghijklmno"))
	fileMeta, err := hub.UploadFileContext(context.Background(), "projectpurgespill", "spill.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	metaState, _, _ := hub.loadRepoMetadata(context.Background(), "projectpurgespill")
	firstRelease := metaState.Chunks()[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "projectpurgespill", firstRelease, 999)
	hub.invalidateReleaseCache("projectpurgespill")

	patched, err := hub.PatchFileContext(context.Background(), "projectpurgespill", "spill.txt", 4, 4, []byte("ZZZZ"))
	if err != nil {
		t.Fatalf("patch file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	metaState, _, err = hub.loadRepoMetadata(context.Background(), "projectpurgespill")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	spillTag := ""
	for _, id := range patched.Chunks {
		if chunk := metaState.Chunks()[id]; chunk.Release != firstRelease {
			spillTag = chunk.Release
		}
	}
	if spillTag == "" {
		t.Fatal("expected patch to spill into a second release")
	}

	result, err := hub.PruneProject("projectpurgespill", "assets", 0, false)
	if err != nil {
		t.Fatalf("purge untracked: %v", err)
	}
	if result.DeletedReleases != 0 {
		t.Fatalf("purge deleted live releases: %+v", result)
	}
	repo := backend.repo("projectpurgespill")
	if repo == nil || repo.releasesByTag[spillTag] == nil {
		t.Fatalf("spill release %s was deleted by purge", spillTag)
	}
	output := filepath.Join(t.TempDir(), "spill.out")
	if err := hub.DownloadFileContext(context.Background(), "projectpurgespill", "spill.txt", output); err != nil {
		t.Fatalf("download after purge: %v", err)
	}
	assertFileContent(t, output, []byte("abcdZZZZijklmno"))
}

// A fresh client (cold cache) must be able to delete data that exists only
// remotely; previously the delete consulted an empty in-memory view.
func TestDeleteFileWorksOnColdCache(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	seedHub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cold.txt", []byte("cold payload"))
	if _, err := seedHub.UploadFileContext(context.Background(), "projectcolddelete", "cold.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := seedHub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	coldHub := backend.newClient(t, smallTransferTestConfig())
	if err := coldHub.DeleteFileContext(context.Background(), "projectcolddelete", "cold.txt"); err != nil {
		t.Fatalf("cold-cache delete of existing file failed: %v", err)
	}
}

func TestPruneAssetsScopePrunesUnreferencedChunks(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	inputV1 := writeTempFile(t, t.TempDir(), "v1.txt", []byte("version one payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectprune", "file.txt", inputV1); err != nil {
		t.Fatalf("upload v1: %v", err)
	}
	inputV2 := writeTempFile(t, t.TempDir(), "v2.txt", []byte("completely different version two"))
	if _, err := hub.ReplaceFileContext(context.Background(), "projectprune", "file.txt", inputV2); err != nil {
		t.Fatalf("replace with v2: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	before, _, _ := hub.loadRepoMetadataFresh(context.Background(), "projectprune")
	totalBefore := len(before.Chunks())
	if totalBefore < 2 {
		t.Fatalf("expected stale chunk records before purge, got %d", totalBefore)
	}

	if _, err := hub.PruneProject("projectprune", "assets", 0, false); err != nil {
		t.Fatalf("purge untracked: %v", err)
	}

	after, _, _ := hub.loadRepoMetadataFresh(context.Background(), "projectprune")
	referenced := make(map[int64]bool)
	for _, file := range after.Files() {
		for _, id := range file.Chunks {
			referenced[id] = true
		}
	}
	for id := range after.Chunks() {
		if !referenced[id] {
			t.Fatalf("chunk %d survived purge despite no live references", id)
		}
	}
	if len(after.Chunks()) >= totalBefore {
		t.Fatalf("expected chunk catalog to shrink from %d, got %d", totalBefore, len(after.Chunks()))
	}

	output := filepath.Join(t.TempDir(), "out.txt")
	if err := hub.DownloadFileContext(context.Background(), "projectprune", "file.txt", output); err != nil {
		t.Fatalf("download after prune: %v", err)
	}
	assertFileContent(t, output, []byte("completely different version two"))
}

// TestMarkProjectDirtyRevivesEvictedMetadata pins the eviction-race guard:
// an operation that captured pm before eviction can still land its
// acknowledged mutation - the instance is revived with a live commit loop
// instead of silently stranding dirty state on a dead loop.
func TestMarkProjectDirtyRevivesEvictedMetadata(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{
		ChunkSize:         32 << 20,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	})
	ctx := context.Background()
	project := "projectrevive"
	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seed := writeTempFile(t, t.TempDir(), "revive.txt", []byte("survives eviction"))
	if _, err := hub.UploadFileContext(ctx, project, "docs/revive.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}

	hub.metaMu.RLock()
	pm := hub.metaCache[project]
	hub.metaMu.RUnlock()
	if pm == nil {
		t.Fatal("project metadata missing from cache after upload")
	}

	// Let the async commit loop drain so eviction preconditions hold.
	waitFor(t, time.Second, "metadata drain before forced eviction", func() bool {
		pm.mu.Lock()
		defer pm.mu.Unlock()
		return !pm.dirty
	})

	// Simulate eviction racing an in-flight operation.
	hub.metaMu.Lock()
	pm.mu.Lock()
	if pm.dirty {
		pm.mu.Unlock()
		hub.metaMu.Unlock()
		t.Fatal("precondition: metadata should be clean before forced eviction")
	}
	pm.stopped = true
	close(pm.stopCh)
	delete(hub.metaCache, project)
	pm.mu.Unlock()
	hub.metaMu.Unlock()

	// Finalize a mutation against the orphaned pointer, exactly what a long
	// operation does after its network phase.
	pm.mu.Lock()
	pm.meta.EnsureDirectory("late", time.Now().Unix())
	hub.markProjectDirtyLiveLocked(project, pm)
	if pm.stopped {
		t.Fatal("mutation must revive the evicted instance")
	}
	pm.mu.Unlock()

	hub.metaMu.RLock()
	_, cached := hub.metaCache[project]
	hub.metaMu.RUnlock()
	if !cached {
		t.Fatal("revived instance was not re-inserted into the cache")
	}

	// The revived commit loop must eventually publish the late change.
	pollDeadline := time.Now().Add(time.Second)
	for time.Now().Before(pollDeadline) {
		if _, err := hub.StatPathContext(ctx, project, "late"); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("revived metadata never committed the late mutation")
}

// TestColdCacheMutationDoesNotClobberRemote pins the hydration guard: a
// mutation issued from a process that never loaded the project must adopt
// remote state instead of committing an empty tree over it.
func TestColdCacheMutationDoesNotClobberRemote(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := Config{
		ChunkSize:         32 << 20,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	}
	writer := backend.newClient(t, cfg)
	ctx := context.Background()
	project := "projectcoldcache"

	if err := writer.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "keep.txt", []byte("precious"))
	if _, err := writer.UploadFileContext(ctx, project, "docs/keep.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := writer.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// A second, completely cold client mutates the same project.
	cold := backend.newClient(t, cfg)
	if err := cold.MkdirContext(ctx, project, "latecomer"); err != nil {
		t.Fatalf("cold mkdir: %v", err)
	}
	if err := cold.FlushMetadata(ctx); err != nil {
		t.Fatalf("cold flush: %v", err)
	}

	observer := backend.newClient(t, cfg)
	entry, err := observer.StatPathContext(ctx, project, "docs/keep.txt")
	if err != nil {
		t.Fatalf("cold-cache mutation clobbered remote state; docs/keep.txt missing: %v", err)
	}
	if entry.Size != int64(len("precious")) {
		t.Fatalf("kept file corrupted by cold-cache mutation: %+v", entry)
	}
	if _, err := observer.StatPathContext(ctx, project, "latecomer"); err != nil {
		t.Fatalf("cold mutation did not land: %v", err)
	}
}

// --- mock fidelity pins ------------------------------------------------
//
// These tests pin the MOCK to real GitHub behavior: the
// divergences it must mirror (422 create-collision, 404 unknown ref,
// 403 oversized dir, 422 sha-less delete, 404 unknown-repo delete, real
// blob shas, delete commits, newest-first releases, scoped rate fault,
// expiring CDN URLs, JSON-accept asset metadata) must not silently regress.
