package storage

import (
	"context"
	"net/http"
	"strings"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

// Commit-loop hardening regressions.
//
// Each test below fails on the pre-fix code and passes after the fix, except
// TestHardeningShutdownCommitsDirtyWithoutFlush which locks in pre-existing
// drain behavior that the shutdown fix must preserve.

// A rollback snapshot that references a chunk ID absent from the chunk
// catalog must be rejected, not silently skipped.
func TestHardeningValidateSnapshotRejectsMissingChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seed"))
	if _, err := hub.UploadFile("project-snap-missing", "seed.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "project-snap-missing"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	meta := NewRepoMetadata("project-snap-missing")
	meta.Files()["ghost.txt"] = FileMeta{
		Size:   1,
		Mode:   0o644,
		Inode:  meta.AllocateInode(),
		Chunks: []int64{424242},
	}
	if err := hub.validateMetadataSnapshot(ctx, "project-snap-missing", meta); err == nil {
		t.Fatal("a snapshot referencing a missing chunk must be rejected")
	} else if !strings.Contains(err.Error(), "424242") {
		t.Fatalf("unexpected error (want missing-chunk detail): %v", err)
	}
}

// TOCTOU: an asset deleted between validation and commit must fail the
// rollback. The intercept deletes the asset inside the commit PUT itself,
// i.e. after validateMetadataSnapshot already passed.
func TestHardeningRollbackRechecksSnapshotAtCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seed-data"))
	if _, err := hub.UploadFile("project-snap-toctou", "seed.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "project-snap-toctou"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	repoMeta, _, err := hub.loadRepoMetadata(ctx, "project-snap-toctou")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	file := repoMeta.FindFile("seed.txt")
	if file == nil || len(file.Chunks) == 0 {
		t.Fatalf("seed file has no chunks: %+v", file)
	}
	assetID := repoMeta.Chunks()[file.Chunks[0]].AssetID
	revisions, err := hub.ListMetadataRevisionsContext(ctx, "project-snap-toctou")
	if err != nil || len(revisions) == 0 {
		t.Fatalf("revisions: %v %d", err, len(revisions))
	}
	backend.onContentsPUT(t, func(w http.ResponseWriter, r *http.Request) bool {
		backend.removeAsset(t, "project-snap-toctou", assetID)
		return false
	})
	err = hub.RollbackMetadataContext(ctx, "project-snap-toctou", revisions[0].CommitSHA)
	if err == nil {
		t.Fatal("rollback must fail after its asset is deleted mid-commit")
	}
	if !strings.Contains(err.Error(), "missing asset") {
		t.Fatalf("unexpected error (want missing-asset recheck): %v", err)
	}
}

// oversizePaddedTestEntries is sized to the 8MiB ceiling from the measured
// ~732 bytes of JSON per 600-padded entry, plus 10% headroom: 12700 entries
// serialize to ~9.3MB, the minimum fixture that crosses it (entry count,
// not bytes, drives the commit-path work).
const oversizePaddedTestEntries = 12700

// A failed commit must not mutate shared state. The oversize payload is
// staged by direct (test-only) surgery to bypass admission; LastMod is
// the canary: pre-fix Normalize/LastMod/RecomputeStats run in place before
// the size check.
func TestHardeningFailedCommitLeavesSharedStateUntouched(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("oversize fixture is heavy")
	}
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	pm := hub.getOrCreateProjectMeta("project-commit-purity")
	pm.mu.Lock()
	pm.meta.UpsertFile("tiny.txt", FileMeta{Size: 0, Mode: 0o644}, 1700000000)
	pm.meta.LastMod = 12345
	sharedChunk := pm.meta.AllocateChunkID()
	pm.meta.Chunks()[sharedChunk] = ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 1}
	pad := strings.Repeat("x", 600)
	for i := 0; i < oversizePaddedTestEntries; i++ {
		name := strings.Repeat("x", 8) + itoa(i) + pad
		pm.meta.UpsertFile(name, FileMeta{Size: 1, Mode: 0o644, Chunks: []int64{sharedChunk}}, 1700000000)
	}
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()
	err := hub.FlushProjectContext(ctx, "project-commit-purity")
	if err == nil || !strings.Contains(err.Error(), "metadata too large") {
		t.Fatalf("expected oversize commit failure, got: %v", err)
	}
	pm.mu.RLock()
	lastMod := pm.meta.LastMod
	dirty := pm.dirty
	pm.mu.RUnlock()
	if lastMod != 12345 {
		t.Fatalf("failed commit must not mutate shared LastMod: got %d, want 12345", lastMod)
	}
	if !dirty {
		t.Fatal("failed commit must retain dirty state for retry")
	}
}

// FlushMetadata must run commitLoop-style conflict recovery: after a
// 409 the local cache must converge on remote HEAD instead of staying stale.
func TestHardeningFlushMetadataRecoversFromConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hubA := backend.newClient(t, smallTransferTestConfig())
	hubB := backend.newClient(t, smallTransferTestConfig())
	proj := "project-flush-conflict"
	first := writeTempFile(t, t.TempDir(), "f1.txt", []byte("one"))
	if _, err := hubA.UploadFile(proj, "f1.txt", first); err != nil {
		t.Fatalf("upload f1: %v", err)
	}
	if err := hubA.FlushProjectContext(ctx, proj); err != nil {
		t.Fatalf("flush f1: %v", err)
	}
	if _, _, err := hubB.loadRepoMetadata(ctx, proj); err != nil {
		t.Fatalf("hubB hydrate: %v", err)
	}
	second := writeTempFile(t, t.TempDir(), "f2.txt", []byte("two"))
	if _, err := hubA.UploadFile(proj, "f2.txt", second); err != nil {
		t.Fatalf("upload f2: %v", err)
	}
	if err := hubA.FlushProjectContext(ctx, proj); err != nil {
		t.Fatalf("flush f2: %v", err)
	}
	_, headSHA, err := hubA.loadRepoMetadataFresh(ctx, proj)
	if err != nil {
		t.Fatalf("hubA head: %v", err)
	}
	// HubB mutates against a stale SHA without poking its commit loop, so
	// only FlushMetadata can observe the conflict. The contract is that the
	// flush REBASES the pending op onto upstream instead of discarding it,
	// so the flush succeeds and the mutation survives alongside hubA's.
	pmB := hubB.getOrCreateProjectMeta(proj)
	pmB.mu.Lock()
	pmB.meta.UpsertFile("b.txt", FileMeta{Size: 0, Mode: 0o644}, 1700000000)
	entry := pmB.meta.FindFile("b.txt").Clone()
	pmB.opStack.append(Op{Type: OpPutFile, Paths: []string{"b.txt"}, Cause: "test", Timestamp: 1700000000,
		File: &entry})
	markProjectDirtyLocked(pmB)
	pmB.mu.Unlock()
	flushErr := hubB.FlushMetadata(ctx)
	if flushErr != nil {
		t.Fatalf("expected rebase to converge the stale flush, got %v", flushErr)
	}
	meta, recoveredSHA, err := hubB.loadRepoMetadata(ctx, proj)
	if err != nil {
		t.Fatalf("hubB reload: %v", err)
	}
	if recoveredSHA == headSHA {
		t.Fatal("expected the rebased flush to advance the remote HEAD")
	}
	if meta.FindFile("b.txt") == nil {
		t.Fatal("expected the rebased mutation to survive")
	}
	if meta.FindFile("f2.txt") == nil {
		t.Fatal("expected hubA's concurrent mutation to survive the rebase")
	}
}

// Branch names are not revisions. 'main' must be rejected even though
// the contents API would otherwise resolve it to HEAD content.
func TestHardeningRollbackRejectsBranchName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seed"))
	if _, err := hub.UploadFile("project-rollback-rev", "seed.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "project-rollback-rev"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	revisions, err := hub.ListMetadataRevisionsContext(ctx, "project-rollback-rev")
	if err != nil || len(revisions) == 0 {
		t.Fatalf("revisions: %v %d", err, len(revisions))
	}
	err = hub.RollbackMetadataContext(ctx, "project-rollback-rev", "main")
	if err == nil {
		t.Fatal("rollback must reject a branch name as a revision")
	}
	if !strings.Contains(err.Error(), "main") {
		t.Fatalf("error should name the rejected revision: %v", err)
	}
	// Positive control: a genuine revision still rolls back.
	if err := hub.RollbackMetadataContext(ctx, "project-rollback-rev", revisions[len(revisions)-1].CommitSHA); err != nil {
		t.Fatalf("valid revision rollback failed: %v", err)
	}
}

// Shutdown drains dirty metadata without an explicit flush.
// Must keep passing after the shutdown sweep lands.
func TestHardeningShutdownCommitsDirtyWithoutFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("hello"))
	if _, err := hub.UploadFile("project-shutdown-drain", "a.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	// No flush: Shutdown alone must persist the mutation.
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	probe := backend.newClient(t, smallTransferTestConfig())
	fresh, _, err := probe.loadRepoMetadataFresh(ctx, "project-shutdown-drain")
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	if fresh.FindFile("a.txt") == nil {
		t.Fatal("shutdown dropped dirty metadata (a.txt missing remotely)")
	}
}

// A mutation that lands after its commit loop already exited (its
// trigger poke has no listener) must still converge on a later Shutdown.
func TestHardeningShutdownSweepCoversStrandedDirty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("hello"))
	if _, err := hub.UploadFile("project-shutdown-gap", "a.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "project-shutdown-gap"); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// All commit loops are now dead; the trigger below wakes nobody.
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	if _, err := hub.UpdateRepoMetadataContext(ctx, "project-shutdown-gap", func(meta *RepoMetadata) error {
		meta.EnsureDirectory("d", 1700000000)
		meta.UpsertFile("d/stranded.txt", FileMeta{Size: 0, Mode: 0o644}, 1700000000)
		return nil
	}, "strand a mutation with no live loop"); err != nil {
		t.Fatalf("post-shutdown mutation: %v", err)
	}
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	probe := backend.newClient(t, smallTransferTestConfig())
	fresh, _, err := probe.loadRepoMetadataFresh(ctx, "project-shutdown-gap")
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	if fresh.FindFile("d/stranded.txt") == nil {
		t.Fatal("Shutdown must commit a mutation stranded after its commit loop exited")
	}
}

// Patch builders must return the release that actually holds the new
// chunks, not the (now full) release they started from.
func TestHardeningPatchBuildersReturnActualRelease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "p.txt", []byte("12345678"))
	if _, err := hub.UploadFile("project-patch-tag", "p.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "project-patch-tag"); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	repoMeta, _, err := hub.loadRepoMetadata(ctx, "project-patch-tag")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	fileMeta := repoMeta.FindFile("p.txt")
	if fileMeta == nil || len(fileMeta.Chunks) == 0 {
		t.Fatalf("seed file has no chunks: %+v", fileMeta)
	}
	initialTag := repoMeta.Chunks()[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-patch-tag", initialTag, 999)
	newChunks, actualTag, err := hub.buildPatchedChunks(ctx, "project-patch-tag", repoMeta, *fileMeta, "p.txt", 0, 1, []byte("Z"))
	if err != nil {
		t.Fatalf("buildPatchedChunks: %v", err)
	}
	if actualTag == initialTag {
		t.Fatalf("builder must not return the stale initial tag %q after rotation", initialTag)
	}
	// The tag must be where the NEW bytes landed, and the
	// inserted chunk (offset 0, 1 byte) must carry it. Spliced views of
	// the old chunks legitimately keep their original release - retagging
	// them would lie about which asset holds their bytes.
	inserted := 0
	for _, c := range newChunks {
		if c.Offset == 0 && c.Size == 1 {
			if c.Release != actualTag {
				t.Fatalf("inserted chunk landed on %q but builder reported %q", c.Release, actualTag)
			}
			inserted++
		}
	}
	if inserted != 1 {
		t.Fatalf("expected exactly 1 inserted chunk at the edit span, got %d", inserted)
	}
}

// The release cache must deep-copy: mutating a fetched slice must not
// corrupt the cache.
func TestHardeningReleaseCacheDeepCopy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "a.txt", []byte("a"))
	if _, err := hub.UploadFile("project-cache-copy", "a.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	_ = ctx
	first, ok := hub.getCachedReleases("project-cache-copy")
	if !ok || len(first) == 0 || len(first[0].Assets) == 0 {
		t.Fatalf("expected cached release with assets, got %+v %v", first, ok)
	}
	wantID := first[0].Assets[0].ID
	first[0].Assets[0].ID = -999999
	again, ok := hub.getCachedReleases("project-cache-copy")
	if !ok || len(again) == 0 || len(again[0].Assets) == 0 {
		t.Fatalf("expected cached release on refetch, got %+v %v", again, ok)
	}
	if again[0].Assets[0].ID != wantID {
		t.Fatalf("release cache must not alias caller-mutable assets (got %d, want %d)", again[0].Assets[0].ID, wantID)
	}
}

// -1 placeholder bumps must not skew picker math: once placeholders are
// present the true count has to be resolved from the server.
func TestHardeningPickerResolvesTrueCountWithPlaceholders(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	backend.mu.Lock()
	backend.faults.embedCap = 2
	backend.mu.Unlock()
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-placeholder-count"
	first := writeTempFile(t, t.TempDir(), "f1.txt", []byte("one"))
	meta1, err := hub.UploadFile(proj, "f1.txt", first)
	if err != nil {
		t.Fatalf("upload f1: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(ctx, proj)
	fullRelease := repoMeta.Chunks()[meta1.Chunks[0]].Release
	// Server truth is exactly 1000 (full), but the truncated embedded view
	// shows only embedCap entries.
	backend.addAssetsToRelease(t, proj, fullRelease, 999)
	hub.invalidateReleaseCache(proj)
	if _, err := hub.listReleasesCached(ctx, proj); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	// Simulate a legacy -1 upload placeholder bumped onto the truncated
	// cached entry.
	hub.releaseMu.Lock()
	entry := hub.releaseCache[proj]
	for i := range entry.releases {
		if entry.releases[i].TagName == fullRelease {
			entry.releases[i].Assets = append(entry.releases[i].Assets, ghapi.Asset{ID: -1})
		}
	}
	hub.releaseCache[proj] = entry
	hub.releaseMu.Unlock()
	working, _, _ := hub.loadRepoMetadata(ctx, proj)
	working.RemoveFile("f1.txt")
	tag, _, err := hub.getOrCreateUploadRelease(ctx, proj, working, 1)
	if err != nil {
		t.Fatalf("getOrCreate: %v", err)
	}
	if tag == fullRelease {
		t.Fatalf("picker must not trust a placeholder-skewed embedded count or reuse server-full %s", fullRelease)
	}
}

// 8MB ceiling — fail fast, never accept-then-never-commit. An
// UpdateRepoMetadataContext mutation whose result exceeds maxMetadataBytes
// must be rejected at admission with a clear error, leaving shared state
// untouched and the project still usable (sized by oversizePaddedTestEntries).
func TestHardeningOversizeAdmissionFailsFast(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("oversize fixture is heavy")
	}
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	pad := strings.Repeat("y", 600)
	firstName := "bulk-0-" + pad
	_, err := hub.UpdateRepoMetadataContext(ctx, "project-ceiling", func(meta *RepoMetadata) error {
		for i := 0; i < oversizePaddedTestEntries; i++ {
			meta.UpsertFile("bulk-"+itoa(i)+"-"+pad, FileMeta{Size: 1, Mode: 0o644}, 1700000000)
		}
		return nil
	}, "bulk fill past ceiling")
	if err == nil || !strings.Contains(err.Error(), "metadata too large") {
		t.Fatalf("oversize mutation must be rejected at admission (want fail-fast 'metadata too large', got %v)", err)
	}
	if !strings.Contains(err.Error(), "PurgeUntracked") {
		t.Fatalf("rejection must point at remediation (PurgeUntracked): %v", err)
	}
	pm := hub.getOrCreateProjectMeta("project-ceiling")
	pm.mu.RLock()
	stillDirty := pm.dirty
	ghost := pm.meta.FindFile(firstName)
	pm.mu.RUnlock()
	if ghost != nil {
		t.Fatal("rejected bulk leaked into shared state")
	}
	if stillDirty {
		t.Fatal("rejected mutation must not mark the project dirty")
	}
	// The project must remain usable afterwards.
	if _, err := hub.UpdateRepoMetadataContext(ctx, "project-ceiling", func(meta *RepoMetadata) error {
		meta.UpsertFile("small.txt", FileMeta{Size: 0, Mode: 0o644}, 1700000000)
		return nil
	}, "small follow-up"); err != nil {
		t.Fatalf("project unusable after rejection: %v", err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
