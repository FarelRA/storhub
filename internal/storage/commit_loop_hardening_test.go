package storage

import (
	"context"
	"net/http"
	"strings"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

// Commit-loop hardening regressions (D4-D9, B3, B7, B8).
//
// Each test below fails on the pre-fix code (RED) and passes after the fix
// (GREEN), except TestHardeningShutdownCommitsDirtyWithoutFlush which locks
// in pre-existing drain behavior that D5's fix must preserve.

// D6: a rollback snapshot that references a chunk ID absent from the chunk
// catalog must be rejected, not silently skipped.
func TestHardeningValidateSnapshotRejectsMissingChunk(t *testing.T) {
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
	meta.Files["ghost.txt"] = FileMeta{
		Size:   1,
		Mode:   0o644,
		Inode:  meta.AllocateInode(),
		Chunks: []int64{424242},
	}
	if err := hub.validateMetadataSnapshot(ctx, "project-snap-missing", meta); err == nil {
		t.Fatal("RED: snapshot referencing missing chunk 424242 was accepted")
	} else if !strings.Contains(err.Error(), "424242") {
		t.Fatalf("unexpected error (want missing-chunk detail): %v", err)
	}
}

// D6 TOCTOU: an asset deleted between validation and commit must fail the
// rollback. The intercept deletes the asset inside the commit PUT itself,
// i.e. after validateMetadataSnapshot already passed.
func TestHardeningRollbackRechecksSnapshotAtCommit(t *testing.T) {
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
	assetID := repoMeta.Chunks[file.Chunks[0]].AssetID
	revisions, err := hub.ListMetadataRevisionsContext(ctx, "project-snap-toctou")
	if err != nil || len(revisions) == 0 {
		t.Fatalf("revisions: %v %d", err, len(revisions))
	}
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			backend.removeAsset(t, "project-snap-toctou", assetID)
		}
		return false
	})
	err = hub.RollbackMetadataContext(ctx, "project-snap-toctou", revisions[0].CommitSHA)
	if err == nil {
		t.Fatal("RED: rollback committed after its asset was deleted mid-commit")
	}
	if !strings.Contains(err.Error(), "missing asset") {
		t.Fatalf("unexpected error (want missing-asset recheck): %v", err)
	}
}

// D7: a failed commit must not mutate shared state. The oversize payload is
// staged by direct (test-only) surgery to bypass D4 admission; LastMod is
// the canary: pre-fix Normalize/LastMod/RecomputeStats run in place before
// the size check. 65000 chunked files serialize to ~8.7MB, safely past the
// 8MB ceiling (60000 only reaches ~8.0MB and never trips it).
func TestHardeningFailedCommitLeavesSharedStateUntouched(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	pm := hub.getOrCreateProjectMeta("project-commit-purity")
	pm.mu.Lock()
	pm.meta.UpsertFile("tiny.txt", FileMeta{Size: 0, Mode: 0o644}, 1700000000)
	pm.meta.LastMod = 12345
	sharedChunk := pm.meta.AllocateChunkID()
	pm.meta.Chunks[sharedChunk] = ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 1}
	for i := 0; i < 65000; i++ {
		name := strings.Repeat("x", 8) + itoa(i)
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
		t.Fatalf("RED: failed commit mutated shared LastMod: got %d, want 12345", lastMod)
	}
	if !dirty {
		t.Fatal("failed commit must retain dirty state for retry")
	}
}

// D8: FlushMetadata must run commitLoop-style conflict recovery: after a
// 409 the local cache must converge on remote HEAD instead of staying stale.
func TestHardeningFlushMetadataRecoversFromConflict(t *testing.T) {
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
	// only FlushMetadata can observe (and recover from) the conflict.
	pmB := hubB.getOrCreateProjectMeta(proj)
	pmB.mu.Lock()
	pmB.meta.UpsertFile("b.txt", FileMeta{Size: 0, Mode: 0o644}, 1700000000)
	markProjectDirtyLocked(pmB)
	pmB.mu.Unlock()
	flushErr := hubB.FlushMetadata(ctx)
	if flushErr == nil {
		t.Fatal("expected conflict error from stale flush")
	}
	_, recoveredSHA, err := hubB.loadRepoMetadata(ctx, proj)
	if err != nil {
		t.Fatalf("hubB reload: %v", err)
	}
	if recoveredSHA != headSHA {
		t.Fatalf("RED: FlushMetadata left stale sha %q, want remote HEAD %q", shortSHA(recoveredSHA), shortSHA(headSHA))
	}
}

// D9: branch names are not revisions. 'main' must be rejected even though
// the contents API would otherwise resolve it to HEAD content.
func TestHardeningRollbackRejectsBranchName(t *testing.T) {
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
		t.Fatal("RED: rollback accepted branch name 'main' as a revision")
	}
	if !strings.Contains(err.Error(), "main") {
		t.Fatalf("error should name the rejected revision: %v", err)
	}
	// Positive control: a genuine revision still rolls back.
	if err := hub.RollbackMetadataContext(ctx, "project-rollback-rev", revisions[len(revisions)-1].CommitSHA); err != nil {
		t.Fatalf("valid revision rollback failed: %v", err)
	}
}

// D5 lock-in: Shutdown drains dirty metadata without an explicit flush.
// Must keep passing after the D5 sweep lands.
func TestHardeningShutdownCommitsDirtyWithoutFlush(t *testing.T) {
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

// D5 gap: a mutation that lands after its commit loop already exited (its
// trigger poke has no listener) must still converge on a later Shutdown.
func TestHardeningShutdownSweepCoversStrandedDirty(t *testing.T) {
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
		t.Fatal("RED: mutation stranded after its commit loop exited was lost by Shutdown")
	}
}

// B3: patch builders must return the release that actually holds the new
// chunks, not the (now full) release they started from.
func TestHardeningPatchBuildersReturnActualRelease(t *testing.T) {
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
	initialTag := repoMeta.Chunks[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-patch-tag", initialTag, 999)
	newChunks, actualTag, err := hub.buildPatchedChunks(ctx, "project-patch-tag", repoMeta, *fileMeta, "p.txt", 0, 1, []byte("Z"))
	if err != nil {
		t.Fatalf("buildPatchedChunks: %v", err)
	}
	if actualTag == initialTag {
		t.Fatalf("RED: builder returned stale initial tag %q after rotation", initialTag)
	}
	// B3 contract: the tag must be where the NEW bytes landed, and the
	// inserted chunk (offset 0, 1 byte) must carry it. Spliced views of
	// the old chunks legitimately keep their original release - retagging
	// them would lie about which asset holds their bytes.
	inserted := 0
	for _, c := range newChunks {
		if c.Offset == 0 && c.Size == 1 {
			if c.Release != actualTag {
				t.Fatalf("RED: inserted chunk landed on %q, builder reported %q", c.Release, actualTag)
			}
			inserted++
		}
	}
	if inserted != 1 {
		t.Fatalf("expected exactly 1 inserted chunk at the edit span, got %d", inserted)
	}
}

// B7: the release cache must deep-copy: mutating a fetched slice must not
// corrupt the cache.
func TestHardeningReleaseCacheDeepCopy(t *testing.T) {
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
		t.Fatalf("RED: release cache aliased caller-mutated assets (got %d, want %d)", again[0].Assets[0].ID, wantID)
	}
}

// B8: -1 placeholder bumps must not skew picker math: once placeholders are
// present the true count has to be resolved from the server.
func TestHardeningPickerResolvesTrueCountWithPlaceholders(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	backend.embedCap = 2
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-placeholder-count"
	first := writeTempFile(t, t.TempDir(), "f1.txt", []byte("one"))
	meta1, err := hub.UploadFile(proj, "f1.txt", first)
	if err != nil {
		t.Fatalf("upload f1: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(ctx, proj)
	fullRelease := repoMeta.Chunks[meta1.Chunks[0]].Release
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
		t.Fatalf("RED: picker trusted placeholder-skewed embedded count and reused server-full %s", fullRelease)
	}
}

// D4 (8MB CEILING — fail fast, never accept-then-never-commit): an
// UpdateRepoMetadataContext mutation whose result exceeds maxMetadataBytes
// must be rejected at admission with a clear error, leaving shared state
// untouched and the project still usable. 70000 plain files serialize to
// ~8.5MB, safely past the ceiling (60000 only reaches ~7.3MB).
func TestHardeningOversizeAdmissionFailsFast(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	_, err := hub.UpdateRepoMetadataContext(ctx, "project-ceiling", func(meta *RepoMetadata) error {
		for i := 0; i < 70000; i++ {
			meta.UpsertFile("bulk-"+itoa(i), FileMeta{Size: 1, Mode: 0o644}, 1700000000)
		}
		return nil
	}, "bulk fill past ceiling")
	if err == nil || !strings.Contains(err.Error(), "metadata too large") {
		t.Fatalf("RED: oversize mutation admitted (want fail-fast 'metadata too large', got %v)", err)
	}
	if !strings.Contains(err.Error(), "PurgeUntracked") {
		t.Fatalf("rejection must point at remediation (PurgeUntracked): %v", err)
	}
	pm := hub.getOrCreateProjectMeta("project-ceiling")
	pm.mu.RLock()
	stillDirty := pm.dirty
	ghost := pm.meta.FindFile("bulk-0")
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
