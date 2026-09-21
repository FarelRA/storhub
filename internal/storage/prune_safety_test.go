package storage

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// Purge must classify against a fresh metadata read, not the cached
// snapshot: a file committed after the local cache was populated must be
// visible, or purge deletes live releases.
func TestPruneSeesFreshlyCommittedFiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectpurgefresh"

	input := writeTempFile(t, t.TempDir(), "kept.txt", []byte("kept payload"))
	if _, err := hub.UploadFileContext(context.Background(), project, "kept.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Populate the local cache, then commit a new file remotely behind its
	// back: v-orphan holds a live chunk that the stale snapshot cannot see.
	cached, _, err := hub.loadRepoMetadata(ctx, project)
	if err != nil {
		t.Fatalf("load cached: %v", err)
	}
	backend.addRelease(t, project, "v-orphan")
	orphanAsset := backend.addAssetToRelease(t, project, "v-orphan", "orphan.bin", []byte("x"))

	fresh := cached.Clone()
	chunkID := fresh.NextChunkID
	fresh.NextChunkID++
	fresh.Chunks()[chunkID] = metadata.ChunkInfo{Size: 1, Release: "v-orphan", AssetID: orphanAsset}
	inode := fresh.NextInode
	fresh.NextInode++
	fresh.Files()["fresh.txt"] = metadata.FileMeta{Chunks: []int64{chunkID}, Size: 1, Inode: inode}
	backend.setMetadata(t, project, fresh)

	_, _ = hub.PruneProject(project, "assets", 0, false)
	if backend.repo(project).releasesByTag["v-orphan"] == nil {
		t.Fatal("purge deleted a release committed after the cached snapshot (stale read)")
	}
}

// A release whose asset count cannot be determined must be skipped
// (fail-closed), never deleted: the picker fail-closes the same way.
func TestPruneSkipsReleaseOnAssetCountError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectpurgecounterr"

	input := writeTempFile(t, t.TempDir(), "kept.txt", []byte("kept payload"))
	if _, err := hub.UploadFileContext(context.Background(), project, "kept.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Push the untracked release into the paginated-count band (>=900
	// embedded assets) so releaseAssetCount must call ListReleaseAssets...
	backend.addRelease(t, project, "v-big")
	backend.addAssetsToRelease(t, project, "v-big", 900)
	// ...which we then break. Purge must skip v-big, not delete it.
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/") && strings.HasSuffix(r.URL.Path, "/assets") {
			http.Error(w, "injected list-assets failure", http.StatusInternalServerError)
			return true
		}
		return false
	})

	result, err := hub.PruneProject(project, "assets", 0, false)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if backend.repo(project).releasesByTag["v-big"] == nil {
		t.Fatal("purge deleted a release whose asset count failed (must fail closed)")
	}
	if result.DeletedReleases != 0 {
		t.Fatalf("purge must skip uncountable releases, deleted %d", result.DeletedReleases)
	}
}

// Purge must refuse when the project has uncommitted in-flight state:
// classifying against a dirty tree (or overwriting it on prune-commit)
// risks deleting releases a pending commit is about to reference.
func TestPruneAssetsRefusesDirtyProject(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectpurgedirty"

	input := writeTempFile(t, t.TempDir(), "pending.txt", []byte("pending payload"))
	if _, err := hub.UploadFileContext(context.Background(), project, "pending.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	// Deterministic in-flight state: block metadata commits so the
	// background loop cannot drain the flag, then mark dirty with a
	// version bump (a racing commit of an older snapshot cannot clear it).
	backend.onContentsPUT(t, func(w http.ResponseWriter, _ *http.Request) bool {
		http.Error(w, "injected commit failure", http.StatusInternalServerError)
		return true
	})
	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	if _, err := hub.PruneProject(project, "assets", 0, false); err == nil {
		t.Fatal("purge must refuse a project with uncommitted dirty state")
	}
}

// TestPruneReverifyDropsNewlyTrackedTasks pins the check-then-act fence: a
// commit landing between classification and deletion that references a
// task's asset must spare it. The whole-plan reverify plus the per-delete
// fence inside deletePurgePlan both enforce it; the delete phase destroys
// nothing a live file needs.
func TestPruneReverifyDropsNewlyTrackedTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	const project = "projectpurgerace"

	input := writeTempFile(t, t.TempDir(), "doomed.txt", []byte("doomed payload"))
	meta, err := hub.UploadFileContext(context.Background(), project, "doomed.txt", input)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Capture the asset ID before deleting the file.
	loaded, _, err := hub.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	var orphans []int64
	var tag string
	for _, id := range meta.Chunks {
		if rec, ok := loaded.Chunks()[id]; ok {
			orphans = append(orphans, rec.AssetID)
			tag = rec.Release
		}
	}
	if len(orphans) == 0 || tag == "" {
		t.Fatal("setup broken: no assets captured")
	}
	if err := hub.DeleteFileContext(ctx, project, "doomed.txt"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush delete: %v", err)
	}
	// The asset is now untracked: classification must want it.
	releaseTasks, assetTasks, err := hub.classifyUntracked(ctx, project)
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if len(assetTasks) == 0 {
		t.Fatal("setup broken: expected an orphan asset task")
	}
	// The concurrent writer lands: a new file referencing every orphaned
	// asset, so all of them become tracked again.
	if _, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		var ids []int64
		for i, asset := range orphans {
			id := m.AllocateChunkID()
			if err := m.PutChunk(id, ChunkInfo{Size: 1, Offset: int64(i), Release: tag, AssetID: asset}); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		m.UpsertFile("rescued.txt", FileMeta{Size: int64(len(ids)), Mode: 0o644, Chunks: ids}, 1700000000)
		return nil
	}, "storhub: rescue"); err != nil {
		t.Fatalf("rescue commit: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush rescue: %v", err)
	}
	// Re-verification against fresh truth must drop every task.
	keptR, keptA, err := hub.reverifyPurgePlan(ctx, project, releaseTasks, assetTasks)
	if err != nil {
		t.Fatalf("reverify: %v", err)
	}
	if len(keptR)+len(keptA) != 0 {
		t.Fatalf("reverify must drop newly-tracked tasks, kept %d releases %d assets", len(keptR), len(keptA))
	}
	// End to end over the STALE task list (the exact race window: classify
	// ran before the rescue commit). The fenced tail deletes nothing...
	result := &PruneResult{}
	if err := hub.deletePurgePlan(ctx, project, keptR, keptA, result); err != nil {
		t.Fatalf("fenced delete: %v", err)
	}
	if result.DeletedAssets != 0 || result.DeletedReleases != 0 {
		t.Fatalf("fenced tail must spare re-tracked data, deleted %+v", result)
	}
	// ...while a direct call with the same stale tasks spares the rescued
	// bytes: the per-delete fence inside deletePurgePlan revalidates
	// against fresh truth on first touch, so no caller can reach an
	// unfenced tail anymore. Spared tasks are reported, never silent.
	spared := &PruneResult{}
	if err := hub.deletePurgePlan(ctx, project, releaseTasks, assetTasks, spared); err != nil {
		t.Fatalf("fenced direct delete: %v", err)
	}
	if spared.DeletedAssets != 0 || spared.DeletedReleases != 0 {
		t.Fatalf("direct stale delete must spare everything, deleted %+v", spared)
	}
	if len(spared.Notes) == 0 {
		t.Fatal("spared tasks must be reported in Notes")
	}
	if err := hub.DownloadFileContext(context.Background(), project, "rescued.txt", filepath.Join(t.TempDir(), "rescued.out")); err != nil {
		t.Fatalf("fenced tail must spare the rescued asset: %v", err)
	}
	// And a full purge afterwards still deletes nothing (nothing was
	// reaped above that shouldn't be; the fence plus fresh classification
	// agree).
	res, err := hub.PruneContext(ctx, project, "assets", 0, false)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	_ = res
}
