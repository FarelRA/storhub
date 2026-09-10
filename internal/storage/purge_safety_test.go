package storage

import (
	"context"
	"net/http"
	"strings"
	"testing"

	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// Purge must classify against a fresh metadata read, not the cached
// snapshot: a file committed after the local cache was populated must be
// visible, or purge deletes live releases.
func TestPurgeSeesFreshlyCommittedFiles(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-purge-fresh"

	input := writeTempFile(t, t.TempDir(), "kept.txt", []byte("kept payload"))
	if _, err := hub.UploadFile(project, "kept.txt", input); err != nil {
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
	fresh.Chunks[chunkID] = metadata.ChunkInfo{Size: 1, Release: "v-orphan", AssetID: orphanAsset}
	inode := fresh.NextInode
	fresh.NextInode++
	fresh.Files["fresh.txt"] = metadata.FileMeta{Chunks: []int64{chunkID}, Size: 1, Inode: inode}
	backend.setMetadata(t, project, fresh)

	_, _ = hub.PurgeUntracked(project)
	if backend.repo(project).releasesByTag["v-orphan"] == nil {
		t.Fatal("purge deleted a release committed after the cached snapshot (stale read)")
	}
}

// A release whose asset count cannot be determined must be skipped
// (fail-closed), never deleted: the picker fail-closes the same way.
func TestPurgeSkipsReleaseOnAssetCountError(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-purge-count-err"

	input := writeTempFile(t, t.TempDir(), "kept.txt", []byte("kept payload"))
	if _, err := hub.UploadFile(project, "kept.txt", input); err != nil {
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

	result, err := hub.PurgeUntracked(project)
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
func TestPurgeRefusesDirtyProject(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-purge-dirty"

	input := writeTempFile(t, t.TempDir(), "pending.txt", []byte("pending payload"))
	if _, err := hub.UploadFile(project, "pending.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	// Deterministic in-flight state: block metadata commits so the
	// background loop cannot drain the flag, then mark dirty with a
	// version bump (a racing commit of an older snapshot cannot clear it).
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			http.Error(w, "injected commit failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	if _, err := hub.PurgeUntracked(project); err == nil {
		t.Fatal("purge must refuse a project with uncommitted dirty state")
	}
}
