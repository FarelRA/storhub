package storage

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// Capacity refusal must be typed so callers can tell a full project table
// from a degraded latch or a prune fence without matching message text.
func TestCapacityRefusalIsTyped(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := singleChunkTestConfig()
	cfg.MaxTrackedProjects = 2
	hub := backend.newClient(t, cfg)

	dirty := func(pm *projectMetadata) {
		pm.mu.Lock()
		markProjectDirtyLocked(pm)
		pm.mu.Unlock()
	}
	dirty(hub.getOrCreateProjectMeta("cap-one"))
	dirty(hub.getOrCreateProjectMeta("cap-two"))

	_, err := hub.getOrCreateProjectMetaAdmitted("cap-new")
	if err == nil {
		t.Fatal("admission past a full dirty table must refuse")
	}
	var strained *CapacityStrainedError
	if !errors.As(err, &strained) {
		t.Fatalf("refusal must be *CapacityStrainedError, got %T: %v", err, err)
	}
	if strained.Project != "cap-new" {
		t.Fatalf("refusal must name the refused project, got %q", strained.Project)
	}
}

// The sweep backstop poke must count separately from the live crossing poke
// so operators can tell backstop retries from live pressure.
func TestSweepRetryCountedSeparately(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())

	hub.pressure.noteForceRetry()
	hub.pressure.noteSweepRetry()

	snap := hub.PressureSnapshot()
	if snap.ForceRetryPokes != 1 {
		t.Fatalf("live pokes = %d, want 1", snap.ForceRetryPokes)
	}
	if snap.SweepRetryPokes != 1 {
		t.Fatalf("sweep pokes = %d, want 1", snap.SweepRetryPokes)
	}
}

// The legacy spool migration must move entries without leaving a symlink
// behind: a permanent dual-truth forces the reaper to sweep both dirs.
func TestSpoolMigrationLeavesNoShim(t *testing.T) {
	base := t.TempDir()
	legacy := filepath.Join(base, "storhub", "rest")
	if err := os.MkdirAll(legacy, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "upload-1"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	rest := filepath.Join(base, "rest")
	if err := os.MkdirAll(rest, 0o755); err != nil {
		t.Fatal(err)
	}

	migrateLegacySpoolDir(base, rest)

	if _, err := os.ReadFile(filepath.Join(rest, "upload-1")); err != nil {
		t.Fatalf("entry must move to the new dir: %v", err)
	}
	if info, err := os.Lstat(legacy); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			t.Fatal("migration must not leave a symlink shim at the old path")
		}
		if entries, err := os.ReadDir(legacy); err != nil || len(entries) != 0 {
			t.Fatalf("leftover legacy dir must be empty, err=%v", err)
		}
	}
}

// The file to chunk-id traversal behind every orphan classifier must live
// in one helper so the chunk collector and the asset classifier can never
// disagree on what counts as referenced.
func TestLiveFileChunkIDsUnionsEveryFile(t *testing.T) {
	t.Parallel()
	files := map[string]FileMeta{
		"a.txt": {Chunks: []int64{1, 2}},
		"b.txt": {Chunks: []int64{2, 3}},
		"empty": {},
	}
	got := liveFileChunkIDs(files)
	for _, id := range []int64{1, 2, 3} {
		if _, ok := got[id]; !ok {
			t.Fatalf("chunk %d must be live", id)
		}
	}
	if len(got) != 3 {
		t.Fatalf("live set = %v, want exactly {1 2 3}", got)
	}
}

// One NotFound dispatch must recognize every backend absence shape: the git
// sentinel chain, the OS sentinel, and the API error shape.
func TestNotFoundDispatchCoversAllShapes(t *testing.T) {
	t.Parallel()
	api404 := &ghapi.APIError{StatusCode: http.StatusNotFound}
	if !isMetadataNotFound(api404) {
		t.Fatal("API 404 must read as NotFound")
	}
	if !isMetadataNotFound(os.ErrNotExist) {
		t.Fatal("OS not-exist must read as NotFound")
	}
	if isMetadataNotFound(nil) {
		t.Fatal("nil must not read as NotFound")
	}
	if isMetadataNotFound(errors.New("boom")) {
		t.Fatal("unrelated errors must not read as NotFound")
	}
}

// The typed request form is the single live prune entry: unknown scopes
// must fail loud through it.
func TestPruneRequestRejectsUnknownScope(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())
	if _, err := hub.PruneReq(context.Background(), "scope", PruneRequest{Scope: "bogus"}); err == nil {
		t.Fatal("unknown scope must fail")
	}
}

// Purge must classify against a fresh metadata read, not the cached
// snapshot: a file committed after the local cache was populated must be
// visible, or purge deletes live releases.
func TestPurgeSeesFreshlyCommittedFiles(t *testing.T) {
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

	_, _ = hub.PruneContext(context.Background(), project, "assets", 0, false)
	if backend.repo(project).releasesByTag["v-orphan"] == nil {
		t.Fatal("purge deleted a release committed after the cached snapshot (stale read)")
	}
}

// A release whose asset count cannot be determined must be skipped
// (fail-closed), never deleted: the picker fail-closes the same way.
func TestPurgeSkipsReleaseOnAssetCountError(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectpurgecounterr"

	input := writeTempFile(t, t.TempDir(), "kept.txt", []byte("kept payload"))
	if _, err := hub.UploadFileContext(context.Background(), project, "kept.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
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

	result, err := hub.PruneContext(context.Background(), project, "assets", 0, false)
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

	if _, err := hub.PruneContext(context.Background(), project, "assets", 0, false); err == nil {
		t.Fatal("purge must refuse a project with uncommitted dirty state")
	}
}
