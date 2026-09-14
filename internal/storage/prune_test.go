package storage

import (
	"context"
	"strings"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// injectOrphanObject writes a well-formed content-addressed object that no
// manifest references, simulating garbage from a failed CAS attempt.
func injectOrphanObject(t *testing.T, hub *StorHub, project string) string {
	t.Helper()
	data := []byte(`{"m":{"i":999999},"f":{}}`)
	sha := meta.ObjectSHA(data)
	if err := hub.ensureOwner(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := hub.gh.PutFileContent(context.Background(), hub.owner, project, objectRepoPath(sha), data, "", "orphan"); err != nil {
		t.Fatalf("inject orphan: %v", err)
	}
	return sha
}

func TestPruneObjectsRemovesOrphans(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "pruneobj", "docs", "a.txt", 1)

	orphan := injectOrphanObject(t, hub, "pruneobj")
	// The orphan must be present before pruning.
	if !mockHas(backend, "pruneobj", objectRepoPath(orphan)) {
		t.Fatal("orphan object not injected")
	}

	// Dry run reports it but deletes nothing.
	dry, err := hub.Prune(ctx, "pruneobj", PruneObjects, 0, true)
	if err != nil {
		t.Fatalf("dry prune: %v", err)
	}
	if dry.DeletedObjects != 1 {
		t.Fatalf("dry run should report 1 orphan, got %d", dry.DeletedObjects)
	}
	if !mockHas(backend, "pruneobj", objectRepoPath(orphan)) {
		t.Fatal("dry run deleted the orphan")
	}

	// Real prune removes the orphan and keeps every referenced object.
	res, err := hub.Prune(ctx, "pruneobj", PruneObjects, 0, false)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if res.DeletedObjects != 1 {
		t.Fatalf("expected 1 object deleted, got %d", res.DeletedObjects)
	}
	if mockHas(backend, "pruneobj", objectRepoPath(orphan)) {
		t.Fatal("orphan survived prune")
	}
	// The live tree still loads (referenced objects untouched).
	hub2 := backend.newClient(t, smallTransferTestConfig())
	files, err := hub2.ListFilesContext(ctx, "pruneobj")
	if err != nil || len(files) != 1 {
		t.Fatalf("tree broken after prune: %v (%d files)", err, len(files))
	}
}

func TestPruneObjectsLegacyIsNoop(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	// A legacy version-4 blob that has NOT been migrated: HEAD is a single
	// blob, so there are no content-addressed objects to prune.
	seedLegacyBlob(t, hub, "v1prune", "docs", "a.txt", 1)
	res, err := hub.Prune(ctx, "v1prune", PruneObjects, 0, false)
	if err != nil {
		t.Fatalf("prune legacy: %v", err)
	}
	if res.DeletedObjects != 0 {
		t.Fatalf("legacy layout has no objects to prune, deleted %d", res.DeletedObjects)
	}
	if len(res.Notes) == 0 || !strings.Contains(res.Notes[0], "legacy") {
		t.Fatalf("expected a legacy-layout note, got %v", res.Notes)
	}
}

func TestPruneHistoryRESTRefusesHonestly(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "resthist", "docs", "a.txt", 1)
	res, err := hub.Prune(ctx, "resthist", PruneHistory, 1, false)
	if err != nil {
		t.Fatalf("prune history rest: %v", err)
	}
	if res.HistoryCompacted {
		t.Fatal("REST history must not claim compaction")
	}
	if len(res.Notes) == 0 || !strings.Contains(strings.ToLower(res.Notes[0]), "github") {
		t.Fatalf("expected an honest REST-history note, got %v", res.Notes)
	}
}

func TestPruneHistoryGitCompactsAndPreservesObjects(t *testing.T) {
	ctx := context.Background()
	url := seedBareMetadataRepo(t)
	hub := newGitBackedHub(t, url, smallTransferTestConfig())
	seedMeta(t, hub, "demo", "docs", "a.txt", 1)
	seedMeta(t, hub, "demo", "photos", "b.txt", 2)

	repo := hub.getGitRepo("demo")
	before, err := repo.listFileCommits(ctx, indexFilePath)
	if err != nil {
		t.Fatalf("list index commits: %v", err)
	}
	if len(before) < 2 {
		t.Fatalf("expected >=2 manifest commits, got %d", len(before))
	}

	res, err := hub.Prune(ctx, "demo", PruneHistory, 1, false)
	if err != nil {
		t.Fatalf("prune history git: %v", err)
	}
	if !res.HistoryCompacted {
		t.Fatal("expected history compaction on git backend")
	}
	after, err := repo.listFileCommits(ctx, indexFilePath)
	if err != nil {
		t.Fatalf("list after: %v", err)
	}
	if len(after) >= len(before) {
		t.Fatalf("history not compacted: %d -> %d", len(before), len(after))
	}
	// Objects survive the squash: a cold reader still sees both files.
	hub2 := newGitBackedHub(t, url, smallTransferTestConfig())
	files, err := hub2.ListFilesContext(ctx, "demo")
	if err != nil {
		t.Fatalf("read after history prune: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("history prune dropped objects: expected 2 files, got %d", len(files))
	}
}

func TestRollbackAsRevertV2(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.MkdirContext(ctx, "rb", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.MkdirContext(ctx, "rb", "tmp"); err != nil {
		t.Fatalf("mkdir tmp: %v", err)
	}

	// Real uploads so rollback's live-asset validation has releases to check.
	in1 := writeTempFile(t, t.TempDir(), "keep.txt", []byte("keep me"))
	if _, err := hub.UploadFileContext(ctx, "rb", "docs/keep.txt", in1); err != nil {
		t.Fatalf("upload 1: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "rb"); err != nil {
		t.Fatalf("flush 1: %v", err)
	}
	revs1, err := hub.ListMetadataRevisionsContext(ctx, "rb")
	if err != nil || len(revs1) == 0 {
		t.Fatalf("revisions: %v (%d)", err, len(revs1))
	}
	firstSHA := revs1[0].CommitSHA

	in2 := writeTempFile(t, t.TempDir(), "gone.txt", []byte("revert me"))
	if _, err := hub.UploadFileContext(ctx, "rb", "tmp/gone.txt", in2); err != nil {
		t.Fatalf("upload 2: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "rb"); err != nil {
		t.Fatalf("flush 2: %v", err)
	}

	if err := hub.RollbackMetadataContext(ctx, "rb", firstSHA); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	// After rollback the newer file is gone, the older survives, and the
	// project is STILL v2 (the manifest was repointed, not the v1 blob).
	hub2 := backend.newClient(t, smallTransferTestConfig())
	m, _, err := hub2.loadRepoMetadataFresh(ctx, "rb")
	if err != nil {
		t.Fatalf("reload after rollback: %v", err)
	}
	names := map[string]bool{}
	for p := range m.Files {
		names[p] = true
	}
	if !names["docs/keep.txt"] {
		t.Fatalf("rollback lost the kept file: %v", names)
	}
	if names["tmp/gone.txt"] {
		t.Fatalf("rollback did not revert the newer file: %v", names)
	}
	if !mockHas(backend, "rb", ".storhub/index.json") {
		t.Fatal("rollback must keep the project on the v2 layout")
	}
}

func TestPruneAssetsWiresPurge(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "pa", "docs", "a.txt", 1)
	// An empty untracked release is skipped by PurgeUntracked (curated
	// headroom), so create one WITH an asset that no chunk references.
	rel, err := hub.createRelease(ctx, "pa", "v9", "untracked")
	if err != nil {
		t.Fatalf("create release: %v", err)
	}
	if _, err := hub.uploadAssetStreaming(ctx, "pa", "v9", rel.UploadURL, "orphan-asset", strings.NewReader("x"), 1); err != nil {
		t.Fatalf("upload orphan asset: %v", err)
	}
	res, err := hub.Prune(ctx, "pa", PruneAssets, 0, false)
	if err != nil {
		t.Fatalf("prune assets: %v", err)
	}
	if res.DeletedReleases+res.DeletedAssets == 0 {
		t.Fatalf("expected the untracked release/asset to be reclaimed, got %+v", res)
	}
}
