package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// injectOrphanObjectBytes writes a well-formed content-addressed object that
// no manifest references, simulating garbage from a failed CAS attempt.
func injectOrphanObjectBytes(t *testing.T, hub *StorHub, project string, data []byte) string {
	t.Helper()
	sha := meta.ObjectSHA(data)
	if err := hub.ensureOwner(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := hub.gh.PutFileContent(context.Background(), hub.owner, project, objectRepoPath(sha), data, "", "orphan"); err != nil {
		t.Fatalf("inject orphan: %v", err)
	}
	return sha
}

// injectOrphanObject writes the canonical orphan (a valid tree-node payload
// no manifest names) and returns its sha.
func injectOrphanObject(t *testing.T, hub *StorHub, project string) string {
	t.Helper()
	return injectOrphanObjectBytes(t, hub, project, []byte(`{"m":{"i":999999},"f":{}}`))
}

func TestPruneObjectsRemovesOrphans(t *testing.T) {
	t.Parallel()
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

// Sabotage check: a revision whose manifest cannot be READ must abort the
// prune, never be skipped. The old reachability loop swallowed per-revision
// errors, so one blip orphaned (and deleted) everything only that revision
// referenced — including, if the blip hit HEAD's own read, the live index.
func TestPruneObjectsAbortsWhenRevisionUnreadable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "c1sab", "docs", "a.txt", 1)
	seedMeta(t, hub, "c1sab", "photos", "b.txt", 2)
	orphan := injectOrphanObject(t, hub, "c1sab")
	objsBefore := countObjectFiles(backend, "c1sab")

	// Sabotage: every ref-pinned manifest read (the revision walk) fails.
	// HEAD reads (no ref), commit listings, and object fetches still work.
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet &&
			strings.Contains(r.URL.Path, "/contents/.storhub/index.json") &&
			r.URL.Query().Get("ref") != "" {
			http.Error(w, "injected revision read failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	if _, err := hub.Prune(ctx, "c1sab", PruneObjects, 0, false); err == nil {
		t.Fatal("prune must abort when a revision cannot be read, not prune against a partial union")
	}
	backend.intercept.Store(func(http.ResponseWriter, *http.Request) bool { return false })

	if !mockHas(backend, "c1sab", objectRepoPath(orphan)) {
		t.Fatal("orphan deleted by an aborted prune")
	}
	if countObjectFiles(backend, "c1sab") != objsBefore {
		t.Fatal("referenced objects deleted despite the abort")
	}
	if !mockHas(backend, "c1sab", indexFilePath) {
		t.Fatal("live index destroyed")
	}
	// The tree still loads, and a later prune (fault cleared) succeeds.
	hub2 := backend.newClient(t, smallTransferTestConfig())
	files, err := hub2.ListFilesContext(ctx, "c1sab")
	if err != nil || len(files) != 2 {
		t.Fatalf("tree broken after aborted prune: %v (%d files)", err, len(files))
	}
	res, err := hub.Prune(ctx, "c1sab", PruneObjects, 0, false)
	if err != nil || res.DeletedObjects != 1 {
		t.Fatalf("prune after cleared fault: %v (%+v)", err, res)
	}
}

// GitHub caps a contents-API directory listing at 1000 entries. A shard
// at the cap may be truncated, and an object the cap hides would be deleted
// as an orphan — prune must refuse to classify from a partial enumeration.
func TestPruneObjectsRefusesTruncatedListing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "cap", "docs", "a.txt", 1)
	orphan := injectOrphanObject(t, hub, "cap")

	type entry struct {
		Name string `json:"name"`
		Path string `json:"path"`
		Type string `json:"type"`
		SHA  string `json:"sha"`
	}
	root := "/repos/" + hub.owner + "/cap/contents/.storhub/objects"
	shard := root + "/ff"
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet {
			return false
		}
		switch r.URL.Path {
		case root:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]entry{{Name: "ff", Path: ".storhub/objects/ff", Type: "dir"}})
			return true
		case shard:
			entries := make([]entry, contentsListingCap)
			for i := range entries {
				name := fmt.Sprintf("%064d", i)
				entries[i] = entry{Name: name, Path: ".storhub/objects/ff/" + name, Type: "file", SHA: "blob-x"}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(entries)
			return true
		}
		return false
	})
	if _, err := hub.Prune(ctx, "cap", PruneObjects, 0, false); err == nil {
		t.Fatal("prune must refuse a possibly truncated object enumeration")
	} else if !strings.Contains(err.Error(), "cap") {
		t.Fatalf("error must explain the listing cap, got %v", err)
	}
	backend.intercept.Store(func(http.ResponseWriter, *http.Request) bool { return false })
	if !mockHas(backend, "cap", objectRepoPath(orphan)) {
		t.Fatal("orphan deleted despite the refusal")
	}
}

// When a REST delete loop fails partway, every object whose delete
// SUCCEEDED must already be out of the local cache. A stale cached sha makes
// a later commit skip re-uploading bytes that no longer exist upstream.
func TestPrunePartialDeleteDropsCachedShas(t *testing.T) {
	t.Setenv("STORHUB_CACHE_DIR", t.TempDir())
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "m1", "docs", "a.txt", 1)

	d1 := []byte(`{"m":{"i":111111},"f":{}}`)
	d2 := []byte(`{"m":{"i":222222},"f":{}}`)
	sha1 := injectOrphanObjectBytes(t, hub, "m1", d1)
	sha2 := injectOrphanObjectBytes(t, hub, "m1", d2)
	// Pretend both orphans are cached (as a fetch or an earlier write left them).
	cache := hub.objectCacheFor("m1")
	if !cache.put(sha1, d1) || !cache.put(sha2, d2) {
		t.Fatal("cache seeding failed")
	}

	// Prune deletes orphans in path order; fail the SECOND delete so the
	// first is a proven-successful delete followed by a loop error.
	p1, p2 := objectRepoPath(sha1), objectRepoPath(sha2)
	first, second := sha1, sha2
	failPath := p2
	if p1 > p2 {
		first, second = sha2, sha1
		failPath = p1
	}
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, failPath) {
			http.Error(w, "injected delete failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	if _, err := hub.Prune(ctx, "m1", PruneObjects, 0, false); err == nil {
		t.Fatal("prune must surface the delete failure")
	}
	backend.intercept.Store(func(http.ResponseWriter, *http.Request) bool { return false })

	if mockHas(backend, "m1", objectRepoPath(first)) {
		t.Fatal("first orphan should have been deleted upstream")
	}
	if cache.contains(first) {
		t.Fatal("deleted sha left in the cache after a partial failure")
	}
	if !mockHas(backend, "m1", objectRepoPath(second)) {
		t.Fatal("failed orphan must survive upstream")
	}
	if !cache.contains(second) {
		t.Fatal("undeleted sha must stay cached")
	}

	// A retry completes the reclaim.
	res, err := hub.Prune(ctx, "m1", PruneObjects, 0, false)
	if err != nil {
		t.Fatalf("retry prune: %v", err)
	}
	if res.DeletedObjects != 1 {
		t.Fatalf("retry should delete exactly the survivor, got %d", res.DeletedObjects)
	}
	if mockHas(backend, "m1", objectRepoPath(second)) {
		t.Fatal("survivor not reclaimed on retry")
	}
}

func TestPruneObjectsLegacyIsNoop(t *testing.T) {
	t.Parallel()
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

// An uninitialized project must be reported honestly, not mislabeled as the
// legacy single-blob layout it never had.
func TestPruneObjectsUninitializedIsNoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.EnsureRepoContext(ctx, "freshprune"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	res, err := hub.Prune(ctx, "freshprune", PruneObjects, 0, false)
	if err != nil {
		t.Fatalf("prune fresh: %v", err)
	}
	if len(res.Notes) == 0 || !strings.Contains(res.Notes[0], "no index yet") {
		t.Fatalf("expected an honest uninitialized-project note, got %v", res.Notes)
	}
}

func TestPruneHistoryRESTRefusesHonestly(t *testing.T) {
	t.Parallel()
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

// The checkpoint collapses ALL older manifests into ONE commit, so keep
// is a threshold, not a retention count. keep > 1 promises retention the
// squash cannot deliver and must be rejected — on either backend.
func TestPruneHistoryRejectsKeepAboveOne(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "restkeep", "docs", "a.txt", 1)
	if _, err := hub.Prune(ctx, "restkeep", PruneHistory, 50, false); err == nil ||
		!strings.Contains(err.Error(), "keep=50") {
		t.Fatalf("REST: expected keep>1 rejection, got %v", err)
	}

	url := seedBareMetadataRepo(t)
	gith := newGitBackedHub(t, url, smallTransferTestConfig())
	seedMeta(t, gith, "demo", "docs", "a.txt", 1)
	seedMeta(t, gith, "demo", "photos", "b.txt", 2)
	repo := gith.getGitRepo("demo")
	before, err := repo.listFileCommits(ctx, indexFilePath)
	if err != nil {
		t.Fatalf("list index commits: %v", err)
	}
	if _, err := gith.Prune(ctx, "demo", PruneHistory, 50, false); err == nil ||
		!strings.Contains(err.Error(), "keep=50") {
		t.Fatalf("git: expected keep>1 rejection, got %v", err)
	}
	after, err := repo.listFileCommits(ctx, indexFilePath)
	if err != nil {
		t.Fatalf("list after rejection: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("rejected prune must not touch history: %d -> %d", len(before), len(after))
	}

	// A dry-run reports without claiming compaction.
	dry, err := gith.Prune(ctx, "demo", PruneHistory, 1, true)
	if err != nil {
		t.Fatalf("dry history prune: %v", err)
	}
	if dry.HistoryCompacted {
		t.Fatal("dry-run must not report HistoryCompacted")
	}
	dryAfter, err := repo.listFileCommits(ctx, indexFilePath)
	if err != nil {
		t.Fatalf("list after dry: %v", err)
	}
	if len(dryAfter) != len(before) {
		t.Fatalf("dry-run compacted history: %d -> %d", len(before), len(dryAfter))
	}

	// keep < 1 is the documented coercion to 1; the real run compacts to
	// exactly one checkpoint commit.
	res, err := gith.Prune(ctx, "demo", PruneHistory, 0, false)
	if err != nil {
		t.Fatalf("history prune keep=0: %v", err)
	}
	if !res.HistoryCompacted {
		t.Fatal("expected compaction flag after a successful squash")
	}
	final, err := repo.listFileCommits(ctx, indexFilePath)
	if err != nil {
		t.Fatalf("list after compaction: %v", err)
	}
	if len(final) != 1 {
		t.Fatalf("checkpoint must leave exactly one manifest commit, got %d", len(final))
	}
}

func TestPruneHistoryGitCompactsAndPreservesObjects(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
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
	t.Parallel()
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

// PruneAssets must wire PurgeUntracked exactly: the untracked release with an
// asset is reclaimed, the tracked release AND its referenced asset survive,
// and the reported counts are the truth, not a sum.
func TestPruneAssetsWiresPurge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "pa", "docs", "a.txt", 1)

	// A REAL release v1 (the metadata catalog already tracks the tag) with
	// an asset the live chunk references: purge must spare both.
	backend.addRelease(t, "pa", "v1")
	assetID := backend.addAssetToRelease(t, "pa", "v1", "tracked.bin", []byte("x"))
	if _, err := hub.UpdateRepoMetadataContext(ctx, "pa", func(m *RepoMetadata) error {
		c := m.Chunks[1]
		c.AssetID = assetID
		m.Chunks[1] = c
		return nil
	}, "storhub: repoint chunk asset"); err != nil {
		t.Fatalf("repoint: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "pa"); err != nil {
		t.Fatalf("flush: %v", err)
	}

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
	if res.DeletedReleases != 1 {
		t.Fatalf("expected exactly 1 release reclaimed, got %d (%+v)", res.DeletedReleases, res)
	}
	if res.DeletedAssets != 0 {
		t.Fatalf("expected exactly 0 loose assets reclaimed, got %d (%+v)", res.DeletedAssets, res)
	}
	repo := backend.repo("pa")
	if repo.releasesByTag["v9"] != nil {
		t.Fatal("untracked release survived the purge")
	}
	if repo.releasesByTag["v1"] == nil {
		t.Fatal("seeded tracked release was deleted")
	}
	if repo.assets[assetID] == nil {
		t.Fatal("asset referenced by a live chunk was deleted")
	}
	// The live tree still loads.
	hub2 := backend.newClient(t, smallTransferTestConfig())
	files, err := hub2.ListFilesContext(ctx, "pa")
	if err != nil || len(files) != 1 {
		t.Fatalf("tree broken after asset prune: %v (%d files)", err, len(files))
	}
}

// PruneAll = history (honestly refused on REST) + objects + assets, with
// exact per-stage counts.
func TestPruneAllReclaimsObjectsAndAssets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "all", "docs", "a.txt", 1)
	orphan := injectOrphanObject(t, hub, "all")
	rel, err := hub.createRelease(ctx, "all", "v9", "untracked")
	if err != nil {
		t.Fatalf("create release: %v", err)
	}
	if _, err := hub.uploadAssetStreaming(ctx, "all", "v9", rel.UploadURL, "orphan-asset", strings.NewReader("x"), 1); err != nil {
		t.Fatalf("upload orphan asset: %v", err)
	}

	res, err := hub.Prune(ctx, "all", PruneAll, 1, false)
	if err != nil {
		t.Fatalf("prune all: %v", err)
	}
	if res.DeletedObjects != 1 {
		t.Fatalf("expected 1 orphan reclaimed, got %d", res.DeletedObjects)
	}
	if res.DeletedReleases != 1 {
		t.Fatalf("expected 1 untracked release reclaimed, got %d", res.DeletedReleases)
	}
	if res.DeletedAssets != 0 {
		t.Fatalf("expected 0 loose assets reclaimed, got %d", res.DeletedAssets)
	}
	if res.HistoryCompacted {
		t.Fatal("REST history must not claim compaction")
	}
	honest := false
	for _, n := range res.Notes {
		if strings.Contains(strings.ToLower(n), "github") {
			honest = true
		}
	}
	if !honest {
		t.Fatalf("expected the honest REST-history note, got %v", res.Notes)
	}
	if mockHas(backend, "all", objectRepoPath(orphan)) {
		t.Fatal("orphan survived prune all")
	}
	if backend.repo("all").releasesByTag["v9"] != nil {
		t.Fatal("untracked release survived prune all")
	}
	hub2 := backend.newClient(t, smallTransferTestConfig())
	files, err := hub2.ListFilesContext(ctx, "all")
	if err != nil || len(files) != 1 {
		t.Fatalf("tree broken after prune all: %v (%d files)", err, len(files))
	}
}

// PruneContext is the string-scope adapter for the REST layer: valid scopes
// map to the typed scopes and unknown scopes are rejected, not silently
// treated as no-ops.
func TestPruneContextScopeAdapter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "scope", "docs", "a.txt", 1)

	for _, s := range []string{"objects", "assets", "history", "all"} {
		res, err := hub.PruneContext(ctx, "scope", s, 1, true)
		if err != nil {
			t.Fatalf("scope %q: %v", s, err)
		}
		if res.Scope != PruneScope(s) {
			t.Fatalf("scope %q echoed as %q", s, res.Scope)
		}
	}
	if _, err := hub.PruneContext(ctx, "scope", "garbage", 1, false); err == nil ||
		!strings.Contains(err.Error(), "unknown prune scope") {
		t.Fatalf("expected unknown-scope error, got %v", err)
	}
	// The context-free CLI wrapper reaches the same adapter.
	if _, err := hub.PruneProject("scope", "bogus", 1, false); err == nil ||
		!strings.Contains(err.Error(), "unknown prune scope") {
		t.Fatalf("PruneProject must surface the unknown-scope error, got %v", err)
	}
}

// Every prune scope must refuse a project with uncommitted metadata changes:
// classifying (or committing) against a dirty tree risks deleting what a
// pending commit is about to reference.
func TestPruneRefusesDirtyProject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "dirty", "docs", "a.txt", 1)

	// Deterministic in-flight state: block commits so the background loop
	// cannot drain the flag, then mark the project dirty.
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			http.Error(w, "injected commit failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	pm := hub.getOrCreateProjectMeta("dirty")
	pm.mu.Lock()
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	for _, scope := range []PruneScope{PruneObjects, PruneAssets, PruneHistory, PruneAll} {
		if _, err := hub.Prune(ctx, "dirty", scope, 1, false); err == nil ||
			!strings.Contains(err.Error(), "prune refused") {
			t.Fatalf("scope %s: expected prune refusal for a dirty project, got %v", scope, err)
		}
	}
	backend.intercept.Store(func(http.ResponseWriter, *http.Request) bool { return false })
}
