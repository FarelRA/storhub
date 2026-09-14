package storage

import (
	"context"
	"strings"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// indexV2Config opts into the v2 split index on the REST backend.
func indexV2Config() Config {
	cfg := smallTransferTestConfig()
	cfg.IndexV2 = true
	return cfg
}

// seedMeta adds a directory tree with files + chunk records through a
// metadata transaction (no real asset upload needed to exercise the index).
func seedMeta(t *testing.T, hub *StorHub, project, dir, file string, chunkID int64) {
	t.Helper()
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, project); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	_, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		m.EnsureDirectory(dir, 1700000000)
		m.Chunks[chunkID] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: chunkID}
		m.UpsertFile(dir+"/"+file, FileMeta{Size: 4, Mode: 0o644, UploadedAt: 1700000000, ModifiedAt: 1700000000, Chunks: []int64{chunkID}}, 1700000000)
		m.EnsureRelease("v1", 1700000000)
		return nil
	}, "storhub: seed "+dir)
	if err != nil {
		t.Fatalf("seed meta %s: %v", dir, err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// mockFilePaths lists the stored file paths under a repo (test inspection).
func mockFilePaths(backend *mockGitHub, project string) []string {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	repo := backend.repos[project]
	if repo == nil {
		return nil
	}
	out := make([]string, 0, len(repo.files))
	for p := range repo.files {
		out = append(out, p)
	}
	return out
}

func mockHas(backend *mockGitHub, project, path string) bool {
	for _, p := range mockFilePaths(backend, project) {
		if p == path {
			return true
		}
	}
	return false
}

func countObjectFiles(backend *mockGitHub, project string) int {
	n := 0
	for _, p := range mockFilePaths(backend, project) {
		if strings.HasPrefix(p, ".storhub/objects/") {
			n++
		}
	}
	return n
}

func TestIndexV2EndToEndREST(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, indexV2Config())

	seedMeta(t, hub, "v2proj", "docs", "a.txt", 1)

	if !mockHas(backend, "v2proj", ".storhub/index.json") {
		t.Fatalf("expected v2 manifest at HEAD, files: %v", mockFilePaths(backend, "v2proj"))
	}
	if countObjectFiles(backend, "v2proj") == 0 {
		t.Fatal("expected content-addressed objects written")
	}

	// A fresh client (cold cache) must read the v2 tree back identically.
	hub2 := backend.newClient(t, indexV2Config())
	files, err := hub2.ListFilesContext(ctx, "v2proj")
	if err != nil {
		t.Fatalf("cold read: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file after v2 round-trip, got %d", len(files))
	}
}

func TestIndexV2MigrationFromV1(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)

	// Phase 1: a v1 project (IndexV2 off) commits the single blob.
	hub1 := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub1, "mig", "docs", "old.txt", 1)
	if mockHas(backend, "mig", ".storhub/index.json") {
		t.Fatal("v1 project must not have a manifest")
	}
	if !mockHas(backend, "mig", ".storhub/metadata.json") {
		t.Fatal("v1 project must have metadata.json")
	}

	// Phase 2: reopen with IndexV2 on; the first write migrates.
	hub2 := backend.newClient(t, indexV2Config())
	seedMeta(t, hub2, "mig", "photos", "new.txt", 2)
	if !mockHas(backend, "mig", ".storhub/index.json") {
		t.Fatal("migration must create the manifest")
	}
	// Grace window: the v1 blob is retained so a rollback across the
	// boundary still resolves.
	if !mockHas(backend, "mig", ".storhub/metadata.json") {
		t.Fatal("migration must keep metadata.json for the grace window")
	}

	// Phase 3: a cold v2 reader sees BOTH files (old survived migration).
	hub3 := backend.newClient(t, indexV2Config())
	files, err := hub3.ListFilesContext(ctx, "mig")
	if err != nil {
		t.Fatalf("post-migration read: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files after migration, got %d", len(files))
	}
}

func TestIndexV2V1RevisionReadableAfterMigration(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub1 := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub1, "migrev", "docs", "old.txt", 1)

	// Capture the v1-era revision before migration.
	revs, err := hub1.ListMetadataRevisionsContext(ctx, "migrev")
	if err != nil || len(revs) == 0 {
		t.Fatalf("list v1 revisions: %v (%d)", err, len(revs))
	}
	v1SHA := revs[0].CommitSHA

	hub2 := backend.newClient(t, indexV2Config())
	seedMeta(t, hub2, "migrev", "photos", "new.txt", 2)

	// The pre-migration (v1) revision must still parse.
	m, err := hub2.getMetadataRevision(ctx, "migrev", v1SHA)
	if err != nil {
		t.Fatalf("read v1 revision across migration boundary: %v", err)
	}
	if _, ok := m.Files["docs/old.txt"]; !ok {
		t.Fatalf("v1 revision lost its content: %+v", m.Files)
	}
}

func TestIndexV2DedupUnchangedSubtree(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, indexV2Config())

	seedMeta(t, hub, "dedup", "alpha", "a.txt", 1)
	snap1 := objectCommitCounts(backend, "dedup")
	seedMeta(t, hub, "dedup", "beta", "b.txt", 2)
	snap2 := objectCommitCounts(backend, "dedup")

	if len(snap1) == 0 {
		t.Fatal("no objects after first commit")
	}
	// Adding beta changes the root chain and adds beta's node, but alpha's
	// leaf node object is byte-identical and already cached, so it must NOT
	// be re-uploaded. If every seed-1 object were rewritten there is no
	// write-side dedup.
	grew := 0
	for p, c := range snap1 {
		if snap2[p] > c {
			grew++
		}
	}
	if grew >= len(snap1) {
		t.Fatalf("no write-side dedup: all %d seed-1 objects re-uploaded", len(snap1))
	}
}

func objectCommitCounts(backend *mockGitHub, project string) map[string]int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	out := map[string]int{}
	repo := backend.repos[project]
	if repo == nil {
		return out
	}
	for p, commits := range repo.commitsByPath {
		if strings.HasPrefix(p, ".storhub/objects/") {
			out[p] = len(commits)
		}
	}
	return out
}

func TestHistoryWarnOncePerWindow(t *testing.T) {
	backend := newMockGitHub(t)
	cfg := indexV2Config()
	cfg.HistoryWarnObjects = 1 // any commit crosses it
	logBuf := &syncBuffer{}
	cfg.LogOutput = logBuf
	cfg.LogLevel = "warn"
	hub := backend.newClient(t, cfg)

	seedMeta(t, hub, "warn", "d1", "f1", 1)
	seedMeta(t, hub, "warn", "d2", "f2", 2)

	logged := strings.Count(logBuf.String(), "index object accumulation")
	if logged == 0 {
		t.Fatal("expected the history threshold warning to fire")
	}
	if logged > 1 {
		t.Fatalf("expected the warning once per window, got %d", logged)
	}
}

func TestOversizeErrorMessagePointsAtRemediation(t *testing.T) {
	err := &oversizeError{size: 9 << 20, limit: 8 << 20}
	msg := err.Error()
	if !strings.Contains(msg, "storhub prune") || !strings.Contains(msg, "index_v2") {
		t.Fatalf("oversize error must name both remedies, got: %s", msg)
	}
}

// newGitBackedHub builds a hub whose metadata I/O goes through the go-git
// backend against the local bare repo at url (auth still served by a mock).
func newGitBackedHub(t *testing.T, url string, cfg Config) *StorHub {
	t.Helper()
	backend := newMockGitHub(t)
	cfg.GitCacheDir = t.TempDir()
	cfg.DisableGitBackend = false
	hub := backend.newClient(t, cfg)
	hub.getGitRepo("demo").remoteBase = url
	return hub
}

func TestIndexV2GitPathEndToEnd(t *testing.T) {
	ctx := context.Background()
	url := seedBareMetadataRepo(t)
	hub := newGitBackedHub(t, url, indexV2Config())

	seedMeta(t, hub, "demo", "docs", "g.txt", 1)

	// The git worktree must carry the manifest + objects.
	repo := hub.getGitRepo("demo")
	if repo == nil {
		t.Fatal("expected git repo")
	}
	if _, err := repo.readFileHead(ctx, indexFilePath); err != nil {
		t.Fatalf("manifest not committed on git path: %v", err)
	}
	// Cold read back.
	hub2 := newGitBackedHub(t, url, indexV2Config())
	files, err := hub2.ListFilesContext(ctx, "demo")
	if err != nil {
		t.Fatalf("git v2 cold read: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected files after git v2 round-trip")
	}
}

func TestIndexV2RoundTripPreservesXAttrsAndCounters(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, indexV2Config())
	if err := hub.EnsureRepoContext(ctx, "rt"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	_, err := hub.UpdateRepoMetadataContext(ctx, "rt", func(m *RepoMetadata) error {
		m.EnsureDirectory("x", 1700000000)
		m.Chunks[1] = ChunkInfo{Size: 2, Offset: 0, Release: "v1", AssetID: 7}
		m.UpsertFile("x/f", FileMeta{Size: 2, Mode: 0o600, UID: 42, GID: 43, UploadedAt: 1700000000, ModifiedAt: 1700000000, Chunks: []int64{1}, XAttrs: meta.XAttrMap{"user.k": []byte("v")}}, 1700000000)
		m.EnsureRelease("v1", 1700000000)
		return nil
	}, "storhub: seed")
	if err != nil {
		t.Fatal(err)
	}
	if err := hub.FlushProjectContext(ctx, "rt"); err != nil {
		t.Fatal(err)
	}
	hub2 := backend.newClient(t, indexV2Config())
	m, _, err := hub2.loadRepoMetadataFresh(ctx, "rt")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	f := m.FindFile("x/f")
	if f == nil {
		t.Fatal("file lost")
	}
	if f.Mode != 0o600 || f.UID != 42 || f.GID != 43 || string(f.XAttrs["user.k"]) != "v" {
		t.Fatalf("attrs lost across v2 round-trip: %+v", f)
	}
	if m.NextInode < 2 || m.NextChunkID < 2 {
		t.Fatalf("counters not preserved: ni=%d nc=%d", m.NextInode, m.NextChunkID)
	}
}
