package storage

import (
	"context"
	"strings"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// seedMeta adds a directory tree with files + chunk records through a
// metadata transaction (no real asset upload needed to exercise the index).
// The write lands on the default split layout (version 5).
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

// seedLegacyBlob writes a version-4 single-blob metadata.json DIRECTLY to the
// backend, simulating a project created before the split layout existed, so a
// later hub write must migrate it to version 5.
func seedLegacyBlob(t *testing.T, hub *StorHub, project, dir, file string, chunkID int64) {
	t.Helper()
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, project); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	m := NewRepoMetadata(project)
	m.EnsureDirectory(dir, 1700000000)
	m.Chunks[chunkID] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: chunkID}
	m.UpsertFile(dir+"/"+file, FileMeta{Size: 4, Mode: 0o644, UploadedAt: 1700000000, ModifiedAt: 1700000000, Chunks: []int64{chunkID}}, 1700000000)
	m.EnsureRelease("v1", 1700000000)
	m.Normalize(project, 1700000000)
	m.Version = 4 // pin the legacy single-blob schema (the split default is 5)
	blob, err := m.ToJSON()
	if err != nil {
		t.Fatalf("marshal legacy blob: %v", err)
	}
	if !strings.Contains(string(blob), `"v":4`) {
		t.Fatalf("legacy fixture must be version 4, got %s", blob[:20])
	}
	if _, _, err := hub.gh.PutFileContent(ctx, hub.owner, project, metadataFilePath, blob, "", "legacy seed"); err != nil {
		t.Fatalf("seed legacy blob: %v", err)
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

func TestSplitIndexEndToEndREST(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	seedMeta(t, hub, "v5proj", "docs", "a.txt", 1)

	if !mockHas(backend, "v5proj", ".storhub/index.json") {
		t.Fatalf("expected v5 manifest at HEAD, files: %v", mockFilePaths(backend, "v5proj"))
	}
	if countObjectFiles(backend, "v5proj") == 0 {
		t.Fatal("expected content-addressed objects written")
	}
	// The default write path never touches the legacy blob.
	if mockHas(backend, "v5proj", ".storhub/metadata.json") {
		t.Fatal("a new project must not write metadata.json")
	}

	// A fresh client (cold cache) must read the v5 tree back identically.
	hub2 := backend.newClient(t, smallTransferTestConfig())
	files, err := hub2.ListFilesContext(ctx, "v5proj")
	if err != nil {
		t.Fatalf("cold read: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file after v5 round-trip, got %d", len(files))
	}
}

func TestSplitIndexMigrationFromLegacy(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)

	// Phase 1: a legacy version-4 project (single blob, no manifest).
	hub1 := backend.newClient(t, smallTransferTestConfig())
	seedLegacyBlob(t, hub1, "mig", "docs", "old.txt", 1)
	if mockHas(backend, "mig", ".storhub/index.json") {
		t.Fatal("legacy project must not have a manifest")
	}
	if !mockHas(backend, "mig", ".storhub/metadata.json") {
		t.Fatal("legacy project must have metadata.json")
	}

	// Phase 2: the next write migrates it to the split layout.
	hub2 := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub2, "mig", "photos", "new.txt", 2)
	if !mockHas(backend, "mig", ".storhub/index.json") {
		t.Fatal("migration must create the manifest")
	}
	// Grace window: the legacy blob is retained so a rollback across the
	// boundary still resolves.
	if !mockHas(backend, "mig", ".storhub/metadata.json") {
		t.Fatal("migration must keep metadata.json for the grace window")
	}

	// Phase 3: a cold reader sees BOTH files (old survived migration).
	hub3 := backend.newClient(t, smallTransferTestConfig())
	files, err := hub3.ListFilesContext(ctx, "mig")
	if err != nil {
		t.Fatalf("post-migration read: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files after migration, got %d", len(files))
	}
}

func TestSplitIndexLegacyRevisionReadableAfterMigration(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub1 := backend.newClient(t, smallTransferTestConfig())
	seedLegacyBlob(t, hub1, "migrev", "docs", "old.txt", 1)

	revs, err := hub1.ListMetadataRevisionsContext(ctx, "migrev")
	if err != nil || len(revs) == 0 {
		t.Fatalf("list legacy revisions: %v (%d)", err, len(revs))
	}
	legacySHA := revs[0].CommitSHA

	hub2 := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub2, "migrev", "photos", "new.txt", 2)

	// The pre-migration (version-4) revision must still parse.
	m, err := hub2.getMetadataRevision(ctx, "migrev", legacySHA)
	if err != nil {
		t.Fatalf("read legacy revision across migration boundary: %v", err)
	}
	if _, ok := m.Files["docs/old.txt"]; !ok {
		t.Fatalf("legacy revision lost its content: %+v", m.Files)
	}
}

func TestSplitIndexDedupUnchangedSubtree(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

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

func TestHistoryWarnOncePerWindow(t *testing.T) {
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
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
	if !strings.Contains(msg, "storhub prune") || !strings.Contains(msg, "subdirectories") {
		t.Fatalf("oversize error must name both remedies, got: %s", msg)
	}
}

func TestSplitIndexGitPathEndToEnd(t *testing.T) {
	ctx := context.Background()
	url := seedBareMetadataRepo(t)
	hub := newGitBackedHub(t, url, smallTransferTestConfig())

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
	hub2 := newGitBackedHub(t, url, smallTransferTestConfig())
	files, err := hub2.ListFilesContext(ctx, "demo")
	if err != nil {
		t.Fatalf("git v5 cold read: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected files after git v5 round-trip")
	}
}

func TestSplitIndexRoundTripPreservesXAttrsAndCounters(t *testing.T) {
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
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
	hub2 := backend.newClient(t, smallTransferTestConfig())
	m, _, err := hub2.loadRepoMetadataFresh(ctx, "rt")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	f := m.FindFile("x/f")
	if f == nil {
		t.Fatal("file lost")
	}
	if f.Mode != 0o600 || f.UID != 42 || f.GID != 43 || string(f.XAttrs["user.k"]) != "v" {
		t.Fatalf("attrs lost across v5 round-trip: %+v", f)
	}
	if m.NextInode < 2 || m.NextChunkID < 2 {
		t.Fatalf("counters not preserved: ni=%d nc=%d", m.NextInode, m.NextChunkID)
	}
}
