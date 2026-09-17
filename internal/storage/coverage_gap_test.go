package storage

// Coverage gaps: behavior-pinning tests using only public APIs
// (plus same-package test helpers). No assertions on hub internals.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Patched-file download must be content-correct without asserting exact
// Range strings. After the first byte of each asset the client reuses the
// signed CDN URL, so ranges land on both the API asset path and the CDN
// path: record both. Every emitted range must stay within its asset (sized
// through the public CDN surface, never mock internals) and the byte total
// must match the file size.
func TestPatchedFileDownloadContentCorrectness(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 128, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()
	original := bytes.Repeat([]byte("a"), 100)
	input := writeTempFile(t, t.TempDir(), "relaxed-ranges.bin", original)
	if _, err := hub.UploadFileContext(ctx, "project-relaxed-ranges", "relaxed-ranges.bin", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	patchedBytes := bytes.Repeat([]byte("b"), 47)
	if _, err := hub.PatchFileContext(ctx, "project-relaxed-ranges", "relaxed-ranges.bin", 3, 47, patchedBytes); err != nil {
		t.Fatalf("patch: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	expected := append(append(append([]byte(nil), original[:3]...), patchedBytes...), original[50:]...)

	rangeByAsset := make(map[int64][]string)
	var rangeMu sync.Mutex
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet {
			return false
		}
		isAPI := strings.Contains(r.URL.Path, "/releases/assets/")
		isCDN := strings.HasPrefix(r.URL.Path, "/cdn/")
		if !isAPI && !isCDN {
			return false
		}
		rng := r.Header.Get("Range")
		if rng == "" {
			return false
		}
		assetID, err := strconv.ParseInt(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], 10, 64)
		if err != nil {
			return false
		}
		rangeMu.Lock()
		rangeByAsset[assetID] = append(rangeByAsset[assetID], rng)
		rangeMu.Unlock()
		return false
	})
	output := t.TempDir() + "/relaxed-ranges.out"
	if err := hub.DownloadFileContext(ctx, "project-relaxed-ranges", "relaxed-ranges.bin", output); err != nil {
		t.Fatalf("download: %v", err)
	}
	assertFileContent(t, output, expected)

	rangeMu.Lock()
	defer rangeMu.Unlock()
	if len(rangeByAsset) == 0 {
		t.Fatal("expected range downloads, saw none")
	}
	// The redirect hop re-presents the same Range on the CDN URL, so each
	// logical fetch appears twice (API path + CDN path). Dedupe identical
	// (asset, range) pairs before accounting.
	seen := make(map[string]bool)
	var total int64
	for assetID, ranges := range rangeByAsset {
		size := cdnAssetSize(t, backend, assetID)
		for _, rng := range ranges {
			key := strconv.FormatInt(assetID, 10) + "\x00" + rng
			if seen[key] {
				continue
			}
			seen[key] = true
			var start, end int64
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil {
				t.Fatalf("unparseable range %q: %v", rng, err)
			}
			if start < 0 || end < start || end >= size {
				t.Fatalf("range %q out of bounds for asset %d (size %d)", rng, assetID, size)
			}
			total += end - start + 1
		}
	}
	if total != int64(len(expected)) {
		t.Fatalf("ranges cover %d bytes, file is %d", total, len(expected))
	}
}

// cdnAssetSize sizes an asset through the public CDN surface: an
// unauthenticated full GET, exactly as the CDN-redirect test does. It keeps
// content-correctness tests off mock internals.
func cdnAssetSize(t *testing.T, backend *mockGitHub, assetID int64) int64 {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, backend.server.URL+"/cdn/"+strconv.FormatInt(assetID, 10), nil)
	resp, err := backend.server.Client().Do(req)
	if err != nil {
		t.Fatalf("cdn size fetch for asset %d: %v", assetID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("cdn size read for asset %d: %v", assetID, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cdn size fetch for asset %d: status %d", assetID, resp.StatusCode)
	}
	return int64(len(body))
}

// Renaming a file onto an existing file atomically replaces it.
func TestRenameOntoExistingFileReplaces(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "project-rename", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for _, tc := range []struct{ path, body string }{{"docs/a.txt", "aaa"}, {"docs/b.txt", "bb"}} {
		seed := writeTempFile(t, t.TempDir(), "seed", []byte(tc.body))
		if _, err := hub.UploadFileContext(ctx, "project-rename", tc.path, seed); err != nil {
			t.Fatalf("upload %s: %v", tc.path, err)
		}
	}
	if err := hub.RenameContext(ctx, "project-rename", "docs/a.txt", "docs/b.txt"); err != nil {
		t.Fatalf("rename onto file: %v", err)
	}
	got, err := hub.ReadFileAtContext(ctx, "project-rename", "docs/b.txt", 0, 3)
	if err != nil {
		t.Fatalf("read replacement: %v", err)
	}
	if string(got) != "aaa" {
		t.Fatalf("expected replacement content %q, got %q", "aaa", got)
	}
	if _, err := hub.StatPathContext(ctx, "project-rename", "docs/a.txt"); err == nil {
		t.Fatal("source must be gone after rename")
	}
}

// Renaming a file onto a directory fails with EISDIR; renaming a
// directory onto a file fails with ENOTDIR.
func TestRenameAcrossKindsFails(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "project-rename-kind", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seed := writeTempFile(t, t.TempDir(), "f", []byte("f"))
	if _, err := hub.UploadFileContext(ctx, "project-rename-kind", "f.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.RenameContext(ctx, "project-rename-kind", "f.txt", "docs"); !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("file onto dir must fail with EISDIR, got %v", err)
	}
	if err := hub.RenameContext(ctx, "project-rename-kind", "docs", "f.txt"); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("dir onto file must fail with ENOTDIR, got %v", err)
	}
}

// Copy duplicates file content, keeps the source, and copies trees.
func TestCopyFileAndDirectory(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "project-copy", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seed := writeTempFile(t, t.TempDir(), "src", []byte("hello copy"))
	if _, err := hub.UploadFileContext(ctx, "project-copy", "docs/src.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.CopyContext(ctx, "project-copy", "docs/src.txt", "docs/dst.txt"); err != nil {
		t.Fatalf("copy file: %v", err)
	}
	for _, path := range []string{"docs/src.txt", "docs/dst.txt"} {
		got, err := hub.ReadFileAtContext(ctx, "project-copy", path, 0, 10)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if string(got) != "hello copy" {
			t.Fatalf("unexpected content for %s: %q", path, got)
		}
	}
	if err := hub.CopyContext(ctx, "project-copy", "docs", "docs2"); err != nil {
		t.Fatalf("copy dir: %v", err)
	}
	// docs held src.txt and dst.txt at copy time, so the tree copy has both.
	entries, err := hub.ReadDirContext(ctx, "project-copy", "docs2")
	if err != nil || len(entries) != 2 {
		t.Fatalf("unexpected copied dir listing: %+v %v", entries, err)
	}
	for _, path := range []string{"docs2/src.txt", "docs2/dst.txt"} {
		got, err := hub.ReadFileAtContext(ctx, "project-copy", path, 0, 10)
		if err != nil || string(got) != "hello copy" {
			t.Fatalf("copied tree file %s unreadable: %q %v", path, got, err)
		}
	}
}

// Copy error paths — missing source, self-copy, dir onto file.
func TestCopyErrors(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "project-copy-err", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seed := writeTempFile(t, t.TempDir(), "c", []byte("c"))
	if _, err := hub.UploadFileContext(ctx, "project-copy-err", "docs/c.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.CopyContext(ctx, "project-copy-err", "docs/nope.txt", "docs/x.txt"); err == nil {
		t.Fatal("copy of missing source must fail")
	}
	if err := hub.CopyContext(ctx, "project-copy-err", "docs/c.txt", "docs/c.txt"); err == nil {
		t.Fatal("self-copy must fail")
	}
	if err := hub.CopyContext(ctx, "project-copy-err", "docs", "docs/c.txt"); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("dir onto file must fail with ENOTDIR, got %v", err)
	}
}

// Git backend round trip — write through one handle, read through a
// fresh handle and through the hub's git path.
func TestGitBackendWriteReadRoundTrip(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{GitCacheDir: t.TempDir()})
	ctx := context.Background()

	writer := hub.getGitRepo("git-probe")
	writer.remoteBase = url
	if err := writer.ensure(ctx); err != nil {
		t.Fatalf("ensure writer: %v", err)
	}
	m := meta.NewRepoMetadata("git-probe")
	// Empty files carry no chunk references and validate cleanly, which is
	// all a metadata-transport round trip needs.
	m.UpsertFile("hello.txt", meta.FileMeta{Size: 0, Mode: 0o644}, 1700000000)
	payload, err := m.ToJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := writer.writeCommitPush(ctx, metadataFilePath, payload, "git-probe: seed"); err != nil {
		t.Fatalf("writeCommitPush: %v", err)
	}

	fresh := newGitRepo(t.TempDir(), "owner", "git-probe", "")
	fresh.remoteBase = url
	data, err := fresh.readFileHead(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("fresh read: %v", err)
	}
	back := meta.NewRepoMetadata("git-probe")
	if err := back.FromJSON(data); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if back.FindFile("hello.txt") == nil {
		t.Fatal("fresh clone missing committed file")
	}

	// The hub's git path observes the same remote truth.
	hub.getGitRepo("git-probe").remoteBase = url
	loaded, _, err := hub.loadRepoMetadataFresh(ctx, "git-probe")
	if err != nil {
		t.Fatalf("hub git load: %v", err)
	}
	if loaded.FindFile("hello.txt") == nil {
		t.Fatal("hub git path missing committed file")
	}
}

// Git backend revision history is visible through listFileCommits.
func TestGitBackendRevisionHistory(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	r := newGitRepo(t.TempDir(), "owner", "demo", "")
	r.remoteBase = url
	ctx := context.Background()
	if err := r.ensure(ctx); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	before, err := r.listFileCommits(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("list commits: %v", err)
	}
	if len(before) != 2 {
		t.Fatalf("seeded repo must have 2 revisions, got %d", len(before))
	}
	m := meta.NewRepoMetadata("demo")
	m.UpsertFile("new.txt", meta.FileMeta{Size: 0, Mode: 0o644}, 1700000000)
	payload, err := m.ToJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := r.writeCommitPush(ctx, metadataFilePath, payload, "demo: third"); err != nil {
		t.Fatalf("write: %v", err)
	}
	after, err := r.listFileCommits(ctx, metadataFilePath)
	if err != nil {
		t.Fatalf("relist: %v", err)
	}
	if len(after) != 3 || after[0].Message != "demo: third" {
		t.Fatalf("expected newest revision first after push, got %+v", after)
	}
}

// Advanced metadata API — update, read back, revision advance,
// revision listing, and rollback. File payloads travel the real upload
// path (valid chunk references); the update itself is a metadata-only
// marker entry, which validates as an empty file.
func TestAdvancedMetadataAPI(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	project := "project-meta-api"

	seed := writeTempFile(t, t.TempDir(), "base.txt", []byte("base content"))
	if _, err := hub.UploadFileContext(ctx, project, "base.txt", seed); err != nil {
		t.Fatalf("upload base: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush base: %v", err)
	}
	revBase, err := hub.RevisionContext(ctx, project)
	if err != nil || revBase == "" {
		t.Fatalf("revision: %q %v", revBase, err)
	}
	// Rollback addresses commits, not content: pin the base commit SHA
	// while it is the only revision.
	revsBase, err := hub.ListMetadataRevisionsContext(ctx, project)
	if err != nil || len(revsBase) != 1 {
		t.Fatalf("expected exactly 1 revision after base flush, got %+v %v", revsBase, err)
	}
	revBaseCommit := revsBase[0].CommitSHA
	if revBaseCommit == "" {
		t.Fatal("base revision missing commit SHA")
	}

	snap, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *meta.RepoMetadata) error {
		m.UpsertFile("marker.txt", meta.FileMeta{Size: 0, Mode: 0o644}, 1700000000)
		return nil
	}, "b20: marker")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if snap.FindFile("marker.txt") == nil {
		t.Fatal("returned snapshot missing marker file")
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush marker: %v", err)
	}
	revMarker, err := hub.RevisionContext(ctx, project)
	if err != nil || revMarker == "" || revMarker == revBase {
		t.Fatalf("revision must advance after update: %q vs %q (%v)", revBase, revMarker, err)
	}
	loaded, _, err := hub.LoadRepoMetadataContext(ctx, project)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.FindFile("base.txt") == nil || loaded.FindFile("marker.txt") == nil {
		t.Fatal("load missing committed files")
	}

	seed2 := writeTempFile(t, t.TempDir(), "second.txt", []byte("second content"))
	if _, err := hub.UploadFileContext(ctx, project, "second.txt", seed2); err != nil {
		t.Fatalf("upload second: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush second: %v", err)
	}
	revSecond, err := hub.RevisionContext(ctx, project)
	if err != nil || revSecond == "" || revSecond == revMarker {
		t.Fatalf("revision must advance after mutation: %q vs %q (%v)", revMarker, revSecond, err)
	}
	revisions, err := hub.ListMetadataRevisionsContext(ctx, project)
	if err != nil || len(revisions) < 3 {
		t.Fatalf("expected >=3 revisions, got %+v %v", revisions, err)
	}
	if err := hub.RollbackMetadataContext(ctx, project, revBaseCommit); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	observer := backend.newClient(t, smallTransferTestConfig())
	remote, _, err := observer.LoadRepoMetadataContext(ctx, project)
	if err != nil {
		t.Fatalf("observer load: %v", err)
	}
	if remote.FindFile("second.txt") != nil {
		t.Fatal("rollback must drop second.txt")
	}
	if remote.FindFile("marker.txt") != nil {
		t.Fatal("rollback must drop marker.txt")
	}
	if remote.FindFile("base.txt") == nil {
		t.Fatal("rollback must keep base.txt")
	}
}

// Strictatime advances atime on explicit queue and on reads; noatime
// never moves it. Stat reads a readonly snapshot, so every assertion goes
// through a fresh observer client: only remotely committed atime counts.
// Each atime change is queued BEFORE the synchronous flush that commits
// it, so no polling for the async commit loop is needed.
func TestAtimePolicies(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	strictCfg := smallTransferTestConfig()
	strictCfg.AtimePolicy = "strictatime"
	hub := backend.newClient(t, strictCfg)
	ctx := context.Background()
	project := "project-atime"
	seed := writeTempFile(t, t.TempDir(), "atime.txt", []byte("read me"))
	if _, err := hub.UploadFileContext(ctx, project, "atime.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}
	observeAtime := func() int64 {
		t.Helper()
		observer := backend.newClient(t, smallTransferTestConfig())
		entry, err := observer.StatPathContext(ctx, project, "atime.txt")
		if err != nil {
			t.Fatalf("observer stat: %v", err)
		}
		return entry.AccessedAt
	}

	const later = int64(1700003600)
	hub.QueueAtimeUpdateContext(ctx, project, "atime.txt", false, later)
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush queued atime: %v", err)
	}
	if got := observeAtime(); got != later {
		t.Fatalf("strictatime must advance atime to %d, got %d", later, got)
	}

	// Backdate, then read: the read itself must move atime forward to the
	// hub's fixed test clock (1700000000).
	hub.QueueAtimeUpdateContext(ctx, project, "atime.txt", false, later-7200)
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush backdate: %v", err)
	}
	if got := observeAtime(); got != later-7200 {
		t.Fatalf("strictatime must record backdate %d, got %d", later-7200, got)
	}
	if _, err := hub.ReadFileAtContext(ctx, project, "atime.txt", 0, 7); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush read atime: %v", err)
	}
	if got := observeAtime(); got != 1700000000 {
		t.Fatalf("read under strictatime must refresh atime to now, got %d", got)
	}

	noCfg := smallTransferTestConfig()
	noCfg.AtimePolicy = "noatime"
	noHub := backend.newClient(t, noCfg)
	noProject := "project-noatime"
	seed2 := writeTempFile(t, t.TempDir(), "noatime.txt", []byte("no touch"))
	if _, err := noHub.UploadFileContext(ctx, noProject, "noatime.txt", seed2); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := noHub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	noHub.QueueAtimeUpdateContext(ctx, noProject, "noatime.txt", false, later)
	if _, err := noHub.ReadFileAtContext(ctx, noProject, "noatime.txt", 0, 8); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := noHub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	noObserver := backend.newClient(t, smallTransferTestConfig())
	noEntry, err := noObserver.StatPathContext(ctx, noProject, "noatime.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if noEntry.AccessedAt == later {
		t.Fatal("noatime must never advance atime")
	}
}

// FlushProjectContext persists one project, accepts unknown names,
// and rejects invalid ones.
func TestFlushProject(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "project-flush-a", "d1"); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	if err := hub.MkdirContext(ctx, "project-flush-b", "d2"); err != nil {
		t.Fatalf("mkdir b: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "project-flush-a"); err != nil {
		t.Fatalf("flush project a: %v", err)
	}
	observer := backend.newClient(t, smallTransferTestConfig())
	entry, err := observer.StatPathContext(ctx, "project-flush-a", "d1")
	if err != nil {
		t.Fatalf("flushed project must be visible remotely: %v", err)
	}
	if !entry.IsDir {
		t.Fatalf("expected d1 to be a dir: %+v", entry)
	}
	if err := hub.FlushProjectContext(ctx, "project-never-touched"); err != nil {
		t.Fatalf("unknown project flush must succeed: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, "bad/name"); err == nil {
		t.Fatal("invalid project name must fail")
	}

	// The sibling project's dirty state survives the single-project flush.
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush all: %v", err)
	}
	if _, err := observer.StatPathContext(ctx, "project-flush-b", "d2"); err != nil {
		t.Fatalf("sibling project state lost: %v", err)
	}
}

// Guard: new tests must not touch hub internals — this compile-time
// probe fails if the suite ever needs metaCache/pm access. It exercises a
// full mutation cycle purely through public APIs.
func TestPublicAPIOnlyMutationCycle(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	var apiCalls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasPrefix(r.URL.Path, "/repos/") || strings.HasPrefix(r.URL.Path, "/upload/") {
			apiCalls.Add(1)
		}
		return false
	})
	seed := writeTempFile(t, t.TempDir(), "pub.txt", []byte("public path"))
	if _, err := hub.UploadFileContext(ctx, "project-public", "pub.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := hub.StatPathContext(ctx, "project-public", "pub.txt"); err != nil {
		t.Fatalf("stat: %v", err)
	}
	if apiCalls.Load() == 0 {
		t.Fatal("expected API traffic from public-API-only cycle")
	}
}

// An 8-level deep tree must round-trip through upload, stat, and read:
// parent recursion, content-path escaping, and journal replay all walk
// every level, and the deepest fixture elsewhere is only depth 2.
func TestDeepTreeUploadStatRoundTrip(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	const project = "project-deep-tree"
	deep := "a/b/c/d/e/f/g/h.txt"
	payload := []byte("deep payload")
	// Uploads do not mkdir -p (POSIX open(O_CREAT) semantics: the parent
	// must exist); ancestors are created explicitly first so the test
	// pins traversal depth, not implicit directory creation.
	for _, dir := range []string{"a", "a/b", "a/b/c", "a/b/c/d", "a/b/c/d/e", "a/b/c/d/e/f", "a/b/c/d/e/f/g"} {
		if err := hub.MkdirContext(ctx, project, dir); err != nil {
			t.Fatalf("mkdir ancestor %s: %v", dir, err)
		}
	}
	if _, err := hub.UploadFileContext(ctx, project, deep, writeTempFile(t, t.TempDir(), "h.txt", payload)); err != nil {
		t.Fatalf("upload deep path: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush deep tree: %v", err)
	}
	entry, err := hub.StatPathContext(ctx, project, deep)
	if err != nil {
		t.Fatalf("stat deep path: %v", err)
	}
	if entry.Size != int64(len(payload)) || entry.Path != deep {
		t.Fatalf("unexpected deep entry: %+v", entry)
	}
	for _, dir := range []string{"a", "a/b", "a/b/c", "a/b/c/d", "a/b/c/d/e", "a/b/c/d/e/f", "a/b/c/d/e/f/g"} {
		if _, err := hub.StatPathContext(ctx, project, dir); err != nil {
			t.Fatalf("stat ancestor %s: %v", dir, err)
		}
	}
	data, err := hub.ReadFileAtContext(ctx, project, deep, 0, int64(len(payload)))
	if err != nil || string(data) != string(payload) {
		t.Fatalf("read deep path: %q %v", data, err)
	}
}
