package storhub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLiveGitHubSmoke(t *testing.T) {
	if os.Getenv("STORHUB_RUN_LIVE") != "1" {
		t.Skip("set STORHUB_RUN_LIVE=1 to run live GitHub smoke test")
	}

	if strings.TrimSpace(os.Getenv("GITHUB_TOKEN")) == "" {
		t.Fatal("GITHUB_TOKEN is required for live smoke test")
	}
	token := os.Getenv("GITHUB_TOKEN")

	hub := newLiveHub(t, token, liveSmokeConfig())

	repoName := fmt.Sprintf("storhub-live-smoke-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if cleanupErr := hub.DeleteProject(repoName); cleanupErr != nil {
			t.Logf("cleanup warning: %v", cleanupErr)
		}
	})

	payload := []byte("live smoke test payload for storhub")
	payloadV2 := []byte("live smoke test payload for storhub version two")
	inputPath := filepath.Join(t.TempDir(), "live.txt")
	inputPathV2 := filepath.Join(t.TempDir(), "live-v2.txt")
	outputPath := filepath.Join(t.TempDir(), "downloaded.txt")
	rollbackPath := filepath.Join(t.TempDir(), "rolled-back.txt")
	if err := os.WriteFile(inputPath, payload, 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}
	if err := os.WriteFile(inputPathV2, payloadV2, 0o644); err != nil {
		t.Fatalf("write second input file: %v", err)
	}

	fileMeta, err := hub.UploadFile(repoName, "live.txt", inputPath)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if fileMeta.Name != "live.txt" || fileMeta.Size != int64(len(payload)) {
		t.Fatalf("unexpected uploaded metadata: %+v", fileMeta)
	}

	files, err := hub.ListFiles(repoName)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 || files[0].Name != "live.txt" {
		t.Fatalf("unexpected listed files: %+v", files)
	}

	if _, err := hub.ReplaceFile(repoName, "live.txt", inputPathV2); err != nil {
		t.Fatalf("replace file: %v", err)
	}
	for i := 0; i < 10; i++ {
		files, err = hub.ListFiles(repoName)
		if err != nil {
			t.Fatalf("list files after replace: %v", err)
		}
		if len(files) == 1 && files[0].Size == int64(len(payloadV2)) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if len(files) != 1 || files[0].Size != int64(len(payloadV2)) {
		t.Fatalf("replace metadata not visible yet: %+v", files)
	}
	patchedMeta, err := hub.PatchFile(repoName, "live.txt", 5, 5, []byte("PATCH"))
	if err != nil {
		t.Fatalf("patch file: %v", err)
	}
	if patchedMeta.Size != int64(len(payloadV2)) {
		t.Fatalf("unexpected patched size: %+v", patchedMeta)
	}
	var revisions []MetadataRevision
	for i := 0; i < 10; i++ {
		revisions, err = hub.ListMetadataRevisions(repoName)
		if err != nil {
			t.Fatalf("list metadata revisions: %v", err)
		}
		if len(revisions) >= 3 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if len(revisions) < 3 {
		t.Fatalf("expected metadata history, got %+v", revisions)
	}
	var initialRevision string
	for _, revision := range revisions {
		if revision.Message == "storhub: add live.txt" {
			initialRevision = revision.CommitSHA
			break
		}
	}
	if initialRevision == "" {
		t.Fatalf("initial metadata revision not found: %+v", revisions)
	}
	if err := hub.RollbackMetadata(repoName, initialRevision); err != nil {
		t.Fatalf("rollback metadata: %v", err)
	}

	if err := hub.DownloadFile(repoName, "live.txt", outputPath); err != nil {
		t.Fatalf("download file: %v", err)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("downloaded content mismatch")
	}
	if err := hub.DownloadFile(repoName, "live.txt", rollbackPath); err != nil {
		t.Fatalf("download rolled back file: %v", err)
	}
	rolledBackData, err := os.ReadFile(rollbackPath)
	if err != nil {
		t.Fatalf("read rolled back file: %v", err)
	}
	if !bytes.Equal(rolledBackData, payload) {
		t.Fatalf("rolled back content mismatch")
	}
}

func TestLiveGitHubFilesystemOps(t *testing.T) {
	if os.Getenv("STORHUB_RUN_LIVE") != "1" {
		t.Skip("set STORHUB_RUN_LIVE=1 to run live GitHub filesystem test")
	}
	if strings.TrimSpace(os.Getenv("GITHUB_TOKEN")) == "" {
		t.Fatal("GITHUB_TOKEN is required for live filesystem test")
	}

	hub := newLiveHub(t, os.Getenv("GITHUB_TOKEN"), liveSmokeConfig())
	repoName := fmt.Sprintf("storhub-live-fs-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if cleanupErr := hub.DeleteProject(repoName); cleanupErr != nil {
			t.Logf("cleanup warning: %v", cleanupErr)
		}
	})

	if err := hub.Mkdir(repoName, "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.Mkdir(repoName, "docs/specs"); err != nil {
		t.Fatalf("mkdir docs/specs: %v", err)
	}
	created, err := hub.CreateFile(repoName, "docs/specs/notes.txt")
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if created.Size != 0 {
		t.Fatalf("expected empty created file, got %+v", created)
	}
	if _, err := hub.WriteFileAt(repoName, "docs/specs/notes.txt", 0, []byte("hello")); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := hub.WriteFileAt(repoName, "docs/specs/notes.txt", 7, []byte("world")); err != nil {
		t.Fatalf("sparse write file: %v", err)
	}
	if _, err := hub.AppendFile(repoName, "docs/specs/notes.txt", []byte("!")); err != nil {
		t.Fatalf("append file: %v", err)
	}
	partial, err := hub.ReadFileAt(repoName, "docs/specs/notes.txt", 0, 13)
	if err != nil {
		t.Fatalf("read file at: %v", err)
	}
	if !bytes.Equal(partial, []byte{'h', 'e', 'l', 'l', 'o', 0, 0, 'w', 'o', 'r', 'l', 'd', '!'}) {
		t.Fatalf("unexpected partial bytes: %v", partial)
	}
	if _, err := hub.TruncateFile(repoName, "docs/specs/notes.txt", 5); err != nil {
		t.Fatalf("truncate file: %v", err)
	}
	if err := hub.Rename(repoName, "docs", "archive"); err != nil {
		t.Fatalf("rename directory: %v", err)
	}

	if err := waitForLiveCondition(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		info, err := hub.StatPath(repoName, "archive/specs/notes.txt")
		if err != nil {
			return false, nil
		}
		return !info.IsDir && info.Size == 5, nil
	}); err != nil {
		t.Fatalf("wait for renamed file metadata: %v", err)
	}

	entries, err := hub.ReadDir(repoName, "archive")
	if err != nil {
		t.Fatalf("readdir archive: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "specs" || !entries[0].IsDir {
		t.Fatalf("unexpected archive entries: %+v", entries)
	}
	stats, err := hub.StatFS(repoName)
	if err != nil {
		t.Fatalf("statfs: %v", err)
	}
	if stats.Files != 1 || stats.Directories < 2 || stats.Bytes != 5 {
		t.Fatalf("unexpected statfs: %+v", stats)
	}

	outputPath := filepath.Join(t.TempDir(), "live-fs.txt")
	if err := hub.DownloadFile(repoName, "archive/specs/notes.txt", outputPath); err != nil {
		t.Fatalf("download file: %v", err)
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("unexpected downloaded content: %q", data)
	}
	meta, err := hub.StatPath(repoName, "archive/specs/notes.txt")
	if err != nil {
		t.Fatalf("stat file after download: %v", err)
	}
	if meta.Size != 5 {
		t.Fatalf("unexpected file size after truncate: %+v", meta)
	}
	if err := hub.DeleteFile(repoName, "archive/specs/notes.txt"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	if err := waitForLiveCondition(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		files, err := hub.ListFiles(repoName)
		if err != nil {
			return false, err
		}
		return len(files) == 0, nil
	}); err != nil {
		t.Fatalf("wait for delete visibility: %v", err)
	}
}

func TestLiveGitHubSmoke2GB(t *testing.T) {
	if os.Getenv("STORHUB_RUN_LIVE_LARGE") != "1" {
		t.Skip("set STORHUB_RUN_LIVE_LARGE=1 to run 2GB live GitHub smoke test")
	}

	if strings.TrimSpace(os.Getenv("GITHUB_TOKEN")) == "" {
		t.Fatal("GITHUB_TOKEN is required for live smoke test")
	}
	token := os.Getenv("GITHUB_TOKEN")

	hub := newLiveHub(t, token, liveLargeSmokeConfig(newProgressHTTPClient(t)))

	repoName := fmt.Sprintf("storhub-live-2gb-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if cleanupErr := hub.DeleteProject(repoName); cleanupErr != nil {
			t.Logf("cleanup warning: %v", cleanupErr)
		}
	})

	const fileSize = int64(2) << 30
	inputPath := filepath.Join(t.TempDir(), "live-2gb.bin")
	outputPath := filepath.Join(t.TempDir(), "live-2gb.out")
	if err := createSparseZeroFile(inputPath, fileSize); err != nil {
		t.Fatalf("create sparse input: %v", err)
	}

	t.Log("uploading 2GB sparse file")
	fileMeta, err := hub.UploadFile(repoName, "live-2gb.bin", inputPath)
	if err != nil {
		t.Fatalf("upload 2GB file: %v", err)
	}
	if fileMeta.Size != fileSize {
		t.Fatalf("unexpected uploaded size: got %d want %d", fileMeta.Size, fileSize)
	}
	expectedChunks := expectedChunkCount(fileSize, hub.config.ChunkSize)
	if len(fileMeta.Chunks) != expectedChunks {
		t.Fatalf("unexpected chunk count: got %d want %d", len(fileMeta.Chunks), expectedChunks)
	}

	files, err := hub.ListFiles(repoName)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 || files[0].Name != "live-2gb.bin" || files[0].Size != fileSize {
		t.Fatalf("unexpected listed files: %+v", files)
	}

	t.Log("downloading 2GB file; integrity verification runs inside DownloadFile")
	if err := hub.DownloadFile(repoName, "live-2gb.bin", outputPath); err != nil {
		t.Fatalf("download 2GB file: %v", err)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("stat output: %v", err)
	}
	if info.Size() != fileSize {
		t.Fatalf("unexpected downloaded size: got %d want %d", info.Size(), fileSize)
	}

	t.Log("purging untracked 2GB file data")
	if err := hub.DeleteFile(repoName, "live-2gb.bin"); err != nil {
		t.Fatalf("delete metadata entry: %v", err)
	}
	purge, err := hub.PurgeUntracked(repoName)
	if err != nil {
		t.Fatalf("purge untracked 2GB file: %v", err)
	}
	if purge.DeletedReleases != 1 || purge.DeletedAssets != 0 {
		t.Fatalf("unexpected purge result: %+v", purge)
	}
}

func TestSparseZeroFileValidationMatrix(t *testing.T) {
	if os.Getenv("STORHUB_RUN_LARGE") != "1" {
		t.Skip("set STORHUB_RUN_LARGE=1 to run large sparse validation")
	}

	backend := newZeroGitHub(t)
	hub := backend.newClient(t, largeValidationConfig())

	type scenario struct {
		name           string
		size           int64
		downloadVerify bool
	}

	scenarios := []scenario{
		{name: "one-mb.bin", size: 1 << 20, downloadVerify: true},
		{name: "thirty-two-mb.bin", size: 32 << 20, downloadVerify: true},
		{name: "one-twenty-eight-mb.bin", size: 128 << 20, downloadVerify: true},
		{name: "one-gb.bin", size: 1 << 30, downloadVerify: false},
		{name: "two-gb.bin", size: 2 << 30, downloadVerify: false},
	}

	project := "project-large-validation"
	uploaded := make([]FileMetadata, 0, len(scenarios))
	for _, scenario := range scenarios {
		inputPath := filepath.Join(t.TempDir(), scenario.name)
		if err := createSparseZeroFile(inputPath, scenario.size); err != nil {
			t.Fatalf("create sparse file %s: %v", scenario.name, err)
		}

		meta, err := hub.UploadFile(project, scenario.name, inputPath)
		if err != nil {
			t.Fatalf("upload %s: %v", scenario.name, err)
		}
		expectedChunks := expectedChunkCount(scenario.size, hub.config.ChunkSize)
		if meta.Size != scenario.size || len(meta.Chunks) != expectedChunks {
			t.Fatalf("unexpected metadata for %s: %+v", scenario.name, meta)
		}
		if meta.CRC32C == "" {
			t.Fatalf("expected crc32c for %s", scenario.name)
		}
		uploaded = append(uploaded, *meta)

		files, err := hub.ListFiles(project)
		if err != nil {
			t.Fatalf("list files after %s: %v", scenario.name, err)
		}
		if len(files) != len(uploaded) {
			t.Fatalf("expected %d files after %s, got %d", len(uploaded), scenario.name, len(files))
		}

		if scenario.downloadVerify {
			outputPath := filepath.Join(t.TempDir(), scenario.name+".out")
			if err := hub.DownloadFile(project, scenario.name, outputPath); err != nil {
				t.Fatalf("download %s: %v", scenario.name, err)
			}
			info, err := os.Stat(outputPath)
			if err != nil {
				t.Fatalf("stat output %s: %v", scenario.name, err)
			}
			if info.Size() != scenario.size {
				t.Fatalf("unexpected output size for %s: got %d want %d", scenario.name, info.Size(), scenario.size)
			}
			if err := VerifyFileIntegrity(outputPath, *meta, hub.config.BufferSize); err != nil {
				t.Fatalf("verify integrity for %s: %v", scenario.name, err)
			}
		}
	}

	if err := hub.DeleteFile(project, "two-gb.bin"); err != nil {
		t.Fatalf("hide two-gb.bin: %v", err)
	}
	purge, err := hub.PurgeUntracked(project)
	if err != nil {
		t.Fatalf("purge untracked: %v", err)
	}
	if purge.DeletedAssets == 0 {
		t.Fatal("expected purge to delete untracked two-gb assets")
	}
	revision := mustMetadataRevision(t, hub, project, "storhub: add two-gb.bin")
	if err := hub.RollbackMetadata(project, revision.CommitSHA); err == nil {
		t.Fatal("expected rollback to purged two-gb metadata to fail")
	}
}

func newProgressHTTPClient(t *testing.T) *http.Client {
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	return &http.Client{
		Timeout:   defaultRequestTimeout,
		Transport: progressTransport{base: baseTransport, t: t},
	}
}

type progressTransport struct {
	base http.RoundTripper
	t    *testing.T
}

func (p progressTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil && strings.Contains(req.URL.Host, "uploads.github.com") {
		p.t.Logf("starting upload %s", req.URL.String())
		req.Body = newProgressReadCloser(req.Body, 128<<20, func(written int64) {
			p.t.Logf("uploaded %d MB", written>>20)
		})
	}
	resp, err := p.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.Body != nil && strings.Contains(req.URL.Path, "/releases/assets/") {
		p.t.Logf("starting download %s", req.URL.String())
		resp.Body = newProgressReadCloser(resp.Body, 128<<20, func(read int64) {
			p.t.Logf("downloaded %d MB", read>>20)
		})
	}
	return resp, nil
}

type progressReadCloser struct {
	base      io.ReadCloser
	step      int64
	total     int64
	nextLog   int64
	onAdvance func(int64)
}

func newProgressReadCloser(base io.ReadCloser, step int64, onAdvance func(int64)) *progressReadCloser {
	return &progressReadCloser{base: base, step: step, nextLog: step, onAdvance: onAdvance}
}

func (p *progressReadCloser) Read(buf []byte) (int, error) {
	n, err := p.base.Read(buf)
	if n > 0 {
		total := atomic.AddInt64(&p.total, int64(n))
		for total >= p.nextLog {
			if p.onAdvance != nil {
				p.onAdvance(p.nextLog)
			}
			p.nextLog += p.step
		}
	}
	return n, err
}

func (p *progressReadCloser) Close() error {
	return p.base.Close()
}

type zeroGitHub struct {
	t      *testing.T
	server *httptest.Server
	mu     sync.Mutex
	owner  string
	repos  map[string]*zeroRepo
}

type zeroRepo struct {
	name          string
	private       bool
	nextReleaseID int64
	nextAssetID   int64
	nextBlobID    int64
	nextCommitID  int64
	releasesByTag map[string]*zeroRelease
	releasesByID  map[int64]*zeroRelease
	assets        map[int64]*zeroAsset
	files         map[string]*zeroFile
	commitsByPath map[string][]zeroCommit
}

type zeroRelease struct {
	id        int64
	tag       string
	name      string
	uploadURL string
}

type zeroAsset struct {
	id         int64
	name       string
	releaseTag string
	size       int64
}

type zeroFile struct {
	path string
	sha  string
	data []byte
}

type zeroCommit struct {
	sha     string
	message string
	path    string
	data    []byte
	when    time.Time
}

func newZeroGitHub(t *testing.T) *zeroGitHub {
	t.Helper()
	backend := &zeroGitHub{t: t, owner: "storhub-large", repos: make(map[string]*zeroRepo)}
	backend.server = httptest.NewServer(http.HandlerFunc(backend.serveHTTP))
	t.Cleanup(backend.server.Close)
	return backend
}

func (z *zeroGitHub) newClient(t *testing.T, cfg Config) *StorHub {
	t.Helper()
	cfg.APIBaseURL = z.server.URL
	cfg.HTTPClient = z.server.Client()
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	}
	if cfg.Sleep == nil {
		cfg.Sleep = func(_ context.Context, _ time.Duration) error { return nil }
	}
	hub, err := NewStorHubWithContext(context.Background(), "token", cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return hub
}

func (z *zeroGitHub) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/user":
		z.writeJSON(w, http.StatusOK, map[string]any{"login": z.owner})
	case r.Method == http.MethodPost && r.URL.Path == "/user/repos":
		z.handleCreateRepo(w, r)
	case strings.HasPrefix(r.URL.Path, "/repos/"):
		z.handleRepos(w, r)
	case strings.HasPrefix(r.URL.Path, "/upload/"):
		z.handleUpload(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (z *zeroGitHub) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Name    string `json:"name"`
		Private bool   `json:"private"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		z.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	if _, exists := z.repos[payload.Name]; exists {
		z.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "repository already exists"})
		return
	}
	z.repos[payload.Name] = &zeroRepo{name: payload.Name, private: payload.Private, nextReleaseID: 1, nextAssetID: 1, nextBlobID: 1, nextCommitID: 1, releasesByTag: make(map[string]*zeroRelease), releasesByID: make(map[int64]*zeroRelease), assets: make(map[int64]*zeroAsset), files: make(map[string]*zeroFile), commitsByPath: make(map[string][]zeroCommit)}
	z.writeJSON(w, http.StatusCreated, map[string]any{"name": payload.Name})
}

func (z *zeroGitHub) handleRepos(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "repos" || parts[1] != z.owner {
		http.NotFound(w, r)
		return
	}
	repo := z.repo(parts[2])
	if repo == nil {
		z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "repo not found"})
		return
	}
	switch {
	case len(parts) == 3 && r.Method == http.MethodGet:
		z.writeJSON(w, http.StatusOK, map[string]any{"name": repo.name})
	case len(parts) >= 5 && parts[3] == "contents":
		z.handleContents(w, r, repo, strings.Join(parts[4:], "/"))
	case len(parts) == 4 && parts[3] == "commits" && r.Method == http.MethodGet:
		z.handleListCommits(w, r, repo)
	case len(parts) == 4 && parts[3] == "releases" && r.Method == http.MethodPost:
		z.handleCreateRelease(w, r, repo)
	case len(parts) == 4 && parts[3] == "releases" && r.Method == http.MethodGet:
		z.handleListReleases(w, repo)
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "tags" && r.Method == http.MethodGet:
		z.handleGetReleaseByTag(w, repo, parts[5])
	case len(parts) == 5 && parts[3] == "releases" && r.Method == http.MethodDelete:
		z.handleDeleteRelease(w, repo, parts[4])
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "assets" && r.Method == http.MethodGet:
		z.handleDownloadAsset(w, r, repo, parts[5])
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "assets" && r.Method == http.MethodDelete:
		z.handleDeleteAsset(w, repo, parts[5])
	case len(parts) == 3 && r.Method == http.MethodDelete:
		z.handleDeleteRepo(w, parts[2])
	default:
		http.NotFound(w, r)
	}
}

func (z *zeroGitHub) handleContents(w http.ResponseWriter, r *http.Request, repo *zeroRepo, filePath string) {
	cleanPath := strings.TrimPrefix(filePath, "/")
	switch r.Method {
	case http.MethodGet:
		z.handleGetContent(w, r, repo, cleanPath)
	case http.MethodPut:
		z.handlePutContent(w, r, repo, cleanPath)
	default:
		http.NotFound(w, r)
	}
}

func (z *zeroGitHub) handleGetContent(w http.ResponseWriter, r *http.Request, repo *zeroRepo, filePath string) {
	z.mu.Lock()
	defer z.mu.Unlock()
	ref := r.URL.Query().Get("ref")
	if ref != "" {
		for _, commit := range repo.commitsByPath[filePath] {
			if commit.sha == ref {
				z.writeJSON(w, http.StatusOK, map[string]any{"name": filepath.Base(filePath), "path": filePath, "sha": fmt.Sprintf("blob-%s", commit.sha), "encoding": "base64", "type": "file", "content": base64.StdEncoding.EncodeToString(commit.data)})
				return
			}
		}
	}
	file := repo.files[filePath]
	if file == nil {
		z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	z.writeJSON(w, http.StatusOK, map[string]any{"name": filepath.Base(filePath), "path": filePath, "sha": file.sha, "encoding": "base64", "type": "file", "content": base64.StdEncoding.EncodeToString(file.data)})
}

func (z *zeroGitHub) handlePutContent(w http.ResponseWriter, r *http.Request, repo *zeroRepo, filePath string) {
	var payload struct {
		Message string `json:"message"`
		Content string `json:"content"`
		SHA     string `json:"sha"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		z.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	data, err := base64.StdEncoding.DecodeString(payload.Content)
	if err != nil {
		z.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	current := repo.files[filePath]
	if current != nil && payload.SHA != "" && payload.SHA != current.sha {
		z.writeJSON(w, http.StatusConflict, map[string]any{"message": "sha does not match"})
		return
	}
	blobSHA := fmt.Sprintf("blob-%d", repo.nextBlobID)
	repo.nextBlobID++
	repo.files[filePath] = &zeroFile{path: filePath, sha: blobSHA, data: append([]byte(nil), data...)}
	commitSHA := fmt.Sprintf("commit-%d", repo.nextCommitID)
	repo.nextCommitID++
	commit := zeroCommit{sha: commitSHA, message: payload.Message, path: filePath, data: append([]byte(nil), data...), when: time.Unix(1700000000+repo.nextCommitID, 0).UTC()}
	repo.commitsByPath[filePath] = append([]zeroCommit{commit}, repo.commitsByPath[filePath]...)
	z.writeJSON(w, http.StatusOK, map[string]any{"content": map[string]any{"name": filepath.Base(filePath), "path": filePath, "sha": blobSHA}, "commit": map[string]any{"sha": commitSHA}})
}

func (z *zeroGitHub) handleListCommits(w http.ResponseWriter, r *http.Request, repo *zeroRepo) {
	filePath := r.URL.Query().Get("path")
	z.mu.Lock()
	defer z.mu.Unlock()
	commits := repo.commitsByPath[filePath]
	response := make([]map[string]any, 0, len(commits))
	for _, commit := range commits {
		response = append(response, map[string]any{"sha": commit.sha, "commit": map[string]any{"message": commit.message, "author": map[string]any{"date": commit.when.Format(time.RFC3339)}}})
	}
	z.writeJSON(w, http.StatusOK, response)
}

func (z *zeroGitHub) handleCreateRelease(w http.ResponseWriter, r *http.Request, repo *zeroRepo) {
	var payload struct {
		TagName string `json:"tag_name"`
		Name    string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		z.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	release := &zeroRelease{id: repo.nextReleaseID, tag: payload.TagName, name: payload.Name, uploadURL: fmt.Sprintf("%s/upload/%s/%s{?name}", z.server.URL, repo.name, payload.TagName)}
	repo.nextReleaseID++
	repo.releasesByTag[release.tag] = release
	repo.releasesByID[release.id] = release
	z.writeJSON(w, http.StatusCreated, map[string]any{"id": release.id, "tag_name": release.tag, "name": release.name, "upload_url": release.uploadURL, "draft": false, "assets": z.releaseAssetsLocked(repo, release.tag)})
}

func (z *zeroGitHub) handleGetReleaseByTag(w http.ResponseWriter, repo *zeroRepo, tag string) {
	z.mu.Lock()
	defer z.mu.Unlock()
	release := repo.releasesByTag[tag]
	if release == nil {
		z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	z.writeJSON(w, http.StatusOK, map[string]any{"id": release.id, "tag_name": release.tag, "name": release.name, "upload_url": release.uploadURL, "draft": false, "assets": z.releaseAssetsLocked(repo, release.tag)})
}

func (z *zeroGitHub) handleListReleases(w http.ResponseWriter, repo *zeroRepo) {
	z.mu.Lock()
	defer z.mu.Unlock()
	response := make([]map[string]any, 0, len(repo.releasesByTag))
	for _, release := range repo.releasesByTag {
		response = append(response, map[string]any{"id": release.id, "tag_name": release.tag, "name": release.name, "upload_url": release.uploadURL, "draft": false, "assets": z.releaseAssetsLocked(repo, release.tag)})
	}
	z.writeJSON(w, http.StatusOK, response)
}

func (z *zeroGitHub) releaseAssetsLocked(repo *zeroRepo, tag string) []map[string]any {
	assets := make([]map[string]any, 0)
	for _, asset := range repo.assets {
		if asset.releaseTag == tag {
			assets = append(assets, map[string]any{"id": asset.id, "name": asset.name, "size": asset.size})
		}
	}
	return assets
}

func (z *zeroGitHub) handleDeleteRelease(w http.ResponseWriter, repo *zeroRepo, rawID string) {
	releaseID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		z.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	release := repo.releasesByID[releaseID]
	if release == nil {
		z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	delete(repo.releasesByID, releaseID)
	delete(repo.releasesByTag, release.tag)
	for id, asset := range repo.assets {
		if asset.releaseTag == release.tag {
			delete(repo.assets, id)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (z *zeroGitHub) handleUpload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	repo := z.repo(parts[1])
	if repo == nil {
		z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "repo not found"})
		return
	}
	name := r.URL.Query().Get("name")
	size, err := io.Copy(io.Discard, r.Body)
	if err != nil {
		z.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	asset := &zeroAsset{id: repo.nextAssetID, name: name, releaseTag: parts[2], size: size}
	repo.nextAssetID++
	repo.assets[asset.id] = asset
	z.writeJSON(w, http.StatusCreated, map[string]any{"id": asset.id, "name": asset.name})
}

func (z *zeroGitHub) handleDownloadAsset(w http.ResponseWriter, r *http.Request, repo *zeroRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		z.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	z.mu.Lock()
	asset := repo.assets[assetID]
	z.mu.Unlock()
	if asset == nil {
		z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	start, end, partial, err := resolveByteRange(r.Header.Get("Range"), asset.size)
	if err != nil {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	length := asset.size
	status := http.StatusOK
	if partial {
		length = end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, asset.size))
		status = http.StatusPartialContent
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(status)
	_, _ = io.CopyN(w, zeroReader{}, length)
}

func (z *zeroGitHub) handleDeleteAsset(w http.ResponseWriter, repo *zeroRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		z.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	delete(repo.assets, assetID)
	w.WriteHeader(http.StatusNoContent)
}

func (z *zeroGitHub) handleDeleteRepo(w http.ResponseWriter, name string) {
	z.mu.Lock()
	defer z.mu.Unlock()
	delete(z.repos, name)
	w.WriteHeader(http.StatusNoContent)
}

func (z *zeroGitHub) repo(name string) *zeroRepo {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.repos[name]
}

func (z *zeroGitHub) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func createSparseZeroFile(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Truncate(size)
}

func waitForLiveCondition(t *testing.T, timeout, interval time.Duration, check func() (bool, error)) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within %v", timeout)
		}
		time.Sleep(interval)
	}
}
