package storage

// Large sparse-file validation matrix and its dedicated zero-filled mock.
import (
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

	chunking "github.com/FarelRA/storhub/internal/chunking"
	"github.com/FarelRA/storhub/internal/test"
)

func expectedChunkCount(size, chunkSize int64) int {
	if chunkSize <= 0 {
		chunkSize = chunking.DefaultChunkSize
	}
	if chunkSize > chunking.MaxReleaseAssetSize {
		chunkSize = chunking.MaxReleaseAssetSize
	}
	chunks := int((size + chunkSize - 1) / chunkSize)
	if chunks == 0 {
		return 1
	}
	return chunks
}

func TestSparseZeroFileValidationMatrix(t *testing.T) {
	test.RequireFlag(t, "STORHUB_RUN_LARGE")

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

	project := "projectlargevalidation"
	uploaded := make([]FileMeta, 0, len(scenarios))
	for _, scenario := range scenarios {
		inputPath := filepath.Join(t.TempDir(), scenario.name)
		if err := createSparseZeroFile(inputPath, scenario.size); err != nil {
			t.Fatalf("create sparse file %s: %v", scenario.name, err)
		}

		meta, err := hub.UploadFileContext(context.Background(), project, scenario.name, inputPath)
		if err != nil {
			t.Fatalf("upload %s: %v", scenario.name, err)
		}
		expectedChunks := expectedChunkCount(scenario.size, hub.config.ChunkSize)
		if meta.Size != scenario.size || len(meta.Chunks) != expectedChunks {
			t.Fatalf("unexpected metadata for %s: %+v", scenario.name, meta)
		}
		uploaded = append(uploaded, *meta)

		files, err := hub.ListFilesContext(context.Background(), project)
		if err != nil {
			t.Fatalf("list files after %s: %v", scenario.name, err)
		}
		if len(files) != len(uploaded) {
			t.Fatalf("expected %d files after %s, got %d", len(uploaded), scenario.name, len(files))
		}

		if scenario.downloadVerify {
			outputPath := filepath.Join(t.TempDir(), scenario.name+".out")
			if err := hub.DownloadFileContext(context.Background(), project, scenario.name, outputPath); err != nil {
				t.Fatalf("download %s: %v", scenario.name, err)
			}
			info, err := os.Stat(outputPath)
			if err != nil {
				t.Fatalf("stat output %s: %v", scenario.name, err)
			}
			if info.Size() != scenario.size {
				t.Fatalf("unexpected output size for %s: got %d want %d", scenario.name, info.Size(), scenario.size)
			}
			if info.Size() != scenario.size {
				t.Fatalf("unexpected output size for %s after download: got %d want %d", scenario.name, info.Size(), scenario.size)
			}
		}
	}

	if err := hub.DeleteFileContext(context.Background(), project, "twogb.bin"); err != nil {
		t.Fatalf("hide two-gb.bin: %v", err)
	}
	purge, err := hub.PruneContext(context.Background(), project, "assets", 0, false)
	if err != nil {
		t.Fatalf("purge untracked: %v", err)
	}
	if purge.DeletedAssets == 0 {
		t.Fatal("expected purge to delete untracked two-gb assets")
	}
	revision := mustMetadataRevision(t, hub, project, "storhub: add two-gb.bin")
	if err := hub.RollbackMetadataContext(context.Background(), project, revision.CommitSHA); err == nil {
		t.Fatal("expected rollback to purged two-gb metadata to fail")
	}
}

type zeroGitHub struct {
	t      *testing.T
	server *httptest.Server
	mu     sync.Mutex
	owner  string
	repos  map[string]*zeroRepo
	// nextAssetID mints GLOBALLY unique asset IDs like mockGitHub and
	// real GitHub do (per-repo counters make repo-agnostic CDN lookups
	// ambiguous the moment such a route exists).
	nextAssetID atomic.Int64
}

type zeroRepo struct {
	name          string
	private       bool
	nextReleaseID int64
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
	z.repos[payload.Name] = &zeroRepo{name: payload.Name, private: payload.Private, nextReleaseID: 1, nextBlobID: 1, nextCommitID: 1, releasesByTag: make(map[string]*zeroRelease), releasesByID: make(map[int64]*zeroRelease), assets: make(map[int64]*zeroAsset), files: make(map[string]*zeroFile), commitsByPath: make(map[string][]zeroCommit)}
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
	if ref != "" && ref != "HEAD" && ref != defaultBranch {
		// Mirror GitHub: an unknown/stale ref 404s; it never silently
		// serves HEAD. A known ref resolves to the bytes the
		// path carried at that commit.
		data, known, present := z.contentAtRefLocked(repo, filePath, ref)
		switch {
		case !known:
			z.writeJSON(w, http.StatusNotFound, map[string]any{"message": fmt.Sprintf("No commit found for SHA: %s", ref)})
			return
		case present:
			z.writeJSON(w, http.StatusOK, map[string]any{"name": filepath.Base(filePath), "path": filePath, "sha": computeGitBlobSHA(data), "encoding": "base64", "type": "file", "content": base64.StdEncoding.EncodeToString(data)})
			return
		default:
			z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
			return
		}
	}
	file := repo.files[filePath]
	if file == nil {
		z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	z.writeJSON(w, http.StatusOK, map[string]any{"name": filepath.Base(filePath), "path": filePath, "sha": file.sha, "encoding": "base64", "type": "file", "content": base64.StdEncoding.EncodeToString(file.data)})
}

// contentAtRefLocked mirrors mockGitHub.contentAtRefLocked for the
// zero-payload mock: (known=false) unknown ref, (present=false) the path
// did not exist at that commit.
func (z *zeroGitHub) contentAtRefLocked(repo *zeroRepo, filePath, ref string) (data []byte, known, present bool) {
	var refWhen time.Time
	for _, commits := range repo.commitsByPath {
		for _, c := range commits {
			if c.sha == ref {
				refWhen, known = c.when, true
				break
			}
		}
		if known {
			break
		}
	}
	if !known {
		return nil, false, false
	}
	commits := repo.commitsByPath[filePath]
	if len(commits) == 0 {
		if f := repo.files[filePath]; f != nil {
			return f.data, true, true
		}
		return nil, true, false
	}
	for _, c := range commits {
		if !c.when.After(refWhen) {
			return c.data, true, true
		}
	}
	return nil, true, false
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
	if payload.Message == "" {
		z.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"message\" wasn't supplied."})
		return
	}
	data, err := base64.StdEncoding.DecodeString(payload.Content)
	if err != nil {
		z.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"content\" is not valid base64-encoded data."})
		return
	}
	z.mu.Lock()
	defer z.mu.Unlock()
	current := repo.files[filePath]
	// Mirror GitHub: a sha-less PUT onto an existing path is a
	// create collision - 422, never a silent overwrite.
	if current != nil && payload.SHA == "" {
		z.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"sha\" wasn't supplied."})
		return
	}
	if current != nil && payload.SHA != "" && payload.SHA != current.sha {
		z.writeJSON(w, http.StatusConflict, map[string]any{"message": "sha does not match"})
		return
	}
	if current == nil && payload.SHA != "" {
		z.writeJSON(w, http.StatusConflict, map[string]any{"message": "file does not exist"})
		return
	}
	// Real git blob shas, like mockGitHub and the live API store:
	// the client computes CAS tokens from raw bytes, so counter shas
	// would 409 preconditions that pass upstream.
	blobSHA := computeGitBlobSHA(data)
	repo.files[filePath] = &zeroFile{path: filePath, sha: blobSHA, data: append([]byte(nil), data...)}
	commitSHA := fmt.Sprintf("commit-%d", repo.nextCommitID)
	repo.nextCommitID++
	commit := zeroCommit{sha: commitSHA, message: payload.Message, path: filePath, data: append([]byte(nil), data...), when: time.Unix(1700000000+repo.nextCommitID, 0).UTC()}
	repo.commitsByPath[filePath] = append([]zeroCommit{commit}, repo.commitsByPath[filePath]...)
	status := http.StatusOK
	if current == nil {
		status = http.StatusCreated
	}
	z.writeJSON(w, status, map[string]any{"content": map[string]any{"name": filepath.Base(filePath), "path": filePath, "sha": blobSHA}, "commit": map[string]any{"sha": commitSHA}})
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
	asset := &zeroAsset{id: z.nextAssetID.Add(1), name: name, releaseTag: parts[2], size: size}
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
	// Mirror GitHub: unknown repo 404s.
	if _, ok := z.repos[name]; !ok {
		z.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
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

func mustMetadataRevision(t *testing.T, hub *StorHub, project, message string) MetadataRevision {
	t.Helper()
	revisions, err := hub.ListMetadataRevisionsContext(context.Background(), project)
	if err != nil {
		t.Fatalf("list metadata revisions: %v", err)
	}
	for _, revision := range revisions {
		if revision.Message == message {
			return revision
		}
	}
	t.Fatalf("metadata revision %q not found", message)
	return MetadataRevision{}
}
