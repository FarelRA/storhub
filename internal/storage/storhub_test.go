package storage

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	testSmallChunkSize   int64 = 8
	testSmallBufferSize        = 16
	testSingleChunkSize  int64 = 1024
	testSingleBufferSize       = 32
	testLargeChunkSize   int64 = 1 << 30
	testLargeBufferSize        = 4 << 20
	// NOTE: the 1000-assets-per-release ceiling lives in non-test source
	// as releaseAssetCap; tests reference that const, never a literal.
)

func singleChunkTestConfig() Config {
	return Config{
		ChunkSize:         testSingleChunkSize,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	}
}

func defaultTestConfig() Config {
	cfg := DefaultConfig()
	cfg.DisableGitBackend = true
	return cfg
}

func smallTransferTestConfig() Config {
	return baseTestConfig()
}

func smallRetryDisabledTestConfig() Config {
	return baseTestConfig()
}

func retryTestConfig() Config {
	return Config{
		MaxRetries:        2,
		BaseRetryDelay:    time.Millisecond,
		MaxRetryDelay:     5 * time.Millisecond,
		DisableGitBackend: true,
		RateMaxWait:       disableRateLimit,
		RatePointsPerMin:  testRatePointsPerMin,
		RateContentPerMin: testRateContentPerMin,
	}
}

func smallRetryTestConfig() Config {
	return baseTestConfig(withRetries(2))
}

// disableRateLimit is the RateMaxWait sentinel meaning "fail fast, never
// wait on the governor" (was: a bare -1 pasted across builders).
const disableRateLimit = -1

const (
	testRatePointsPerMin  = int64(1) << 40
	testRateContentPerMin = int64(1) << 40
)

// baseTestConfig is the single funnel for mock-backed test hubs: small
// chunks, no retries, no atime, no git backend, fail-fast governor with an
// effectively infinite per-minute budget. Variants pass opts (withRetries)
// instead of pasting the literal block.
func baseTestConfig(opts ...func(*Config)) Config {
	cfg := Config{
		ChunkSize:         testSmallChunkSize,
		BufferSize:        testSmallBufferSize,
		MaxRetries:        0,
		AtimePolicy:       "noatime",
		DisableGitBackend: true,
		RateMaxWait:       disableRateLimit,
		RatePointsPerMin:  testRatePointsPerMin,
		RateContentPerMin: testRateContentPerMin,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// withRetries enables n retries with millisecond test-scale backoff.
func withRetries(n int) func(*Config) {
	return func(cfg *Config) {
		cfg.MaxRetries = n
		cfg.BaseRetryDelay = time.Millisecond
		cfg.MaxRetryDelay = 5 * time.Millisecond
	}
}

// onContentsPUT arms a one-shot intercept filtered to contents-API PUTs,
// replacing the pasted Store(Contains(..."/contents/...")) closures with a
// named hook. t.Cleanup auto-disarms back to pass-through.
func (m *mockGitHub) onContentsPUT(t *testing.T, fn func(w http.ResponseWriter, r *http.Request) bool) {
	t.Helper()
	m.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			return fn(w, r)
		}
		return false
	})
	t.Cleanup(func() {
		m.intercept.Store((func(http.ResponseWriter, *http.Request) bool)(nil))
	})
}

// onAssetGET arms a one-shot intercept filtered to release-asset GETs
// (both the /releases/assets/<id> resolve and the /releases/<id>/assets
// list endpoint).
func (m *mockGitHub) onAssetGET(t *testing.T, fn func(w http.ResponseWriter, r *http.Request) bool) {
	t.Helper()
	m.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/assets") {
			return fn(w, r)
		}
		return false
	})
	t.Cleanup(func() {
		m.intercept.Store((func(http.ResponseWriter, *http.Request) bool)(nil))
	})
}

// uploadFixture uploads payload to project/path and flushes, returning the
// flushed hub. Shared by fidelity/contract tests instead of pasting the
// writeTempFile+Upload+Flush triple.
func uploadFixture(t *testing.T, backend *mockGitHub, project, name string, payload []byte) *StorHub {
	t.Helper()
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), name, payload)
	if _, err := hub.UploadFileContext(ctx, project, name, input); err != nil {
		t.Fatalf("upload fixture: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush fixture: %v", err)
	}
	return hub
}

// assertNamerShape pins the asset-name contract for n generated names:
// lowercase words+extensions shape, no derivation from source file names,
// no duplicates. Shared by the helper smoke test and the dictionary
// regression test (which adds only the diversity assertion).
func assertNamerShape(t *testing.T, namer *assetNamer, n int) []string {
	t.Helper()
	nameRe := regexp.MustCompile(`^[a-z]+(?:[-_]?[a-z]+){0,4}(?:\.[a-z]+){1,5}$`)
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name, err := namer.Next()
		if err != nil {
			t.Fatalf("generate asset name: %v", err)
		}
		if !nameRe.MatchString(name) {
			t.Fatalf("unexpected asset name format: %q", name)
		}
		if strings.Contains(name, "file") || strings.Contains(name, "txt") || strings.Contains(name, filepath.Base("docs/file.txt")) {
			t.Fatalf("asset name should not derive from source file name: %q", name)
		}
		if _, ok := seen[name]; ok {
			t.Fatalf("duplicate asset name generated: %q", name)
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func rateLimitTestConfig(sleep func(context.Context, time.Duration) error) Config {
	return Config{
		MaxRetries:        1,
		BaseRetryDelay:    time.Millisecond,
		MaxRetryDelay:     2 * time.Millisecond,
		Sleep:             sleep,
		Now:               time.Now,
		DisableGitBackend: true,
		RateMaxWait:       15 * time.Minute,
		RatePointsPerMin:  1 << 40,
		RateContentPerMin: 1 << 40,
	}
}

func liveSmokeConfig() Config {
	return Config{
		ChunkSize:         testLargeChunkSize,
		BufferSize:        testLargeBufferSize,
		MaxRetries:        4,
		RateMaxWait:       -1,
		RatePointsPerMin:  1 << 40,
		RateContentPerMin: 1 << 40,
	}
}

func liveLargeSmokeConfig(client *http.Client) Config {
	cfg := liveSmokeConfig()
	cfg.HTTPClient = client
	cfg.ChunkSize = chunking.MaxReleaseAssetSize
	return cfg
}

func largeValidationConfig() Config {
	return defaultTestConfig()
}

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

func newLiveHub(t *testing.T, token string, cfg Config) *StorHub {
	t.Helper()
	hub, err := NewStorHubWithConfig(token, cfg)
	if err != nil {
		t.Fatalf("new storhub client: %v", err)
	}
	return hub
}

func TestUploadUsesCallerOwnershipForNewFiles(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	adminCtx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})
	if err := hub.MkdirContext(adminCtx, "project-owner-upload", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, "project-owner-upload", "docs", 0o777); err != nil {
		t.Fatalf("chmod docs: %v", err)
	}
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 986, GID: 986, Groups: []uint32{986}})
	input := writeTempFile(t, t.TempDir(), "owner.txt", []byte("owner"))
	file, err := hub.UploadFileContext(ctx, "project-owner-upload", "docs/file.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if file.UID != 986 || file.GID != 986 {
		t.Fatalf("expected caller-owned uploaded file, got %d:%d", file.UID, file.GID)
	}
}

func TestUploadListDownloadSingleChunk(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())

	input := writeTempFile(t, t.TempDir(), "single.txt", []byte("hello streaming world"))
	meta, err := hub.UploadFile("project-a", "single.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if len(meta.Chunks) != 1 {
		t.Fatalf("unexpected metadata: %+v", meta)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	files, err := hub.ListFiles("project-a")
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("unexpected files: %+v", files)
	}

	output := filepath.Join(t.TempDir(), "downloaded.txt")
	if err := hub.DownloadFile("project-a", "single.txt", output); err != nil {
		t.Fatalf("download file: %v", err)
	}
	assertFileContent(t, output, []byte("hello streaming world"))
	backend.assertRepoStats(t, "project-a", 1, int64(len("hello streaming world")))
	if repo := backend.repo("project-a"); repo == nil || !repo.private {
		t.Fatal("expected repositories to be private by default")
	}
}

func TestReadFileAtContextDownloadsChunksSequentially(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var active atomic.Int32
	var maxActive atomic.Int32
	var chunkGets atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			current := active.Add(1)
			for {
				peak := maxActive.Load()
				if current <= peak || maxActive.CompareAndSwap(peak, current) {
					break
				}
			}
			// The first chunk alone carries the sleep: it exists to
			// widen the overlap window so a concurrent second fetch
			// would be observed. Later chunks add no proof value.
			if chunkGets.Add(1) == 1 {
				time.Sleep(15 * time.Millisecond)
			}
			active.Add(-1)
		}
		return false
	})
	// 3 full chunks + a partial tail: the same multi-chunk shape as the
	// original 96MB fixture at ~1/32 the size (the size is not the
	// invariant; the sequential download of several chunks is).
	hub := backend.newClient(t, Config{ChunkSize: 1 << 20, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()
	data := bytes.Repeat([]byte("z"), 3*(1<<20)+12345)
	input := writeTempFile(t, t.TempDir(), "video.bin", data)
	if _, err := hub.UploadFileContext(ctx, "project-sequential-read", "video.bin", input); err != nil {
		t.Fatalf("upload large file: %v", err)
	}
	got, err := hub.ReadFileAtContext(ctx, "project-sequential-read", "video.bin", 0, int64(len(data)))
	if err != nil {
		t.Fatalf("read file at: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("read data mismatch")
	}
	if peak := maxActive.Load(); peak != 1 {
		t.Fatalf("expected strictly sequential chunk downloads, got peak %d", peak)
	}
}

func TestDirectoryOperationsAndPathSemantics(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.Mkdir("project-tree", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.Mkdir("project-tree", "docs/specs"); err != nil {
		t.Fatalf("mkdir docs/specs: %v", err)
	}
	entries, err := hub.ReadDir("project-tree", "")
	if err != nil {
		t.Fatalf("readdir root: %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "docs" || !entries[0].IsDir {
		t.Fatalf("unexpected root entries: %+v", entries)
	}
	info, err := hub.StatPath("project-tree", "docs/specs")
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !info.IsDir {
		t.Fatalf("expected directory info, got %+v", info)
	}
	if err := hub.Rmdir("project-tree", "docs"); err == nil {
		t.Fatal("expected non-empty rmdir to fail")
	}
}

func TestCreateRenameReadWriteAndTruncateFileOperations(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 4, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	if err := hub.Mkdir("project-fs-ops", "notes"); err != nil {
		t.Fatalf("mkdir notes: %v", err)
	}
	created, err := hub.CreateFile("project-fs-ops", "notes/todo.txt")
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if created.Size != 0 {
		t.Fatalf("expected empty file, got %+v", created)
	}
	if _, err := hub.WriteFileAt("project-fs-ops", "notes/todo.txt", 0, []byte("hello")); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if _, err := hub.WriteFileAt("project-fs-ops", "notes/todo.txt", 7, []byte("world")); err != nil {
		t.Fatalf("write beyond eof: %v", err)
	}
	data, err := hub.ReadFileAt("project-fs-ops", "notes/todo.txt", 0, 12)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(data, []byte{'h', 'e', 'l', 'l', 'o', 0, 0, 'w', 'o', 'r', 'l', 'd'}) {
		t.Fatalf("unexpected file data: %v", data)
	}
	if _, err := hub.AppendFile("project-fs-ops", "notes/todo.txt", []byte("!")); err != nil {
		t.Fatalf("append file: %v", err)
	}
	if _, err := hub.TruncateFile("project-fs-ops", "notes/todo.txt", 5); err != nil {
		t.Fatalf("truncate shrink: %v", err)
	}
	if err := hub.Rename("project-fs-ops", "notes/todo.txt", "notes/done.txt"); err != nil {
		t.Fatalf("rename file: %v", err)
	}
	output := filepath.Join(t.TempDir(), "done.txt")
	if err := hub.DownloadFile("project-fs-ops", "notes/done.txt", output); err != nil {
		t.Fatalf("download renamed file: %v", err)
	}
	assertFileContent(t, output, []byte("hello"))
	entries, err := hub.ReadDir("project-fs-ops", "notes")
	if err != nil {
		t.Fatalf("readdir notes: %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "notes/done.txt" {
		t.Fatalf("unexpected notes entries: %+v", entries)
	}
	stats, err := hub.StatFS("project-fs-ops")
	if err != nil {
		t.Fatalf("statfs: %v", err)
	}
	if stats.Files != 1 || stats.Directories != 1 || stats.Bytes != 5 {
		t.Fatalf("unexpected fs stats: %+v", stats)
	}
}

func TestCreateFileStoresEmptyMetadataWithoutAssetUpload(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.Mkdir("project-empty-upload", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	meta, err := hub.CreateFile("project-empty-upload", "docs/empty.txt")
	if err != nil {
		t.Fatalf("create empty file: %v", err)
	}
	if meta.Size != 0 {
		t.Fatalf("expected zero-sized file metadata, got %+v", meta)
	}
	if len(meta.Chunks) != 0 {
		t.Fatalf("expected empty file to have no chunks, got %+v", meta.Chunks)
	}

	if uploadCalls.Load() != 0 {
		t.Fatalf("expected no asset uploads for empty file, got %d", uploadCalls.Load())
	}
}

func TestFilesystemEdgeCasesAndRootSemantics(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	// POSIX: mkdir on the always-existing root reports EEXIST.
	if err := hub.Mkdir("project-fs-edge", "."); err == nil {
		t.Fatal("expected mkdir on root to fail")
	}
	if err := hub.Rmdir("project-fs-edge", ""); err == nil {
		t.Fatal("expected rmdir root to fail")
	}
	if _, err := hub.CreateFile("project-fs-edge", ""); err == nil {
		t.Fatal("expected empty create path to fail")
	}
	if err := hub.Mkdir("project-fs-edge", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.Mkdir("project-fs-edge", "docs"); err == nil {
		t.Fatal("expected duplicate mkdir to fail")
	}
	if err := hub.Mkdir("project-fs-edge", "docs/nested"); err != nil {
		t.Fatalf("mkdir docs/nested: %v", err)
	}
	if _, err := hub.CreateFile("project-fs-edge", "docs/nested/file.txt"); err != nil {
		t.Fatalf("create nested file: %v", err)
	}
	if err := hub.Unlink("project-fs-edge", "docs"); err == nil {
		t.Fatal("expected unlink directory path to fail")
	}
	if err := hub.Rename("project-fs-edge", "docs", "docs/nested/docs"); err == nil {
		t.Fatal("expected renaming directory into itself to fail")
	}
	rootInfo, err := hub.StatPath("project-fs-edge", "")
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if !rootInfo.IsDir || rootInfo.Path != "" {
		t.Fatalf("unexpected root stat: %+v", rootInfo)
	}
	entries, err := hub.ReadDir("project-fs-edge", "")
	if err != nil {
		t.Fatalf("readdir root: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "docs" || !entries[0].IsDir {
		t.Fatalf("unexpected root entries: %+v", entries)
	}
}

func TestRenameDirectoryMovesTree(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.Mkdir("project-dir-rename", "a"); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	if err := hub.Mkdir("project-dir-rename", "a/b"); err != nil {
		t.Fatalf("mkdir a/b: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "nested.txt", []byte("payload"))
	if _, err := hub.UploadFile("project-dir-rename", "a/b/file.txt", input); err != nil {
		t.Fatalf("upload nested file: %v", err)
	}
	if err := hub.Rename("project-dir-rename", "a", "renamed"); err != nil {
		t.Fatalf("rename dir: %v", err)
	}
	if _, err := hub.StatPath("project-dir-rename", "renamed/b/file.txt"); err != nil {
		t.Fatalf("stat moved file: %v", err)
	}
	if _, err := hub.StatPath("project-dir-rename", "a/b/file.txt"); err == nil {
		t.Fatal("expected old path lookup to fail")
	}
}

// Zero MaxRetries uniformly means "no retries"; there is no ambiguous
// unset-vs-zero split. Negative values are rejected by Validate.
func TestConfigRetriesSemantics(t *testing.T) {
	t.Parallel()
	defaults := (Config{}).WithDefaults()
	if defaults.MaxRetries != 0 {
		t.Fatalf("zero MaxRetries must mean no retries, got %d", defaults.MaxRetries)
	}
	explicit := (Config{ChunkSize: chunking.DefaultChunkSize, MaxRetries: 3}).WithDefaults()
	if explicit.MaxRetries != 3 {
		t.Fatalf("expected explicit retries preserved, got %d", explicit.MaxRetries)
	}
	if err := (Config{MaxRetries: -1}).WithDefaults().Validate(); err == nil {
		t.Fatal("expected negative MaxRetries to be rejected")
	}
}

func TestReplaceDeleteRollbackMetadata(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	inputA := writeTempFile(t, t.TempDir(), "v1.txt", []byte("version-a"))
	first, err := hub.UploadFile("project-history", "artifact.txt", inputA)
	if err != nil {
		t.Fatalf("upload first: %v", err)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata after upload: %v", err)
	}

	inputB := writeTempFile(t, t.TempDir(), "v2.txt", []byte("version-b-better"))
	_, err = hub.ReplaceFile("project-history", "artifact.txt", inputB)
	if err != nil {
		t.Fatalf("replace file: %v", err)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata after replace: %v", err)
	}

	revisions, err := hub.ListMetadataRevisions("project-history")
	if err != nil {
		t.Fatalf("list metadata revisions: %v", err)
	}
	if len(revisions) < 2 {
		t.Fatalf("expected metadata history, got %+v", revisions)
	}

	if err := hub.DeleteFile("project-history", "artifact.txt"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	files, err := hub.ListFiles("project-history")
	if err != nil {
		t.Fatalf("list files after delete: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected no active files, got %+v", files)
	}

	repo := backend.repo("project-history")
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-history")
	firstChunkInfo := repoMeta.Chunks()[first.Chunks[0]]
	if repo == nil || repo.releasesByTag[firstChunkInfo.Release] == nil {
		t.Fatalf("expected immutable release to remain")
	}
	if len(repo.assets) < 2 {
		t.Fatalf("expected immutable assets to remain, got %d", len(repo.assets))
	}

	oldest := revisions[len(revisions)-1]
	if err := hub.RollbackMetadata("project-history", oldest.CommitSHA); err != nil {
		t.Fatalf("rollback metadata: %v", err)
	}
	files, err = hub.ListFiles("project-history")
	if err != nil {
		t.Fatalf("list files after rollback: %v", err)
	}
	if len(files) != 1 || files[0].Size != first.Size {
		t.Fatalf("expected rollback to restore first version, got %+v", files)
	}

	output := filepath.Join(t.TempDir(), "rolled-back.txt")
	if err := hub.DownloadFile("project-history", "artifact.txt", output); err != nil {
		t.Fatalf("download rolled back file: %v", err)
	}
	assertFileContent(t, output, []byte("version-a"))
}

func TestPatchFileReusesExistingAssetRanges(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	input := writeTempFile(t, t.TempDir(), "patch.txt", []byte("abcdefghij"))
	meta, err := hub.UploadFile("project-patch", "patch.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	patched, err := hub.PatchFile("project-patch", "patch.txt", 3, 3, []byte("XYZ"))
	if err != nil {
		t.Fatalf("patch file: %v", err)
	}
	if len(patched.Chunks) != 3 {
		t.Fatalf("expected three logical chunks after patch, got %+v", patched.Chunks)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-patch")
	patchedChunks0 := repoMeta.Chunks()[patched.Chunks[0]]
	patchedChunks2 := repoMeta.Chunks()[patched.Chunks[2]]
	metaChunks0 := repoMeta.Chunks()[meta.Chunks[0]]
	patchedChunks1 := repoMeta.Chunks()[patched.Chunks[1]]
	if patchedChunks0.AssetID != metaChunks0.AssetID || patchedChunks2.AssetID != metaChunks0.AssetID {
		t.Fatalf("expected unchanged data to reuse original asset, got %+v", patched.Chunks)
	}
	if patchedChunks1.AssetID == metaChunks0.AssetID {
		t.Fatalf("expected edited segment to use a new asset, got %+v", patched.Chunks)
	}
	output := filepath.Join(t.TempDir(), "patched.txt")
	if err := hub.DownloadFile("project-patch", "patch.txt", output); err != nil {
		t.Fatalf("download patched file: %v", err)
	}
	assertFileContent(t, output, []byte("abcXYZghij"))
	if len(backend.repo("project-patch").assets) != 2 {
		t.Fatalf("expected one original asset and one patch asset, got %d", len(backend.repo("project-patch").assets))
	}
}

func TestPatchFileUsesRangeDownloads(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 1, DisableGitBackend: true})
	input := writeTempFile(t, t.TempDir(), "ranges.txt", []byte("abcdefghij"))
	if _, err := hub.UploadFile("project-range-patch", "ranges.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if _, err := hub.PatchFile("project-range-patch", "ranges.txt", 4, 2, []byte("ZZ")); err != nil {
		t.Fatalf("patch file: %v", err)
	}
	var sawRange atomic.Bool
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") && r.Header.Get("Range") != "" {
			sawRange.Store(true)
		}
		return false
	})
	output := filepath.Join(t.TempDir(), "ranges.out")
	if err := hub.DownloadFile("project-range-patch", "ranges.txt", output); err != nil {
		t.Fatalf("download patched file: %v", err)
	}
	if !sawRange.Load() {
		t.Fatal("expected patched download to use range requests")
	}
	assertFileContent(t, output, []byte("abcdZZghij"))
}

func TestPatchedFileDownloadUsesExactAssetRanges(t *testing.T) {
	t.Parallel()
	t.Skip("retired: CDN-redirect behavior makes exact API-path ranges unobservable; see TestPatchedFileDownloadContentCorrectness")
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 128, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	original := bytes.Repeat([]byte("a"), 100)
	input := writeTempFile(t, t.TempDir(), "exact-ranges.bin", original)
	meta, err := hub.UploadFile("project-exact-ranges", "exact-ranges.bin", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	patchedBytes := bytes.Repeat([]byte("b"), 47)
	patched, err := hub.PatchFile("project-exact-ranges", "exact-ranges.bin", 3, 47, patchedBytes)
	if err != nil {
		t.Fatalf("patch file: %v", err)
	}
	if len(patched.Chunks) != 3 {
		t.Fatalf("expected three logical chunks after patch, got %+v", patched.Chunks)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	rangeByAsset := make(map[int64][]string)
	var rangeMu sync.Mutex
	backend.onAssetGET(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/releases/assets/") {
			return false
		}
		assetID, err := strconv.ParseInt(path.Base(r.URL.Path), 10, 64)
		if err != nil {
			return false
		}
		rangeMu.Lock()
		rangeByAsset[assetID] = append(rangeByAsset[assetID], r.Header.Get("Range"))
		rangeMu.Unlock()
		return false
	})
	output := filepath.Join(t.TempDir(), "exact-ranges.out")
	if err := hub.DownloadFile("project-exact-ranges", "exact-ranges.bin", output); err != nil {
		t.Fatalf("download patched file: %v", err)
	}
	assertFileContent(t, output, append(append(append([]byte(nil), original[:3]...), patchedBytes...), original[50:]...))
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-exact-ranges")
	metaChunk0 := repoMeta.Chunks()[meta.Chunks[0]]
	patchedChunk1 := repoMeta.Chunks()[patched.Chunks[1]]
	for assetID := range rangeByAsset {
		sort.Strings(rangeByAsset[assetID])
	}
	if got := rangeByAsset[metaChunk0.AssetID]; !reflect.DeepEqual(got, []string{"bytes=0-2", "bytes=50-99"}) {
		t.Fatalf("unexpected original asset ranges: %+v", rangeByAsset)
	}
	if got := rangeByAsset[patchedChunk1.AssetID]; !reflect.DeepEqual(got, []string{"bytes=0-46"}) {
		t.Fatalf("unexpected patch asset ranges: %+v", rangeByAsset)
	}
}

// NOTE: TestPatchedFileDownloadUsesExactAssetRanges was retired. With the
// mock's CDN-redirect behavior, per-asset API-path Range strings are no
// longer observable (bytes past the first touch go direct-to-CDN), so exact
// assertions fail despite correct downloads. Coverage lives on in
// TestPatchedFileDownloadContentCorrectness (mock_fidelity_test.go).

func TestPatchFileCanSpanMultipleReleases(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "multi-release.txt", []byte("abcdefghijklmno"))
	fileMeta, err := hub.UploadFile("project-multi-release-patch", "multi-release.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	metaState, _, _ := hub.loadRepoMetadata(context.Background(), "project-multi-release-patch")
	firstRelease := metaState.Chunks()[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-multi-release-patch", firstRelease, 999)
	hub.invalidateReleaseCache("project-multi-release-patch")
	patched, err := hub.PatchFile("project-multi-release-patch", "multi-release.txt", 4, 4, []byte("ZZZZ"))
	if err != nil {
		t.Fatalf("patch file: %v", err)
	}
	metaState, _, err = hub.loadRepoMetadata(context.Background(), "project-multi-release-patch")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	seenOld := false
	seenNew := false
	for _, chunkName := range patched.Chunks {
		chunk := metaState.Chunks()[chunkName]
		if chunk.Release == firstRelease {
			seenOld = true
		} else {
			seenNew = true
		}
	}
	if !seenOld || !seenNew {
		t.Fatalf("expected patched file to span old and new releases, got %+v", patched.Chunks)
	}
	output := filepath.Join(t.TempDir(), "multi-release.out")
	if err := hub.DownloadFile("project-multi-release-patch", "multi-release.txt", output); err != nil {
		t.Fatalf("download patched file: %v", err)
	}
	assertFileContent(t, output, []byte("abcdZZZZijklmno"))
	metaState, _, err = hub.loadRepoMetadata(context.Background(), "project-multi-release-patch")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if metaState.GetRelease(firstRelease) == nil {
		t.Fatalf("expected old release to remain referenced in metadata")
	}
	if err := hub.DeleteRelease("project-multi-release-patch", firstRelease); err != nil {
		t.Fatalf("delete release: %v", err)
	}
}

func TestPatchFileRejectsOutOfBoundsEdit(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "bounds.txt", []byte("abc"))
	if _, err := hub.UploadFile("project-patch-bounds", "bounds.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if _, err := hub.PatchFile("project-patch-bounds", "bounds.txt", 2, 7, []byte("toolong")); err == nil {
		t.Fatal("expected out-of-bounds patch to fail")
	}
}

func TestPatchFileSupportsEdits(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		project    string
		file       string
		original   string
		offset     int64
		deleteSize int64
		edit       []byte
		want       string
	}{
		{"insert growth", "project-patch-insert", "insert.txt", "abcdij", 4, 0, []byte("efgh"), "abcdefghij"},
		{"delete shrink", "project-patch-delete", "delete.txt", "abcXXdef", 3, 2, nil, "abcdef"},
		{"replace different size", "project-patch-resize", "resize.txt", "abc123xyz", 3, 3, []byte("LONGER"), "abcLONGERxyz"},
		{"truncate to empty", "project-patch-empty", "empty.txt", "abc", 0, 3, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := newMockGitHub(t)
			hub := backend.newClient(t, smallTransferTestConfig())
			input := writeTempFile(t, t.TempDir(), tc.file, []byte(tc.original))
			if _, err := hub.UploadFile(tc.project, tc.file, input); err != nil {
				t.Fatalf("upload file: %v", err)
			}
			patched, err := hub.PatchFile(tc.project, tc.file, tc.offset, tc.deleteSize, tc.edit)
			if err != nil {
				t.Fatalf("patch: %v", err)
			}
			if patched.Size != int64(len(tc.want)) {
				t.Fatalf("unexpected patched size: %d, want %d", patched.Size, len(tc.want))
			}
			output := filepath.Join(t.TempDir(), tc.file+".out")
			if err := hub.DownloadFile(tc.project, tc.file, output); err != nil {
				t.Fatalf("download patched file: %v", err)
			}
			assertFileContent(t, output, []byte(tc.want))
		})
	}
}

func TestDeleteReleaseHidesCatalogOnly(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallRetryDisabledTestConfig())

	input := writeTempFile(t, t.TempDir(), "release.txt", []byte("release payload"))
	fileMeta, err := hub.UploadFile("project-release", "release.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-release")
	firstRelease := repoMeta.Chunks()[fileMeta.Chunks[0]].Release
	if err := hub.DeleteRelease("project-release", firstRelease); err != nil {
		t.Fatalf("delete release: %v", err)
	}
	releases, err := hub.ListReleases("project-release")
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 0 {
		t.Fatalf("expected no catalog releases, got %+v", releases)
	}
	repo := backend.repo("project-release")
	if repo == nil || repo.releasesByTag[firstRelease] == nil {
		t.Fatalf("expected immutable release to remain")
	}
}

func TestDeleteProject(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, defaultTestConfig())
	input := writeTempFile(t, t.TempDir(), "file.txt", []byte("payload"))
	if _, err := hub.UploadFile("project-delete", "file.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := hub.DeleteProject("project-delete"); err != nil {
		t.Fatalf("delete project: %v", err)
	}
	if backend.repo("project-delete") != nil {
		t.Fatal("expected repo to be deleted")
	}
}

func TestEnsureRepoUsesExistenceCheckBeforeCreate(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	// Guarded by the backend lock: the shared mock server reads repos
	// under mu on other tests' HTTP goroutines, so an unguarded fixture
	// write races parallel CDN reads (race detector, CI race-arm64).
	backend.mu.Lock()
	backend.repos["existing-project"] = &mockRepo{
		name:          "existing-project",
		private:       true,
		nextReleaseID: 1,
		nextBlobID:    1,
		nextCommitID:  1,
		releasesByTag: make(map[string]*mockRelease),
		releasesByID:  make(map[int64]*mockRelease),
		assets:        make(map[int64]*mockAsset),
		files:         make(map[string]*mockFile),
		commitsByPath: make(map[string][]mockCommit),
	}
	backend.mu.Unlock()
	createCalls := 0
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Path == "/user/repos" {
			createCalls++
		}
		return false
	})
	hub := backend.newClient(t, defaultTestConfig())
	if err := hub.ensureRepo(context.Background(), "existing-project"); err != nil {
		t.Fatalf("ensure existing repo: %v", err)
	}
	if createCalls != 0 {
		t.Fatalf("expected ensureRepo to skip create for existing repo, got %d create calls", createCalls)
	}
}

func TestPurgeUntrackedRemovesOrphanedAssetsAndReleases(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	inputA := writeTempFile(t, t.TempDir(), "tracked.txt", []byte("tracked payload"))
	tracked, err := hub.UploadFile("project-purge", "tracked.txt", inputA)
	if err != nil {
		t.Fatalf("upload tracked file: %v", err)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata after first upload: %v", err)
	}

	inputB := writeTempFile(t, t.TempDir(), "orphan.txt", []byte("orphan payload"))
	orphan, err := hub.UploadFile("project-purge", "orphan.txt", inputB)
	if err != nil {
		t.Fatalf("upload orphan file: %v", err)
	}

	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-purge")
	trackedRelease := repoMeta.Chunks()[tracked.Chunks[0]].Release
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata after second upload: %v", err)
	}

	if err := hub.DeleteFile("project-purge", "orphan.txt"); err != nil {
		t.Fatalf("hide orphan file: %v", err)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata after delete: %v", err)
	}

	repo := backend.repo("project-purge")
	if repo == nil {
		t.Fatal("expected repo to exist")
	}
	manualRelease := backend.addRelease(t, "project-purge", "v999")
	backend.addAssetToRelease(t, "project-purge", manualRelease.tag, "manual.bin", []byte("manual orphan"))
	backend.addAssetToRelease(t, "project-purge", trackedRelease, "extra.bin", []byte("extra orphan"))

	result, err := hub.PurgeUntracked("project-purge")
	if err != nil {
		t.Fatalf("purge untracked: %v", err)
	}
	if result.DeletedReleases != 1 {
		t.Fatalf("expected 1 deleted release, got %+v", result)
	}
	if result.DeletedAssets != len(orphan.Chunks)+1 {
		t.Fatalf("expected hidden-file assets plus one explicitly orphaned tracked-release asset to be deleted, got %+v", result)
	}

	repo = backend.repo("project-purge")
	if repo.releasesByTag[manualRelease.tag] != nil {
		t.Fatalf("expected manual release %s to be deleted", manualRelease.tag)
	}
	for _, asset := range repo.assets {
		if asset.name == "extra.bin" {
			t.Fatal("expected extra orphan asset to be deleted")
		}
	}
	if repo.releasesByTag[trackedRelease] == nil {
		t.Fatalf("expected tracked release to remain")
	}
	// Find a revision that still contained orphan.txt (whose assets the
	// purge destroyed) to prove rollback after purge fails destructively.
	revisions, err := hub.ListMetadataRevisions("project-purge")
	if err != nil {
		t.Fatalf("list metadata revisions: %v", err)
	}
	var orphanRevision string
	for _, rev := range revisions {
		snap, err := hub.getMetadataRevision(context.Background(), "project-purge", rev.CommitSHA)
		if err != nil {
			t.Fatalf("fetch revision %s: %v", rev.CommitSHA, err)
		}
		if _, ok := snap.Files()["orphan.txt"]; ok {
			orphanRevision = rev.CommitSHA
			break
		}
	}
	if orphanRevision == "" {
		t.Fatal("expected a metadata revision containing orphan.txt")
	}
	if err := hub.RollbackMetadata("project-purge", orphanRevision); err == nil {
		t.Fatal("expected rollback after purge to fail because purge is destructive")
	}
}

func TestRollbackMetadataFailsWhenDataMissing(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "missing-data.txt", []byte("payload"))
	fileMeta, err := hub.UploadFile("project-missing-data", "missing-data.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	if err := hub.DeleteFile("project-missing-data", "missing-data.txt"); err != nil {
		t.Fatalf("hide file: %v", err)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-missing-data")
	firstChunk := repoMeta.Chunks()[fileMeta.Chunks[0]]
	backend.removeAsset(t, "project-missing-data", firstChunk.AssetID)
	// Get the oldest metadata revision to test rollback failure when data is missing
	revisions, err := hub.ListMetadataRevisions("project-missing-data")
	if err != nil {
		t.Fatalf("list metadata revisions: %v", err)
	}
	if len(revisions) == 0 {
		t.Fatal("expecte at least one metadata revision")
	}
	revision := revisions[len(revisions)-1] // Last in list is oldest
	if err := hub.RollbackMetadata("project-missing-data", revision.CommitSHA); err == nil {
		t.Fatal("expected rollback to fail when referenced asset is missing")
	}
}

func TestReplaceAvoidsFullPreferredRelease(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	inputA := writeTempFile(t, t.TempDir(), "first.txt", []byte("alpha"))
	fileMeta, err := hub.UploadFile("project-capacity", "capacity.txt", inputA)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-capacity")
	firstRelease := repoMeta.Chunks()[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-capacity", firstRelease, 999)
	hub.invalidateReleaseCache("project-capacity")
	inputB := writeTempFile(t, t.TempDir(), "second.txt", []byte("beta"))
	replaced, err := hub.ReplaceFile("project-capacity", "capacity.txt", inputB)
	if err != nil {
		t.Fatalf("replace file: %v", err)
	}
	repoMeta, _, _ = hub.loadRepoMetadata(context.Background(), "project-capacity")
	replacedRelease := repoMeta.Chunks()[replaced.Chunks[0]].Release
	if replacedRelease == firstRelease {
		t.Fatalf("expected replacement to avoid full release")
	}
}

func TestUploadMissingFile(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, defaultTestConfig())
	_, err := hub.UploadFile("project-missing", "missing.txt", filepath.Join(t.TempDir(), "missing.txt"))
	if err == nil || !strings.Contains(err.Error(), "stat input file") {
		t.Fatalf("expected stat error, got %v", err)
	}
}

func TestUploadEmptyFileUsesMetadataOnly(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "empty-upload.txt", nil)
	meta, err := hub.UploadFile("project-empty-upload-file", "empty-upload.txt", input)
	if err != nil {
		t.Fatalf("upload empty file: %v", err)
	}
	if meta.Size != 0 || len(meta.Chunks) != 0 {
		t.Fatalf("unexpected empty upload metadata: %+v", meta)
	}
	output := filepath.Join(t.TempDir(), "empty-upload.out")
	if err := hub.DownloadFile("project-empty-upload-file", "empty-upload.txt", output); err != nil {
		t.Fatalf("download empty file: %v", err)
	}
	assertFileContent(t, output, []byte{})
}

func TestDownloadMissingFile(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, defaultTestConfig())
	err := hub.DownloadFile("project-missing", "missing.txt", filepath.Join(t.TempDir(), "missing.txt"))
	if err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
		t.Fatalf("expected project-not-found error, got %v", err)
	}
}

func TestDownloadUsesPersistedChunkOffsets(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	uploader := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "offsets.bin", []byte("abcdefghijklmnopqrstuvwxyz0123456789"))
	meta, err := uploader.UploadFile("project-offsets", "offsets.bin", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if len(meta.Chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %+v", meta)
	}
	if err := uploader.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	downloader := backend.newClient(t, singleChunkTestConfig())
	output := filepath.Join(t.TempDir(), "offsets.out")
	if err := downloader.DownloadFile("project-offsets", "offsets.bin", output); err != nil {
		t.Fatalf("download file with different chunk size config: %v", err)
	}
	assertFileContent(t, output, []byte("abcdefghijklmnopqrstuvwxyz0123456789"))
}

func TestReadFileAtHandlesEOFAndPartialRanges(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "partial.txt", []byte("abcdefghij"))
	if _, err := hub.UploadFile("project-read-partial", "partial.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	data, err := hub.ReadFileAt("project-read-partial", "partial.txt", 7, 10)
	if err != nil {
		t.Fatalf("partial read: %v", err)
	}
	if string(data) != "hij" {
		t.Fatalf("unexpected partial range: %q", data)
	}
	endData, err := hub.ReadFileAt("project-read-partial", "partial.txt", 10, 1)
	if err != nil {
		t.Fatalf("read at end should be empty success, got %v", err)
	}
	if len(endData) != 0 {
		t.Fatalf("expected empty read at end, got %q", endData)
	}
}

func TestDownloadRetriesInterruptedChunkStream(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallRetryTestConfig())
	input := writeTempFile(t, t.TempDir(), "retry-download.bin", []byte("download retry payload"))
	fileMeta, err := hub.UploadFile("project-download-retry", "retry-download.bin", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-download-retry")
	assetID := repoMeta.Chunks()[fileMeta.Chunks[0]].AssetID
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/releases/assets/%d", assetID)) || failures.Load() != 0 {
			return false
		}
		failures.Add(1)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("response writer does not support hijacking")
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("hijack response: %v", err)
		}
		defer func() { _ = conn.Close() }()
		payload := []byte("download retry payload")
		_, _ = fmt.Fprintf(rw, "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n", len(payload))
		_, _ = rw.Write(payload[:len(payload)/2])
		_ = rw.Flush()
		return true
	})
	output := filepath.Join(t.TempDir(), "retry-download.out")
	if err := hub.DownloadFile("project-download-retry", "retry-download.bin", output); err != nil {
		t.Fatalf("download with retry: %v", err)
	}
	if failures.Load() != 1 {
		t.Fatalf("expected one interrupted download, got %d", failures.Load())
	}
	assertFileContent(t, output, []byte("download retry payload"))
}

func TestRetryOnTransientServerError(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/repos/") && failures.Load() == 0 {
			failures.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upstream issue"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, retryTestConfig())
	input := writeTempFile(t, t.TempDir(), "retry.txt", []byte("retry payload"))
	if _, err := hub.UploadFile("project-retry", "retry.txt", input); err != nil {
		t.Fatalf("expected retried idempotent request to succeed, got %v", err)
	}
	if failures.Load() != 1 {
		t.Fatalf("expected one transient failure, got %d", failures.Load())
	}
}

// Non-idempotent POSTs (repo creation, asset upload) must surface transient
// failures instead of blind-retrying them.
func TestNonIdempotentPostsDoNotRetry(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var repoAttempts atomic.Int32
	var uploadAttempts atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/user/repos":
			repoAttempts.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upstream issue"}`))
			return true
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/"):
			uploadAttempts.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upload issue"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, retryTestConfig())
	input := writeTempFile(t, t.TempDir(), "noretry.txt", []byte("no retry payload"))
	if _, err := hub.UploadFile("project-noretry", "noretry.txt", input); err == nil {
		t.Fatal("expected non-idempotent POST failure to surface")
	}
	if repoAttempts.Load() != 1 || uploadAttempts.Load() != 0 {
		t.Fatalf("expected exactly one repo POST and no upload POST, got %d/%d", repoAttempts.Load(), uploadAttempts.Load())
	}
}

func TestConstructorDefersAuthentication(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var hits atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && r.URL.Path == "/user" {
			hits.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"bad credentials"}`))
			return true
		}
		return false
	})
	cfg := smallTransferTestConfig()
	cfg.APIBaseURL = backend.server.URL
	cfg.HTTPClient = backend.server.Client()
	cfg.Sleep = func(_ context.Context, _ time.Duration) error { return nil }
	hub, err := NewStorHubWithContext(context.Background(), backend.token, cfg)
	if err != nil {
		t.Fatalf("constructor should not authenticate eagerly: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("expected no auth requests during construction, got %d", hits.Load())
	}
	if _, err := hub.ListFiles("project-auth-lazy"); err == nil || !strings.Contains(err.Error(), "resolve authenticated user") {
		t.Fatalf("expected deferred auth failure, got %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected one auth request after first operation, got %d", hits.Load())
	}
}

func TestUploadChunkRetriesTransientFailure(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var failures, successes atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			switch {
			case failures.Load() == 0:
				failures.Add(1)
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"message":"temporary upload issue"}`))
			default:
				successes.Add(1)
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":777}`))
			}
			return true
		}
		return false
	})
	hub := backend.newClient(t, smallRetryTestConfig())
	payload := bytes.Repeat([]byte("r"), int(testSmallChunkSize)) // exactly one chunk
	input := writeTempFile(t, t.TempDir(), "upload-retry.txt", payload)
	meta, err := hub.UploadFile("project-upload-retry", "upload-retry.txt", input)
	if err != nil {
		t.Fatalf("transient upload failure must be retried: %v", err)
	}
	if meta.Size != int64(len(payload)) {
		t.Fatalf("unexpected uploaded size %d", meta.Size)
	}
	if failures.Load() != 1 || successes.Load() != 1 {
		t.Fatalf("expected one failure then one success, got %d/%d", failures.Load(), successes.Load())
	}

	persistent := newMockGitHub(t)
	var attempts atomic.Int32
	persistent.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			attempts.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"still broken"}`))
			return true
		}
		return false
	})
	hub2 := persistent.newClient(t, smallRetryTestConfig())
	payload2 := bytes.Repeat([]byte("d"), int(testSmallChunkSize))
	input2 := writeTempFile(t, t.TempDir(), "upload-broken.txt", payload2)
	if _, err := hub2.UploadFile("project-upload-doomed", "upload-broken.txt", input2); err == nil {
		t.Fatal("persistent upload failure must surface")
	}
	// initial attempt + MaxRetries(2) retries
	if got := attempts.Load(); got != 3 {
		t.Fatalf("persistent failure must exhaust retries, attempts=%d", got)
	}
}

func TestRateLimitAwareRetry(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var hits atomic.Int32
	var slept atomic.Int64
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && r.URL.Path == "/user" && hits.Load() == 0 {
			hits.Add(1)
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-RateLimit-Remaining", "0")
			// A wide reset window keeps the assertion stable under load:
			// recorded sleeps do not advance the wall clock, so the
			// measured wait must reflect until-reset, not backoff clamps.
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(30*time.Second).Unix(), 10))
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, rateLimitTestConfig(func(_ context.Context, d time.Duration) error {
		slept.Store(int64(d))
		return nil
	}))
	if hub.Owner() != "" {
		t.Fatalf("expected lazy owner resolution, got %s", hub.Owner())
	}
	if _, err := hub.ListFiles("project-rate-limit-miss"); err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
		t.Fatalf("expected project-not-found error after owner resolution, got %v", err)
	}
	if hub.Owner() != backend.owner {
		t.Fatalf("unexpected owner after request: %s", hub.Owner())
	}
	if hits.Load() != 1 {
		t.Fatalf("expected one rate-limited response, got %d", hits.Load())
	}
	waited := time.Duration(slept.Load())
	// New doctrine: a primary rate-limit wait honors x-ratelimit-reset
	// (~30s away) instead of being clamped to MaxRetryDelay. The last
	// recorded sleep is the governor's pre-send wait for the retry.
	if waited < 5*time.Second || waited > 35*time.Second {
		t.Fatalf("rate-limit wait must honor the reset window, got %v", waited)
	}
}

func TestReadAPIsReturnProjectNotFound(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	checks := []struct {
		name string
		fn   func() error
	}{
		{name: "list files", fn: func() error { _, err := hub.ListFiles("missing-project"); return err }},
		{name: "list releases", fn: func() error { _, err := hub.ListReleases("missing-project"); return err }},
		{name: "list revisions", fn: func() error { _, err := hub.ListMetadataRevisions("missing-project"); return err }},
		{name: "read dir", fn: func() error { _, err := hub.ReadDir("missing-project", ""); return err }},
		{name: "stat path", fn: func() error { _, err := hub.StatPath("missing-project", ""); return err }},
		{name: "rollback", fn: func() error { return hub.RollbackMetadata("missing-project", "commit-1") }},
	}
	for _, check := range checks {
		if err := check.fn(); err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
			t.Fatalf("expected project-not-found for %s, got %v", check.name, err)
		}
	}
}

func TestListFilesUsesMetadataCache(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	seed := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cache.txt", []byte("cache payload"))
	if _, err := seed.UploadFile("project-cache", "cache.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	var metadataGets atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/contents/.storhub/index.json") {
			metadataGets.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	if _, err := hub.ListFiles("project-cache"); err != nil {
		t.Fatalf("first list files: %v", err)
	}
	if _, err := hub.ListFiles("project-cache"); err != nil {
		t.Fatalf("second list files: %v", err)
	}
	if metadataGets.Load() != 1 {
		t.Fatalf("expected one metadata fetch, got %d", metadataGets.Load())
	}
}

func TestMetadataCacheInvalidatesAcrossMutationsAndDeleteProject(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cache-mutate.txt", []byte("cache mutate payload"))
	if _, err := hub.UploadFile("project-cache-mutate", "cache-mutate.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	var metadataGets atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/contents/.storhub/index.json") {
			metadataGets.Add(1)
		}
		return false
	})
	if _, err := hub.ListFiles("project-cache-mutate"); err != nil {
		t.Fatalf("list files from warm cache: %v", err)
	}
	if metadataGets.Load() != 0 {
		t.Fatalf("expected warm cache to avoid metadata fetch, got %d", metadataGets.Load())
	}
	if err := hub.DeleteFile("project-cache-mutate", "cache-mutate.txt"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	beforeListAfterDelete := metadataGets.Load()
	files, err := hub.ListFiles("project-cache-mutate")
	if err != nil {
		t.Fatalf("list files after delete: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected deleted file to disappear, got %+v", files)
	}
	if metadataGets.Load() != beforeListAfterDelete {
		t.Fatalf("expected list after delete to reuse updated cache, got %d total fetches", metadataGets.Load())
	}
	if err := hub.DeleteProject("project-cache-mutate"); err != nil {
		t.Fatalf("delete project: %v", err)
	}
	beforePostDeleteList := metadataGets.Load()
	if _, err := hub.ListFiles("project-cache-mutate"); err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
		t.Fatalf("expected project-not-found after delete project, got %v", err)
	}
	if metadataGets.Load() <= beforePostDeleteList {
		t.Fatal("expected metadata fetch after cache invalidation from delete project")
	}
}

func TestReadFileAtRetriesInterruptedRangeRead(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, DisableGitBackend: true})
	input := writeTempFile(t, t.TempDir(), "range-read.txt", []byte("abcdefghijklmnopqrstuvwxyz"))
	fileMeta, err := hub.UploadFile("project-range-read", "range-read.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-range-read")
	assetID := repoMeta.Chunks()[fileMeta.Chunks[0]].AssetID
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/releases/assets/%d", assetID)) || r.Header.Get("Range") == "" || failures.Load() != 0 {
			return false
		}
		failures.Add(1)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("response writer does not support hijacking")
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("hijack response: %v", err)
		}
		defer func() { _ = conn.Close() }()
		payload := []byte("cdefgh")
		_, _ = fmt.Fprintf(rw, "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n", len(payload))
		_, _ = rw.Write(payload[:3])
		_ = rw.Flush()
		return true
	})
	data, err := hub.ReadFileAt("project-range-read", "range-read.txt", 2, 6)
	if err != nil {
		t.Fatalf("read file at with retry: %v", err)
	}
	if string(data) != "cdefgh" {
		t.Fatalf("unexpected range data: %q", data)
	}
	if failures.Load() != 1 {
		t.Fatalf("expected one interrupted range read, got %d", failures.Load())
	}
}

func TestPatchRetriesInterruptedRangeSliceRead(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, DisableGitBackend: true})
	input := writeTempFile(t, t.TempDir(), "patch-retry.txt", []byte("abcdefghij"))
	fileMeta, err := hub.UploadFile("project-patch-range-retry", "patch-retry.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-patch-range-retry")
	assetID := repoMeta.Chunks()[fileMeta.Chunks[0]].AssetID
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/releases/assets/%d", assetID)) || r.Header.Get("Range") == "" || failures.Load() != 0 {
			return false
		}
		failures.Add(1)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("response writer does not support hijacking")
		}
		conn, rw, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("hijack response: %v", err)
		}
		defer func() { _ = conn.Close() }()
		payload := []byte("ab")
		_, _ = fmt.Fprintf(rw, "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n", len(payload))
		_, _ = rw.Write(payload[:1])
		_ = rw.Flush()
		return true
	})
	if _, err := hub.PatchFile("project-patch-range-retry", "patch-retry.txt", 2, 3, []byte("XYZ")); err != nil {
		t.Fatalf("patch file with retry: %v", err)
	}
	output := filepath.Join(t.TempDir(), "patch-retry.out")
	if err := hub.DownloadFile("project-patch-range-retry", "patch-retry.txt", output); err != nil {
		t.Fatalf("download patched file: %v", err)
	}
	assertFileContent(t, output, []byte("abXYZfghij"))
	if failures.Load() != 1 {
		t.Fatalf("expected one interrupted slice read, got %d", failures.Load())
	}
}

func TestUploadHonorsCanceledContext(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	uploadStarted := make(chan struct{}, 1)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("response writer does not support hijacking")
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Fatalf("hijack upload response: %v", err)
			}
			select {
			case uploadStarted <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			_ = conn.Close()
			return true
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "upload-cancel.txt", []byte("upload cancel payload"))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := hub.UploadFileContext(ctx, "project-upload-cancel", "upload-cancel.txt", input)
		errCh <- err
	}()
	<-uploadStarted
	cancel()
	if err := <-errCh; err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled upload, got %v", err)
	}
}

func TestValidateProjectRejectsInvalidNames(t *testing.T) {
	t.Parallel()
	invalid := []string{"", " ", ".", "..", "bad/name", "bad name", "bad*name", ".hidden", "trailing."}
	for _, name := range invalid {
		if err := validateProject(name); err == nil {
			t.Fatalf("expected invalid project name %q to be rejected", name)
		}
	}
	if err := validateProject("valid.project-name_123"); err != nil {
		t.Fatalf("expected valid project name, got %v", err)
	}
}

// A transient commit failure must NOT discard acknowledged writes: dirty
// state is retained and the commit loop retries it.
func TestTransientMetadataCommitFailureRetriesWithRetainedState(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var failed atomic.Bool
	backend.onContentsPUT(t, func(w http.ResponseWriter, r *http.Request) bool {
		if !failed.Load() {
			failed.Store(true)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"transient failure"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "retry.txt", []byte("retry payload"))
	if _, err := hub.UploadFile("project-retry", "retry.txt", input); err != nil {
		t.Fatalf("upload should succeed (metadata commit is async): %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		files, err := hub.ListFiles("project-retry")
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		if len(files) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained dirty state was not retried after transient failure; files=%+v", files)
		}
		time.Sleep(5 * time.Millisecond)
	}
	repo := backend.repo("project-retry")
	if repo == nil || len(repo.assets) == 0 || repo.releasesByTag["v1"] == nil {
		t.Fatalf("expected immutable data to remain, repo=%+v", repo)
	}
}

func TestUploadRetriesMetadataConflictByReloading(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var conflicts atomic.Int32
	var commitCount atomic.Int32
	backend.onContentsPUT(t, func(w http.ResponseWriter, r *http.Request) bool {
		commitCount.Add(1)
		if commitCount.Load() == 2 && conflicts.Load() == 0 {
			conflicts.Add(1)
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"sha does not match"}`))
			return true
		}
		return false
	})
	cfg := smallTransferTestConfig()
	cfg.MaxRetries = 2
	cfg.BaseRetryDelay = time.Millisecond
	cfg.MaxRetryDelay = 5 * time.Millisecond
	hub := backend.newClient(t, cfg)

	// First upload to create initial metadata
	input1 := writeTempFile(t, t.TempDir(), "initial.txt", []byte("initial"))
	if _, err := hub.UploadFile("project-conflict", "initial.txt", input1); err != nil {
		t.Fatalf("upload initial file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush first metadata: %v", err)
	}

	// Second upload - its async commit will hit an injected conflict,
	// triggering the rebase: the pending op replays onto upstream and the
	// retry lands it. The observable contract is one conflict plus a
	// converged two-file state (the mutation SURVIVES the conflict now).
	input2 := writeTempFile(t, t.TempDir(), "conflict.txt", []byte("conflict payload"))
	if _, err := hub.UploadFile("project-conflict", "conflict.txt", input2); err != nil {
		t.Fatalf("upload should succeed (commit is async): %v", err)
	}
	stateDeadline := time.Now().Add(time.Second)
	for {
		conflictsSeen := conflicts.Load() >= 1
		files, err := hub.ListFiles("project-conflict")
		if err != nil {
			t.Fatalf("list files: %v", err)
		}
		if conflictsSeen && len(files) == 2 {
			break
		}
		if time.Now().After(stateDeadline) {
			t.Fatalf("conflict rebase did not converge: conflicts=%d files=%d",
				conflicts.Load(), len(files))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMetadataCommitRetriesTransientFailure(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") && failures.Load() == 0 {
			failures.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary metadata issue"}`))
			return true
		}
		return false
	})
	cfg := smallTransferTestConfig()
	cfg.MaxRetries = 2
	cfg.BaseRetryDelay = time.Millisecond
	cfg.MaxRetryDelay = 5 * time.Millisecond
	hub := backend.newClient(t, cfg)
	input := writeTempFile(t, t.TempDir(), "meta-retry.txt", []byte("meta retry payload"))
	if _, err := hub.UploadFile("project-meta-retry", "meta-retry.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	// With batching, commit happens later. Poll (bounded) for the commit,
	// failure, and retry cycle instead of sleeping a fixed duration.
	failureDeadline := time.Now().Add(time.Second)
	for failures.Load() < 1 {
		if time.Now().After(failureDeadline) {
			t.Fatalf("expected one transient metadata failure, got %d", failures.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDownloadHonorsContextCancellation(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	assetStarted := make(chan struct{}, 1)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			select {
			case assetStarted <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return true
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cancel.txt", []byte("cancel payload"))
	if _, err := hub.UploadFile("project-cancel", "cancel.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	output := filepath.Join(t.TempDir(), "cancel.out")
	errCh := make(chan error, 1)
	go func() { errCh <- hub.DownloadFileContext(ctx, "project-cancel", "cancel.txt", output) }()
	<-assetStarted
	cancel()
	err := <-errCh
	if err == nil {
		t.Fatal("expected canceled download to fail")
	}
}

func TestMetadataBatchesAndFlushes(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	// Upload multiple files - metadata commits happen asynchronously.
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("file-%d.txt", i)
		input := writeTempFile(t, t.TempDir(), name, []byte(name))
		if _, err := hub.UploadFile("project-batching", name, input); err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
	}

	// Wait for all background commit operations to complete
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	// Verify all 3 files are in the metadata
	files, err := hub.ListFiles("project-batching")
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 files in metadata, got %d", len(files))
	}
}

func TestListReleasesPaginates(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seed"))
	if _, err := hub.UploadFile("project-release-pages", "seed.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	for i := 0; i < 105; i++ {
		backend.addRelease(t, "project-release-pages", fmt.Sprintf("extra-%03d", i))
	}
	releases, err := hub.listReleases(context.Background(), "project-release-pages")
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 106 {
		t.Fatalf("expected 106 releases, got %d", len(releases))
	}
}

func TestRejectsInvalidMetadataSnapshots(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "invalid.bin", []byte("invalid metadata payload"))
	if _, err := hub.UploadFile("project-invalid-metadata", "invalid.bin", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	meta := mustLoadMetadata(t, backend.repo("project-invalid-metadata"))
	// corrupt a chunk offset to create invalid metadata
	for _, file := range meta.Files() {
		if len(file.Chunks) > 0 {
			chunk := meta.Chunks()[file.Chunks[0]]
			chunk.Offset = 99
			meta.Chunks()[file.Chunks[0]] = chunk
			break
		}
	}
	backend.setMetadata(t, "project-invalid-metadata", meta)
	hub = backend.newClient(t, smallTransferTestConfig())
	if _, err := hub.ListFiles("project-invalid-metadata"); err == nil {
		t.Fatal("expected invalid metadata to be rejected")
	}
}

func TestCleanupProjectSkipsNoopCommit(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cleanup.txt", []byte("cleanup payload"))
	if _, err := hub.UploadFile("project-cleanup", "cleanup.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	backend.mu.Lock()
	repo := backend.repos["project-cleanup"]
	before := len(repo.commitsByPath[metadataFilePath])
	backend.mu.Unlock()
	if err := hub.CleanupProject("project-cleanup"); err != nil {
		t.Fatalf("cleanup project: %v", err)
	}
	backend.mu.Lock()
	after := len(repo.commitsByPath[metadataFilePath])
	backend.mu.Unlock()
	if after != before {
		t.Fatalf("expected cleanup noop commit count to stay %d, got %d", before, after)
	}
}

func TestPOSIXMetadataOpsHardlinksSymlinksAndXAttrs(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	// Ownership and chown operations require an explicitly identified
	// privileged caller; absent identities fail closed to the process user.
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0})

	if err := hub.MkdirContext(ctx, "project-posix", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "base.txt", []byte("hello world"))
	base, err := hub.UploadFileContext(ctx, "project-posix", "docs/base.txt", input)
	if err != nil {
		t.Fatalf("upload base file: %v", err)
	}
	linked, err := hub.LinkContext(ctx, "project-posix", "docs/base.txt", "docs/alias.txt")
	if err != nil {
		t.Fatalf("create hard link: %v", err)
	}
	if linked.Inode != base.Inode {
		t.Fatalf("expected hard link to reuse inode, base=%d alias=%d", base.Inode, linked.Inode)
	}
	baseInfo, err := hub.StatPathContext(ctx, "project-posix", "docs/base.txt")
	if err != nil {
		t.Fatalf("stat base: %v", err)
	}
	aliasInfo, err := hub.StatPathContext(ctx, "project-posix", "docs/alias.txt")
	if err != nil {
		t.Fatalf("stat alias: %v", err)
	}
	if baseInfo.Inode != aliasInfo.Inode || baseInfo.NLink != 2 || aliasInfo.NLink != 2 {
		t.Fatalf("unexpected hard link stats: base=%+v alias=%+v", baseInfo, aliasInfo)
	}
	if err := hub.ChmodContext(ctx, "project-posix", "docs/base.txt", 0o600); err != nil {
		t.Fatalf("chmod hardlink family: %v", err)
	}
	if err := hub.ChownContext(ctx, "project-posix", "docs/alias.txt", 123, 456); err != nil {
		t.Fatalf("chown hardlink family: %v", err)
	}
	if err := hub.ChtimesContext(ctx, "project-posix", "docs/base.txt", 10, 20); err != nil {
		t.Fatalf("chtimes hardlink family: %v", err)
	}
	if err := hub.SetXAttrContext(ctx, "project-posix", "docs/alias.txt", "user.note", []byte("linked")); err != nil {
		t.Fatalf("setxattr hardlink family: %v", err)
	}
	attrs, err := hub.ListXAttrContext(ctx, "project-posix", "docs/base.txt")
	if err != nil {
		t.Fatalf("listxattr base: %v", err)
	}
	if len(attrs) != 1 || attrs[0] != "user.note" {
		t.Fatalf("unexpected xattrs: %v", attrs)
	}
	value, err := hub.GetXAttrContext(ctx, "project-posix", "docs/base.txt", "user.note")
	if err != nil {
		t.Fatalf("getxattr base: %v", err)
	}
	if string(value) != "linked" {
		t.Fatalf("unexpected xattr value: %q", value)
	}
	updated, err := hub.WriteFileAtContext(ctx, "project-posix", "docs/base.txt", 6, []byte("storhub"))
	if err != nil {
		t.Fatalf("write through hardlink family: %v", err)
	}
	if updated.Inode != base.Inode {
		t.Fatalf("expected inode preservation after write, got %d want %d", updated.Inode, base.Inode)
	}
	aliasDownload := filepath.Join(t.TempDir(), "alias.txt")
	if err := hub.DownloadFileContext(ctx, "project-posix", "docs/alias.txt", aliasDownload); err != nil {
		t.Fatalf("download alias: %v", err)
	}
	assertFileContent(t, aliasDownload, []byte("hello storhub"))
	aliasInfo, err = hub.StatPathContext(ctx, "project-posix", "docs/alias.txt")
	if err != nil {
		t.Fatalf("restat alias: %v", err)
	}
	if aliasInfo.Mode != 0o600 || aliasInfo.UID != 123 || aliasInfo.GID != 456 {
		t.Fatalf("hardlink family metadata did not propagate: %+v", aliasInfo)
	}
	if aliasInfo.ModifiedAt <= time.Unix(20, 0).Unix() {
		t.Fatalf("expected modified time to advance after write, got %v", aliasInfo.ModifiedAt)
	}
	if err := hub.RemoveXAttrContext(ctx, "project-posix", "docs/base.txt", "user.note"); err != nil {
		t.Fatalf("removexattr family: %v", err)
	}
	attrs, err = hub.ListXAttrContext(ctx, "project-posix", "docs/base.txt")
	if err != nil {
		t.Fatalf("listxattr after remove: %v", err)
	}
	if len(attrs) != 0 {
		t.Fatalf("expected xattrs to be removed, got %v", attrs)
	}
	if err := hub.UnlinkContext(ctx, "project-posix", "docs/base.txt"); err != nil {
		t.Fatalf("unlink one hardlink: %v", err)
	}
	aliasInfo, err = hub.StatPathContext(ctx, "project-posix", "docs/alias.txt")
	if err != nil {
		t.Fatalf("stat alias after unlink: %v", err)
	}
	if aliasInfo.NLink != 1 {
		t.Fatalf("expected remaining hardlink count 1, got %+v", aliasInfo)
	}
	symlink, err := hub.SymlinkContext(ctx, "project-posix", "alias.txt", "docs/alias-link")
	if err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if symlink.Symlink != "alias.txt" {
		t.Fatalf("unexpected symlink metadata: %+v", symlink)
	}
	target, err := hub.ReadlinkContext(ctx, "project-posix", "docs/alias-link")
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "alias.txt" {
		t.Fatalf("unexpected symlink target: %q", target)
	}
	linkInfo, err := hub.StatPathContext(ctx, "project-posix", "docs/alias-link")
	if err != nil {
		t.Fatalf("stat symlink: %v", err)
	}
	if !linkInfo.IsSymlink || linkInfo.SymlinkTarget != "alias.txt" {
		t.Fatalf("unexpected symlink stat: %+v", linkInfo)
	}
	// Read has open() semantics and follows the final symlink.
	if data, err := hub.ReadFileAtContext(ctx, "project-posix", "docs/alias-link", 0, 4); err != nil || string(data) != "hell" {
		t.Fatalf("read through symlink must return the target bytes, got %q err=%v", data, err)
	}
	if err := hub.SetXAttrContext(ctx, "project-posix", "", "user.root", []byte("rooted")); err != nil {
		t.Fatalf("set root xattr: %v", err)
	}
	rootAttrs, err := hub.ListXAttrContext(ctx, "project-posix", "")
	if err != nil {
		t.Fatalf("list root xattrs: %v", err)
	}
	if len(rootAttrs) != 1 || rootAttrs[0] != "user.root" {
		t.Fatalf("unexpected root xattrs: %v", rootAttrs)
	}
}

func TestFUSEAdapterCallbacksAndHandles(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()

	if err := hub.MkdirContext(ctx, "project-fuse", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "fuse.txt", []byte("hello world"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse", "docs/file.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	fsys, err := hub.NewFUSE("project-fuse", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	var rootOut fuse.EntryOut
	_, errno := fsys.RootNode().Lookup(ctx, "docs", &rootOut)
	if errno != 0 {
		t.Fatalf("lookup docs failed: %v", errno)
	}
	docsEntry, err := hub.StatPathContext(ctx, "project-fuse", "docs")
	if err != nil {
		t.Fatalf("stat docs: %v", err)
	}
	docsNode := fsys.EnsureNodeForTest(ctx, docsEntry)
	if docsNode == nil {
		t.Fatal("expected docs node")
	}
	dirStream, errno := docsNode.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("readdir docs failed: %v", errno)
	}
	// Skip "." and ".." entries to find "file.txt"
	var entry fuse.DirEntry
	for {
		entry, errno = dirStream.Next()
		if errno != 0 {
			t.Fatalf("readdir next failed: %v", errno)
		}
		if entry.Name != "." && entry.Name != ".." {
			break
		}
	}
	if entry.Name != "file.txt" {
		t.Fatalf("unexpected directory entry: %+v", entry)
	}
	var fileOut fuse.EntryOut
	_, errno = docsNode.Lookup(ctx, "file.txt", &fileOut)
	if errno != 0 {
		t.Fatalf("lookup file failed: %v", errno)
	}
	fileEntry, err := hub.StatPathContext(ctx, "project-fuse", "docs/file.txt")
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	fileNode := fsys.EnsureNodeForTest(ctx, fileEntry)
	if fileNode == nil {
		t.Fatal("expected file node")
	}
	handleAny, _, errno := fileNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open file failed: %v", errno)
	}
	handle, ok := handleAny.(*fusefs.TestHandle)
	if !ok {
		t.Fatalf("unexpected handle type: %T", handleAny)
	}
	if written, errno := handle.Write(ctx, []byte("FUSE"), 6); errno != 0 || written != 4 {
		t.Fatalf("write failed: written=%d errno=%v", written, errno)
	}
	if errno := handle.Flush(ctx); errno != 0 {
		t.Fatalf("flush failed: %v", errno)
	}
	// With writeback caching, close(2)-time Flush may be the only
	// durability signal the kernel sends, so Flush commits the dirty
	// overlay. The remote file is therefore already updated here; Fsync
	// below is an idempotent second commit.
	postFlush := filepath.Join(t.TempDir(), "fuse-postflush.out")
	if err := hub.DownloadFileContext(ctx, "project-fuse", "docs/file.txt", postFlush); err != nil {
		t.Fatalf("download after flush: %v", err)
	}
	assertFileContent(t, postFlush, []byte("hello FUSEd"))
	if errno := handle.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync failed: %v", errno)
	}
	output := filepath.Join(t.TempDir(), "fuse.out")
	if err := hub.DownloadFileContext(ctx, "project-fuse", "docs/file.txt", output); err != nil {
		t.Fatalf("download after fuse write: %v", err)
	}
	assertFileContent(t, output, []byte("hello FUSEd"))
	if errno := fileNode.Setxattr(ctx, "user.cache", []byte("warm"), 0); errno != 0 {
		t.Fatalf("setxattr via node failed: %v", errno)
	}
	size, errno := fileNode.Listxattr(ctx, nil)
	if errno != 0 {
		t.Fatalf("listxattr size failed: %v", errno)
	}
	buf := make([]byte, size)
	if _, errno := fileNode.Listxattr(ctx, buf); errno != 0 {
		t.Fatalf("listxattr payload failed: %v", errno)
	}
	if string(bytes.TrimRight(buf, "\x00")) != "user.cache" {
		t.Fatalf("unexpected xattr list payload: %q", buf)
	}
	getBuf := make([]byte, 16)
	n, errno := fileNode.Getxattr(ctx, "user.cache", getBuf)
	if errno != 0 {
		t.Fatalf("getxattr failed: %v", errno)
	}
	if string(getBuf[:n]) != "warm" {
		t.Fatalf("unexpected xattr data: %q", getBuf[:n])
	}
	lock := &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_WRLCK}
	if errno := handle.Setlk(ctx, 1, lock, 0); errno != 0 {
		t.Fatalf("setlk failed: %v", errno)
	}
	var outLock fuse.FileLock
	if errno := handle.Getlk(ctx, 2, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_WRLCK}, 0, &outLock); errno != 0 {
		t.Fatalf("getlk failed: %v", errno)
	}
	if outLock.Typ != syscall.F_WRLCK {
		t.Fatalf("expected active write lock, got %+v", outLock)
	}
	if errno := handle.Setlk(ctx, 1, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_UNLCK}, 0); errno != 0 {
		t.Fatalf("unlock failed: %v", errno)
	}
	if errno := fileNode.Removexattr(ctx, "user.cache"); errno != 0 {
		t.Fatalf("removexattr failed: %v", errno)
	}
	var createOut fuse.EntryOut
	createdInode, createHandleAny, _, errno := docsNode.Create(ctx, "created.txt", syscall.O_RDWR, 0o640, &createOut)
	if errno != 0 {
		t.Fatalf("create failed: %v", errno)
	}
	_ = createdInode
	createdHandle := createHandleAny.(*fusefs.TestHandle)
	if written, errno := createdHandle.Write(ctx, []byte("created"), 0); errno != 0 || written != 7 {
		t.Fatalf("write created file failed: written=%d errno=%v", written, errno)
	}
	if errno := createdHandle.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync created file failed: %v", errno)
	}
	if errno := createdHandle.Release(ctx); errno != 0 {
		t.Fatalf("release created file failed: %v", errno)
	}
	if _, errno := docsNode.Symlink(ctx, "docs/file.txt", "link.txt", &createOut); errno != 0 {
		t.Fatalf("node symlink failed: %v", errno)
	}
	_, errno = docsNode.Lookup(ctx, "link.txt", &createOut)
	if errno != 0 {
		t.Fatalf("lookup symlink failed: %v", errno)
	}
	linkEntry, err := hub.StatPathContext(ctx, "project-fuse", "docs/link.txt")
	if err != nil {
		t.Fatalf("stat link: %v", err)
	}
	linkNode := fsys.EnsureNodeForTest(ctx, linkEntry)
	target, errno := linkNode.Readlink(ctx)
	if errno != 0 || string(target) != "docs/file.txt" {
		t.Fatalf("unexpected readlink result: target=%q errno=%v", target, errno)
	}
	if _, errno := docsNode.Link(ctx, fileNode, "hard.txt", &createOut); errno != 0 {
		t.Fatalf("node hard link failed: %v", errno)
	}
	if errno := docsNode.Rename(ctx, "created.txt", docsNode, "file.txt", 0); errno != 0 {
		t.Fatalf("rename with replace failed: %v", errno)
	}
	replaced := filepath.Join(t.TempDir(), "replaced.txt")
	if err := hub.DownloadFileContext(ctx, "project-fuse", "docs/file.txt", replaced); err != nil {
		t.Fatalf("download replaced file: %v", err)
	}
	assertFileContent(t, replaced, []byte("created"))
	if errno := docsNode.Unlink(ctx, "hard.txt"); errno != 0 {
		t.Fatalf("unlink hard link failed: %v", errno)
	}
	var attrOut fuse.AttrOut
	if errno := docsNode.Getattr(ctx, nil, &attrOut); errno != 0 {
		t.Fatalf("getattr docs failed: %v", errno)
	}
	if attrOut.Ino == 0 || attrOut.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("unexpected directory attr: %+v", attrOut.Attr)
	}
	var statfs fuse.StatfsOut
	if errno := fsys.RootNode().Statfs(ctx, &statfs); errno != 0 {
		t.Fatalf("statfs failed: %v", errno)
	}
	if statfs.Files == 0 || statfs.Bsize == 0 {
		t.Fatalf("unexpected statfs result: %+v", statfs)
	}
	if _, errno := docsNode.Mkdir(ctx, "subdir", 0o755, &createOut); errno != 0 {
		t.Fatalf("mkdir via node failed: %v", errno)
	}
	if errno := docsNode.Rmdir(ctx, "subdir"); errno != 0 {
		t.Fatalf("rmdir via node failed: %v", errno)
	}
	if errno := handle.Release(ctx); errno != 0 {
		t.Fatalf("release handle failed: %v", errno)
	}
}

func TestFUSEOptionalMountLifecycle(t *testing.T) {
	requireEnvFlag(t, "STORHUB_RUN_FUSE")
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("/dev/fuse unavailable")
	}
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "mount.txt", []byte("mounted"))
	if _, err := hub.UploadFile("project-fuse-mount", "mount.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	fsys, err := hub.NewFUSE("project-fuse-mount", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}
	if err := fsys.Mount(mountPoint); err != nil {
		t.Fatalf("mount fuse fs: %v", err)
	}
	mountedPath := filepath.Join(mountPoint, "mount.txt")
	data, err := os.ReadFile(mountedPath)
	if err != nil {
		t.Fatalf("read mounted file: %v", err)
	}
	if string(data) != "mounted" {
		t.Fatalf("unexpected mounted file content: %q", data)
	}
	newFile := filepath.Join(mountPoint, "created.txt")
	createdHandle, err := os.OpenFile(newFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o640)
	if err != nil {
		t.Fatalf("open mounted file for write: %v", err)
	}
	if _, err := createdHandle.Write([]byte("created via mount")); err != nil {
		t.Fatalf("write mounted file: %v", err)
	}
	if err := createdHandle.Sync(); err != nil {
		t.Fatalf("sync mounted file: %v", err)
	}
	if err := createdHandle.Close(); err != nil {
		t.Fatalf("close mounted file: %v", err)
	}
	if got, err := os.ReadFile(newFile); err != nil || string(got) != "created via mount" {
		t.Fatalf("read created mounted file: got=%q err=%v", got, err)
	}
	hardPath := filepath.Join(mountPoint, "hard.txt")
	if err := os.Link(newFile, hardPath); err != nil {
		t.Fatalf("create hardlink on mount: %v", err)
	}
	linkPath := filepath.Join(mountPoint, "sym.txt")
	if err := os.Symlink("created.txt", linkPath); err != nil {
		t.Fatalf("create symlink on mount: %v", err)
	}
	if target, err := os.Readlink(linkPath); err != nil || target != "created.txt" {
		t.Fatalf("read mounted symlink: target=%q err=%v", target, err)
	}
	renamedPath := filepath.Join(mountPoint, "renamed.txt")
	if err := os.Rename(newFile, renamedPath); err != nil {
		t.Fatalf("rename mounted file: %v", err)
	}
	if err := os.Chmod(renamedPath, 0o600); err != nil {
		t.Fatalf("chmod mounted file: %v", err)
	}
	if err := os.WriteFile(hardPath, []byte("hardlink update"), 0o600); err != nil {
		t.Fatalf("write through mounted hardlink: %v", err)
	}
	if got, err := os.ReadFile(renamedPath); err != nil || string(got) != "hardlink update" {
		t.Fatalf("expected hardlink content reflection: got=%q err=%v", got, err)
	}
	if err := os.Chmod(renamedPath, 0o000); err != nil {
		t.Fatalf("chmod mounted file to 000: %v", err)
	}
	if _, err := os.ReadFile(renamedPath); err == nil {
		t.Fatal("expected mounted permission denial after chmod 000")
	}
	if err := os.Chmod(renamedPath, 0o600); err != nil {
		t.Fatalf("restore chmod mounted file: %v", err)
	}
	info, err := os.Lstat(renamedPath)
	if err != nil {
		t.Fatalf("lstat renamed file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("unexpected mounted mode: %o", info.Mode().Perm())
	}
	if err := testSetUserXattr(renamedPath, "user.mount", []byte("warm")); err == nil {
		got, err := testGetUserXattr(renamedPath, "user.mount")
		if err != nil {
			t.Fatalf("get mounted xattr: %v", err)
		}
		if string(got) != "warm" {
			t.Fatalf("unexpected mounted xattr: %q", got)
		}
	}
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("unmount fuse fs: %v", err)
	}
}

func TestFUSECloseIdempotent(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	fsys, err := hub.NewFUSE("project-fuse-close", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestFUSEHandleRenameAndUnlinkSemantics(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "project-fuse-semantics", "dir"); err != nil {
		t.Fatalf("mkdir dir: %v", err)
	}
	if err := hub.MkdirContext(ctx, "project-fuse-semantics", "dir/sub"); err != nil {
		t.Fatalf("mkdir dir/sub: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "semantics.txt", []byte("payload"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse-semantics", "dir/sub/file.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	fsys, err := hub.NewFUSE("project-fuse-semantics", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	dirEntry, err := hub.StatPathContext(ctx, "project-fuse-semantics", "dir")
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	dirNode := fsys.EnsureNodeForTest(ctx, dirEntry)
	var out fuse.EntryOut
	_, errno := dirNode.Lookup(ctx, "sub", &out)
	if errno != 0 {
		t.Fatalf("lookup sub: %v", errno)
	}
	fileEntry, err := hub.StatPathContext(ctx, "project-fuse-semantics", "dir/sub/file.txt")
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	fileNode := fsys.EnsureNodeForTest(ctx, fileEntry)
	hAny, _, errno := fileNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open file: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	if errno := fsys.RootNode().Rename(ctx, "dir", fsys.RootNode(), "renamed", 0); errno != 0 {
		t.Fatalf("rename dir: %v", errno)
	}
	if written, errno := h.Write(ctx, []byte("R"), 0); errno != 0 || written != 1 {
		t.Fatalf("write after rename: written=%d errno=%v", written, errno)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync after rename: %v", errno)
	}
	output := filepath.Join(t.TempDir(), "renamed-semantic.txt")
	if err := hub.DownloadFileContext(ctx, "project-fuse-semantics", "renamed/sub/file.txt", output); err != nil {
		t.Fatalf("download renamed path: %v", err)
	}
	assertFileContent(t, output, []byte("Rayload"))
	if _, err := hub.StatPathContext(ctx, "project-fuse-semantics", "dir/sub/file.txt"); err == nil {
		t.Fatal("expected old path to be gone after rename")
	}
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release renamed handle: %v", errno)
	}
	unlinkEntry, err := hub.StatPathContext(ctx, "project-fuse-semantics", "renamed/sub/file.txt")
	if err != nil {
		t.Fatalf("stat unlink file: %v", err)
	}
	unlinkNode := fsys.EnsureNodeForTest(ctx, unlinkEntry)
	hAny, _, errno = unlinkNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open unlink file: %v", errno)
	}
	h = hAny.(*fusefs.TestHandle)
	parentEntry, err := hub.StatPathContext(ctx, "project-fuse-semantics", "renamed/sub")
	if err != nil {
		t.Fatalf("stat parent dir: %v", err)
	}
	parentNode := fsys.EnsureNodeForTest(ctx, parentEntry)
	if errno := parentNode.Unlink(ctx, "file.txt"); errno != 0 {
		t.Fatalf("unlink open file: %v", errno)
	}
	if written, errno := h.Write(ctx, []byte("gone"), 0); errno != 0 || written != 4 {
		t.Fatalf("write after unlink: written=%d errno=%v", written, errno)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync after unlink: %v", errno)
	}
	readBuf := make([]byte, 4)
	res, errno := h.Read(ctx, readBuf, 0)
	if errno != 0 {
		t.Fatalf("read after unlink: %v", errno)
	}
	readData, status := res.Bytes(readBuf)
	if status != 0 {
		t.Fatalf("read result bytes: %v", status)
	}
	if string(readData) != "gone" {
		t.Fatalf("unexpected read after unlink: %q", readData)
	}
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release unlinked handle: %v", errno)
	}
	if _, err := hub.StatPathContext(ctx, "project-fuse-semantics", "renamed/sub/file.txt"); err == nil {
		t.Fatal("expected unlinked file to remain absent after release")
	}
}

func TestFUSEReadOnlyHandleSurvivesPathLoss(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "project-fuse-readonly-loss", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	oldPath := writeTempFile(t, t.TempDir(), "old.txt", []byte("old-data"))
	newPath := writeTempFile(t, t.TempDir(), "new.txt", []byte("new-data"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse-readonly-loss", "docs/victim.txt", oldPath); err != nil {
		t.Fatalf("upload victim: %v", err)
	}
	if _, err := hub.UploadFileContext(ctx, "project-fuse-readonly-loss", "docs/replacement.txt", newPath); err != nil {
		t.Fatalf("upload replacement: %v", err)
	}
	fsys, err := hub.NewFUSE("project-fuse-readonly-loss", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	victimEntry, err := hub.StatPathContext(ctx, "project-fuse-readonly-loss", "docs/victim.txt")
	if err != nil {
		t.Fatalf("stat victim: %v", err)
	}
	victimNode := fsys.EnsureNodeForTest(ctx, victimEntry)
	roAny, _, errno := victimNode.Open(ctx, syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("open readonly victim: %v", errno)
	}
	ro := roAny.(*fusefs.TestHandle)
	docsEntry, err := hub.StatPathContext(ctx, "project-fuse-readonly-loss", "docs")
	if err != nil {
		t.Fatalf("stat docs: %v", err)
	}
	docsNode := fsys.EnsureNodeForTest(ctx, docsEntry)
	if errno := docsNode.Unlink(ctx, "victim.txt"); errno != 0 {
		t.Fatalf("unlink victim: %v", errno)
	}
	buf := make([]byte, 16)
	res, errno := ro.Read(ctx, buf, 0)
	if errno != 0 {
		t.Fatalf("read readonly unlinked handle: %v", errno)
	}
	got, status := res.Bytes(buf)
	if status != 0 {
		t.Fatalf("bytes readonly unlinked handle: %v", status)
	}
	if string(got) != "old-data" {
		t.Fatalf("unexpected readonly unlinked data: %q", got)
	}
	if errno := ro.Release(ctx); errno != 0 {
		t.Fatalf("release readonly unlinked handle: %v", errno)
	}

	if _, err := hub.UploadFileContext(ctx, "project-fuse-readonly-loss", "docs/victim.txt", oldPath); err != nil {
		t.Fatalf("re-upload victim: %v", err)
	}
	victimEntry, err = hub.StatPathContext(ctx, "project-fuse-readonly-loss", "docs/victim.txt")
	if err != nil {
		t.Fatalf("restat victim: %v", err)
	}
	// The re-upload minted a fresh inode for the recreated file; drop the
	// mount's cached node for the old identity the same way a kernel
	// re-Lookup would after invalidation.
	fsys.ResetNodeForTest("docs/victim.txt")
	victimNode = fsys.EnsureNodeForTest(ctx, victimEntry)
	roAny, _, errno = victimNode.Open(ctx, syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("reopen readonly victim: %v", errno)
	}
	ro = roAny.(*fusefs.TestHandle)
	if errno := docsNode.Rename(ctx, "replacement.txt", docsNode, "victim.txt", 0); errno != 0 {
		t.Fatalf("rename replacement over victim: %v", errno)
	}
	res, errno = ro.Read(ctx, buf, 0)
	// Pinned reads are deterministic: the handle captured its metadata
	// snapshot at open, so a rename over the path cannot change what this
	// returns. No settling window exists anymore.
	if errno != 0 {
		t.Fatalf("read readonly replaced handle: %v", errno)
	}
	got = mustBytes(t, res, buf)
	if string(got) != "old-data" {
		t.Fatalf("unexpected readonly replaced data: %q", got)
	}
	if errno := ro.Release(ctx); errno != 0 {
		t.Fatalf("release readonly replaced handle: %v", errno)
	}
	output := filepath.Join(t.TempDir(), "replaced-readonly.txt")
	if err := hub.DownloadFileContext(ctx, "project-fuse-readonly-loss", "docs/victim.txt", output); err != nil {
		t.Fatalf("download replaced victim: %v", err)
	}
	assertFileContent(t, output, []byte("new-data"))
}

func TestFUSEConcurrentWritableHandlesShareState(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "shared.txt", []byte("abcdefghij"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse-shared-writes", "shared.txt", input); err != nil {
		t.Fatalf("upload shared file: %v", err)
	}
	fsys, err := hub.NewFUSE("project-fuse-shared-writes", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "project-fuse-shared-writes", "shared.txt")
	if err != nil {
		t.Fatalf("stat shared file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	h1Any, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle1: %v", errno)
	}
	h2Any, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle2: %v", errno)
	}
	h1 := h1Any.(*fusefs.TestHandle)
	h2 := h2Any.(*fusefs.TestHandle)
	if written, errno := h1.Write(ctx, []byte("HELLO"), 0); errno != 0 || written != 5 {
		t.Fatalf("write handle1: written=%d errno=%v", written, errno)
	}
	if written, errno := h2.Write(ctx, []byte("WORLD"), 5); errno != 0 || written != 5 {
		t.Fatalf("write handle2: written=%d errno=%v", written, errno)
	}
	if errno := h1.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync handle1: %v", errno)
	}
	output := filepath.Join(t.TempDir(), "shared-writes.txt")
	if err := hub.DownloadFileContext(ctx, "project-fuse-shared-writes", "shared.txt", output); err != nil {
		t.Fatalf("download shared file: %v", err)
	}
	assertFileContent(t, output, []byte("HELLOWORLD"))
	if errno := h1.Release(ctx); errno != 0 {
		t.Fatalf("release handle1: %v", errno)
	}
	if errno := h2.Release(ctx); errno != 0 {
		t.Fatalf("release handle2: %v", errno)
	}
}

func TestFUSEPartialWritebackAvoidsFullMaterializeAndReupload(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			assetDownloadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "large.txt", []byte("abcdefghijklmnopqrstuvwx"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse-partial-writeback", "large.txt", input); err != nil {
		t.Fatalf("upload large file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("project-fuse-partial-writeback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "project-fuse-partial-writeback", "large.txt")
	if err != nil {
		t.Fatalf("stat large file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	if written, errno := h.Write(ctx, []byte("Z"), 10); errno != 0 || written != 1 {
		t.Fatalf("single-byte overwrite: written=%d errno=%v", written, errno)
	}
	if got := assetDownloadCalls.Load(); got != 0 {
		t.Fatalf("expected no asset download during open/write, got %d", got)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync partial overwrite: %v", errno)
	}
	if delta := uploadCalls.Load() - baselineUploads; delta != 1 {
		t.Fatalf("expected one uploaded patch chunk, got %d", delta)
	}
	output := filepath.Join(t.TempDir(), "partial-writeback.txt")
	if err := hub.DownloadFileContext(ctx, "project-fuse-partial-writeback", "large.txt", output); err != nil {
		t.Fatalf("download partially updated file: %v", err)
	}
	assertFileContent(t, output, []byte("abcdefghijZlmnopqrstuvwx"))
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release handle: %v", errno)
	}
}

func TestFUSEAppendWritebackUsesPatchPath(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			assetDownloadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "append.txt", []byte("abcdefgh"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse-append-writeback", "append.txt", input); err != nil {
		t.Fatalf("upload append file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("project-fuse-append-writeback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "project-fuse-append-writeback", "append.txt")
	if err != nil {
		t.Fatalf("stat append file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR|syscall.O_APPEND)
	if errno != 0 {
		t.Fatalf("open append handle: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	// The kernel VFS positions every O_APPEND write at EOF before the
	// FUSE_WRITE is issued (pwrite offsets are ignored for O_APPEND
	// fds), so the handler receives the end offset, not 0. Forcing a
	// sub-EOF offset to EOF here would corrupt merged writeback replays
	// (kernel 7 vs server 11 divergence); sub-EOF offsets are honored
	// so retransmits stay idempotent.
	if written, errno := h.Write(ctx, []byte("XYZ"), 8); errno != 0 || written != 3 {
		t.Fatalf("append write: written=%d errno=%v", written, errno)
	}
	if got := assetDownloadCalls.Load(); got != 0 {
		t.Fatalf("expected no asset download during append write, got %d", got)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync append: %v", errno)
	}
	if delta := uploadCalls.Load() - baselineUploads; delta != 1 {
		t.Fatalf("expected one uploaded append chunk, got %d", delta)
	}
	output := filepath.Join(t.TempDir(), "append-writeback.txt")
	if err := hub.DownloadFileContext(ctx, "project-fuse-append-writeback", "append.txt", output); err != nil {
		t.Fatalf("download appended file: %v", err)
	}
	assertFileContent(t, output, []byte("abcdefghXYZ"))
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release append handle: %v", errno)
	}
}

func TestFUSETruncateWritebackAvoidsUploads(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "truncate.txt", []byte("abcdefghijklmnop"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse-truncate-writeback", "truncate.txt", input); err != nil {
		t.Fatalf("upload truncate file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("project-fuse-truncate-writeback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "project-fuse-truncate-writeback", "truncate.txt")
	if err != nil {
		t.Fatalf("stat truncate file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open truncate handle: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = 5
	var out fuse.AttrOut
	if errno := node.Setattr(ctx, h, &attr, &out); errno != 0 {
		t.Fatalf("setattr truncate: %v", errno)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync truncate: %v", errno)
	}
	if delta := uploadCalls.Load() - baselineUploads; delta != 0 {
		t.Fatalf("expected truncate to avoid uploads, got %d", delta)
	}
	output := filepath.Join(t.TempDir(), "truncate-writeback.txt")
	if err := hub.DownloadFileContext(ctx, "project-fuse-truncate-writeback", "truncate.txt", output); err != nil {
		t.Fatalf("download truncated file: %v", err)
	}
	assertFileContent(t, output, []byte("abcde"))
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release truncate handle: %v", errno)
	}
}

func TestFUSERepeatedEditorStyleSaveCycles(t *testing.T) {
	t.Parallel()
	testFUSEEditorSaveCycle(t, "full-rewrite", func(original []byte) ([]byte, []byte) {
		first := append(append([]byte(nil), original...), 'X')
		second := append([]byte(nil), original...)
		return first, second
	})
}

func TestFUSERepeatedEditorStyleSaveCyclesWithoutSetattrHandle(t *testing.T) {
	t.Parallel()
	testFUSEEditorSaveCycleWithSetattrHandle(t, "full-rewrite-no-setattr-handle", false, func(original []byte) ([]byte, []byte) {
		first := append(append([]byte(nil), original...), 'X')
		second := append([]byte(nil), original...)
		return first, second
	})
}

func TestFUSEPartialEditorRewriteSaveCycles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutator func([]byte) ([]byte, []byte)
	}{
		{
			name: "rewrite-99-percent",
			mutator: func(original []byte) ([]byte, []byte) {
				first := append([]byte(nil), original...)
				first[len(first)-1] = 'Z'
				second := append([]byte(nil), original...)
				return first, second
			},
		},
		{
			name: "rewrite-80-percent",
			mutator: func(original []byte) ([]byte, []byte) {
				first := append([]byte(nil), original[:len(original)/5]...)
				first = append(first, bytes.Repeat([]byte("Q"), len(original)-len(original)/5)...)
				second := append([]byte(nil), original...)
				return first, second
			},
		},
		{
			name: "rewrite-50-percent",
			mutator: func(original []byte) ([]byte, []byte) {
				half := len(original) / 2
				first := append([]byte(nil), original[:half]...)
				first = append(first, bytes.Repeat([]byte("R"), len(original)-half)...)
				second := append([]byte(nil), original...)
				return first, second
			},
		},
		{
			name: "rewrite-40-percent",
			mutator: func(original []byte) ([]byte, []byte) {
				prefix := (len(original) * 3) / 5
				first := append([]byte(nil), original[:prefix]...)
				first = append(first, bytes.Repeat([]byte("S"), len(original)-prefix)...)
				second := append([]byte(nil), original...)
				return first, second
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFUSEEditorSaveCycle(t, tt.name, tt.mutator)
		})
	}
}

func testFUSEEditorSaveCycle(t *testing.T, projectSuffix string, mutator func([]byte) ([]byte, []byte)) {
	testFUSEEditorSaveCycleWithSetattrHandle(t, projectSuffix, true, mutator)
}

func testFUSEEditorSaveCycleWithSetattrHandle(t *testing.T, projectSuffix string, passHandleToSetattr bool, mutator func([]byte) ([]byte, []byte)) {
	t.Helper()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 4096, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()
	original := append(bytes.Repeat([]byte("A"), 4096), bytes.Repeat([]byte("B"), 4096)...)
	original = append(original, bytes.Repeat([]byte("C"), 4096)...)
	original = append(original, []byte("tail")...)
	input := writeTempFile(t, t.TempDir(), "editor.txt", original)
	project := "project-fuse-editor-cycles-" + projectSuffix
	if _, err := hub.UploadFileContext(ctx, project, "editor.txt", input); err != nil {
		t.Fatalf("upload editor file: %v", err)
	}
	fsys, err := hub.NewFUSE(project, fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, project, "editor.txt")
	if err != nil {
		t.Fatalf("stat editor file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	save := func(content []byte) {
		hAny, _, errno := node.Open(ctx, syscall.O_WRONLY)
		if errno != 0 {
			t.Fatalf("open editor handle: %v", errno)
		}
		h := hAny.(*fusefs.TestHandle)
		var attr fuse.SetAttrIn
		attr.Valid = fuse.FATTR_SIZE
		attr.Size = 0
		var out fuse.AttrOut
		var setattrHandle gofusefs.FileHandle
		if passHandleToSetattr {
			setattrHandle = h
		}
		if errno := node.Setattr(ctx, setattrHandle, &attr, &out); errno != 0 {
			t.Fatalf("truncate editor handle: %v", errno)
		}
		for offset := 0; offset < len(content); offset += 4096 {
			end := offset + 4096
			if end > len(content) {
				end = len(content)
			}
			part := content[offset:end]
			if written, errno := h.Write(ctx, part, int64(offset)); errno != 0 || written != uint32(len(part)) {
				t.Fatalf("write editor handle: written=%d errno=%v", written, errno)
			}
		}
		if errno := h.Fsync(ctx, 0); errno != 0 {
			t.Fatalf("fsync editor handle: %v", errno)
		}
		if errno := h.Release(ctx); errno != 0 {
			t.Fatalf("release editor handle: %v", errno)
		}
	}
	first, second := mutator(original)
	save(first)
	save(second)
	output := filepath.Join(t.TempDir(), projectSuffix+"-editor-cycles.txt")
	if err := hub.DownloadFileContext(ctx, project, "editor.txt", output); err != nil {
		t.Fatalf("download saved file: %v", err)
	}
	assertFileContent(t, output, second)
}

func TestFUSEFragmentedWritebackUploadsTouchedChunks(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var metadataWrites atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/.storhub/index.json") {
			metadataWrites.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			assetDownloadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, Config{
		ChunkSize:         8,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	})
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "fragmented.txt", []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse-fragmented-writeback", "fragmented.txt", input); err != nil {
		t.Fatalf("upload fragmented file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	baselineMetadataWrites := metadataWrites.Load()
	fsys, err := hub.NewFUSE("project-fuse-fragmented-writeback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "project-fuse-fragmented-writeback", "fragmented.txt")
	if err != nil {
		t.Fatalf("stat fragmented file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open fragmented handle: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	for i, off := range []int64{0, 8, 16, 24, 32, 40} {
		if written, errno := h.Write(ctx, []byte{byte('0' + i)}, off); errno != 0 || written != 1 {
			t.Fatalf("fragmented write %d: written=%d errno=%v", i, written, errno)
		}
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync fragmented writes: %v", errno)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	if delta := uploadCalls.Load() - baselineUploads; delta != 6 {
		t.Fatalf("expected six uploaded touched chunks, got %d", delta)
	}
	if delta := metadataWrites.Load() - baselineMetadataWrites; delta < 1 || delta > 6 {
		t.Fatalf("expected 1-6 metadata writes for 6 fragmented dirty ranges, got %d", delta)
	}
	if got := assetDownloadCalls.Load(); got > 8 {
		t.Fatalf("expected bounded base reads during fragmented write commit, got %d", got)
	}
	output := filepath.Join(t.TempDir(), "fragmented-writeback.txt")
	if err := hub.DownloadFileContext(ctx, "project-fuse-fragmented-writeback", "fragmented.txt", output); err != nil {
		t.Fatalf("download fragmented file: %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read fragmented output: %v", err)
	}
	for i, off := range []int64{0, 8, 16, 24, 32, 40} {
		if data[off] != byte('0'+i) {
			t.Fatalf("unexpected byte at %d: %q", off, data[off])
		}
	}
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release fragmented handle: %v", errno)
	}
}

func TestFUSEFlagsAndLocks(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "project-fuse-flags", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	first := writeTempFile(t, t.TempDir(), "first.txt", []byte("first"))
	second := writeTempFile(t, t.TempDir(), "second.txt", []byte("second"))
	if _, err := hub.UploadFileContext(ctx, "project-fuse-flags", "docs/a.txt", first); err != nil {
		t.Fatalf("upload a: %v", err)
	}
	if _, err := hub.UploadFileContext(ctx, "project-fuse-flags", "docs/b.txt", second); err != nil {
		t.Fatalf("upload b: %v", err)
	}
	fsys, err := hub.NewFUSE("project-fuse-flags", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	docsEntry, err := hub.StatPathContext(ctx, "project-fuse-flags", "docs")
	if err != nil {
		t.Fatalf("stat docs: %v", err)
	}
	docsNode := fsys.EnsureNodeForTest(ctx, docsEntry)
	if errno := docsNode.Rename(ctx, "a.txt", docsNode, "b.txt", 0x1); errno != syscall.EEXIST {
		t.Fatalf("expected rename noreplace to fail with EEXIST, got %v", errno)
	}
	if errno := docsNode.Rename(ctx, "a.txt", docsNode, "b.txt", 0x2); errno != syscall.EINVAL {
		t.Fatalf("expected rename exchange to fail with EINVAL, got %v", errno)
	}
	aEntry, err := hub.StatPathContext(ctx, "project-fuse-flags", "docs/a.txt")
	if err != nil {
		t.Fatalf("stat a: %v", err)
	}
	aNode := fsys.EnsureNodeForTest(ctx, aEntry)
	if errno := aNode.Setxattr(ctx, "user.flag", []byte("one"), 0x1); errno != 0 {
		t.Fatalf("setxattr create: %v", errno)
	}
	if errno := aNode.Setxattr(ctx, "user.flag", []byte("two"), 0x1); errno != syscall.EEXIST {
		t.Fatalf("expected xattr create conflict, got %v", errno)
	}
	if errno := aNode.Setxattr(ctx, "user.other", []byte("two"), 0x2); errno != syscall.ENODATA {
		t.Fatalf("expected xattr replace miss, got %v", errno)
	}
	if errno := aNode.Removexattr(ctx, "user.missing"); errno != syscall.ENODATA {
		t.Fatalf("expected removexattr miss, got %v", errno)
	}
	h1Any, _, errno := aNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle1: %v", errno)
	}
	h2Any, _, errno := aNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle2: %v", errno)
	}
	h1 := h1Any.(*fusefs.TestHandle)
	h2 := h2Any.(*fusefs.TestHandle)
	if errno := h1.Setlk(ctx, 1, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_RDLCK}, 0); errno != 0 {
		t.Fatalf("set read lock owner1: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_RDLCK}, 0); errno != 0 {
		t.Fatalf("set read lock owner2: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_WRLCK}, 0); errno != syscall.EAGAIN {
		t.Fatalf("expected write lock conflict, got %v", errno)
	}
	if errno := h1.Release(ctx); errno != 0 {
		t.Fatalf("release handle1: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_WRLCK}, 0); errno != 0 {
		t.Fatalf("upgrade own lock after release: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}, 0); errno != 0 {
		t.Fatalf("set broad write lock: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 3, End: 6, Typ: syscall.F_UNLCK}, 0); errno != 0 {
		t.Fatalf("partial unlock: %v", errno)
	}
	var conflict fuse.FileLock
	if errno := h2.Getlk(ctx, 3, &fuse.FileLock{Start: 4, End: 4, Typ: syscall.F_WRLCK}, 0, &conflict); errno != 0 {
		t.Fatalf("getlk unlocked middle: %v", errno)
	}
	if conflict.Typ != syscall.F_UNLCK {
		t.Fatalf("expected middle range to be unlocked, got %+v", conflict)
	}
	if errno := h2.Getlk(ctx, 3, &fuse.FileLock{Start: 2, End: 2, Typ: syscall.F_WRLCK}, 0, &conflict); errno != 0 {
		t.Fatalf("getlk locked prefix: %v", errno)
	}
	if conflict.Typ != syscall.F_WRLCK {
		t.Fatalf("expected prefix range to stay locked, got %+v", conflict)
	}
	if errno := h2.Getlk(ctx, 3, &fuse.FileLock{Start: 8, End: 8, Typ: syscall.F_WRLCK}, 0, &conflict); errno != 0 {
		t.Fatalf("getlk locked suffix: %v", errno)
	}
	if conflict.Typ != syscall.F_WRLCK {
		t.Fatalf("expected suffix range to stay locked, got %+v", conflict)
	}
	if errno := h2.Release(ctx); errno != 0 {
		t.Fatalf("release handle2: %v", errno)
	}
}

type mockGitHub struct {
	t      *testing.T
	server *mockServerRef
	mu     sync.Mutex
	owner  string
	repos  map[string]*mockRepo
	// intercept is a per-test hook consulted before routing (fault
	// injection, request counting). Reset in t.Cleanup via
	// resetMockMutables; prefer the named onContentsPUT/onAssetGET
	// helpers for new tests over raw path-substring pastes.
	intercept atomic.Value
	// faults groups every fault-injection switch (opt-in 429s, CDN
	// expiry/delay/618, header injector, truncation, collisions) away
	// from server state. All are per-test and reset in t.Cleanup.
	faults mockFaults
	// token is the bearer token expected on API routes (unique per test;
	// the shared server dispatches on it). The CDN host is exempt:
	// signed-URL fetches carry no Authorization header.
	token string
}

// mockFaults holds the mock's fault-injection switches. Each fault is
// opt-in per test and reset by resetMockMutables, so arming one never
// leaks into another test sharing the server socket.
type mockFaults struct {
	// embedCap simulates GitHub truncating the asset array embedded in
	// release objects. When > 0, list/get-release responses carry at most
	// embedCap embedded assets while the true count stays higher. The
	// dedicated list-assets endpoint always serves the true set.
	embedCap int
	// collideNext forces the next upload to repo/tag to fail once with a
	// 422 already_exists body, simulating a concurrent writer winning the
	// same asset name. Keyed "repo/tag". Test-only fault injection.
	collideNext map[string]bool
	// rateLimitOnce makes the next API response a 429 with Retry-After,
	// letting retry tests opt into rate-limit simulation without arming
	// the governor for the whole suite.
	rateLimitOnce atomic.Bool
	// rateLimitHeadersOnce makes the next API response carry exhausted
	// X-RateLimit-* headers (Remaining 0, Reset at mockNow+2s) instead of
	// a 429, exercising the governor's sustainable-pace wait on a 200.
	// NOTE: the governor paces against the client's cfg.Now clock, so
	// tests using this fault must pin faults.mockNow to that same clock.
	rateLimitHeadersOnce atomic.Bool
	// cdnSawAuth records whether any CDN fetch carried an Authorization
	// header (it must not: signed URLs are bearer credentials already).
	cdnSawAuth atomic.Bool
	// rateLimitServed counts opt-in 429s served, so retry tests can prove
	// the fault actually fired instead of inferring it from request counts.
	rateLimitServed atomic.Int32
	// cdnTTL (nanoseconds) makes signed CDN URLs expire: when > 0, the
	// octet-stream redirect carries jwt/se expiries and the CDN 403s
	// expired fetches, exercising the client's SAS-rejection and
	// re-resolution path (real GitHub: Azure SAS URLs are short-lived).
	// Zero (default) keeps URLs immortal so unrelated tests never trip.
	// Use SetCDNTTL/LoadCDNTTL (time.Duration) instead of raw nanos.
	cdnTTL atomic.Int64
	// cdnTimeShift (nanoseconds) fast-forwards the CDN's expiry clock
	// exactly once (consumed by the next signed-URL check), so tests can
	// force "the cached URL is now stale" as an event instead of sleeping
	// past the TTL. The re-resolved URL must then serve normally, so the
	// shift must not linger.
	cdnTimeShift atomic.Int64
	// cdnFail618Once makes the next CDN fetch answer 618
	// (StatusSignedURLExpired: the front-door JWT died) once, exercising
	// the client's 618 re-resolution path. Consumed on fire.
	cdnFail618Once atomic.Bool
	// cdnDelay (nanoseconds) stalls every CDN body by the given duration,
	// simulating a slow stream without real latency elsewhere.
	cdnDelay atomic.Int64
	// mockNow (unix nanos) overrides the mock's clock; 0 means wall time.
	// Pin it to the client's frozen cfg.Now when a test needs TTL/reset
	// headers interpreted against test time instead of wall time.
	mockNow atomic.Int64
}

// SetCDNTTL arms signed-URL expiry; LoadCDNTTL reads it back.
func (f *mockFaults) SetCDNTTL(d time.Duration) { f.cdnTTL.Store(int64(d)) }

// LoadCDNTTL returns the armed TTL (0 = immortal URLs).
func (f *mockFaults) LoadCDNTTL() time.Duration { return time.Duration(f.cdnTTL.Load()) }

// armRateLimit arms one opt-in 429 for the next API call.
func (m *mockGitHub) armRateLimit() { m.faults.rateLimitOnce.Store(true) }

// expireCDNOnce fast-forwards the CDN expiry clock once (event, not sleep).
func (m *mockGitHub) expireCDNOnce(d time.Duration) { m.faults.cdnTimeShift.Store(int64(d)) }

type mockRepo struct {
	name          string
	private       bool
	nextReleaseID int64
	nextBlobID    int64
	nextCommitID  int64
	releasesByTag map[string]*mockRelease
	releasesByID  map[int64]*mockRelease
	assets        map[int64]*mockAsset
	// assetsByTag indexes assets per release tag (name uniqueness,
	// per-tag counts, and per-tag listings all read here instead of
	// scanning every asset). Maintained on upload/delete paths.
	assetsByTag map[string]map[int64]*mockAsset
	files       map[string]*mockFile
	// dirChildren indexes immediate children per directory path:
	// dirChildren[dir][name] = isDir. Maintained by putMockFileLocked /
	// deleteMockFileLocked so ListDir never scans every file.
	dirChildren   map[string]map[string]bool
	commitsByPath map[string][]mockCommit
	// commitTime indexes commit SHA -> commit time across every path, so
	// ?ref= resolution never scans every path's history.
	commitTime map[string]time.Time
}

type mockRelease struct {
	id        int64
	tag       string
	name      string
	uploadURL string
}

type mockAsset struct {
	id         int64
	name       string
	releaseTag string
	data       []byte
}

type mockFile struct {
	path string
	sha  string
	data []byte
}

type mockCommit struct {
	sha     string
	message string
	path    string
	data    []byte
	when    time.Time
	// deleted marks a contents-DELETE commit: the path existed in every
	// earlier commit and is gone from this one onward (GitHub records the
	// delete and 404s file reads at that ref).
	deleted bool
}

func newMockGitHub(t *testing.T) *mockGitHub {
	t.Helper()
	token := fmt.Sprintf("token-%d", sharedMockSeq.Add(1))
	backend := &mockGitHub{
		t:     t,
		owner: "storhub-tester",
		repos: make(map[string]*mockRepo),
		token: token,
	}
	backend.faults.collideNext = make(map[string]bool)
	backend.server = &mockServerRef{URL: sharedMockBaseURL(t), token: token}
	registerMockBackend(token, backend)
	t.Cleanup(func() {
		backend.resetMockMutables()
		backend.server.Close()
	})
	return backend
}

func (m *mockGitHub) newClient(t *testing.T, cfg Config) *StorHub {
	t.Helper()
	cfg.APIBaseURL = m.server.URL
	cfg.HTTPClient = m.server.Client()
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Unix(1700000000, 0).UTC() }
	}
	if cfg.Sleep == nil {
		cfg.Sleep = func(_ context.Context, _ time.Duration) error { return nil }
	}
	hub, err := NewStorHubWithContext(context.Background(), m.token, cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() {
		_ = hub.Shutdown(context.Background())
	})
	return hub
}

// computeGitBlobSHA is intentionally implemented LOCALLY (sha1 over the
// "blob <len>\0" header + bytes), byte-identical to the production client's
// computation in internal/github/client.go:710. It must NOT be imported or
// re-exported from the github package: the mock has to stay an independent
// oracle, so a regression in the shared helper fails the cross-pin test
// below instead of passing vacuously on both sides.
func computeGitBlobSHA(data []byte) string {
	header := fmt.Sprintf("blob %d\x00", len(data))
	h := sha1.New()
	h.Write([]byte(header))
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (m *mockGitHub) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if fn, ok := m.intercept.Load().(func(http.ResponseWriter, *http.Request) bool); ok && fn != nil && fn(w, r) {
		return
	}
	// Mirror GitHub: every API route requires a bearer token. The CDN
	// host is exempt: signed-URL fetches carry no Authorization header.
	if !strings.HasPrefix(r.URL.Path, "/cdn/") && r.Header.Get("Authorization") != "Bearer "+m.authToken() {
		m.writeJSON(w, http.StatusUnauthorized, map[string]any{"message": "Requires authentication"})
		return
	}
	// Opt-in rate-limit fault for retry tests. One 429 with
	// Retry-After, then normal service resumes. Scoped to authenticated
	// API routes: a CDN fetch or an unauthenticated probe must never
	// spend the fault armed for the next API call.
	if !strings.HasPrefix(r.URL.Path, "/cdn/") && m.faults.rateLimitOnce.CompareAndSwap(true, false) {
		m.faults.rateLimitServed.Add(1)
		w.Header().Set("Retry-After", "1")
		m.writeJSON(w, http.StatusTooManyRequests, map[string]any{"message": "API rate limit exceeded for user ID"})
		return
	}
	// Opt-in exhausted-budget headers for governor tests. The next API
	// response carries Remaining 0 with Reset at mockNow+2s (a 200, not a
	// 429), so the governor's sustainable-pace wait engages on the
	// FOLLOWING requests. Scoped to API routes like the 429 fault; the
	// CDN never sends rate-limit headers (its authority has no budget).
	// NOTE: the governor paces against the client's cfg.Now clock, so pin
	// faults.mockNow to that clock or the reset lands years off.
	if !strings.HasPrefix(r.URL.Path, "/cdn/") && m.faults.rateLimitHeadersOnce.CompareAndSwap(true, false) {
		w.Header().Set("X-RateLimit-Limit", "60")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(m.mockTime().Add(2*time.Second).Unix(), 10))
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/user":
		m.writeJSON(w, http.StatusOK, map[string]any{"login": m.owner})
	case r.Method == http.MethodPost && r.URL.Path == "/user/repos":
		m.handleCreateRepo(w, r)
	case strings.HasPrefix(r.URL.Path, "/cdn/"):
		m.handleCDN(w, r)
	case strings.HasPrefix(r.URL.Path, "/repos/"):
		m.handleRepos(w, r)
	case strings.HasPrefix(r.URL.Path, "/upload/"):
		m.handleUpload(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (m *mockGitHub) authToken() string {
	if m.token == "" {
		return "token"
	}
	return m.token
}

// mockTime is the mock's clock: faults.mockNow when pinned, wall time
// otherwise. CDN expiries and rate-limit resets are minted from it so tests
// can freeze time instead of racing the wall clock.
func (m *mockGitHub) mockTime() time.Time {
	if nanos := m.faults.mockNow.Load(); nanos != 0 {
		return time.Unix(0, nanos).UTC()
	}
	return time.Now().UTC()
}

// mockCommitSHA mints a 40-hex commit SHA like real GitHub (sha1 of the
// counter), never the old "commit-%d" shape: a future client-side hex check
// must fail loudly on the mock exactly as it would in prod.
func mockCommitSHA(counter int64) string {
	sum := sha1.Sum([]byte(fmt.Sprintf("commit-%d", counter)))
	return fmt.Sprintf("%x", sum)
}

// mockTestJWT mints an unsigned test JWT carrying only the exp claim,
// mirroring the front-door token shape in
// internal/github/client_test.go:testSignedAssetURL (payload exp is real;
// sig is never verified, by design on both sides).
func mockTestJWT(exp time.Time) string {
	enc := func(v any) string {
		raw, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return enc(map[string]string{"alg": "none", "typ": "JWT"}) + "." +
		enc(map[string]int64{"exp": exp.Unix()}) + ".testsig"
}

// mockJWTExpiry decodes the exp claim from a compact JWS without verifying
// the signature (mirrors the client's jwtExpiry: expiry is public metadata).
func mockJWTExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp <= 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Exp, 0).UTC(), true
}

// mockSignedURLExpiry returns the earliest expiry carried by a signed CDN
// URL: the minimum of the front-door JWT exp and the backing SAS se
// (mirrors the client's signedURLExpiry min()+margin contract). Legacy
// ?exp= (unix nanos) URLs are honored for back-compat.
func mockSignedURLExpiry(query url.Values) (time.Time, bool) {
	var earliest time.Time
	found := false
	consider := func(t time.Time, ok bool) {
		if ok && (!found || t.Before(earliest)) {
			earliest, found = t, true
		}
	}
	consider(mockJWTExpiry(query.Get("jwt")))
	if se := query.Get("se"); se != "" {
		if exp, err := time.Parse(time.RFC3339, se); err == nil {
			consider(exp, true)
		}
	}
	if exp := query.Get("exp"); exp != "" {
		if nanos, err := strconv.ParseInt(exp, 10, 64); err == nil {
			consider(time.Unix(0, nanos).UTC(), true)
		}
	}
	return earliest, found
}

// mintSignedCDNURL builds the redirect target for an octet-stream asset GET:
// a /cdn/<id> URL carrying BOTH real expiries (front-door jwt with a short
// TTL, backing SAS se 10 minutes later), exactly the shape the production
// client parses via signedURLExpiry. The JWT governs: the client's cache
// lapses at the earlier expiry minus margin, never at the SAS alone.
// defaultMockCDNTTL is the signed-URL lifetime minted when a test does not
// arm cdnTTL: URLs always carry both real expiries (front-door jwt, backing
// SAS se) like production, so the client's proactive min(jwt,se)-30s cache
// path is exercised on every download, not just TTL-pinned tests. An hour
// keeps test traffic far from expiry without wall-clock dependence.
const defaultMockCDNTTL = time.Hour

func (m *mockGitHub) mintSignedCDNURL(assetID int64) string {
	base := fmt.Sprintf("%s/cdn/%d", m.server.URL, assetID)
	ttl := m.faults.LoadCDNTTL()
	if ttl <= 0 {
		ttl = defaultMockCDNTTL
	}
	// JWT exp is whole-second precision: round the short TTL UP to a
	// strictly-future second so a 50ms TTL is not born expired by
	// truncation (a truncated-down exp lands in the past and the
	// first fetch 403s). The se backs it 10 minutes later.
	jwtExp := m.mockTime().Add(ttl).Truncate(time.Second)
	if !jwtExp.After(m.mockTime()) {
		jwtExp = jwtExp.Add(time.Second)
	}
	q := url.Values{}
	q.Set("jwt", mockTestJWT(jwtExp))
	q.Set("se", jwtExp.Add(10*time.Minute).UTC().Format(time.RFC3339))
	return base + "?" + q.Encode()
}

// findAssetLocked resolves a global asset ID to its record. Caller holds
// m.mu. Asset IDs are globally unique (globalMockAssetID), so no repo scan
// ambiguity exists; the shared-server CDN router uses this per backend.
func (m *mockGitHub) findAssetLocked(id int64) (*mockAsset, bool) {
	for _, repo := range m.repos {
		if asset := repo.assets[id]; asset != nil {
			return asset, true
		}
	}
	return nil, false
}

func (m *mockGitHub) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Name    string `json:"name"`
		Private bool   `json:"private"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.repos[payload.Name]; exists {
		m.writeAlreadyExists(w, "Repository", "name", payload.Name)
		return
	}
	m.repos[payload.Name] = &mockRepo{
		name:          payload.Name,
		private:       payload.Private,
		nextReleaseID: 1,
		nextBlobID:    1,
		nextCommitID:  1,
		releasesByTag: make(map[string]*mockRelease),
		releasesByID:  make(map[int64]*mockRelease),
		assets:        make(map[int64]*mockAsset),
		assetsByTag:   make(map[string]map[int64]*mockAsset),
		files:         make(map[string]*mockFile),
		dirChildren:   make(map[string]map[string]bool),
		commitsByPath: make(map[string][]mockCommit),
		commitTime:    make(map[string]time.Time),
	}
	m.writeJSON(w, http.StatusCreated, map[string]any{"name": payload.Name})
}

// putMockFileLocked stores file bytes under the real git blob SHA and
// maintains the dirChildren index. Caller holds m.mu. Every repo.files
// write must go through here (or deleteMockFileLocked) so the directory
// index never drifts from the file map.
func (m *mockGitHub) putMockFileLocked(repo *mockRepo, filePath string, data []byte) *mockFile {
	_ = m
	file := repo.files[filePath]
	if file == nil {
		file = &mockFile{path: filePath}
		repo.files[filePath] = file
	}
	file.sha = computeGitBlobSHA(data)
	file.data = append([]byte(nil), data...)
	indexMockDirChild(repo, filePath)
	return file
}

// deleteMockFileLocked removes a file and prunes now-empty ancestor index
// entries. Caller holds m.mu.
func (m *mockGitHub) deleteMockFileLocked(repo *mockRepo, filePath string) {
	delete(repo.files, filePath)
	unindexMockDirChild(repo, filePath)
}

// indexMockDirChild records filePath's leaf and every ancestor directory in
// dirChildren: dirChildren[dir][name] reports whether name is a directory.
// A directory entry always wins over a file entry of the same name.
func indexMockDirChild(repo *mockRepo, filePath string) {
	segments := strings.Split(filePath, "/")
	for i := range segments {
		dir := strings.Join(segments[:i], "/")
		child := segments[i]
		isDir := i < len(segments)-1
		if repo.dirChildren == nil {
			repo.dirChildren = make(map[string]map[string]bool)
		}
		kids := repo.dirChildren[dir]
		if kids == nil {
			kids = make(map[string]bool)
			repo.dirChildren[dir] = kids
		}
		kids[child] = kids[child] || isDir
	}
}

// unindexMockDirChild drops filePath's leaf, then prunes ancestors left
// childless (a directory with no children does not exist on GitHub).
func unindexMockDirChild(repo *mockRepo, filePath string) {
	segments := strings.Split(filePath, "/")
	if len(segments) == 0 {
		return
	}
	dir := strings.Join(segments[:len(segments)-1], "/")
	if kids := repo.dirChildren[dir]; kids != nil {
		delete(kids, segments[len(segments)-1])
	}
	// Walk up: a directory whose index entry is now empty vanishes, which
	// may empty its own parent in turn.
	for d := dir; d != ""; {
		if kids := repo.dirChildren[d]; kids != nil && len(kids) == 0 {
			delete(repo.dirChildren, d)
		} else {
			break
		}
		parent, base := parentDirBase(d)
		if kids := repo.dirChildren[parent]; kids != nil {
			delete(kids, base)
		}
		d = parent
	}
}

func parentDirBase(dir string) (parent, base string) {
	if i := strings.LastIndex(dir, "/"); i >= 0 {
		return dir[:i], dir[i+1:]
	}
	return "", dir
}

// recordMockCommitLocked appends a newest-first commit on filePath's history
// with a 40-hex SHA and maintains the commitTime index. Caller holds m.mu.
func (m *mockGitHub) recordMockCommitLocked(repo *mockRepo, filePath, message string, data []byte, deleted bool) string {
	_ = m
	commitSHA := mockCommitSHA(repo.nextCommitID)
	repo.nextCommitID++
	when := time.Unix(1700000000+repo.nextCommitID, 0).UTC()
	repo.commitsByPath[filePath] = append([]mockCommit{{
		sha: commitSHA, message: message, path: filePath,
		data: append([]byte(nil), data...), when: when, deleted: deleted,
	}}, repo.commitsByPath[filePath]...)
	if repo.commitTime == nil {
		repo.commitTime = make(map[string]time.Time)
	}
	repo.commitTime[commitSHA] = when
	return commitSHA
}

func (m *mockGitHub) handleRepos(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "repos" || parts[1] != m.owner {
		http.NotFound(w, r)
		return
	}
	repo := m.repo(parts[2])
	if repo == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "repo not found"})
		return
	}

	switch {
	case len(parts) == 3 && r.Method == http.MethodGet:
		m.writeJSON(w, http.StatusOK, map[string]any{"name": repo.name})
	case len(parts) >= 5 && parts[3] == "contents":
		m.handleContents(w, r, repo, strings.Join(parts[4:], "/"))
	case len(parts) == 4 && parts[3] == "commits" && r.Method == http.MethodGet:
		m.handleListCommits(w, r, repo)
	case len(parts) == 4 && parts[3] == "releases" && r.Method == http.MethodPost:
		m.handleCreateRelease(w, r, repo)
	case len(parts) == 4 && parts[3] == "releases" && r.Method == http.MethodGet:
		m.handleListReleases(w, r, repo)
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "tags" && r.Method == http.MethodGet:
		m.handleGetReleaseByTag(w, repo, parts[5])
	case len(parts) == 5 && parts[3] == "releases" && r.Method == http.MethodDelete:
		m.handleDeleteRelease(w, repo, parts[4])
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "assets" && r.Method == http.MethodGet:
		m.handleDownloadAsset(w, r, repo, parts[5])
	case len(parts) == 6 && parts[3] == "releases" && parts[5] == "assets" && r.Method == http.MethodGet:
		m.handleListReleaseAssets(w, r, repo, parts[4])
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "assets" && r.Method == http.MethodDelete:
		m.handleDeleteAsset(w, repo, parts[5])
	case len(parts) == 3 && r.Method == http.MethodDelete:
		m.handleDeleteRepo(w, parts[2])
	default:
		http.NotFound(w, r)
	}
}

func (m *mockGitHub) handleContents(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	cleanPath := strings.TrimPrefix(filePath, "/")
	switch r.Method {
	case http.MethodGet:
		m.handleGetContent(w, r, repo, cleanPath)
	case http.MethodPut:
		m.handlePutContent(w, r, repo, cleanPath)
	case http.MethodDelete:
		m.handleDeleteContent(w, r, repo, cleanPath)
	default:
		http.NotFound(w, r)
	}
}

// handleDeleteContent removes a file, verifying the caller's blob sha matches
// (GitHub's optimistic-concurrency precondition for deletes).
func (m *mockGitHub) handleDeleteContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	var payload struct {
		Message string `json:"message"`
		SHA     string `json:"sha"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	// Mirror GitHub: contents DELETE requires both a commit message and
	// the current blob sha; either missing is a 422, never a silent delete.
	if payload.Message == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"message\" wasn't supplied."})
		return
	}
	if payload.SHA == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"sha\" wasn't supplied."})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := repo.files[filePath]
	if current == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	if payload.SHA != current.sha {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "sha does not match"})
		return
	}
	delete(repo.files, filePath)
	unindexMockDirChild(repo, filePath)
	// Mirror GitHub: the delete is itself a commit on the path's history
	// (revision lists after a delete must show it), and the path is gone
	// from that commit onward.
	commitSHA := m.recordMockCommitLocked(repo, filePath, payload.Message, nil, true)
	m.writeJSON(w, http.StatusOK, map[string]any{"commit": map[string]any{"sha": commitSHA}})
}

func (m *mockGitHub) handleGetContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ref := r.URL.Query().Get("ref")

	// Check if client wants raw content
	acceptRaw := r.Header.Get("Accept") == "application/vnd.github.raw"

	if ref != "" && ref != "HEAD" && ref != defaultBranch {
		// Real GitHub resolves ?ref= against the commit graph and 404s a
		// ref it does not know; it never silently serves HEAD bytes for an
		// unknown/stale SHA. "HEAD" and the default branch name
		// are valid refs and resolve to current state below.
		data, known, present := m.contentAtRefLocked(repo, filePath, ref)
		switch {
		case !known:
			m.writeJSON(w, http.StatusNotFound, map[string]any{"message": fmt.Sprintf("No commit found for SHA: %s", ref)})
			return
		case present:
			m.serveFileContentLocked(w, filePath, data, acceptRaw)
			return
		case repo.files[filePath] != nil:
			// A file now (or again) at HEAD that did not exist at ref:
			// GitHub 404s the historical read.
			m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
			return
			// Otherwise the path may be a directory prefix: fall through
			// to the HEAD listing (the mock keeps no per-commit trees).
		}
	}
	file := repo.files[filePath]
	if file == nil {
		// Not an exact file: it may be a directory prefix. GitHub's contents
		// API returns an array of entries for a directory. Prune's object
		// enumeration relies on this, so mirror it - including GitHub's
		// hard 1000-entry ceiling, past which the API 403s "too large"
		// rather than truncating.
		if entries, ok := m.dirEntriesLocked(repo, filePath); ok {
			if len(entries) > contentsListingCap {
				m.writeJSON(w, http.StatusForbidden, map[string]any{"message": "too large"})
				return
			}
			m.writeJSON(w, http.StatusOK, entries)
			return
		}
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	m.serveFileContentLocked(w, filePath, file.data, acceptRaw)
}

// serveFileContentLocked answers a contents GET (raw or JSON envelope)
// with the given bytes. Caller holds m.mu.
func (m *mockGitHub) serveFileContentLocked(w http.ResponseWriter, filePath string, data []byte, acceptRaw bool) {
	if acceptRaw {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}
	m.writeJSON(w, http.StatusOK, map[string]any{
		"name":     filepath.Base(filePath),
		"path":     filePath,
		"sha":      computeGitBlobSHA(data),
		"encoding": "base64",
		"type":     "file",
		"content":  base64.StdEncoding.EncodeToString(data),
	})
}

// contentAtRefLocked resolves filePath at commit ref the way GitHub does:
// the bytes the path carried in that commit's tree. known=false means ref
// is not a commit the mock knows at all (GitHub: 404 "No commit found for
// SHA" - the ref is resolved across EVERY path's history, since GitHub
// accepts any valid SHA, not just commits that touched this file).
// known=true, present=false means the path did not exist (or was deleted)
// at that commit (GitHub: 404 "Not Found").
func (m *mockGitHub) contentAtRefLocked(repo *mockRepo, filePath, ref string) (data []byte, known, present bool) {
	// O(1) ref resolution via the commitTime index (was: a scan of every
	// path's history per ?ref= GET).
	refWhen, known := repo.commitTime[ref]
	if !known {
		return nil, false, false
	}
	commits := repo.commitsByPath[filePath]
	if len(commits) == 0 {
		// No recorded history for this path: it predates every commit the
		// mock knows (seeded state), so HEAD bytes are its content at any
		// valid ref.
		if f := repo.files[filePath]; f != nil {
			return f.data, true, true
		}
		return nil, true, false
	}
	// commitsByPath is newest-first; the content at ref is the newest
	// entry at or before ref's timestamp.
	for _, c := range commits {
		if !c.when.After(refWhen) {
			if c.deleted {
				return nil, true, false
			}
			return c.data, true, true
		}
	}
	// Every change to this path postdates ref: it did not exist then.
	return nil, true, false
}

// dirEntriesLocked returns the immediate children of a directory prefix
// from the dirChildren index (was: a scan of every file per ListDir).
// ok=false when nothing lives under it. Caller holds m.mu.
func (m *mockGitHub) dirEntriesLocked(repo *mockRepo, dirPath string) ([]map[string]any, bool) {
	// Preserve the historical contract: the root (""/"/") never lists.
	// GitHub has no root-contents listing on this route and prune's
	// enumeration always passes a real directory prefix.
	if strings.Trim(dirPath, "/") == "" {
		return nil, false
	}
	dir := strings.TrimSuffix(dirPath, "/")
	kids, ok := repo.dirChildren[dir]
	if !ok || len(kids) == 0 {
		return nil, false
	}
	names := make([]string, 0, len(kids))
	for name := range kids {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]map[string]any, 0, len(names))
	for _, name := range names {
		if kids[name] {
			out = append(out, map[string]any{"name": name, "path": dir + "/" + name, "type": "dir"})
			continue
		}
		p := dir + "/" + name
		sha := ""
		if f := repo.files[p]; f != nil {
			sha = f.sha
		}
		out = append(out, map[string]any{"name": name, "path": p, "type": "file", "sha": sha})
	}
	return out, true
}

func (m *mockGitHub) handlePutContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	var payload struct {
		Message string `json:"message"`
		Content string `json:"content"`
		SHA     string `json:"sha"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	// Mirror GitHub's request validation: a contents PUT
	// without a commit message 422s, and invalid base64 is a 422 - not
	// the 400/200 the old mock answered with.
	if payload.Message == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"message\" wasn't supplied."})
		return
	}
	data, err := base64.StdEncoding.DecodeString(payload.Content)
	if err != nil {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"content\" is not valid base64-encoded data."})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := repo.files[filePath]
	// Mirror GitHub: a sha-less PUT onto an EXISTING path is not a silent
	// overwrite, it is a create collision - 422 with the "sha wasn't
	// supplied" request-validation body. The old 200 made the
	// production benign-concurrent-create branch unreachable in tests.
	if current != nil && payload.SHA == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"sha\" wasn't supplied."})
		return
	}
	if current != nil && payload.SHA != "" && payload.SHA != current.sha {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "sha does not match"})
		return
	}
	if current == nil && payload.SHA != "" {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "file does not exist"})
		return
	}
	// Compute real git blob SHA instead of fake counter-based SHA
	blobSHA := computeGitBlobSHA(data)
	m.putMockFileLocked(repo, filePath, data)
	commitSHA := m.recordMockCommitLocked(repo, filePath, payload.Message, data, false)
	// Mirror GitHub: creating a new file answers 201, updating 200.
	status := http.StatusOK
	if current == nil {
		status = http.StatusCreated
	}
	m.writeJSON(w, status, map[string]any{
		"content": map[string]any{
			"name": filepath.Base(filePath),
			"path": filePath,
			"sha":  blobSHA,
		},
		"commit": map[string]any{
			"sha": commitSHA,
		},
	})
}

func (m *mockGitHub) handleListCommits(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	filePath := r.URL.Query().Get("path")
	m.mu.Lock()
	defer m.mu.Unlock()
	commits := repo.commitsByPath[filePath]
	m.writePaginationLinks(w, r, len(commits))
	pageItems := paginateSlice(commits, r.URL.Query())
	response := make([]map[string]any, 0, len(pageItems))
	for _, commit := range pageItems {
		response = append(response, map[string]any{
			"sha": commit.sha,
			"commit": map[string]any{
				"message": commit.message,
				"author":  map[string]any{"date": commit.when.Format(time.RFC3339)},
			},
		})
	}
	m.writeJSON(w, http.StatusOK, response)
}

func (m *mockGitHub) handleCreateRelease(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	var payload struct {
		TagName string `json:"tag_name"`
		Name    string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: creating a tag that already exists 422s instead of
	// overwriting. The production double-create race depends on this.
	if existing := repo.releasesByTag[payload.TagName]; existing != nil {
		m.writeAlreadyExists(w, "Release", "tag_name", payload.TagName)
		return
	}
	release := &mockRelease{
		id:        repo.nextReleaseID,
		tag:       payload.TagName,
		name:      payload.Name,
		uploadURL: fmt.Sprintf("%s/upload/%s/%s{?name}", m.server.URL, repo.name, payload.TagName),
	}
	repo.nextReleaseID++
	repo.releasesByTag[release.tag] = release
	repo.releasesByID[release.id] = release
	m.writeJSON(w, http.StatusCreated, map[string]any{
		"id":         release.id,
		"tag_name":   release.tag,
		"name":       release.name,
		"upload_url": release.uploadURL,
		"draft":      false,
	})
}

func (m *mockGitHub) handleGetReleaseByTag(w http.ResponseWriter, repo *mockRepo, tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	release := repo.releasesByTag[tag]
	if release == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	m.writeJSON(w, http.StatusOK, map[string]any{
		"id":         release.id,
		"tag_name":   release.tag,
		"name":       release.name,
		"upload_url": release.uploadURL,
		"draft":      false,
		"assets":     m.embeddedAssetsLocked(repo, release.tag),
	})
}

func (m *mockGitHub) handleListReleases(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	releases := make([]*mockRelease, 0, len(repo.releasesByTag))
	for _, release := range repo.releasesByTag {
		releases = append(releases, release)
	}
	// Mirror GitHub: the releases list is ordered by created_at DESC, not
	// by tag. IDs are minted in creation order, so descending
	// ID is the same sequence; tag-string order ("v10" < "v9") is not.
	sort.Slice(releases, func(i, j int) bool { return releases[i].id > releases[j].id })
	m.writePaginationLinks(w, r, len(releases))
	pageReleases := paginateSlice(releases, r.URL.Query())
	response := make([]map[string]any, 0, len(pageReleases))
	for _, release := range pageReleases {
		assets := m.embeddedAssetsLocked(repo, release.tag)
		response = append(response, map[string]any{
			"id":         release.id,
			"tag_name":   release.tag,
			"name":       release.name,
			"upload_url": release.uploadURL,
			"draft":      false,
			"assets":     assets,
		})
	}
	m.writeJSON(w, http.StatusOK, response)
}

func (m *mockGitHub) handleListReleaseAssets(w http.ResponseWriter, r *http.Request, repo *mockRepo, rawID string) {
	releaseID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	release := repo.releasesByID[releaseID]
	if release == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	allAssets := m.releaseAssetsLocked(repo, release.tag)
	m.writePaginationLinks(w, r, len(allAssets))
	m.writeJSON(w, http.StatusOK, paginateSlice(allAssets, r.URL.Query()))
}

// embeddedAssetsLocked mirrors GitHub's truncated embedded asset array when
// embedCap is set; the dedicated list-assets endpoint always serves truth.
func (m *mockGitHub) embeddedAssetsLocked(repo *mockRepo, tag string) []map[string]any {
	assets := m.releaseAssetsLocked(repo, tag)
	if cap := m.faults.embedCap; cap > 0 && len(assets) > cap {
		assets = assets[:cap]
	}
	return assets
}

func (m *mockGitHub) releaseAssetsLocked(repo *mockRepo, tag string) []map[string]any {
	// O(tag assets) via the per-tag index (was: a scan of every asset in
	// the repo per call, O(R*A) per releases page when embedded).
	byTag := repo.assetsByTag[tag]
	rows := make([]*mockAsset, 0, len(byTag))
	for _, asset := range byTag {
		rows = append(rows, asset)
	}
	// Deterministic embed order: GitHub lists assets by ascending
	// ID; Go map iteration is random.
	sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
	assets := make([]map[string]any, 0, len(rows))
	for _, asset := range rows {
		assets = append(assets, map[string]any{"id": asset.id, "name": asset.name, "size": len(asset.data)})
	}
	return assets
}

// tagAssetLocked adds asset to the per-tag index. Caller holds m.mu.
func tagAssetLocked(repo *mockRepo, asset *mockAsset) {
	if repo.assetsByTag == nil {
		repo.assetsByTag = make(map[string]map[int64]*mockAsset)
	}
	byTag := repo.assetsByTag[asset.releaseTag]
	if byTag == nil {
		byTag = make(map[int64]*mockAsset)
		repo.assetsByTag[asset.releaseTag] = byTag
	}
	byTag[asset.id] = asset
}

// untagAssetLocked removes id from the per-tag index. Caller holds m.mu.
func untagAssetLocked(repo *mockRepo, tag string, id int64) {
	byTag := repo.assetsByTag[tag]
	if byTag == nil {
		return
	}
	delete(byTag, id)
	if len(byTag) == 0 {
		delete(repo.assetsByTag, tag)
	}
}

// parsePaging extracts (perPage, page) from a query once for both the
// slicer and the Link-header writer. A missing/invalid per_page defaults
// to 30 like real GitHub (was: "return everything"), so a client that
// forgets per_page still pages instead of passing vacuously.
func parsePaging(query url.Values) (perPage, page int) {
	perPage, _ = strconv.Atoi(query.Get("per_page"))
	page, _ = strconv.Atoi(query.Get("page"))
	if perPage <= 0 {
		perPage = 30
	}
	if page <= 0 {
		page = 1
	}
	return perPage, page
}

// writePaginationLinks mirrors GitHub's RFC 5988 Link headers so paging is
// observable without relying on body-length heuristics.
func (m *mockGitHub) writePaginationLinks(w http.ResponseWriter, r *http.Request, total int) {
	perPage, page := parsePaging(r.URL.Query())
	last := (total + perPage - 1) / perPage
	if last < 1 {
		last = 1
	}
	if page >= last {
		return
	}
	setPage := func(p int) string {
		q := r.URL.Query()
		q.Set("per_page", strconv.Itoa(perPage))
		q.Set("page", strconv.Itoa(p))
		return fmt.Sprintf("http://%s%s?%s", r.Host, r.URL.Path, q.Encode())
	}
	w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="next", <%s>; rel="last"`, setPage(page+1), setPage(last)))
}

func (m *mockGitHub) handleDeleteRelease(w http.ResponseWriter, repo *mockRepo, rawID string) {
	releaseID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	release := repo.releasesByID[releaseID]
	if release == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	delete(repo.releasesByID, releaseID)
	delete(repo.releasesByTag, release.tag)
	delete(repo.assetsByTag, release.tag)
	for id, asset := range repo.assets {
		if asset.releaseTag == release.tag {
			delete(repo.assets, id)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) handleUpload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	repo := m.repo(parts[1])
	if repo == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "repo not found"})
		return
	}
	name := r.URL.Query().Get("name")
	data, err := io.ReadAll(r.Body)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: uploads target a release that must exist.
	if repo.releasesByTag[parts[2]] == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	// Fault injection: one forced name collision for the sink retry test.
	if m.faults.collideNext[parts[1]+"/"+parts[2]] {
		delete(m.faults.collideNext, parts[1]+"/"+parts[2])
		m.writeAlreadyExists(w, "ReleaseAsset", "name", name)
		return
	}
	// Mirror GitHub: asset names are unique per release; a duplicate 422s.
	// The per-tag index answers both the dup check and the count below in
	// one pass (was: two full asset scans per upload, O(N^2) to fill).
	byTag := repo.assetsByTag[parts[2]]
	for _, asset := range byTag {
		if asset.name == name {
			m.writeAlreadyExists(w, "ReleaseAsset", "name", name)
			return
		}
	}
	// Mirror GitHub: a release holds at most releaseAssetCap assets;
	// further uploads 422 with a file_count body (live shape from
	// storhub-web v18).
	if len(byTag) >= releaseAssetCap {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"message": "Validation Failed",
			"errors": []map[string]any{{
				"resource": "ReleaseAsset",
				"code":     "custom",
				"field":    "file_count",
				"message":  "file_count limited to 1000 assets per release",
			}},
		})
		return
	}
	asset := &mockAsset{id: globalMockAssetID.Add(1), name: name, releaseTag: parts[2], data: append([]byte(nil), data...)}
	repo.assets[asset.id] = asset
	tagAssetLocked(repo, asset)
	m.writeJSON(w, http.StatusCreated, map[string]any{"id": asset.id, "name": asset.name})
}

func (m *mockGitHub) handleDownloadAsset(w http.ResponseWriter, r *http.Request, repo *mockRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	asset, ok := repo.assets[assetID]
	if !ok {
		m.mu.Unlock()
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	name, size := asset.name, len(asset.data)
	m.mu.Unlock()
	// Mirror GitHub: only the octet-stream GET is a redirect to a
	// short-lived signed CDN URL (range fetches then hit the CDN directly
	// without auth headers). Any other Accept answers with the asset's
	// metadata JSON - never the bytes.
	if r.Header.Get("Accept") != "application/octet-stream" {
		m.writeJSON(w, http.StatusOK, map[string]any{
			"id":                   assetID,
			"name":                 name,
			"size":                 size,
			"content_type":         "application/octet-stream",
			"browser_download_url": fmt.Sprintf("%s/cdn/%d", m.server.URL, assetID),
		})
		return
	}
	w.Header().Set("Location", m.mintSignedCDNURL(assetID))
	w.WriteHeader(http.StatusFound)
}

// handleCDN serves signed-URL range fetches: no auth required (the URL is
// the bearer credential), Range honored, Accept-Ranges advertised. When
// the URL carries expiries (cdnTTL armed) an expired fetch 403s like
// GitHub's expired Azure SAS does, forcing the client to re-resolve
// through the API. A one-shot 618 (cdnFail618Once) mirrors the front-door
// JWT dying first, which the client also re-resolves.
func (m *mockGitHub) handleCDN(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "" {
		m.faults.cdnSawAuth.Store(true)
	}
	if m.faults.cdnFail618Once.CompareAndSwap(true, false) {
		m.writeJSON(w, ghapi.StatusSignedURLExpired, map[string]any{"message": "jwt:expired"})
		return
	}
	if exp, ok := mockSignedURLExpiry(r.URL.Query()); ok {
		shift := m.faults.cdnTimeShift.Swap(0)
		if m.mockTime().Add(time.Duration(shift)).After(exp) {
			m.writeJSON(w, http.StatusForbidden, map[string]any{"message": "Signature is not valid on this request"})
			return
		}
	}
	assetID, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/cdn/"), 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	asset, ok := m.findAssetLocked(assetID)
	var data []byte
	if ok {
		data = append([]byte(nil), asset.data...)
	}
	m.mu.Unlock()
	if !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	if delay := m.faults.cdnDelay.Load(); delay > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(time.Duration(delay)):
		}
	}
	start, end, partial, err := resolveByteRange(r.Header.Get("Range"), int64(len(data)))
	if err != nil {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	body := data
	status := http.StatusOK
	if partial {
		body = data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		status = http.StatusPartialContent
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (m *mockGitHub) handleDeleteAsset(w http.ResponseWriter, repo *mockRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	asset, ok := repo.assets[assetID]
	if !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	untagAssetLocked(repo, asset.releaseTag, assetID)
	delete(repo.assets, assetID)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) handleDeleteRepo(w http.ResponseWriter, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: deleting an unknown repo 404s. The client
	// treats 404 on delete as success; the old unconditional 204 hid the
	// lost-response-retry divergence.
	if _, ok := m.repos[name]; !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	delete(m.repos, name)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) repo(name string) *mockRepo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.repos[name]
}

func (m *mockGitHub) addRelease(t *testing.T, project, tag string) *mockRelease {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	release := &mockRelease{id: repo.nextReleaseID, tag: tag, name: "manual " + tag, uploadURL: fmt.Sprintf("%s/upload/%s/%s{?name}", m.server.URL, repo.name, tag)}
	repo.nextReleaseID++
	repo.releasesByTag[tag] = release
	repo.releasesByID[release.id] = release
	return release
}

func (m *mockGitHub) addAssetToRelease(t *testing.T, project, tag, name string, data []byte) int64 {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	asset := &mockAsset{id: globalMockAssetID.Add(1), name: name, releaseTag: tag, data: append([]byte(nil), data...)}
	repo.assets[asset.id] = asset
	tagAssetLocked(repo, asset)
	return asset.id
}

func (m *mockGitHub) addAssetsToRelease(t *testing.T, project, tag string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		m.addAssetToRelease(t, project, tag, fmt.Sprintf("extra-%03d.bin", i), []byte("x"))
	}
}

func (m *mockGitHub) removeAsset(t *testing.T, project string, assetID int64) {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if asset, ok := repo.assets[assetID]; ok {
		untagAssetLocked(repo, asset.releaseTag, assetID)
	}
	delete(repo.assets, assetID)
}

func (m *mockGitHub) setMetadata(t *testing.T, project string, md *RepoMetadata) {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Match the project's current layout: a split (version-5) project stores
	// a manifest plus objects; a legacy project a single blob.
	if repo.files[indexFilePath] != nil {
		res, err := meta.BuildTree(md)
		if err != nil {
			t.Fatalf("build tree: %v", err)
		}
		for sha, data := range res.Objects {
			m.putMockFileLocked(repo, objectRepoPath(sha), data)
		}
		mf := &meta.Manifest{
			Version: meta.CurrentVersion, Project: md.Project, TreeRoot: res.RootSHA,
			ChunkBuckets: res.ChunkBuckets, Releases: res.ReleasesSHA,
			NextInode: md.NextInode, NextChunkID: md.NextChunkID,
			Stats: meta.ManifestStats{Files: md.TotalFiles, Bytes: md.TotalSize}, LastMod: md.LastMod,
		}
		mb, err := meta.MarshalManifest(mf)
		if err != nil {
			t.Fatalf("marshal manifest: %v", err)
		}
		m.putMockFileLocked(repo, indexFilePath, mb)
		return
	}
	payload, err := md.ToJSON()
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	// Mirror GitHub's stored blob sha: the client computes its
	// CAS token locally from raw bytes, so a fake counter sha would make
	// a read-modify-write of seeded legacy state 409 on a precondition
	// that passes against the real API.
	m.putMockFileLocked(repo, metadataFilePath, payload)
}

func (m *mockGitHub) assertRepoStats(t *testing.T, project string, files int, size int64) {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	repoMeta := mustLoadMetadata(t, repo)
	if repoMeta.TotalFiles != files || repoMeta.TotalSize != size {
		t.Fatalf("unexpected repo stats: %+v", repoMeta)
	}
}

func mustLoadMetadata(t *testing.T, repo *mockRepo) *RepoMetadata {
	t.Helper()
	// Layout-aware: a split (version-5) project stores its index as a
	// manifest plus content-addressed objects; a legacy project as one blob.
	if idx := repo.files[indexFilePath]; idx != nil {
		manifest, err := meta.ParseManifest(idx.data)
		if err != nil {
			t.Fatalf("parse manifest: %v", err)
		}
		get := func(sha string) ([]byte, error) {
			f := repo.files[objectRepoPath(sha)]
			if f == nil {
				return nil, fmt.Errorf("missing object %s", sha)
			}
			return f.data, nil
		}
		m, err := meta.LoadTree(manifest, get)
		if err != nil {
			t.Fatalf("load split index: %v", err)
		}
		m.Normalize(manifest.Project, m.LastMod)
		return m
	}
	file := repo.files[metadataFilePath]
	if file == nil {
		return &RepoMetadata{}
	}
	legacy := &RepoMetadata{}
	if err := legacy.FromJSON(file.data); err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	return legacy
}

func (m *mockGitHub) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// writeAlreadyExists mirrors GitHub's 422 duplicate body (live shape: 422
// Validation Failed with an already_exists error entry).
func (m *mockGitHub) writeAlreadyExists(w http.ResponseWriter, resource, field, value string) {
	m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
		"message": "Validation Failed",
		"errors": []map[string]any{{
			"resource": resource,
			"code":     "already_exists",
			"field":    field,
			"message":  field + " already_exists: " + value,
		}},
	})
}

// TestMockBlobSHAMatchesGitVectors cross-pins the mock's local blob-SHA
// oracle against git's documented hashes: the empty blob must hash to
// e69de29bb2d1d6434b8b29ae775ad8c2e48c5391 (git hash-object -t blob
// /dev/null), and the digest must be sensitive to both content and length.
// If the production helper drifts from real git, this fails while the
// integration pins keep passing, isolating the oracle from the client.
func TestMockBlobSHAMatchesGitVectors(t *testing.T) {
	t.Parallel()
	if got := computeGitBlobSHA(nil); got != "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391" {
		t.Fatalf("empty blob SHA = %q, want git's e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", got)
	}
	a := computeGitBlobSHA([]byte("hello"))
	b := computeGitBlobSHA([]byte("hello!"))
	c := computeGitBlobSHA([]byte("hello"))
	if len(a) != 40 {
		t.Fatalf("blob SHA must be 40-hex, got %q", a)
	}
	if a == b {
		t.Fatal("blob SHA must be content-sensitive")
	}
	if a != c {
		t.Fatal("blob SHA must be deterministic")
	}
	if computeGitBlobSHA([]byte("")) != computeGitBlobSHA(nil) {
		t.Fatal("empty slice and nil must hash identically (length-0 header)")
	}
}

func writeTempFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

func paginateSlice[T any](items []T, query url.Values) []T {
	if query == nil {
		return items
	}
	perPage, page := parsePaging(query)
	start := (page - 1) * perPage
	if start >= len(items) {
		return nil
	}
	end := start + perPage
	if end > len(items) {
		end = len(items)
	}
	return items[start:end]
}

func resolveByteRange(header string, size int64) (int64, int64, bool, error) {
	if strings.TrimSpace(header) == "" {
		if size == 0 {
			return 0, -1, false, nil
		}
		return 0, size - 1, false, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, fmt.Errorf("unsupported range header: %s", header)
	}
	parts := strings.SplitN(strings.TrimPrefix(header, "bytes="), "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid range header: %s", header)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false, err
	}
	if start < 0 || end < start || end >= size {
		return 0, 0, false, fmt.Errorf("range %d-%d out of bounds for size %d", start, end, size)
	}
	return start, end, true, nil
}

func assertFileContent(t *testing.T, path string, expected []byte) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	if !bytes.Equal(data, expected) {
		t.Fatalf("unexpected file content for %s", path)
	}
}

func mustMetadataRevision(t *testing.T, hub *StorHub, project, message string) MetadataRevision {
	t.Helper()
	revisions, err := hub.ListMetadataRevisions(project)
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

// Regression test for the purge data-destruction chain: a patch that spills
// into a new release must register that release in the metadata catalog so
// PurgeUntracked cannot delete it (GitHub cascade-deletes its assets).
func TestPurgeUntrackedKeepsReleaseAfterPatchSpill(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "spill.txt", []byte("abcdefghijklmno"))
	fileMeta, err := hub.UploadFile("project-purge-spill", "spill.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	metaState, _, _ := hub.loadRepoMetadata(context.Background(), "project-purge-spill")
	firstRelease := metaState.Chunks()[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-purge-spill", firstRelease, 999)
	hub.invalidateReleaseCache("project-purge-spill")

	patched, err := hub.PatchFile("project-purge-spill", "spill.txt", 4, 4, []byte("ZZZZ"))
	if err != nil {
		t.Fatalf("patch file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	metaState, _, err = hub.loadRepoMetadata(context.Background(), "project-purge-spill")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	spillTag := ""
	for _, id := range patched.Chunks {
		if chunk := metaState.Chunks()[id]; chunk.Release != firstRelease {
			spillTag = chunk.Release
		}
	}
	if spillTag == "" {
		t.Fatal("expected patch to spill into a second release")
	}

	result, err := hub.PurgeUntracked("project-purge-spill")
	if err != nil {
		t.Fatalf("purge untracked: %v", err)
	}
	if result.DeletedReleases != 0 {
		t.Fatalf("purge deleted live releases: %+v", result)
	}
	repo := backend.repo("project-purge-spill")
	if repo == nil || repo.releasesByTag[spillTag] == nil {
		t.Fatalf("spill release %s was deleted by purge", spillTag)
	}
	output := filepath.Join(t.TempDir(), "spill.out")
	if err := hub.DownloadFile("project-purge-spill", "spill.txt", output); err != nil {
		t.Fatalf("download after purge: %v", err)
	}
	assertFileContent(t, output, []byte("abcdZZZZijklmno"))
}

// A fresh client (cold cache) must be able to delete data that exists only
// remotely; previously the delete consulted an empty in-memory view.
func TestDeleteFileWorksOnColdCache(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	seedHub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cold.txt", []byte("cold payload"))
	if _, err := seedHub.UploadFile("project-cold-delete", "cold.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := seedHub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	coldHub := backend.newClient(t, smallTransferTestConfig())
	if err := coldHub.DeleteFile("project-cold-delete", "cold.txt"); err != nil {
		t.Fatalf("cold-cache delete of existing file failed: %v", err)
	}
}

func mustBytes(t *testing.T, res fuse.ReadResult, buf []byte) []byte {
	t.Helper()
	got, status := res.Bytes(buf)
	if status != 0 {
		t.Fatalf("read result bytes: %v", status)
	}
	return got
}

func TestPurgeUntrackedPrunesUnreferencedChunks(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	inputV1 := writeTempFile(t, t.TempDir(), "v1.txt", []byte("version one payload"))
	if _, err := hub.UploadFile("project-prune", "file.txt", inputV1); err != nil {
		t.Fatalf("upload v1: %v", err)
	}
	inputV2 := writeTempFile(t, t.TempDir(), "v2.txt", []byte("completely different version two"))
	if _, err := hub.ReplaceFile("project-prune", "file.txt", inputV2); err != nil {
		t.Fatalf("replace with v2: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	before, _, _ := hub.loadRepoMetadataFresh(context.Background(), "project-prune")
	totalBefore := len(before.Chunks())
	if totalBefore < 2 {
		t.Fatalf("expected stale chunk records before purge, got %d", totalBefore)
	}

	if _, err := hub.PurgeUntracked("project-prune"); err != nil {
		t.Fatalf("purge untracked: %v", err)
	}

	after, _, _ := hub.loadRepoMetadataFresh(context.Background(), "project-prune")
	referenced := make(map[int64]bool)
	for _, file := range after.Files() {
		for _, id := range file.Chunks {
			referenced[id] = true
		}
	}
	for id := range after.Chunks() {
		if !referenced[id] {
			t.Fatalf("chunk %d survived purge despite no live references", id)
		}
	}
	if len(after.Chunks()) >= totalBefore {
		t.Fatalf("expected chunk catalog to shrink from %d, got %d", totalBefore, len(after.Chunks()))
	}

	output := filepath.Join(t.TempDir(), "out.txt")
	if err := hub.DownloadFile("project-prune", "file.txt", output); err != nil {
		t.Fatalf("download after prune: %v", err)
	}
	assertFileContent(t, output, []byte("completely different version two"))
}

// TestMarkProjectDirtyRevivesEvictedMetadata pins the eviction-race guard:
// an operation that captured pm before eviction can still land its
// acknowledged mutation - the instance is revived with a live commit loop
// instead of silently stranding dirty state on a dead loop.
func TestMarkProjectDirtyRevivesEvictedMetadata(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{
		ChunkSize:         32 << 20,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	})
	ctx := context.Background()
	project := "project-revive"
	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seed := writeTempFile(t, t.TempDir(), "revive.txt", []byte("survives eviction"))
	if _, err := hub.UploadFileContext(ctx, project, "docs/revive.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}

	hub.metaMu.RLock()
	pm := hub.metaCache[project]
	hub.metaMu.RUnlock()
	if pm == nil {
		t.Fatal("project metadata missing from cache after upload")
	}

	// Let the async commit loop drain so eviction preconditions hold.
	waitFor(t, time.Second, "metadata drain before forced eviction", func() bool {
		pm.mu.Lock()
		defer pm.mu.Unlock()
		return !pm.dirty
	})

	// Simulate eviction racing an in-flight operation.
	hub.metaMu.Lock()
	pm.mu.Lock()
	if pm.dirty {
		pm.mu.Unlock()
		hub.metaMu.Unlock()
		t.Fatal("precondition: metadata should be clean before forced eviction")
	}
	pm.stopped = true
	close(pm.stopCh)
	delete(hub.metaCache, project)
	pm.mu.Unlock()
	hub.metaMu.Unlock()

	// Finalize a mutation against the orphaned pointer, exactly what a long
	// operation does after its network phase.
	pm.mu.Lock()
	pm.meta.EnsureDirectory("late", time.Now().Unix())
	hub.markProjectDirtyLiveLocked(project, pm)
	if pm.stopped {
		t.Fatal("mutation must revive the evicted instance")
	}
	pm.mu.Unlock()

	hub.metaMu.RLock()
	_, cached := hub.metaCache[project]
	hub.metaMu.RUnlock()
	if !cached {
		t.Fatal("revived instance was not re-inserted into the cache")
	}

	// The revived commit loop must eventually publish the late change.
	pollDeadline := time.Now().Add(time.Second)
	for time.Now().Before(pollDeadline) {
		if _, err := hub.StatPathContext(ctx, project, "late"); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("revived metadata never committed the late mutation")
}

// TestExpectedRevisionCAS pins the compare-and-swap primitive: a mutation
// carrying WithExpectedRevision succeeds only while the remote metadata
// revision still matches the declared one.
func TestExpectedRevisionCAS(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := Config{
		ChunkSize:         32 << 20,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	}
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	project := "project-cas"

	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	rev, err := hub.RevisionContext(ctx, project)
	if err != nil || rev == "" {
		t.Fatalf("revision: %q %v", rev, err)
	}

	seed := writeTempFile(t, t.TempDir(), "cas.txt", []byte("guarded payload"))
	if _, err := hub.UploadFileContext(ctx, project, "docs/cas.txt", seed); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	// Capture AFTER all seeding so the expectation matches current HEAD.
	guardedRev, err := hub.RevisionContext(ctx, project)
	if err != nil {
		t.Fatalf("guarded revision: %v", err)
	}
	if _, err := hub.AppendFileContext(ctx, project, "docs/cas.txt", []byte("x"), shfs.WithExpectedRevision(guardedRev)); err != nil {
		t.Fatalf("mutation under matching revision must succeed: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush guarded change: %v", err)
	}

	// Advance the remote behind the first client's back.
	other := backend.newClient(t, cfg)
	if err := other.MkdirContext(ctx, project, "competing"); err != nil {
		t.Fatalf("competing mkdir: %v", err)
	}
	if err := other.FlushMetadata(ctx); err != nil {
		t.Fatalf("competing flush: %v", err)
	}

	staleRev := rev // captured before the competing writer advanced HEAD
	err = hub.DeleteFileContext(ctx, project, "docs/cas.txt", shfs.WithExpectedRevision(staleRev))
	if !errors.Is(err, shfs.ErrPreconditionFailed) {
		t.Fatalf("stale revision must fail with ErrPreconditionFailed, got %v", err)
	}

	// Re-reading and retrying converges.
	freshRev, err := hub.RevisionContext(ctx, project)
	if err != nil {
		t.Fatalf("fresh revision: %v", err)
	}
	if freshRev == staleRev {
		t.Fatal("remote revision should have advanced")
	}
	observer := backend.newClient(t, cfg)
	files, _ := observer.ListFiles(project)
	t.Logf("remote files after competing write: %v", files)
	if _, err := hub.StatPathContext(ctx, project, "docs/cas.txt"); err != nil {
		t.Fatalf("precondition debug: cas.txt visibility: %v", err)
	}
	if err := hub.DeleteFileContext(ctx, project, "docs/cas.txt", shfs.WithExpectedRevision(freshRev)); err != nil {
		t.Fatalf("mutation under fresh revision must succeed: %+v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush delete: %v", err)
	}
}

// TestColdCacheMutationDoesNotClobberRemote pins the hydration guard: a
// mutation issued from a process that never loaded the project must adopt
// remote state instead of committing an empty tree over it.
func TestColdCacheMutationDoesNotClobberRemote(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := Config{
		ChunkSize:         32 << 20,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	}
	writer := backend.newClient(t, cfg)
	ctx := context.Background()
	project := "project-cold-cache"

	if err := writer.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "keep.txt", []byte("precious"))
	if _, err := writer.UploadFileContext(ctx, project, "docs/keep.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := writer.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// A second, completely cold client mutates the same project.
	cold := backend.newClient(t, cfg)
	if err := cold.MkdirContext(ctx, project, "latecomer"); err != nil {
		t.Fatalf("cold mkdir: %v", err)
	}
	if err := cold.FlushMetadata(ctx); err != nil {
		t.Fatalf("cold flush: %v", err)
	}

	observer := backend.newClient(t, cfg)
	entry, err := observer.StatPathContext(ctx, project, "docs/keep.txt")
	if err != nil {
		t.Fatalf("cold-cache mutation clobbered remote state; docs/keep.txt missing: %v", err)
	}
	if entry.Size != int64(len("precious")) {
		t.Fatalf("kept file corrupted by cold-cache mutation: %+v", entry)
	}
	if _, err := observer.StatPathContext(ctx, project, "latecomer"); err != nil {
		t.Fatalf("cold mutation did not land: %v", err)
	}
}

// --- mock fidelity pins ------------------------------------------------
//
// These tests pin the MOCK to real GitHub behavior: the
// divergences it must mirror (422 create-collision, 404 unknown ref,
// 403 oversized dir, 422 sha-less delete, 404 unknown-repo delete, real
// blob shas, delete commits, newest-first releases, scoped rate fault,
// expiring CDN URLs, JSON-accept asset metadata) must not silently regress.

func mockRaw(t *testing.T, backend *mockGitHub, method, path string, body any) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal raw body: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, backend.server.URL+path, rdr)
	if err != nil {
		t.Fatalf("build raw request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+backend.token)
	resp, err := backend.server.Client().Do(req)
	if err != nil {
		t.Fatalf("raw %s %s: %v", method, path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func putRawStatus(t *testing.T, backend *mockGitHub, project, path string, body map[string]any) int {
	t.Helper()
	resp := mockRaw(t, backend, http.MethodPut, "/repos/"+backend.owner+"/"+project+"/contents/"+path, body)
	return resp.StatusCode
}

// A sha-less PUT onto an existing path is a create collision and
// must 422 with GitHub's "sha wasn't supplied" validation body - never a
// silent 200 overwrite. Creates answer 201, updates 200, missing message
// and invalid base64 answer 422.
func TestMockContentsPutCreateCollisionIs422(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "put422"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString([]byte("v1"))
	if got := putRawStatus(t, backend, "put422", "f.txt", map[string]any{"message": "create", "content": b64}); got != http.StatusCreated {
		t.Fatalf("create must 201, got %d", got)
	}
	resp := mockRaw(t, backend, http.MethodPut, "/repos/"+backend.owner+"/put422/contents/f.txt", map[string]any{"message": "collision", "content": b64})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("sha-less create onto existing path must 422, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "sha") || !strings.Contains(string(body), "Invalid request") {
		t.Fatalf("422 body must carry GitHub's sha-validation prose, got %q", body)
	}
	// A stale sha still 409s (update precondition), the current sha updates (200).
	if got := putRawStatus(t, backend, "put422", "f.txt", map[string]any{"message": "stale", "content": b64, "sha": "deadbeef"}); got != http.StatusConflict {
		t.Fatalf("stale sha must 409, got %d", got)
	}
	current := computeGitBlobSHA([]byte("v1"))
	if got := putRawStatus(t, backend, "put422", "f.txt", map[string]any{"message": "ok", "content": base64.StdEncoding.EncodeToString([]byte("v2")), "sha": current}); got != http.StatusOK {
		t.Fatalf("matching sha must update with 200, got %d", got)
	}
	if got := putRawStatus(t, backend, "put422", "g.txt", map[string]any{"content": b64}); got != http.StatusUnprocessableEntity {
		t.Fatalf("missing message must 422, got %d", got)
	}
	if got := putRawStatus(t, backend, "put422", "h.txt", map[string]any{"message": "bad b64", "content": "!!!not-base64!!!"}); got != http.StatusUnprocessableEntity {
		t.Fatalf("invalid base64 must 422, got %d", got)
	}
}

// ?ref= must resolve like GitHub - exact commit, any valid commit
// (content at-or-before), 404 for unknown refs, 404 for a path deleted at
// that ref, and HEAD/branch names resolving to current state.
func TestMockUnknownRefIs404(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "ref404"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	get := func(path, ref string) int {
		p := "/repos/" + backend.owner + "/ref404/contents/" + path
		if ref != "" {
			p += "?ref=" + url.QueryEscape(ref)
		}
		return mockRaw(t, backend, http.MethodGet, p, nil).StatusCode
	}
	putRawStatus(t, backend, "ref404", "a.txt", map[string]any{"message": "a1", "content": base64.StdEncoding.EncodeToString([]byte("a1"))})
	// commit SHAs are 40-hex like prod; resolve them dynamically instead of
	// pinning the old counter shape.
	revsA, err := hub.gh.ListFileCommits(ctx, hub.Owner(), "ref404", "a.txt")
	if err != nil || len(revsA) != 1 {
		t.Fatalf("list a.txt commits: %v %+v", err, revsA)
	}
	commitA := revsA[0].SHA
	if len(commitA) != 40 {
		t.Fatalf("mock commit SHAs must be 40-hex like prod, got %q", commitA)
	}
	// commit-1 created a.txt. An unknown SHA must 404, not serve HEAD.
	if got := get("a.txt", "0123456789abcdef"); got != http.StatusNotFound {
		t.Fatalf("unknown ref must 404, got %d", got)
	}
	// A commit that touched a DIFFERENT path still resolves: b.txt's
	// create commit sees a.txt at its commit-1 content.
	putRawStatus(t, backend, "ref404", "b.txt", map[string]any{"message": "b1", "content": base64.StdEncoding.EncodeToString([]byte("b1"))})
	revsB, err := hub.gh.ListFileCommits(ctx, hub.Owner(), "ref404", "b.txt")
	if err != nil || len(revsB) != 1 {
		t.Fatalf("list b.txt commits: %v %+v", err, revsB)
	}
	commitB := revsB[0].SHA
	resp := mockRaw(t, backend, http.MethodGet, "/repos/"+backend.owner+"/ref404/contents/a.txt?ref="+commitB, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid cross-path ref must resolve, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), base64.StdEncoding.EncodeToString([]byte("a1"))) {
		t.Fatalf("ref read must serve the at-or-before content, got %q", body)
	}
	// b.txt did not exist at commit-1: 404 Not Found (not HEAD!).
	if got := get("b.txt", commitA); got != http.StatusNotFound {
		t.Fatalf("path created after ref must 404, got %d", got)
	}
	// HEAD and the default branch resolve to current state.
	if got := get("a.txt", "HEAD"); got != http.StatusOK {
		t.Fatalf("HEAD ref must resolve, got %d", got)
	}
	if got := get("a.txt", defaultBranch); got != http.StatusOK {
		t.Fatalf("branch ref must resolve, got %d", got)
	}
	// Deleting a.txt records a delete commit; reading the path AT that
	// commit 404s while the commit appears in the path's history.
	del := mockRaw(t, backend, http.MethodDelete, "/repos/"+backend.owner+"/ref404/contents/a.txt", map[string]any{"message": "bye", "sha": computeGitBlobSHA([]byte("a1"))})
	if del.StatusCode != http.StatusOK {
		t.Fatalf("delete with matching sha must 200, got %d", del.StatusCode)
	}
	delBody, _ := io.ReadAll(del.Body)
	var parsed struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := json.Unmarshal(delBody, &parsed); err != nil || parsed.Commit.SHA == "" {
		t.Fatalf("delete must report its commit: %v %s", err, delBody)
	}
	if got := get("a.txt", parsed.Commit.SHA); got != http.StatusNotFound {
		t.Fatalf("read at the delete commit must 404, got %d", got)
	}
	revs, err := hub.gh.ListFileCommits(ctx, hub.Owner(), "ref404", "a.txt")
	if err != nil {
		t.Fatalf("list commits: %v", err)
	}
	if len(revs) != 2 || revs[0].SHA != parsed.Commit.SHA {
		t.Fatalf("delete commit must head the path history, got %+v", revs)
	}
}

// A directory listing over GitHub's 1000-entry cap must 403 "too
// large", exactly like the live API, so prune's oversized-repo fallback
// branch is reachable in tests.
func TestMockDirListingOverCapIs403(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "cap403"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	repo := backend.repo("cap403")
	backend.mu.Lock()
	for i := 0; i <= contentsListingCap; i++ {
		name := fmt.Sprintf("big/f%05d", i)
		backend.putMockFileLocked(repo, name, []byte(name))
	}
	backend.mu.Unlock()
	resp := mockRaw(t, backend, http.MethodGet, "/repos/"+backend.owner+"/cap403/contents/big", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("dir over the cap must 403, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "too large") {
		t.Fatalf("403 body must carry GitHub's too-large message, got %q", body)
	}
	// At the cap the listing still answers.
	backend.mu.Lock()
	backend.deleteMockFileLocked(repo, "big/f00000")
	backend.mu.Unlock()
	if got := mockRaw(t, backend, http.MethodGet, "/repos/"+backend.owner+"/cap403/contents/big", nil).StatusCode; got != http.StatusOK {
		t.Fatalf("listing at the cap must 200, got %d", got)
	}
}

// Contents DELETE without a sha must 422 (GitHub requires the blob
// sha), never delete silently.
func TestMockDeleteRequiresSHA(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "del422"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	putRawStatus(t, backend, "del422", "k.txt", map[string]any{"message": "keep", "content": base64.StdEncoding.EncodeToString([]byte("k"))})
	resp := mockRaw(t, backend, http.MethodDelete, "/repos/"+backend.owner+"/del422/contents/k.txt", map[string]any{"message": "wipe"})
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("sha-less delete must 422, got %d", resp.StatusCode)
	}
	if backend.repo("del422").files["k.txt"] == nil {
		t.Fatal("refused delete must not remove the file")
	}
}

// DELETE on an unknown repo 404s (the router already refuses unknown
// repos; pin it so the client's 404-is-success tolerance stays exercised).
func TestMockDeleteUnknownRepoIs404(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	if got := mockRaw(t, backend, http.MethodDelete, "/repos/"+backend.owner+"/never-existed", nil).StatusCode; got != http.StatusNotFound {
		t.Fatalf("unknown repo delete must 404, got %d", got)
	}
}

// Legacy setMetadata must store the REAL git blob sha, so a hub that
// reads the seeded file and CAS-writes it back (token computed locally
// from bytes) is not 409ed by a precondition GitHub would pass.
func TestMockLegacySetMetadataStoresRealBlobSHA(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "legacy6"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	md := NewRepoMetadata("legacy6")
	md.EnsureDirectory("docs", 1700000000)
	backend.setMetadata(t, "legacy6", md)
	repo := backend.repo("legacy6")
	backend.mu.Lock()
	file := repo.files[metadataFilePath]
	want := computeGitBlobSHA(file.data)
	backend.mu.Unlock()
	if file.sha != want {
		t.Fatalf("legacy seed must store the real blob sha, got %s want %s", file.sha, want)
	}
	// The read-modify-write the client performs must pass the mock's CAS.
	_, sha, err := hub.gh.GetFileContent(ctx, hub.Owner(), "legacy6", metadataFilePath, "")
	if err != nil {
		t.Fatalf("read legacy: %v", err)
	}
	if _, _, err := hub.gh.PutFileContent(ctx, hub.Owner(), "legacy6", metadataFilePath, file.data, sha, "rewrite"); err != nil {
		t.Fatalf("CAS rewrite of seeded legacy file must succeed: %v", err)
	}
}

// The releases list must be ordered newest-created first, like GitHub
// (created_at desc), not by tag string.
func TestMockReleasesListedNewestFirst(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "relorder"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	for _, tag := range []string{"v9", "v10", "v2"} {
		backend.addRelease(t, "relorder", tag)
	}
	releases, err := hub.gh.ListReleases(ctx, hub.Owner(), "relorder")
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 3 || releases[0].TagName != "v2" || releases[1].TagName != "v10" || releases[2].TagName != "v9" {
		t.Fatalf("releases must come back created-desc (v2, v10, v9), got %+v", releases)
	}
}

// The opt-in rate fault may only be spent on an authenticated API
// route - never on a CDN fetch.
func TestMockRateLimitFaultScopedToAPIRoutes(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	backend.armRateLimit()
	req, _ := http.NewRequest(http.MethodGet, backend.server.URL+"/cdn/404110", nil)
	resp, err := backend.server.Client().Do(req)
	if err != nil {
		t.Fatalf("cdn probe: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		t.Fatal("CDN fetch must not consume the rate fault")
	}
	if got := mockRaw(t, backend, http.MethodGet, "/user", nil).StatusCode; got != http.StatusTooManyRequests {
		t.Fatalf("API route must take the armed fault, got %d", got)
	}
	if backend.faults.rateLimitServed.Load() != 1 {
		t.Fatalf("fault must fire exactly once, served=%d", backend.faults.rateLimitServed.Load())
	}
}

// With cdnTTL armed, signed CDN URLs expire (403) and the client's
// SAS-rejection path re-resolves through the API instead of failing.
func TestMockCDNExpiryForcesReResolution(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	backend.faults.SetCDNTTL(50 * time.Millisecond)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	payload := []byte("expiring cdn payload")
	input := writeTempFile(t, t.TempDir(), "exp.txt", payload)
	if _, err := hub.UploadFileContext(ctx, "cdnexp", "exp.txt", input); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var apiAssetHits atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			apiAssetHits.Add(1)
		}
		return false
	})
	out := filepath.Join(t.TempDir(), "exp.out")
	if err := hub.DownloadFileContext(ctx, "cdnexp", "exp.txt", out); err != nil {
		t.Fatalf("first download: %v", err)
	}
	// Event, not wall clock: shift the CDN's expiry clock past the TTL
	// once, so the cached signed URL is provably stale for the next fetch.
	// The shift (2s, virtual - zero wall cost) must exceed the minted
	// whole-second JWT exp (up to ~1s out: 50ms TTL rounded up), or the
	// cached URL would still verify and no re-resolution would fire.
	backend.expireCDNOnce(2 * time.Second)
	if err := hub.DownloadFileContext(ctx, "cdnexp", "exp.txt", out); err != nil {
		t.Fatalf("download with expired cached URL must re-resolve, got: %v", err)
	}
	assertFileContent(t, out, payload)
	if apiAssetHits.Load() < 2 {
		t.Fatalf("expired URL must force at least two API resolutions, got %d", apiAssetHits.Load())
	}
}

// An asset GET with a JSON Accept answers with the asset's metadata,
// never the bytes.
func TestMockAssetJSONAcceptReturnsMetadata(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.EnsureRepoContext(ctx, "assetjson"); err != nil {
		t.Fatalf("ensure repo: %v", err)
	}
	backend.addRelease(t, "assetjson", "v1")
	id := backend.addAssetToRelease(t, "assetjson", "v1", "data.bin", []byte("raw bytes should not stream here"))
	resp := mockRaw(t, backend, http.MethodGet, fmt.Sprintf("/repos/%s/assetjson/releases/assets/%d", backend.owner, id), nil)
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("JSON-accept asset GET must answer metadata JSON, got content-type %q", ct)
	}
	var meta map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatalf("decode asset metadata: %v", err)
	}
	if meta["name"] != "data.bin" || meta["id"] != float64(id) {
		t.Fatalf("unexpected asset metadata: %+v", meta)
	}
}

// --- git_repo sabotage checks ------------------------------------------

// casConflict must classify go-git's real lease failures as 409 and
// must NOT misread "release" prose (hooks, URLs) as a conflict - the old
// bare "lease" substring did.
func TestCasConflictWordBoundary(t *testing.T) {
	t.Parallel()
	base := plumbing.ZeroHash
	if err := casConflict(errors.New("remote: hook declined push for releases/v9"), base); err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
			t.Fatal("error text containing 'release' must not be misread as a lease conflict")
		}
	}
	conflict := casConflict(errors.New("non-fast-forward update: refs/heads/main"), base)
	var apiErr *ghapi.APIError
	if !errors.As(conflict, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("non-fast-forward must map to 409, got %v", conflict)
	}
	conflict = casConflict(errors.New("! [rejected] main -> main (stale info)"), base)
	if !errors.As(conflict, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("stale info must map to 409, got %v", conflict)
	}
}

// listFileCommits must count only commits that CHANGE the path
// (REST commits?path= semantics), not every commit whose tree contains it.
func TestListFileCommitsOnlyTouchingCommits(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	bareDir := filepath.Join(t.TempDir(), "touch.git")
	bare, err := git.PlainInit(bareDir, true)
	if err != nil {
		t.Fatalf("init bare: %v", err)
	}
	if err := bare.Storer.SetReference(plumbNewHead()); err != nil {
		t.Fatalf("set HEAD: %v", err)
	}
	work := filepath.Join(t.TempDir(), "touchwork")
	repo, err := git.PlainInit(work, false)
	if err != nil {
		t.Fatalf("init work: %v", err)
	}
	if err := repo.Storer.SetReference(plumbNewHead()); err != nil {
		t.Fatalf("set work HEAD: %v", err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("worktree: %v", err)
	}
	commit := func(name, content, msg string) {
		t.Helper()
		p := filepath.Join(work, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := wt.Add(name); err != nil {
			t.Fatal(err)
		}
		sig := &object.Signature{Name: "test", Email: "t@e.st", When: time.Now()}
		if _, err := wt.Commit(msg, &git.CommitOptions{Author: sig, Committer: sig}); err != nil {
			t.Fatalf("commit %q: %v", msg, err)
		}
	}
	commit(metadataFilePath, "one", "index: first")
	commit("other.txt", "x", "other only") // must NOT count for metadataFilePath
	commit(metadataFilePath, "two", "index: second")
	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"file://" + bareDir}}); err != nil {
		t.Fatalf("remote: %v", err)
	}
	if err := repo.PushContext(context.Background(), &git.PushOptions{RemoteName: "origin", RefSpecs: pushMainRefSpecs()}); err != nil {
		t.Fatalf("push: %v", err)
	}
	r := newGitRepo(t.TempDir(), "owner", "touch", "")
	r.remoteBase = "file://" + bareDir
	revs, err := r.listFileCommits(context.Background(), metadataFilePath)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(revs) != 2 {
		t.Fatalf("only commits that change the path may count: got %d (%+v)", len(revs), revs)
	}
	if revs[0].Message != "index: second" || revs[1].Message != "index: first" {
		t.Fatalf("unexpected revision list %+v", revs)
	}
}

// readFileHead must answer from the HEAD tree. A file left untracked
// by a write canceled before Commit survives every HardReset; serving the
// worktree filesystem would present that ghost as HEAD truth.
func TestReadFileHeadIgnoresUntrackedGhost(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	r := newGitRepo(t.TempDir(), "owner", "ghost", "")
	r.remoteBase = url
	ctx := context.Background()
	if err := r.ensure(ctx); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := r.sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	ghost := filepath.Join(r.dir, ".storhub", "ghost.txt")
	if err := os.WriteFile(ghost, []byte("never committed"), 0o644); err != nil {
		t.Fatalf("plant ghost: %v", err)
	}
	if _, err := r.readFileHead(ctx, ".storhub/ghost.txt"); err == nil {
		t.Fatal("readFileHead must not serve an untracked worktree ghost as HEAD truth")
	}
	// Tracked reads still work.
	if data, err := r.readFileHead(ctx, metadataFilePath); err != nil || len(data) == 0 {
		t.Fatalf("tracked HEAD read broken: %v", err)
	}
}

// headCommitSHA runs off-lock against a repo that release() nils. The
// race detector must see no unsynchronized access while it is called
// concurrently with mutations.
func TestHeadCommitSHALockedAgainstRelease(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("git-backend test: heavy go-git fixture, skipped in short mode")
	}
	url := seedBareMetadataRepo(t)
	r := newGitRepo(t.TempDir(), "owner", "race", "")
	r.remoteBase = url
	ctx := context.Background()
	if err := r.ensure(ctx); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Two spinners + five push cycles + a yield per spin: the race
	// invariant is proven by ANY overlap of headCommitSHA with the
	// mutation/release path, not by millions of instrumented iterations.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = r.headCommitSHA()
					runtime.Gosched()
				}
			}
		}()
	}
	for i := 0; i < 5; i++ {
		if _, _, err := r.writeCommitPush(ctx, metadataFilePath, []byte(fmt.Sprintf(`{"v":4,"p":"race","i":%d}`, i)), fmt.Sprintf("cycle %d", i)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	close(stop)
	wg.Wait()
	if err := r.release(true); err != nil {
		t.Fatalf("release: %v", err)
	}
	for i := 0; i < 100; i++ {
		if got := r.headCommitSHA(); got != "" {
			t.Fatalf("headCommitSHA after release must report empty, got %q", got)
		}
	}
}
