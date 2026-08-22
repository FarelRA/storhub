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
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
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
)

func singleChunkTestConfig() Config {
	return Config{
		ChunkSize:              testSingleChunkSize,
		BufferSize:             testSingleBufferSize,
		MaxRetries:             0,
		MetadataCommitInterval: 100 * time.Millisecond,
		DisableGitBackend:      true,
	}
}

func defaultTestConfig() Config {
	cfg := DefaultConfig()
	cfg.DisableGitBackend = true
	return cfg
}

func smallTransferTestConfig() Config {
	return Config{
		ChunkSize:              testSmallChunkSize,
		BufferSize:             testSmallBufferSize,
		MaxRetries:             0,
		AtimePolicy:            "noatime",
		MetadataCommitInterval: 100 * time.Millisecond,
		DisableGitBackend:      true,
	}
}

func smallRetryDisabledTestConfig() Config {
	cfg := smallTransferTestConfig()
	cfg.MaxRetries = -1
	return cfg
}

func retryTestConfig() Config {
	return Config{
		MaxRetries:             2,
		BaseRetryDelay:         time.Millisecond,
		MaxRetryDelay:          5 * time.Millisecond,
		MetadataCommitInterval: 100 * time.Millisecond,
		DisableGitBackend:      true,
	}
}

func smallRetryTestConfig() Config {
	cfg := smallTransferTestConfig()
	cfg.MaxRetries = 2
	cfg.BaseRetryDelay = time.Millisecond
	cfg.MaxRetryDelay = 5 * time.Millisecond
	return cfg
}

func rateLimitTestConfig(sleep func(context.Context, time.Duration) error) Config {
	return Config{
		MaxRetries:             1,
		BaseRetryDelay:         time.Millisecond,
		MaxRetryDelay:          2 * time.Millisecond,
		Sleep:                  sleep,
		MetadataCommitInterval: 100 * time.Millisecond,
		Now:                    time.Now,
		DisableGitBackend:      true,
	}
}

func liveSmokeConfig() Config {
	return Config{ChunkSize: testLargeChunkSize, BufferSize: testLargeBufferSize, MaxRetries: 4, MetadataCommitInterval: 100 * time.Millisecond}
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
	backend := newMockGitHub(t)
	var active atomic.Int32
	var maxActive atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			current := active.Add(1)
			for {
				peak := maxActive.Load()
				if current <= peak || maxActive.CompareAndSwap(peak, current) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
			active.Add(-1)
		}
		return false
	})
	hub := backend.newClient(t, Config{ChunkSize: 32 << 20, BufferSize: testSingleBufferSize, MaxRetries: 0, MetadataCommitInterval: 100 * time.Millisecond, DisableGitBackend: true})
	ctx := context.Background()
	data := bytes.Repeat([]byte("z"), int((32<<20)*3+12345))
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 4, BufferSize: testSingleBufferSize, MaxRetries: 0, MetadataCommitInterval: 100 * time.Millisecond, DisableGitBackend: true})
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

func TestConfigDefaultsPreserveExplicitZeroRetries(t *testing.T) {
	defaults := (Config{}).WithDefaults()
	if defaults.MaxRetries != DefaultConfig().MaxRetries {
		t.Fatalf("expected zero config to use default retries, got %d", defaults.MaxRetries)
	}
	explicit := (Config{ChunkSize: chunking.DefaultChunkSize, MaxRetries: 0}).WithDefaults()
	if explicit.MaxRetries != 0 {
		t.Fatalf("expected explicit zero retries to be preserved, got %d", explicit.MaxRetries)
	}
}

func TestReplaceDeleteRollbackMetadata(t *testing.T) {
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
	firstChunkInfo := repoMeta.Chunks[first.Chunks[0]]
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
	patchedChunks0 := repoMeta.Chunks[patched.Chunks[0]]
	patchedChunks2 := repoMeta.Chunks[patched.Chunks[2]]
	metaChunks0 := repoMeta.Chunks[meta.Chunks[0]]
	patchedChunks1 := repoMeta.Chunks[patched.Chunks[1]]
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 128, BufferSize: testSingleBufferSize, MaxRetries: 0, MetadataCommitInterval: 100 * time.Millisecond, DisableGitBackend: true})
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
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
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
	metaChunk0 := repoMeta.Chunks[meta.Chunks[0]]
	patchedChunk1 := repoMeta.Chunks[patched.Chunks[1]]
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

func TestPatchFileCanSpanMultipleReleases(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "multi-release.txt", []byte("abcdefghijklmno"))
	fileMeta, err := hub.UploadFile("project-multi-release-patch", "multi-release.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	metaState, _, _ := hub.loadRepoMetadata(context.Background(), "project-multi-release-patch")
	firstRelease := metaState.Chunks[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-multi-release-patch", firstRelease, 999)
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
		chunk := metaState.Chunks[chunkName]
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

func TestPatchFileSupportsInsertGrowth(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "insert.txt", []byte("abcdij"))
	if _, err := hub.UploadFile("project-patch-insert", "insert.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	patched, err := hub.PatchFile("project-patch-insert", "insert.txt", 4, 0, []byte("efgh"))
	if err != nil {
		t.Fatalf("insert patch: %v", err)
	}
	if patched.Size != int64(len("abcdefghij")) {
		t.Fatalf("unexpected patched size: %d", patched.Size)
	}
	output := filepath.Join(t.TempDir(), "insert.out")
	if err := hub.DownloadFile("project-patch-insert", "insert.txt", output); err != nil {
		t.Fatalf("download inserted file: %v", err)
	}
	assertFileContent(t, output, []byte("abcdefghij"))
}

func TestPatchFileSupportsDeleteShrink(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "delete.txt", []byte("abcXXdef"))
	if _, err := hub.UploadFile("project-patch-delete", "delete.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	patched, err := hub.PatchFile("project-patch-delete", "delete.txt", 3, 2, nil)
	if err != nil {
		t.Fatalf("delete patch: %v", err)
	}
	if patched.Size != int64(len("abcdef")) {
		t.Fatalf("unexpected patched size: %d", patched.Size)
	}
	output := filepath.Join(t.TempDir(), "delete.out")
	if err := hub.DownloadFile("project-patch-delete", "delete.txt", output); err != nil {
		t.Fatalf("download deleted file: %v", err)
	}
	assertFileContent(t, output, []byte("abcdef"))
}

func TestPatchFileSupportsReplacingWithDifferentSize(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "resize.txt", []byte("abc123xyz"))
	if _, err := hub.UploadFile("project-patch-resize", "resize.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	patched, err := hub.PatchFile("project-patch-resize", "resize.txt", 3, 3, []byte("LONGER"))
	if err != nil {
		t.Fatalf("resize patch: %v", err)
	}
	if patched.Size != int64(len("abcLONGERxyz")) {
		t.Fatalf("unexpected patched size: %d", patched.Size)
	}
	output := filepath.Join(t.TempDir(), "resize.out")
	if err := hub.DownloadFile("project-patch-resize", "resize.txt", output); err != nil {
		t.Fatalf("download resized file: %v", err)
	}
	assertFileContent(t, output, []byte("abcLONGERxyz"))
}

func TestPatchFileSupportsTruncateToEmpty(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "empty.txt", []byte("abc"))
	if _, err := hub.UploadFile("project-patch-empty", "empty.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	patched, err := hub.PatchFile("project-patch-empty", "empty.txt", 0, 3, nil)
	if err != nil {
		t.Fatalf("truncate patch: %v", err)
	}
	if patched.Size != 0 {
		t.Fatalf("expected empty file size, got %d", patched.Size)
	}
	output := filepath.Join(t.TempDir(), "empty.out")
	if err := hub.DownloadFile("project-patch-empty", "empty.txt", output); err != nil {
		t.Fatalf("download emptied file: %v", err)
	}
	assertFileContent(t, output, []byte{})
}

func TestDeleteReleaseHidesCatalogOnly(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallRetryDisabledTestConfig())

	input := writeTempFile(t, t.TempDir(), "release.txt", []byte("release payload"))
	fileMeta, err := hub.UploadFile("project-release", "release.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-release")
	firstRelease := repoMeta.Chunks[fileMeta.Chunks[0]].Release
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
	backend := newMockGitHub(t)
	backend.repos["existing-project"] = &mockRepo{
		name:          "existing-project",
		private:       true,
		nextReleaseID: 1,
		nextAssetID:   1,
		nextBlobID:    1,
		nextCommitID:  1,
		releasesByTag: make(map[string]*mockRelease),
		releasesByID:  make(map[int64]*mockRelease),
		assets:        make(map[int64]*mockAsset),
		files:         make(map[string]*mockFile),
		commitsByPath: make(map[string][]mockCommit),
	}
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
	trackedRelease := repoMeta.Chunks[tracked.Chunks[0]].Release
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
	// Get the revision that contained orphan.txt (middle commit) to test rollback failure after purge
	revisions, err := hub.ListMetadataRevisions("project-purge")
	if err != nil {
		t.Fatalf("list metadata revisions: %v", err)
	}
	if len(revisions) < 2 {
		t.Fatalf("expected at least 2 metadata revisions, got %d", len(revisions))
	}
	orphanRevision := revisions[1] // Middle revision that had orphan.txt (revisions ordered newest-first)
	if err := hub.RollbackMetadata("project-purge", orphanRevision.CommitSHA); err == nil {
		t.Fatal("expected rollback after purge to fail because purge is destructive")
	}
}

func TestRollbackMetadataFailsWhenDataMissing(t *testing.T) {
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
	firstChunk := repoMeta.Chunks[fileMeta.Chunks[0]]
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	inputA := writeTempFile(t, t.TempDir(), "first.txt", []byte("alpha"))
	fileMeta, err := hub.UploadFile("project-capacity", "capacity.txt", inputA)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-capacity")
	firstRelease := repoMeta.Chunks[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-capacity", firstRelease, 999)
	inputB := writeTempFile(t, t.TempDir(), "second.txt", []byte("beta"))
	replaced, err := hub.ReplaceFile("project-capacity", "capacity.txt", inputB)
	if err != nil {
		t.Fatalf("replace file: %v", err)
	}
	repoMeta, _, _ = hub.loadRepoMetadata(context.Background(), "project-capacity")
	replacedRelease := repoMeta.Chunks[replaced.Chunks[0]].Release
	if replacedRelease == firstRelease {
		t.Fatalf("expected replacement to avoid full release")
	}
}

func TestUploadMissingFile(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, defaultTestConfig())
	_, err := hub.UploadFile("project-missing", "missing.txt", filepath.Join(t.TempDir(), "missing.txt"))
	if err == nil || !strings.Contains(err.Error(), "stat input file") {
		t.Fatalf("expected stat error, got %v", err)
	}
}

func TestUploadEmptyFileUsesMetadataOnly(t *testing.T) {
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, defaultTestConfig())
	err := hub.DownloadFile("project-missing", "missing.txt", filepath.Join(t.TempDir(), "missing.txt"))
	if err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
		t.Fatalf("expected project-not-found error, got %v", err)
	}
}

func TestDownloadUsesPersistedChunkOffsets(t *testing.T) {
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

func TestDownloadRejectsCorruptOutput(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	input := writeTempFile(t, t.TempDir(), "corrupt.bin", []byte("this file will be corrupted during download"))
	fileMeta, err := hub.UploadFile("project-corrupt", "corrupt.bin", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-corrupt")
	backend.corruptAsset(repoMeta.Chunks[fileMeta.Chunks[0]].AssetID)

	output := filepath.Join(t.TempDir(), "corrupt.out")
	if err := hub.DownloadFile("project-corrupt", "corrupt.bin", output); err == nil || !strings.Contains(err.Error(), "integrity") {
		t.Fatalf("expected download to fail loudly on corrupt bytes, got %v", err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("corrupt download output must be removed")
	}
}

func TestDownloadRetriesInterruptedChunkStream(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallRetryTestConfig())
	input := writeTempFile(t, t.TempDir(), "retry-download.bin", []byte("download retry payload"))
	fileMeta, err := hub.UploadFile("project-download-retry", "retry-download.bin", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-download-retry")
	assetID := repoMeta.Chunks[fileMeta.Chunks[0]].AssetID
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
		defer conn.Close()
		payload := []byte("download retry payload")
		_, _ = rw.WriteString(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n", len(payload)))
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
	backend := newMockGitHub(t)
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Path == "/user/repos" && failures.Load() == 0 {
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
		t.Fatalf("expected retry upload success, got %v", err)
	}
	if failures.Load() != 1 {
		t.Fatalf("expected one transient failure, got %d", failures.Load())
	}
}

func TestConstructorDefersAuthentication(t *testing.T) {
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
	hub, err := NewStorHubWithContext(context.Background(), "token", cfg)
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
	backend := newMockGitHub(t)
	var failures atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") && failures.Load() == 0 {
			failures.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upload issue"}`))
			return true
		}
		return false
	})
	hub := backend.newClient(t, smallRetryTestConfig())
	input := writeTempFile(t, t.TempDir(), "upload-retry.txt", []byte("retry upload payload"))
	if _, err := hub.UploadFile("project-upload-retry", "upload-retry.txt", input); err != nil {
		t.Fatalf("expected retried upload success, got %v", err)
	}
	if failures.Load() != 1 {
		t.Fatalf("expected one transient upload failure, got %d", failures.Load())
	}
}

func TestRateLimitAwareRetry(t *testing.T) {
	backend := newMockGitHub(t)
	var hits atomic.Int32
	var slept atomic.Int64
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && r.URL.Path == "/user" && hits.Load() == 0 {
			hits.Add(1)
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Second).Unix(), 10))
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
	if time.Duration(slept.Load()) < time.Second {
		t.Fatalf("expected unclamped rate-limit sleep, got %v", time.Duration(slept.Load()))
	}
}

func TestReadAPIsReturnProjectNotFound(t *testing.T) {
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
	backend := newMockGitHub(t)
	seed := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cache.txt", []byte("cache payload"))
	if _, err := seed.UploadFile("project-cache", "cache.txt", input); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	var metadataGets atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/contents/.storhub/metadata.json") {
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "cache-mutate.txt", []byte("cache mutate payload"))
	if _, err := hub.UploadFile("project-cache-mutate", "cache-mutate.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	var metadataGets atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/contents/.storhub/metadata.json") {
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, DisableGitBackend: true})
	input := writeTempFile(t, t.TempDir(), "range-read.txt", []byte("abcdefghijklmnopqrstuvwxyz"))
	fileMeta, err := hub.UploadFile("project-range-read", "range-read.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-range-read")
	assetID := repoMeta.Chunks[fileMeta.Chunks[0]].AssetID
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
		defer conn.Close()
		payload := []byte("cdefgh")
		_, _ = rw.WriteString(fmt.Sprintf("HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n", len(payload)))
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, DisableGitBackend: true})
	input := writeTempFile(t, t.TempDir(), "patch-retry.txt", []byte("abcdefghij"))
	fileMeta, err := hub.UploadFile("project-patch-range-retry", "patch-retry.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-patch-range-retry")
	assetID := repoMeta.Chunks[fileMeta.Chunks[0]].AssetID
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
		defer conn.Close()
		payload := []byte("ab")
		_, _ = rw.WriteString(fmt.Sprintf("HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n", len(payload)))
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
	backend := newMockGitHub(t)
	var failed atomic.Bool
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") && !failed.Load() {
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

	deadline := time.Now().Add(3 * time.Second)
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
		time.Sleep(20 * time.Millisecond)
	}
	repo := backend.repo("project-retry")
	if repo == nil || len(repo.assets) == 0 || repo.releasesByTag["v1"] == nil {
		t.Fatalf("expected immutable data to remain, repo=%+v", repo)
	}
}

func TestUploadRetriesMetadataConflictByReloading(t *testing.T) {
	backend := newMockGitHub(t)
	var conflicts atomic.Int32
	var commitCount atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") {
			commitCount.Add(1)
			if commitCount.Load() == 2 && conflicts.Load() == 0 {
				conflicts.Add(1)
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"message":"sha does not match"}`))
				return true
			}
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

	// Second upload - its async commit will hit a conflict, triggering reload and retry.
	input2 := writeTempFile(t, t.TempDir(), "conflict.txt", []byte("conflict payload"))
	if _, err := hub.UploadFile("project-conflict", "conflict.txt", input2); err != nil {
		t.Fatalf("upload should succeed (commit is async): %v", err)
	}
	// Wait for the background commit loop to process the conflict recovery
	time.Sleep(100 * time.Millisecond)
	if conflicts.Load() != 1 {
		t.Fatalf("expected one metadata conflict, got %d", conflicts.Load())
	}
	// After conflict, the background commit loop reloaded from GitHub and discarded
	// the conflict.txt changes. Only the committed data is visible.
	files, err := hub.ListFiles("project-conflict")
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file after conflict (uncommitted changes discarded), got %d", len(files))
	}
}

func TestMetadataCommitRetriesTransientFailure(t *testing.T) {
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
	// With batching, commit happens later. Wait for commit to trigger retry logic.
	time.Sleep(300 * time.Millisecond) // Wait for commit, failure, and retry cycle
	if failures.Load() != 1 {
		t.Fatalf("expected one transient metadata failure, got %d", failures.Load())
	}
}

func TestDownloadHonorsContextCancellation(t *testing.T) {
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
	for _, file := range meta.Files {
		if len(file.Chunks) > 0 {
			chunk := meta.Chunks[file.Chunks[0]]
			chunk.Offset = 99
			meta.Chunks[file.Chunks[0]] = chunk
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()

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
	symlink, err := hub.SymlinkContext(ctx, "project-posix", "docs/alias.txt", "docs/alias-link")
	if err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if symlink.Symlink != "docs/alias.txt" {
		t.Fatalf("unexpected symlink metadata: %+v", symlink)
	}
	target, err := hub.ReadlinkContext(ctx, "project-posix", "docs/alias-link")
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "docs/alias.txt" {
		t.Fatalf("unexpected symlink target: %q", target)
	}
	linkInfo, err := hub.StatPathContext(ctx, "project-posix", "docs/alias-link")
	if err != nil {
		t.Fatalf("stat symlink: %v", err)
	}
	if !linkInfo.IsSymlink || linkInfo.SymlinkTarget != "docs/alias.txt" {
		t.Fatalf("unexpected symlink stat: %+v", linkInfo)
	}
	if _, err := hub.ReadFileAtContext(ctx, "project-posix", "docs/alias-link", 0, 4); err == nil {
		t.Fatal("expected reading symlink as file to fail")
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
	defer fsys.Close()

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
	preCommit := filepath.Join(t.TempDir(), "fuse-precommit.out")
	if err := hub.DownloadFileContext(ctx, "project-fuse", "docs/file.txt", preCommit); err != nil {
		t.Fatalf("download before fsync: %v", err)
	}
	assertFileContent(t, preCommit, []byte("hello world"))
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
	defer fsys.Close()
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
	if value := []byte("warm"); syscall.Setxattr(renamedPath, "user.mount", value, 0) == nil {
		buf := make([]byte, 16)
		n, err := syscall.Getxattr(renamedPath, "user.mount", buf)
		if err != nil {
			t.Fatalf("get mounted xattr: %v", err)
		}
		if string(buf[:n]) != string(value) {
			t.Fatalf("unexpected mounted xattr: %q", buf[:n])
		}
	}
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("unmount fuse fs: %v", err)
	}
}

func TestFUSECloseIdempotent(t *testing.T) {
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
	defer fsys.Close()
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
	defer fsys.Close()
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
	if errno != 0 {
		t.Fatalf("read readonly replaced handle: %v", errno)
	}
	got, status = res.Bytes(buf)
	if status != 0 {
		t.Fatalf("bytes readonly replaced handle: %v", status)
	}
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
	defer fsys.Close()
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
	defer fsys.Close()
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
	defer fsys.Close()
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
	if written, errno := h.Write(ctx, []byte("XYZ"), 0); errno != 0 || written != 3 {
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
	defer fsys.Close()
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
	testFUSEEditorSaveCycle(t, "full-rewrite", func(original []byte) ([]byte, []byte) {
		first := append(append([]byte(nil), original...), 'X')
		second := append([]byte(nil), original...)
		return first, second
	})
}

func TestFUSERepeatedEditorStyleSaveCyclesWithoutSetattrHandle(t *testing.T) {
	testFUSEEditorSaveCycleWithSetattrHandle(t, "full-rewrite-no-setattr-handle", false, func(original []byte) ([]byte, []byte) {
		first := append(append([]byte(nil), original...), 'X')
		second := append([]byte(nil), original...)
		return first, second
	})
}

func TestFUSEPartialEditorRewriteSaveCycles(t *testing.T) {
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
	defer fsys.Close()
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
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var metadataWrites atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/.storhub/metadata.json") {
			metadataWrites.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			assetDownloadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, Config{
		ChunkSize:              8,
		BufferSize:             testSingleBufferSize,
		MaxRetries:             0,
		MetadataCommitInterval: 100 * time.Millisecond,
		DisableGitBackend:      true,
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
	defer fsys.Close()
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
	defer fsys.Close()
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
	t         *testing.T
	server    *httptest.Server
	mu        sync.Mutex
	owner     string
	repos     map[string]*mockRepo
	intercept atomic.Value
}

type mockRepo struct {
	name          string
	private       bool
	nextReleaseID int64
	nextAssetID   int64
	nextBlobID    int64
	nextCommitID  int64
	releasesByTag map[string]*mockRelease
	releasesByID  map[int64]*mockRelease
	assets        map[int64]*mockAsset
	files         map[string]*mockFile
	commitsByPath map[string][]mockCommit
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
}

func newMockGitHub(t *testing.T) *mockGitHub {
	t.Helper()
	backend := &mockGitHub{t: t, owner: "storhub-tester", repos: make(map[string]*mockRepo)}
	backend.server = httptest.NewServer(http.HandlerFunc(backend.serveHTTP))
	t.Cleanup(backend.server.Close)
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
	hub, err := NewStorHubWithContext(context.Background(), "token", cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() {
		hub.Shutdown(context.Background())
	})
	return hub
}

func computeGitBlobSHA(data []byte) string {
	header := fmt.Sprintf("blob %d\x00", len(data))
	h := sha1.New()
	h.Write([]byte(header))
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (m *mockGitHub) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if fn, ok := m.intercept.Load().(func(http.ResponseWriter, *http.Request) bool); ok && fn(w, r) {
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/user":
		m.writeJSON(w, http.StatusOK, map[string]any{"login": m.owner})
	case r.Method == http.MethodPost && r.URL.Path == "/user/repos":
		m.handleCreateRepo(w, r)
	case strings.HasPrefix(r.URL.Path, "/repos/"):
		m.handleRepos(w, r)
	case strings.HasPrefix(r.URL.Path, "/upload/"):
		m.handleUpload(w, r)
	default:
		http.NotFound(w, r)
	}
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
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "repository already exists"})
		return
	}
	m.repos[payload.Name] = &mockRepo{
		name:          payload.Name,
		private:       payload.Private,
		nextReleaseID: 1,
		nextAssetID:   1,
		nextBlobID:    1,
		nextCommitID:  1,
		releasesByTag: make(map[string]*mockRelease),
		releasesByID:  make(map[int64]*mockRelease),
		assets:        make(map[int64]*mockAsset),
		files:         make(map[string]*mockFile),
		commitsByPath: make(map[string][]mockCommit),
	}
	m.writeJSON(w, http.StatusCreated, map[string]any{"name": payload.Name})
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
	default:
		http.NotFound(w, r)
	}
}

func (m *mockGitHub) handleGetContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ref := r.URL.Query().Get("ref")

	// Check if client wants raw content
	acceptRaw := r.Header.Get("Accept") == "application/vnd.github.raw"

	if ref != "" {
		for _, commit := range repo.commitsByPath[filePath] {
			if commit.sha == ref {
				if acceptRaw {
					w.Header().Set("Content-Type", "application/octet-stream")
					w.WriteHeader(http.StatusOK)
					w.Write(commit.data)
				} else {
					m.writeJSON(w, http.StatusOK, map[string]any{
						"name":     filepath.Base(filePath),
						"path":     filePath,
						"sha":      fmt.Sprintf("blob-%s", commit.sha),
						"encoding": "base64",
						"type":     "file",
						"content":  base64.StdEncoding.EncodeToString(commit.data),
					})
				}
				return
			}
		}
	}
	file := repo.files[filePath]
	if file == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}

	if acceptRaw {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(file.data)
	} else {
		m.writeJSON(w, http.StatusOK, map[string]any{
			"name":     filepath.Base(filePath),
			"path":     filePath,
			"sha":      file.sha,
			"encoding": "base64",
			"type":     "file",
			"content":  base64.StdEncoding.EncodeToString(file.data),
		})
	}
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
	data, err := base64.StdEncoding.DecodeString(payload.Content)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := repo.files[filePath]
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
	repo.files[filePath] = &mockFile{path: filePath, sha: blobSHA, data: append([]byte(nil), data...)}
	commitSHA := fmt.Sprintf("commit-%d", repo.nextCommitID)
	repo.nextCommitID++
	commit := mockCommit{sha: commitSHA, message: payload.Message, path: filePath, data: append([]byte(nil), data...), when: time.Unix(1700000000+repo.nextCommitID, 0).UTC()}
	repo.commitsByPath[filePath] = append([]mockCommit{commit}, repo.commitsByPath[filePath]...)
	m.writeJSON(w, http.StatusOK, map[string]any{
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
		"assets":     m.releaseAssetsLocked(repo, release.tag),
	})
}

func (m *mockGitHub) handleListReleases(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	releases := make([]*mockRelease, 0, len(repo.releasesByTag))
	for _, release := range repo.releasesByTag {
		releases = append(releases, release)
	}
	sort.Slice(releases, func(i, j int) bool { return releases[i].tag < releases[j].tag })
	pageReleases := paginateSlice(releases, r.URL.Query())
	response := make([]map[string]any, 0, len(pageReleases))
	for _, release := range pageReleases {
		assets := m.releaseAssetsLocked(repo, release.tag)
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

func (m *mockGitHub) releaseAssetsLocked(repo *mockRepo, tag string) []map[string]any {
	assets := make([]map[string]any, 0)
	for _, asset := range repo.assets {
		if asset.releaseTag == tag {
			assets = append(assets, map[string]any{"id": asset.id, "name": asset.name, "size": len(asset.data)})
		}
	}
	return assets
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
	asset := &mockAsset{id: repo.nextAssetID, name: name, releaseTag: parts[2], data: append([]byte(nil), data...)}
	repo.nextAssetID++
	repo.assets[asset.id] = asset
	m.writeJSON(w, http.StatusCreated, map[string]any{"id": asset.id, "name": asset.name})
}

func (m *mockGitHub) handleDownloadAsset(w http.ResponseWriter, r *http.Request, repo *mockRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	asset := repo.assets[assetID]
	if asset == nil {
		m.mu.Unlock()
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	data := append([]byte(nil), asset.data...)
	m.mu.Unlock()
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
	if _, ok := repo.assets[assetID]; !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	delete(repo.assets, assetID)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) handleDeleteRepo(w http.ResponseWriter, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.repos, name)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) repo(name string) *mockRepo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.repos[name]
}

func (m *mockGitHub) corruptAsset(assetID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, repo := range m.repos {
		if asset := repo.assets[assetID]; asset != nil && len(asset.data) > 0 {
			asset.data[0] ^= 0xFF
			return
		}
	}
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
	asset := &mockAsset{id: repo.nextAssetID, name: name, releaseTag: tag, data: append([]byte(nil), data...)}
	repo.nextAssetID++
	repo.assets[asset.id] = asset
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
	delete(repo.assets, assetID)
}

func (m *mockGitHub) setMetadata(t *testing.T, project string, meta RepoMetadata) {
	t.Helper()
	repo := m.repo(project)
	if repo == nil {
		t.Fatalf("repo %s not found", project)
	}
	payload, err := meta.ToJSON()
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	file := repo.files[metadataFilePath]
	if file == nil {
		file = &mockFile{path: metadataFilePath}
		repo.files[metadataFilePath] = file
	}
	file.data = append([]byte(nil), payload...)
	file.sha = fmt.Sprintf("blob-%d", repo.nextBlobID)
	repo.nextBlobID++
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

func mustLoadMetadata(t *testing.T, repo *mockRepo) RepoMetadata {
	t.Helper()
	file := repo.files[metadataFilePath]
	if file == nil {
		return RepoMetadata{}
	}
	var meta RepoMetadata
	if err := meta.FromJSON(file.data); err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	return meta
}

func (m *mockGitHub) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
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
	perPage, _ := strconv.Atoi(query.Get("per_page"))
	page, _ := strconv.Atoi(query.Get("page"))
	if perPage <= 0 {
		perPage = len(items)
	}
	if page <= 0 {
		page = 1
	}
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
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "spill.txt", []byte("abcdefghijklmno"))
	fileMeta, err := hub.UploadFile("project-purge-spill", "spill.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	metaState, _, _ := hub.loadRepoMetadata(context.Background(), "project-purge-spill")
	firstRelease := metaState.Chunks[fileMeta.Chunks[0]].Release
	backend.addAssetsToRelease(t, "project-purge-spill", firstRelease, 999)

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
		if chunk := metaState.Chunks[id]; chunk.Release != firstRelease {
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

// Downloaded chunks are verified against the SHA-256 digest recorded at
// upload time; silent bit-rot must fail loudly instead of serving corrupt bytes.
func TestDownloadDetectsCorruptedChunkDigest(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "digest.txt", []byte("integrity matters"))
	fileMeta, err := hub.UploadFile("project-digest", "digest.txt", input)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	repoMeta, _, _ := hub.loadRepoMetadata(context.Background(), "project-digest")
	chunkID := fileMeta.Chunks[0]
	chunk := repoMeta.Chunks[chunkID]
	if chunk.Digest == "" {
		t.Fatal("expected upload to record a chunk digest")
	}

	// Flip one byte in every asset backing this file.
	backend.mu.Lock()
	for _, id := range fileMeta.Chunks {
		asset := backend.repos["project-digest"].assets[repoMeta.Chunks[id].AssetID]
		if asset != nil && len(asset.data) > 0 {
			asset.data[0] ^= 0xFF
		}
	}
	backend.mu.Unlock()

	output := filepath.Join(t.TempDir(), "digest.out")
	err = hub.DownloadFile("project-digest", "digest.txt", output)
	if err == nil || !strings.Contains(err.Error(), "integrity") {
		t.Fatalf("expected integrity failure, got %v", err)
	}
}
