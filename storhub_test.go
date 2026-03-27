package storhub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testSmallChunkSize   int64 = 8
	testSmallBufferSize        = 16
	testSingleChunkSize  int64 = 1024
	testSingleBufferSize       = 32
	testLargeChunkSize   int64 = 1 << 30
	testLargeBufferSize        = 4 << 20
	testLargeConcurrency       = 2
)

func singleChunkTestConfig() Config {
	return Config{ChunkSize: testSingleChunkSize, BufferSize: testSingleBufferSize, MaxConcurrentTransfers: 2, MaxRetries: 0}
}

func defaultTestConfig() Config {
	return DefaultConfig()
}

func smallTransferTestConfig() Config {
	return Config{ChunkSize: testSmallChunkSize, BufferSize: testSmallBufferSize, MaxRetries: 0}
}

func smallRetryDisabledTestConfig() Config {
	cfg := smallTransferTestConfig()
	cfg.MaxRetries = -1
	return cfg
}

func smallConcurrentTestConfig() Config {
	cfg := smallTransferTestConfig()
	cfg.MaxConcurrentTransfers = 2
	return cfg
}

func retryTestConfig() Config {
	return Config{MaxRetries: 2, BaseRetryDelay: time.Millisecond, MaxRetryDelay: 5 * time.Millisecond}
}

func smallRetryTestConfig() Config {
	cfg := smallTransferTestConfig()
	cfg.MaxRetries = 2
	cfg.BaseRetryDelay = time.Millisecond
	cfg.MaxRetryDelay = 5 * time.Millisecond
	return cfg
}

func rateLimitTestConfig(sleep func(context.Context, time.Duration) error) Config {
	return Config{MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: 2 * time.Millisecond, Sleep: sleep}
}

func liveSmokeConfig() Config {
	return Config{ChunkSize: testLargeChunkSize, BufferSize: testLargeBufferSize, MaxConcurrentTransfers: testLargeConcurrency, MaxRetries: 4}
}

func liveLargeSmokeConfig(client *http.Client) Config {
	cfg := liveSmokeConfig()
	cfg.HTTPClient = client
	cfg.ChunkSize = MaxReleaseAssetSize
	cfg.MaxConcurrentTransfers = 1
	return cfg
}

func largeValidationConfig() Config {
	return defaultTestConfig()
}

func expectedChunkCount(size, chunkSize int64) int {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize > MaxReleaseAssetSize {
		chunkSize = MaxReleaseAssetSize
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

func TestUploadListDownloadSingleChunk(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())

	input := writeTempFile(t, t.TempDir(), "single.txt", []byte("hello streaming world"))
	meta, err := hub.UploadFile("project-a", "single.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if len(meta.Chunks) != 1 || meta.CRC32C == "" {
		t.Fatalf("unexpected metadata: %+v", meta)
	}

	files, err := hub.ListFiles("project-a")
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 || files[0].Name != "single.txt" {
		t.Fatalf("unexpected files: %+v", files)
	}

	output := filepath.Join(t.TempDir(), "downloaded.txt")
	if err := hub.DownloadFile("project-a", "single.txt", output); err != nil {
		t.Fatalf("download file: %v", err)
	}
	assertFileContent(t, output, []byte("hello streaming world"))
	assertIntegrity(t, output, *meta)
	backend.assertRepoStats(t, "project-a", 1, int64(len("hello streaming world")))
	if repo := backend.repo("project-a"); repo == nil || !repo.private {
		t.Fatal("expected repositories to be private by default")
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
	hub := backend.newClient(t, Config{ChunkSize: 4, BufferSize: testSingleBufferSize, MaxConcurrentTransfers: 2, MaxRetries: 0})
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
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		return false
	}
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
	if meta.CRC32C != formatCRC32C(0) {
		t.Fatalf("unexpected empty-file crc32c: %s", meta.CRC32C)
	}
	if uploadCalls.Load() != 0 {
		t.Fatalf("expected no asset uploads for empty file, got %d", uploadCalls.Load())
	}
}

func TestFilesystemEdgeCasesAndRootSemantics(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	if err := hub.Mkdir("project-fs-edge", "."); err != nil {
		t.Fatalf("mkdir root should be a no-op: %v", err)
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
	defaults := (Config{}).withDefaults()
	if defaults.MaxRetries != DefaultConfig().MaxRetries {
		t.Fatalf("expected zero config to use default retries, got %d", defaults.MaxRetries)
	}
	explicit := (Config{ChunkSize: DefaultChunkSize, MaxRetries: 0}).withDefaults()
	if explicit.MaxRetries != 0 {
		t.Fatalf("expected explicit zero retries to be preserved, got %d", explicit.MaxRetries)
	}
}

func TestReplaceDeleteRollbackMetadata(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallRetryDisabledTestConfig())

	inputA := writeTempFile(t, t.TempDir(), "v1.txt", []byte("version-a"))
	first, err := hub.UploadFile("project-history", "artifact.txt", inputA)
	if err != nil {
		t.Fatalf("upload first: %v", err)
	}

	inputB := writeTempFile(t, t.TempDir(), "v2.txt", []byte("version-b-better"))
	second, err := hub.ReplaceFile("project-history", "artifact.txt", inputB)
	if err != nil {
		t.Fatalf("replace file: %v", err)
	}
	if second.CRC32C == first.CRC32C {
		t.Fatal("expected replacement to change integrity")
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
	if repo == nil || repo.releasesByTag[first.Release] == nil {
		t.Fatalf("expected immutable release %s to remain", first.Release)
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
	if len(files) != 1 || files[0].CRC32C != first.CRC32C {
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
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxConcurrentTransfers: 2, MaxRetries: 0})
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
	if patched.Chunks[0].AssetID != meta.Chunks[0].AssetID || patched.Chunks[2].AssetID != meta.Chunks[0].AssetID {
		t.Fatalf("expected unchanged data to reuse original asset, got %+v", patched.Chunks)
	}
	if patched.Chunks[1].AssetID == meta.Chunks[0].AssetID {
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
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxConcurrentTransfers: 2, MaxRetries: 1})
	input := writeTempFile(t, t.TempDir(), "ranges.txt", []byte("abcdefghij"))
	if _, err := hub.UploadFile("project-range-patch", "ranges.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if _, err := hub.PatchFile("project-range-patch", "ranges.txt", 4, 2, []byte("ZZ")); err != nil {
		t.Fatalf("patch file: %v", err)
	}
	var sawRange atomic.Bool
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") && r.Header.Get("Range") != "" {
			sawRange.Store(true)
		}
		return false
	}
	output := filepath.Join(t.TempDir(), "ranges.out")
	if err := hub.DownloadFile("project-range-patch", "ranges.txt", output); err != nil {
		t.Fatalf("download patched file: %v", err)
	}
	if !sawRange.Load() {
		t.Fatal("expected patched download to use range requests")
	}
	assertFileContent(t, output, []byte("abcdZZghij"))
}

func TestPatchFileCanSpanMultipleReleases(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "multi-release.txt", []byte("abcdefghijklmno"))
	meta, err := hub.UploadFile("project-multi-release-patch", "multi-release.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	backend.addAssetsToRelease(t, "project-multi-release-patch", meta.Release, 999)
	patched, err := hub.PatchFile("project-multi-release-patch", "multi-release.txt", 4, 4, []byte("ZZZZ"))
	if err != nil {
		t.Fatalf("patch file: %v", err)
	}
	seenOld := false
	seenNew := false
	for _, chunk := range patched.Chunks {
		if chunk.Release == meta.Release {
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
	metaState, _, err := hub.loadRepoMetadata(context.Background(), "project-multi-release-patch")
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if metaState.GetRelease(meta.Release) == nil {
		t.Fatalf("expected old release %s to remain referenced in metadata", meta.Release)
	}
	if err := hub.DeleteRelease("project-multi-release-patch", meta.Release); err == nil {
		t.Fatal("expected deleting a referenced-only release to fail")
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
	meta, err := hub.UploadFile("project-release", "release.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := hub.DeleteRelease("project-release", meta.Release); err != nil {
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
	if repo == nil || repo.releasesByTag[meta.Release] == nil {
		t.Fatalf("expected immutable release %s to remain", meta.Release)
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

func TestPurgeUntrackedRemovesOrphanedAssetsAndReleases(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallRetryDisabledTestConfig())

	inputA := writeTempFile(t, t.TempDir(), "tracked.txt", []byte("tracked payload"))
	tracked, err := hub.UploadFile("project-purge", "tracked.txt", inputA)
	if err != nil {
		t.Fatalf("upload tracked file: %v", err)
	}

	inputB := writeTempFile(t, t.TempDir(), "orphan.txt", []byte("orphan payload"))
	orphan, err := hub.UploadFile("project-purge", "orphan.txt", inputB)
	if err != nil {
		t.Fatalf("upload orphan file: %v", err)
	}
	if err := hub.DeleteFile("project-purge", "orphan.txt"); err != nil {
		t.Fatalf("hide orphan file: %v", err)
	}

	repo := backend.repo("project-purge")
	if repo == nil {
		t.Fatal("expected repo to exist")
	}
	manualRelease := backend.addRelease(t, "project-purge", "v999")
	backend.addAssetToRelease(t, "project-purge", manualRelease.tag, "manual.bin", []byte("manual orphan"))
	backend.addAssetToRelease(t, "project-purge", tracked.Release, "extra.bin", []byte("extra orphan"))

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
	if repo.releasesByTag[tracked.Release] == nil {
		t.Fatalf("expected tracked release %s to remain", tracked.Release)
	}
	oldest := mustMetadataRevision(t, hub, "project-purge", "storhub: add orphan.txt")
	if err := hub.RollbackMetadata("project-purge", oldest.CommitSHA); err == nil {
		t.Fatal("expected rollback after purge to fail because purge is destructive")
	}
}

func TestRollbackMetadataFailsWhenDataMissing(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "missing-data.txt", []byte("payload"))
	meta, err := hub.UploadFile("project-missing-data", "missing-data.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if err := hub.DeleteFile("project-missing-data", "missing-data.txt"); err != nil {
		t.Fatalf("hide file: %v", err)
	}
	backend.removeAsset(t, "project-missing-data", meta.Chunks[0].AssetID)
	revision := mustMetadataRevision(t, hub, "project-missing-data", "storhub: add missing-data.txt")
	if err := hub.RollbackMetadata("project-missing-data", revision.CommitSHA); err == nil {
		t.Fatal("expected rollback to fail when referenced asset is missing")
	}
}

func TestReplaceAvoidsFullPreferredRelease(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	inputA := writeTempFile(t, t.TempDir(), "first.txt", []byte("alpha"))
	meta, err := hub.UploadFile("project-capacity", "capacity.txt", inputA)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	backend.addAssetsToRelease(t, "project-capacity", meta.Release, 999)
	inputB := writeTempFile(t, t.TempDir(), "second.txt", []byte("beta"))
	replaced, err := hub.ReplaceFile("project-capacity", "capacity.txt", inputB)
	if err != nil {
		t.Fatalf("replace file: %v", err)
	}
	if replaced.Release == meta.Release {
		t.Fatalf("expected replacement to avoid full release %s", meta.Release)
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
	if meta.Size != 0 || len(meta.Chunks) != 0 || meta.CRC32C != formatCRC32C(0) {
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
	if err == nil || !strings.Contains(err.Error(), ErrProjectNotFound.Error()) {
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

func TestDownloadRemovesCorruptOutput(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallConcurrentTestConfig())

	input := writeTempFile(t, t.TempDir(), "corrupt.bin", []byte("this file will be corrupted during download"))
	meta, err := hub.UploadFile("project-corrupt", "corrupt.bin", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	backend.corruptAsset(meta.Chunks[0].AssetID)

	output := filepath.Join(t.TempDir(), "corrupt.out")
	err = hub.DownloadFile("project-corrupt", "corrupt.bin", output)
	if err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("expected integrity mismatch, got %v", err)
	}
	if _, statErr := os.Stat(output); !os.IsNotExist(statErr) {
		t.Fatalf("expected direct target file to be absent after integrity failure, stat err=%v", statErr)
	}
}

func TestDownloadRetriesInterruptedChunkStream(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallRetryTestConfig())
	input := writeTempFile(t, t.TempDir(), "retry-download.bin", []byte("download retry payload"))
	meta, err := hub.UploadFile("project-download-retry", "retry-download.bin", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	var failures atomic.Int32
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/releases/assets/%d", meta.Chunks[0].AssetID)) || failures.Load() != 0 {
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
	}
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
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && r.URL.Path == "/user/repos" && failures.Load() == 0 {
			failures.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upstream issue"}`))
			return true
		}
		return false
	}
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
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && r.URL.Path == "/user" {
			hits.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"bad credentials"}`))
			return true
		}
		return false
	}
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
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") && failures.Load() == 0 {
			failures.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary upload issue"}`))
			return true
		}
		return false
	}
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
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
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
	}
	hub := backend.newClient(t, rateLimitTestConfig(func(_ context.Context, d time.Duration) error {
		slept.Store(int64(d))
		return nil
	}))
	if hub.Owner() != "" {
		t.Fatalf("expected lazy owner resolution, got %s", hub.Owner())
	}
	if _, err := hub.ListFiles("project-rate-limit-miss"); err == nil || !strings.Contains(err.Error(), ErrProjectNotFound.Error()) {
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
		if err := check.fn(); err == nil || !strings.Contains(err.Error(), ErrProjectNotFound.Error()) {
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
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/contents/.storhub/metadata.json") {
			metadataGets.Add(1)
		}
		return false
	}
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
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/contents/.storhub/metadata.json") {
			metadataGets.Add(1)
		}
		return false
	}
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
	if _, err := hub.ListFiles("project-cache-mutate"); err == nil || !strings.Contains(err.Error(), ErrProjectNotFound.Error()) {
		t.Fatalf("expected project-not-found after delete project, got %v", err)
	}
	if metadataGets.Load() <= beforePostDeleteList {
		t.Fatal("expected metadata fetch after cache invalidation from delete project")
	}
}

func TestRunConcurrentReturnsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- runConcurrent(ctx, 2, 4, func(i int) error {
			if i == 0 {
				select {
				case started <- struct{}{}:
				default:
				}
			}
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-started
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestReadFileAtRetriesInterruptedRangeRead(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxConcurrentTransfers: 1, MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond})
	input := writeTempFile(t, t.TempDir(), "range-read.txt", []byte("abcdefghijklmnopqrstuvwxyz"))
	meta, err := hub.UploadFile("project-range-read", "range-read.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	var failures atomic.Int32
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/releases/assets/%d", meta.Chunks[0].AssetID)) || r.Header.Get("Range") == "" || failures.Load() != 0 {
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
	}
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
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxConcurrentTransfers: 1, MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond})
	input := writeTempFile(t, t.TempDir(), "patch-retry.txt", []byte("abcdefghij"))
	meta, err := hub.UploadFile("project-patch-range-retry", "patch-retry.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	var failures atomic.Int32
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, fmt.Sprintf("/releases/assets/%d", meta.Chunks[0].AssetID)) || r.Header.Get("Range") == "" || failures.Load() != 0 {
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
	}
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
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
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
	}
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

func TestCalculateCRC32CHelpersHandleZeroBuffer(t *testing.T) {
	data := []byte("crc32c helper payload")
	reader := bytes.NewReader(data)
	sum, err := CalculateCRC32CReader(reader, 0)
	if err != nil {
		t.Fatalf("calculate crc32c reader: %v", err)
	}
	chunked, err := CalculateChunkedIntegrityReader(bytes.NewReader(data), FileMetadata{
		Name:   "payload.bin",
		Size:   int64(len(data)),
		CRC32C: sum,
		Chunks: []ChunkInfo{{Index: 0, Offset: 0, Size: int64(len(data)), CRC32C: sum}},
	}, 0)
	if err != nil {
		t.Fatalf("calculate chunked integrity reader: %v", err)
	}
	if chunked != sum {
		t.Fatalf("unexpected combined crc32c: %s", chunked)
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

func TestUploadMetadataCommitFailureKeepsDataHidden(t *testing.T) {
	backend := newMockGitHub(t)
	var failed atomic.Bool
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") && !failed.Load() {
			failed.Store(true)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"metadata write failed"}`))
			return true
		}
		return false
	}
	hub := backend.newClient(t, smallRetryDisabledTestConfig())
	input := writeTempFile(t, t.TempDir(), "rollback.txt", []byte("rollback payload"))
	if _, err := hub.UploadFile("project-hidden", "rollback.txt", input); err == nil {
		t.Fatal("expected upload failure")
	}
	files, err := hub.ListFiles("project-hidden")
	if err != nil {
		t.Fatalf("list files after failed upload: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected hidden data after failed metadata commit, got %+v", files)
	}
	repo := backend.repo("project-hidden")
	if repo == nil || len(repo.assets) == 0 || repo.releasesByTag["v1"] == nil {
		t.Fatalf("expected immutable data to remain after failed metadata commit, repo=%+v", repo)
	}
}

func TestUploadRetriesMetadataConflictByReloading(t *testing.T) {
	backend := newMockGitHub(t)
	var conflicts atomic.Int32
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") && conflicts.Load() == 0 {
			conflicts.Add(1)
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"sha does not match"}`))
			return true
		}
		return false
	}
	hub := backend.newClient(t, smallRetryTestConfig())
	input := writeTempFile(t, t.TempDir(), "conflict.txt", []byte("conflict payload"))
	if _, err := hub.UploadFile("project-conflict", "conflict.txt", input); err != nil {
		t.Fatalf("upload with metadata conflict retry: %v", err)
	}
	if conflicts.Load() != 1 {
		t.Fatalf("expected one metadata conflict, got %d", conflicts.Load())
	}
}

func TestMetadataCommitRetriesTransientFailure(t *testing.T) {
	backend := newMockGitHub(t)
	var failures atomic.Int32
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/") && failures.Load() == 0 {
			failures.Add(1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"message":"temporary metadata issue"}`))
			return true
		}
		return false
	}
	hub := backend.newClient(t, smallRetryTestConfig())
	input := writeTempFile(t, t.TempDir(), "meta-retry.txt", []byte("meta retry payload"))
	if _, err := hub.UploadFile("project-meta-retry", "meta-retry.txt", input); err != nil {
		t.Fatalf("expected metadata commit retry success, got %v", err)
	}
	if failures.Load() != 1 {
		t.Fatalf("expected one transient metadata failure, got %d", failures.Load())
	}
}

func TestDownloadHonorsContextCancellation(t *testing.T) {
	backend := newMockGitHub(t)
	assetStarted := make(chan struct{}, 1)
	backend.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			select {
			case assetStarted <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return true
		}
		return false
	}
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

func TestListMetadataRevisionsPaginates(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	for i := 0; i < 105; i++ {
		name := fmt.Sprintf("file-%03d.txt", i)
		input := writeTempFile(t, t.TempDir(), name, []byte(name))
		if _, err := hub.UploadFile("project-history-pages", name, input); err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
	}
	revisions, err := hub.ListMetadataRevisions("project-history-pages")
	if err != nil {
		t.Fatalf("list metadata revisions: %v", err)
	}
	if len(revisions) != 105 {
		t.Fatalf("expected 105 metadata revisions, got %d", len(revisions))
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
	meta := mustLoadMetadata(t, backend.repo("project-invalid-metadata"))
	meta.Releases[0].Files[0].Chunks[0].Offset = 99
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
	repo := backend.repo("project-cleanup")
	before := len(repo.commitsByPath[metadataFilePath])
	if err := hub.CleanupProject("project-cleanup"); err != nil {
		t.Fatalf("cleanup project: %v", err)
	}
	after := len(repo.commitsByPath[metadataFilePath])
	if after != before {
		t.Fatalf("expected cleanup noop commit count to stay %d, got %d", before, after)
	}
}

type mockGitHub struct {
	t         *testing.T
	server    *httptest.Server
	mu        sync.Mutex
	owner     string
	repos     map[string]*mockRepo
	intercept func(http.ResponseWriter, *http.Request) bool
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
	return hub
}

func (m *mockGitHub) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if m.intercept != nil && m.intercept(w, r) {
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
	if ref != "" {
		for _, commit := range repo.commitsByPath[filePath] {
			if commit.sha == ref {
				m.writeJSON(w, http.StatusOK, map[string]any{
					"name":     filepath.Base(filePath),
					"path":     filePath,
					"sha":      fmt.Sprintf("blob-%s", commit.sha),
					"encoding": "base64",
					"type":     "file",
					"content":  base64.StdEncoding.EncodeToString(commit.data),
				})
				return
			}
		}
	}
	file := repo.files[filePath]
	if file == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	m.writeJSON(w, http.StatusOK, map[string]any{
		"name":     filepath.Base(filePath),
		"path":     filePath,
		"sha":      file.sha,
		"encoding": "base64",
		"type":     "file",
		"content":  base64.StdEncoding.EncodeToString(file.data),
	})
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
	blobSHA := fmt.Sprintf("blob-%d", repo.nextBlobID)
	repo.nextBlobID++
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
	m.mu.Unlock()
	if asset == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	start, end, partial, err := resolveByteRange(r.Header.Get("Range"), int64(len(asset.data)))
	if err != nil {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	body := asset.data
	status := http.StatusOK
	if partial {
		body = asset.data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(asset.data)))
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

func assertIntegrity(t *testing.T, path string, metadata FileMetadata) {
	t.Helper()
	if err := VerifyFileIntegrity(path, metadata, DefaultBufferSize); err != nil {
		t.Fatalf("verify integrity for %s: %v", path, err)
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
