package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func defaultTestConfig() Config {
	cfg := DefaultConfig()
	cfg.DisableGitBackend = true
	return cfg
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
	meta, err := hub.UploadFileContext(context.Background(), "project-a", "single.txt", input)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if len(meta.Chunks) != 1 {
		t.Fatalf("unexpected metadata: %+v", meta)
	}

	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}

	files, err := hub.ListFilesContext(context.Background(), "project-a")
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("unexpected files: %+v", files)
	}

	output := filepath.Join(t.TempDir(), "downloaded.txt")
	if err := hub.DownloadFileContext(context.Background(), "project-a", "single.txt", output); err != nil {
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
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
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

func TestUploadMissingFile(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, defaultTestConfig())
	_, err := hub.UploadFileContext(context.Background(), "project-missing", "missing.txt", filepath.Join(t.TempDir(), "missing.txt"))
	if err == nil || !strings.Contains(err.Error(), "stat input file") {
		t.Fatalf("expected stat error, got %v", err)
	}
}

func TestUploadEmptyFileUsesMetadataOnly(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "empty-upload.txt", nil)
	meta, err := hub.UploadFileContext(context.Background(), "project-empty-upload-file", "empty-upload.txt", input)
	if err != nil {
		t.Fatalf("upload empty file: %v", err)
	}
	if meta.Size != 0 || len(meta.Chunks) != 0 {
		t.Fatalf("unexpected empty upload metadata: %+v", meta)
	}
	output := filepath.Join(t.TempDir(), "empty-upload.out")
	if err := hub.DownloadFileContext(context.Background(), "project-empty-upload-file", "empty-upload.txt", output); err != nil {
		t.Fatalf("download empty file: %v", err)
	}
	assertFileContent(t, output, []byte{})
}

func TestDownloadMissingFile(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, defaultTestConfig())
	err := hub.DownloadFileContext(context.Background(), "project-missing", "missing.txt", filepath.Join(t.TempDir(), "missing.txt"))
	if err == nil || !strings.Contains(err.Error(), shfs.ErrNotFound.Error()) {
		t.Fatalf("expected project-not-found error, got %v", err)
	}
}

func TestDownloadUsesPersistedChunkOffsets(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	uploader := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "offsets.bin", []byte("abcdefghijklmnopqrstuvwxyz0123456789"))
	meta, err := uploader.UploadFileContext(context.Background(), "project-offsets", "offsets.bin", input)
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
	if err := downloader.DownloadFileContext(context.Background(), "project-offsets", "offsets.bin", output); err != nil {
		t.Fatalf("download file with different chunk size config: %v", err)
	}
	assertFileContent(t, output, []byte("abcdefghijklmnopqrstuvwxyz0123456789"))
}

func TestReadFileAtHandlesEOFAndPartialRanges(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "partial.txt", []byte("abcdefghij"))
	if _, err := hub.UploadFileContext(context.Background(), "project-read-partial", "partial.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	data, err := hub.ReadFileAtContext(context.Background(), "project-read-partial", "partial.txt", 7, 10)
	if err != nil {
		t.Fatalf("partial read: %v", err)
	}
	if string(data) != "hij" {
		t.Fatalf("unexpected partial range: %q", data)
	}
	endData, err := hub.ReadFileAtContext(context.Background(), "project-read-partial", "partial.txt", 10, 1)
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
	fileMeta, err := hub.UploadFileContext(context.Background(), "project-download-retry", "retry-download.bin", input)
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
	if err := hub.DownloadFileContext(context.Background(), "project-download-retry", "retry-download.bin", output); err != nil {
		t.Fatalf("download with retry: %v", err)
	}
	if failures.Load() != 1 {
		t.Fatalf("expected one interrupted download, got %d", failures.Load())
	}
	assertFileContent(t, output, []byte("download retry payload"))
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
	meta, err := hub.UploadFileContext(context.Background(), "project-upload-retry", "upload-retry.txt", input)
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
	if _, err := hub2.UploadFileContext(context.Background(), "project-upload-doomed", "upload-broken.txt", input2); err == nil {
		t.Fatal("persistent upload failure must surface")
	}
	// initial attempt + MaxRetries(2) retries
	if got := attempts.Load(); got != 3 {
		t.Fatalf("persistent failure must exhaust retries, attempts=%d", got)
	}
}

func TestReadFileAtRetriesInterruptedRangeRead(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 1, BaseRetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond, DisableGitBackend: true})
	input := writeTempFile(t, t.TempDir(), "range-read.txt", []byte("abcdefghijklmnopqrstuvwxyz"))
	fileMeta, err := hub.UploadFileContext(context.Background(), "project-range-read", "range-read.txt", input)
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
	data, err := hub.ReadFileAtContext(context.Background(), "project-range-read", "range-read.txt", 2, 6)
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

func TestDownloadHonorsContextCancellation(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	assetStarted := make(chan struct{}, 1)
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
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
	if _, err := hub.UploadFileContext(context.Background(), "project-cancel", "cancel.txt", input); err != nil {
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
