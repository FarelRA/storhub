package storage

// FUSE writeback paths: partial, append, truncate, and fragmented writes.
import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestFUSEPartialWritebackAvoidsFullMaterializeAndReupload(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
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
	if _, err := hub.UploadFileContext(ctx, "projectfusepartialwriteback", "large.txt", input); err != nil {
		t.Fatalf("upload large file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("projectfusepartialwriteback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfusepartialwriteback", "large.txt")
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
	if err := hub.DownloadFileContext(ctx, "projectfusepartialwriteback", "large.txt", output); err != nil {
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
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
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
	if _, err := hub.UploadFileContext(ctx, "projectfuseappendwriteback", "append.txt", input); err != nil {
		t.Fatalf("upload append file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("projectfuseappendwriteback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfuseappendwriteback", "append.txt")
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
	if err := hub.DownloadFileContext(ctx, "projectfuseappendwriteback", "append.txt", output); err != nil {
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
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "truncate.txt", []byte("abcdefghijklmnop"))
	if _, err := hub.UploadFileContext(ctx, "projectfusetruncatewriteback", "truncate.txt", input); err != nil {
		t.Fatalf("upload truncate file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("projectfusetruncatewriteback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfusetruncatewriteback", "truncate.txt")
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
	if err := hub.DownloadFileContext(ctx, "projectfusetruncatewriteback", "truncate.txt", output); err != nil {
		t.Fatalf("download truncated file: %v", err)
	}
	assertFileContent(t, output, []byte("abcde"))
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release truncate handle: %v", errno)
	}
}

func TestFUSEFragmentedWritebackUploadsTouchedChunks(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var metadataWrites atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
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
	if _, err := hub.UploadFileContext(ctx, "projectfusefragmentedwriteback", "fragmented.txt", input); err != nil {
		t.Fatalf("upload fragmented file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	baselineMetadataWrites := metadataWrites.Load()
	fsys, err := hub.NewFUSE("projectfusefragmentedwriteback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfusefragmentedwriteback", "fragmented.txt")
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
	if err := hub.DownloadFileContext(ctx, "projectfusefragmentedwriteback", "fragmented.txt", output); err != nil {
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
