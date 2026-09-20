package fusefs

import (
	"context"
	"os"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestFallocateExtendsAndCommitsZeros(t *testing.T) {
	t.Parallel()
	var replacedBytes []byte
	fsys, err := New(&stubHub{
		replaceFile: func(_ context.Context, _ string, _ string, inputPath string) (*meta.FileMeta, error) {
			data, readErr := os.ReadFile(inputPath)
			replacedBytes = data
			return &meta.FileMeta{Size: int64(len(data))}, readErr
		},
	}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if errno := h.Allocate(context.Background(), 0, 1000, 0); errno != 0 {
		t.Fatalf("allocate: %v", errno)
	}
	if h.writeState.logicalSize != 1000 {
		t.Fatalf("logical size after allocate: %d", h.writeState.logicalSize)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release handle: %v", errno)
	}
	if len(replacedBytes) != 1000 {
		t.Fatalf("committed size: %d", len(replacedBytes))
	}
	for i, b := range replacedBytes {
		if b != 0 {
			t.Fatalf("committed byte %d = %#x, want zero", i, b)
		}
	}
}

func TestFallocateKeepSizeLeavesLogicalSize(t *testing.T) {
	t.Parallel()
	var replacedBytes []byte
	fsys, err := New(&stubHub{
		replaceFile: func(_ context.Context, _ string, _ string, inputPath string) (*meta.FileMeta, error) {
			data, readErr := os.ReadFile(inputPath)
			replacedBytes = data
			return &meta.FileMeta{Size: int64(len(data))}, readErr
		},
	}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_RDWR, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("hello"), 0); errno != 0 || n != 5 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.Allocate(context.Background(), 1<<20, 4096, fallocFlKeepSize); errno != 0 {
		t.Fatalf("keep-size allocate: %v", errno)
	}
	if h.writeState.logicalSize != 5 {
		t.Fatalf("logical size after keep-size allocate: %d", h.writeState.logicalSize)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release handle: %v", errno)
	}
	if string(replacedBytes) != "hello" {
		t.Fatalf("committed payload changed: %q", replacedBytes)
	}
}

func TestFallocateInsideExistingSizeKeepsContent(t *testing.T) {
	t.Parallel()
	var replacedBytes []byte
	fsys, err := New(&stubHub{
		replaceFile: func(_ context.Context, _ string, _ string, inputPath string) (*meta.FileMeta, error) {
			data, readErr := os.ReadFile(inputPath)
			replacedBytes = data
			return &meta.FileMeta{Size: int64(len(data))}, readErr
		},
	}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_RDWR, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("0123456789"), 0); errno != 0 || n != 10 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	if errno := h.Allocate(context.Background(), 2, 4, 0); errno != 0 {
		t.Fatalf("interior allocate: %v", errno)
	}
	if h.writeState.logicalSize != 10 {
		t.Fatalf("logical size after interior allocate: %d", h.writeState.logicalSize)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release handle: %v", errno)
	}
	if string(replacedBytes) != "0123456789" {
		t.Fatalf("committed payload changed: %q", replacedBytes)
	}
}

func TestFallocateReadOnlyHandleReturnsEBADF(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h := &storhubHandle{fs: fsys, inode: 7, path: "demo.bin", pinned: &pinnedContent{file: meta.FileMeta{Inode: 7, Size: 16}}}
	if errno := h.Allocate(context.Background(), 0, 100, 0); errno != syscall.EBADF {
		t.Fatalf("readonly allocate: %v, want EBADF", errno)
	}
}

func TestFallocateRejectsUnsupportedModes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		off  uint64
		size uint64
		mode uint32
		want syscall.Errno
	}{
		{"punch hole without keep size", 0, 100, fallocFlPunchHole, syscall.EINVAL},
		{"punch hole with keep size", 0, 100, fallocFlPunchHole | fallocFlKeepSize, syscall.EOPNOTSUPP},
		{"zero range", 0, 100, fallocFlZeroRange, syscall.EOPNOTSUPP},
		{"collapse range", 0, 100, fallocFlCollapseRange, syscall.EOPNOTSUPP},
		{"insert range", 0, 100, fallocFlInsertRange, syscall.EOPNOTSUPP},
		{"unshare", 0, 100, fallocFlUnshare, syscall.EOPNOTSUPP},
		{"unknown flag bit", 0, 100, 0x80, syscall.EINVAL},
		{"offset overflow", ^uint64(0), 1, 0, syscall.EINVAL},
		{"length overflow", 0, ^uint64(0), 0, syscall.EINVAL},
	}
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	for _, tc := range cases {
		if got := h.Allocate(context.Background(), tc.off, tc.size, tc.mode); got != tc.want {
			t.Errorf("%s: errno=%v, want %v", tc.name, got, tc.want)
		}
	}
}
