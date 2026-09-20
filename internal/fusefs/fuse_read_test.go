package fusefs

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestReadFromHubReadsWithinChunks(t *testing.T) {
	t.Parallel()
	var lengths []int64
	fsys, err := New(&stubHub{
		chunkSize: 16,
		readFileAt: func(_ context.Context, _ string, _ string, off, length int64) ([]byte, error) {
			lengths = append(lengths, length)
			data := []byte("abcdefghijklmnop")
			if off >= int64(len(data)) {
				return []byte{}, io.EOF
			}
			end := off + length
			if end > int64(len(data)) {
				end = int64(len(data))
			}
			return append([]byte(nil), data[off:end]...), nil
		},
	}, "demo", Options{CacheDir: t.TempDir(), OverlayBufferSize: 4})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h := &storhubHandle{fs: fsys, inode: 7, path: "demo.bin", pinned: &pinnedContent{file: meta.FileMeta{Inode: 7, Size: 16}}}
	for _, off := range []int64{0, 4, 8} {
		res, errno := h.Read(context.Background(), make([]byte, 4), off)
		if errno != 0 {
			t.Fatalf("read offset %d: %v", off, errno)
		}
		buf, status := res.Bytes(make([]byte, 4))
		if status != fuse.OK || len(buf) != 4 {
			t.Fatalf("read result offset %d: %q %v", off, buf, status)
		}
	}
	// Each read is within a single chunk, so each is a direct hub call
	if len(lengths) != 3 {
		t.Fatalf("expected 3 backend reads, got %v", lengths)
	}
}

func TestReadIntoLockedFailsOnZeroProgressBaseRead(t *testing.T) {
	t.Parallel()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	base, err := os.CreateTemp(t.TempDir(), "base-*")
	if err != nil {
		t.Fatalf("create base temp: %v", err)
	}
	defer func() { _ = os.Remove(base.Name()) }()
	state := &inodeWriteState{fs: fsys, inode: 1, baseTemp: base, baseTempPath: base.Name(), baseSize: 4, logicalSize: 4}
	buf := make([]byte, 4)
	if _, err := state.readIntoLocked(context.Background(), buf, 0); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("expected io.ErrNoProgress, got %v", err)
	}
}

// TestReadonlyOpenServesPinnedRangesWithoutFullDownload pins the read-path
// contract: a plain readonly open transfers ZERO file bytes - only
// metadata - and every Read is served on demand against the pinned chunk
// layout. Full-file materialization is reserved for destructive-path
// snapshots (unlink/rename-over), never for reading.
func TestReadonlyOpenServesPinnedRangesWithoutFullDownload(t *testing.T) {
	t.Parallel()
	now := int64(50)
	hub := &stubHub{chunkSize: 64}
	hub.loadReadonly = func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
		repo := meta.NewRepoMetadata("demo")
		repo.UpsertFile("docs/big.txt", meta.FileMeta{Inode: 9, Size: 4096}, now)
		repo.RebuildIndexes()
		return repo, "sha-1", nil
	}
	hub.statPath = func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
		if target == "docs/big.txt" {
			return &shfs.EntryInfo{Path: target, Inode: 9, Size: 4096, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		}
		return nil, syscall.ENOENT
	}
	served := make(map[int64]string)
	hub.readFileAt = func(_ context.Context, _, _ string, off, length int64) ([]byte, error) {
		served[off] = string(rune('a' + off))
		out := make([]byte, length)
		for i := range out {
			out[i] = byte('a' + int(off) + i)
		}
		return out, nil
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "docs/big.txt", Inode: 9, Size: 4096, Mode: 0o600, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
	hAny, _, errno := node.Open(context.Background(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("open: %v", errno)
	}
	h := hAny.(*storhubHandle)
	if h.temp != nil {
		t.Fatal("readonly open must not materialize the file body")
	}

	buf := make([]byte, 4)
	res, errno := h.Read(context.Background(), buf, 3)
	if errno != 0 {
		t.Fatalf("read at 3: %v", errno)
	}
	got, _ := res.Bytes(buf)
	if len(got) != 4 || string(got) != "defg" {
		t.Fatalf("pinned range read returned %q, want \"defg\"", got)
	}
	res, errno = h.Read(context.Background(), buf, 100)
	if errno != 0 {
		t.Fatalf("read at 100: %v", errno)
	}
	got, _ = res.Bytes(buf)
	if len(got) != 4 {
		t.Fatalf("short tail read: %q", got)
	}
	if hub.downloads != 0 {
		t.Fatalf("readonly reads must never trigger full-file downloads, got %d", hub.downloads)
	}
}
