package fusefs

import (
	"context"
	"os"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const posixBenchNow = int64(100)

func posixBenchFuseAvailable(b *testing.B) {
	b.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		b.Skip("skipping FUSE benchmark: /dev/fuse unavailable")
	}
}

func posixBenchPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}
	return payload
}

func posixBenchFixture(b *testing.B, benchPath string, inode uint64, size int64, payload []byte) (*Filesystem, *storhubNode) {
	b.Helper()
	hub := &stubHub{
		chunkSize: 1 << 20,
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			if target != benchPath {
				return nil, syscall.ENOENT
			}
			entry := mkEntry(target, size, posixBenchNow)
			entry.Inode = inode
			return entry, nil
		},
		loadReadonly: func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
			repo := meta.NewRepoMetadata("demo")
			repo.UpsertFile(benchPath, meta.FileMeta{
				Inode:      inode,
				Size:       size,
				Mode:       0o644,
				ModifiedAt: posixBenchNow,
				AccessedAt: posixBenchNow,
				ChangedAt:  posixBenchNow,
			}, posixBenchNow)
			repo.RebuildIndexes()
			return repo, "shabench", nil
		},
		readFileAt: func(_ context.Context, _ string, _ string, off, length int64) ([]byte, error) {
			if off >= int64(len(payload)) {
				return []byte{}, nil
			}
			end := off + length
			if end > int64(len(payload)) {
				end = int64(len(payload))
			}
			return append([]byte(nil), payload[off:end]...), nil
		},
		replaceFile: func(_ context.Context, _ string, _ string, _ string) (*meta.FileMeta, error) {
			return &meta.FileMeta{Inode: inode, Size: size}, nil
		},
		patchFile: func(_ context.Context, _ string, _ string, _, _ int64, _ []byte) (*meta.FileMeta, error) {
			return &meta.FileMeta{Inode: inode, Size: size}, nil
		},
		patchRanges: func([]shfs.RangeEdit) (*meta.FileMeta, error) {
			return &meta.FileMeta{Inode: inode, Size: size}, nil
		},
		rewriteFn: func(_ context.Context, _, _, _ string) (*meta.FileMeta, error) {
			return &meta.FileMeta{Inode: inode, Size: size}, nil
		},
	}
	fsys, err := New(hub, "demo", Options{CacheDir: b.TempDir()})
	if err != nil {
		b.Fatalf("new filesystem: %v", err)
	}
	b.Cleanup(func() { _ = fsys.Close() })
	entry := mkEntry(benchPath, size, posixBenchNow)
	entry.Inode = inode
	node := fsys.EnsureNodeForTest(context.Background(), entry)
	return fsys, node
}

func BenchmarkPosixOpenStatClose(b *testing.B) {
	b.ReportAllocs()
	posixBenchFuseAvailable(b)
	ctx := context.Background()
	const benchPath = "pb-open-stat-close/file.txt"
	const inode = uint64(101)
	payload := posixBenchPayload(4096)
	_, node := posixBenchFixture(b, benchPath, inode, int64(len(payload)), payload)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hAny, _, errno := node.Open(ctx, syscall.O_RDONLY)
		if errno != 0 {
			b.Fatalf("open: %v", errno)
		}
		h := hAny.(*storhubHandle)
		var attrOut fuse.AttrOut
		if errno := node.Getattr(ctx, h, &attrOut); errno != 0 {
			b.Fatalf("getattr: %v", errno)
		}
		if errno := h.Release(ctx); errno != 0 {
			b.Fatalf("release: %v", errno)
		}
	}
}

func BenchmarkPosixReadColdPin(b *testing.B) {
	b.ReportAllocs()
	posixBenchFuseAvailable(b)
	ctx := context.Background()
	const benchPath = "pb-read-cold-pin/file.txt"
	const inode = uint64(102)
	payload := posixBenchPayload(4096)
	fsys, node := posixBenchFixture(b, benchPath, inode, int64(len(payload)), payload)
	buf := make([]byte, len(payload))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fsys.dropPinnedForPath(benchPath)
		hAny, _, errno := node.Open(ctx, syscall.O_RDONLY)
		if errno != 0 {
			b.Fatalf("open: %v", errno)
		}
		h := hAny.(*storhubHandle)
		res, errno := h.Read(ctx, buf, 0)
		if errno != 0 {
			b.Fatalf("read: %v", errno)
		}
		got, _ := res.Bytes(buf)
		if len(got) != len(payload) {
			b.Fatalf("short read: got %d want %d", len(got), len(payload))
		}
		if errno := h.Release(ctx); errno != 0 {
			b.Fatalf("release: %v", errno)
		}
	}
}

func BenchmarkPosixReadWarmPin(b *testing.B) {
	b.ReportAllocs()
	posixBenchFuseAvailable(b)
	ctx := context.Background()
	const benchPath = "pb-read-warm-pin/file.txt"
	const inode = uint64(103)
	payload := posixBenchPayload(4096)
	_, node := posixBenchFixture(b, benchPath, inode, int64(len(payload)), payload)
	hAny, _, errno := node.Open(ctx, syscall.O_RDONLY)
	if errno != 0 {
		b.Fatalf("open: %v", errno)
	}
	h := hAny.(*storhubHandle)
	buf := make([]byte, len(payload))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, errno := h.Read(ctx, buf, 0)
		if errno != 0 {
			b.Fatalf("read: %v", errno)
		}
		got, _ := res.Bytes(buf)
		if len(got) != len(payload) {
			b.Fatalf("short read: got %d want %d", len(got), len(payload))
		}
	}
	b.StopTimer()
	if errno := h.Release(ctx); errno != 0 {
		b.Fatalf("release: %v", errno)
	}
}

func BenchmarkPosixReadOwnWrites(b *testing.B) {
	b.ReportAllocs()
	posixBenchFuseAvailable(b)
	ctx := context.Background()
	const benchPath = "pb-read-own-writes/file.txt"
	const inode = uint64(104)
	payload := posixBenchPayload(4096)
	_, node := posixBenchFixture(b, benchPath, inode, int64(len(payload)), payload)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		b.Fatalf("open: %v", errno)
	}
	h := hAny.(*storhubHandle)
	buf := make([]byte, len(payload))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, errno := h.Write(ctx, payload, 0)
		if errno != 0 || int(n) != len(payload) {
			b.Fatalf("write: n=%d errno=%v", n, errno)
		}
		res, errno := h.Read(ctx, buf, 0)
		if errno != 0 {
			b.Fatalf("read: %v", errno)
		}
		got, _ := res.Bytes(buf)
		if len(got) != len(payload) {
			b.Fatalf("short read: got %d want %d", len(got), len(payload))
		}
	}
	b.StopTimer()
	if errno := h.Release(ctx); errno != 0 {
		b.Fatalf("release: %v", errno)
	}
}

func BenchmarkPosixWrite4K(b *testing.B) {
	b.ReportAllocs()
	posixBenchFuseAvailable(b)
	ctx := context.Background()
	const benchPath = "pb-write-4k/file.txt"
	const inode = uint64(105)
	payload := posixBenchPayload(4096)
	_, node := posixBenchFixture(b, benchPath, inode, int64(len(payload)), payload)
	hAny, _, errno := node.Open(ctx, syscall.O_WRONLY)
	if errno != 0 {
		b.Fatalf("open: %v", errno)
	}
	h := hAny.(*storhubHandle)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, errno := h.Write(ctx, payload, 0)
		if errno != 0 || int(n) != len(payload) {
			b.Fatalf("write: n=%d errno=%v", n, errno)
		}
	}
	b.StopTimer()
	if errno := h.Release(ctx); errno != 0 {
		b.Fatalf("release: %v", errno)
	}
}

func BenchmarkPosixGetattr(b *testing.B) {
	b.ReportAllocs()
	posixBenchFuseAvailable(b)
	ctx := context.Background()
	const benchPath = "pb-getattr/file.txt"
	const inode = uint64(106)
	payload := posixBenchPayload(4096)
	_, node := posixBenchFixture(b, benchPath, inode, int64(len(payload)), payload)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out fuse.AttrOut
		if errno := node.Getattr(ctx, nil, &out); errno != 0 {
			b.Fatalf("getattr: %v", errno)
		}
		if out.Size != uint64(len(payload)) {
			b.Fatalf("unexpected size: got %d want %d", out.Size, len(payload))
		}
	}
}

func BenchmarkPosixSequentialRead1M(b *testing.B) {
	b.ReportAllocs()
	posixBenchFuseAvailable(b)
	ctx := context.Background()
	const benchPath = "pb-sequential-read-1m/file.bin"
	const inode = uint64(107)
	payload := posixBenchPayload(1 << 20)
	_, node := posixBenchFixture(b, benchPath, inode, int64(len(payload)), payload)
	buf := make([]byte, len(payload))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hAny, _, errno := node.Open(ctx, syscall.O_RDONLY)
		if errno != 0 {
			b.Fatalf("open: %v", errno)
		}
		h := hAny.(*storhubHandle)
		res, errno := h.Read(ctx, buf, 0)
		if errno != 0 {
			b.Fatalf("read: %v", errno)
		}
		got, _ := res.Bytes(buf)
		if len(got) != len(payload) {
			b.Fatalf("short read: got %d want %d", len(got), len(payload))
		}
		if errno := h.Release(ctx); errno != 0 {
			b.Fatalf("release: %v", errno)
		}
	}
}
