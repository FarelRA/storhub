package fusefs

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestLockAndErrorHelpers(t *testing.T) {
	t.Parallel()
	cacheDir := filepath.Join(t.TempDir(), "cache")
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	lock := fuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}
	if errno := fsys.setLock(1, 10, lock); errno != 0 {
		t.Fatalf("set first lock: %v", errno)
	}
	if errno := fsys.setLock(1, 11, fuse.FileLock{Start: 5, End: 12, Typ: syscall.F_RDLCK}); errno != syscall.EAGAIN {
		t.Fatalf("expected conflict, got %v", errno)
	}
	// FUSE sends End as exclusive (start+len), so adjacent ranges
	// [0,9) and [9,20) must NOT conflict or overlap.
	if locksOverlap(lock, fuse.FileLock{Start: 9, End: 20}) {
		t.Fatal("adjacent exclusive lock ranges must not overlap")
	}
	if !locksOverlap(lock, fuse.FileLock{Start: 8, End: 20}) {
		t.Fatal("overlapping exclusive lock ranges must overlap")
	}
	if segments := subtractLock(lock, fuse.FileLock{Start: 3, End: 5}); len(segments) != 2 {
		t.Fatalf("expected split lock segments, got %+v", segments)
	}
	// Partial unlock must retain both neighbors byte-exact (the old
	// inclusive math dropped one byte from the retained left segment).
	if segments := subtractLock(fuse.FileLock{Start: 0, End: 10, Typ: syscall.F_WRLCK}, fuse.FileLock{Start: 5, End: 6, Typ: syscall.F_UNLCK}); len(segments) != 2 || segments[0].End != 5 || segments[1].Start != 6 {
		t.Fatalf("partial unlock must split [0,10) into [0,5)+[6,10), got %+v", segments)
	}
	if errno := fsys.setLock(1, 10, fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_UNLCK}); errno != 0 {
		t.Fatalf("unlock lock: %v", errno)
	}
	if !lockConflicts(lockRecord{owner: 1, lock: fuse.FileLock{Start: 0, End: 1, Typ: syscall.F_WRLCK}}, 2, fuse.FileLock{Start: 0, End: 1, Typ: syscall.F_RDLCK}) {
		t.Fatal("expected conflicting locks")
	}
	if errnoFromError(nil) != 0 || errnoFromError(context.Canceled) != syscall.EINTR || errnoFromError(context.DeadlineExceeded) != syscall.ETIMEDOUT {
		t.Fatal("unexpected context error mapping")
	}
	if errnoFromError(shfs.ErrAlreadyExists) != syscall.EEXIST || errnoFromError(shfs.ErrXAttrNotFound) != syscall.ENODATA {
		t.Fatal("unexpected sentinel errno mapping")
	}
	if errnoFromError(errors.New("some other issue")) != syscall.EIO {
		t.Fatal("expected default errno mapping")
	}
	if normalizedChunkSize(0) <= 0 || minInt64(1, 2) != 1 || maxInt64(1, 2) != 2 || durationPtr(time.Second) == nil {
		t.Fatal("unexpected helper values")
	}
	if err := validateProject(strings.Repeat("a", 101)); err == nil {
		t.Fatal("expected long project validation error")
	}
}

func TestFillAndNodeAttributeHelpers(t *testing.T) {
	t.Parallel()
	now := int64(20)
	entry := &shfs.EntryInfo{Path: "docs/file.txt", Inode: 4, Size: 7, UID: 1, GID: 2, NLink: 3, Mode: 0o640, ModifiedAt: now, AccessedAt: now, ChangedAt: now, IsSymlink: true}
	var attr fuse.Attr
	fillAttr(&attr, entry)
	if attr.Ino != 4 || attr.Mode&syscall.S_IFLNK == 0 {
		t.Fatalf("unexpected filled attr: %+v", attr)
	}
	var out fuse.EntryOut
	fillEntryOut(&out, entry, DefaultOptions())
	if out.Ino != 4 {
		t.Fatalf("unexpected filled entry out: %+v", out)
	}
	file := &meta.FileMeta{Symlink: "target", Inode: 9, Size: 2, Mode: 0o777, UID: 1, GID: 2, UploadedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}
	converted := entryInfoFromFile(file, "docs/file.txt", 1)
	if !converted.IsSymlink || converted.Path != "docs/file.txt" {
		t.Fatalf("unexpected file conversion: %+v", converted)
	}

	var setCalls int
	fake := &stubHub{
		readDir: func(context.Context, string, string) ([]shfs.DirEntry, error) {
			return []shfs.DirEntry{{Name: "child", Path: "docs/child", Inode: 5, IsDir: true}}, nil
		},
		statFS: func(context.Context, string) (*shfs.FSStats, error) {
			return &shfs.FSStats{Inodes: 3, Bytes: 8192}, nil
		},
		getXAttr: func(context.Context, string, string, string) ([]byte, error) {
			if setCalls == 0 {
				return nil, shfs.ErrXAttrNotFound
			}
			return []byte("value"), nil
		},
		setXAttr: func(context.Context, string, string, string, []byte) error {
			setCalls++
			return nil
		},
		listXAttr: func(context.Context, string, string) ([]string, error) {
			return []string{"user.demo", "user.other"}, nil
		},
		removeXAttr: func(context.Context, string, string, string) error { return nil },
		readlink:    func(context.Context, string, string) (string, error) { return "target", nil },
	}
	fsys, err := New(fake, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	dirNode := &storhubNode{fs: fsys, inode: 1, isDir: true}
	stream, errno := dirNode.Readdir(context.Background())
	if errno != 0 || stream == nil {
		t.Fatalf("unexpected readdir result: %v %v", stream, errno)
	}
	var statOut fuse.StatfsOut
	if errno := dirNode.Statfs(context.Background(), &statOut); errno != 0 || statOut.Files != 3 {
		t.Fatalf("unexpected statfs output: %+v errno=%v", statOut, errno)
	}
	node := &storhubNode{fs: fsys, inode: 9, kind: meta.NodeKindSymlink}
	fsys.rememberPath(9, "docs/file.txt")
	if _, errno := node.Getxattr(context.Background(), "user.demo", nil); errno != syscall.ENODATA {
		t.Fatalf("expected missing xattr errno, got %v", errno)
	}
	if errno := node.Setxattr(context.Background(), "user.demo", []byte("value"), xattrReplace); errno != syscall.ENODATA {
		t.Fatalf("expected replace on missing attr to fail, got %v", errno)
	}
	if errno := node.Setxattr(context.Background(), "user.demo", []byte("value"), 0); errno != 0 {
		t.Fatalf("unexpected setxattr error: %v", errno)
	}
	if size, errno := node.Getxattr(context.Background(), "user.demo", nil); errno != 0 || size != uint32(len("value")) {
		t.Fatalf("unexpected getxattr size probe: %d errno=%v", size, errno)
	}
	buf := make([]byte, 8)
	if size, errno := node.Getxattr(context.Background(), "user.demo", buf); errno != 0 || string(buf[:size]) != "value" {
		t.Fatalf("unexpected getxattr payload: %q errno=%v", buf[:size], errno)
	}
	if _, errno := node.Getxattr(context.Background(), "user.demo", make([]byte, 2)); errno != syscall.ERANGE {
		t.Fatalf("expected small getxattr buffer error, got %v", errno)
	}
	if errno := node.Setxattr(context.Background(), "user.demo", []byte("value"), xattrCreate); errno != syscall.EEXIST {
		t.Fatalf("expected create on existing attr to fail, got %v", errno)
	}
	if size, errno := node.Listxattr(context.Background(), nil); errno != 0 || size == 0 {
		t.Fatalf("unexpected listxattr size: %d errno=%v", size, errno)
	}
	if _, errno := node.Listxattr(context.Background(), make([]byte, 2)); errno != syscall.ERANGE {
		t.Fatalf("expected small listxattr buffer error, got %v", errno)
	}
	listBuf := make([]byte, 32)
	if _, errno := node.Listxattr(context.Background(), listBuf); errno != 0 {
		t.Fatalf("unexpected listxattr error: %v", errno)
	}
	if errno := node.Removexattr(context.Background(), "user.demo"); errno != 0 {
		t.Fatalf("unexpected removexattr error: %v", errno)
	}
	if target, errno := node.Readlink(context.Background()); errno != 0 || string(target) != "target" {
		t.Fatalf("unexpected readlink: %q errno=%v", target, errno)
	}
}
