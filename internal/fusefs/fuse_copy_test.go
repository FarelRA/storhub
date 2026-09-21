package fusefs

import (
	"context"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// copyFixture is an in-memory byte store behind stubHub.cloneFn with the
// core's snapshot semantics: the source span is captured before any
// destination byte lands, so same-file overlaps behave like memmove.
type copyFixture struct {
	files map[string][]byte
	calls int
	last  [5]int64
}

func (f *copyFixture) clone(_ context.Context, _ string, src string, srcOff int64, dst string, dstOff int64, length int64) (*meta.FileMeta, error) {
	f.calls++
	f.last = [5]int64{srcOff, dstOff, length, 0, 0}
	snap, ok := f.files[src]
	if !ok {
		return nil, shfs.NotFound(src)
	}
	if srcOff > int64(len(snap)) || length > int64(len(snap))-srcOff {
		return nil, syscall.EINVAL
	}
	seg := append([]byte(nil), snap[srcOff:srcOff+length]...)
	d := append([]byte(nil), f.files[dst]...)
	if need := dstOff + length; int64(len(d)) < need {
		d = append(d, make([]byte, need-int64(len(d)))...)
	}
	copy(d[dstOff:], seg)
	f.files[dst] = d
	return &meta.FileMeta{Size: int64(len(d)), Mode: 0o644}, nil
}

func newCopyTestFS(t *testing.T, fix *copyFixture) *Filesystem {
	t.Helper()
	hub := &stubHub{
		cloneFn: fix.clone,
	}
	fsys := mustMount(t, hub, "", Options{})
	ctx := context.Background()
	fsys.EnsureNodeForTest(ctx, &shfs.EntryInfo{Path: "src.txt", Inode: 10, Mode: 0o644, Size: int64(len(fix.files["src.txt"]))})
	fsys.EnsureNodeForTest(ctx, &shfs.EntryInfo{Path: "dst.txt", Inode: 11, Mode: 0o644, Size: int64(len(fix.files["dst.txt"]))})
	return fsys
}

func copyTestHandle(fsys *Filesystem, inode uint64, targetPath string) *storhubHandle {
	h := &storhubHandle{fs: fsys, inode: inode, id: fsys.nextHandle.Add(1), path: targetPath}
	fsys.mu.Lock()
	fsys.handles[h.id] = h
	fsys.mu.Unlock()
	return h
}

func TestCopyFileRangeCrossFileByteExact(t *testing.T) {
	t.Parallel()
	fix := &copyFixture{files: map[string][]byte{"src.txt": []byte("abcdefgh"), "dst.txt": []byte("XXXX")}}
	fsys := newCopyTestFS(t, fix)
	before := fsys.Invalidations()
	srcNode := fsys.EnsureNodeForTest(context.Background(), &shfs.EntryInfo{Path: "src.txt", Inode: 10, Mode: 0o644})
	srcH := copyTestHandle(fsys, 10, "src.txt")
	dstH := copyTestHandle(fsys, 11, "dst.txt")
	// Seed a shared pin for the destination so the test can prove the
	// clone dropped it.
	fsys.storePinned(pinnedKeyFor("demo", "dst.txt", &meta.FileMeta{Inode: 11, Size: 4}), &pinnedContent{})
	got, errno := srcNode.CopyFileRange(context.Background(), srcH, 2, nil, dstH, 1, 3, 0)
	if errno != 0 {
		t.Fatalf("copy_file_range: %v", errno)
	}
	if got != 3 {
		t.Fatalf("expected 3 bytes cloned, got %d", got)
	}
	if string(fix.files["dst.txt"]) != "Xcde" {
		t.Fatalf("byte-exact clone failed, got %q", fix.files["dst.txt"])
	}
	if fix.calls != 1 {
		t.Fatalf("expected one core call, got %d", fix.calls)
	}
	if _, ok := fsys.pinnedFor(pinnedKeyFor("demo", "dst.txt", &meta.FileMeta{Inode: 11, Size: 4})); ok {
		t.Fatal("destination pin survived the clone")
	}
	if fsys.Invalidations() <= before {
		t.Fatal("clone issued no kernel-cache invalidation")
	}
}

func TestCopyFileRangePartialAndSelfOverlap(t *testing.T) {
	t.Parallel()
	t.Run("partial range", func(t *testing.T) {
		t.Parallel()
		fix := &copyFixture{files: map[string][]byte{"src.txt": []byte("0123456789"), "dst.txt": {}}}
		fsys := newCopyTestFS(t, fix)
		srcNode := fsys.EnsureNodeForTest(context.Background(), &shfs.EntryInfo{Path: "src.txt", Inode: 10, Mode: 0o644})
		if got, errno := srcNode.CopyFileRange(context.Background(), copyTestHandle(fsys, 10, "src.txt"), 4, nil, copyTestHandle(fsys, 11, "dst.txt"), 0, 3, 0); errno != 0 || got != 3 {
			t.Fatalf("partial clone: got=%d errno=%v", got, errno)
		}
		if string(fix.files["dst.txt"]) != "456" {
			t.Fatalf("partial clone bytes wrong, got %q", fix.files["dst.txt"])
		}
	})
	t.Run("forward overlap reads pre-op bytes", func(t *testing.T) {
		t.Parallel()
		fix := &copyFixture{files: map[string][]byte{"src.txt": []byte("abcdefgh")}}
		fsys := newCopyTestFS(t, fix)
		srcNode := fsys.EnsureNodeForTest(context.Background(), &shfs.EntryInfo{Path: "src.txt", Inode: 10, Mode: 0o644})
		if got, errno := srcNode.CopyFileRange(context.Background(), copyTestHandle(fsys, 10, "src.txt"), 0, nil, copyTestHandle(fsys, 10, "src.txt"), 2, 4, 0); errno != 0 || got != 4 {
			t.Fatalf("overlap clone: got=%d errno=%v", got, errno)
		}
		if string(fix.files["src.txt"]) != "ababcdgh" {
			t.Fatalf("forward overlap must clone pre-op bytes, got %q", fix.files["src.txt"])
		}
	})
	t.Run("backward overlap reads pre-op bytes", func(t *testing.T) {
		t.Parallel()
		fix := &copyFixture{files: map[string][]byte{"src.txt": []byte("abcdefgh")}}
		fsys := newCopyTestFS(t, fix)
		srcNode := fsys.EnsureNodeForTest(context.Background(), &shfs.EntryInfo{Path: "src.txt", Inode: 10, Mode: 0o644})
		if got, errno := srcNode.CopyFileRange(context.Background(), copyTestHandle(fsys, 10, "src.txt"), 2, nil, copyTestHandle(fsys, 10, "src.txt"), 0, 4, 0); errno != 0 || got != 4 {
			t.Fatalf("overlap clone: got=%d errno=%v", got, errno)
		}
		if string(fix.files["src.txt"]) != "cdefefgh" {
			t.Fatalf("backward overlap must clone pre-op bytes, got %q", fix.files["src.txt"])
		}
	})
}

func TestCopyFileRangeHandleResolutionAndGuards(t *testing.T) {
	t.Parallel()
	t.Run("handle paths win over node paths", func(t *testing.T) {
		t.Parallel()
		var gotSrc, gotDst string
		hub := &stubHub{
			cloneFn: func(_ context.Context, _ string, src string, _ int64, dst string, _ int64, _ int64) (*meta.FileMeta, error) {
				gotSrc, gotDst = src, dst
				return &meta.FileMeta{}, nil
			},
		}
		fsys := mustMount(t, hub, "", Options{})
		ctx := context.Background()
		// Nodes are registered under stale names; the open handles carry
		// the live (renamed) paths, which must win.
		srcNode := fsys.EnsureNodeForTest(ctx, &shfs.EntryInfo{Path: "stalesrc", Inode: 20, Mode: 0o644})
		fsys.EnsureNodeForTest(ctx, &shfs.EntryInfo{Path: "staledst", Inode: 21, Mode: 0o644})
		srcH := copyTestHandle(fsys, 20, "live-src.txt")
		dstH := copyTestHandle(fsys, 21, "live-dst.txt")
		if _, errno := srcNode.CopyFileRange(ctx, srcH, 0, nil, dstH, 0, 1, 0); errno != 0 {
			t.Fatalf("copy_file_range: %v", errno)
		}
		if gotSrc != "live-src.txt" || gotDst != "live-dst.txt" {
			t.Fatalf("handle paths must win, got %q -> %q", gotSrc, gotDst)
		}
	})
	t.Run("node path fallback for handleless handles", func(t *testing.T) {
		t.Parallel()
		var gotSrc, gotDst string
		hub := &stubHub{
			cloneFn: func(_ context.Context, _ string, src string, _ int64, dst string, _ int64, _ int64) (*meta.FileMeta, error) {
				gotSrc, gotDst = src, dst
				return &meta.FileMeta{}, nil
			},
		}
		fsys := mustMount(t, hub, "", Options{})
		ctx := context.Background()
		srcNode := fsys.EnsureNodeForTest(ctx, &shfs.EntryInfo{Path: "src.txt", Inode: 10, Mode: 0o644})
		fsys.EnsureNodeForTest(ctx, &shfs.EntryInfo{Path: "dst.txt", Inode: 11, Mode: 0o644})
		// An empty handle path falls back to the node path; the
		// destination still resolves through its own handle.
		if _, errno := srcNode.CopyFileRange(ctx, copyTestHandle(fsys, 10, ""), 0, nil, copyTestHandle(fsys, 11, "dst.txt"), 0, 1, 0); errno != 0 {
			t.Fatalf("copy_file_range: %v", errno)
		}
		if gotSrc != "src.txt" || gotDst != "dst.txt" {
			t.Fatalf("node fallback failed, got %q -> %q", gotSrc, gotDst)
		}
	})
	t.Run("guards", func(t *testing.T) {
		t.Parallel()
		fix := &copyFixture{files: map[string][]byte{"src.txt": []byte("abc")}}
		fsys := newCopyTestFS(t, fix)
		srcNode := fsys.EnsureNodeForTest(context.Background(), &shfs.EntryInfo{Path: "src.txt", Inode: 10, Mode: 0o644})
		srcH := copyTestHandle(fsys, 10, "src.txt")
		dstH := copyTestHandle(fsys, 11, "dst.txt")
		if got, errno := srcNode.CopyFileRange(context.Background(), srcH, 0, nil, dstH, 0, 0, 0); errno != 0 || got != 0 {
			t.Fatalf("zero length must be a no-op success, got=%d errno=%v", got, errno)
		}
		if fix.calls != 0 {
			t.Fatal("zero-length clone must not reach the core")
		}
		if _, errno := srcNode.CopyFileRange(context.Background(), srcH, 0, nil, dstH, 0, 1, 1); errno != syscall.EINVAL {
			t.Fatalf("nonzero flags must fail EINVAL, got %v", errno)
		}
		if _, errno := srcNode.CopyFileRange(context.Background(), srcH, 0, nil, dstH, 0, 1<<33, 0); errno != syscall.EINVAL {
			t.Fatalf("unrepresentable length must fail EINVAL, got %v", errno)
		}
		if _, errno := srcNode.CopyFileRange(context.Background(), srcH, 0, nil, dstH, 0, 1, 0); errno != 0 {
			t.Fatalf("copy_file_range: %v", errno)
		}
	})
}
