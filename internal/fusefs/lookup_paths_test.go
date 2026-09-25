package fusefs

import (
	"bytes"
	"context"
	"log/slog"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Both lookup routes must fill the same cached entry for the same child:
// the node route serves a fresh stat, the directory handle route serves
// the loaded listing, and both register through the shared attach tail.
func TestLookupRoutesFillSameEntry(t *testing.T) {
	hub := &stubHub{
		statPath: func(_ context.Context, _, target string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: target, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, Inode: 7, Size: 3}, nil
		},
		readDir: func(_ context.Context, _, _ string) ([]shfs.DirEntry, error) {
			return []shfs.DirEntry{{Name: "kid", Inode: 7, Mode: 0o644, NLink: 1, UID: 1000, GID: 1000, Size: 3}}, nil
		},
	}
	fsys := mustMount(t, hub, "", DefaultOptions())

	var nodeOut fuse.EntryOut
	if _, errno := fsys.root.Lookup(context.Background(), "kid", &nodeOut); errno != 0 {
		t.Fatalf("node lookup errno: %v", errno)
	}
	handle := &storhubDirHandle{n: fsys.root}
	var dirOut fuse.EntryOut
	if _, errno := handle.Lookup(context.Background(), "kid", &dirOut); errno != 0 {
		t.Fatalf("dir handle lookup errno: %v", errno)
	}
	if nodeOut.Ino != 7 || dirOut.Ino != 7 {
		t.Fatalf("ino mismatch: node=%d dir=%d", nodeOut.Ino, dirOut.Ino)
	}
	if nodeOut.Size != dirOut.Size {
		t.Fatalf("size mismatch: node=%d dir=%d", nodeOut.Size, dirOut.Size)
	}
	fsys.mu.RLock()
	defer fsys.mu.RUnlock()
	if fsys.pathToInode["kid"] != 7 {
		t.Fatalf("path index missing kid: %v", fsys.pathToInode)
	}
}

// A name newer than the directory snapshot must fall back to the live
// node route instead of failing.
func TestDirLookupMissFallsBackToLiveStat(t *testing.T) {
	hub := &stubHub{
		statPath: func(_ context.Context, _, target string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: target, Mode: 0o644, NLink: 1, Inode: 9, Size: 1}, nil
		},
		readDir: func(_ context.Context, _, _ string) ([]shfs.DirEntry, error) {
			return nil, nil
		},
	}
	fsys := mustMount(t, hub, "", DefaultOptions())
	handle := &storhubDirHandle{n: fsys.root}
	var out fuse.EntryOut
	if _, errno := handle.Lookup(context.Background(), "fresh", &out); errno != 0 {
		t.Fatalf("lookup errno: %v", errno)
	}
	if out.Ino != 9 {
		t.Fatalf("ino = %d, want 9", out.Ino)
	}
}

// Lookup must observe the shared write overlay (staged size), exactly as
// a handle stat does: one overlay view for every stat route.
func TestLookupSeesSharedOverlaySize(t *testing.T) {
	hub := &stubHub{
		statPath: func(_ context.Context, _, target string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: target, Mode: 0o644, NLink: 1, Inode: 7, Size: 3}, nil
		},
	}
	fsys := mustMount(t, hub, "", DefaultOptions())
	if _, err := fsys.acquireWriteState(context.Background(), 7, "kid", &writeBootstrap{baseSize: 10}); err != nil {
		t.Fatalf("acquire write state: %v", err)
	}
	var out fuse.EntryOut
	if _, errno := fsys.root.Lookup(context.Background(), "kid", &out); errno != 0 {
		t.Fatalf("lookup errno: %v", errno)
	}
	if out.Size != 10 {
		t.Fatalf("lookup size = %d, want staged 10", out.Size)
	}
}

// The usage report carries no caller identity of its own, but it must
// still run through the caller injector so suppression markers travel.
func TestStatfsCarriesCallerMarkers(t *testing.T) {
	var suppressed bool
	hub := &stubHub{
		statFS: func(ctx context.Context, _ string) (*shfs.FSStats, error) {
			suppressed = shfs.AtimeSuppressed(ctx)
			return &shfs.FSStats{Bytes: 4096, Inodes: 5}, nil
		},
	}
	fsys := mustMount(t, hub, "", DefaultOptions())
	var out fuse.StatfsOut
	if errno := fsys.root.Statfs(context.Background(), &out); errno != 0 {
		t.Fatalf("statfs errno: %v", errno)
	}
	if !suppressed {
		t.Fatal("statfs hub call misses the caller markers")
	}
	if out.Files != 5 {
		t.Fatalf("files = %d, want 5", out.Files)
	}
}

// The trace emitter is ungated by design: call sites carry the single
// gate, so a direct emit always lands when the handler accepts it.
func TestTraceEmitNeedsNoOuterGuard(t *testing.T) {
	var buf bytes.Buffer
	opts := DefaultOptions()
	opts.Debug = false
	opts.Logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	fsys := mustMount(t, &stubHub{}, "", opts)
	fsys.debugOp("probe-trace", "k", "v")
	if !bytes.Contains(buf.Bytes(), []byte("probe-trace")) {
		t.Fatal("direct trace emit produced no record")
	}
}

// Creation-mode bits have one spelling: permission bits only.
func TestPermissionBitsStripFileType(t *testing.T) {
	if got := permBits(0o100644); got != 0o644 {
		t.Fatalf("permBits = %o, want 644", got)
	}
	if got := permBits(0o4755); got != 0o4755 {
		t.Fatalf("permBits = %o, want 4755", got)
	}
}

// The root node resolves to the empty path; a node with no surviving
// path reports stale instead of addressing the root by accident.
func TestSafePathRootAndStale(t *testing.T) {
	fsys := mustMount(t, &stubHub{}, "", DefaultOptions())
	if p, errno := fsys.root.safePath(); errno != 0 || p != "" {
		t.Fatalf("root safePath = %q, %v", p, errno)
	}
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "gone", Inode: 9, Mode: 0o644})
	fsys.dropPath(9, "gone")
	if _, errno := node.safePath(); errno != syscall.ESTALE {
		t.Fatalf("pathless safePath errno = %v, want ESTALE", errno)
	}
}

// Registering a node and remembering a path land in the same index.
func TestEnsureNodeRegistersPathIndex(t *testing.T) {
	fsys := mustMount(t, &stubHub{}, "", DefaultOptions())
	fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "a/b", Inode: 11, Mode: 0o644})
	fsys.mu.RLock()
	defer fsys.mu.RUnlock()
	if fsys.pathToInode["a/b"] != 11 {
		t.Fatalf("path index = %v", fsys.pathToInode)
	}
	if _, ok := fsys.inodePaths[11]["a/b"]; !ok {
		t.Fatalf("reverse index = %v", fsys.inodePaths[11])
	}
}

// A subtree move rewrites the index and every affected handle path.
func TestSubtreeMoveRewritesIndexAndHandles(t *testing.T) {
	fsys := mustMount(t, &stubHub{}, "", DefaultOptions())
	fsys.rememberPath(7, "a/x")
	h := &storhubHandle{fs: fsys, inode: 7, path: "a/x"}
	fsys.mu.Lock()
	fsys.handles[1] = h
	fsys.mu.Unlock()
	state, err := fsys.acquireWriteState(context.Background(), 7, "a/x", &writeBootstrap{baseSize: 4})
	if err != nil {
		t.Fatalf("acquire write state: %v", err)
	}
	fsys.remapPaths("a", "b")
	fsys.mu.RLock()
	ino := fsys.pathToInode["b/x"]
	fsys.mu.RUnlock()
	if ino != 7 {
		t.Fatalf("moved index = %d, want 7", ino)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.path != "b/x" {
		t.Fatalf("handle path = %q, want b/x", h.path)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.path != "b/x" {
		t.Fatalf("write state path = %q, want b/x", state.path)
	}
}

// Overlap merging for seek views follows the canonical range merge:
// overlapping and adjacent spans coalesce, order does not matter.
func TestSeekMergeCoalescesOverlaps(t *testing.T) {
	got := shfs.MergeByteRanges([]ByteRange{{Start: 20, End: 25}, {Start: 0, End: 10}, {Start: 5, End: 15}})
	want := []ByteRange{{Start: 0, End: 15}, {Start: 20, End: 25}}
	if len(got) != len(want) {
		t.Fatalf("merged = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merged = %v, want %v", got, want)
		}
	}
}

// The create/replace request flags reach the backend with backend
// vocabulary: create on a present name fails, replace on a missing name
// fails.
func TestSetxattrRequestFlagsReachBackend(t *testing.T) {
	hub := &stubHub{
		getXAttr: func(_ context.Context, _, _, _ string) ([]byte, error) {
			return []byte("v"), nil
		},
	}
	fsys := mustMount(t, hub, "", DefaultOptions())
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "f", Inode: 7, Mode: 0o644})
	if errno := node.Setxattr(context.Background(), "user.k", []byte("v2"), 1); errno != syscall.EEXIST {
		t.Fatalf("create on present name errno = %v, want EEXIST", errno)
	}
	missing := &stubHub{
		getXAttr: func(_ context.Context, _, _, _ string) ([]byte, error) {
			return nil, shfs.ErrXAttrNotFound
		},
	}
	fsys2 := mustMount(t, missing, "", DefaultOptions())
	node2 := fsys2.ensureNode(context.Background(), &shfs.EntryInfo{Path: "g", Inode: 8, Mode: 0o644})
	if errno := node2.Setxattr(context.Background(), "user.k", []byte("v2"), 2); errno != syscall.ENODATA {
		t.Fatalf("replace on missing name errno = %v, want ENODATA", errno)
	}
}
