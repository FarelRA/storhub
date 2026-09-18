package fusefs

// Handle-level tests for overlay-immediate privilege-bit clearing: a
// non-admin data write or overlay ftruncate stages the cleared mode into
// the write-state pending patch at once, so pre-commit stat already
// observes it. Admin writers are exempt, explicit fchmod still sets
// exactly what was requested, and the commit path only ever forwards the
// staged (cleared) mode.

import (
	"context"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// privStatHub serves a stub hub whose stat reports mode for every path.
func privStatHub(mode uint32) *stubHub {
	return &stubHub{
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: target, Inode: 7, Size: 4, Mode: mode, UID: 1000, GID: 1000, NLink: 1}, nil
		},
	}
}

func privOpenWriter(t *testing.T, fsys *Filesystem, ctx context.Context) *storhubHandle {
	t.Helper()
	h, err := fsys.newHandle(ctx, 7, "priv.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 4})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	return h
}

func privPending(t *testing.T, h *storhubHandle) (bool, uint32) {
	t.Helper()
	h.writeState.mu.Lock()
	defer h.writeState.mu.Unlock()
	return h.writeState.pending.HasMode, h.writeState.pending.Mode
}

// A non-admin data write stages the cleared mode immediately, and a
// pre-commit Getattr already shows the bits gone.
func TestOverlayWriteStagesClearedModeNonAdmin(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o4755), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h := privOpenWriter(t, fsys, callerCtx(1000, 1000))
	if _, errno := h.Write(callerCtx(1000, 1000), []byte("x"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o755 {
		t.Fatalf("pending must stage cleared 0755, got has=%v mode=%#o", has, mode)
	}
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "priv.bin", Inode: 7, Size: 4, Mode: 0o4755, UID: 1000, GID: 1000, NLink: 1})
	var out fuse.AttrOut
	if errno := node.Getattr(callerCtx(1000, 1000), nil, &out); errno != 0 {
		t.Fatalf("getattr: %v", errno)
	}
	if out.Mode&0o6000 != 0 {
		t.Fatalf("pre-commit getattr must hide setuid/setgid, got mode=%#o", out.Mode)
	}
	if out.Mode&0o777 != 0o755 {
		t.Fatalf("base bits must be preserved, got mode=%#o", out.Mode)
	}
}

// A setgid-only mode clears the same way through PWrite-style offsets.
func TestOverlayPWriteStagesClearedModeSetgid(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o2755), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h := privOpenWriter(t, fsys, callerCtx(1000, 1000))
	if _, errno := h.Write(callerCtx(1000, 1000), []byte("y"), 2); errno != 0 {
		t.Fatalf("pwrite: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o755 {
		t.Fatalf("pending must stage cleared 0755, got has=%v mode=%#o", has, mode)
	}
}

// An admin (uid 0) data write leaves the bits untouched.
func TestOverlayWriteAdminKeepsBits(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o4755), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h := privOpenWriter(t, fsys, callerCtx(0, 0))
	if _, errno := h.Write(callerCtx(0, 0), []byte("x"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if has, mode := privPending(t, h); has {
		t.Fatalf("admin write must not stage a mode patch, got mode=%#o", mode)
	}
}

// Direct library use carries no caller identity and is treated as
// non-admin: the bits still clear.
func TestOverlayWriteNoIdentityClears(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o4755), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h := privOpenWriter(t, fsys, context.Background())
	if _, errno := h.Write(context.Background(), []byte("x"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o755 {
		t.Fatalf("pending must stage cleared 0755, got has=%v mode=%#o", has, mode)
	}
}

// A write to a file that carries no privilege bits stages nothing, so no
// spurious metadata patch rides the next commit.
func TestOverlayWriteAlreadyClearStagesNothing(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o644), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h := privOpenWriter(t, fsys, callerCtx(1000, 1000))
	if _, errno := h.Write(callerCtx(1000, 1000), []byte("x"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if has, mode := privPending(t, h); has {
		t.Fatalf("clean-mode write must not stage a mode patch, got mode=%#o", mode)
	}
}

// Composition: an explicit fchmod after a clearing write sets exactly the
// requested bits, and a later write clears them again from the staged mode.
func TestExplicitChmodOverwritesStagedClear(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o4755), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "priv.bin", Inode: 7, Size: 4, Mode: 0o4755, UID: 1000, GID: 1000, NLink: 1})
	h := privOpenWriter(t, fsys, callerCtx(1000, 1000))
	if _, errno := h.Write(callerCtx(1000, 1000), []byte("x"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o755 {
		t.Fatalf("write must stage 0755, got has=%v mode=%#o", has, mode)
	}
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_MODE
	attr.Mode = 0o4755
	var out fuse.AttrOut
	if errno := node.Setattr(callerCtx(1000, 1000), h, &attr, &out); errno != 0 {
		t.Fatalf("fchmod: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o4755 {
		t.Fatalf("explicit chmod must overwrite with exactly 04755, got has=%v mode=%#o", has, mode)
	}
	if _, errno := h.Write(callerCtx(1000, 1000), []byte("z"), 1); errno != 0 {
		t.Fatalf("second write: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o755 {
		t.Fatalf("write after chmod must re-clear to 0755, got has=%v mode=%#o", has, mode)
	}
}

// An overlay ftruncate stages the cleared mode immediately too.
func TestOverlayFtruncateStagesClearedMode(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o6750), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "priv.bin", Inode: 7, Size: 4, Mode: 0o6750, UID: 1000, GID: 1000, NLink: 1})
	h := privOpenWriter(t, fsys, callerCtx(1000, 1000))
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = 2
	var out fuse.AttrOut
	if errno := node.Setattr(callerCtx(1000, 1000), h, &attr, &out); errno != 0 {
		t.Fatalf("ftruncate: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o750 {
		t.Fatalf("ftruncate must stage cleared 0750, got has=%v mode=%#o", has, mode)
	}
	if out.Size != 2 {
		t.Fatalf("ftruncate reply size must be 2, got %d", out.Size)
	}
	if out.Mode&0o6000 != 0 {
		t.Fatalf("ftruncate reply must hide setuid/setgid, got mode=%#o", out.Mode)
	}
}

// The commit path forwards the staged cleared mode and never resurrects
// the bits: the data op lands and the metadata patch carries no 0o6000.
func TestCommitForwardsClearedModeWithoutResurrect(t *testing.T) {
	t.Parallel()
	var patched *shfs.MetadataPatch
	hub := privStatHub(0o4755)
	hub.patchRanges = func([]shfs.RangeEdit) (*meta.FileMeta, error) {
		return &meta.FileMeta{Inode: 7, Size: 4, Mode: 0o755, UID: 1000, GID: 1000}, nil
	}
	hub.applyPatch = func(_ context.Context, _, _ string, patch shfs.MetadataPatch) error {
		cp := patch
		patched = &cp
		return nil
	}
	fsys, err := New(hub, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h := privOpenWriter(t, fsys, callerCtx(1000, 1000))
	if _, errno := h.Write(callerCtx(1000, 1000), []byte("x"), 0); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := h.Flush(callerCtx(1000, 1000)); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	if hub.patchRangeCalls == 0 {
		t.Fatal("commit must reach the backend data verb")
	}
	if patched == nil || !patched.HasMode {
		t.Fatalf("commit must forward the staged mode patch, got %+v", patched)
	}
	if patched.Mode&0o6000 != 0 {
		t.Fatalf("commit must not resurrect privilege bits, got mode=%#o", patched.Mode)
	}
	if patched.Mode != 0o755 {
		t.Fatalf("commit must forward exactly 0755, got mode=%#o", patched.Mode)
	}
}
