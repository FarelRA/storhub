package fusefs

// Handler-level tests for chown privilege-bit clearing: a non-admin chown
// stages the cleared mode into the write-state pending patch at once, so a
// pre-commit fstat already observes it instead of stale setuid/setgid.
// Admin chown keeps the bits. Mirrors the data-write staging in
// fuse_privclear_test.go for the owner-change path.

import (
	"context"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func chownNode(t *testing.T, fsys *Filesystem, mode uint32) *storhubNode {
	t.Helper()
	return fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "priv.bin", Inode: 7, Size: 4, Mode: mode, UID: 1000, GID: 1000, NLink: 1})
}

func chownGIDSame(attr *fuse.SetAttrIn) {
	// A no-op group change passes CanChown for the owner without any
	// group-membership dependence on the host NSS lookup.
	attr.Valid = fuse.FATTR_GID
	attr.Gid = 1000
}

// A non-admin chown stages the cleared mode immediately, and the
// setattr reply plus a pre-commit fstat already hide the bits.
func TestChownStagesClearedModeNonAdmin(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o4755), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := chownNode(t, fsys, 0o4755)
	h := privOpenWriter(t, fsys, callerCtx(1000, 1000))
	var attr fuse.SetAttrIn
	chownGIDSame(&attr)
	var out fuse.AttrOut
	if errno := node.Setattr(callerCtx(1000, 1000), h, &attr, &out); errno != 0 {
		t.Fatalf("chown: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o755 {
		t.Fatalf("pending must stage cleared 0755, got has=%v mode=%#o", has, mode)
	}
	if out.Mode&0o6000 != 0 {
		t.Fatalf("setattr reply must hide setuid/setgid, got mode=%#o", out.Mode)
	}
	var gout fuse.AttrOut
	if errno := node.Getattr(callerCtx(1000, 1000), h, &gout); errno != 0 {
		t.Fatalf("getattr: %v", errno)
	}
	if gout.Mode&0o6000 != 0 {
		t.Fatalf("pre-commit fstat must hide setuid/setgid, got mode=%#o", gout.Mode)
	}
}

// An admin chown keeps the bits: no mode patch is staged.
func TestChownAdminKeepsBits(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o4755), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := chownNode(t, fsys, 0o4755)
	h := privOpenWriter(t, fsys, callerCtx(0, 0))
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_UID | fuse.FATTR_GID
	attr.Uid = 2000
	attr.Gid = 2000
	var out fuse.AttrOut
	if errno := node.Setattr(callerCtx(0, 0), h, &attr, &out); errno != 0 {
		t.Fatalf("chown: %v", errno)
	}
	if has, mode := privPending(t, h); has {
		t.Fatalf("admin chown must not stage a mode patch, got mode=%#o", mode)
	}
}

// Composition: an explicit fchmod restoring the bits followed by a chown
// clears them again from the effective overlay mode.
func TestChownAfterExplicitChmodClears(t *testing.T) {
	t.Parallel()
	fsys, err := New(privStatHub(0o755), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := chownNode(t, fsys, 0o755)
	h := privOpenWriter(t, fsys, callerCtx(1000, 1000))
	var chmod fuse.SetAttrIn
	chmod.Valid = fuse.FATTR_MODE
	chmod.Mode = 0o4755
	var cout fuse.AttrOut
	if errno := node.Setattr(callerCtx(1000, 1000), h, &chmod, &cout); errno != 0 {
		t.Fatalf("fchmod: %v", errno)
	}
	var attr fuse.SetAttrIn
	chownGIDSame(&attr)
	var out fuse.AttrOut
	if errno := node.Setattr(callerCtx(1000, 1000), h, &attr, &out); errno != 0 {
		t.Fatalf("chown: %v", errno)
	}
	if has, mode := privPending(t, h); !has || mode != 0o755 {
		t.Fatalf("chown after chmod must re-clear to 0755, got has=%v mode=%#o", has, mode)
	}
}
