package fusefs

// Handler-level tests for fstat overlay visibility: Getattr on a linked
// file applies the live write-state patch (staged mode/owner/times/size)
// exactly as finishSetattr does, so fstat after fchmod-before-commit
// observes the staged values. The calling handle's own state wins even
// when it is no longer registered under the inode (e.g. after a
// quarantine unregister); handleless getattr falls back to the shared
// state lookup.

import (
	"context"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func overlayStatHub() *stubHub {
	const now = int64(100)
	return &stubHub{
		statPath: func(_ context.Context, _ string, target string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: target, Inode: 7, Size: 4, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, nil
		},
	}
}

func overlayNode(t *testing.T, fsys *Filesystem) *storhubNode {
	t.Helper()
	const now = int64(100)
	return fsys.ensureNode(context.Background(), &shfs.EntryInfo{Path: "priv.bin", Inode: 7, Size: 4, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, ModifiedAt: now, AccessedAt: now, ChangedAt: now})
}

// fstat observes staged mode, size, and times before commit, both with an
// fh and handleless.
func TestFstatSeesStagedOverlay(t *testing.T) {
	t.Parallel()
	fsys, err := New(overlayStatHub(), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := overlayNode(t, fsys)
	h := privOpenWriter(callerCtx(1000, 1000), t, fsys)
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_MODE | fuse.FATTR_SIZE
	attr.Mode = 0o600
	attr.Size = 9
	var sout fuse.AttrOut
	if errno := node.Setattr(callerCtx(1000, 1000), h, &attr, &sout); errno != 0 {
		t.Fatalf("setattr: %v", errno)
	}
	var times fuse.SetAttrIn
	times.Valid = fuse.FATTR_MTIME
	times.Mtime = 200
	times.Mtimensec = 0
	var tout fuse.AttrOut
	if errno := node.Setattr(callerCtx(1000, 1000), h, &times, &tout); errno != 0 {
		t.Fatalf("utimens: %v", errno)
	}
	var fhOut, noFhOut fuse.AttrOut
	if errno := node.Getattr(callerCtx(1000, 1000), h, &fhOut); errno != 0 {
		t.Fatalf("getattr with fh: %v", errno)
	}
	if errno := node.Getattr(callerCtx(1000, 1000), nil, &noFhOut); errno != 0 {
		t.Fatalf("getattr handleless: %v", errno)
	}
	for _, tc := range []struct {
		name string
		out  fuse.AttrOut
	}{
		{"with fh", fhOut},
		{"handleless", noFhOut},
	} {
		if tc.out.Mode&0o7777 != 0o600 {
			t.Fatalf("fstat %s must show staged 0600, got mode=%#o", tc.name, tc.out.Mode)
		}
		if tc.out.Size != 9 {
			t.Fatalf("fstat %s must show staged size 9, got %d", tc.name, tc.out.Size)
		}
		if tc.out.Mtime != 200 {
			t.Fatalf("fstat %s must show staged mtime 200, got %d", tc.name, tc.out.Mtime)
		}
	}
}

// fstat via the handle still observes the staged mode after the state
// unregisters from the inode map (quarantine path); handleless getattr
// falls back to the committed stat.
func TestFstatViaHandleAfterUnregister(t *testing.T) {
	t.Parallel()
	fsys, err := New(overlayStatHub(), "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	node := overlayNode(t, fsys)
	h := privOpenWriter(callerCtx(1000, 1000), t, fsys)
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_MODE
	attr.Mode = 0o600
	var sout fuse.AttrOut
	if errno := node.Setattr(callerCtx(1000, 1000), h, &attr, &sout); errno != 0 {
		t.Fatalf("fchmod: %v", errno)
	}
	// Simulate the quarantine unregister: the map drops the state while
	// the handle keeps its reference.
	fsys.mu.Lock()
	delete(fsys.writeStates, 7)
	fsys.mu.Unlock()
	var fhOut fuse.AttrOut
	if errno := node.Getattr(callerCtx(1000, 1000), h, &fhOut); errno != 0 {
		t.Fatalf("getattr with fh: %v", errno)
	}
	if fhOut.Mode&0o7777 != 0o600 {
		t.Fatalf("fstat via handle must show staged 0600, got mode=%#o", fhOut.Mode)
	}
	var noFhOut fuse.AttrOut
	if errno := node.Getattr(callerCtx(1000, 1000), nil, &noFhOut); errno != 0 {
		t.Fatalf("getattr handleless: %v", errno)
	}
	if noFhOut.Mode&0o7777 != 0o644 {
		t.Fatalf("handleless fstat must fall back to committed 0644, got mode=%#o", noFhOut.Mode)
	}
}
