package fusefs

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Mount-lock tests: one live Filesystem per cacheDir, enforced by flock
// (split from the fuse_test.go god-file).

func TestMountLockRejectsSecondLiveMount(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	lockPath := filepath.Join(cacheDir, mountLockFileName)
	holder := spawnFlockHolder(t, lockPath)
	// The flock tool does not write diagnostics; record the holder pid as
	// a real mount would have.
	if err := os.WriteFile(lockPath, []byte(strconv.Itoa(holder)), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err == nil || !strings.Contains(err.Error(), "already locked by process") || !strings.Contains(err.Error(), strconv.Itoa(holder)) {
		t.Fatalf("expected loud error naming holder pid %d, got %v", holder, err)
	}
	// A refused New must leave the foreign claim untouched.
	data, readErr := os.ReadFile(lockPath)
	if readErr != nil || string(data) != strconv.Itoa(holder) {
		t.Fatalf("foreign claim was tampered with: %q (err=%v)", data, readErr)
	}
}

func TestMountLockReleasedOnCloseAllowsRemount(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	first, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("first mount: %v", err)
	}
	data, readErr := os.ReadFile(filepath.Join(cacheDir, mountLockFileName))
	if readErr != nil || string(data) != strconv.Itoa(os.Getpid()) {
		t.Fatalf("expected own pid diagnostics while mounted: %q (err=%v)", data, readErr)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	second, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("remount after close must succeed once the flock is released: %v", err)
	}
	defer func() { _ = second.Close() }()
}

// TestMountLockRejectsSameProcessRemount pins the invariant the kernel
// lock exists to enforce: one live Filesystem per cacheDir, including two
// mounts inside a single process - exactly where a pid heuristic would
// have waved the collision through.
func TestMountLockRejectsSameProcessRemount(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	first, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("first mount: %v", err)
	}
	_, err = New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err == nil || !strings.Contains(err.Error(), "already locked") {
		t.Fatalf("same-process double mount must fail loudly, got %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	third, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("mount after close must succeed: %v", err)
	}
	defer func() { _ = third.Close() }()
}

func TestMountLockTakesOverStaleClaim(t *testing.T) {
	t.Parallel()
	cacheDir := t.TempDir()
	dead := exitedProcessPid(t)
	lockPath := filepath.Join(cacheDir, mountLockFileName)
	if err := os.WriteFile(lockPath, []byte(strconv.Itoa(dead)), 0o644); err != nil {
		t.Fatal(err)
	}
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("claim left by dead pid %d must be reclaimable: %v", dead, err)
	}
	defer func() { _ = fsys.Close() }()
	data, readErr := os.ReadFile(lockPath)
	if readErr != nil || string(data) != strconv.Itoa(os.Getpid()) {
		t.Fatalf("expected takeover recorded with own pid: %q (err=%v)", data, readErr)
	}
}
