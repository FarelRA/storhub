//go:build linux

package fusefs

// Detached-handle pins: an unlinked-but-open inode keeps serving
// fh-carried ops from its detach snapshot plus the live overlay.
//
// Background: the kernel pushes cached mtime through SETATTR with an fh
// on close under writeback caching, and fstat arrives as GETATTR with an
// fh. Both used to resolve via the (now gone) path and answer ESTALE,
// which failed close(2) with EIO on unlinked-but-open files. Found by
// the conformance unlink-while-open scenario; pinned here at the syscall
// level including the fstat/futimens/fchmod paths the shared table does
// not cover.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func futimens(t *testing.T, f *os.File, ut []unix.Timespec) {
	t.Helper()
	// No unix.Futimens in x/sys: utimensat with an empty path plus
	// AT_EMPTY_PATH is the identical syscall on the open fd.
	if err := unix.UtimesNanoAt(int(f.Fd()), "", ut, unix.AT_EMPTY_PATH); err != nil {
		t.Fatalf("futimens: %v", err)
	}
}
func mountDetachTest(t *testing.T) (*Filesystem, string) {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("detached-handle test needs /dev/fuse: %v", err)
	}
	parent := t.TempDir()
	mountPoint := filepath.Join(parent, "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}
	fsys, err := New(newPCHub(), "demo", Options{CacheDir: filepath.Join(parent, "cache")})
	if err != nil {
		t.Skipf("detached-handle test needs a filesystem: %v", err)
	}
	if err := fsys.Mount(mountPoint); err != nil {
		_ = fsys.Close()
		t.Skipf("detached-handle test needs a working mount: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(mountPoint); err == nil {
			break
		} else if time.Now().After(deadline) {
			_ = fsys.Unmount()
			_ = fsys.Close()
			t.Fatalf("mount point never became ready: %v", err)
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Cleanup(func() {
		// Same discipline as the conformance harness: a failed test
		// may leave fds open, which makes a plain unmount fail busy
		// and leak the mount. Retry after a lazy detach so teardown
		// never wedges the suite (never kill mounts from outside).
		if err := fsys.Unmount(); err != nil {
			pcLazyUnmount(mountPoint)
			_ = fsys.Unmount()
		}
		_ = fsys.Close()
	})
	return fsys, mountPoint
}

func TestDetachedHandleReadWriteStatChmod(t *testing.T) {
	_, mount := mountDetachTest(t)
	full := filepath.Join(mount, "detachrw")
	f, err := os.OpenFile(full, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write([]byte("keep")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Remove(full); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if _, err := os.Stat(full); !os.IsNotExist(err) {
		t.Fatalf("stat after unlink: want ENOENT, got %v", err)
	}
	// fstat on the surviving fd serves the staged size, not ESTALE.
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatalf("fstat detached: %v", err)
	}
	if st.Size != 4 {
		t.Fatalf("fstat detached size: want 4, got %d", st.Size)
	}
	if st.Nlink != 0 {
		t.Fatalf("fstat detached nlink: want 0, got %d", st.Nlink)
	}
	// futimens and fchmod on the surviving fd succeed; the change is
	// staged into the overlay (discarded at close, like the data).
	ut := []unix.Timespec{{Sec: 1700000000, Nsec: 0}, {Sec: 1700000001, Nsec: 0}}
	futimens(t, f, ut)
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatalf("fstat after futimens: %v", err)
	}
	if st.Mtim.Sec != 1700000001 {
		t.Fatalf("fstat mtime after futimens: want 1700000001, got %d", st.Mtim.Sec)
	}
	if err := f.Chmod(0o600); err != nil {
		t.Fatalf("fchmod detached: %v", err)
	}
	// Growing through the detached fd works and close succeeds: the
	// kernel's close-time mtime flush must not fail.
	if _, err := f.WriteAt([]byte("more"), 4); err != nil {
		t.Fatalf("writeat detached: %v", err)
	}
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatalf("fstat after grow: %v", err)
	}
	if st.Size != 8 {
		t.Fatalf("fstat grown size: want 8, got %d", st.Size)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close detached (want success, data discarded): %v", err)
	}
}

func TestDetachedHandleReadOnlyStat(t *testing.T) {
	_, mount := mountDetachTest(t)
	full := filepath.Join(mount, "detachro")
	if err := os.WriteFile(full, []byte("data"), 0o644); err != nil {
		t.Fatalf("create: %v", err)
	}
	f, err := os.Open(full)
	if err != nil {
		t.Fatalf("open ro: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := os.Remove(full); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	// A read-only survivor has no overlay: fstat serves the open-time
	// pin, futimens is accepted-and-discarded, close succeeds.
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatalf("fstat detached ro: %v", err)
	}
	if st.Size != 4 {
		t.Fatalf("fstat detached ro size: want 4, got %d", st.Size)
	}
	ut := []unix.Timespec{{Sec: 1700000000, Nsec: 0}, {Sec: 1700000001, Nsec: 0}}
	futimens(t, f, ut)
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("read detached ro: %v", err)
	}
	if string(buf) != "data" {
		t.Fatalf("read detached ro: want %q, got %q", "data", buf)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close detached ro: %v", err)
	}
}

// TestRecreateNeverServesStalePin closes audit H1 (delete-recreate storm
// asserting pin identity). The feared shape is a delete plus recreate
// cycle hitting a stale open-time pin: same path, same size, same
// second-precision stamps. Three independent barriers make it
// impossible, and this storm proves the observable behavior: (1) the
// inode allocator is persisted-monotonic, so the recreated file never
// reuses the inode the pin key requires; (2) unlink evicts the path's
// pins outright (dropPinnedForPath); (3) even a key collision would need
// identical size plus mtime plus ctime. Fifty same-size, same-second
// delete-recreate cycles must always read the fresh content.
func TestRecreateNeverServesStalePin(t *testing.T) {
	_, mount := mountDetachTest(t)
	full := filepath.Join(mount, "pinstorm")
	for i := 0; i < 50; i++ {
		want := []byte{byte(i), byte(i >> 8), 'x', 'x'}
		if err := os.WriteFile(full, want, 0o644); err != nil {
			t.Fatalf("cycle %d create: %v", i, err)
		}
		f, err := os.Open(full)
		if err != nil {
			t.Fatalf("cycle %d open: %v", i, err)
		}
		got := make([]byte, 4)
		if _, err := f.ReadAt(got, 0); err != nil {
			_ = f.Close()
			t.Fatalf("cycle %d read: %v", i, err)
		}
		if string(got) != string(want) {
			_ = f.Close()
			t.Fatalf("cycle %d: stale pin served want %q got %q", i, want, got)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("cycle %d close: %v", i, err)
		}
		if err := os.Remove(full); err != nil {
			t.Fatalf("cycle %d unlink: %v", i, err)
		}
	}
}
