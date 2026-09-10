package fusefs

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// Startup sweep must quarantine leftovers, not delete them.
func TestStartupSweepQuarantinesLeftovers(t *testing.T) {
	cacheDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cacheDir, "inode-aaa"), []byte("dirty-inode"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheDir, "handle-bbb"), []byte("dirty-handle"), 0o600); err != nil {
		t.Fatal(err)
	}
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entries, err := os.ReadDir(filepath.Join(cacheDir, "recovery"))
	if err != nil {
		t.Fatalf("expected recovery dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 quarantined files, got %d", len(entries))
	}
	found := map[string]bool{}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(cacheDir, "recovery", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		found[string(data)] = true
	}
	if !found["dirty-inode"] || !found["dirty-handle"] {
		t.Fatalf("quarantined data lost: %v", found)
	}
	rootEntries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range rootEntries {
		if !e.IsDir() && e.Name() != mountLockFileName {
			t.Fatalf("stray temp left in cache root: %s", e.Name())
		}
	}
}

// POSIX unlinked-open-handle semantics pin: commit on a deleted handle
// succeeds (data lives in the temp overlay until Release discards it,
// link count zero). See TestFUSEHandleRenameAndUnlinkSemantics.
func TestCommitDeletedHandleKeepsPosixSemantics(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 7, "demo.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	if n, errno := h.Write(context.Background(), []byte("unlinked-data"), 0); errno != 0 || n != 13 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	// Simulate unlink-after-open.
	h.mu.Lock()
	h.path = ""
	h.deleted = true
	h.mu.Unlock()
	h.writeState.mu.Lock()
	h.writeState.path = ""
	h.writeState.deleted = true
	h.writeState.mu.Unlock()
	if errno := h.commit(context.Background()); errno != 0 {
		t.Fatalf("commit on deleted handle must succeed (POSIX), got %v", errno)
	}
}

// Read-only opens must not attach to another writer's writeState.
func TestReadOnlyOpenDoesNotAttachWriteState(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	w, err := fsys.newHandle(context.Background(), 9, "shared.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: 0})
	if err != nil {
		t.Fatalf("new writer handle: %v", err)
	}
	if n, errno := w.Write(context.Background(), []byte("writer-data"), 0); errno != 0 || n != 11 {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	r, err := fsys.newHandle(context.Background(), 9, "shared.bin", syscall.O_RDONLY, nil)
	if err != nil {
		t.Fatalf("new reader handle: %v", err)
	}
	if r.writeState != nil {
		t.Fatal("read-only handle attached another writer's writeState")
		_, _ = r.Release(context.Background()), w.Release(context.Background())
		return
	}
	// Committing the reader must be a no-op success that leaves the
	// writer's dirty state untouched.
	if errno := r.commit(context.Background()); errno != 0 {
		t.Fatalf("reader commit: %v", errno)
	}
	w.writeState.mu.Lock()
	dirty := len(w.writeState.dirtyRanges)
	w.writeState.mu.Unlock()
	if dirty == 0 {
		t.Fatal("reader commit consumed the writer's dirty ranges")
	}
	if errno := r.Release(context.Background()); errno != 0 {
		t.Fatalf("reader release: %v", errno)
	}
	// Writer state must still be registered (reader release must not
	// have dropped its ref).
	fsys.mu.RLock()
	_, stillRegistered := fsys.writeStates[9]
	fsys.mu.RUnlock()
	if !stillRegistered {
		t.Fatal("reader release dropped the writer's writeState registration")
	}
	if errno := w.Release(context.Background()); errno == 0 {
		// Writer commit may succeed against stub hub (truncate no-op);
		// either outcome is fine as long as it does not crash.
		_ = errno
	}
}

// Setlkw must not hold s.mu while taking handle.mu (ABBA vs Release):
// hammer grant + release concurrently; any lock-order inversion deadlocks.
func TestSetlkwReleaseNoDeadlock(t *testing.T) {
	cacheDir := t.TempDir()
	fsys, err := New(&stubHub{}, "demo", Options{CacheDir: cacheDir})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	h, err := fsys.newHandle(context.Background(), 11, "locked.bin", syscall.O_RDONLY, nil)
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	defer func() { _ = h.Release(context.Background()) }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			_ = h.fs.setLock(h.inode, uint64(i+1), fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_UNLCK})
			h.trackLockOwner(uint64(i+1), syscall.F_RDLCK)
			h.releaseTrackedLocks()
		}
	}()
	for i := 0; i < 50; i++ {
		lk := fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_RDLCK}
		_ = h.Setlk(context.Background(), uint64(1000+i), &lk, 0)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: concurrent lock grant + release hung")
	}
}
