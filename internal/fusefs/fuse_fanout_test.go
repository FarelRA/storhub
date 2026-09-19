package fusefs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFanoutInvalidationBeatsTimeout proves cross-surface fan-out end to
// end: with entry/attr/negative timeouts at 10 minutes, only a kernel
// invalidation (not expiry) can make a backend-side change visible. The
// backend mutation below simulates a REST publish on the shared hub;
// pollInvalidationsOnce is the same method the background loop calls.
func TestFanoutInvalidationBeatsTimeout(t *testing.T) {
	backend := newPCHub()
	longTimeouts := DefaultOptions()
	longTimeouts.EntryTimeout = 10 * time.Minute
	longTimeouts.AttrTimeout = 10 * time.Minute
	longTimeouts.NegativeTimeout = 10 * time.Minute
	cacheDir := t.TempDir()
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatalf("mkdir mount: %v", err)
	}
	fsys, err := New(backend, "demo", Options{CacheDir: cacheDir, EntryTimeout: longTimeouts.EntryTimeout, AttrTimeout: longTimeouts.AttrTimeout, NegativeTimeout: longTimeouts.NegativeTimeout})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	if err := fsys.Mount(mountPoint); err != nil {
		t.Skipf("fan-out mount proof needs a working mount: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(mountPoint); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mount never answered")
		}
		time.Sleep(10 * time.Millisecond)
	}

	ctx := context.Background()
	full := filepath.Join(mountPoint, "f")
	if err := os.WriteFile(full, []byte("v1"), 0o644); err != nil {
		t.Fatalf("write through mount: %v", err)
	}
	st, err := os.Stat(full)
	if err != nil {
		t.Fatalf("stat cached: %v", err)
	}
	if st.Size() != 2 {
		t.Fatalf("cached size: want 2, got %d", st.Size())
	}
	before := fsys.Invalidations()

	// REST-side change, bypassing the mount: grow the file in the backend
	// and journal it for fan-out, exactly like a published verb would.
	if _, err := backend.TruncateFileContext(ctx, "demo", "f", 5); err != nil {
		t.Fatalf("backend truncate: %v", err)
	}
	backend.fanoutFn = func(since uint64) ([]string, bool, uint64) {
		if since > 0 {
			return nil, false, since
		}
		return []string{"f"}, false, 1
	}
	if cur := fsys.pollInvalidationsOnce(0); cur != 1 {
		t.Fatalf("poll cursor: want 1, got %d", cur)
	}
	if fsys.Invalidations() <= before {
		t.Fatal("poll of a changed path issued no invalidations")
	}
	// With 10-minute timeouts this stat can only turn fresh by
	// invalidation. Delivery is async (notify slots), so poll for
	// freshness with a deadline far below the timeout.
	deadline = time.Now().Add(5 * time.Second)
	for {
		st, err = os.Stat(full)
		if err != nil {
			t.Fatalf("stat after fan-out: %v", err)
		}
		if st.Size() == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale size after fan-out: want 5, got %d", st.Size())
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("read after fan-out: %v", err)
	}
	if string(got) != "v1\x00\x00\x00" {
		t.Fatalf("stale content after fan-out: %q", got)
	}

	// Empty window: no spurious invalidations.
	before = fsys.Invalidations()
	if cur := fsys.pollInvalidationsOnce(1); cur != 1 {
		t.Fatalf("empty poll cursor: want 1, got %d", cur)
	}
	if fsys.Invalidations() != before {
		t.Fatal("empty fan-out window issued invalidations")
	}

	// Unknown scope: invalidate-all still turns the stat fresh.
	if _, err := backend.TruncateFileContext(ctx, "demo", "f", 7); err != nil {
		t.Fatalf("backend truncate 2: %v", err)
	}
	backend.fanoutFn = func(since uint64) ([]string, bool, uint64) {
		return nil, true, since + 1
	}
	before = fsys.Invalidations()
	fsys.pollInvalidationsOnce(1)
	if fsys.Invalidations() <= before {
		t.Fatal("unknown-scope poll issued no invalidations")
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		st, err = os.Stat(full)
		if err != nil {
			t.Fatalf("stat after unknown-scope fan-out: %v", err)
		}
		if st.Size() == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale size after unknown-scope fan-out: want 7, got %d", st.Size())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
