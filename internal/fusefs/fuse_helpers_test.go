package fusefs

import (
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// newTestFilesystem builds a bare Filesystem value for unit tests that
// drive notify or broadcast paths without a mount: both notify fields
// are always set, since beginNotify blocks forever on a nil slot channel
// and panics on a nil pending map.
func newTestFilesystem() *Filesystem {
	return &Filesystem{
		notifyQueued: make(map[notifyKey]struct{}),
		notifySlots:  make(chan struct{}, 1),
		relCh:        make(chan struct{}),
	}
}

// mustMount builds a stub-backed Filesystem for tests, failing fast on
// setup errors and closing the mount in cleanup. It replaces the pasted
// New(&stubHub{chunkSize:4}…) + error-check triples.
func mustMount(t *testing.T, hub *stubHub, cacheDir string, opts Options) *Filesystem {
	t.Helper()
	if cacheDir == "" {
		cacheDir = t.TempDir()
	}
	opts.CacheDir = cacheDir
	fsys, err := New(hub, "demo", opts)
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	return fsys
}

// mkEntry builds a canonical file EntryInfo for stubHub fixtures: regular
// file, caller-owned, single link.
func mkEntry(path string, size int64, now int64) *shfs.EntryInfo {
	return &shfs.EntryInfo{
		Path: path, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1,
		Size: size, ModifiedAt: now, AccessedAt: now, ChangedAt: now,
	}
}
