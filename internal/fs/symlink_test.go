package fs

import (
	"context"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestSymlinkResolution(t *testing.T) {
	m := meta.NewRepoMetadata("demo")
	m.EnsureDirectory("docs", 1)
	m.EnsureDirectory("target", 1)
	m.UpsertFile("docs/base.txt", meta.FileMeta{Size: 0, Inode: 3}, 1)
	// Relative link inside the same directory.
	m.UpsertFile("docs/link", meta.FileMeta{Symlink: "base.txt", Inode: 4}, 1)
	// Absolute link to a directory.
	m.UpsertFile("abs", meta.FileMeta{Symlink: "/target", Inode: 5}, 1)

	resolved, err := ResolvePath(m, "docs/link", true)
	if err != nil || resolved != "docs/base.txt" {
		t.Fatalf("relative link resolved to %q err=%v", resolved, err)
	}
	resolved, err = ResolvePath(m, "docs/link", false)
	if err != nil || resolved != "docs/link" {
		t.Fatalf("no-follow final resolved to %q err=%v", resolved, err)
	}
	resolved, err = ResolvePath(m, "abs", true)
	if err != nil || resolved != "target" {
		t.Fatalf("absolute dir link resolved to %q err=%v", resolved, err)
	}
	attrs, err := LookupNodeFollowed(m, "docs/link")
	if err != nil || attrs.Kind == meta.NodeKindSymlink {
		t.Fatalf("followed lookup should report target attrs, got %+v err=%v", attrs, err)
	}

	// Traversal permission checks resolve intermediate links.
	if err := CheckTraverse(context.Background(), m, "docs/link"); err != nil {
		t.Fatalf("traverse through symlink failed: %v", err)
	}

	// Cycles must fail with ELOOP, not hang or succeed.
	m.UpsertFile("loop-a", meta.FileMeta{Symlink: "loop-b", Inode: 6}, 1)
	m.UpsertFile("loop-b", meta.FileMeta{Symlink: "loop-a", Inode: 7}, 1)
	if _, err := ResolvePath(m, "loop-a", true); err != syscall.ELOOP {
		t.Fatalf("expected ELOOP for cyclic links, got %v", err)
	}
}
