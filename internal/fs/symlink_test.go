package fs

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func TestSymlinkResolution(t *testing.T) {
	t.Parallel()
	m := meta.NewRepoMetadata("demo")
	m.EnsureDirectory("docs", 1)
	m.EnsureDirectory("target", 1)
	m.UpsertFile("docs/base.txt", meta.FileMeta{Size: 0, Inode: 3}, 1)
	// Relative link inside the same directory.
	m.UpsertFile("docs/link", meta.FileMeta{Symlink: "base.txt", Inode: 4}, 1)
	// Absolute link to a directory.
	m.UpsertFile("abs", meta.FileMeta{Symlink: "/target", Inode: 5}, 1)

	resolved, _, err := StatResolveTracked(m, "docs/link")
	if err != nil || resolved != "docs/base.txt" {
		t.Fatalf("relative link resolved to %q err=%v", resolved, err)
	}
	resolved, _, err = LstatResolveTracked(m, "docs/link")
	if err != nil || resolved != "docs/link" {
		t.Fatalf("no-follow final resolved to %q err=%v", resolved, err)
	}
	resolved, _, err = StatResolveTracked(m, "abs")
	if err != nil || resolved != "target" {
		t.Fatalf("absolute dir link resolved to %q err=%v", resolved, err)
	}
	// stat() semantics compose StatResolve with a lookup.
	resolved, _, err = StatResolveTracked(m, "docs/link")
	if err != nil {
		t.Fatalf("resolve followed: %v", err)
	}
	attrs, err := lookupNode(m, resolved)
	if err != nil || attrs.Kind == meta.NodeKindSymlink {
		t.Fatalf("followed lookup should report target attrs, got %+v err=%v", attrs, err)
	}

	// Traversal permission checks resolve intermediate links.
	if err := CheckWalk(context.Background(), m, "docs/link"); err != nil {
		t.Fatalf("traverse through symlink failed: %v", err)
	}

	// Cycles must fail with ELOOP, not hang or succeed.
	m.UpsertFile("loop-a", meta.FileMeta{Symlink: "loop-b", Inode: 6}, 1)
	m.UpsertFile("loop-b", meta.FileMeta{Symlink: "loop-a", Inode: 7}, 1)
	if _, _, err := StatResolveTracked(m, "loop-a"); err != syscall.ELOOP {
		t.Fatalf("expected ELOOP for cyclic links, got %v", err)
	}
}

// An absolute symlink resets the resolution cursor, so the ancestors
// of the link's own parent chain must still be exec-checked. A 0700
// directory containing "link -> /pub/x" must not leak the link's existence
// or target to a caller with no permission on the directory.
func TestCheckWalkAbsoluteSymlinkKeepsLinkParentChain(t *testing.T) {
	t.Parallel()
	m := meta.NewRepoMetadata("demo")
	m.EnsureDirectory("v", 1)
	m.EnsureDirectory("pub", 1)
	m.UpsertFile("pub/x", meta.FileMeta{Size: 1, Inode: 3}, 1)
	m.UpsertFile("v/link", meta.FileMeta{Symlink: "/pub/x", Mode: 0o777, Inode: 4}, 1)
	v := m.GetDirectory("v")
	v.Mode = 0o700
	v.UID = 1000
	v.GID = 1000
	m.Dirs()["v"] = *v
	pub := m.GetDirectory("pub")
	pub.Mode = 0o755
	m.Dirs()["pub"] = *pub
	m.RebuildIndexes()

	attacker := WithIdentity(context.Background(), Identity{UID: 1001, GID: 1001, Groups: []uint32{1001}})
	if err := CheckWalk(attacker, m, "v/link"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected EACCES reaching a link inside a 0700 dir, got %v", err)
	}
	if err := CheckWalk(attacker, m, "v/link/deeper"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected EACCES through the absolute link, got %v", err)
	}
	owner := WithIdentity(context.Background(), Identity{UID: 1000, GID: 1000, Groups: []uint32{1000}})
	if err := CheckWalk(owner, m, "v/link"); err != nil {
		t.Fatalf("owner traverse: %v", err)
	}
}

// The hop budget must match Linux's SYMLOOP_MAX (40); legitimate
// deep chains get spurious ELOOP below it.
func TestStatResolveDeepChainWithinLinuxSymlinkLimit(t *testing.T) {
	t.Parallel()
	m := meta.NewRepoMetadata("demo")
	m.UpsertFile("leaf", meta.FileMeta{Size: 1, Inode: 2}, 1)
	for i := 0; i < 30; i++ {
		target := "leaf"
		if i > 0 {
			target = fmt.Sprintf("link%d", i-1)
		}
		m.UpsertFile(fmt.Sprintf("link%d", i), meta.FileMeta{Symlink: target, Inode: uint64(10 + i)}, 1)
	}
	resolved, _, err := StatResolveTracked(m, "link29")
	if err != nil || resolved != "leaf" {
		t.Fatalf("30-hop chain must resolve under the 40-hop limit, got %q err=%v", resolved, err)
	}
}
