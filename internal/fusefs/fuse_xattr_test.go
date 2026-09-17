package fusefs

import (
	"context"
	"testing"
)

// Xattr mutations must invalidate kernel attr caches: the write bumps the
// file's ctime server-side, and kernels holding AttrTimeout copies would
// otherwise serve the stale ctime indefinitely (C9).
func TestXattrMutationsInvalidateKernelCaches(t *testing.T) {
	t.Parallel()
	fake := &stubHub{
		setXAttr:    func(context.Context, string, string, string, []byte) error { return nil },
		removeXAttr: func(context.Context, string, string, string) error { return nil },
	}
	fsys := mustMount(t, fake, "", Options{})
	fsys.mu.Lock()
	fsys.pathToInode["docs/f.txt"] = 7
	fsys.inodePaths[7] = map[string]struct{}{"docs/f.txt": {}}
	fsys.mu.Unlock()
	node := &storhubNode{fs: fsys, inode: 7}
	base := fsys.invalCount.Load()
	if errno := node.Setxattr(context.Background(), "user.k", []byte("v"), 0); errno != 0 {
		t.Fatalf("setxattr: %v", errno)
	}
	if errno := node.Removexattr(context.Background(), "user.k"); errno != 0 {
		t.Fatalf("removexattr: %v", errno)
	}
	if got := fsys.invalCount.Load() - base; got != 2 {
		t.Fatalf("expected 2 invalidations (set + remove), got %d", got)
	}
}
