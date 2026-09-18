package fusefs

import (
	"context"
	"math"
	"path"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
)

// Compile-time proof that file nodes serve the kernel's copy_file_range
// (go-fuse v2.11.0 surfaces it as the filesystem-level
// NodeCopyFileRanger.CopyFileRange: source fh plus offsets, dest node/fh).
var _ gofusefs.NodeCopyFileRanger = (*storhubNode)(nil)

// CopyFileRange serves copy_file_range(2) as one server-side CloneRange:
// zero bytes travel through the caller, the destination records reference
// the same release assets the source bytes already live in.
//
// Handle resolution mirrors Rename: each side prefers its open handle's
// path (an open-but-renamed handle keeps addressing its own file) and
// falls back to the node's current path. The caller identity travels in
// ctx via callerContext; DAC and CAS stay inside the core op, never
// re-checked here.
//
// Overlay rule: the clone sees committed state only. Source bytes staged
// in a live overlay temp but not yet committed are NOT part of the clone,
// even where the read path (readLiveOverlay) would serve them to a
// reader; symmetrically, a live overlay on the destination keeps
// shadowing the cloned spans for readers until its own commit, and that
// later commit wins for its dirty spans. Committing the overlays first
// would publish user data earlier than the application's own fsync asked
// for, so the handler documents the divergence instead of auto-flushing.
//
// Kernels that never issue the syscall keep working through read/write
// emulation: there is deliberately no other path here.
func (n *storhubNode) CopyFileRange(ctx context.Context, fhIn gofusefs.FileHandle, offIn uint64, out *gofusefs.Inode, fhOut gofusefs.FileHandle, offOut uint64, length uint64, flags uint64) (uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	if flags != 0 {
		return 0, syscall.EINVAL
	}
	if length == 0 {
		return 0, 0
	}
	if offIn > math.MaxInt64 || offOut > math.MaxInt64 || length > math.MaxInt64 {
		return 0, syscall.EINVAL
	}
	if length > math.MaxUint32 {
		// The wire return is uint32: callers that need more loop with
		// smaller lenses (cp does), so an oversized single shot is a
		// usage error, not a short copy. Cloning only a prefix would
		// break the core's memmove snapshot semantics for same-file
		// overlaps across the loop.
		return 0, syscall.EINVAL
	}
	srcPath, errno := copySourcePath(n, fhIn)
	if errno != 0 {
		return 0, errno
	}
	dstPath, errno := copyDestPath(n.fs, out, fhOut)
	if errno != 0 {
		return 0, errno
	}
	if _, err := n.fs.hub.CloneRange(ctx, n.fs.project, srcPath, int64(offIn), dstPath, int64(offOut), int64(length)); err != nil {
		return 0, errnoFromError(err)
	}
	// The destination content moved under every cached view: drop shared
	// open-time layouts keyed by this path (open handles keep their own
	// pin pointers and are unaffected), wake the parent namespace for the
	// create case, and invalidate kernel content/attr caches when the
	// inode is still tracked. Every branch bumps invalCount through the
	// notify helpers, so the 60s timeouts never serve the pre-clone tree.
	n.fs.dropPinnedForPath(dstPath)
	n.fs.notifyEntryForPath(shfs.ParentPath(dstPath), path.Base(dstPath))
	n.fs.mu.RLock()
	dstInode, tracked := n.fs.pathToInode[dstPath]
	n.fs.mu.RUnlock()
	if tracked {
		n.fs.notifyKernelContentChanged(dstInode)
	}
	n.fs.debugf("copy_file_range src=%s src_off=%d dst=%s dst_off=%d len=%d", srcPath, offIn, dstPath, offOut, length)
	return uint32(length), 0
}

// copySourcePath resolves the clone source: the input handle's path when
// it still names a file, else the receiving node's current path (a
// handleless copy issued against the source inode itself).
func copySourcePath(n *storhubNode, fhIn gofusefs.FileHandle) (string, syscall.Errno) {
	if h, ok := fhIn.(*storhubHandle); ok && h != nil {
		h.mu.Lock()
		targetPath, detached := h.path, h.deleted
		h.mu.Unlock()
		if targetPath != "" && !detached {
			return targetPath, 0
		}
	}
	return n.safePath()
}

// copyDestPath resolves the clone destination: the output handle's path
// when it still names a file, else the destination node's current path.
func copyDestPath(fs *Filesystem, out *gofusefs.Inode, fhOut gofusefs.FileHandle) (string, syscall.Errno) {
	if h, ok := fhOut.(*storhubHandle); ok && h != nil {
		h.mu.Lock()
		targetPath, detached := h.path, h.deleted
		h.mu.Unlock()
		if targetPath != "" && !detached {
			return targetPath, 0
		}
	}
	if out == nil {
		return "", syscall.EINVAL
	}
	ops := out.Operations()
	node, ok := ops.(*storhubNode)
	if !ok || node == nil {
		return "", syscall.EINVAL
	}
	return node.safePath()
}
