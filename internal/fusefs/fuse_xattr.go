package fusefs

import (
	"context"
	"strings"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func (n *storhubNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return 0, stale
	}
	data, err := n.fs.hub.GetXAttrContext(ctx, n.fs.project, targetPath, attr)
	if err != nil {
		return 0, errnoFromError(err)
	}
	if len(dest) == 0 {
		return uint32(len(data)), 0
	}
	if len(dest) < len(data) {
		return uint32(len(data)), syscall.ERANGE
	}
	copy(dest, data)
	return uint32(len(data)), 0
}

func (n *storhubNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	// Enforce the xattr resource caps at the FUSE boundary too
	// (defense in depth; posix.Service.SetXAttrContext is the authority).
	if len(attr) > shfs.XAttrNameMax || len(data) > shfs.XAttrSizeMax {
		return syscall.ERANGE
	}
	// XATTR_CREATE/XATTR_REPLACE are enforced atomically inside
	// posix.Service.SetXAttrContext's transaction (via shfs.XAttrMode), so
	// there is no separate Get-then-set TOCTOU window. errnoFromError maps
	// the transaction's EEXIST / XAttrNotFound back to the FUSE errno.
	var mode shfs.XAttrMode
	if flags&xattrCreate != 0 {
		mode |= shfs.XAttrCreate
	}
	if flags&xattrReplace != 0 {
		mode |= shfs.XAttrReplace
	}
	if err := n.fs.hub.SetXAttrContext(ctx, n.fs.project, targetPath, attr, data, mode); err != nil {
		return errnoFromError(err)
	}
	// The xattr write bumps the file's ctime server-side; kernels holding
	// cached attrs (AttrTimeout) would serve the stale ctime without this.
	// Same funnel as setattr's tail: entry/content copies elsewhere expire
	// only via invalidation.
	n.fs.notifyKernelContentChanged(n.inode)
	return 0
}

func (n *storhubNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return 0, stale
	}
	attrs, err := n.fs.hub.ListXAttrContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return 0, errnoFromError(err)
	}
	payload := []byte(strings.Join(attrs, "\x00"))
	if len(attrs) > 0 {
		payload = append(payload, 0)
	}
	if len(dest) == 0 {
		return uint32(len(payload)), 0
	}
	if len(dest) < len(payload) {
		return uint32(len(payload)), syscall.ERANGE
	}
	copy(dest, payload)
	return uint32(len(payload)), 0
}

func (n *storhubNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	if err := n.fs.hub.RemoveXAttrContext(ctx, n.fs.project, targetPath, attr); err != nil {
		return errnoFromError(err)
	}
	// Same ctime-invalidation reasoning as Setxattr above.
	n.fs.notifyKernelContentChanged(n.inode)
	return 0
}
