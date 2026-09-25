package fusefs

import (
	"context"
	"path"
	"strings"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// invalidateSelf drops the kernel's cached entry (and with it the cached
// attrs) for the node's own path: xattr writes bump ctime server-side,
// and AttrTimeout kernels would otherwise serve the stale ctime until it
// expires. Content notification would be the wrong cache (the bytes did
// not change); the entry is what carries the attrs.
func (n *storhubNode) invalidateSelf(targetPath string) {
	dir, base := path.Split(targetPath)
	n.fs.notifyEntryForPath(strings.TrimSuffix(dir, "/"), base)
}

func (n *storhubNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return 0, stale
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("getxattr start", "path", targetPath, "attr", attr)
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
	if n.fs.debugEnabled() {
		n.fs.debugOp("getxattr complete", "path", targetPath, "attr", attr, "size", len(data))
	}
	return uint32(len(data)), 0
}

func (n *storhubNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("setxattr start", "path", targetPath, "attr", attr, "size", len(data))
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
	if flags&uint32(shfs.XAttrCreate) != 0 {
		mode |= shfs.XAttrCreate
	}
	if flags&uint32(shfs.XAttrReplace) != 0 {
		mode |= shfs.XAttrReplace
	}
	if err := n.fs.hub.SetXAttrContext(ctx, n.fs.project, targetPath, attr, data, mode); err != nil {
		return errnoFromError(err)
	}
	n.invalidateSelf(targetPath)
	if n.fs.debugEnabled() {
		n.fs.debugOp("setxattr complete", "path", targetPath, "attr", attr, "size", len(data))
	}
	return 0
}

func (n *storhubNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return 0, stale
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("listxattr start", "path", targetPath)
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
	if n.fs.debugEnabled() {
		n.fs.debugOp("listxattr complete", "path", targetPath, "count", len(attrs))
	}
	return uint32(len(payload)), 0
}

func (n *storhubNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath, stale := n.safePath()
	if stale != 0 {
		return stale
	}
	if n.fs.debugEnabled() {
		n.fs.debugOp("removexattr start", "path", targetPath, "attr", attr)
	}
	if err := n.fs.hub.RemoveXAttrContext(ctx, n.fs.project, targetPath, attr); err != nil {
		return errnoFromError(err)
	}
	// Same ctime-invalidation reasoning as Setxattr above.
	n.invalidateSelf(targetPath)
	if n.fs.debugEnabled() {
		n.fs.debugOp("removexattr complete", "path", targetPath, "attr", attr)
	}
	return 0
}
