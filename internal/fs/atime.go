// Package fs implements the key-addressed file verbs and access control.
package fs

import (
	"context"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// AtimeBackend is the backend surface atime updates need: policy and queue.
type AtimeBackend interface {
	AtimePolicy() storcfg.AtimePolicy
	QueueAtimeUpdateContext(ctx context.Context, project, targetPath string, isDir bool, now int64)
}

type atimeContextKey string

const suppressAtimeContextKey atimeContextKey = "storhub.suppress_atime"

// WithSuppressedAtime returns a context that skips atime queueing.
func WithSuppressedAtime(ctx context.Context) context.Context {
	return context.WithValue(ctx, suppressAtimeContextKey, true)
}

// AtimeSuppressed reports whether the context skips atime queueing.
func AtimeSuppressed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	value, _ := ctx.Value(suppressAtimeContextKey).(bool)
	return value
}

// ShouldUpdateAtime applies the atime policy ladder to one node.
func ShouldUpdateAtime(policy storcfg.AtimePolicy, accessedAt, modifiedAt, changedAt, now int64) bool {
	switch policy {
	case storcfg.AtimeNo:
		return false
	case storcfg.AtimeStrict:
		return true
	default:
	}
	// Relatime ladder, cheapest test first: a never-accessed node always
	// updates; an access older than the mtime/ctime is not a reread. A
	// zero mtime keeps the historical always-update behavior (such nodes
	// predate tracked timestamps; treating them as dirty is fail-safe).
	if accessedAt == 0 || modifiedAt == 0 {
		return true
	}
	if accessedAt < modifiedAt {
		return true
	}
	if changedAt != 0 && accessedAt < changedAt {
		return true
	}
	return (now - accessedAt) >= relatimeInterval
}

// relatimeInterval is the Linux default relatime window (24h) in Unix
// nanoseconds: atime updates at most once per interval unless
// mtime/ctime moved first.
const relatimeInterval = 86_400_000_000_000

// TouchAccessTime queues one atime update for the node at targetPath,
// honouring the backend policy and the suppression context. It is the
// single funnel behind TouchFileAccessTime/TouchDirectoryAccessTime.
func TouchAccessTime(ctx context.Context, backend AtimeBackend, project, targetPath string, isDir bool, now int64) {
	policy := backend.AtimePolicy()
	if policy == storcfg.AtimeNo || AtimeSuppressed(ctx) {
		return
	}
	backend.QueueAtimeUpdateContext(ctx, project, targetPath, isDir, now)
}

// TouchFileAccessTime queues one atime update for a file node.
func TouchFileAccessTime(ctx context.Context, backend AtimeBackend, project, targetPath string, now int64) {
	TouchAccessTime(ctx, backend, project, targetPath, false, now)
}

// TouchDirectoryAccessTime queues one atime update for a directory node.
func TouchDirectoryAccessTime(ctx context.Context, backend AtimeBackend, project, targetPath string, now int64) {
	TouchAccessTime(ctx, backend, project, targetPath, true, now)
}
