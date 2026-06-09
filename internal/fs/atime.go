package fs

import (
	"context"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

type AtimeBackend interface {
	AtimePolicy() storcfg.AtimePolicy
	QueueAtimeUpdateContext(ctx context.Context, project, targetPath string, isDir bool, now int64)
}

type atimeContextKey string

const suppressAtimeContextKey atimeContextKey = "storhub.suppress_atime"

func WithSuppressedAtime(ctx context.Context) context.Context {
	return context.WithValue(ctx, suppressAtimeContextKey, true)
}

func AtimeSuppressed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	value, _ := ctx.Value(suppressAtimeContextKey).(bool)
	return value
}

func ShouldUpdateAtime(policy storcfg.AtimePolicy, accessedAt, modifiedAt, changedAt, now int64) bool {
	switch policy {
	case storcfg.AtimeNo:
		return false
	case storcfg.AtimeStrict:
		return true
	default:
		if accessedAt == 0 {
			return true
		}
		if modifiedAt != 0 && accessedAt >= modifiedAt {
			if changedAt == 0 || accessedAt >= changedAt {
				return (now - accessedAt) >= 86400
			}
		}
		return true
	}
}

func TouchFileAccessTime(ctx context.Context, backend AtimeBackend, project, targetPath string, now int64) {
	policy := backend.AtimePolicy()
	if policy == storcfg.AtimeNo || AtimeSuppressed(ctx) {
		return
	}
	backend.QueueAtimeUpdateContext(ctx, project, targetPath, false, now)
}

func TouchDirectoryAccessTime(ctx context.Context, backend AtimeBackend, project, targetPath string, now int64) {
	policy := backend.AtimePolicy()
	if policy == storcfg.AtimeNo || AtimeSuppressed(ctx) {
		return
	}
	backend.QueueAtimeUpdateContext(ctx, project, targetPath, true, now)
}
