package fs

import (
	"context"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

type AtimeBackend interface {
	AtimePolicy() storcfg.AtimePolicy
	QueueAtimeUpdateContext(ctx context.Context, project, targetPath string, isDir bool, now time.Time)
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

func ShouldUpdateAtime(policy storcfg.AtimePolicy, accessedAt, modifiedAt, changedAt, now time.Time) bool {
	switch policy {
	case storcfg.AtimeNo:
		return false
	case storcfg.AtimeStrict:
		return true
	default:
		if accessedAt.IsZero() {
			return true
		}
		if !modifiedAt.IsZero() && !accessedAt.Before(modifiedAt) {
			if changedAt.IsZero() || !accessedAt.Before(changedAt) {
				return now.Sub(accessedAt) >= 24*time.Hour
			}
		}
		return true
	}
}

func TouchFileAccessTime(ctx context.Context, backend AtimeBackend, project, targetPath string, now time.Time) {
	policy := backend.AtimePolicy()
	if policy == storcfg.AtimeNo || AtimeSuppressed(ctx) {
		return
	}
	backend.QueueAtimeUpdateContext(ctx, project, targetPath, false, now.UTC())
}

func TouchDirectoryAccessTime(ctx context.Context, backend AtimeBackend, project, targetPath string, now time.Time) {
	policy := backend.AtimePolicy()
	if policy == storcfg.AtimeNo || AtimeSuppressed(ctx) {
		return
	}
	backend.QueueAtimeUpdateContext(ctx, project, targetPath, true, now.UTC())
}
