package fs

import (
	"context"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

type AtimeBackend interface {
	AtimePolicy() storcfg.AtimePolicy
	UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error)
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
	_, _ = backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		file := repo.FindFile(targetPath)
		if file == nil {
			return nil
		}
		if !ShouldUpdateAtime(policy, file.AccessedAt, file.ModifiedAt, file.ChangedAt, now) {
			return nil
		}
		file.AccessedAt = now.UTC()
		return nil
	}, "storhub: touch atime "+targetPath)
}

func TouchDirectoryAccessTime(ctx context.Context, backend AtimeBackend, project, targetPath string, now time.Time) {
	policy := backend.AtimePolicy()
	if policy == storcfg.AtimeNo || AtimeSuppressed(ctx) {
		return
	}
	_, _ = backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
		if targetPath == "" {
			if !ShouldUpdateAtime(policy, repo.Root.AccessedAt, repo.Root.ModifiedAt, repo.Root.ChangedAt, now) {
				return nil
			}
			repo.Root.AccessedAt = now.UTC()
			return nil
		}
		dir := repo.GetDirectory(targetPath)
		if dir == nil {
			return nil
		}
		if !ShouldUpdateAtime(policy, dir.AccessedAt, dir.ModifiedAt, dir.ChangedAt, now) {
			return nil
		}
		dir.AccessedAt = now.UTC()
		return nil
	}, "storhub: touch dir atime "+targetPath)
}
