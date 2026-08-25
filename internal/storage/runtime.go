package storage

import (
	"context"
	"log/slog"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
)

func (h *StorHub) Logger() *slog.Logger {
	return h.logger
}

func (h *StorHub) projectLogger(project string) *slog.Logger {
	return h.logger.With("project", project)
}

// QueueAtimeUpdateContext updates atime directly in metadata (simple batching system)
func (h *StorHub) QueueAtimeUpdateContext(ctx context.Context, project, targetPath string, isDir bool, now int64) {
	if project == "" {
		return
	}
	// Atime updates are advisory; drop them once the driving request has
	// been canceled instead of touching metadata nobody will observe.
	if err := ctx.Err(); err != nil {
		return
	}

	// Check if atime updates are disabled
	if h.config.AtimePolicy == "noatime" {
		return
	}

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	// Cold-cache guard: advisory or not, writing into an empty unhydrated
	// tree and committing it would clobber remote state (the round-3 P0
	// class); hydrate exactly like the mutation transaction path.
	if err := h.ensureHydratedLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		logging.Error(h.projectLogger(project), "atime update skipped; hydration failed", "project", project, "path", targetPath, "err", err)
		return
	}

	// A non-nil trigger channel means dirtiness was actually marked.
	var trigger chan struct{}
	// Update atime directly in metadata
	if isDir {
		if targetPath == "" {
			// Root directory
			if shfs.ShouldUpdateAtime(h.config.AtimePolicy, pm.meta.Root.AccessedAt, pm.meta.Root.ModifiedAt, pm.meta.Root.ChangedAt, now) {
				pm.meta.Root.AccessedAt = now
				trigger = h.markProjectDirtyLiveLocked(project, pm)
			}
		} else {
			// Subdirectory
			dir := pm.meta.GetDirectory(targetPath)
			if dir != nil && shfs.ShouldUpdateAtime(h.config.AtimePolicy, dir.AccessedAt, dir.ModifiedAt, dir.ChangedAt, now) {
				dir.AccessedAt = now
				trigger = h.markProjectDirtyLiveLocked(project, pm)
			}
		}
	} else {
		// File
		file := pm.meta.FindFile(targetPath)
		if file != nil && shfs.ShouldUpdateAtime(h.config.AtimePolicy, file.AccessedAt, file.ModifiedAt, file.ChangedAt, now) {
			file.AccessedAt = now
			trigger = h.markProjectDirtyLiveLocked(project, pm)
		}
	}
	pm.mu.Unlock()

	// A read that actually moved an atime pokes the commit loop like any
	// other mutation; without this the dirtiness would wait for a later
	// operation or shutdown, since there is no periodic flush.
	if trigger != nil {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
}
