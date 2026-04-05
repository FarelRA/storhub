package storage

import (
	"context"
	"log/slog"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func (h *StorHub) Logger() *slog.Logger {
	return h.logger
}

func (h *StorHub) projectLogger(project string) *slog.Logger {
	return h.logger.With("project", project)
}

// QueueAtimeUpdateContext updates atime directly in metadata (simple batching system)
func (h *StorHub) QueueAtimeUpdateContext(ctx context.Context, project, targetPath string, isDir bool, now time.Time) {
	if project == "" {
		return
	}

	// Check if atime updates are disabled
	if h.config.AtimePolicy == "noatime" {
		return
	}

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Update atime directly in metadata
	if isDir {
		if targetPath == "" {
			// Root directory
			if shfs.ShouldUpdateAtime(h.config.AtimePolicy, pm.meta.Root.AccessedAt, pm.meta.Root.ModifiedAt, pm.meta.Root.ChangedAt, now) {
				pm.meta.Root.AccessedAt = now.UTC()
				pm.dirty = true
			}
		} else {
			// Subdirectory
			dir := pm.meta.GetDirectory(targetPath)
			if dir != nil && shfs.ShouldUpdateAtime(h.config.AtimePolicy, dir.AccessedAt, dir.ModifiedAt, dir.ChangedAt, now) {
				dir.AccessedAt = now.UTC()
				pm.dirty = true
			}
		}
	} else {
		// File
		file := pm.meta.FindFile(targetPath)
		if file != nil && shfs.ShouldUpdateAtime(h.config.AtimePolicy, file.AccessedAt, file.ModifiedAt, file.ChangedAt, now) {
			file.AccessedAt = now.UTC()
			pm.dirty = true
		}
	}
}
