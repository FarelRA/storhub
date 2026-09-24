package storage

import (
	"context"
	"log/slog"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
)

// Logger returns the hub-wide structured logger.
func (h *StorHub) Logger() *slog.Logger {
	return h.logger
}

// projectLogger returns the project-bound logger, cached per project.
// logger.With allocates a new slog.Logger on every call, and this sits on
// the hot path of every operation; the cache makes repeat calls allocation
// free. sync.Map is the right shape: writes are rare (one per project) and
// reads dominate.
func (h *StorHub) projectLogger(project string) *slog.Logger {
	if cached, ok := h.loggers.Load(project); ok {
		return cached.(*slog.Logger)
	}
	logger := h.logger.With("project", project)
	actual, _ := h.loggers.LoadOrStore(project, logger)
	return actual.(*slog.Logger)
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

	// Degraded projects still queue atime through this cheap metadata-only
	// path: an atime bump is one OpSetattr with no asset uploads, and
	// silently skipping it would leave stale stamps on every read made
	// while degraded. Reads (which queue atime) keep working, and the
	// stamp is correct. The commit loop still refuses or fails loudly per
	// the degraded policy; rescue verbs (drain, rollback) carry the
	// queued atime ops like any other pending work.

	pm := h.getOrCreateProjectMeta(project)
	pm.mu.Lock()

	// Cold-cache guard: advisory or not, writing into an empty unhydrated
	// tree and committing it would clobber remote state (an empty-tree-over-remote clobber);
	// hydrate exactly like the mutation transaction path.
	if err := h.ensureHydratedLocked(ctx, project, pm); err != nil {
		pm.mu.Unlock()
		logging.Warn(h.projectLogger(project), "atime update skipped; hydration failed, will retry on next access", "path", targetPath, "err", err)
		return
	}

	// Size-ceiling gate (uncommittable-dirty livelock guard): a capped project can never commit
	// growth, so appending atime ops only grows opStack toward the 4096
	// force-retry while every commit fails oversizeError and re-arms.
	// Drop advisory atime like noatime; direct mutation paths already
	// gate via ensureMutableLocked (caches.go:334).
	if pm.sizeCapped {
		pm.mu.Unlock()
		return
	}

	// A non-nil trigger channel means dirtiness was actually marked.
	var trigger chan struct{}
	// The published tree is shared with lock-free readers, so an atime bump
	// that actually changes state must go through copy-on-write. The
	// ShouldUpdateAtime policy is evaluated against the shared tree first
	// (read-only); the COW copy is taken only when a write is warranted, so
	// the common no-op read stays allocation-free.
	if isDir {
		if targetPath == "" {
			// Root directory
			if shfs.ShouldUpdateAtime(h.config.AtimePolicy, pm.meta.Root.AccessedAt, pm.meta.Root.ModifiedAt, pm.meta.Root.ChangedAt, now) {
				tree := cloneForWrite(pm.meta)
				tree.Root.AccessedAt = now
				publishTreeLocked(pm, tree, []string{""})
				trigger = h.markProjectDirtyLiveLocked(project, pm)
				root := pm.meta.Root.Clone()
				h.appendOpLocked(project, pm, Op{
					Type: OpSetattr, Paths: []string{""}, Cause: "atime",
					Timestamp: now, Dir: &root,
				})
			}
		} else {
			// Subdirectory (SetDirAtime: GetDirectory returns a copy)
			dir := pm.meta.GetDirectory(targetPath)
			if dir != nil && shfs.ShouldUpdateAtime(h.config.AtimePolicy, dir.AccessedAt, dir.ModifiedAt, dir.ChangedAt, now) {
				tree := cloneForWrite(pm.meta)
				if tree.SetDirAtime(targetPath, now) {
					publishTreeLocked(pm, tree, []string{targetPath})
					trigger = h.markProjectDirtyLiveLocked(project, pm)
					// markProjectDirtyLiveLocked drops pm.mu during
					// eviction revival; a concurrent delete can remove
					// the entry in that window. Re-check and skip
					// the op when the target is gone.
					if updated := pm.meta.GetDirectory(targetPath); updated != nil {
						u := updated.Clone()
						h.appendOpLocked(project, pm, Op{
							Type: OpSetattr, Paths: []string{targetPath}, Cause: "atime",
							Timestamp: now, Dir: &u,
						})
					}
				}
			}
		}
	} else {
		// File (SetFileAtime: FindFile returns a copy)
		file := pm.meta.FindFile(targetPath)
		if file != nil && shfs.ShouldUpdateAtime(h.config.AtimePolicy, file.AccessedAt, file.ModifiedAt, file.ChangedAt, now) {
			tree := cloneForWrite(pm.meta)
			if tree.SetFileAtime(targetPath, now) {
				publishTreeLocked(pm, tree, []string{targetPath})
				trigger = h.markProjectDirtyLiveLocked(project, pm)
				// Same revival-window re-check as the directory branch
				// above: a nil here means the entry was deleted
				// while pm.mu was dropped.
				if updated := pm.meta.FindFile(targetPath); updated != nil {
					u := updated.Clone()
					h.appendOpLocked(project, pm, Op{
						Type: OpSetattr, Paths: []string{targetPath}, Cause: "atime",
						Timestamp: now, File: &u,
					})
				}
			}
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
