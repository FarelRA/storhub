package storage

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

const atimeFlushDelay = 250 * time.Millisecond

type atimeKey struct {
	path string
	dir  bool
}

type atimeUpdate struct {
	path string
	dir  bool
	at   time.Time
}

func (h *StorHub) Logger() *slog.Logger {
	return h.logger
}

func (h *StorHub) projectLogger(project string) *slog.Logger {
	return h.logger.With("project", project)
}

func (h *StorHub) metadataLock(project string) *sync.Mutex {
	h.metaLockMu.Lock()
	defer h.metaLockMu.Unlock()
	if lock := h.metaLocks[project]; lock != nil {
		return lock
	}
	lock := &sync.Mutex{}
	h.metaLocks[project] = lock
	return lock
}

func (h *StorHub) QueueAtimeUpdateContext(_ context.Context, project, targetPath string, isDir bool, now time.Time) {
	if project == "" {
		return
	}
	key := atimeKey{path: targetPath, dir: isDir}
	h.atimeMu.Lock()
	if h.atimePending[project] == nil {
		h.atimePending[project] = make(map[atimeKey]atimeUpdate)
	}
	queued := h.atimePending[project][key]
	if queued.at.Before(now) {
		h.atimePending[project][key] = atimeUpdate{path: targetPath, dir: isDir, at: now.UTC()}
	}
	if h.atimeScheduled[project] {
		h.atimeMu.Unlock()
		return
	}
	h.atimeScheduled[project] = true
	h.atimeMu.Unlock()
	go h.flushQueuedAtime(project)
}

func (h *StorHub) flushQueuedAtime(project string) {
	_ = h.config.Sleep(context.Background(), atimeFlushDelay)

	h.atimeMu.Lock()
	pendingMap := h.atimePending[project]
	if len(pendingMap) == 0 {
		h.atimeScheduled[project] = false
		h.atimeMu.Unlock()
		return
	}
	updates := make([]atimeUpdate, 0, len(pendingMap))
	for _, update := range pendingMap {
		updates = append(updates, update)
	}
	delete(h.atimePending, project)
	h.atimeScheduled[project] = false
	h.atimeMu.Unlock()

	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "flush atime batch start", "entries", len(updates))
	_, err := h.UpdateRepoMetadataContext(context.Background(), project, func(repo *meta.RepoMetadata) error {
		for _, update := range updates {
			if update.dir {
				if update.path == "" {
					if shfs.ShouldUpdateAtime(h.config.AtimePolicy, repo.Root.AccessedAt, repo.Root.ModifiedAt, repo.Root.ChangedAt, update.at) {
						repo.Root.AccessedAt = update.at
					}
					continue
				}
				dir := repo.GetDirectory(update.path)
				if dir == nil {
					continue
				}
				if shfs.ShouldUpdateAtime(h.config.AtimePolicy, dir.AccessedAt, dir.ModifiedAt, dir.ChangedAt, update.at) {
					dir.AccessedAt = update.at
				}
				continue
			}
			file := repo.FindFile(update.path)
			if file == nil {
				continue
			}
			if shfs.ShouldUpdateAtime(h.config.AtimePolicy, file.AccessedAt, file.ModifiedAt, file.ChangedAt, update.at) {
				file.AccessedAt = update.at
			}
		}
		return nil
	}, fmt.Sprintf("storhub: touch atime batch (%d entries)", len(updates)))
	if err != nil {
		logging.Warn(h.projectLogger(project), "flush atime batch failed", "entries", len(updates), "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		h.requeueAtime(project, updates)
		return
	}
	logging.Debug(h.projectLogger(project), "flush atime batch complete", "entries", len(updates), "elapsed", h.config.Now().UTC().Sub(started))
}

func (h *StorHub) requeueAtime(project string, updates []atimeUpdate) {
	h.atimeMu.Lock()
	if h.atimePending[project] == nil {
		h.atimePending[project] = make(map[atimeKey]atimeUpdate)
	}
	for _, update := range updates {
		key := atimeKey{path: update.path, dir: update.dir}
		queued := h.atimePending[project][key]
		if queued.at.Before(update.at) {
			h.atimePending[project][key] = update
		}
	}
	if h.atimeScheduled[project] {
		h.atimeMu.Unlock()
		return
	}
	h.atimeScheduled[project] = true
	h.atimeMu.Unlock()
	go h.flushQueuedAtime(project)
}
