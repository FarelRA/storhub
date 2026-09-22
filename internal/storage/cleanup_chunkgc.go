package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/FarelRA/storhub/internal/logging"
	"log/slog"
	"sort"
)

// ChunkGCResult reports what a scan saw or a compaction did (or would
// do under DryRun). DeletedIDs is sorted ascending for stable logs.
type ChunkGCResult struct {
	DryRun           bool    `json:"dry_run"`
	ScannedChunks    int     `json:"scanned_chunks"`
	OrphanChunks     int     `json:"orphan_chunks"`
	OrphanBytes      int64   `json:"orphan_bytes"`
	CollectedChunks  int     `json:"collected_chunks"`
	CollectedBytes   int64   `json:"collected_bytes"`
	DeletedIDs       []int64 `json:"deleted_ids,omitempty"`
	RefusedBySession bool    `json:"refused_by_session,omitempty"`
}

// ChunkGCRefusedError is the loud typed refusal: compaction never
// deletes silently under doubt (live session, bad project).
type ChunkGCRefusedError struct {
	Project string
	Reason  string
}

func (e *ChunkGCRefusedError) Error() string {
	return fmt.Sprintf("chunk GC refused for project %s: %s", e.Project, e.Reason)
}

// errChunkGCNoop aborts a compaction transaction with no publish when
// classification finds zero orphans. Internal sentinel, never surfaced:
// the caller translates it into a zero-result success so a no-op run
// stays side-effect free (no dirty mark, no empty commit).
var errChunkGCNoop = errors.New("chunk GC: nothing to collect")

// chunkGCHasLiveSession reports whether any non-destroyed session
// handle names the project. Conservative: expiry is ignored, a handle
// present in the table blocks until it is closed or reaped.
func (h *StorHub) chunkGCHasLiveSession(project string) bool {
	sh := h.sessionHub()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for _, s := range sh.byID {
		if s == nil || s.destroyed {
			continue
		}
		if s.project == project {
			return true
		}
	}
	return false
}

// chunkGCRoots unions the live-tree file references with the pending-op
// references (file payloads plus catalog keys). ops is the live stack
// copy; callers holding pm.mu may pass the stack directly.
func chunkGCRoots(tree *RepoMetadata, ops []Op) map[int64]struct{} {
	roots := make(map[int64]struct{})
	for _, file := range tree.Files() {
		for _, id := range file.Chunks {
			roots[id] = struct{}{}
		}
	}
	for _, op := range ops {
		if op.File != nil {
			for _, id := range op.File.Chunks {
				roots[id] = struct{}{}
			}
		}
		for id := range op.Chunks {
			roots[id] = struct{}{}
		}
	}
	return roots
}

// chunkGCClassify splits the catalog into sorted orphan IDs plus their
// unreachable byte total. Pure function, no I/O, so tests can pin the
// keep-vs-collect decision without a hub.
func chunkGCClassify(catalog map[int64]ChunkInfo, roots map[int64]struct{}) (orphans []int64, unreachable int64) {
	for id, info := range catalog {
		if _, ok := roots[id]; ok {
			continue
		}
		orphans = append(orphans, id)
		unreachable += info.Size
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i] < orphans[j] })
	return orphans, unreachable
}

// ScanChunkGC is the phase-1 read-only probe: it classifies orphans,
// publishes the scan counter plus the last-scan gauges to the pressure
// registry, logs loudly, and mutates nothing. Safe under live
// sessions (it deletes nothing, so the session gate does not apply).
func (h *StorHub) ScanChunkGC(ctx context.Context, project string) (*ChunkGCResult, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "chunk GC scan start", "dry_run", true)
	pm := h.lookupProjectMeta(project)
	if pm == nil {
		// Untracked project: nothing cached, nothing to classify.
		// No entry is created as a side effect (read-only means
		// read-only).
		h.pressure.noteChunkGCScan()
		h.pressure.noteOrphanSnapshot(0, 0)
		return &ChunkGCResult{DryRun: true}, nil
	}
	pm.mu.RLock()
	roots := chunkGCRoots(pm.meta, append([]Op(nil), pm.opStack.ops...))
	scanned := len(pm.meta.Chunks())
	orphans, unreachable := chunkGCClassify(pm.meta.Chunks(), roots)
	pm.mu.RUnlock()

	_ = ctx
	h.pressure.noteChunkGCScan()
	h.pressure.noteOrphanSnapshot(uint64(len(orphans)), uint64(unreachable))
	logging.Debug(h.projectLogger(project), "chunk GC scan complete",
		"scanned", scanned, "orphaned", len(orphans), "reclaimed", 0, "reclaimed_bytes", int64(0), "unreachable_bytes", unreachable, "dry_run", true, "elapsed", h.config.Now().UTC().Sub(started))
	return &ChunkGCResult{
		DryRun:        true,
		ScannedChunks: scanned,
		OrphanChunks:  len(orphans),
		OrphanBytes:   unreachable,
		DeletedIDs:    append([]int64(nil), orphans...),
	}, nil
}

// CompactOrphanChunks compacts unreachable chunk catalog records.
// dryRun=true classifies and logs without deleting (same numbers the
// real run would act on, barring concurrent writers). dryRun=false
// deletes inside one metadata transaction (see the race discipline
// above) and records the collection counters. A live session for the
// project refuses loudly with *ChunkGCRefusedError and deletes
// nothing. Release assets are never touched.
func (h *StorHub) CompactOrphanChunks(ctx context.Context, project string, dryRun bool) (*ChunkGCResult, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	if h.chunkGCHasLiveSession(project) {
		logging.Warn(h.projectLogger(project), "chunk GC refused: live session holds pins", "scanned", 0, "orphaned", 0, "reclaimed", 0, "dry_run", dryRun)
		return nil, &ChunkGCRefusedError{Project: project, Reason: "live session pins chunks; close sessions and retry"}
	}
	if dryRun {
		res, err := h.ScanChunkGC(ctx, project)
		if err != nil {
			return nil, err
		}
		res.RefusedBySession = false
		logging.Debug(h.projectLogger(project), "chunk GC dry-run complete",
			"scanned", res.ScannedChunks, "orphaned", res.OrphanChunks, "reclaimed", 0, "reclaimed_bytes", int64(0), "unreachable_bytes", res.OrphanBytes, "dry_run", true)
		return res, nil
	}
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "chunk GC compact start", "dry_run", false)
	pm := h.lookupProjectMeta(project)
	if pm == nil {
		h.pressure.noteChunkGCScan()
		h.pressure.noteOrphanSnapshot(0, 0)
		return &ChunkGCResult{}, nil
	}
	// Snapshot the pending-op roots BEFORE the transaction, under a
	// read lock. WHY not read pm.opStack inside fn: the transaction
	// resolves its own pm (an idle eviction could swap the entry
	// between our lookup and the txn), and dereferencing our possibly
	// stale pm under the txn's lock would be an unlocked read of a
	// revivable stack. The snapshot pairs with the candidate inside
	// fn; appends racing between snapshot and commit are still safe:
	// every verb mints FRESH chunk IDs (never reuses an orphan ID),
	// a racing verb serialized behind this txn looks its chunks up
	// in the post-GC candidate and fails loud ("chunk not found")
	// instead of corrupting, and the synthesized OpChunkPrune replay
	// defensively skips still-referenced IDs on crash replay.
	pm.mu.RLock()
	pendingOps := append([]Op(nil), pm.opStack.ops...)
	pm.mu.RUnlock()
	// The transaction holds pm.mu exclusively across classify+delete,
	// so concurrent writers serialize around us: a writer either
	// landed before (visible in the candidate or the snapshot above)
	// or blocks until after (fresh IDs postdate the delete set).
	var deleted []int64
	var collectedBytes int64
	var scanned int
	_, err := h.UpdateRepoMetadataContext(ctx, project, func(candidate *RepoMetadata) error {
		// No session re-check in here: the table mutex and pm.mu
		// have no defined lock order (session close/sync takes the
		// table lock then enters a metadata txn), so nesting the
		// table lock under the txn-owned pm.mu risks deadlock. The
		// single pre-check plus the serialization argument in the
		// package doc is the whole gate: a handle racing the gate
		// pins from a tree read serialized against this txn, so it
		// either blocked the run up front or pins post-GC state.
		roots := chunkGCRoots(candidate, pendingOps)
		scanned = len(candidate.Chunks())
		orphans, _ := chunkGCClassify(candidate.Chunks(), roots)
		if len(orphans) == 0 {
			// Abort the transaction with no publish: a no-op
			// compaction must stay side-effect free (no dirty
			// mark, no empty commit, no journal line).
			return errChunkGCNoop
		}
		for _, id := range orphans {
			// Defensive re-check: the opStack union above was built
			// from the same critical section, but a paranoid second
			// membership test costs nothing and turns any future
			// refactor that splits classify from delete into a
			// keep-instead-of-collect mistake, never the reverse.
			if _, ok := roots[id]; ok {
				continue
			}
			info, ok := candidate.Chunks()[id]
			if !ok {
				continue
			}
			candidate.DeleteChunk(id)
			deleted = append(deleted, id)
			collectedBytes += info.Size
		}
		return nil
	}, "storhub: chunk GC compact orphans")
	if err != nil {
		if errors.Is(err, errChunkGCNoop) {
			h.pressure.noteChunkGCScan()
			h.pressure.noteOrphanSnapshot(0, 0)
			logging.Debug(h.projectLogger(project), "chunk GC compaction complete",
				"scanned", scanned, "orphaned", 0, "reclaimed", 0, "reclaimed_bytes", 0, "dry_run", false, "elapsed", h.config.Now().UTC().Sub(started))
			return &ChunkGCResult{ScannedChunks: scanned}, nil
		}
		logging.Error(h.projectLogger(project), "chunk GC compaction failed", "scanned", scanned, "dry_run", false, "elapsed", h.config.Now().UTC().Sub(started), "err", err)
		return nil, err
	}
	h.pressure.noteChunkGCScan()
	h.pressure.noteOrphanSnapshot(0, 0)
	if len(deleted) > 0 {
		h.pressure.noteChunkGCCollected(uint64(len(deleted)), uint64(collectedBytes))
	}
	// Per-object detail stays at Debug behind the level gate: an idle
	// operator reads the summary below, not one line per chunk.
	if h.logger.Enabled(context.Background(), slog.LevelDebug) {
		for _, id := range deleted {
			logging.Debug(h.projectLogger(project), "chunk GC collected orphan chunk", "chunk_id", id)
		}
	}
	logging.Debug(h.projectLogger(project), "chunk GC compaction complete",
		"scanned", scanned, "orphaned", len(deleted), "reclaimed", len(deleted), "reclaimed_bytes", collectedBytes, "dry_run", false, "elapsed", h.config.Now().UTC().Sub(started))
	return &ChunkGCResult{
		ScannedChunks:   scanned,
		OrphanChunks:    len(deleted),
		OrphanBytes:     collectedBytes,
		CollectedChunks: len(deleted),
		CollectedBytes:  collectedBytes,
		DeletedIDs:      append([]int64(nil), deleted...),
	}, nil
}
