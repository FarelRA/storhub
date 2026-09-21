package storage

// commitSnapshot captures the immutable inputs one commit-attempt series
// works from: a private working copy, the op batch the message describes,
// the CAS token, the admission version, and the rebase baseline. Taken under
// pm.mu; the commit then runs without holding it.
type commitSnapshot struct {
	working     *RepoMetadata
	previousSHA string
	version     uint64
	ops         []Op
	opSeq       uint64
	// snapGen is the frozen generation this snapshot publishes: the commit
	// owns every pending op at or below it, and clearUpTo drops exactly
	// those on success. No prev-mark is kept: a failed publish restores
	// nothing, because the generation boundary needs no restoring (see
	// opStack.freeze).
	snapGen     uint64
	baseTree    *RepoMetadata
	objectCount uint64
	headSplit   bool
	now         int64
}

// snapshotCommitState snapshots the dirty state for one commit: a private
// working copy plus the op batch the message describes. The freeze seals
// the open generation into the batch and opens a newer one, so mutations
// landing mid-commit neither join the batch nor merge into it; recording
// the snapshot seq lets the success path drop exactly the published ops.
// Returns nil when clean (nothing to do). Exits with pm.mu released on
// every path.
func (h *StorHub) snapshotCommitState(_ string, pm *projectMetadata) *commitSnapshot {
	pm.mu.Lock()
	if !pm.dirty {
		pm.mu.Unlock()
		return nil
	}
	// Normalize a private copy, never the shared tree. A failed commit
	// (size ceiling, push error) must leave pm.meta exactly as the
	// mutations left it; the normalized working copy is applied back only
	// on success below. cowTree is a shallow copy over immutable entries,
	// so the commit no longer deep-copies every Chunks/XAttrs.
	working := cowTree(pm.meta)
	// Defensive backfill for the same nil-baseline case: a commit can
	// snapshot dirty state whose mutations predated the appendOpLocked
	// heal above (or arrived via a path that bypassed it). The snapshot
	// carries the baseline the rebase fingerprints on conflict, so a nil
	// baseline here would degrade the first conflict exactly as before.
	if pm.baseTree == nil && pm.meta != nil {
		pm.baseTree = pm.meta
	}
	snap := &commitSnapshot{
		working:     working,
		previousSHA: pm.sha,
		version:     pm.version,
		opSeq:       pm.opStack.maxSeq(),
		baseTree:    pm.baseTree,
		objectCount: pm.objectCount,
		headSplit:   working.IsSplit(),
		now:         h.config.Now().UnixNano(),
	}
	// The freeze is the generational boundary (JBD2 shape): the batch
	// below is exactly the frozen generation, and every append after
	// this point lands in a newer one. Snapshot and freeze are one
	// critical section under pm.mu, so no append can slip between them.
	snap.ops, snap.snapGen = pm.opStack.freeze()
	pm.mu.Unlock()
	return snap
}
