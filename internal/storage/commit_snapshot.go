package storage

// commitSnapshot captures the immutable inputs one commit-attempt series
// works from: a private working copy, the op batch the message describes,
// the CAS token, the admission version, and the rebase baseline. Taken under
// pm.mu; the commit then runs without holding it.
//
// Identity numbering across the pipeline, five levels that must not be
// conflated:
//
//	revision: one git history entry (commit SHA), the durable past;
//	version: in-memory publish counter (pm.version), bumped on every state
//	  swap so watchers observe arrivals;
//	sha: content-addressed token (previousSHA/contentSHA), the CAS guard;
//	seq: op journal number (opStack.seq, Op.Seq), the drain and resolution
//	  numbering;
//	gen: coalescing boundary (opStack.gen, Op.Gen), the merge scope.
//
// The success path drops exactly the published ops by seq cutoff
// (clearUpTo); the generation boundary agrees with that cutoff by
// construction, so no generation field is stored here.
type commitSnapshot struct {
	working     *RepoMetadata
	previousSHA string // CAS token for the upstream layout: manifest blob sha on split, metadata blob sha on legacy, HEAD commit sha on git
	version     uint64
	ops         []Op
	opSeq       uint64
	baseTree    *RepoMetadata
	objectCount uint64
	headSplit   bool
	now         int64
	opBytes     int64
}

// freezeCommitBatch snapshots the dirty state for one commit: a private
// working copy plus the op batch the message describes. The freeze seals
// the open generation into the batch and opens a newer one, so mutations
// landing mid-commit neither join the batch nor merge into it; recording
// the snapshot seq lets the success path drop exactly the published ops.
// Returns nil when clean (nothing to do). Exits with pm.mu released on
// every path.
func (h *StorHub) freezeCommitBatch(_ string, pm *projectMetadata) *commitSnapshot {
	pm.mu.Lock()
	if !pm.dirty {
		pm.mu.Unlock()
		return nil
	}
	// Normalize a private copy, never the shared tree. A failed commit
	// (size ceiling, push error) must leave pm.meta exactly as the
	// mutations left it; the normalized working copy is applied back only
	// on success below. cloneForWrite is a shallow copy over immutable entries,
	// so the commit no longer deep-copies every Chunks/XAttrs.
	working := cloneForWrite(pm.meta)
	healBaseTreeLocked(pm)
	snap := &commitSnapshot{
		working:     working,
		previousSHA: pm.sha,
		version:     pm.version,
		opSeq:       pm.opStack.maxSeq(),
		baseTree:    pm.baseTree,
		objectCount: pm.objectCount,
		headSplit:   working.IsSplit(),
		now:         h.config.Now().UnixNano(),
		opBytes:     pm.opStack.bytes,
	}
	// The freeze is the generational boundary (JBD2 shape): the batch
	// below is exactly the frozen generation, and every append after
	// this point lands in a newer one. Snapshot and freeze are one
	// critical section under pm.mu, so no append can slip between them.
	snap.ops, _ = pm.opStack.freeze()
	pm.mu.Unlock()
	return snap
}
