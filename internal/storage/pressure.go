package storage

import "sync"

// Pressure counters (item A1): a hub-level registry of commit
// pipeline pressure events for operators and degraded-mode
// policy. All counters are monotonic except the per-project
// consecutive-failure streak, which resets on success.
//
// Design notes for Wave-2 consumers (A4/A5/A10):
//   - A4 reads streaks via PressureFailureStreak / Snapshot.
//   - A5 reads queue depth via PressurePendingDepth (live stack
//     length, not a counter: depth falls on every commit).
//   - A10 adds orphan/sprawl counters here: one uint64 field,
//     one note* method, one Snapshot line each. The extension
//     pattern is one monotonic counter per event class, all
//     guarded by the same mutex.
//
// Cost: increments take mu only and allocate nothing on the
// hot path. The streak map is touched on the commit result
// path alone (cold): failure may allocate one map entry per
// previously unseen project; success deletes the entry so the
// map cannot grow past the tracked-project set.
type pressureRegistry struct {
	mu sync.Mutex
	// Monotonic event totals.
	capCrosses      uint64
	forceRetryPokes uint64
	commitSuccesses uint64
	commitFailures  uint64
	rebases         uint64
	// Item A10 chunk-GC totals. Scans/Collected/Reclaimed are
	// monotonic event counters; OrphanLast* are gauges holding the
	// most recent scan result (set, not incremented) so operators
	// can derive the orphan rate (last/scanned) without a probe.
	// WHY gauges plus counters: a monotonic sum over scans cannot
	// answer "how many orphans right now", and a gauge alone cannot
	// answer "how much did we ever reclaim". Both live under mu.
	chunkGCScans          uint64
	orphanChunksLast      uint64
	orphanBytesLast       uint64
	orphanChunksCollected uint64
	orphanBytesReclaimed  uint64
	// Consecutive commit failures per project: incremented on
	// failure, deleted on success. Absent means zero.
	streaks map[string]uint64
}

// PressureSnapshot is the operator/test copy of the registry.
// FailureStreaks maps project to its current consecutive
// failure count; the map is always non-nil and owned by the
// caller, so mutating it cannot affect the registry.
type PressureSnapshot struct {
	CapCrosses      uint64
	ForceRetryPokes uint64
	CommitSuccesses uint64
	CommitFailures  uint64
	Rebases         uint64
	FailureStreaks  map[string]uint64
	// Extension point (A10): orphan/sprawl totals go here as
	// new uint64 fields with matching note* methods below.
	ChunkGCScans          uint64
	OrphanChunksLast      uint64
	OrphanBytesLast       uint64
	OrphanChunksCollected uint64
	OrphanBytesReclaimed  uint64
}

// noteCapCross records one residency-bound crossing: the append
// that hits the op-count cap or crosses the byte cap.
func (p *pressureRegistry) noteCapCross() {
	p.mu.Lock()
	p.capCrosses++
	p.mu.Unlock()
}

// noteForceRetry records one force-retry poke: a crossing
// mutation (or the sweep backstop) waking the commit loop
// because the stack outgrew its residency bound.
func (p *pressureRegistry) noteForceRetry() {
	p.mu.Lock()
	p.forceRetryPokes++
	p.mu.Unlock()
}

// noteCommitSuccess records one committed project and clears
// its failure streak: a success breaks the consecutive run.
func (p *pressureRegistry) noteCommitSuccess(project string) {
	p.mu.Lock()
	p.commitSuccesses++
	if p.streaks != nil {
		delete(p.streaks, project)
	}
	p.mu.Unlock()
}

// noteCommitFailure records one failed commit and extends the
// project's consecutive-failure streak.
func (p *pressureRegistry) noteCommitFailure(project string) {
	p.mu.Lock()
	p.commitFailures++
	if p.streaks == nil {
		p.streaks = make(map[string]uint64)
	}
	p.streaks[project]++
	p.mu.Unlock()
}

// noteRebase records one successful rebase-on-conflict replay
// inside the commit/rebase cycle.
func (p *pressureRegistry) noteRebase() {
	p.mu.Lock()
	p.rebases++
	p.mu.Unlock()
}

// noteChunkGCScan records one completed orphan scan (read-only probe
// or the classification phase of a compaction).
func (p *pressureRegistry) noteChunkGCScan() {
	p.mu.Lock()
	p.chunkGCScans++
	p.mu.Unlock()
}

// noteOrphanSnapshot sets the last-scan gauges: how many orphan chunk
// records the most recent scan found and how many unreachable bytes
// they pin. A set, not an increment: the gauge answers "how much
// sprawl right now" while ChunkGCScans answers "how many probes ran".
func (p *pressureRegistry) noteOrphanSnapshot(orphans, bytes uint64) {
	p.mu.Lock()
	p.orphanChunksLast = orphans
	p.orphanBytesLast = bytes
	p.mu.Unlock()
}

// noteChunkGCCollected records one compaction's reclaimed records:
// monotonic totals of collected chunk IDs and unreachable bytes freed
// from the catalog (remote assets are never touched here; purge owns
// those).
func (p *pressureRegistry) noteChunkGCCollected(chunks, bytes uint64) {
	p.mu.Lock()
	p.orphanChunksCollected += chunks
	p.orphanBytesReclaimed += bytes
	p.mu.Unlock()
}

// snapshot returns a caller-owned copy of the registry.
func (p *pressureRegistry) snapshot() PressureSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := PressureSnapshot{
		CapCrosses:            p.capCrosses,
		ForceRetryPokes:       p.forceRetryPokes,
		CommitSuccesses:       p.commitSuccesses,
		CommitFailures:        p.commitFailures,
		Rebases:               p.rebases,
		ChunkGCScans:          p.chunkGCScans,
		OrphanChunksLast:      p.orphanChunksLast,
		OrphanBytesLast:       p.orphanBytesLast,
		OrphanChunksCollected: p.orphanChunksCollected,
		OrphanBytesReclaimed:  p.orphanBytesReclaimed,
		FailureStreaks:        make(map[string]uint64, len(p.streaks)),
	}
	for name, n := range p.streaks {
		out.FailureStreaks[name] = n
	}
	return out
}

// streak reports one project's consecutive-failure count.
func (p *pressureRegistry) streak(project string) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.streaks[project]
}

// PressureSnapshot returns a caller-owned copy of the hub's
// pressure counters for operators and tests.
func (h *StorHub) PressureSnapshot() PressureSnapshot {
	return h.pressure.snapshot()
}

// PressureFailureStreak reports one project's consecutive
// commit-failure count (zero when absent or after a success).
func (h *StorHub) PressureFailureStreak(project string) uint64 {
	return h.pressure.streak(project)
}

// PressurePendingDepth reports the live pending-op depth for a
// project: the number of ops awaiting commit. Unknown or
// evicted projects read as zero.
func (h *StorHub) PressurePendingDepth(project string) int {
	h.metaMu.RLock()
	pm, ok := h.metaCache[project]
	h.metaMu.RUnlock()
	if !ok {
		return 0
	}
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.opStack.ops)
}
