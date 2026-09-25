package storage

// opStack accumulates pending metadata operations with per-path coalescing:
// fifty writes of one file collapse into one op carrying the final state
// and a times counter, keeping stacks (and commit messages) proportional to
// what changed, not to how many syscalls produced it.
//
// Coalescing lookups are indexed (path -> stack position) so bulk imports
// of many distinct paths stay O(1) per append; a coalesce rebuilds the op
// at the END of the stack, preserving replay-order semantics when renames
// intervene. Rename chains and rename-then-delete collapse only when
// adjacent, for the same reason.
//
// The stack is bounded twice: maxPendingOpsPerProject caps the op count and
// opStackMaxBytes caps the serialized weight (each op carries full state
// plus chunk catalog records, so a few ops on huge files can outweigh
// thousands of tiny ones). Either bound crossing compacts the journal and
// force-retries the commit: acknowledged ops are never dropped.
type opStack struct {
	ops      []Op
	seq      uint64
	byPath   map[string]int // stackPathKey(op) -> index (kind-prefixed, never raw)
	byTarget map[string]int // Paths[1] of rename ops -> index
	pruneIdx int            // index of the merged chunkprune op, -1 when none
	// bytes is the running approxOpBytes total over ops, maintained
	// incrementally by every mutation below (append/removeAt/clear/clearUpTo/
	// reindex) so the byte cap needs no O(stack) recount.
	bytes int64
	// gen is the open generation. A commit snapshot freezes the stack:
	// every op appended after the freeze lands in a strictly newer
	// generation and never merges with a frozen op, so the fold can
	// never span a commit boundary by construction. Zero means
	// uninitialized (a fresh stack opens generation 1 on first append);
	// 0 is also what pre-generation journal lines decode to, which is
	// exactly the legacy behavior (those lines merge freely).
	gen uint64
}

// opStackMaxBytes bounds one project's pending op stack by weight (64MiB),
// complementing the maxPendingOpsPerProject count bound. Crossing it never
// drops acknowledged work (drop-never): the journal is compacted to the
// folded survivors and the commit is force-retried until it drains, with a
// warn log (no fail-loud backpressure).
const opStackMaxBytes = 64 << 20

// approxOpBytes estimates one op's memory/journal weight for cap accounting.
// It does not need to match the JSON encoding byte-for-byte; it must be
// monotone in payload size and cheap (no marshaling on the hot path).
func approxOpBytes(op Op) int {
	n := 200 // envelope: seq, type, cause, timestamps, JSON framing
	for _, p := range op.Paths {
		n += len(p)
	}
	n += len(op.Cause) + len(op.Tag) + len(op.XAttr)
	if op.File != nil {
		n += 160 + len(op.File.Chunks)*8 + len(op.File.Symlink)
		for k, v := range op.File.XAttrs {
			n += len(k) + len(v)
		}
	}
	if op.Dir != nil {
		n += 120
		for k, v := range op.Dir.XAttrs {
			n += len(k) + len(v)
		}
	}
	n += len(op.Chunks) * 72 // id plus ChunkInfo record
	n += len(op.RemovedChunks) * 8
	for _, m := range op.Members {
		n += len(m)
	}
	if op.Release != nil {
		n += 32
	}
	return n
}

// needsForceFlush reports whether the stack crossed a residency bound (op
// count or bytes). The append site pokes the commit trigger when true; ops
// are never dropped for crossing.
//
// Contract: the force-flush condition is count-or-bytes
// (`len(s.ops) >= maxPendingOpsPerProject || s.bytes >= opStackMaxBytes`,
// still evaluated under pm.mu), shared by the append-site poke and the
// sweepCachesOnce retry leg. Everything else stays as-is.
func (s *opStack) needsForceFlush() bool {
	return len(s.ops) >= maxPendingOpsPerProject || s.bytes >= opStackMaxBytes
}

// stackPathKey returns the byPath lookup key for an op that participates in
// per-path coalescing. Keys are kind-prefixed so namespaces sharing the raw
// string space can never collide: file paths ("f:"), directory paths ("d:",
// including the root path ""), and release tags ("r:"): a file put of "v1"
// and a release tag "v1" previously overwrote each other's index entry and
// missed coalescing. Rename, chunkprune and unknown ops are not
// byPath-indexed (ok=false); renames live in byTarget, prunes in pruneIdx.
func stackPathKey(op Op) (key string, ok bool) {
	switch op.Type {
	case OpRelease:
		return "r:" + op.Tag, true
	case OpDeleteFile:
		if len(op.Paths) > 0 {
			return "f:" + op.Paths[0], true
		}
	case OpRmdir:
		if len(op.Paths) > 0 {
			if op.Dir != nil {
				return "d:" + op.Paths[0], true
			}
			return "", false
		}
	case OpPutFile, OpTruncate, OpPatch:
		if len(op.Paths) > 0 {
			return "f:" + op.Paths[0], true
		}
	case OpMkdir:
		if len(op.Paths) > 0 {
			if op.Dir != nil {
				return "d:" + op.Paths[0], true
			}
			return "", false
		}
	case OpSetattr, OpXattr:
		if len(op.Paths) > 0 {
			if op.File != nil {
				return "f:" + op.Paths[0], true
			}
			if op.Dir != nil {
				return "d:" + op.Paths[0], true
			}
			return "", false
		}
	}
	return "", false
}

func (s *opStack) initIndex() {
	if s.byPath == nil {
		s.byPath = make(map[string]int)
		s.byTarget = make(map[string]int)
		s.pruneIdx = -1
	}
}

// indexOp records op's position under its lookup keys.
func (s *opStack) indexOp(op Op, idx int) {
	switch op.Type {
	case OpRename:
		if len(op.Paths) == 2 {
			s.byTarget[op.Paths[1]] = idx
		}
	case OpChunkPrune:
		s.pruneIdx = idx
	default:
		if key, ok := stackPathKey(op); ok {
			s.byPath[key] = idx
		}
	}
}

// removeAt drops the op at idx and shifts the indices of everything after
// it. O(n) but only reached on coalesce/transform, which bulk imports never
// hit. The byte total drops with the op.
func (s *opStack) removeAt(idx int) {
	s.bytes -= int64(approxOpBytes(s.ops[idx]))
	s.ops = append(s.ops[:idx], s.ops[idx+1:]...)
	for path, i := range s.byPath {
		if i > idx {
			s.byPath[path] = i - 1
		}
	}
	for path, i := range s.byTarget {
		if i > idx {
			s.byTarget[path] = i - 1
		}
	}
	if s.pruneIdx > idx {
		s.pruneIdx--
	} else if s.pruneIdx == idx {
		s.pruneIdx = -1
	}
}

// append folds op into the stack. Coalescing rules:
//
//	state + state (same path)   -> latest state wins, times accumulate
//	state + delete (same path)  -> delete wins (put-then-delete nets to delete)
//	delete + state (same path)  -> state wins (recreate)
//	delete + delete             -> one delete
//	rename A->B + rename B->C   -> rename A->C (adjacent only)
//	rename A->B + delete B      -> delete A (adjacent only)
//	prune + prune               -> merged ID set
//
// Cross-class combinations that cannot collapse (rename then put on the
// target, put then rename of the source) are kept as-is: replay applies
// them in order and each op is a full-state assertion, so composition stays
// correct.
func (s *opStack) append(op Op) {
	s.seq++
	op.Seq = s.seq
	if op.Times <= 0 {
		op.Times = 1
	}
	s.initIndex()
	// Generation stamping: a fresh stack opens generation 1 (0 is the
	// legacy-journal decoding, never a live generation). An op that
	// already carries a newer generation (journal refold, crash-replay
	// hydrate) fast-forwards the open generation first, so its stamp
	// survives and the merge rule below reproduces the fold exactly;
	// ordinary mutations carry Gen 0 and take the open generation.
	// Either way the stamped op satisfies op.Gen == s.gen afterwards,
	// and every stacked op satisfies Gen <= s.gen: the merge rule
	// (candidate.Gen == s.gen) is exactly "same open generation".
	if s.gen == 0 {
		s.gen = 1
	}
	if op.Gen > s.gen {
		s.gen = op.Gen
	}
	op.Gen = s.gen
	switch {
	case isDeleteClass(op.Type):
		// rename A->B followed by delete B nets to delete A; the transform
		// can chain (rename A->B, rename B->C, delete C), hence the loop.
		for s.deleteTransform(&op) {
		}
		if key, ok := stackPathKey(op); ok {
			if idx, found := s.byPath[key]; found {
				existing := s.ops[idx]
				if isStateClass(existing.Type) || isDeleteClass(existing.Type) {
					op.Times += existing.Times
					s.removeAt(idx)
					s.ops = append(s.ops, op)
					s.bytes += int64(approxOpBytes(op))
					s.indexOp(op, len(s.ops)-1)
					return
				}
			}
		}
		s.ops = append(s.ops, op)
		s.bytes += int64(approxOpBytes(op))
		s.indexOp(op, len(s.ops)-1)
	case isStateClass(op.Type):
		if key, ok := stackPathKey(op); ok {
			if idx, found := s.byPath[key]; found {
				existing := s.ops[idx]
				if isStateClass(existing.Type) || isDeleteClass(existing.Type) {
					op.Times += existing.Times
					s.removeAt(idx)
					s.ops = append(s.ops, op)
					s.bytes += int64(approxOpBytes(op))
					s.indexOp(op, len(s.ops)-1)
					return
				}
			}
		}
		s.ops = append(s.ops, op)
		s.bytes += int64(approxOpBytes(op))
		s.indexOp(op, len(s.ops)-1)
	case op.Type == OpRename:
		from, to := op.Paths[0], op.Paths[1]
		if idx, ok := s.byTarget[from]; ok && idx == len(s.ops)-1 && s.ops[idx].Type == OpRename && s.ops[idx].Gen == s.gen {
			// Adjacent chain: A->B then B->C collapses to A->C carrying
			// the latest entry state. Non-adjacent chains stay split: an
			// intervening op on B or C would change the net effect. A
			// chain whose predecessor is in a frozen generation also
			// stays split: the in-flight commit publishes A->B, so
			// rewriting it to A->C would leave B as a phantom once the
			// next rebase replays A->C. The structure excludes the
			// interleaving the old snapshot mark policed: appends after
			// a freeze land in a newer generation, so a cross-boundary
			// pair can never satisfy the equality above.
			s.bytes -= int64(approxOpBytes(s.ops[idx]))
			existing := s.ops[idx]
			delete(s.byTarget, existing.Paths[1])
			// Replace the slice, never write through it: snapshot()
			// shallow-copies the stack, so an in-flight commit may still
			// be reading this op's Paths backing array.
			existing.Paths = []string{existing.Paths[0], to}
			existing.File, existing.Dir = op.File, op.Dir
			existing.Chunks = op.Chunks
			existing.Timestamp = op.Timestamp
			existing.Times += op.Times
			existing.Seq = op.Seq
			s.ops[idx] = existing
			s.bytes += int64(approxOpBytes(existing))
			s.byTarget[to] = idx
			return
		}
		s.ops = append(s.ops, op)
		s.bytes += int64(approxOpBytes(op))
		s.indexOp(op, len(s.ops)-1)
	case op.Type == OpChunkPrune:
		if s.pruneIdx >= 0 {
			s.bytes -= int64(approxOpBytes(s.ops[s.pruneIdx]))
			existing := s.ops[s.pruneIdx]
			existing.RemovedChunks = append(existing.RemovedChunks, op.RemovedChunks...)
			existing.Timestamp = op.Timestamp
			existing.Times += op.Times
			existing.Seq = op.Seq
			s.ops[s.pruneIdx] = existing
			s.bytes += int64(approxOpBytes(existing))
			return
		}
		s.ops = append(s.ops, op)
		s.bytes += int64(approxOpBytes(op))
		s.indexOp(op, len(s.ops)-1)
	case op.Type == OpRelease:
		if key, ok := stackPathKey(op); ok {
			if idx, found := s.byPath[key]; found && s.ops[idx].Type == OpRelease {
				op.Times += s.ops[idx].Times
				s.removeAt(idx)
			}
		}
		s.ops = append(s.ops, op)
		s.bytes += int64(approxOpBytes(op))
		s.indexOp(op, len(s.ops)-1)
	default:
		s.ops = append(s.ops, op)
		s.bytes += int64(approxOpBytes(op))
	}
}

// appendWithDelta folds op into the stack and returns the pre-coalescing
// delta for the crash-recovery journal: the op exactly as appended (Times
// normalized to 1), BEFORE coalescing rewrote it. The journal stores deltas,
// never post-coalescing tails, so foldOps over the journal lines replays the
// identical append sequence and converges to exactly the live stack,
// including cross-transaction rename chains (T1 A->B, T2 B->C) and
// rename-then-delete, which folded tails cannot reproduce (a journaled tail
// A->C misses the byTarget chain check; a journaled del A misses the
// deleteTransform). Times accumulates during the fold from per-delta Times=1
// lines, so the folded total stays exact.
func (s *opStack) appendWithDelta(op Op) Op {
	if op.Times <= 0 {
		op.Times = 1
	}
	delta := op
	delta.Times = 1
	// The delta carries the open generation at append time, so the
	// journal fold replays this op's merge decisions exactly as the
	// live stack made them: same-generation pairs merge, frozen pairs
	// stay split. The old snapshot mark (SnapSeq) is no longer stamped:
	// journal lines shrink by the `snap` key and recovery needs no marks.
	s.append(op)
	delta.Seq = s.seq
	delta.Gen = s.gen
	return delta
}

// deleteTransform applies the rename-then-delete collapse for a delete-class
// op whose target path is a pending adjacent rename target. Returns true
// when the op was rewritten (caller re-runs its matching). A rename in a
// frozen generation must not be consumed (same boundary as the chain
// merge): the commit publishes A->B, so the delete has to survive as
// "delete B" for the next replay, not collapse to "delete A". The
// structure excludes the old mark-check interleaving: only the open
// generation satisfies Gen == s.gen, so a frozen rename can never match.
func (s *opStack) deleteTransform(op *Op) bool {
	path := opPath(*op)
	idx, ok := s.byTarget[path]
	if !ok || idx != len(s.ops)-1 || s.ops[idx].Type != OpRename {
		return false
	}
	if s.ops[idx].Gen != s.gen {
		return false
	}
	op.Times += s.ops[idx].Times
	op.Paths = []string{s.ops[idx].Paths[0]}
	s.removeAt(idx)
	return true
}

// clearUpTo drops every op whose last append happened at or before seq.
// Appends always stamp op.Seq with the newest sequence number, so an op
// coalesced by a mutation that landed mid-commit carries a seq above the
// snapshot and survives - exactly the ops the next commit must include.
// (Those survivors also carry a newer generation than the frozen batch,
// so the generation boundary agrees with the seq cutoff by construction;
// the seq comparison stays because drain targets and resolutions number
// by seq, not by generation.)
func (s *opStack) clearUpTo(seq uint64) {
	kept := make([]Op, 0, len(s.ops))
	var keptBytes int64
	for _, op := range s.ops {
		if op.Seq > seq {
			kept = append(kept, op)
			keptBytes += int64(approxOpBytes(op))
		}
	}
	s.ops = kept
	s.bytes = keptBytes
	s.reindex()
}

func (s *opStack) clear() {
	s.ops = nil
	s.byPath = nil
	s.byTarget = nil
	s.pruneIdx = -1
	s.bytes = 0
	// Reset the open generation: the stack is empty, so no merge partner
	// survives, and a later hydrate (crash-replay appends folded journal
	// ops) must fast-forward from the JOURNAL's generations, not restamp
	// them upward into a stale open generation: restamping would merge
	// chains the fold kept split and break fold==live after recovery.
	s.gen = 0
}

// reindex rebuilds the lookup maps from scratch after bulk mutation. The
// byte total is recomputed too, so any accounting drift self-heals here.
func (s *opStack) reindex() {
	s.byPath = make(map[string]int, len(s.ops))
	s.byTarget = make(map[string]int)
	s.pruneIdx = -1
	var total int64
	for i, op := range s.ops {
		s.indexOp(op, i)
		total += int64(approxOpBytes(op))
	}
	s.bytes = total
}

func (s *opStack) snapshot() []Op {
	if len(s.ops) == 0 {
		return nil
	}
	out := make([]Op, len(s.ops))
	copy(out, s.ops)
	// Deep-copy every reference-backed field: the live stack keeps
	// mutating (rename coalescing, prune merging, collision remaps), and
	// an in-flight commit reads this snapshot outside pm.mu. Sharing
	// backing arrays or maps is a data race and can rewrite the committed
	// message/rebase mid-flight. Payloads copy through cloneOpPayloads
	// (the same copier replay uses); the remaining slice and pointer
	// fields copy here.
	for i := range out {
		out[i] = cloneOpPayloads(out[i])
		if out[i].Paths != nil {
			out[i].Paths = append([]string(nil), out[i].Paths...)
		}
		if out[i].RemovedChunks != nil {
			out[i].RemovedChunks = append([]int64(nil), out[i].RemovedChunks...)
		}
		if out[i].Members != nil {
			out[i].Members = append([]string(nil), out[i].Members...)
		}
		if out[i].Release != nil {
			r := *out[i].Release
			out[i].Release = &r
		}
	}
	return out
}

// freeze seals the open generation for one commit: it returns the pending
// ops (the exact publish batch, a copy the commit owns) plus the frozen
// generation, and opens a strictly newer generation for later appends.
// The frozen batch is immutable from here on; the commit publishes
// exactly it, and clearUpTo drops exactly it on success.
//
// A failed publish needs no rollback call: there is no mark to restore.
// The sealed ops keep their generation, post-freeze appends landed in a
// newer one, and the merge rule never spans generations: so the next
// freeze simply seals every still-pending generation at once, and both
// the live stack and any refold agree on the split. This is the property
// the old noteSnapshot/rollbackSnapshot pair maintained by hand:
// rollbackSnapshot existed because a stale mark poisoned later merges
// (live split vs fold merged); with the boundary in the structure, a
// failed snapshot leaves nothing behind that a mark-restore could fix.
func (s *opStack) freeze() ([]Op, uint64) {
	batch := s.snapshot()
	frozen := s.gen
	if s.gen == 0 {
		s.gen = 1
		frozen = 1
	}
	s.gen++
	return batch, frozen
}

func (s *opStack) maxSeq() uint64 { return s.seq }

// The fold contract lives on foldOps in ops_replay.go; this note stays a
// pointer so the two can never drift.
