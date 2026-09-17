package storage

import (
	"fmt"
	"math"
	"strings"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// OpType classifies one metadata operation in the pending op stack.
type OpType string

const (
	OpPutFile    OpType = "put"
	OpDeleteFile OpType = "del"
	OpMkdir      OpType = "mkdir"
	OpRmdir      OpType = "rmdir"
	OpRename     OpType = "rename"
	OpSetattr    OpType = "setattr"
	OpTruncate   OpType = "trunc"
	OpPatch      OpType = "patch"
	OpXattr      OpType = "xattr"
	OpRelease    OpType = "release"
	OpChunkPrune OpType = "chunk-prune"
)

// Op is one discrete, self-contained metadata operation. Every op carries
// the FULL resulting state for its path scope, never a delta: replaying the
// stack against arbitrary upstream state is what makes rebase-on-conflict
// possible, and full-state assertions make replay idempotent (a journal
// replayed after a crash that landed between commit and journal truncation
// re-applies harmlessly).
type Op struct {
	Seq       uint64   `json:"seq"`
	Type      OpType   `json:"type"`
	Paths     []string `json:"paths"` // scope: 1 path, 2 (from, to) for rename
	Cause     string   `json:"cause"` // originating operation, e.g. "upload", "mkdir"
	Timestamp int64    `json:"ts"`    // unix seconds of the latest coalesced mutation
	Times     int      `json:"times,omitempty"`

	// Full resulting state for the op scope. File/Dir is the complete
	// entry as it should exist after replay; Chunks carries the chunk
	// catalog records the entry references.
	File   *FileMeta           `json:"file,omitempty"`
	Dir    *DirMeta            `json:"dir,omitempty"`
	Chunks map[int64]ChunkInfo `json:"chunks,omitempty"`

	FreedChunks   int         `json:"freed_chunks,omitempty"`   // del: chunk records the removed entry referenced
	Tag           string      `json:"tag,omitempty"`            // release op
	Release       *ReleaseRef `json:"release,omitempty"`        // release op: nil = delete the tag
	RemovedChunks []int64     `json:"removed_chunks,omitempty"` // chunk-prune op
	XAttr         string      `json:"xattr,omitempty"`          // xattr op

	// Members carries the explicit subtree member from-paths for a
	// directory rename, recorded at fold time from the candidate tree.
	// Replay moves exactly these members (missing members are skipped),
	// so a rename never sweeps up entries created under the old path by
	// a LATER op in the same batch: both delivery orders converge to the
	// candidate. The empty list is meaningful (rename covered an empty
	// subtree: move nothing but the dir record) and round-trips through
	// JSON as [], which is why this field has NO omitempty. Nil means
	// legacy (move the live subtree); only journals written before
	// member lists existed take that path.
	Members []string `json:"members"` // dir rename only
}

// ConflictResolution records one policy decision made while replaying ops,
// so a commit message (and strict mode) can surface exactly what was
// resolved instead of silently picking winners.
type ConflictResolution struct {
	Seq  uint64
	Path string
	Note string
}

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
// force-retries the commit — acknowledged ops are never dropped.
type opStack struct {
	ops      []Op
	seq      uint64
	byPath   map[string]int // stackPathKey(op) -> index (kind-prefixed, never raw)
	byTarget map[string]int // Paths[1] of rename ops -> index
	pruneIdx int            // index of the merged chunk-prune op, -1 when none
	// bytes is the running approxOpBytes total over ops, maintained
	// incrementally by every mutation below (append/removeAt/clear/clearUpTo/
	// reindex) so the byte cap needs no O(stack) recount.
	bytes int64
	// lastSnapshotSeq is the seq stamped at the most recent commit
	// snapshot. Ops at or below it may be in flight inside that commit
	// (its rebase replays the snapshot, not the live stack), so
	// coalescing must never rewrite them.
	lastSnapshotSeq uint64
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
// count or bytes). The sweeper force-retries the commit when true; ops are
// never dropped for crossing.
//
// Contract for the sweeper owner (caches.go sweepCachesOnce, ~line 583):
// replace the count-only condition
// `forceFlush := pm.dirty && len(pm.opStack.ops) >= maxPendingOpsPerProject`
// with `forceFlush := pm.dirty && pm.opStack.needsForceFlush()` (still under
// pm.mu). Everything else in the sweeper stays as-is.
func (s *opStack) needsForceFlush() bool {
	return len(s.ops) >= maxPendingOpsPerProject || s.bytes >= opStackMaxBytes
}

// stackPathKey returns the byPath lookup key for an op that participates in
// per-path coalescing. Keys are kind-prefixed so namespaces sharing the raw
// string space can never collide: file paths ("f:"), directory paths ("d:",
// including the root path ""), and release tags ("r:") — a file put of "v1"
// and a release tag "v1" previously overwrote each other's index entry and
// missed coalescing. Rename, chunk-prune and unknown ops are not
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
			return "d:" + op.Paths[0], true
		}
	case OpPutFile, OpTruncate, OpPatch:
		if len(op.Paths) > 0 {
			return "f:" + op.Paths[0], true
		}
	case OpMkdir:
		if len(op.Paths) > 0 {
			return "d:" + op.Paths[0], true
		}
	case OpSetattr, OpXattr:
		if len(op.Paths) > 0 {
			if op.File != nil {
				return "f:" + op.Paths[0], true
			}
			return "d:" + op.Paths[0], true
		}
	}
	return "", false
}

func isStateClass(t OpType) bool {
	switch t {
	case OpPutFile, OpTruncate, OpPatch, OpSetattr, OpXattr, OpMkdir:
		return true
	}
	return false
}

func isDeleteClass(t OpType) bool {
	return t == OpDeleteFile || t == OpRmdir
}

func opPath(op Op) string {
	if len(op.Paths) > 0 {
		return op.Paths[0]
	}
	return ""
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
		if idx, ok := s.byTarget[from]; ok && idx == len(s.ops)-1 && s.ops[idx].Type == OpRename && s.ops[idx].Seq > s.lastSnapshotSeq {
			// Adjacent chain: A->B then B->C collapses to A->C carrying
			// the latest entry state. Non-adjacent chains stay split: an
			// intervening op on B or C would change the net effect. A
			// chain whose predecessor is at or below the last commit
			// snapshot also stays split: the in-flight commit
			// publishes A->B, so rewriting it to A->C would leave B as a
			// phantom once the next rebase replays A->C.
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
// identical append sequence and converges to exactly the live stack —
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
	s.append(op)
	delta.Seq = s.seq
	return delta
}

// deleteTransform applies the rename-then-delete collapse for a delete-class
// op whose target path is a pending adjacent rename target. Returns true
// when the op was rewritten (caller re-runs its matching). A rename already
// inside the in-flight commit snapshot must not be consumed (same
// boundary as the chain merge): the commit publishes A->B, so the delete has
// to survive as "delete B" for the next replay, not collapse to "delete A".
func (s *opStack) deleteTransform(op *Op) bool {
	path := opPath(*op)
	idx, ok := s.byTarget[path]
	if !ok || idx != len(s.ops)-1 || s.ops[idx].Type != OpRename {
		return false
	}
	if s.ops[idx].Seq <= s.lastSnapshotSeq {
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
	// Deep-copy the slice-backed fields: the live stack keeps mutating
	// (rename coalescing, prune merging), and an in-flight commit reads
	// this snapshot outside pm.mu. Sharing backing arrays is a data race
	// and can rewrite the committed message/rebase mid-flight.
	for i := range out {
		if out[i].Paths != nil {
			out[i].Paths = append([]string(nil), out[i].Paths...)
		}
		if out[i].RemovedChunks != nil {
			out[i].RemovedChunks = append([]int64(nil), out[i].RemovedChunks...)
		}
	}
	return out
}

// noteSnapshot records that everything up to seq may now be in flight in a
// commit; coalescing must not rewrite those ops.
func (s *opStack) noteSnapshot(seq uint64) {
	if seq > s.lastSnapshotSeq {
		s.lastSnapshotSeq = seq
	}
}

func (s *opStack) maxSeq() uint64 { return s.seq }

// foldOps coalesces a raw op sequence (journal DELTA lines, one per
// appendWithDelta) through the same rules as live appends, so a replayed
// journal yields exactly the stack the crashed process held. The rules are
// deliberately identical to live appends — no cross-line rewrites live here:
// convergence comes from replaying the identical append sequence, not from
// fold-specific chain/delete handling. Journal lines carry Times=1 (see
// journalAppend/journalRead); the fold accumulates Times exactly as live
// coalescing does.
func foldOps(ops []Op) []Op {
	stack := &opStack{}
	for _, op := range ops {
		stack.append(op)
	}
	return stack.ops
}

// applyOps replays ops onto meta in order. Ops are full-state assertions,
// so replay is defensive by construction: deletes of missing entries are
// no-ops, and an rmdir of a directory that gained upstream children is
// skipped (data preservation) with a recorded resolution.
func applyOps(meta *RepoMetadata, ops []Op) error {
	return applyOpsWithResolutions(meta, ops, nil)
}

// replayPlan makes a batch of ops apply order-independently. The fold
// emits ops in map order, so emission carries no meaning and replay must
// converge to the same final tree in ANY sequence. Two pieces of batch
// state make that true:
//
//   - doomed: every path the batch removes - OpDeleteFile and OpRmdir
//     paths plus OpRename from-paths. OpRmdir skips only when children
//     exist OUTSIDE this set, which preserves the upstream-children
//     guarantee (data preservation) for intra-batch deletes replayed in
//     any order.
//
//   - moved: subtree prefixes relocated by dir renames applied so far in
//     this batch (recorded-from prefix -> current prefix). A later op
//     that references a path under a moved prefix resolves it to the live
//     location; a path that still exists literally wins (a same-path
//     recreation is real state, not a stale reference). Only references
//     to PRE-EXISTING state are resolved (rename-from, delete, rmdir,
//     setattr/patch/truncate targets); creates, mkdirs, rename-to paths
//     and EnsureDirectory parents stay literal - a literal new path is
//     real, never stale.
type replayPlan struct {
	doomed map[string]struct{}
	moved  map[string]string
	// mkdirs records every path an OpMkdir in this batch asserts. A dir
	// rename never removes a source record owned by the set: the mkdir
	// carries the candidate's record and lands in every delivery order,
	// so removing the source under it would fork the record identity by
	// order (kept setup record vs recreated fresh one). Keeping lets the
	// mkdir converge all orders onto the candidate record.
	mkdirs map[string]struct{}
	// targets records every path this batch asserts a record for:
	// rename-to paths, file-state op paths, and mkdir paths carrying a
	// record. Collision remapping exempts occupants at these paths: the
	// batch overwrites them with its own records in every delivery
	// order, so a live occupant is replay scaffolding (e.g. an
	// ensureParentFor mint for a not-yet-replayed rename target), not a
	// divergent upstream allocation. Genuine upstream occupants live at
	// paths no batch op asserts and still remap. Rebase drops skipped
	// ops from the set (untarget) so a skipped overwrite restores the
	// occupant's standing.
	targets map[string]struct{}
	// claimedInodes/claimedChunks hold every identifier the batch's op
	// payloads assert (file/dir inodes, chunk catalog ids and references).
	// Collision remapping mints around them: a remap that reissues an
	// inode another batch op carries would trade one collision for
	// another (the mkdir-remapped-onto-the-file's-inode shape). The
	// counter only moves forward and the claimed set is finite, so the
	// avoidance loop always terminates. Skipped ops keep their claims
	// (conservative: skipping a few counter values is always safe).
	claimedInodes map[uint64]struct{}
	claimedChunks map[int64]struct{}
	// liveCache memoizes the forwarded doomed set for the OpRmdir
	// children check. Rebuilding it per rmdir is O(rmdirs x doomed);
	// the set only changes when unremove/recordMove mutate batch state,
	// which clear liveValid for a lazy recompute.
	liveCache map[string]struct{}
	liveValid bool
}

func newReplayPlan(ops []Op) *replayPlan {
	p := &replayPlan{doomed: make(map[string]struct{}), moved: make(map[string]string), mkdirs: make(map[string]struct{}), targets: make(map[string]struct{}), claimedInodes: make(map[uint64]struct{}), claimedChunks: make(map[int64]struct{})}
	for _, op := range ops {
		switch op.Type {
		case OpDeleteFile, OpRmdir:
			if len(op.Paths) > 0 {
				p.doomed[op.Paths[0]] = struct{}{}
			}
		case OpRename:
			if len(op.Paths) == 2 {
				p.doomed[op.Paths[0]] = struct{}{}
				p.targets[op.Paths[1]] = struct{}{}
			}
		case OpMkdir:
			if len(op.Paths) > 0 {
				p.mkdirs[op.Paths[0]] = struct{}{}
			}
		}
		opTargetPath, ok := opTarget(op)
		if ok {
			p.targets[opTargetPath] = struct{}{}
		}
		if op.File != nil {
			if op.File.Inode != 0 {
				p.claimedInodes[op.File.Inode] = struct{}{}
			}
			for _, id := range op.File.Chunks {
				p.claimedChunks[id] = struct{}{}
			}
		}
		if op.Dir != nil && op.Dir.Inode != 0 {
			p.claimedInodes[op.Dir.Inode] = struct{}{}
		}
		for id := range op.Chunks {
			p.claimedChunks[id] = struct{}{}
		}
		for _, id := range op.RemovedChunks {
			p.claimedChunks[id] = struct{}{}
		}
	}
	return p
}

// opTarget reports the path an op asserts a record for: rename-to,
// file-state, and mkdir-with-record paths. Deletes, rmdirs, catalog ops,
// xattrs, and record-less mkdirs assert nothing (an EnsureDirectory-only
// mkdir keeps a live occupant, so it must not exempt it).
func opTarget(op Op) (string, bool) {
	switch op.Type {
	case OpRename:
		return "", false // handled explicitly (needs Paths[1])
	case OpPutFile, OpSetattr, OpTruncate, OpPatch:
		if len(op.Paths) > 0 {
			return op.Paths[0], true
		}
	case OpMkdir:
		if len(op.Paths) > 0 && op.Dir != nil {
			return op.Paths[0], true
		}
	}
	return "", false
}

// allocInodeAvoiding mints a fresh inode that no batch op claims.
// Collision remapping must never reissue an identifier another batch op
// carries, or the remap trades one collision for another. Terminates: the
// claimed set is finite and the counter strictly increases.
func (p *replayPlan) allocInodeAvoiding(meta *RepoMetadata) uint64 {
	if p == nil {
		return meta.AllocateInode()
	}
	for {
		id := meta.AllocateInode()
		if _, bad := p.claimedInodes[id]; !bad {
			return id
		}
	}
}

// allocChunkAvoiding is allocInodeAvoiding for chunk catalog ids.
func (p *replayPlan) allocChunkAvoiding(meta *RepoMetadata) int64 {
	if p == nil {
		return meta.AllocateChunkID()
	}
	for {
		id := meta.AllocateChunkID()
		if _, bad := p.claimedChunks[id]; !bad {
			return id
		}
	}
}

// batchTargetsExcept returns the plan's batch-asserted paths minus own:
// occupants at those paths are overwritten by the batch in every delivery
// order, so they never count as divergent allocations. The op's own path
// stays out so a file replacing a same-inode directory still remaps (the
// pre-existing file-branch behavior). Nil-plan safe.
func (p *replayPlan) batchTargetsExcept(own string) map[string]struct{} {
	if p == nil || len(p.targets) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(p.targets))
	for t := range p.targets {
		if t != own {
			out[t] = struct{}{}
		}
	}
	return out
}

// untarget drops an op's asserted paths from the target set. Call it when
// a batch member is SKIPPED (rebase conflict resolution): its overwrite
// never lands, so live occupants at its paths are genuine again and later
// collision checks must see them. Removing a path that was never targeted
// is a no-op.
func (p *replayPlan) untarget(op Op) {
	if len(op.Paths) == 2 && op.Type == OpRename {
		delete(p.targets, op.Paths[1])
	}
	if t, ok := opTarget(op); ok {
		delete(p.targets, t)
	}
}

// unremove drops an op's paths from the doomed set. Call it when a batch
// member is SKIPPED during a conflict-resolving replay (rebase): its removal
// never lands, so a later rmdir must see the survivor as a live child again.
// Removing a path that was never doomed is a no-op.
func (p *replayPlan) unremove(op Op) {
	for _, path := range op.Paths {
		delete(p.doomed, path)
	}
	p.liveValid = false
}

func applyOpsWithResolutions(meta *RepoMetadata, ops []Op, resolutions *[]ConflictResolution) error {
	plan := newReplayPlan(ops)
	cidx := newCollisionIndex(meta)
	for _, op := range ops {
		if err := applyOneOpIndexed(meta, op, plan, resolutions, cidx); err != nil {
			return err
		}
	}
	return nil
}

// recordMove notes that a dir rename relocated a subtree: later ops in
// this batch referencing the recorded from-prefix resolve to the live
// location. Called only when the source existed at apply time, so the
// table never maps a phantom prefix onto an unrelated live tree.
func (p *replayPlan) recordMove(from, to string) {
	if from == "" {
		return
	}
	p.moved[from] = to
	p.liveValid = false
}

// longestMovedPrefix returns the longest table key that is the path itself
// or a strict parent of it (the trailing-slash check keeps "/ab" from
// matching key "/a"). Empty keys never match.
func longestMovedPrefix(moved map[string]string, path string) string {
	best := ""
	for k := range moved {
		if k == "" {
			continue
		}
		if path == k || strings.HasPrefix(path, k+"/") {
			if len(k) > len(best) {
				best = k
			}
		}
	}
	return best
}

// Replay reference flavors (do not conflate):
//
//	resolveLive(meta, path)  — a reference to PRE-EXISTING state: the
//	    literal path when anything lives there, else the longest
//	    moved-prefix translation that lands on something live. Chains are
//	    followed with a visited set; anything unresolvable returns the
//	    original path so the handler's missing-entry semantics apply
//	    unchanged. Used for rename-from, delete, rmdir, and
//	    setattr/patch/truncate targets.
//	translateForward(path)   — pure batch translation: follows the batch's
//	    recorded subtree moves to a fixed point (cycle-safe), with no
//	    liveness checks. Used to map recorded removals to their current
//	    locations (liveRemovals) and to exempt a rename's own
//	    batch-forwarded location from collision remapping.
//
// resolveLive returns the live location for a reference to pre-existing state:
//
//	the literal path when anything lives there, else the longest
//	moved-prefix translation that lands on something live. Chains are
//	followed with a visited set; anything unresolvable returns the original
//	path so the handler's missing-entry semantics apply unchanged.
func (p *replayPlan) resolveLive(meta *RepoMetadata, path string) string {
	if path == "" {
		return path
	}
	live := func(s string) bool {
		return meta.GetDirectory(s) != nil || meta.FindFile(s) != nil
	}
	if live(path) {
		return path
	}
	seen := map[string]struct{}{path: {}}
	cur := path
	for range len(p.moved) + 1 {
		key := longestMovedPrefix(p.moved, cur)
		if key == "" {
			return path
		}
		next := p.moved[key] + cur[len(key):]
		if _, dup := seen[next]; dup {
			return path
		}
		seen[next] = struct{}{}
		cur = next
		if live(cur) {
			return cur
		}
	}
	return path
}

// translateForward translates a recorded path to its current location by
// following the batch's recorded subtree moves to a fixed point (cycle-safe
// via the visited set). Pure translation: no liveness checks.
func (p *replayPlan) translateForward(path string) string {
	cur := path
	seen := map[string]struct{}{path: {}}
	for range len(p.moved) + 1 {
		key := longestMovedPrefix(p.moved, cur)
		if key == "" {
			break
		}
		next := p.moved[key] + cur[len(key):]
		if _, dup := seen[next]; dup {
			break
		}
		seen[next] = struct{}{}
		cur = next
	}
	return cur
}

// liveRemovals translates every recorded-removed path to its current
// location at this point in the replay, for the OpRmdir children check.
// Computed once per batch and memoized; unremove/recordMove invalidate the
// cache for a lazy recompute on next use.
func (p *replayPlan) liveRemovals() map[string]struct{} {
	if p.liveValid && p.liveCache != nil {
		return p.liveCache
	}
	out := make(map[string]struct{}, len(p.doomed))
	for r := range p.doomed {
		out[p.translateForward(r)] = struct{}{}
	}
	p.liveCache = out
	p.liveValid = true
	return out
}

func recordResolution(resolutions *[]ConflictResolution, op Op, path, note string) {
	if resolutions == nil {
		return
	}
	*resolutions = append(*resolutions, ConflictResolution{Seq: op.Seq, Path: path, Note: note})
}

// cloneOpPayloads copies an op's state payloads so replay-time rewrites
// (collision remapping) never mutate the caller's stack entry.
func cloneOpPayloads(op Op) Op {
	if op.File != nil {
		f := op.File.Clone()
		op.File = &f
	}
	if op.Dir != nil {
		d := op.Dir.Clone()
		op.Dir = &d
	}
	if op.Chunks != nil {
		chunks := make(map[int64]ChunkInfo, len(op.Chunks))
		for id, info := range op.Chunks {
			chunks[id] = info
		}
		op.Chunks = chunks
	}
	return op
}

// collisionIndex snapshots inode occupancy once per replay batch so
// remapOpCollisions stays O(1) per op instead of scanning the whole tree per
// replayed op (bulk journal/cold replays were O(ops x tree)). The index is
// updated incrementally as the batch applies (every handler records its
// writes/removes/moves), so it tracks the live tree exactly without rescans:
//
//   - adds are exact: every materializing handler records its payload inode,
//     and batch inodes are unique within the candidate domain (hardlink
//     families share file inodes, which never remap);
//   - removes drop by path, so a stale entry can only cause a spurious
//     remap (fresh inode + note), never a missed collision;
//   - subtree moves relocate index keys with the same prefix rule as
//     remapSubtree.
//
// Chunk-ID collisions need no index: meta.Chunks() is already an O(1) map
// lookup and stays live.
type collisionIndex struct {
	rootInode uint64
	files     map[string]uint64              // live file path -> inode
	dirs      map[string]uint64              // live dir path -> inode
	fileInode map[uint64]map[string]struct{} // inode -> paths holding it
	dirInode  map[uint64]map[string]struct{}
}

func newCollisionIndex(meta *RepoMetadata) *collisionIndex {
	c := &collisionIndex{
		files:     make(map[string]uint64),
		dirs:      make(map[string]uint64),
		fileInode: make(map[uint64]map[string]struct{}),
		dirInode:  make(map[uint64]map[string]struct{}),
	}
	if meta == nil {
		return c
	}
	c.rootInode = meta.Root.Inode
	for path, f := range meta.Files() {
		c.setFile(path, f.Inode)
	}
	for path, d := range meta.Dirs() {
		c.setDir(path, d.Inode)
	}
	return c
}

func indexLink(set map[uint64]map[string]struct{}, inode uint64, path string) {
	m := set[inode]
	if m == nil {
		m = make(map[string]struct{})
		set[inode] = m
	}
	m[path] = struct{}{}
}

func indexUnlink(set map[uint64]map[string]struct{}, inode uint64, path string) {
	m, ok := set[inode]
	if !ok {
		return
	}
	delete(m, path)
	if len(m) == 0 {
		delete(set, inode)
	}
}

// setFile records path holding inode (nil-safe no-op on a nil index).
func (c *collisionIndex) setFile(path string, inode uint64) {
	if c == nil {
		return
	}
	if old, ok := c.files[path]; ok {
		if old == inode {
			return
		}
		indexUnlink(c.fileInode, old, path)
	}
	c.files[path] = inode
	indexLink(c.fileInode, inode, path)
}

// setDir records path holding inode (nil-safe).
func (c *collisionIndex) setDir(path string, inode uint64) {
	if c == nil {
		return
	}
	if old, ok := c.dirs[path]; ok {
		if old == inode {
			return
		}
		indexUnlink(c.dirInode, old, path)
	}
	c.dirs[path] = inode
	indexLink(c.dirInode, inode, path)
}

// delFile drops path's occupancy (nil-safe; missing paths are no-ops).
func (c *collisionIndex) delFile(path string) {
	if c == nil {
		return
	}
	if old, ok := c.files[path]; ok {
		delete(c.files, path)
		indexUnlink(c.fileInode, old, path)
	}
}

// delDir drops path's occupancy (nil-safe; missing paths are no-ops).
func (c *collisionIndex) delDir(path string) {
	if c == nil {
		return
	}
	if old, ok := c.dirs[path]; ok {
		delete(c.dirs, path)
		indexUnlink(c.dirInode, old, path)
	}
}

// setRoot records a root inode change (nil-safe).
func (c *collisionIndex) setRoot(inode uint64) {
	if c == nil {
		return
	}
	c.rootInode = inode
}

// moveSubtree relocates every indexed path under from to its remapped path,
// mirroring remapSubtree with the same prefix rule. Overwritten targets drop
// their previous occupant first, matching the meta overwrite.
func (c *collisionIndex) moveSubtree(from, to string) {
	if c == nil || from == "" {
		return
	}
	move := func(paths map[string]uint64, inodes map[uint64]map[string]struct{}) {
		type pathMove struct{ from, to string }
		var moves []pathMove
		for path := range paths {
			if shfs.IsParentOrSame(from, path) {
				moves = append(moves, pathMove{from: path, to: shfs.RemapPath(from, to, path)})
			}
		}
		for _, m := range moves {
			inode := paths[m.from]
			delete(paths, m.from)
			indexUnlink(inodes, inode, m.from)
			if old, ok := paths[m.to]; ok {
				indexUnlink(inodes, old, m.to)
			}
			paths[m.to] = inode
			indexLink(inodes, inode, m.to)
		}
	}
	move(c.files, c.fileInode)
	move(c.dirs, c.dirInode)
}

// fileCollidesWithDirFamilyExcept is fileCollidesWithDirFamily ignoring
// occupants at skip paths: entries the batch overwrites with its own
// records (replay scaffolding or earlier batch state), never divergent
// upstream allocations. The root always collides.
func (c *collisionIndex) fileCollidesWithDirFamilyExcept(inode uint64, skip map[string]struct{}) bool {
	if c == nil {
		return false
	}
	if inode == c.rootInode {
		return true
	}
	for path := range c.dirInode[inode] {
		if _, ok := skip[path]; !ok {
			return true
		}
	}
	return false
}

// takenByAnotherNode reports whether inode is taken by the root, any
// directory, or any file OTHER than the entries at exceptPaths.
func (c *collisionIndex) takenByAnotherNode(inode uint64, exceptPaths map[string]struct{}) bool {
	if c == nil {
		return false
	}
	if inode == c.rootInode {
		return true
	}
	for path := range c.dirInode[inode] {
		if _, ok := exceptPaths[path]; !ok {
			return true
		}
	}
	for path := range c.fileInode[inode] {
		if _, ok := exceptPaths[path]; !ok {
			return true
		}
	}
	return false
}

// syncDirChain records path and every ancestor directory in the collision
// index (nil-safe). EnsureDirectory may materialize several missing levels
// at once; syncing only the leaf would leave fresh ancestor inodes invisible
// to later collision checks in the batch.
func syncDirChain(meta *RepoMetadata, path string, cidx *collisionIndex) {
	if cidx == nil {
		return
	}
	for p := path; p != ""; p = shfs.ParentPath(p) {
		if d := meta.GetDirectory(p); d != nil {
			cidx.setDir(p, d.Inode)
		}
	}
}

// ensureParentFor creates the parent directory for a literal target path and
// records created parents in the collision index. Shared by every handler
// that materializes state at a new location.
func ensureParentFor(meta *RepoMetadata, target string, now int64, cidx *collisionIndex) {
	parent := shfs.ParentPath(target)
	if parent == "" {
		return
	}
	meta.EnsureDirectory(parent, now)
	syncDirChain(meta, parent, cidx)
}

// applyHandler applies one op class. path is the op's raw Paths[0] scope;
// handlers resolve stale references via the plan themselves.
type applyHandler func(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex, now int64, path string) error

// opApplyHandlers dispatches applyOneOp by class: file-state assertions,
// literal creates, removals, moves, and catalog ops each read independently.
var opApplyHandlers = map[OpType]applyHandler{
	OpPutFile:    applyFileStateOp,
	OpTruncate:   applyFileStateOp,
	OpPatch:      applyFileStateOp,
	OpSetattr:    applyFileStateOp,
	OpXattr:      applyFileStateOp,
	OpMkdir:      applyMkdirOp,
	OpDeleteFile: applyDeleteFileOp,
	OpRmdir:      applyRmdirOp,
	OpRename:     applyRenameOp,
	OpRelease:    applyReleaseOp,
	OpChunkPrune: applyChunkPruneOp,
}

// applyOneOpIndexed is the batch entry: the caller builds one collisionIndex
// per batch (applyOpsWithResolutions, rebaseWorkingTree) and threads it
// through every op, so identifier remapping stays O(1) per op.
func applyOneOpIndexed(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex) error {
	now := op.Timestamp
	path := opPath(op)
	// Work on private payloads: collision remapping rewrites identifiers
	// in place and must never touch the caller's op (the pending stack
	// shares these pointers).
	op = cloneOpPayloads(op)
	// Divergent-writer protection: identifiers both writers allocated for
	// different records are remapped before the state assertion applies.
	remapOpCollisionsIndexed(meta, &op, plan, resolutions, cidx)
	handler, ok := opApplyHandlers[op.Type]
	if !ok {
		return fmt.Errorf("unknown op type %q", op.Type)
	}
	return handler(meta, op, plan, resolutions, cidx, now, path)
}

func applyFileStateOp(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex, now int64, path string) error {
	target := path
	if op.Type != OpPutFile {
		// Rewrite targets reference pre-existing state: resolve a
		// stale prefix moved by an earlier rename in this batch. Puts
		// stay literal - a put's path is its final location, recorded
		// against the post-mutation candidate.
		target = plan.resolveLive(meta, path)
	}
	if op.File != nil {
		ensureParentFor(meta, target, now, cidx)
		meta.WriteFileDirect(target, op.File.Clone())
		cidx.setFile(target, op.File.Inode)
	} else if op.Dir != nil {
		if target != "" {
			ensureParentFor(meta, target, now, cidx)
			meta.WriteDirDirect(target, op.Dir.Clone())
			cidx.setDir(target, op.Dir.Inode)
		} else {
			meta.Root = op.Dir.Clone()
			cidx.setRoot(op.Dir.Inode)
		}
	}
	for id, info := range op.Chunks {
		if err := meta.PutChunk(id, info); err != nil {
			return fmt.Errorf("replay chunk %d: %w", id, err)
		}
	}
	return nil
}

func applyMkdirOp(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex, now int64, path string) error {
	if op.Dir != nil {
		ensureParentFor(meta, path, now, cidx)
		meta.WriteDirDirect(path, op.Dir.Clone())
		cidx.setDir(path, op.Dir.Inode)
	} else {
		meta.EnsureDirectory(path, now)
		syncDirChain(meta, path, cidx)
	}
	return nil
}

func applyDeleteFileOp(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex, now int64, path string) error {
	target := plan.resolveLive(meta, path)
	cidx.delFile(target)
	meta.RemoveFile(target)
	return nil
}

func applyRmdirOp(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex, now int64, path string) error {
	path = plan.resolveLive(meta, path)
	// Order-independent delete: skip only for children the batch did
	// NOT remove (liveRemovals translates recorded removals to their
	// current locations, following intra-batch renames, computed once per
	// batch and memoized). An upstream child still blocks, exactly as before.
	removed := plan.liveRemovals()
	childDirs, childFiles := meta.DirectoryChildren(path)
	blocked := false
	for _, child := range childDirs {
		if _, ok := removed[child]; !ok {
			blocked = true
			break
		}
	}
	if !blocked {
		for _, child := range childFiles {
			if _, ok := removed[child]; !ok {
				blocked = true
				break
			}
		}
	}
	if blocked {
		recordResolution(resolutions, op, path,
			"skipped rmdir: upstream directory non-empty (data preservation)")
		return nil
	}
	cidx.delDir(path)
	meta.RemoveDirectory(path)
	return nil
}

func applyRenameOp(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex, now int64, _ string) error {
	if len(op.Paths) != 2 {
		return fmt.Errorf("rename op %d has %d paths, want 2", op.Seq, len(op.Paths))
	}
	from, to := plan.resolveLive(meta, op.Paths[0]), op.Paths[1]
	if op.File != nil {
		cidx.delFile(from)
		meta.RemoveFile(from)
		ensureParentFor(meta, to, now, cidx)
		meta.WriteFileDirect(to, op.File.Clone())
		cidx.setFile(to, op.File.Inode)
	} else {
		// Directory rename moves exactly the members recorded at fold
		// time (remapSubtreeMembers): entries created under the old path
		// by a LATER op in this batch are not part of the rename, so
		// both delivery orders converge to the candidate. A nil member
		// list means a pre-member-list journal: fall back to moving the
		// live subtree. Either way the move is recorded in the batch
		// plan, and later ops referencing the old prefix resolve to the
		// live location.
		fromExisted := meta.GetDirectory(from) != nil
		if op.Members != nil {
			remapSubtreeMembers(meta, from, to, op.Members, cidx)
		} else {
			remapSubtree(meta, from, to, cidx)
		}
		ensureParentFor(meta, to, now, cidx)
		if op.Dir != nil {
			meta.WriteDirDirect(to, op.Dir.Clone())
			cidx.setDir(to, op.Dir.Inode)
		} else if src, ok := meta.Dirs()[from]; ok {
			meta.WriteDirDirect(to, src)
			cidx.setDir(to, src.Inode)
		} else {
			meta.EnsureDirectory(to, now)
			syncDirChain(meta, to, cidx)
		}
		// Remove the RECORDED source, not the resolved one: resolve
		// only diverts when the literal is missing (nothing there to
		// remove), and when resolution maps from onto to itself this
		// must not delete the just-written target. And only remove it
		// when no other batch member owns the record: a source in the
		// mkdir set is asserted by an OpMkdir carrying the candidate's
		// record, which converges every delivery order onto it - removing
		// here would fork the identity (kept setup record vs recreated
		// fresh one) by order. Otherwise remove only a vacated source: a
		// from-path that still holds children was repopulated by a LATER
		// op in this batch (recreation under the old path), and deleting
		// its dir record would orphan them in one delivery order but not
		// the other.
		cidx.delDir(op.Paths[0])
		_, owned := plan.mkdirs[op.Paths[0]]
		if op.Paths[0] == "" || (!owned && !subtreePopulated(meta, op.Paths[0])) {
			meta.RemoveDirectory(op.Paths[0])
		}
		if fromExisted {
			plan.recordMove(op.Paths[0], to)
		}
	}
	return nil
}

func applyReleaseOp(meta *RepoMetadata, op Op, _ *replayPlan, _ *[]ConflictResolution, _ *collisionIndex, _ int64, _ string) error {
	if op.Release != nil {
		if err := meta.PutRelease(op.Tag, *op.Release); err != nil {
			return fmt.Errorf("replay release %s: %w", op.Tag, err)
		}
	} else {
		meta.RemoveRelease(op.Tag)
	}
	return nil
}

func applyChunkPruneOp(meta *RepoMetadata, op Op, _ *replayPlan, _ *[]ConflictResolution, _ *collisionIndex, _ int64, _ string) error {
	referenced := make(map[int64]struct{})
	for _, file := range meta.Files() {
		for _, id := range file.Chunks {
			referenced[id] = struct{}{}
		}
	}
	for _, id := range op.RemovedChunks {
		if _, ok := referenced[id]; ok {
			continue
		}
		delete(meta.Chunks(), id)
	}
	return nil
}

// remapSubtree moves every entry under from to its remapped path (directory
// rename semantics). Keys are rewritten; entry bodies are untouched. Each
// move goes through the tracked mutators (remove old, write new) so the
// derived index and size cache stay warm incrementally. The collision index
// moves with the entries (nil-safe) so later ops in the batch still resolve
// identifier occupancy exactly.
func remapSubtree(meta *RepoMetadata, from, to string, cidx *collisionIndex) {
	type dirMove struct {
		from, to string
		dir      DirMeta
	}
	var dirMoves []dirMove
	for path, dir := range meta.Dirs() {
		if shfs.IsParentOrSame(from, path) {
			dirMoves = append(dirMoves, dirMove{from: path, to: shfs.RemapPath(from, to, path), dir: dir})
		}
	}
	for _, mv := range dirMoves {
		meta.RemoveDirectory(mv.from)
		meta.WriteDirDirect(mv.to, mv.dir)
	}
	type fileMove struct {
		from, to string
		file     FileMeta
	}
	var fileMoves []fileMove
	for path, file := range meta.Files() {
		if shfs.IsParentOrSame(from, path) {
			fileMoves = append(fileMoves, fileMove{from: path, to: shfs.RemapPath(from, to, path), file: file})
		}
	}
	for _, mv := range fileMoves {
		meta.RemoveFile(mv.from)
		meta.WriteFileDirect(mv.to, mv.file)
	}
	cidx.moveSubtree(from, to)
}

// remapSubtreeMembers moves exactly the listed from-paths to their
// remapped locations: the member list a dir rename recorded at fold time.
// Each move is an independent map-key relocation (remove old, write new)
// through the tracked mutators, so no ordering between members is needed.
// Members missing at replay time are skipped: they were deleted by another
// op in the batch (whose own delete/put carries the final state), or the
// op is replaying onto a tree that already absorbed them. Entries NOT in
// the list — created under the old path after the rename was synthesized —
// stay put, which is what makes both delivery orders converge.
func remapSubtreeMembers(meta *RepoMetadata, from, to string, members []string, cidx *collisionIndex) {
	for _, m := range members {
		dst := shfs.RemapPath(from, to, m)
		if dst == m {
			continue
		}
		if file := meta.FindFile(m); file != nil {
			meta.RemoveFile(m)
			meta.WriteFileDirect(dst, *file)
			cidx.delFile(m)
			cidx.setFile(dst, file.Inode)
			continue
		}
		if dir := meta.GetDirectory(m); dir != nil {
			meta.RemoveDirectory(m)
			meta.WriteDirDirect(dst, *dir)
			cidx.delDir(m)
			cidx.setDir(dst, dir.Inode)
		}
	}
}

// subtreePopulated reports whether any file or directory lives strictly
// under path: after a member-list rename, a populated source was
// repopulated by another op in the batch and its dir record must be kept.
func subtreePopulated(meta *RepoMetadata, path string) bool {
	for p := range meta.Files() {
		if p != path && shfs.IsParentOrSame(path, p) {
			return true
		}
	}
	for p := range meta.Dirs() {
		if p != path && shfs.IsParentOrSame(path, p) {
			return true
		}
	}
	return false
}

func chunksEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// fileEqual reports whether two file entries carry the same identity and
// content. With ignoreChangedAt, ChangedAt is zeroed first: renames and
// child mutations bump it without changing what the entry IS.
func fileEqual(a, b FileMeta, ignoreChangedAt bool) bool {
	if ignoreChangedAt {
		a.ChangedAt, b.ChangedAt = 0, 0
	}
	return fileBodiesEqual(a, b)
}

func fileBodiesEqual(a, b FileMeta) bool {
	if !chunksEqual(a.Chunks, b.Chunks) ||
		a.Size != b.Size || a.Symlink != b.Symlink ||
		a.UploadedAt != b.UploadedAt || a.ModifiedAt != b.ModifiedAt ||
		a.AccessedAt != b.AccessedAt || a.Mode != b.Mode ||
		a.UID != b.UID || a.GID != b.GID || a.Inode != b.Inode ||
		len(a.XAttrs) != len(b.XAttrs) {
		return false
	}
	for k, v := range a.XAttrs {
		bv, ok := b.XAttrs[k]
		if !ok || string(v) != string(bv) {
			return false
		}
	}
	return true
}

// dirEqual reports whether two directory entries carry the same identity.
// With ignoreTimes, ModifiedAt/ChangedAt are zeroed first: renames and child
// mutations touch them without changing the directory's own identity.
func dirEqual(a, b DirMeta, ignoreTimes bool) bool {
	if ignoreTimes {
		a.ModifiedAt, b.ModifiedAt = 0, 0
		a.ChangedAt, b.ChangedAt = 0, 0
	}
	return dirBodiesEqual(a, b)
}

func dirBodiesEqual(a, b DirMeta) bool {
	if a.CreatedAt != b.CreatedAt || a.AccessedAt != b.AccessedAt ||
		a.Mode != b.Mode || a.UID != b.UID || a.GID != b.GID || a.Inode != b.Inode ||
		len(a.XAttrs) != len(b.XAttrs) {
		return false
	}
	for k, v := range a.XAttrs {
		bv, ok := b.XAttrs[k]
		if !ok || string(v) != string(bv) {
			return false
		}
	}
	return true
}

// chunkRecordsFor collects the catalog records a file's chunk IDs reference,
// making a put op self-contained (replay never depends on the records
// already existing upstream).
func chunkRecordsFor(meta *RepoMetadata, ids []int64) map[int64]ChunkInfo {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[int64]ChunkInfo, len(ids))
	for _, id := range ids {
		if info, ok := meta.Chunks()[id]; ok {
			out[id] = info
		}
	}
	return out
}

// opSummaryCounts renders the per-class op counts for a commit summary
// line, fixed order, non-zero classes only: "2 put, 1 del, 1 mkdir".
func opSummaryCounts(ops []Op) string {
	order := []struct {
		label string
		match func(OpType) bool
	}{
		{"put", func(t OpType) bool {
			return t == OpPutFile || t == OpTruncate || t == OpPatch || t == OpSetattr || t == OpXattr
		}},
		{"del", isDeleteClass},
		{"mkdir", func(t OpType) bool { return t == OpMkdir }},
		{"rename", func(t OpType) bool { return t == OpRename }},
		{"release", func(t OpType) bool { return t == OpRelease }},
		{"prune", func(t OpType) bool { return t == OpChunkPrune }},
	}
	counts := make(map[string]int, len(order))
	for _, op := range ops {
		for _, c := range order {
			if c.match(op.Type) {
				counts[c.label]++
				break
			}
		}
	}
	parts := make([]string, 0, len(order))
	for _, c := range order {
		if counts[c.label] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[c.label], c.label))
		}
	}
	return strings.Join(parts, ", ")
}

// buildCommitMessage renders the commit message for a batch of ops: a
// summary first line plus one body line per op (capped), giving the
// metadata history the "what happened to this file" answer the generic
// message never could.
func buildCommitMessage(ops []Op, previousSHA string) string {
	if len(ops) == 0 {
		return "storhub: update metadata"
	}
	var sb strings.Builder
	counts := opSummaryCounts(ops)
	if previousSHA != "" {
		fmt.Fprintf(&sb, "storhub: %d ops (%s) on top of %s", len(ops), counts, shortSHA(previousSHA))
	} else {
		fmt.Fprintf(&sb, "storhub: %d ops (%s)", len(ops), counts)
	}
	const maxBodyLines = 100
	for i, op := range ops {
		if i == maxBodyLines {
			fmt.Fprintf(&sb, "\n+ %d more", len(ops)-i)
			break
		}
		sb.WriteString("\n")
		sb.WriteString(opMessageLine(op))
	}
	return sb.String()
}

func opMessageLine(op Op) string {
	var sb strings.Builder
	mode := func() string {
		if op.File != nil {
			return fmt.Sprintf("%04o", op.File.Mode)
		}
		if op.Dir != nil {
			return fmt.Sprintf("%04o", op.Dir.Mode)
		}
		return ""
	}
	switch op.Type {
	case OpPutFile:
		sb.WriteString("put ")
		sb.WriteString(opPath(op))
		if op.File != nil {
			if op.File.Symlink != "" {
				fmt.Fprintf(&sb, " symlink -> %s", op.File.Symlink)
			} else {
				release := ""
				for _, id := range op.File.Chunks {
					if info, ok := op.Chunks[id]; ok && info.Release != "" {
						release = info.Release
						break
					}
				}
				if release != "" {
					fmt.Fprintf(&sb, " %s %d chunks (%s) mode %s", humanizeBytes(op.File.Size), len(op.File.Chunks), release, mode())
				} else {
					fmt.Fprintf(&sb, " %s mode %s", humanizeBytes(op.File.Size), mode())
				}
			}
		}
	case OpTruncate:
		size := int64(0)
		if op.File != nil {
			size = op.File.Size
		}
		fmt.Fprintf(&sb, "trunc %s to %s", opPath(op), humanizeBytes(size))
	case OpPatch:
		sb.WriteString("patch ")
		sb.WriteString(opPath(op))
	case OpSetattr:
		fmt.Fprintf(&sb, "setattr %s", opPath(op))
		if m := mode(); m != "" {
			fmt.Fprintf(&sb, " mode %s", m)
		}
	case OpXattr:
		fmt.Fprintf(&sb, "xattr %s %s", opPath(op), op.XAttr)
	case OpDeleteFile:
		fmt.Fprintf(&sb, "del %s", opPath(op))
		if op.FreedChunks > 0 {
			fmt.Fprintf(&sb, " freed %d chunks", op.FreedChunks)
		}
	case OpMkdir:
		fmt.Fprintf(&sb, "mkdir %s", opPath(op))
		if m := mode(); m != "" {
			fmt.Fprintf(&sb, " mode %s", m)
		}
	case OpRmdir:
		fmt.Fprintf(&sb, "rmdir %s", opPath(op))
	case OpRename:
		to := opPath(op)
		if len(op.Paths) == 2 {
			to = op.Paths[1]
		}
		fmt.Fprintf(&sb, "rename %s -> %s", opPath(op), to)
	case OpRelease:
		verb := "add"
		if op.Release == nil {
			verb = "del"
		}
		fmt.Fprintf(&sb, "release %s %s", verb, op.Tag)
	case OpChunkPrune:
		fmt.Fprintf(&sb, "prune %d chunk records", len(op.RemovedChunks))
	default:
		fmt.Fprintf(&sb, "%s %s", op.Type, opPath(op))
	}
	fmt.Fprintf(&sb, " [%s]", op.Cause)
	if op.Times > 1 {
		fmt.Fprintf(&sb, " (x%d)", op.Times)
	}
	return sb.String()
}

// causeFromMessage extracts the operation word from a transaction message
// ("storhub: mkdir /path" -> "mkdir") so op causes stay short and stable.
func causeFromMessage(message string) string {
	m := strings.TrimSpace(message)
	m = strings.TrimPrefix(m, "storhub:")
	m = strings.TrimSpace(m)
	if i := strings.IndexAny(m, " \t:"); i >= 0 {
		m = m[:i]
	}
	if m == "" {
		return "update"
	}
	return m
}

// humanizeBytes renders a byte count for commit messages: 512B, 3.0KiB,
// 2.2MiB, 1.5GiB. Scales truncate (floor) rather than round so the shown
// size never overstates the stored bytes.
func humanizeBytes(n int64) string {
	if n < 0 {
		return "?" + humanizeBytes(-n)
	}
	units := []struct {
		name string
		size float64
	}{
		{"B", 1}, {"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	}
	value := float64(n)
	unit := units[0]
	for _, u := range units {
		if value >= u.size {
			unit = u
		}
	}
	if unit.size == 1 {
		return fmt.Sprintf("%dB", n)
	}
	scaled := math.Floor(value/unit.size*10) / 10
	return fmt.Sprintf("%.1f%s", scaled, unit.name)
}
