package storage

import (
	"fmt"
	"math"
	"strings"
	"syscall"
)

// foldOps coalesces a raw op sequence (journal DELTA lines, one per
// appendWithDelta) through the same rules as live appends, so a replayed
// journal yields exactly the stack the crashed process held. The rules are
// deliberately identical to live appends: no cross-line rewrites live here:
// convergence comes from replaying the identical append sequence, not from
// fold-specific chain/delete handling. Journal lines carry Times=1 (see
// journalAppend/journalRead); the fold accumulates Times exactly as live
// coalescing does.
//
// Each delta also carries its append-time generation (Op.Gen), and append
// tracks it line by line, merging only within one generation: the fold
// reproduces the live stack exactly, including chains split by a commit
// freeze mid-chain. The old per-delta snapshot-mark replay is deleted:
// with the boundary in the structure, no mark is needed to steer merge
// decisions, and legacy marked lines (SnapSeq>0, Gen 0) merge freely while
// still replaying to the same tree as the old split fold
// (TestJournalGenCompatSnapMarkedLinesConverge). Marks share numbering
// with op seqs only historically; the fold still preserves the journaled
// seqs (the stack counter fast-forwards to each line) because drain
// targets and resolutions number by them: only the merge shape, order,
// and numbering must match, which is what the equivalence tests pin.
func foldOps(ops []Op) []Op {
	stack := &opStack{}
	for _, op := range ops {
		if op.Seq > stack.seq {
			stack.seq = op.Seq - 1
		}
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
			}
		case OpMkdir:
			if len(op.Paths) > 0 {
				p.mkdirs[op.Paths[0]] = struct{}{}
			}
		}
		for _, t := range opAssertPaths(op) {
			p.targets[t] = struct{}{}
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

// opAssertPaths reports every path an op asserts a record for: rename-to,
// file-state, and mkdir-with-record paths. Deletes, rmdirs, catalog ops,
// xattrs, and record-less mkdirs assert nothing (an EnsureDirectory-only
// mkdir keeps a live occupant, so it must not exempt it).
func opAssertPaths(op Op) []string {
	switch op.Type {
	case OpRename:
		if len(op.Paths) == 2 {
			return []string{op.Paths[1]}
		}
	case OpPutFile, OpSetattr, OpTruncate, OpPatch:
		if len(op.Paths) > 0 {
			return []string{op.Paths[0]}
		}
	case OpMkdir:
		if len(op.Paths) > 0 && op.Dir != nil {
			return []string{op.Paths[0]}
		}
	}
	return nil
}

// allocInodeAvoiding mints a fresh inode that no batch op claims.
// Collision remapping must never reissue an identifier another batch op
// carries, or the remap trades one collision for another. The loop is
// pigeonhole-bounded: at most len(claimed) mints can collide, so
// len(claimed)+1 iterations always succeed on a live counter. A wrapped
// counter (minting math.MaxUint64) means the id space is exhausted: the
// next mint would reissue low ids, so the allocator fails closed with an
// ENOSPC-wrapped error instead of reusing identifiers.
func (p *replayPlan) allocInodeAvoiding(meta *RepoMetadata) (uint64, error) {
	if p == nil {
		id := meta.AllocateInode()
		if id == math.MaxUint64 {
			return 0, fmt.Errorf("alloc inode: %w: inode id space exhausted", syscall.ENOSPC)
		}
		return id, nil
	}
	for i := 0; i <= len(p.claimedInodes); i++ {
		id := meta.AllocateInode()
		if id == math.MaxUint64 {
			return 0, fmt.Errorf("alloc inode: %w: inode id space exhausted", syscall.ENOSPC)
		}
		if _, bad := p.claimedInodes[id]; !bad {
			return id, nil
		}
	}
	// Unreachable on a live counter: the pigeonhole bound above guarantees
	// a return. Returning an error instead of looping forever turns a logic
	// error into a diagnosed commit failure rather than a wedged loop.
	return 0, fmt.Errorf("alloc inode: %w: exhausted pigeonhole bound with %d claimed ids", syscall.ENOSPC, len(p.claimedInodes))
}

// allocChunkAvoiding is allocInodeAvoiding for chunk catalog ids.
func (p *replayPlan) allocChunkAvoiding(meta *RepoMetadata) (int64, error) {
	if p == nil {
		id := meta.AllocateChunkID()
		if id == math.MaxInt64 {
			return 0, fmt.Errorf("alloc chunk: %w: chunk id space exhausted", syscall.ENOSPC)
		}
		return id, nil
	}
	for i := 0; i <= len(p.claimedChunks); i++ {
		id := meta.AllocateChunkID()
		if id == math.MaxInt64 {
			return 0, fmt.Errorf("alloc chunk: %w: chunk id space exhausted", syscall.ENOSPC)
		}
		if _, bad := p.claimedChunks[id]; !bad {
			return id, nil
		}
	}
	return 0, fmt.Errorf("alloc chunk: %w: exhausted pigeonhole bound with %d claimed ids", syscall.ENOSPC, len(p.claimedChunks))
}

// untarget drops an op's asserted paths from the target set. Call it when
// a batch member is SKIPPED (rebase conflict resolution): its overwrite
// never lands, so live occupants at its paths are genuine again and later
// collision checks must see them. Removing a path that was never targeted
// is a no-op.
func (p *replayPlan) untarget(op Op) {
	for _, t := range opAssertPaths(op) {
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
//	resolveLive(meta, path): a reference to PRE-EXISTING state: the
//	    literal path when anything lives there, else the longest
//	    moved-prefix translation that lands on something live. Chains are
//	    followed with a visited set; anything unresolvable returns the
//	    original path so the handler's missing-entry semantics apply
//	    unchanged. Used for rename-from, delete, rmdir, and
//	    setattr/patch/truncate targets.
//	translateForward(path): pure batch translation: follows the batch's
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
