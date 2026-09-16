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
type opStack struct {
	ops      []Op
	seq      uint64
	byPath   map[string]int // Paths[0] of state/delete/release ops -> index
	byTarget map[string]int // Paths[1] of rename ops -> index
	pruneIdx int            // index of the merged chunk-prune op, -1 when none
	// lastSnapshotSeq is the seq stamped at the most recent commit
	// snapshot. Ops at or below it may be in flight inside that commit
	// (its rebase replays the snapshot, not the live stack), so
	// coalescing must never rewrite them.
	lastSnapshotSeq uint64
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
		if len(op.Paths) > 0 {
			s.byPath[op.Paths[0]] = idx
		}
	}
}

// removeAt drops the op at idx and shifts the indices of everything after
// it. O(n) but only reached on coalesce/transform, which bulk imports never
// hit.
func (s *opStack) removeAt(idx int) {
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
		path := opPath(op)
		if idx, ok := s.byPath[path]; ok {
			existing := s.ops[idx]
			if isStateClass(existing.Type) || isDeleteClass(existing.Type) {
				op.Times += existing.Times
				s.removeAt(idx)
				s.ops = append(s.ops, op)
				s.indexOp(op, len(s.ops)-1)
				return
			}
		}
		s.ops = append(s.ops, op)
		s.indexOp(op, len(s.ops)-1)
	case isStateClass(op.Type):
		path := opPath(op)
		if idx, ok := s.byPath[path]; ok {
			existing := s.ops[idx]
			if isStateClass(existing.Type) || isDeleteClass(existing.Type) {
				op.Times += existing.Times
				s.removeAt(idx)
				s.ops = append(s.ops, op)
				s.indexOp(op, len(s.ops)-1)
				return
			}
		}
		s.ops = append(s.ops, op)
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
			s.byTarget[to] = idx
			return
		}
		s.ops = append(s.ops, op)
		s.indexOp(op, len(s.ops)-1)
	case op.Type == OpChunkPrune:
		if s.pruneIdx >= 0 {
			existing := s.ops[s.pruneIdx]
			existing.RemovedChunks = append(existing.RemovedChunks, op.RemovedChunks...)
			existing.Timestamp = op.Timestamp
			existing.Times += op.Times
			existing.Seq = op.Seq
			s.ops[s.pruneIdx] = existing
			return
		}
		s.ops = append(s.ops, op)
		s.indexOp(op, len(s.ops)-1)
	case op.Type == OpRelease:
		if idx, ok := s.byPath[op.Tag]; ok && s.ops[idx].Type == OpRelease {
			op.Times += s.ops[idx].Times
			s.removeAt(idx)
		}
		s.ops = append(s.ops, op)
		s.indexOp(op, len(s.ops)-1)
	default:
		s.ops = append(s.ops, op)
	}
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
	for _, op := range s.ops {
		if op.Seq > seq {
			kept = append(kept, op)
		}
	}
	s.ops = kept
	s.reindex()
}

func (s *opStack) clear() {
	s.ops = nil
	s.byPath = nil
	s.byTarget = nil
	s.pruneIdx = -1
}

// reindex rebuilds the lookup maps from scratch after bulk mutation.
func (s *opStack) reindex() {
	s.byPath = make(map[string]int, len(s.ops))
	s.byTarget = make(map[string]int)
	s.pruneIdx = -1
	for i, op := range s.ops {
		s.indexOp(op, i)
	}
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

// foldOps coalesces a raw op sequence (e.g. journal lines) through the same
// rules as live appends, so a replayed journal yields exactly the stack the
// crashed process would have held.
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
}

func newReplayPlan(ops []Op) *replayPlan {
	p := &replayPlan{doomed: make(map[string]struct{}), moved: make(map[string]string)}
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
		}
	}
	return p
}

// unremove drops an op's paths from the doomed set. Call it when a batch
// member is SKIPPED during a conflict-resolving replay (rebase): its removal
// never lands, so a later rmdir must see the survivor as a live child again.
// Removing a path that was never doomed is a no-op.
func (p *replayPlan) unremove(op Op) {
	for _, path := range op.Paths {
		delete(p.doomed, path)
	}
}

func applyOpsWithResolutions(meta *RepoMetadata, ops []Op, resolutions *[]ConflictResolution) error {
	plan := newReplayPlan(ops)
	for _, op := range ops {
		if err := applyOneOp(meta, op, plan, resolutions); err != nil {
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

// resolve returns the live location for a reference to pre-existing state:
// the literal path when anything lives there, else the longest
// moved-prefix translation that lands on something live. Chains are
// followed with a visited set; anything unresolvable returns the original
// path so the handler's missing-entry semantics apply unchanged.
func (p *replayPlan) resolve(meta *RepoMetadata, path string) string {
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

// forward translates a recorded path to its current location by following
// the batch's recorded subtree moves to a fixed point (cycle-safe via the
// visited set). Pure translation: no liveness checks.
func (p *replayPlan) forward(path string) string {
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
func (p *replayPlan) liveRemovals() map[string]struct{} {
	out := make(map[string]struct{}, len(p.doomed))
	for r := range p.doomed {
		out[p.forward(r)] = struct{}{}
	}
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

func applyOneOp(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution) error {
	now := op.Timestamp
	path := opPath(op)
	// Work on private payloads: collision remapping rewrites identifiers
	// in place and must never touch the caller's op (the pending stack
	// shares these pointers).
	op = cloneOpPayloads(op)
	// Divergent-writer protection: identifiers both writers allocated for
	// different records are remapped before the state assertion applies.
	remapOpCollisions(meta, &op, plan, resolutions)
	switch op.Type {
	case OpPutFile, OpTruncate, OpPatch, OpSetattr, OpXattr:
		target := path
		if op.Type != OpPutFile {
			// Rewrite targets reference pre-existing state: resolve a
			// stale prefix moved by an earlier rename in this batch. Puts
			// stay literal - a put's path is its final location, recorded
			// against the post-mutation candidate.
			target = plan.resolve(meta, path)
		}
		if op.File != nil {
			if parent := shfs.ParentPath(target); parent != "" {
				meta.EnsureDirectory(parent, now)
			}
			meta.WriteFileDirect(target, op.File.Clone())
		} else if op.Dir != nil {
			if target != "" {
				if parent := shfs.ParentPath(target); parent != "" {
					meta.EnsureDirectory(parent, now)
				}
				meta.WriteDirDirect(target, op.Dir.Clone())
			} else {
				meta.Root = op.Dir.Clone()
			}
		}
		for id, info := range op.Chunks {
			meta.PutChunk(id, info)
		}
	case OpMkdir:
		if op.Dir != nil {
			if parent := shfs.ParentPath(path); parent != "" {
				meta.EnsureDirectory(parent, now)
			}
			meta.WriteDirDirect(path, op.Dir.Clone())
		} else {
			meta.EnsureDirectory(path, now)
		}
	case OpDeleteFile:
		meta.RemoveFile(plan.resolve(meta, path))
	case OpRmdir:
		path = plan.resolve(meta, path)
		// Order-independent delete: skip only for children the batch did
		// NOT remove (liveRemovals translates recorded removals to their
		// current locations, following intra-batch renames). An upstream
		// child still blocks, exactly as before.
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
		meta.RemoveDirectory(path)
	case OpRename:
		if len(op.Paths) != 2 {
			return fmt.Errorf("rename op %d has %d paths, want 2", op.Seq, len(op.Paths))
		}
		from, to := plan.resolve(meta, op.Paths[0]), op.Paths[1]
		if op.File != nil {
			meta.RemoveFile(from)
			if parent := shfs.ParentPath(to); parent != "" {
				meta.EnsureDirectory(parent, now)
			}
			meta.WriteFileDirect(to, op.File.Clone())
		} else {
			// Directory rename moves the whole subtree in place
			// (remapSubtree): entries upstream added under the old subtree
			// ride along instead of being orphaned. No emission ordering is
			// needed: the move is recorded in the batch plan, and later
			// ops referencing the old prefix resolve to the live location.
			fromExisted := meta.GetDirectory(from) != nil
			remapSubtree(meta, from, to)
			if parent := shfs.ParentPath(to); parent != "" {
				meta.EnsureDirectory(parent, now)
			}
			if op.Dir != nil {
				meta.WriteDirDirect(to, op.Dir.Clone())
			} else if src, ok := meta.Dirs()[from]; ok {
				meta.WriteDirDirect(to, src)
			} else {
				meta.EnsureDirectory(to, now)
			}
			// Remove the RECORDED source, not the resolved one: resolve
			// only diverts when the literal is missing (nothing there to
			// remove), and when resolution maps from onto to itself this
			// must not delete the just-written target.
			meta.RemoveDirectory(op.Paths[0])
			if fromExisted {
				plan.recordMove(op.Paths[0], to)
			}
		}
	case OpRelease:
		if op.Release != nil {
			meta.PutRelease(op.Tag, op.Release.Clone())
		} else {
			meta.RemoveRelease(op.Tag)
		}
	case OpChunkPrune:
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
	default:
		return fmt.Errorf("unknown op type %q", op.Type)
	}
	return nil
}

// remapSubtree moves every entry under from to its remapped path (directory
// rename semantics). Keys are rewritten; entry bodies are untouched. Each
// move goes through the tracked mutators (remove old, write new) so the
// derived index and size cache stay warm incrementally.
func remapSubtree(meta *RepoMetadata, from, to string) {
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

// fileEntriesEquivalent reports whether two file entries carry the same
// identity and content. ChangedAt is ignored: rename bumps it without
// changing what the entry IS.
func fileEntriesEquivalent(a, b FileMeta) bool {
	if a.ChangedAt != b.ChangedAt {
		a.ChangedAt, b.ChangedAt = 0, 0
	}
	return fileBodiesEqual(a, b)
}

func fileRenameEquivalent(a, b FileMeta) bool {
	a.ChangedAt, b.ChangedAt = 0, 0
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

// dirEntriesEquivalent ignores ModifiedAt/ChangedAt: renames and child
// mutations touch them without changing the directory's own identity.
func dirEntriesEquivalent(a, b DirMeta) bool {
	a.ModifiedAt, b.ModifiedAt = 0, 0
	a.ChangedAt, b.ChangedAt = 0, 0
	return dirBodiesEqual(a, b)
}

func dirRenameEquivalent(a, b DirMeta) bool {
	a.ModifiedAt, b.ModifiedAt = 0, 0
	a.ChangedAt, b.ChangedAt = 0, 0
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
