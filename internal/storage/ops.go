package storage

import (
	"fmt"
	"math"
	"sort"
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

func applyOpsWithResolutions(meta *RepoMetadata, ops []Op, resolutions *[]ConflictResolution) error {
	for _, op := range ops {
		if err := applyOneOp(meta, op, resolutions); err != nil {
			return err
		}
	}
	return nil
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

func applyOneOp(meta *RepoMetadata, op Op, resolutions *[]ConflictResolution) error {
	now := op.Timestamp
	path := opPath(op)
	// Work on private payloads: collision remapping rewrites identifiers
	// in place and must never touch the caller's op (the pending stack
	// shares these pointers).
	op = cloneOpPayloads(op)
	// Divergent-writer protection: identifiers both writers allocated for
	// different records are remapped before the state assertion applies.
	remapOpCollisions(meta, &op, resolutions)
	switch op.Type {
	case OpPutFile, OpTruncate, OpPatch, OpSetattr, OpXattr:
		if op.File != nil {
			if parent := shfs.ParentPath(path); parent != "" {
				meta.EnsureDirectory(parent, now)
			}
			meta.WriteFileDirect(path, op.File.Clone())
		} else if op.Dir != nil {
			if path != "" {
				if parent := shfs.ParentPath(path); parent != "" {
					meta.EnsureDirectory(parent, now)
				}
				meta.WriteDirDirect(path, op.Dir.Clone())
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
		meta.RemoveFile(path)
	case OpRmdir:
		childDirs, childFiles := meta.DirectoryChildren(path)
		if len(childDirs) > 0 || len(childFiles) > 0 {
			recordResolution(resolutions, op, path,
				"skipped rmdir: upstream directory non-empty (data preservation)")
			return nil
		}
		meta.RemoveDirectory(path)
	case OpRename:
		if len(op.Paths) != 2 {
			return fmt.Errorf("rename op %d has %d paths, want 2", op.Seq, len(op.Paths))
		}
		from, to := op.Paths[0], op.Paths[1]
		if op.File != nil {
			meta.RemoveFile(from)
			if parent := shfs.ParentPath(to); parent != "" {
				meta.EnsureDirectory(parent, now)
			}
			meta.WriteFileDirect(to, op.File.Clone())
		} else {
			// Directory rename: entries upstream added under the old
			// subtree move along with the rename instead of being
			// orphaned; ops for children recorded explicitly have already
			// replayed (children-first ordering), leaving nothing to remap.
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
			meta.RemoveDirectory(from)
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

// synthesizeOpsFromDiff derives the op set for one metadata transaction by
// diffing the pre-transaction tree against the post-mutation candidate. It
// covers every mutation that flows through UpdateRepoMetadataContext; direct
// mutation sites emit their ops explicitly. Rename detection pairs a removed
// entry with an added entry carrying the same identity (rename bumps
// ChangedAt on files and ModifiedAt/ChangedAt on dirs, so those fields are
// ignored when pairing).
func synthesizeOpsFromDiff(before, after *RepoMetadata, cause string, now int64) []Op {
	stack := &opStack{}

	removedFiles := map[string]FileMeta{}
	removedDirs := map[string]DirMeta{}
	type dirChange struct {
		path  string
		entry DirMeta
		kind  OpType
	}
	var dirChanges []dirChange
	addedDirs := map[string]DirMeta{}

	for path, entry := range before.Files() {
		if _, ok := after.Files()[path]; !ok {
			removedFiles[path] = entry
		}
	}
	for path, entry := range before.Dirs() {
		if _, ok := after.Dirs()[path]; !ok {
			removedDirs[path] = entry
		}
	}
	for path, entry := range after.Dirs() {
		prev, ok := before.Dirs()[path]
		if !ok {
			addedDirs[path] = entry
			continue
		}
		if dirEntriesEquivalent(prev, entry) {
			continue
		}
		dirChanges = append(dirChanges, dirChange{path: path, entry: entry, kind: OpSetattr})
	}
	if !dirEntriesEquivalent(before.Root, after.Root) {
		dirChanges = append(dirChanges, dirChange{path: "", entry: after.Root, kind: OpSetattr})
	}

	// Rename pairing: a rename preserves the inode, so pair removed and
	// added entries by inode (O(n)) and confirm with a body comparison
	// that ignores the ChangedAt bump a rename applies. Depth-descending
	// emission keeps replay correct (children move before their parent).
	type renamePair struct {
		from, to string
		file     *FileMeta
		dir      *DirMeta
	}
	var renames []renamePair
	consumedFiles := map[string]bool{}
	consumedDirs := map[string]bool{}
	addedByInode := make(map[uint64][]string)
	for path, entry := range after.Files() {
		if _, existed := before.Files()[path]; !existed {
			addedByInode[entry.Inode] = append(addedByInode[entry.Inode], path)
		}
	}
	for fromPath, fromEntry := range removedFiles {
		for _, toPath := range addedByInode[fromEntry.Inode] {
			if consumedFiles[toPath] {
				continue
			}
			toEntry := after.Files()[toPath]
			if fileRenameEquivalent(fromEntry, toEntry) {
				entry := toEntry.Clone()
				renames = append(renames, renamePair{from: fromPath, to: toPath, file: &entry})
				consumedFiles[toPath] = true
				delete(removedFiles, fromPath)
				break
			}
		}
	}
	addedDirByInode := make(map[uint64]string, len(addedDirs))
	for path, entry := range addedDirs {
		addedDirByInode[entry.Inode] = path
	}
	for fromPath, fromEntry := range removedDirs {
		toPath, ok := addedDirByInode[fromEntry.Inode]
		if !ok || consumedDirs[toPath] {
			continue
		}
		toEntry := addedDirs[toPath]
		if dirRenameEquivalent(fromEntry, toEntry) {
			entry := toEntry.Clone()
			renames = append(renames, renamePair{from: fromPath, to: toPath, dir: &entry})
			consumedDirs[toPath] = true
			delete(removedDirs, fromPath)
		}
	}
	sort.Slice(renames, func(i, j int) bool { return depthOf(renames[i].from) > depthOf(renames[j].from) })
	for _, rn := range renames {
		op := Op{Type: OpRename, Paths: []string{rn.from, rn.to}, Cause: cause, Timestamp: now}
		if rn.file != nil {
			op.File = rn.file
			op.Chunks = chunkRecordsFor(after, rn.file.Chunks)
		} else {
			op.Dir = rn.dir
		}
		stack.append(op)
	}

	// Remaining deletes, deepest first so an rmdir replays after its
	// (separately recorded) children are gone.
	delPaths := make([]string, 0, len(removedFiles)+len(removedDirs))
	for path := range removedFiles {
		delPaths = append(delPaths, path)
	}
	for path := range removedDirs {
		delPaths = append(delPaths, path)
	}
	sort.Slice(delPaths, func(i, j int) bool { return depthOf(delPaths[i]) > depthOf(delPaths[j]) })
	for _, path := range delPaths {
		if entry, ok := removedFiles[path]; ok {
			stack.append(Op{Type: OpDeleteFile, Paths: []string{path}, Cause: cause, Timestamp: now, FreedChunks: len(entry.Chunks)})
			continue
		}
		stack.append(Op{Type: OpRmdir, Paths: []string{path}, Cause: cause, Timestamp: now})
	}

	// Directory creates/changes, shallowest first; root setattr (path "")
	// sorts first naturally.
	sort.Slice(dirChanges, func(i, j int) bool { return depthOf(dirChanges[i].path) < depthOf(dirChanges[j].path) })
	for _, ch := range dirChanges {
		entry := ch.entry.Clone()
		stack.append(Op{Type: ch.kind, Paths: []string{ch.path}, Cause: cause, Timestamp: now, Dir: &entry})
	}
	addedDirPaths := make([]string, 0, len(addedDirs))
	for path := range addedDirs {
		if consumedDirs[path] {
			continue
		}
		addedDirPaths = append(addedDirPaths, path)
	}
	sort.Strings(addedDirPaths)
	for _, path := range addedDirPaths {
		entry := addedDirs[path].Clone()
		stack.append(Op{Type: OpMkdir, Paths: []string{path}, Cause: cause, Timestamp: now, Dir: &entry})
	}

	// File creates/changes, skipping entries already consumed as rename
	// targets.
	for path, entry := range after.Files() {
		if consumedFiles[path] {
			continue
		}
		prev, ok := before.Files()[path]
		if ok && fileEntriesEquivalent(prev, entry) {
			continue
		}
		kind := OpPutFile
		if ok && chunksEqual(prev.Chunks, entry.Chunks) && prev.Size == entry.Size && prev.Symlink == entry.Symlink {
			kind = OpSetattr
		}
		clone := entry.Clone()
		op := Op{Type: kind, Paths: []string{path}, Cause: cause, Timestamp: now, File: &clone}
		if kind == OpPutFile {
			op.Chunks = chunkRecordsFor(after, clone.Chunks)
		}
		stack.append(op)
	}

	// Release catalog: additions/changes and removals. AssetCount is
	// derived (RecomputeStats rewrites it), so only CreatedAt is compared.
	for tag, ref := range after.Releases() {
		if prev, ok := before.Releases()[tag]; ok && prev.CreatedAt == ref.CreatedAt {
			continue
		}
		clone := ref.Clone()
		stack.append(Op{Type: OpRelease, Paths: []string{tag}, Tag: tag, Release: &clone, Cause: cause, Timestamp: now})
	}
	for tag := range before.Releases() {
		if _, ok := after.Releases()[tag]; !ok {
			stack.append(Op{Type: OpRelease, Paths: []string{tag}, Tag: tag, Cause: cause, Timestamp: now})
		}
	}

	// Chunk catalog shrinkage (PruneUnreferencedChunks): one merged op.
	var removedChunks []int64
	for id := range before.Chunks() {
		if _, ok := after.Chunks()[id]; !ok {
			removedChunks = append(removedChunks, id)
		}
	}
	if len(removedChunks) > 0 {
		sort.Slice(removedChunks, func(i, j int) bool { return removedChunks[i] < removedChunks[j] })
		stack.append(Op{Type: OpChunkPrune, Cause: cause, Timestamp: now, RemovedChunks: removedChunks})
	}

	return stack.ops
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

func depthOf(path string) int {
	if path == "" {
		return 0
	}
	return strings.Count(path, "/") + 1
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
