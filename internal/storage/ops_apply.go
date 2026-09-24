package storage

import (
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
)

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

// fileCollidesWithLiveDir reports whether inode is taken by the root or a
// directory the batch does NOT overwrite: occupants at batch-asserted
// paths are replay scaffolding (or earlier batch state), never divergent
// upstream allocations, so they don't count. The op's own path is never
// exempted: a file replacing a same-inode directory keeps the pre-existing
// remap behavior. Occupants per inode are typically one, so testing
// membership inline is O(occupants); building a per-op copy of the target
// set here would make file-heavy batches quadratic. The root always
// collides. Nil-plan safe.
func (c *collisionIndex) fileCollidesWithLiveDir(inode uint64, p *replayPlan, own string) bool {
	if c == nil {
		return false
	}
	if inode == c.rootInode {
		return true
	}
	for path := range c.dirInode[inode] {
		if path == own {
			continue
		}
		if p != nil {
			if _, ok := p.targets[path]; ok {
				continue
			}
		}
		return true
	}
	return false
}

// takenByAnotherNodeExceptTargets is takenByAnotherNode with batch-target
// awareness: occupants at target paths are overwritten by the batch in
// every delivery order, so they never count as divergent allocations.
// Membership is tested inline (O(occupants)) instead of copying the whole
// target set per op, which would make dir-heavy batches quadratic.
func (c *collisionIndex) takenByAnotherNodeExceptTargets(inode uint64, exceptPaths map[string]struct{}, targets map[string]struct{}) bool {
	if c == nil {
		return false
	}
	if inode == c.rootInode {
		return true
	}
	occupied := func(path string) bool {
		if _, ok := exceptPaths[path]; ok {
			return false
		}
		if _, ok := targets[path]; ok {
			return false
		}
		return true
	}
	for path := range c.dirInode[inode] {
		if occupied(path) {
			return true
		}
	}
	for path := range c.fileInode[inode] {
		if occupied(path) {
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
	// A remap failure (id space exhausted) aborts the replay: reusing an
	// identifier would corrupt the tree worse than a failed commit.
	if err := remapOpCollisionsIndexed(meta, &op, plan, resolutions, cidx); err != nil {
		return err
	}
	handler, ok := opApplyHandlers[op.Type]
	if !ok {
		return fmt.Errorf("unknown op type %q", op.Type)
	}
	return handler(meta, op, plan, resolutions, cidx, now, path)
}

func applyFileStateOp(meta *RepoMetadata, op Op, plan *replayPlan, _ *[]ConflictResolution, cidx *collisionIndex, now int64, path string) error {
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

func applyMkdirOp(meta *RepoMetadata, op Op, _ *replayPlan, _ *[]ConflictResolution, cidx *collisionIndex, now int64, path string) error {
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

func applyDeleteFileOp(meta *RepoMetadata, _ Op, plan *replayPlan, _ *[]ConflictResolution, cidx *collisionIndex, _ int64, path string) error {
	target := plan.resolveLive(meta, path)
	cidx.delFile(target)
	meta.RemoveFile(target)
	return nil
}

func applyRmdirOp(meta *RepoMetadata, op Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex, _ int64, path string) error {
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

func applyRenameOp(meta *RepoMetadata, op Op, plan *replayPlan, _ *[]ConflictResolution, cidx *collisionIndex, now int64, _ string) error {
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
// the list (created under the old path after the rename was synthesized)
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
// Walks the child index iteratively with early exit (O(subtree), not
// O(tree)): renames are rare, but a full catalog scan per rename would
// still be quadratic on rename-heavy batches.
func subtreePopulated(meta *RepoMetadata, path string) bool {
	stack := []string{path}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		dirs, files := meta.DirectoryChildren(cur)
		if len(files) > 0 {
			return true
		}
		stack = append(stack, dirs...)
	}
	return false
}
