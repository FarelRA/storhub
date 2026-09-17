package storage

import (
	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// synthesizeOpsFromIntents derives the op set for one metadata transaction
// from the intents the tracked mutators recorded while fn ran: only paths
// the transaction actually touched are classified, so synthesis is
// O(changes) instead of O(tree). Emission order is map order - ops are
// full-state assertions and batch replay is order-independent (see
// replayPlan), so the folded SET is the contract, not the sequence.
//
// The recorded intent pins each touched path's PRE-transaction state; the
// POST-transaction state is always read from the candidate tree, which by
// fold time has been sealed and per-file canonicalized (SealTransaction +
// SortFileChunks on the touched files). Ops are full-state assertions
// replayed onto arbitrary upstream trees, so this guarantees every op
// payload reflects the final normalized state without requiring a proof
// that sealing cannot alter a recorded snapshot.
//
// `before` is used only for the root-directory comparison: Root is an
// exported field mutated in place by design (TouchDirectory, root chmod),
// so it cannot flow through the tracked mutators; the check is O(1).
//
// The fold runs in fixed stages: classifyIntents (net-effect per touched
// path), pairRenames (inode pairing), emitDeletes, emitDirs, emitFiles,
// emitCatalog. Each stage reads the class plus the candidate; rename pairing
// consumes entries so later stages skip them.
func synthesizeOpsFromIntents(before, after *RepoMetadata, rec *metadata.IntentRecorder, cause string, now int64) []Op {
	stack := &opStack{}
	class := classifyIntents(before, after, rec)
	consumedFiles, consumedDirs := pairRenames(after, class, rec, stack, cause, now)
	emitDeletes(stack, class, cause, now)
	emitDirs(stack, class, consumedDirs, cause, now)
	emitFiles(stack, after, rec, consumedFiles, cause, now)
	emitCatalog(stack, after, rec, cause, now)
	return stack.ops
}

type dirChange struct {
	path  string
	entry DirMeta
	kind  OpType
}

// intentClass is the classified net effect of one transaction over the
// touched subset: original state from the intent (first touch wins), final
// state from the candidate.
type intentClass struct {
	removedFiles  map[string]FileMeta
	removedDirs   map[string]DirMeta
	addedDirs     map[string]DirMeta
	addedFiles    map[string]FileMeta
	recreatedDirs map[string]DirMeta // existed before AND after with a new inode
	dirChanges    []dirChange
}

// classifyIntents folds the recorded intents into per-path net effects. A
// key touched but net-unchanged (created then removed, or rewritten with an
// identical body) produces nothing: only the transaction's net effect is
// emitted.
func classifyIntents(before, after *RepoMetadata, rec *metadata.IntentRecorder) *intentClass {
	class := &intentClass{
		removedFiles:  map[string]FileMeta{},
		removedDirs:   map[string]DirMeta{},
		addedDirs:     map[string]DirMeta{},
		addedFiles:    map[string]FileMeta{},
		recreatedDirs: map[string]DirMeta{},
	}
	for path, intent := range rec.FileIntents() {
		entry, exists := after.Files()[path]
		switch {
		case intent.Existed && !exists:
			class.removedFiles[path] = intent.Old
		case !intent.Existed && exists:
			class.addedFiles[path] = entry
		}
	}
	for path, intent := range rec.DirIntents() {
		entry, exists := after.Dirs()[path]
		switch {
		case intent.Existed && !exists:
			class.removedDirs[path] = intent.Old
		case !intent.Existed && exists:
			class.addedDirs[path] = entry
		case intent.Existed && exists && isDirRecreate(intent.Old, entry):
			// Delete+recreate under one transaction with a new inode is a
			// literal recreation, never an in-place chmod: replay resolves
			// a setattr through the move table (stale reference) but
			// applies a mkdir literally, so only OpMkdir recreates the
			// path instead of overwriting a rename target that shares it.
			class.recreatedDirs[path] = entry
		case intent.Existed && exists && !dirEqual(intent.Old, entry, true):
			class.dirChanges = append(class.dirChanges, dirChange{path: path, entry: entry, kind: OpSetattr})
		}
	}
	if !dirEqual(before.Root, after.Root, true) {
		class.dirChanges = append(class.dirChanges, dirChange{path: "", entry: after.Root, kind: OpSetattr})
	}
	return class
}

// isDirRecreate reports whether a directory present before and after one
// transaction is a recreation (delete+mkdir) rather than an in-place change:
// the inode changed. Replay treats a setattr as a stale-reference rewrite
// but a mkdir as a literal assertion, so only the inode signal distinguishes
// them. Zero inodes are inconclusive (unnormalized trees): report no
// recreation and keep the historical setattr behavior.
func isDirRecreate(old, current DirMeta) bool {
	return old.Inode != 0 && current.Inode != 0 && old.Inode != current.Inode
}

// pairRenames pairs removed and added entries by inode into rename ops. A
// rename preserves the inode, so pairing confirms with a body comparison
// that ignores the ChangedAt bump a rename applies. Returns the rename
// targets consumed, so later stages skip them. Emission order is map order:
// replay is order-independent via replayPlan, so the folded SET is the
// contract, not the sequence.
func pairRenames(after *RepoMetadata, class *intentClass, rec *metadata.IntentRecorder, stack *opStack, cause string, now int64) (consumedFiles, consumedDirs map[string]bool) {
	type renamePair struct {
		from, to string
		file     *FileMeta
		dir      *DirMeta
	}
	var renames []renamePair
	consumedFiles = map[string]bool{}
	consumedDirs = map[string]bool{}
	// Pairing indexes are only meaningful when something was removed; a
	// pure create/import transaction skips them entirely.
	if len(class.removedFiles) > 0 {
		addedByInode := make(map[uint64][]string)
		for path, entry := range class.addedFiles {
			addedByInode[entry.Inode] = append(addedByInode[entry.Inode], path)
		}
		for fromPath, fromEntry := range class.removedFiles {
			for _, toPath := range addedByInode[fromEntry.Inode] {
				if consumedFiles[toPath] {
					continue
				}
				toEntry := after.Files()[toPath]
				if fileEqual(fromEntry, toEntry, true) {
					entry := toEntry.Clone()
					renames = append(renames, renamePair{from: fromPath, to: toPath, file: &entry})
					consumedFiles[toPath] = true
					delete(class.removedFiles, fromPath)
					break
				}
			}
		}
	}
	if len(class.removedDirs) > 0 || len(class.recreatedDirs) > 0 {
		// A recreated path's new inode is a fresh allocation, never a
		// rename target: pairing it would steal the recreation. The
		// allocator never reuses freed ids within a tree, so this guard
		// only fires for hand-built trees — defense in depth.
		recreatedInodes := make(map[uint64]struct{}, len(class.recreatedDirs))
		for _, entry := range class.recreatedDirs {
			recreatedInodes[entry.Inode] = struct{}{}
		}
		addedDirByInode := make(map[uint64]string, len(class.addedDirs))
		for path, entry := range class.addedDirs {
			if _, bad := recreatedInodes[entry.Inode]; bad {
				continue
			}
			addedDirByInode[entry.Inode] = path
		}
		// Rename sources are removed paths PLUS recreated paths (by
		// their pre-transaction record): a path renamed away and then
		// recreated under the same name pairs its OLD inode with the
		// new location, while the recreated entry still emits its own
		// mkdir carrying the candidate's record. Without this the
		// rename is lost and replay cannot converge: the move is never
		// recorded.
		removedPool := make(map[string]DirMeta, len(class.removedDirs)+len(class.recreatedDirs))
		for path, entry := range class.removedDirs {
			removedPool[path] = entry
		}
		for path := range class.recreatedDirs {
			if _, already := removedPool[path]; !already {
				if intent, ok := rec.DirIntents()[path]; ok && intent.Existed {
					removedPool[path] = intent.Old
				}
			}
		}
		for fromPath, fromEntry := range removedPool {
			toPath, ok := addedDirByInode[fromEntry.Inode]
			if !ok || consumedDirs[toPath] {
				continue
			}
			toEntry := class.addedDirs[toPath]
			if dirEqual(fromEntry, toEntry, true) {
				entry := toEntry.Clone()
				renames = append(renames, renamePair{from: fromPath, to: toPath, dir: &entry})
				consumedDirs[toPath] = true
				delete(class.removedDirs, fromPath)
				// A paired recreated source stays in recreatedDirs (its
				// mkdir still carries the candidate record) but must not
				// pair again; a removed source leaves removedDirs so no
				// delete is emitted for it. (Deleting the current range
				// key is safe in Go.)
				delete(removedPool, fromPath)
			}
		}
	}
	for _, rn := range renames {
		op := Op{Type: OpRename, Paths: []string{rn.from, rn.to}, Cause: cause, Timestamp: now}
		if rn.file != nil {
			op.File = rn.file
			op.Chunks = chunkRecordsFor(after, rn.file.Chunks)
		} else {
			op.Dir = rn.dir
			op.Members = renameMembers(after, rn.from, rn.to)
		}
		stack.append(op)
	}
	return consumedFiles, consumedDirs
}

// renameMembers records the subtree a directory rename covers, read from
// the candidate tree and expressed as from-paths: every file and dir path
// strictly under to, mapped back under from. Replay moves exactly these
// members, so entries created under the old path after the rename (by a
// later op in the same batch, or a recreation the fold emits separately)
// are not swept along. Collection order is map order; replay moves are
// independent key relocations, so no ordering is imposed.
func renameMembers(after *RepoMetadata, from, to string) []string {
	members := []string{}
	if from == "" || to == "" {
		return members
	}
	back := func(p string) {
		if p != to && shfs.IsParentOrSame(to, p) {
			members = append(members, from+p[len(to):])
		}
	}
	for p := range after.Files() {
		back(p)
	}
	for p := range after.Dirs() {
		back(p)
	}
	return members
}

// emitDeletes folds the surviving removals. Emission order is map order;
// replay order-independence comes from replayPlan (the doomed set), not
// from emission.
func emitDeletes(stack *opStack, class *intentClass, cause string, now int64) {
	delPaths := make([]string, 0, len(class.removedFiles)+len(class.removedDirs))
	for path := range class.removedFiles {
		delPaths = append(delPaths, path)
	}
	for path := range class.removedDirs {
		delPaths = append(delPaths, path)
	}
	for _, path := range delPaths {
		if entry, ok := class.removedFiles[path]; ok {
			stack.append(Op{Type: OpDeleteFile, Paths: []string{path}, Cause: cause, Timestamp: now, FreedChunks: len(entry.Chunks)})
			continue
		}
		stack.append(Op{Type: OpRmdir, Paths: []string{path}, Cause: cause, Timestamp: now})
	}
}

// emitDirs folds directory creates, recreations, and in-place changes.
// Recreated paths (new inode) emit literal OpMkdir, never OpSetattr; the
// root setattr (path "") is one member of dirChanges like any other.
func emitDirs(stack *opStack, class *intentClass, consumedDirs map[string]bool, cause string, now int64) {
	for _, ch := range class.dirChanges {
		entry := ch.entry.Clone()
		stack.append(Op{Type: ch.kind, Paths: []string{ch.path}, Cause: cause, Timestamp: now, Dir: &entry})
	}
	for path, entry := range class.recreatedDirs {
		clone := entry.Clone()
		stack.append(Op{Type: OpMkdir, Paths: []string{path}, Cause: cause, Timestamp: now, Dir: &clone})
	}
	addedDirPaths := make([]string, 0, len(class.addedDirs))
	for path := range class.addedDirs {
		if consumedDirs[path] {
			continue
		}
		addedDirPaths = append(addedDirPaths, path)
	}
	for _, path := range addedDirPaths {
		entry := class.addedDirs[path].Clone()
		stack.append(Op{Type: OpMkdir, Paths: []string{path}, Cause: cause, Timestamp: now, Dir: &entry})
	}
}

// emitFiles folds file creates and changes over the touched subset, skipping
// entries consumed as rename targets. A touched-but-identical entry emits
// nothing. File state ops (put AND setattr) always attach the chunk catalog
// records the entry references: without them a divergent chunk-ID reuse by
// a rival writer silently repoints our entry at the rival's bytes on rebase,
// and a cold replay onto a tree missing the record dangles.
func emitFiles(stack *opStack, after *RepoMetadata, rec *metadata.IntentRecorder, consumedFiles map[string]bool, cause string, now int64) {
	touchedFiles := make([]string, 0, len(rec.FileIntents()))
	for path := range rec.FileIntents() {
		touchedFiles = append(touchedFiles, path)
	}
	for _, path := range touchedFiles {
		if consumedFiles[path] {
			continue
		}
		entry, exists := after.Files()[path]
		if !exists {
			continue
		}
		intent := rec.FileIntents()[path]
		if intent.Existed && fileEqual(intent.Old, entry, true) {
			continue
		}
		kind := OpPutFile
		if intent.Existed && chunksEqual(intent.Old.Chunks, entry.Chunks) && intent.Old.Size == entry.Size && intent.Old.Symlink == entry.Symlink {
			kind = OpSetattr
		}
		clone := entry.Clone()
		op := Op{Type: kind, Paths: []string{path}, Cause: cause, Timestamp: now, File: &clone}
		op.Chunks = chunkRecordsFor(after, clone.Chunks)
		stack.append(op)
	}
}

// emitCatalog folds the release catalog over the touched tags (additions,
// changes, removals) and chunk-catalog shrinkage (DeleteChunk, including the
// prune path) into one merged op. AssetCount is derived (RecomputeStats
// rewrites it), so only CreatedAt is compared.
func emitCatalog(stack *opStack, after *RepoMetadata, rec *metadata.IntentRecorder, cause string, now int64) {
	touchedTags := make([]string, 0, len(rec.ReleaseIntents()))
	for tag := range rec.ReleaseIntents() {
		touchedTags = append(touchedTags, tag)
	}
	var removedTags []string
	for _, tag := range touchedTags {
		intent := rec.ReleaseIntents()[tag]
		ref, exists := after.Releases()[tag]
		switch {
		case !exists && intent.Existed:
			removedTags = append(removedTags, tag)
		case exists && (!intent.Existed || intent.Old.CreatedAt != ref.CreatedAt):
			clone := ref
			stack.append(Op{Type: OpRelease, Paths: []string{tag}, Tag: tag, Release: &clone, Cause: cause, Timestamp: now})
		}
	}
	for _, tag := range removedTags {
		stack.append(Op{Type: OpRelease, Paths: []string{tag}, Tag: tag, Cause: cause, Timestamp: now})
	}

	var removedChunks []int64
	for id, intent := range rec.ChunkIntents() {
		if !intent.Existed {
			continue
		}
		if _, ok := after.Chunks()[id]; ok {
			continue
		}
		removedChunks = append(removedChunks, id)
	}
	if len(removedChunks) > 0 {
		stack.append(Op{Type: OpChunkPrune, Cause: cause, Timestamp: now, RemovedChunks: removedChunks})
	}
}
