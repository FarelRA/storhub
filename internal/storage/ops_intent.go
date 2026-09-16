package storage

import (
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
func synthesizeOpsFromIntents(before, after *RepoMetadata, rec *metadata.IntentRecorder, cause string, now int64) []Op {
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
	addedFiles := map[string]FileMeta{}

	// Classify the touched subset: original state from the intent (first
	// touch wins), final state from the candidate. A key touched but
	// net-unchanged (created then removed, or rewritten with an identical
	// body) produces nothing - the same net-effect rule the diff applies.
	for path, intent := range rec.FileIntents() {
		entry, exists := after.Files()[path]
		switch {
		case intent.Existed && !exists:
			removedFiles[path] = intent.Old
		case !intent.Existed && exists:
			addedFiles[path] = entry
		}
	}
	for path, intent := range rec.DirIntents() {
		entry, exists := after.Dirs()[path]
		switch {
		case intent.Existed && !exists:
			removedDirs[path] = intent.Old
		case !intent.Existed && exists:
			addedDirs[path] = entry
		case intent.Existed && exists && !dirEntriesEquivalent(intent.Old, entry):
			dirChanges = append(dirChanges, dirChange{path: path, entry: entry, kind: OpSetattr})
		}
	}
	if !dirEntriesEquivalent(before.Root, after.Root) {
		dirChanges = append(dirChanges, dirChange{path: "", entry: after.Root, kind: OpSetattr})
	}

	// Rename pairing: identical to the diff's, but over the touched subset.
	// A rename preserves the inode, so pair removed and added entries by
	// inode and confirm with a body comparison that ignores the ChangedAt
	// bump a rename applies. Depth-descending emission keeps replay correct
	// (children move before their parent).
	type renamePair struct {
		from, to string
		file     *FileMeta
		dir      *DirMeta
	}
	var renames []renamePair
	consumedFiles := map[string]bool{}
	consumedDirs := map[string]bool{}
	// Pairing indexes are only meaningful when something was removed; a
	// pure create/import transaction skips them entirely.
	if len(removedFiles) > 0 {
		addedByInode := make(map[uint64][]string)
		for path, entry := range addedFiles {
			addedByInode[entry.Inode] = append(addedByInode[entry.Inode], path)
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
	}
	if len(removedDirs) > 0 {
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
	}
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
	for _, path := range delPaths {
		if entry, ok := removedFiles[path]; ok {
			stack.append(Op{Type: OpDeleteFile, Paths: []string{path}, Cause: cause, Timestamp: now, FreedChunks: len(entry.Chunks)})
			continue
		}
		stack.append(Op{Type: OpRmdir, Paths: []string{path}, Cause: cause, Timestamp: now})
	}

	// Directory creates/changes, shallowest first; root setattr (path "")
	// sorts first naturally.
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
	for _, path := range addedDirPaths {
		entry := addedDirs[path].Clone()
		stack.append(Op{Type: OpMkdir, Paths: []string{path}, Cause: cause, Timestamp: now, Dir: &entry})
	}

	// File creates/changes over the touched subset, skipping entries
	// consumed as rename targets. Emission order is map order: ops are
	// full-state assertions and replay is order-independent, so the folded
	// SET is the contract, not the sequence.
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
		if intent.Existed && fileEntriesEquivalent(intent.Old, entry) {
			continue
		}
		kind := OpPutFile
		if intent.Existed && chunksEqual(intent.Old.Chunks, entry.Chunks) && intent.Old.Size == entry.Size && intent.Old.Symlink == entry.Symlink {
			kind = OpSetattr
		}
		clone := entry.Clone()
		op := Op{Type: kind, Paths: []string{path}, Cause: cause, Timestamp: now, File: &clone}
		if kind == OpPutFile {
			op.Chunks = chunkRecordsFor(after, clone.Chunks)
		}
		stack.append(op)
	}

	// Release catalog over the touched tags: additions/changes and removals.
	// AssetCount is derived (RecomputeStats rewrites it), so only CreatedAt
	// is compared - the same rule the diff applies to the whole catalog.
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
			clone := ref.Clone()
			stack.append(Op{Type: OpRelease, Paths: []string{tag}, Tag: tag, Release: &clone, Cause: cause, Timestamp: now})
		}
	}
	for _, tag := range removedTags {
		stack.append(Op{Type: OpRelease, Paths: []string{tag}, Tag: tag, Cause: cause, Timestamp: now})
	}

	// Chunk catalog shrinkage (DeleteChunk, including the prune path): one
	// merged op with the ids the transaction removed from the catalog.
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

	return stack.ops
}
