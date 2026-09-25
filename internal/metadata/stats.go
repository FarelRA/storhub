package metadata

import "sort"

// Incremental stat maintenance. TotalFiles/TotalSize and the per-release
// AssetCount are derived quantities that RecomputeStats computes from
// scratch; the tracked mutators keep them exact incrementally so a
// transaction never pays the O(files+chunks+releases) walk. Wholesale
// construction paths (load, migrate, rebase replay, LoadTree) still call
// RecomputeStats once, which also reconciles any drift.

// statsFilePut adjusts TotalFiles/TotalSize for one file entry transition.
// Symlinks are excluded from both counters - the same rule RecomputeStats
// applies.
func (m *RepoMetadata) statsFilePut(t entryTransition[FileMeta]) {
	if t.hadOld && t.old.Symlink == "" {
		m.TotalFiles--
		m.TotalSize -= t.old.Size
	}
	if t.hasCur && t.cur.Symlink == "" {
		m.TotalFiles++
		m.TotalSize += t.cur.Size
	}
}

// statsFileRemove adjusts TotalFiles/TotalSize for one removed file entry.
func (m *RepoMetadata) statsFileRemove(old FileMeta) {
	if old.Symlink == "" {
		m.TotalFiles--
		m.TotalSize -= old.Size
	}
}

// countAsset adjusts one release's derived AssetCount after a chunk
// catalog transition. When the ref exists it is updated in place (with the
// size delta); when it does not - a chunk put landing before its release
// op replays, or after a release removal - the delta waits in pendingAssets
// and the next EnsureRelease/PutRelease for that tag drains it into the
// new ref. Counts are therefore exact in every order; RecomputeStats
// rebuilds pending from the chunk walk as the authoritative reset.
func (m *RepoMetadata) countAsset(tag string, delta int) {
	if tag == "" || delta == 0 {
		return
	}
	if ref, ok := m.releases[tag]; ok {
		original := ref
		ref.AssetCount += delta
		m.releases[tag] = ref
		m.sizePutRelease(tag, putTransition(original, true, ref))
		return
	}
	d := m.ensureDerived()
	if d.pendingAssets == nil {
		d.pendingAssets = make(map[string]int)
	}
	d.pendingAssets[tag] += delta
}

// sortIDsByOffset orders chunk ids by data offset in place. It is the one
// backing sort behind SortFileChunks (single file) and sortAllFileChunksByOffset
// (whole tree); missing ids resolve to the zero ChunkInfo, matching the
// chunksOffsetSorted fast-path check.
func sortIDsByOffset(chunks map[int64]ChunkInfo, ids []int64) {
	sort.SliceStable(ids, func(i, j int) bool {
		return chunks[ids[i]].Offset < chunks[ids[j]].Offset
	})
}

// SealTransaction stamps the per-transaction bookkeeping on a candidate
// tree. O(1): the candidate's entries are already normalized (the source
// tree was normalized and the mutators initialize new entries), its stats
// were maintained incrementally by the mutators, and its counters by
// allocateInode/allocateChunkID - so the full Normalize/RecomputeStats
// walk (entry loops, chunk sort, counter reconciliation, index rebuild)
// is wholesale-construction work, not mutation work. The root touch-up is
// the shared counters-free fast path of normalizeRoot, never the O(N)
// counter reconciliation.
func (m *RepoMetadata) SealTransaction(project string, now int64) {
	m.Project = chooseNonEmpty(m.Project, project)
	m.normalizeRootFast()
	if m.Version == 0 {
		m.Version = maxMetadataVersion
	}
	if m.LastMod == 0 {
		m.LastMod = now
	}
}

// SortFileChunks sorts one file's chunk-id list by data offset, maintaining
// the size cache (the id order is part of the entry's serialized bytes) and
// recording the intent (a standalone call on an otherwise-untouched path in
// a transaction must still reach the fold). The transaction calls this for
// the files a mutation touched; Normalize's whole-tree sort remains for
// wholesale construction paths.
func (m *RepoMetadata) SortFileChunks(path string) {
	path = normalizeStoredPath(path)
	file, ok := m.files[path]
	if !ok || len(file.Chunks) < 2 || chunksOffsetSorted(m.chunks, file.Chunks) {
		return
	}
	original := file
	sorted := append([]int64(nil), file.Chunks...)
	sortIDsByOffset(m.chunks, sorted)
	file.Chunks = sorted
	m.files[path] = file
	m.sizePutFile(path, putTransition(original, true, file))
	m.recordFilePut(path, original, true)
}
