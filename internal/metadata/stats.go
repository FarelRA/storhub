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
func (m *RepoMetadata) statsFilePut(old FileMeta, existed bool, cur FileMeta) {
	if existed && old.Symlink == "" {
		m.TotalFiles--
		m.TotalSize -= old.Size
	}
	if cur.Symlink == "" {
		m.TotalFiles++
		m.TotalSize += cur.Size
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
		m.sizePutRelease(tag, original, true, ref)
		return
	}
	d := m.ensureDerived()
	if d.pendingAssets == nil {
		d.pendingAssets = make(map[string]int)
	}
	d.pendingAssets[tag] += delta
}

// SealTransaction stamps the per-transaction bookkeeping on a candidate
// tree. O(1): the candidate's entries are already normalized (the source
// tree was normalized and the mutators initialize new entries), its stats
// were maintained incrementally by the mutators, and its counters by
// allocateInode/allocateChunkID - so the full Normalize/RecomputeStats
// walk (entry loops, chunk sort, counter reconciliation, index rebuild)
// is wholesale-construction work, not mutation work.
func (m *RepoMetadata) SealTransaction(project string, now int64) {
	m.Project = chooseNonEmpty(m.Project, project)
	if m.Root.Inode == 0 {
		m.Root.Inode = 1
	}
	if m.Root.Mode == 0 {
		m.Root.Mode = defaultDirMode()
	}
	m.Root.XAttrs = normalizeXAttrs(m.Root.XAttrs)
	if m.Version == 0 {
		m.Version = maxMetadataVersion
	}
	if m.LastMod == 0 {
		m.LastMod = now
	}
}

// SortFileChunks sorts one file's chunk-id list by data offset, maintaining
// the size cache (the id order is part of the entry's serialized bytes). The
// transaction calls this for the files a mutation touched; Normalize's
// whole-tree sort remains for wholesale construction paths.
func (m *RepoMetadata) SortFileChunks(path string) {
	path = normalizeStoredPath(path)
	file, ok := m.files[path]
	if !ok || len(file.Chunks) < 2 || chunksOffsetSorted(m.chunks, file.Chunks) {
		return
	}
	original := file
	sorted := append([]int64(nil), file.Chunks...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return m.chunks[sorted[i]].Offset < m.chunks[sorted[j]].Offset
	})
	file.Chunks = sorted
	m.files[path] = file
	m.sizePutFile(path, original, true, file)
}
