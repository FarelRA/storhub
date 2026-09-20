package metadata

import (
	"bytes"
	"slices"
	"strconv"
	"strings"
)

// Normalize canonicalizes the tree: maps, entries, order, and stats.
func (m *RepoMetadata) Normalize(project string, now int64) {
	// Version is preserved (it records the document/layout the tree came from
	// or will become), never forced here: a legacy blob stays maxBlobVersion
	// until written as a split document, a split/new tree is maxMetadataVersion.
	m.Project = chooseNonEmpty(m.Project, project)
	m.normalizeRoot()
	if m.dirs == nil {
		m.dirs = make(map[string]DirMeta)
	}
	if m.files == nil {
		m.files = make(map[string]FileMeta)
	}
	if m.chunks == nil {
		m.chunks = make(map[int64]ChunkInfo)
	}
	if m.releases == nil {
		m.releases = make(map[string]ReleaseRef)
	}
	// Entry loops write back ONLY when normalization actually changed the
	// value: an already-normalized tree (the common per-transaction case)
	// then leaves the derived indexes and the size cache untouched, so no
	// rebuild or re-marshalling happens here.
	for path, dir := range m.dirs {
		original := dir
		dir.Normalize()
		if !dirMetaEqual(original, dir) ||
			(original.XAttrs == nil) != (dir.XAttrs == nil) {
			// The second clause persists the nil-collapse Normalize
			// guarantees: lenient equality alone would leave an
			// empty-but-non-nil map stored.
			m.dirs[path] = dir
			m.sizePutDir(path, putTransition(original, true, dir))
		}
	}
	for path, file := range m.files {
		original := file
		file.Normalize()
		if !fileMetaEqual(original, file) ||
			(original.Chunks == nil) != (file.Chunks == nil) ||
			(original.XAttrs == nil) != (file.XAttrs == nil) {
			m.files[path] = file
			m.sizePutFile(path, putTransition(original, true, file))
		}
	}
	m.sortFileChunksByOffset()
	for tag, ref := range m.releases {
		if ref.CreatedAt == 0 {
			original := ref
			ref.CreatedAt = now
			m.releases[tag] = ref
			m.sizePutRelease(tag, putTransition(original, true, ref))
		}
	}
	m.RecomputeStats()
	if m.LastMod == 0 {
		m.LastMod = now
	}
	// Indexes are rebuilt by RecomputeStats above; no second rebuild here.
}

// RecomputeStats rebuilds file counts, sizes, and asset counts from maps.
func (m *RepoMetadata) RecomputeStats() {
	totalFiles := 0
	totalSize := int64(0)
	assetCounts := make(map[string]int)

	for _, chunk := range m.chunks {
		if chunk.Release != "" {
			assetCounts[chunk.Release]++
		}
	}
	for _, file := range m.files {
		if file.Symlink == "" {
			totalFiles++
			totalSize += file.Size
		}
	}

	// pendingAssets is rebuilt from the chunk walk: counts that belong to
	// existing refs land in the refs above; counts for tags with no ref
	// survive here for a later EnsureRelease/PutRelease to drain. This is
	// the authoritative reset that keeps the incremental path exact.
	d := m.ensureDerived()
	d.pendingAssets = make(map[string]int)
	for tag := range m.releases {
		ref := m.releases[tag]
		if ref.AssetCount != assetCounts[tag] {
			original := ref
			ref.AssetCount = assetCounts[tag]
			m.releases[tag] = ref
			m.sizePutRelease(tag, putTransition(original, true, ref))
		}
	}
	for _, chunk := range m.chunks {
		if chunk.Release != "" {
			if _, ok := m.releases[chunk.Release]; !ok {
				d.pendingAssets[chunk.Release]++
			}
		}
	}

	m.TotalFiles = totalFiles
	m.TotalSize = totalSize
	if m.Version == 0 {
		m.Version = maxMetadataVersion
	}
	m.RebuildIndexes()
}

// PruneUnreferencedChunks drops chunk records that no file references
// anymore. Overwrites and deletions otherwise leave stale entries behind,
// and the catalog grows monotonically until metadata hits the size ceiling
// and every subsequent commit fails permanently. Callers must only prune at
// points where no retained history still needs the records (storhub prunes
// immediately before squashing git history, after purge has
// reclaimed the corresponding remote assets) - a rollback to an older
// revision restores its own chunk catalog wholesale.
//
// Removals go through DeleteChunk (not raw map deletes) so the tracked
// mutator stays the single chokepoint for chunk removal: the size section
// stays exact incrementally and a transaction's intent recorder sees the
// prune.
func (m *RepoMetadata) PruneUnreferencedChunks() int {
	referenced := make(map[int64]struct{})
	for _, file := range m.files {
		for _, id := range file.Chunks {
			referenced[id] = struct{}{}
		}
	}
	removed := 0
	for id := range m.chunks {
		if _, ok := referenced[id]; !ok {
			m.DeleteChunk(id)
			removed++
		}
	}
	return removed
}

// Normalize canonicalizes the directory entry in place.
func (d *DirMeta) Normalize() {
	// Mode 0 is an explicit deny-all (000), not an unset field: it
	// survives verbatim. Creation defaults live on the creation paths
	// (EnsureDirectory stamps them; the v3 to v4 migrator materializes
	// them for legacy entries), never here. See the explicit-zero
	// contract on FileMeta.Normalize.
	// Timestamps are NOT repaired here: the v5 contract is complete,
	// authoritative values (the stacked migrator completes legacy docs;
	// creation paths stamp real times). Zeros are real epoch values.
	d.XAttrs = normalizeXAttrs(d.XAttrs)
}

// Normalize canonicalizes the file entry in place under the
// explicit-zero contract: a stored Mode 0 means chmod 000, and Normalize
// preserves it. The unset sentinel for creates is handled one layer up:
// UpsertFile routes fresh nodes through initializeNewFileIdentityFields
// (which defaults a zero mode), EnsureDirectory stamps directory modes
// explicitly, and the stacked migrator materializes legacy zeros. Update
// and verbatim paths (WriteFileDirect, WriteDirDirect, ReplaceFile, and
// the family rewrites built on them) must never invent a mode, so an
// explicit 000 survives store, reload (omitempty round-trips 0 as
// absent and back to 0), and every later Normalize. Conversely,
// preserveFileIdentity still treats a zero mode on an INCOMING update
// as carry-over (keep the existing mode): modes change through the
// chmod path, not through update assembly.
//
// One known edge stays outside this file: re-insertion flows that
// Remove then Upsert (inode-family rebuilds) re-apply creation defaults
// to the re-inserted entry. A 000 file with hardlinks therefore widens
// on the next family rewrite; routing those re-inserts through
// WriteFileDirect (as UpdateFileFamily already does) closes it.
func (f *FileMeta) Normalize() {
	// See DirMeta.Normalize: zeros are authoritative values, never gaps
	// to repair here.
	if f.Chunks == nil {
		f.Chunks = make([]int64, 0)
	}
	// Stored chunk order is by data offset, a RepoMetadata-level invariant
	// enforced by sortFileChunksByOffset; sorting by id here would break it
	// for standalone callers.
	if f.Symlink != "" {
		f.Chunks = make([]int64, 0)
		f.Size = int64(len([]byte(f.Symlink)))
	}
	f.XAttrs = normalizeXAttrs(f.XAttrs)
}

// xAttrsEqual compares by length and content: nil and empty are EQUAL. Go's
// encoding/json omits both nil and len-0 maps under omitempty, so the two
// serialize identically and distinguishing them only causes spurious cache
// misses (and skipped canonical write-backs, which Normalize triggers
// explicitly instead).
func xAttrsEqual(a, b XAttrMap) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok || !bytes.Equal(v, bv) {
			return false
		}
	}
	return true
}

func dirMetaEqual(a, b DirMeta) bool {
	return a.CreatedAt == b.CreatedAt && a.ModifiedAt == b.ModifiedAt &&
		a.AccessedAt == b.AccessedAt && a.ChangedAt == b.ChangedAt &&
		a.Mode == b.Mode && a.UID == b.UID && a.GID == b.GID &&
		a.Inode == b.Inode && xAttrsEqual(a.XAttrs, b.XAttrs)
}

func fileMetaEqual(a, b FileMeta) bool {
	if a.Size != b.Size || a.Symlink != b.Symlink || a.UploadedAt != b.UploadedAt ||
		a.ModifiedAt != b.ModifiedAt || a.AccessedAt != b.AccessedAt ||
		a.ChangedAt != b.ChangedAt || a.Mode != b.Mode || a.UID != b.UID ||
		a.GID != b.GID || a.Inode != b.Inode {
		return false
	}
	// Chunks compare by content only (slices.Equal treats nil and empty as
	// equal): both serialize identically under omitempty, so the
	// distinction only causes spurious cache misses. Normalize triggers
	// canonical write-backs explicitly (see its loops), so collapsing here
	// cannot leave a non-canonical entry stored.
	if !slices.Equal(a.Chunks, b.Chunks) {
		return false
	}
	return xAttrsEqual(a.XAttrs, b.XAttrs)
}

// preserveFileIdentity carries the existing node's stable identity onto an
// updated entry. A type change (regular file <-> symlink) carries nothing.
func preserveFileIdentity(file *FileMeta, existing *FileMeta, now int64) {
	// A type change (regular file <-> symlink) replaces the whole node rather
	// than updating it: no identity carries over. In particular the old
	// symlink target must never leak onto a regular file - Normalize treats
	// any file with a symlink target as pure link data and would silently
	// discard the freshly written content.
	if (file.Symlink == "") != (existing.Symlink == "") {
		return
	}
	if file.Inode == 0 {
		file.Inode = existing.Inode
	}
	if file.Mode == 0 {
		file.Mode = existing.Mode
	}
	if file.UID == 0 {
		file.UID = existing.UID
	}
	if file.GID == 0 {
		file.GID = existing.GID
	}
	if file.UploadedAt == 0 {
		file.UploadedAt = existing.UploadedAt
		if file.UploadedAt == 0 {
			file.UploadedAt = now
		}
	}
	if file.ModifiedAt == 0 {
		file.ModifiedAt = now
		if file.ModifiedAt == 0 {
			file.ModifiedAt = existing.ModifiedAt
		}
		if file.ModifiedAt == 0 {
			file.ModifiedAt = file.UploadedAt
		}
	}
	if file.AccessedAt == 0 {
		file.AccessedAt = existing.AccessedAt
		if file.AccessedAt == 0 {
			file.AccessedAt = file.ModifiedAt
		}
	}
	if file.ChangedAt == 0 {
		file.ChangedAt = now
		if file.ChangedAt == 0 {
			file.ChangedAt = existing.ChangedAt
		}
		if file.ChangedAt == 0 {
			file.ChangedAt = file.ModifiedAt
		}
	}
	if len(file.XAttrs) == 0 && len(existing.XAttrs) > 0 {
		file.XAttrs = existing.XAttrs.Clone()
	}
}

// PreserveFileIdentity carries the existing node's stable identity onto an
// updated entry. Exported because POSIX update paths outside this package
// assemble entries before storing them through the tracked mutators; the
// carry-over rule stays owned here.
func PreserveFileIdentity(file *FileMeta, existing *FileMeta, now int64) {
	preserveFileIdentity(file, existing, now)
}

// initializeNewFileIdentity materializes a complete identity (inode, mode,
// owner, timestamps) for a newly created file entry.
func initializeNewFileIdentity(meta *RepoMetadata, file *FileMeta, now int64) {
	if file.Inode == 0 {
		file.Inode = meta.allocateInode()
	}
	initializeNewFileIdentityFields(file, now)
}

// InitializeNewFileIdentity materializes a complete identity for a newly
// created file entry against this tree. Exported for the same reason as
// InitializeNewFileIdentityFields: upload paths assemble entries before
// storing them; the counter rule in AllocateInode applies.
func InitializeNewFileIdentity(meta *RepoMetadata, file *FileMeta, now int64) {
	initializeNewFileIdentity(meta, file, now)
}

// initializeNewFileIdentityFields applies every creation default EXCEPT the
// inode: mode, owner, and the full timestamp set. Inode minting is reserved
// for initializeNewFileIdentity because the counter lives on exactly one
// authoritative RepoMetadata; stamping an inode against a throwaway clone
// (a readonly snapshot, a working copy) silently skips the counter bump and
// the next allocation re-issues the same inode.
func initializeNewFileIdentityFields(file *FileMeta, now int64) {
	if file.Mode == 0 {
		file.Mode = defaultFileMode(nodeKindOf(file))
	}
	// Owner IDs are NEVER materialized here: 0 legitimately means root,
	// so a zero value cannot double as "unset". Every creation path
	// provisions the owner explicitly before storing (OwnerIDsForCreate
	// for fresh entries, PreserveFileIdentity for updates). Stamping the
	// process user here used to silently reassign root-owned entries to
	// whoever ran the process (regression: TestCloneRangePermissions
	// passed or failed depending on the runner's UID).
	// Fresh nodes get complete timestamps at creation. This runs ONLY on
	// the new-node path (UpdateFileFamily writes the map directly), so an
	// explicit epoch on an existing entry can never be rewritten here.
	if file.UploadedAt == 0 {
		file.UploadedAt = now
	}
	if file.ModifiedAt == 0 {
		file.ModifiedAt = file.UploadedAt
	}
	if file.AccessedAt == 0 {
		file.AccessedAt = file.ModifiedAt
	}
	if file.ChangedAt == 0 {
		file.ChangedAt = file.ModifiedAt
	}
}

// InitializeNewFileIdentityFields applies every creation default EXCEPT the
// inode: mode, owner, and the full timestamp set. Exported because upload
// paths outside this package assemble entries before storing them; the
// inode itself is minted by initializeNewFileIdentity (via UpsertFile) so
// the counter rule above stays intact.
func InitializeNewFileIdentityFields(file *FileMeta, now int64) {
	initializeNewFileIdentityFields(file, now)
}

func (m *RepoMetadata) normalizeRoot() {
	m.normalizeRootFast()
	m.reconcileCounters()
}

// normalizeRootFast applies the O(1) root touch-ups (inode default, xattr
// normalization) without the O(files+dirs+chunks) counter
// reconciliation. normalizeRoot (load/normalize paths) adds the
// reconciliation; SealTransaction uses only this fast path so sealing stays
// O(1) per transaction. The root mode is never defaulted here: like every
// other entry, an explicit 000 survives (see the explicit-zero contract
// on FileMeta.Normalize); fresh trees stamp the default in
// NewRepoMetadata and the migrator materializes legacy zeros.
func (m *RepoMetadata) normalizeRootFast() {
	if m.Root.Inode == 0 {
		m.Root.Inode = 1
	}
	// Root timestamps are authoritative under v5 (see DirMeta.Normalize).
	m.Root.XAttrs = normalizeXAttrs(m.Root.XAttrs)
}

// allocateInode mints the next inode number and bumps the counter. It
// trusts the counter unconditionally; load paths reconcile it first via
// reconcileCounters.
func (m *RepoMetadata) allocateInode() uint64 {
	ino := m.NextInode
	m.NextInode++
	return ino
}

// AllocateInode mints a fresh inode against this tree. Call it ONLY on the
// tree that will be published (the UpdateRepoMetadataContext candidate, or a
// working copy swapped in on success) while holding the owner's
// authoritative lock. Minting against a throwaway clone silently skips the
// counter bump and the next allocation re-issues the same inode, so the
// counter lives on exactly one authoritative RepoMetadata per project.
func (m *RepoMetadata) AllocateInode() uint64 {
	return m.allocateInode()
}

// allocateChunkID mints the next chunk identifier and bumps the counter,
// mirroring allocateInode.
func (m *RepoMetadata) allocateChunkID() int64 {
	id := m.NextChunkID
	m.NextChunkID++
	return id
}

// AllocateChunkID mints a fresh chunk identifier against this tree. Same
// ownership rule as AllocateInode: the tree that will be published, under
// the authoritative lock. Rebase collision remapping and upload chunk-ID
// assignment need concrete IDs before any store call, which is why this
// cannot be folded into PutChunk.
func (m *RepoMetadata) AllocateChunkID() int64 {
	return m.allocateChunkID()
}

// parseNumericReleaseTag extracts the numeric part of a "v<N>" release tag.
func parseNumericReleaseTag(tag string) (int, bool) {
	trimmed := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(tag)), "v")
	if trimmed == "" || trimmed == "-" || trimmed[0] < '0' || trimmed[0] > '9' {
		return 0, false
	}
	n, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, false
	}
	return n, true
}

// ParseNumericReleaseTag extracts the numeric part of a "v<N>" release tag.
// Exported because release-rotation code outside this package walks the
// catalog by number; the parsing rule stays owned here.
func ParseNumericReleaseTag(tag string) (int, bool) {
	return parseNumericReleaseTag(tag)
}
