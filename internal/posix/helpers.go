// Package posix implements the metadata verbs on top of the fs contract.
package posix

import (
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// ApplyUploadIdentity stamps creation/update identity fields onto the file
// entry being staged. It deliberately does NOT mint an inode: the caller
// stages against a readonly snapshot whose inode counter is a throwaway.
// The authoritative allocation happens later, against the live metadata
// under its lock (InitializeNewFileIdentity), so the counter bumps exactly
// once and the next allocation can never re-issue this inode.
func ApplyUploadIdentity(name string, existing *meta.FileMeta, file *meta.FileMeta, now int64) {
	if existing != nil {
		ApplyUpdatedFileIdentity(name, file, existing, now)
		return
	}
	meta.InitializeNewFileIdentityFields(file, now)
}

// ApplyUpdatedFileIdentity stamps update identity fields onto file. The
// Symlink blank is load-bearing, not legacy compat: a regular-file update
// over a symlink entry must drop link-ness, otherwise Normalize discards
// the fresh content (the type-change guard lives in preserveFileIdentity).
func ApplyUpdatedFileIdentity(_ string, file *meta.FileMeta, existing *meta.FileMeta, now int64) {
	meta.PreserveFileIdentity(file, existing, now)
	file.Symlink = ""
	file.ModifiedAt = now
	file.ChangedAt = now
	if file.AccessedAt == 0 {
		if existing.AccessedAt != 0 {
			file.AccessedAt = existing.AccessedAt
		} else {
			file.AccessedAt = now
		}
	}
}

// ReplaceInodeFamily rewrites every hardlink sibling with the updated entry
// under single-writer semantics: Remove + WriteFileDirect in deterministic
// order, mirroring UpdateFileFamily. The old Remove + UpsertFile spelling
// sent siblings down the new-node path (RemoveFile erased the "existing"
// side), letting creation defaults overwrite preserved values, including
// authoritative epoch zeros. Each sibling keeps its own access time.
func ReplaceInodeFamily(repo *meta.RepoMetadata, name string, existing *meta.FileMeta, updated meta.FileMeta, now int64) {
	_ = now
	siblings := repo.FindFilesByInode(existing.Inode)
	if len(siblings) == 0 {
		repo.RemoveFile(name)
		repo.WriteFileDirect(name, updated)
		return
	}
	// Capture sibling access times BEFORE removing the entries; looking
	// them up afterwards always yields nil.
	atimes := make(map[string]int64, len(siblings))
	for _, sibName := range siblings {
		if f := repo.FindFile(sibName); f != nil {
			atimes[sibName] = f.AccessedAt
		}
	}
	for _, sibName := range siblings {
		repo.RemoveFile(sibName)
	}
	// Deterministic write order: ranging over the map above would commit
	// siblings in a random sequence per call. WriteFileDirect keeps every
	// field verbatim (no creation defaults), preserving epoch zeros.
	for _, sibName := range siblings {
		clone := updated.Clone()
		if at, ok := atimes[sibName]; ok {
			// Verbatim per-sibling preserve, epoch zero included: the
			// sibling's own stamp always wins over the updated entry.
			clone.AccessedAt = at
		}
		repo.WriteFileDirect(sibName, clone)
	}
}
