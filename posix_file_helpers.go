package storhub

import "time"

func applyUploadIdentity(meta *RepoMetadata, existing *FileMetadata, file *FileMetadata, now time.Time) {
	if existing != nil {
		applyUpdatedFileIdentity(file, existing, now)
		return
	}
	initializeNewFileIdentity(meta, file, now)
}

func applyUpdatedFileIdentity(file *FileMetadata, existing *FileMetadata, now time.Time) {
	preserveFileIdentity(file, existing, now)
	file.Kind = NodeKindFile
	file.SymlinkTarget = ""
	file.ModifiedAt = now.UTC()
	file.ChangedAt = now.UTC()
	if file.AccessedAt.IsZero() {
		file.AccessedAt = chooseNonZeroTime(existing.AccessedAt, now)
	}
}

func replaceInodeFamily(meta *RepoMetadata, existing *FileMetadata, updated FileMetadata, now time.Time) {
	siblings := meta.FindFilesByInode(existing.Inode)
	if len(siblings) == 0 {
		meta.RemoveFile(existing.Name)
		meta.UpsertFile(updated, now)
		return
	}
	for _, sibling := range siblings {
		meta.RemoveFile(sibling.Name)
	}
	for _, sibling := range siblings {
		clone := updated.Clone()
		clone.Name = sibling.Name
		clone.AccessedAt = chooseNonZeroTime(sibling.AccessedAt, updated.AccessedAt, now)
		meta.UpsertFile(clone, now)
	}
}

func touchInodeFamilyChangedAt(meta *RepoMetadata, inode uint64, now time.Time) error {
	return updateFileFamily(meta, inode, func(current *FileMetadata) {
		current.ChangedAt = now.UTC()
	})
}
