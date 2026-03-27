package metadata

import "time"

func (m *RepoMetadata) AllocateInode() uint64 {
	return m.allocateInode()
}

func (m *RepoMetadata) RebuildIndexes() {
	m.rebuildIndexes()
}

func InitializeNewFileIdentity(meta *RepoMetadata, file *FileMetadata, now time.Time) {
	initializeNewFileIdentity(meta, file, now)
}

func PreserveFileIdentity(file *FileMetadata, existing *FileMetadata, now time.Time) {
	preserveFileIdentity(file, existing, now)
}

func ParseNumericReleaseTag(tag string) (int, bool) {
	return parseNumericReleaseTag(tag)
}
