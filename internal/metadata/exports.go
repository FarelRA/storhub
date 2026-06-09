package metadata

import "time"

func (m *RepoMetadata) AllocateInode() uint64 {
	return m.allocateInode()
}

func InitializeNewFileIdentity(meta *RepoMetadata, file *FileMeta, now time.Time) {
	initializeNewFileIdentity(meta, file, now)
}

func PreserveFileIdentity(file *FileMeta, existing *FileMeta, now time.Time) {
	preserveFileIdentity(file, existing, now)
}

func ParseNumericReleaseTag(tag string) (int, bool) {
	return parseNumericReleaseTag(tag)
}
