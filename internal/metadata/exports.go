package metadata

func (m *RepoMetadata) AllocateInode() uint64 {
	return m.allocateInode()
}

func InitializeNewFileIdentity(meta *RepoMetadata, file *FileMeta, now int64) {
	initializeNewFileIdentity(meta, file, now)
}

func PreserveFileIdentity(file *FileMeta, existing *FileMeta, now int64) {
	preserveFileIdentity(file, existing, now)
}

func ParseNumericReleaseTag(tag string) (int, bool) {
	return parseNumericReleaseTag(tag)
}
