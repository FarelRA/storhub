package posix

import (
	"os"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func ApplyUploadIdentity(repo *meta.RepoMetadata, name string, existing *meta.FileMeta, file *meta.FileMeta, now int64) {
	if existing != nil {
		ApplyUpdatedFileIdentity(name, file, existing, now)
		return
	}
	meta.InitializeNewFileIdentity(repo, file, now)
}

func ApplyUpdatedFileIdentity(name string, file *meta.FileMeta, existing *meta.FileMeta, now int64) {
	meta.PreserveFileIdentity(file, existing, now)
	file.Symlink = ""
	file.ModifiedAt = now
	file.ChangedAt = now
	if file.AccessedAt == 0 {
		file.AccessedAt = ChooseNonZeroTime(existing.AccessedAt, now)
	}
}

func ReplaceInodeFamily(repo *meta.RepoMetadata, name string, existing *meta.FileMeta, updated meta.FileMeta, now int64) {
	siblings := repo.FindFilesByInode(existing.Inode)
	if len(siblings) == 0 {
		repo.RemoveFile(name)
		repo.UpsertFile(name, updated, now)
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
	for _, sibName := range siblings {
		clone := updated.Clone()
		if at, ok := atimes[sibName]; ok && at != 0 {
			clone.AccessedAt = ChooseNonZeroTime(at, updated.AccessedAt, now)
		}
		repo.UpsertFile(sibName, clone, now)
	}
}

func DefaultOwnerIDs() (uint32, uint32) {
	return uint32(os.Getuid()), uint32(os.Getgid())
}

func ChooseNonZeroTime(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func CloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
