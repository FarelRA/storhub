package posix

import (
	"os"
	"time"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func ApplyUploadIdentity(repo *meta.RepoMetadata, existing *meta.FileMetadata, file *meta.FileMetadata, now time.Time) {
	if existing != nil {
		ApplyUpdatedFileIdentity(file, existing, now)
		return
	}
	meta.InitializeNewFileIdentity(repo, file, now)
}

func ApplyUpdatedFileIdentity(file *meta.FileMetadata, existing *meta.FileMetadata, now time.Time) {
	meta.PreserveFileIdentity(file, existing, now)
	file.Kind = meta.NodeKindFile
	file.SymlinkTarget = ""
	file.ModifiedAt = now.UTC()
	file.ChangedAt = now.UTC()
	if file.AccessedAt.IsZero() {
		file.AccessedAt = ChooseNonZeroTime(existing.AccessedAt, now)
	}
}

func ReplaceInodeFamily(repo *meta.RepoMetadata, existing *meta.FileMetadata, updated meta.FileMetadata, now time.Time) {
	siblings := repo.FindFilesByInode(existing.Inode)
	if len(siblings) == 0 {
		repo.RemoveFile(existing.Name)
		repo.UpsertFile(updated, now)
		return
	}
	for _, sibling := range siblings {
		repo.RemoveFile(sibling.Name)
	}
	for _, sibling := range siblings {
		clone := updated.Clone()
		clone.Name = sibling.Name
		clone.AccessedAt = ChooseNonZeroTime(sibling.AccessedAt, updated.AccessedAt, now)
		repo.UpsertFile(clone, now)
	}
}

func DefaultOwnerIDs() (uint32, uint32) {
	return uint32(os.Getuid()), uint32(os.Getgid())
}

func ChooseNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value.UTC()
		}
	}
	return time.Time{}
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
