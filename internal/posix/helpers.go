package posix

import (
	"os"
	"time"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

func ApplyUploadIdentity(repo *meta.RepoMetadata, name string, existing *meta.FileMeta, file *meta.FileMeta, now time.Time) {
	if existing != nil {
		ApplyUpdatedFileIdentity(name, file, existing, now)
		return
	}
	meta.InitializeNewFileIdentity(repo, file, now)
}

func ApplyUpdatedFileIdentity(name string, file *meta.FileMeta, existing *meta.FileMeta, now time.Time) {
	meta.PreserveFileIdentity(file, existing, now)
	file.Symlink = ""
	file.ModifiedAt = now.UTC()
	file.ChangedAt = now.UTC()
	if file.AccessedAt.IsZero() {
		file.AccessedAt = ChooseNonZeroTime(existing.AccessedAt, now)
	}
}

func ReplaceInodeFamily(repo *meta.RepoMetadata, name string, existing *meta.FileMeta, updated meta.FileMeta, now time.Time) {
	siblings := repo.FindFilesByInode(existing.Inode)
	if len(siblings) == 0 {
		repo.RemoveFile(name)
		repo.UpsertFile(name, updated, now)
		return
	}
	for _, sibName := range siblings {
		repo.RemoveFile(sibName)
	}
	for _, sibName := range siblings {
		clone := updated.Clone()
		var sibFile *meta.FileMeta
		if f := repo.FindFile(sibName); f != nil {
			sibFile = f
		}
		if sibFile != nil {
			clone.AccessedAt = ChooseNonZeroTime(sibFile.AccessedAt, updated.AccessedAt, now)
		}
		repo.UpsertFile(sibName, clone, now)
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
