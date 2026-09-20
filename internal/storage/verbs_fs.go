package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// verbs_fs.go: filesystem verbs: create, mutate, read, stat, posix, xattr.

// CreateFileContext creates an empty file at filePath.
func (h *StorHub) CreateFileContext(ctx context.Context, project, filePath string) (*metadata.FileMeta, error) {
	return h.fsService().CreateFileContext(ctx, project, filePath)
}

// MkdirContext creates a directory and missing parents.
func (h *StorHub) MkdirContext(ctx context.Context, project, dirPath string) error {
	return h.fsService().MkdirContext(ctx, project, dirPath)
}

// UnlinkContext deletes the file at filePath.
func (h *StorHub) UnlinkContext(ctx context.Context, project, filePath string) error {
	return h.DeleteFileContext(ctx, project, filePath)
}

// RmdirContext removes an empty directory.
func (h *StorHub) RmdirContext(ctx context.Context, project, dirPath string, opts ...shfs.MutateOption) error {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return err
	}
	return h.fsService().RmdirContext(ctx, project, dirPath)
}

// RenameContext moves oldPath to newPath.
func (h *StorHub) RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...shfs.MutateOption) error {
	return h.fsService().RenameContext(ctx, project, oldPath, newPath, opts...)
}

// CopyContext duplicates srcPath to dstPath without re-uploading chunks.
func (h *StorHub) CopyContext(ctx context.Context, project, srcPath, dstPath string) error {
	return h.fsService().CopyContext(ctx, project, srcPath, dstPath)
}

// TruncateFileContext resizes a file, zero-filling growth.
func (h *StorHub) TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.fsService().TruncateFileContext(ctx, project, filePath, size)
}

// AppendFileContext adds data to the end of a file.
func (h *StorHub) AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.fsService().AppendFileContext(ctx, project, filePath, data)
}

// WriteFileAtContext writes data at an absolute offset.
func (h *StorHub) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	return h.fsService().WriteFileAtContext(ctx, project, filePath, offset, data)
}

// ReadFileAtContext reads length bytes at an absolute offset.
func (h *StorHub) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	result := make([]byte, length)
	n, err := h.ReadFileAtBufferContext(ctx, project, filePath, offset, result)
	if err != nil {
		return nil, err
	}
	return result[:n], nil
}

// ReadFileAtBufferContext reads into result and returns the byte count.
func (h *StorHub) ReadFileAtBufferContext(ctx context.Context, project, filePath string, offset int64, result []byte) (int, error) {
	if err := validateProject(project); err != nil {
		return 0, err
	}
	if err := shfs.ValidateAccessPathShape(filePath); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, errors.New("read offset and length must be non-negative")
	}
	repo, _, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return 0, err
	}
	cleanPath, traversed, err := shfs.ResolveAccessPath(repo, filePath, true)
	if err != nil {
		return 0, err
	}
	if cleanPath == "" {
		return 0, errors.New("file name is required")
	}
	if err := shfs.CheckTraversal(ctx, repo, traversed); err != nil {
		return 0, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return 0, fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanPath)
	}
	if err := shfs.CheckReadAccess(ctx, repo, cleanPath); err != nil {
		return 0, err
	}
	if offset > file.Size {
		return 0, io.EOF
	}
	if len(result) == 0 {
		return 0, nil
	}
	end := offset + int64(len(result))
	if end > file.Size {
		end = file.Size
	}
	segments := overlappingFileSegments(file, repo.Chunks(), offset, end)
	for _, segment := range segments {
		if err := h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end]); err != nil {
			return 0, err
		}
	}
	shfs.TouchFileAccessTime(ctx, h, project, cleanPath, h.config.Now().UnixNano())
	return int(end - offset), nil
}

// ReadPinnedFileContext reads bytes for a caller-held metadata snapshot -
// a file entry plus the chunk descriptors it referenced when captured,
// typically at FUSE open time. Later renames or unlinks of the source
// path cannot change what this returns: resolution never touches live
// metadata, and chunk assets are content-addressed. Access was already
// authorized when the snapshot was taken, so no permission re-check
// runs here, and atime is left untouched because a historical read must
// not refresh the live entry.
func (h *StorHub) ReadPinnedFileContext(ctx context.Context, project string, file *metadata.FileMeta, chunks map[int64]metadata.ChunkInfo, offset, length int64) ([]byte, error) {
	if file == nil {
		return nil, shfs.NotFound("pinned file")
	}
	if length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	if length == 0 || offset >= file.Size {
		return []byte{}, nil
	}
	result := make([]byte, length)
	end := offset + length
	if end > file.Size {
		end = file.Size
	}
	for _, segment := range overlappingFileSegments(file, chunks, offset, end) {
		if err := h.fillAssetRange(ctx, project, segment.chunk, result[segment.start:segment.end]); err != nil {
			return nil, err
		}
	}
	return result[:end-offset], nil
}

// StatPathContext stats one path.
func (h *StorHub) StatPathContext(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	return h.fsService().StatPathContext(ctx, project, targetPath)
}

// ReadDirContext lists one directory.
func (h *StorHub) ReadDirContext(ctx context.Context, project, dirPath string) ([]shfs.DirEntry, error) {
	return h.fsService().ReadDirContext(ctx, project, dirPath)
}

// StatFSContext aggregates project-wide counts.
func (h *StorHub) StatFSContext(ctx context.Context, project string) (*shfs.FSStats, error) {
	return h.fsService().StatFSContext(ctx, project)
}

// SymlinkContext points linkPath at target.
func (h *StorHub) SymlinkContext(ctx context.Context, project, target, linkPath string) (*metadata.FileMeta, error) {
	return h.posixService().SymlinkContext(ctx, project, target, linkPath)
}

// ReadlinkContext returns the target of linkPath.
func (h *StorHub) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	return h.posixService().ReadlinkContext(ctx, project, linkPath)
}

// LinkContext hard-links newPath to existingPath.
func (h *StorHub) LinkContext(ctx context.Context, project, existingPath, newPath string) (*metadata.FileMeta, error) {
	return h.posixService().LinkContext(ctx, project, existingPath, newPath)
}

// ChmodContext sets permission bits, clearing setuid on the way.
func (h *StorHub) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error {
	return h.posixService().ChmodContext(ctx, project, targetPath, mode)
}

// ChownContext sets owner ids, clearing setuid on the way.
func (h *StorHub) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error {
	return h.posixService().ChownContext(ctx, project, targetPath, uid, gid)
}

// ChtimesContext sets atime and mtime as Unix nanoseconds.
func (h *StorHub) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error {
	return h.posixService().ChtimesContext(ctx, project, targetPath, atime, mtime)
}

// ChtimesExplicitContext forwards utimensat-style trinary semantics:
// nil omits a timestamp, non-nil sets it exactly (epoch included).
func (h *StorHub) ChtimesExplicitContext(ctx context.Context, project, targetPath string, atime, mtime *time.Time) error {
	return h.posixService().ChtimesExplicitContext(ctx, project, targetPath, atime, mtime)
}

// SetXAttrContext stores one extended attribute.
func (h *StorHub) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte, mode ...shfs.XAttrMode) error {
	return h.posixService().SetXAttrContext(ctx, project, targetPath, attr, data, mode...)
}

// GetXAttrContext returns one extended attribute.
func (h *StorHub) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
	return h.posixService().GetXAttrContext(ctx, project, targetPath, attr)
}

// ListXAttrContext names every extended attribute on a path.
func (h *StorHub) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	return h.posixService().ListXAttrContext(ctx, project, targetPath)
}

// RemoveXAttrContext deletes one extended attribute.
func (h *StorHub) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error {
	return h.posixService().RemoveXAttrContext(ctx, project, targetPath, attr)
}

// ApplyMetadataPatchContext applies a metadata-only patch to a path.
func (h *StorHub) ApplyMetadataPatchContext(ctx context.Context, project, targetPath string, patch shfs.MetadataPatch) error {
	return h.posixService().ApplyMetadataPatchContext(ctx, project, targetPath, patch)
}
