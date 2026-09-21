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
func (h *StorHub) CreateFileContext(ctx context.Context, project, filePath string) (result *metadata.FileMeta, err error) {
	started := h.logOpStart(project, "create-file", "path", filePath)
	defer func() {
		size := int64(0)
		if result != nil {
			size = result.Size
		}
		h.logOpFinish(project, "create-file", started, err, "path", filePath, "size", size)
	}()
	result, err = h.fsService().CreateFileContext(ctx, project, filePath)
	return result, err
}

// MkdirContext creates a directory and missing parents.
func (h *StorHub) MkdirContext(ctx context.Context, project, dirPath string) (err error) {
	started := h.logOpStart(project, "mkdir", "path", dirPath)
	defer func() { h.logOpFinish(project, "mkdir", started, err, "path", dirPath) }()
	err = h.fsService().MkdirContext(ctx, project, dirPath)
	return err
}

// UnlinkContext deletes the file at filePath.
func (h *StorHub) UnlinkContext(ctx context.Context, project, filePath string) (err error) {
	started := h.logOpStart(project, "unlink", "path", filePath)
	defer func() { h.logOpFinish(project, "unlink", started, err, "path", filePath) }()
	err = h.DeleteFileContext(ctx, project, filePath)
	return err
}

// RmdirContext removes an empty directory.
func (h *StorHub) RmdirContext(ctx context.Context, project, dirPath string, opts ...shfs.MutateOption) (err error) {
	started := h.logOpStart(project, "rmdir", "path", dirPath)
	defer func() { h.logOpFinish(project, "rmdir", started, err, "path", dirPath) }()
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return err
	}
	ctx = gateRevisionFromOpts(ctx, opts)
	err = h.fsService().RmdirContext(ctx, project, dirPath)
	return err
}

// RenameContext moves oldPath to newPath.
func (h *StorHub) RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...shfs.MutateOption) (err error) {
	started := h.logOpStart(project, "rename", "path", oldPath, "new_path", newPath)
	defer func() {
		h.logOpFinish(project, "rename", started, err, "path", oldPath, "new_path", newPath)
	}()
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return err
	}
	ctx = gateRevisionFromOpts(ctx, opts)
	err = h.fsService().RenameContext(ctx, project, oldPath, newPath, opts...)
	return err
}

// CopyContext duplicates srcPath to dstPath without re-uploading chunks.
func (h *StorHub) CopyContext(ctx context.Context, project, srcPath, dstPath string) (err error) {
	started := h.logOpStart(project, "copy", "src", srcPath, "dst", dstPath)
	defer func() {
		h.logOpFinish(project, "copy", started, err, "src", srcPath, "dst", dstPath)
	}()
	err = h.fsService().CopyContext(ctx, project, srcPath, dstPath)
	return err
}

// TruncateFileContext resizes a file, zero-filling growth.
func (h *StorHub) TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (result *metadata.FileMeta, err error) {
	started := h.logOpStart(project, "truncate-file", "path", filePath, "size", size)
	defer func() {
		resultSize := int64(0)
		if result != nil {
			resultSize = result.Size
		}
		h.logOpFinish(project, "truncate-file", started, err, "path", filePath, "size", size, "result_size", resultSize)
	}()
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	ctx = gateRevisionFromOpts(ctx, opts)
	result, err = h.fsService().TruncateFileContext(ctx, project, filePath, size)
	return result, err
}

// AppendFileContext adds data to the end of a file.
func (h *StorHub) AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (result *metadata.FileMeta, err error) {
	started := h.logOpStart(project, "append-file", "path", filePath, "size", len(data))
	defer func() {
		resultSize := int64(0)
		if result != nil {
			resultSize = result.Size
		}
		h.logOpFinish(project, "append-file", started, err, "path", filePath, "size", len(data), "result_size", resultSize)
	}()
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	ctx = gateRevisionFromOpts(ctx, opts)
	result, err = h.fsService().AppendFileContext(ctx, project, filePath, data)
	return result, err
}

// WriteFileAtContext writes data at an absolute offset.
func (h *StorHub) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (result *metadata.FileMeta, err error) {
	started := h.logOpStart(project, "write-file-at", "path", filePath, "offset", offset, "size", len(data))
	defer func() {
		resultSize := int64(0)
		if result != nil {
			resultSize = result.Size
		}
		h.logOpFinish(project, "write-file-at", started, err, "path", filePath, "offset", offset, "size", len(data), "result_size", resultSize)
	}()
	if err := h.enforceExpectedRevision(ctx, project, opts); err != nil {
		return nil, err
	}
	ctx = gateRevisionFromOpts(ctx, opts)
	result, err = h.fsService().WriteFileAtContext(ctx, project, filePath, offset, data)
	return result, err
}

// ReadFileAtContext reads length bytes at an absolute offset.
func (h *StorHub) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) (out []byte, err error) {
	started := h.logOpStart(project, "read-file-at", "path", filePath, "offset", offset, "length", length)
	defer func() {
		h.logOpFinish(project, "read-file-at", started, err, "path", filePath, "offset", offset, "length", length, "bytes", len(out))
	}()
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
	out = result[:n]
	return out, nil
}

// ReadFileAtBufferContext reads into result and returns the byte count.
func (h *StorHub) ReadFileAtBufferContext(ctx context.Context, project, filePath string, offset int64, result []byte) (n int, err error) {
	started := h.logOpStart(project, "read-file-at-buffer", "path", filePath, "offset", offset, "length", len(result))
	defer func() {
		h.logOpFinish(project, "read-file-at-buffer", started, err, "path", filePath, "offset", offset, "length", len(result), "bytes", n)
	}()
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
func (h *StorHub) ReadPinnedFileContext(ctx context.Context, project string, file *metadata.FileMeta, chunks map[int64]metadata.ChunkInfo, offset, length int64) (out []byte, err error) {
	started := h.logOpStart(project, "read-pinned-file", "offset", offset, "length", length)
	defer func() {
		size := int64(0)
		if file != nil {
			size = file.Size
		}
		h.logOpFinish(project, "read-pinned-file", started, err, "offset", offset, "length", length, "size", size, "bytes", len(out))
	}()
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
	out = result[:end-offset]
	return out, nil
}

// StatPathContext stats one path.
func (h *StorHub) StatPathContext(ctx context.Context, project, targetPath string) (result *shfs.EntryInfo, err error) {
	started := h.logOpStart(project, "stat-path", "path", targetPath)
	defer func() {
		h.logOpFinish(project, "stat-path", started, err, "path", targetPath)
	}()
	result, err = h.fsService().StatPathContext(ctx, project, targetPath)
	return result, err
}

// ReadDirContext lists one directory.
func (h *StorHub) ReadDirContext(ctx context.Context, project, dirPath string) (result []shfs.DirEntry, err error) {
	started := h.logOpStart(project, "read-dir", "path", dirPath)
	defer func() {
		h.logOpFinish(project, "read-dir", started, err, "path", dirPath, "count", len(result))
	}()
	result, err = h.fsService().ReadDirContext(ctx, project, dirPath)
	return result, err
}

// StatFSContext aggregates project-wide counts.
func (h *StorHub) StatFSContext(ctx context.Context, project string) (result *shfs.FSStats, err error) {
	started := h.logOpStart(project, "stat-fs")
	defer func() { h.logOpFinish(project, "stat-fs", started, err) }()
	result, err = h.fsService().StatFSContext(ctx, project)
	return result, err
}

// SymlinkContext points linkPath at target.
func (h *StorHub) SymlinkContext(ctx context.Context, project, target, linkPath string) (result *metadata.FileMeta, err error) {
	started := h.logOpStart(project, "symlink", "path", linkPath, "target", target)
	defer func() {
		h.logOpFinish(project, "symlink", started, err, "path", linkPath, "target", target)
	}()
	result, err = h.posixService().SymlinkContext(ctx, project, target, linkPath)
	return result, err
}

// ReadlinkContext returns the target of linkPath.
func (h *StorHub) ReadlinkContext(ctx context.Context, project, linkPath string) (result string, err error) {
	started := h.logOpStart(project, "readlink", "path", linkPath)
	defer func() {
		h.logOpFinish(project, "readlink", started, err, "path", linkPath, "target", result)
	}()
	result, err = h.posixService().ReadlinkContext(ctx, project, linkPath)
	return result, err
}

// LinkContext hard-links newPath to existingPath.
func (h *StorHub) LinkContext(ctx context.Context, project, existingPath, newPath string) (result *metadata.FileMeta, err error) {
	started := h.logOpStart(project, "link", "existing", existingPath, "new_path", newPath)
	defer func() {
		h.logOpFinish(project, "link", started, err, "existing", existingPath, "new_path", newPath)
	}()
	result, err = h.posixService().LinkContext(ctx, project, existingPath, newPath)
	return result, err
}

// ChmodContext sets permission bits, clearing setuid on the way.
func (h *StorHub) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) (err error) {
	started := h.logOpStart(project, "chmod", "path", targetPath, "mode", mode)
	defer func() {
		h.logOpFinish(project, "chmod", started, err, "path", targetPath, "mode", mode)
	}()
	err = h.posixService().ChmodContext(ctx, project, targetPath, mode)
	return err
}

// ChownContext sets owner ids, clearing setuid on the way.
func (h *StorHub) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) (err error) {
	started := h.logOpStart(project, "chown", "path", targetPath, "uid", uid, "gid", gid)
	defer func() {
		h.logOpFinish(project, "chown", started, err, "path", targetPath, "uid", uid, "gid", gid)
	}()
	err = h.posixService().ChownContext(ctx, project, targetPath, uid, gid)
	return err
}

// ChtimesContext sets atime and mtime as Unix nanoseconds.
func (h *StorHub) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) (err error) {
	started := h.logOpStart(project, "chtimes", "path", targetPath, "atime", atime, "mtime", mtime)
	defer func() {
		h.logOpFinish(project, "chtimes", started, err, "path", targetPath, "atime", atime, "mtime", mtime)
	}()
	err = h.posixService().ChtimesContext(ctx, project, targetPath, atime, mtime)
	return err
}

// ChtimesExplicitContext forwards utimensat-style trinary semantics:
// nil omits a timestamp, non-nil sets it exactly (epoch included).
func (h *StorHub) ChtimesExplicitContext(ctx context.Context, project, targetPath string, atime, mtime *time.Time) (err error) {
	started := h.logOpStart(project, "chtimes-explicit", "path", targetPath)
	defer func() {
		h.logOpFinish(project, "chtimes-explicit", started, err, "path", targetPath)
	}()
	err = h.posixService().ChtimesExplicitContext(ctx, project, targetPath, atime, mtime)
	return err
}

// SetXAttrContext stores one extended attribute.
func (h *StorHub) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte, mode ...shfs.XAttrMode) (err error) {
	started := h.logOpStart(project, "set-xattr", "path", targetPath, "attr", attr, "size", len(data))
	defer func() {
		h.logOpFinish(project, "set-xattr", started, err, "path", targetPath, "attr", attr, "size", len(data))
	}()
	err = h.posixService().SetXAttrContext(ctx, project, targetPath, attr, data, mode...)
	return err
}

// GetXAttrContext returns one extended attribute.
func (h *StorHub) GetXAttrContext(ctx context.Context, project, targetPath, attr string) (result []byte, err error) {
	started := h.logOpStart(project, "get-xattr", "path", targetPath, "attr", attr)
	defer func() {
		h.logOpFinish(project, "get-xattr", started, err, "path", targetPath, "attr", attr, "size", len(result))
	}()
	result, err = h.posixService().GetXAttrContext(ctx, project, targetPath, attr)
	return result, err
}

// ListXAttrContext names every extended attribute on a path.
func (h *StorHub) ListXAttrContext(ctx context.Context, project, targetPath string) (result []string, err error) {
	started := h.logOpStart(project, "list-xattr", "path", targetPath)
	defer func() {
		h.logOpFinish(project, "list-xattr", started, err, "path", targetPath, "count", len(result))
	}()
	result, err = h.posixService().ListXAttrContext(ctx, project, targetPath)
	return result, err
}

// RemoveXAttrContext deletes one extended attribute.
func (h *StorHub) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) (err error) {
	started := h.logOpStart(project, "remove-xattr", "path", targetPath, "attr", attr)
	defer func() {
		h.logOpFinish(project, "remove-xattr", started, err, "path", targetPath, "attr", attr)
	}()
	err = h.posixService().RemoveXAttrContext(ctx, project, targetPath, attr)
	return err
}

// ApplyMetadataPatchContext applies a metadata-only patch to a path.
func (h *StorHub) ApplyMetadataPatchContext(ctx context.Context, project, targetPath string, patch shfs.MetadataPatch) (err error) {
	started := h.logOpStart(project, "apply-metadata-patch", "path", targetPath)
	defer func() {
		h.logOpFinish(project, "apply-metadata-patch", started, err, "path", targetPath)
	}()
	err = h.posixService().ApplyMetadataPatchContext(ctx, project, targetPath, patch)
	return err
}
