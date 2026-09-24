package fs

import (
	"context"
	"errors"
	"fmt"
	meta "github.com/FarelRA/storhub/internal/metadata"
	"sort"
	"syscall"
)

// TruncateFileContext resizes the file at filePath to size bytes.
func (s *Service) TruncateFileContext(ctx context.Context, project, filePath string, size int64) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "truncate", true, []any{"path", filePath, "size", size}, func() error {
		// truncate(2) has open() semantics: a final symlink is followed to its
		// target.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		if size < 0 {
			return errors.New("truncate size must be non-negative")
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, filePath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if size == file.Size {
			// POSIX: even a no-op truncate updates mtime/ctime and, for
			// non-admin callers, clears setuid+setgid (see
			// SanitizeWrittenFileModeForContext).
			now := s.backend.Now()
			if _, err := s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
				// Re-resolve against the live transaction state: the
				// pre-transaction walk above is fast-fail only. A
				// concurrent rename/replace of a symlink component must
				// fail closed here, not authorize against the old chain.
				liveClean, liveTraversed, err := StatResolveTracked(repo, filePath)
				if err != nil {
					return err
				}
				if err := CheckWalkResolved(ctx, repo, liveTraversed); err != nil {
					return err
				}
				if err := CheckWriteAccessResolved(ctx, repo, liveClean, liveTraversed); err != nil {
					return err
				}
				current := repo.FindFile(liveClean)
				if current == nil {
					return s.backend.FileNotFound(liveClean)
				}
				clone := current.Clone()
				clone.Mode = SanitizeWrittenFileModeForContext(ctx, clone.Mode)
				clone.ModifiedAt = now
				clone.ChangedAt = now
				repo.ReplaceFile(liveClean, clone)
				// Return the live clone, not a re-sanitized pre-transaction
				// snapshot: a concurrent chmod between the two loads must
				// not leave the stored and returned modes diverged.
				result = &clone
				return nil
			}, fmt.Sprintf("storhub: truncate touch %s", cleanPath)); err != nil {
				return err
			}
			return nil
		}
		if size < file.Size {
			result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, size, file.Size-size, nil)
			return err
		}
		result, err = s.zeroExtendFile(ctx, project, filePath, size)
		return err
	})
	return result, err
}

// maxZeroExtendBytes caps how much a single zeroExtendFile call may grow a
// file. The extension uploads real zero bytes in ONE batched patch (a
// single metadata transaction and release resolution) instead of looping
// per chunk: without a cap, `truncate -s 1T` would issue ~1M uploads with
// no sparse representation. Requests beyond the cap fail with EFBIG rather
// than burning unbounded backend work. True hole-aware chunks (zero ranges
// without assets) need a storage-layer format change and are NOT done here.
//
// Boundary contract: CLI truncate, REST truncate, and FUSE setattr-grow all
// funnel through this path, so every surface reports the same EFBIG for
// over-cap growth (a deliberate deviation from truncate(2), which grows
// sparsely to arbitrary size). The REST status mapping for EFBIG and the CLI
// help text naming the limit live on those layers, not here.
const maxZeroExtendBytes = 16 << 20

// zeroExtendFile grows a file to targetSize with a single zero-filled
// range patch through the backend's patch verb. filePath is the user path:
// it is re-resolved against a fresh snapshot here so callers never hand a
// stale resolution chain across the reload window.
func (s *Service) zeroExtendFile(ctx context.Context, project, filePath string, targetSize int64) (*meta.FileMeta, error) {
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, err
	}
	cleanPath, traversed, err := StatResolveTracked(repo, filePath)
	if err != nil {
		return nil, err
	}
	file := repo.FindFile(cleanPath)
	if file == nil {
		return nil, s.backend.FileNotFound(cleanPath)
	}
	if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
		return nil, err
	}
	if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
		return nil, err
	}
	if file.Size >= targetSize {
		return file, nil
	}
	if targetSize-file.Size > maxZeroExtendBytes {
		return nil, syscall.EFBIG
	}
	zeros := make([]byte, targetSize-file.Size)
	return s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, zeros)
}

// AppendFileContext appends data to the file at filePath.
func (s *Service) AppendFileContext(ctx context.Context, project, filePath string, data []byte) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "append", true, []any{"path", filePath, "bytes", len(data)}, func() error {
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		// append has open() semantics: a final symlink is followed.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, filePath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, file.Size, 0, data)
		return err
	})
	return result, err
}

// WriteFileAtContext writes data at offset in the file at filePath.
func (s *Service) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte) (result *meta.FileMeta, err error) {
	err = s.withOp(project, "write-at", true, []any{"path", filePath, "offset", offset, "bytes", len(data)}, func() error {
		// pwrite has open() semantics: a final symlink is followed.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, filePath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if offset < 0 {
			return errors.New("write offset must be non-negative")
		}
		if len(data) == 0 {
			// Pure no-op by contract: a zero-length pwrite carries no bytes
			// to store, so there is nothing to sanitize or stamp. Mode and
			// timestamps flow through the next non-empty write or an
			// explicit truncate/chmod instead.
			clone := file.Clone()
			result = &clone
			return nil
		}
		if offset > file.Size {
			// The hole is zero-filled in one capped patch, then the real
			// data lands at offset.
			if _, err := s.zeroExtendFile(ctx, project, filePath, offset); err != nil {
				return err
			}
			repo, _, err = s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
			if err != nil {
				return err
			}
			// Re-resolve after the fill: the extension reloaded state, so
			// the pre-transaction chain above is stale past this point.
			cleanPath, traversed, err = StatResolveTracked(repo, filePath)
			if err != nil {
				return err
			}
			if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
				return err
			}
			if err := CheckWriteAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
				return err
			}
			file = repo.FindFile(cleanPath)
			if file == nil {
				return s.backend.FileNotFound(cleanPath)
			}
			result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, offset, 0, data)
			return err
		}
		deleteSize := int64(len(data))
		if capN := file.Size - offset; deleteSize > capN {
			deleteSize = capN
		}
		result, err = s.backend.PatchFileWithMetadataContext(ctx, project, cleanPath, repo, file, offset, deleteSize, data)
		return err
	})
	return result, err
}

// ReadFileAtContext reads length bytes at offset from the file at filePath.
func (s *Service) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) (result []byte, err error) {
	err = s.withOp(project, "read-at", false, []any{"path", filePath, "offset", offset, "length", length}, func() error {
		// pread has open() semantics: a final symlink is followed.
		if err := ValidateAccessPathShape(filePath); err != nil {
			return err
		}
		if offset < 0 || length < 0 {
			return errors.New("read offset and length must be non-negative")
		}
		repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
		if err != nil {
			return err
		}
		cleanPath, traversed, err := StatResolveTracked(repo, filePath)
		if err != nil {
			return err
		}
		if err := CheckWalkResolved(ctx, repo, traversed); err != nil {
			return err
		}
		file := repo.FindFile(cleanPath)
		if file == nil {
			return s.backend.FileNotFound(cleanPath)
		}
		if err := CheckReadAccessResolved(ctx, repo, cleanPath, traversed); err != nil {
			return err
		}
		if offset >= file.Size {
			// POSIX read(2) at or past EOF returns 0 bytes, not an error;
			// surfacing io.EOF here mapped to EIO at the FUSE boundary.
			result = []byte{}
			return nil
		}
		if length == 0 {
			result = []byte{}
			return nil
		}
		// Clamp before adding: a REST-supplied length near MaxInt64 would
		// overflow `offset + length` negative and panic the make below.
		if length > file.Size-offset {
			length = file.Size - offset
		}
		end := offset + length
		result = make([]byte, end-offset)
		chunks := repo.FileChunks(cleanPath)
		startIndex := sort.Search(len(chunks), func(i int) bool {
			return chunks[i].Offset+chunks[i].Size > offset
		})
		for _, chunk := range chunks[startIndex:] {
			chunkEnd := chunk.Offset + chunk.Size
			if chunk.Offset >= end {
				break
			}
			if chunkEnd <= offset || chunk.Size == 0 {
				continue
			}
			start := max(offset, chunk.Offset)
			stop := min(end, chunkEnd)
			segment := chunk
			segment.Offset = start
			segment.AssetOffset = chunk.AssetOffset + (start - chunk.Offset)
			segment.Size = stop - start
			dst := result[start-offset : stop-offset]
			if err := s.backend.FillAssetRangeContext(ctx, project, segment, dst); err != nil {
				return err
			}
		}
		TouchFileAccessTime(ctx, s.backend, project, cleanPath, s.backend.Now())
		return nil
	})
	return result, err
}
