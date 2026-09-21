package fusefs

import (
	"context"
	"os"
	"strings"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

func (h *pcHub) DownloadFileContext(_ context.Context, _ string, target, dest string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return err
	}
	return os.WriteFile(dest, append([]byte(nil), f.data...), 0o600)
}

func (h *pcHub) ReadFileAtContext(_ context.Context, _ string, target string, off, length int64) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	if off < 0 || length < 0 {
		return nil, syscall.EINVAL
	}
	if length == 0 {
		return []byte{}, nil
	}
	if off >= int64(len(f.data)) {
		return []byte{}, nil
	}
	end := off + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return append([]byte(nil), f.data[off:end]...), nil
}

// pcApplyEdits splices RangeEdits into data in order, growing with zero
// fill when an edit starts past EOF.
func pcApplyEdits(data []byte, edits []shfs.RangeEdit) ([]byte, error) {
	out := data
	for _, e := range edits {
		if e.Start < 0 || e.DeleteSize < 0 {
			return nil, syscall.EINVAL
		}
		if e.Start > int64(len(out)) {
			out = append(out, make([]byte, e.Start-int64(len(out)))...)
		}
		end := e.Start + e.DeleteSize
		if end > int64(len(out)) {
			end = int64(len(out))
		}
		nb := make([]byte, 0, int64(len(out))-(end-e.Start)+int64(len(e.Data)))
		nb = append(nb, out[:e.Start]...)
		nb = append(nb, e.Data...)
		nb = append(nb, out[end:]...)
		out = nb
	}
	return out, nil
}

func (h *pcHub) PatchFileContext(_ context.Context, _ string, target string, off, del int64, edit []byte, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	nb, err := pcApplyEdits(f.data, []shfs.RangeEdit{{Start: off, DeleteSize: del, Data: edit}})
	if err != nil {
		return nil, err
	}
	f.data = nb
	h.clearPrivsLocked(f)
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) PatchFileRangesContext(_ context.Context, _ string, target string, edits []shfs.RangeEdit) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	nb, err := pcApplyEdits(f.data, edits)
	if err != nil {
		return nil, err
	}
	f.data = nb
	h.clearPrivsLocked(f)
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) ReplaceFileContext(_ context.Context, _ string, target, inputPath string, _ ...shfs.MutateOption) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, err
	}
	f.data = data
	h.clearPrivsLocked(f)
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) LoadRepoMetadataReadonlyContext(_ context.Context, project string) (*meta.RepoMetadata, string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buildRepoLocked(project), "pcstatic", nil
}

func (h *pcHub) ReadPinnedFileContext(_ context.Context, _ string, file *meta.FileMeta, _ map[int64]meta.ChunkInfo, offset, length int64) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if file == nil {
		return nil, syscall.EIO
	}
	f, ok := h.byInode[file.Inode]
	if !ok {
		return nil, shfs.NotFound("")
	}
	if offset < 0 || length < 0 {
		return nil, syscall.EINVAL
	}
	if length == 0 {
		return []byte{}, nil
	}
	if offset >= int64(len(f.data)) {
		return []byte{}, nil
	}
	end := offset + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return append([]byte(nil), f.data[offset:end]...), nil
}

func (h *pcHub) UpdateRepoMetadataContext(_ context.Context, project string, apply func(*meta.RepoMetadata) error, _ string) (*meta.RepoMetadata, error) {
	// Unreachable from the FUSE layer (which only reads metadata through
	// the readonly loader); behave like the stub hub and apply to a
	// throwaway copy.
	h.mu.Lock()
	repo := h.buildRepoLocked(project)
	h.mu.Unlock()
	if err := apply(repo); err != nil {
		return nil, err
	}
	return repo, nil
}

func (h *pcHub) RewriteFileRangesWithMetadataContext(_ context.Context, _ string, target, inputPath string, _ *meta.RepoMetadata, _ *meta.FileMeta, logicalSize int64, ranges []ByteRange) (*meta.FileMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, err := h.resolveLocked(target)
	if err != nil {
		return nil, err
	}
	snap, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, err
	}
	// The snapshot only carries the planned ranges; splice exactly those
	// spans, then reconcile the size.
	nb := append([]byte(nil), f.data...)
	for _, r := range ranges {
		if r.Start < 0 || r.End < r.Start {
			return nil, syscall.EINVAL
		}
		if r.End > int64(len(nb)) {
			nb = append(nb, make([]byte, r.End-int64(len(nb)))...)
		}
		srcEnd := r.End
		if srcEnd > int64(len(snap)) {
			srcEnd = int64(len(snap))
		}
		if r.Start < srcEnd {
			copy(nb[r.Start:srcEnd], snap[r.Start:srcEnd])
		}
	}
	if logicalSize >= 0 {
		nb2 := make([]byte, logicalSize)
		copy(nb2, nb)
		nb = nb2
	}
	f.data = nb
	h.clearPrivsLocked(f)
	h.touchLocked(f)
	return h.fileMetaLocked(f), nil
}

func (h *pcHub) RenameContext(_ context.Context, _ string, oldPath, newPath string, _ ...shfs.MutateOption) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// The noReplace option is enforced by the FUSE layer pre-check; the
	// option value itself is uninspectable outside the fs package, so a
	// concurrent noReplace race is not atomically closed here. The table
	// exercises noReplace single-threaded, where the pre-check decides.
	if oldPath == newPath {
		if _, ok := h.files[oldPath]; ok {
			return nil
		}
		if _, ok := h.dirs[oldPath]; ok {
			return nil
		}
		return shfs.NotFound(oldPath)
	}
	if _, ok := h.dirs[oldPath]; ok {
		if _, ok := h.files[newPath]; ok {
			if err := h.removeFileLocked(newPath); err != nil {
				return err
			}
		} else if d, ok := h.dirs[newPath]; ok {
			_ = d
			prefix := newPath + "/"
			for p := range h.files {
				if strings.HasPrefix(p, prefix) {
					return shfs.NotEmpty(newPath)
				}
			}
			for p := range h.dirs {
				if strings.HasPrefix(p, prefix) {
					return shfs.NotEmpty(newPath)
				}
			}
			delete(h.dirs, newPath)
		} else if _, ok := h.dirs[pcParent(newPath)]; !ok {
			return shfs.NotFound(newPath)
		}
		h.dirs[newPath] = h.dirs[oldPath]
		delete(h.dirs, oldPath)
		oldPrefix := oldPath + "/"
		newPrefix := newPath + "/"
		for p, f := range h.files {
			if strings.HasPrefix(p, oldPrefix) {
				delete(h.files, p)
				h.files[newPrefix+strings.TrimPrefix(p, oldPrefix)] = f
			}
		}
		for p, d := range h.dirs {
			if p != newPath && strings.HasPrefix(p, oldPrefix) {
				delete(h.dirs, p)
				h.dirs[newPrefix+strings.TrimPrefix(p, oldPrefix)] = d
			}
		}
		return nil
	}
	f, ok := h.files[oldPath]
	if !ok {
		return shfs.NotFound(oldPath)
	}
	if _, ok := h.files[newPath]; ok {
		if err := h.removeFileLocked(newPath); err != nil {
			return err
		}
	} else if _, ok := h.dirs[newPath]; ok {
		return shfs.IsDirectory(newPath)
	} else if _, ok := h.dirs[pcParent(newPath)]; !ok {
		return shfs.NotFound(newPath)
	}
	delete(h.files, oldPath)
	h.files[newPath] = f
	return nil
}

// removeFileLocked drops a name while keeping its inode record for open
// handles. Callers hold h.mu.
func (h *pcHub) removeFileLocked(p string) error {
	if _, ok := h.files[p]; !ok {
		return shfs.NotFound(p)
	}
	delete(h.files, p)
	return nil
}
