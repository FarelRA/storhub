package cli

// POSIX conformance adapter: drives the real cobra CLI (App) in-process so
// the shared table in internal/test executes against CLI commands.
//
// Mapping (honest, gaps fail loudly):
//   CreateFile      -> `touch` (never truncates, so non-exclusive create
//                      is idempotent) with `--exclusive` for O_EXCL (atomic
//                      create gate in storage, no check-then-act); perm
//                      bits ignored, CLI has no mode flag
//   Open            -> `stat --json` (+ `touch` to create missing for every
//                      mode except OpenReadOnly) with `truncate 0` for
//                      OpenTruncate; links resolved through readlink
//   Handle cursors  -> adapter state (allowed by the harness contract);
//                      PRead/Read via `cat`, Write/PWrite via `write`,
//                      append-mode Write via `append`, Truncate via
//                      `truncate`, Sync via `sync`
//   Stat            -> `stat --json` following a final symlink through
//                      `readlink` (lstat semantics in, stat semantics out)
//   ReadRange       -> resolved path via `cat` plus client-side windowing
//                      (the CLI exposes no ranged-read flag); range error
//                      semantics enforced here
//   Append          -> `append` (missing path stays ErrNotFound, like real)
//   Truncate        -> `truncate` (zero-filling growth in the backend)
//   Chmod/Chown     -> `chmod` / `chown` (-1 keeps an id)
//   Utimens         -> `touch --mtimens/--atimens` (ns precision through
//                      the fake; the real backend stores seconds)
//   Unlink/Rename   -> `rm` / `mv` with --noreplace for RENAME_NOREPLACE
//                      (atomic in the storage transaction)
//   Mkdir/Rmdir     -> `mkdir` / `rm -r`
//   Symlink         -> `symlink`; Readlink -> `readlink` (ErrInvalid on
//                      non-links, ELOOP past the hop cap)
//   Sync            -> `sync` (project drain, the --sync flag standalone)
//   Revision        -> `stat --json` ChangedAt token;
//                      CompareAndWrite -> `write --expectedrevision`
//                      (stale tokens answer ErrPrecondition with Actual)
//
// Backend: the per-App one-shot hub seam (app.seams.newHub, the same
// seam app_test.go sets) is pointed at an in-memory fake on every fresh
// App the surface creates. Every byte the adapter sees still travels
// through a real cobra command RunE path; the fake only stands in for
// the networked GitHub storage behind the CLI.
//
// Refused (fail loudly, documented cause): unlink-while-open and
// rename-while-open need open-file descriptions that keep serving an
// unlinked/renamed inode, but every CLI read/write re-resolves its path
// per invocation, so a handle cannot outlive its name. Returning cached
// bytes would fake POSIX instead of implementing it.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/test"
	"github.com/FarelRA/storhub/storhub"
)

var (
	_ test.Surface = (*cliPOSIXSurface)(nil)
	_ test.Handle  = (*cliHandle)(nil)
)

// ---------------------------------------------------------------------------
// In-memory fake hub behind the CLI seam.
// ---------------------------------------------------------------------------

// pcFile is one stored regular file. Timestamps are nanoseconds since the
// epoch (the fake preserves ns precision so touch round-trips exactly;
// atime/mtime are independent fields).
type pcFile struct {
	data  []byte
	mode  uint32
	uid   uint32
	gid   uint32
	mtime int64
	atime int64
	// ino is the open-time identity for session commit resolution,
	// assigned lazily at first session open (rename preserves it by
	// moving the struct; unlink drops it with the map entry).
	ino uint64
}

// pcFakeHub implements hubClient with textbook in-memory semantics and
// shfs/syscall-style errors, mirroring what the real backend reports
// (NotFound on missing append/write targets, AlreadyExists on duplicate
// mkdir, lstat-style StatPath with ELOOP-free... no: loops report
// syscall.ELOOP via the adapter's own link follower, since the fake
// returns link entries exactly like the real lstat backend).
type pcFakeHub struct {
	mu    sync.Mutex
	files map[string]*pcFile
	links map[string]string
	// linkIno carries symlink identity alongside links: lstat of a link
	// reports its own inode, rename moves it, unlink drops it — the
	// same lifecycle as file inodes, in a parallel map so link reads
	// keep their shape.
	linkIno map[string]uint64
	dirs    map[string]bool
	clock   int64
	// Session emulation: open handles with pinned snapshots, staged
	// writes, identity-resolved commit (rename followed, unlink
	// discarded), mirroring the real session manager.
	sessions    map[string]*pcSession
	nextSession int
	nextIno     uint64
}

func newPCFakeHub() *pcFakeHub {
	return &pcFakeHub{
		files:   make(map[string]*pcFile),
		links:   make(map[string]string),
		linkIno: make(map[string]uint64),
		dirs:    map[string]bool{"": true},
		clock:   1700000000000000000,
	}
}

func (h *pcFakeHub) tick() int64 {
	h.clock++
	return h.clock
}

// allocInoLocked mints a fresh inode. Real backends never reuse an
// inode while any handle may reference it; the fake's counter only
// moves forward, so recycled names always observe a new identity and
// open-file pins (adapter h.ino, session s.ino) can tell them apart.
// Caller holds h.mu.
func (h *pcFakeHub) allocInoLocked() uint64 {
	h.nextIno++
	return h.nextIno
}

// fileInoLocked returns the file's identity, assigning one on first
// touch for entries predating eager allocation. Caller holds h.mu.
func (h *pcFakeHub) fileInoLocked(f *pcFile) uint64 {
	if f.ino == 0 {
		f.ino = h.allocInoLocked()
	}
	return f.ino
}

// linkInoLocked returns the symlink's own (lstat) identity, assigning
// on first touch like fileInoLocked. Caller holds h.mu.
func (h *pcFakeHub) linkInoLocked(p string) uint64 {
	if ino := h.linkIno[p]; ino != 0 {
		return ino
	}
	ino := h.allocInoLocked()
	h.linkIno[p] = ino
	return ino
}

// parentOf returns the parent key of a cleaned relative path ("" = root).
func pcParentOf(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[:i]
	}
	return ""
}

func (h *pcFakeHub) ensureParentsLocked(p string) {
	for dir := pcParentOf(p); dir != ""; dir = pcParentOf(dir) {
		if h.dirs[dir] {
			break
		}
		h.dirs[dir] = true
	}
}

func (h *pcFakeHub) UploadFileContext(_ context.Context, _, remotePath, localPath string) (*storhub.FileMetadata, error) {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(remotePath, "/")
	if h.dirs[p] {
		return nil, fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	h.ensureParentsLocked(p)
	if f, ok := h.files[p]; ok {
		cp := append([]byte(nil), data...)
		f.data = cp
		f.mtime = h.tick()
		f.atime = f.mtime
		return &storhub.FileMetadata{Size: int64(len(cp)), Mode: f.mode, Inode: h.fileInoLocked(f)}, nil
	}
	now := h.tick()
	cp := append([]byte(nil), data...)
	nf := &pcFile{data: cp, mode: 0o644, mtime: now, atime: now}
	nf.ino = h.allocInoLocked()
	h.files[p] = nf
	delete(h.links, p)
	delete(h.linkIno, p)
	return &storhub.FileMetadata{Size: int64(len(cp)), Mode: 0o644, Inode: nf.ino}, nil
}

func (h *pcFakeHub) ReplaceFileContext(ctx context.Context, project, remotePath, localPath string, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.mu.Lock()
	_, isFile := h.files[strings.TrimPrefix(remotePath, "/")]
	isDir := h.dirs[strings.TrimPrefix(remotePath, "/")]
	h.mu.Unlock()
	if isDir {
		return nil, fmt.Errorf("%w: %s", shfs.ErrIsDirectory, remotePath)
	}
	if !isFile {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, remotePath)
	}
	return h.UploadFileContext(ctx, project, remotePath, localPath)
}

func (h *pcFakeHub) DownloadFileContext(_ context.Context, _, remotePath, localPath string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(remotePath, "/")
	if h.dirs[p] {
		return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	f, ok := h.files[p]
	if !ok {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	return os.WriteFile(localPath, append([]byte(nil), f.data...), 0o644)
}

func (h *pcFakeHub) ReadDirContext(_ context.Context, _, dir string) ([]storhub.DirEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(dir, "/")
	if p != "" && !h.dirs[p] {
		if _, ok := h.files[p]; ok {
			return nil, fmt.Errorf("%w: %s", shfs.ErrNotDirectory, p)
		}
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	prefix := p
	if prefix != "" {
		prefix += "/"
	}
	var out []storhub.DirEntry
	for name := range h.dirs {
		if name == p || !strings.HasPrefix(name, prefix) || strings.Contains(strings.TrimPrefix(name, prefix), "/") {
			continue
		}
		base := strings.TrimPrefix(name, prefix)
		if base == "" {
			continue
		}
		out = append(out, storhub.DirEntry{Name: base, Path: name, IsDir: true, Mode: 0o755})
	}
	for name, f := range h.files {
		if !strings.HasPrefix(name, prefix) || strings.Contains(strings.TrimPrefix(name, prefix), "/") {
			continue
		}
		base := strings.TrimPrefix(name, prefix)
		out = append(out, storhub.DirEntry{Name: base, Path: name, Size: int64(len(f.data)), Mode: f.mode})
	}
	return out, nil
}

func (h *pcFakeHub) StatPathContext(_ context.Context, _, targetPath string) (*storhub.EntryInfo, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(targetPath, "/")
	if h.dirs[p] {
		return &storhub.EntryInfo{Path: targetPath, IsDir: true, Mode: 0o755, ModifiedAt: h.clock, AccessedAt: h.clock, ChangedAt: h.clock}, nil
	}
	if target, ok := h.links[p]; ok {
		// lstat semantics like the real backend: a terminal symlink
		// reports itself; the CLI adapter follows through readlink.
		return &storhub.EntryInfo{
			Path: targetPath, Size: int64(len(target)), Mode: 0o777, NLink: 1, Inode: h.linkInoLocked(p),
			IsSymlink: true, SymlinkTarget: target, ModifiedAt: h.clock, AccessedAt: h.clock, ChangedAt: h.clock,
		}, nil
	}
	f, ok := h.files[p]
	if !ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	return &storhub.EntryInfo{
		Path: targetPath, Size: int64(len(f.data)), Mode: f.mode,
		UID: f.uid, GID: f.gid, NLink: 1, Inode: h.fileInoLocked(f), ModifiedAt: f.mtime,
		AccessedAt: f.atime, ChangedAt: f.mtime,
	}, nil
}

func (h *pcFakeHub) ReadFileAtContext(_ context.Context, _, filePath string, offset, length int64) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(filePath, "/")
	if h.dirs[p] {
		return nil, fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	f, ok := h.files[p]
	if !ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	if length == 0 {
		return []byte{}, nil
	}
	if offset < 0 || length < 0 {
		return nil, errors.New("read offset and length must be non-negative")
	}
	if offset > int64(len(f.data)) {
		return nil, io.EOF
	}
	end := offset + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return append([]byte(nil), f.data[offset:end]...), nil
}

func (h *pcFakeHub) MkdirContext(_ context.Context, _, dirPath string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(dirPath, "/")
	if h.dirs[p] || h.files[p] != nil {
		return fmt.Errorf("%w: %s", shfs.ErrAlreadyExists, p)
	}
	h.ensureParentsLocked(p)
	h.dirs[p] = true
	return nil
}

func (h *pcFakeHub) DeleteFileContext(_ context.Context, _, filePath string, _ ...storhub.MutateOption) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(filePath, "/")
	if h.dirs[p] {
		return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	// rm on a symlink removes the link itself, never the target.
	if _, ok := h.links[p]; ok {
		delete(h.links, p)
		delete(h.linkIno, p)
		return nil
	}
	if _, ok := h.files[p]; !ok {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	delete(h.files, p)
	return nil
}

func (h *pcFakeHub) RmdirContext(_ context.Context, _, dirPath string, _ ...storhub.MutateOption) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(dirPath, "/")
	if _, ok := h.files[p]; ok {
		return fmt.Errorf("%w: %s", shfs.ErrNotDirectory, p)
	}
	if !h.dirs[p] {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	prefix := p + "/"
	for name := range h.files {
		if strings.HasPrefix(name, prefix) {
			return fmt.Errorf("%w: %s", shfs.ErrNotEmpty, p)
		}
	}
	for name := range h.dirs {
		if strings.HasPrefix(name, prefix) {
			return fmt.Errorf("%w: %s", shfs.ErrNotEmpty, p)
		}
	}
	delete(h.dirs, p)
	return nil
}

func (h *pcFakeHub) AppendFileContext(_ context.Context, _, filePath string, data []byte, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(filePath, "/")
	if h.dirs[p] {
		return nil, fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	f, ok := h.files[p]
	if !ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	f.data = append(f.data, data...)
	f.mode &^= 0o4000 | 0o2000
	f.mtime = h.tick()
	return &storhub.FileMetadata{Size: int64(len(f.data)), Mode: f.mode, Inode: h.fileInoLocked(f)}, nil
}

func (h *pcFakeHub) PatchFileContext(_ context.Context, _, _ string, _, _ int64, _ []byte, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	return nil, errors.New("pcFakeHub: patch not implemented")
}

// CreateFile is atomic O_CREAT|O_EXCL like the real backend: it fails
// with AlreadyExists when anything (file, dir, link) occupies the path.
func (h *pcFakeHub) CreateFileContext(_ context.Context, _, filePath string) (*storhub.FileMetadata, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(filePath, "/")
	if h.dirs[p] || h.files[p] != nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrAlreadyExists, p)
	}
	if _, ok := h.links[p]; ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrAlreadyExists, p)
	}
	h.ensureParentsLocked(p)
	now := h.tick()
	nf := &pcFile{data: []byte{}, mode: 0o644, mtime: now, atime: now}
	nf.ino = h.allocInoLocked()
	h.files[p] = nf
	return &storhub.FileMetadata{Size: 0, Mode: 0o644, Inode: nf.ino}, nil
}

// TruncateFile resizes with zero-filling growth, ticking mtime like a
// real data mutation (which also advances the CAS token).
func (h *pcFakeHub) TruncateFileContext(_ context.Context, _, filePath string, size int64, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(filePath, "/")
	if h.dirs[p] {
		return nil, fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	if _, ok := h.links[p]; ok {
		return nil, fmt.Errorf("not a regular file: %s", p)
	}
	f, ok := h.files[p]
	if !ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	if size < 0 {
		return nil, errors.New("truncate size must be non-negative")
	}
	if int64(len(f.data)) < size {
		nb := make([]byte, size)
		copy(nb, f.data)
		f.data = nb
	} else {
		f.data = append([]byte(nil), f.data[:size]...)
	}
	f.mtime = h.tick()
	return &storhub.FileMetadata{Size: int64(len(f.data)), Mode: f.mode, Inode: h.fileInoLocked(f)}, nil
}

func (h *pcFakeHub) ChmodContext(_ context.Context, _, targetPath string, mode uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(targetPath, "/")
	if _, ok := h.links[p]; ok {
		return fmt.Errorf("chmod on symlink has no meaning here: %s", p)
	}
	if h.dirs[p] {
		return nil
	}
	f, ok := h.files[p]
	if !ok {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	f.mode = mode & 0o7777
	f.mtime = h.tick()
	return nil
}

// Chown replaces owner/group and clears setuid/setgid, like a
// non-privileged chown that succeeds. Uid/gid arrive as decided by the
// CLI's -1 sentinel mapping.
func (h *pcFakeHub) ChownContext(_ context.Context, _, targetPath string, uid, gid uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(targetPath, "/")
	if _, ok := h.links[p]; ok {
		return fmt.Errorf("chown on symlink has no meaning here: %s", p)
	}
	if h.dirs[p] {
		return nil
	}
	f, ok := h.files[p]
	if !ok {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	f.uid, f.gid = uid, gid
	f.mode &^= 0o6000
	f.mtime = h.tick()
	return nil
}

// Chtimes sets both stamps at nanosecond precision (the fake preserves
// ns so touch round-trips exactly; the real backend is second-precision).
func (h *pcFakeHub) ChtimesContext(_ context.Context, _, targetPath string, atime, mtime int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(targetPath, "/")
	if _, ok := h.links[p]; ok {
		return fmt.Errorf("touch on symlink has no meaning here: %s", p)
	}
	if h.dirs[p] {
		return nil
	}
	f, ok := h.files[p]
	if !ok {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	f.atime, f.mtime = atime, mtime
	return nil
}

func (h *pcFakeHub) SymlinkContext(_ context.Context, _, target, linkPath string) (*storhub.FileMetadata, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(linkPath, "/")
	if h.dirs[p] || h.files[p] != nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrAlreadyExists, p)
	}
	if _, ok := h.links[p]; ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrAlreadyExists, p)
	}
	h.ensureParentsLocked(p)
	h.links[p] = target
	h.linkIno[p] = h.allocInoLocked()
	h.tick()
	return &storhub.FileMetadata{Size: int64(len(target)), Mode: 0o777, Inode: h.linkIno[p]}, nil
}

func (h *pcFakeHub) ReadlinkContext(_ context.Context, _, linkPath string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(linkPath, "/")
	if target, ok := h.links[p]; ok {
		return target, nil
	}
	if h.dirs[p] || h.files[p] != nil {
		return "", fmt.Errorf("not a symlink: %s", p)
	}
	return "", fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
}

// Link aliases newPath to the same bytes (regular files only).
func (h *pcFakeHub) LinkContext(_ context.Context, _, existingPath, newPath string) (*storhub.FileMetadata, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := strings.TrimPrefix(existingPath, "/")
	n := strings.TrimPrefix(newPath, "/")
	if h.dirs[o] {
		return nil, fmt.Errorf("%w: %s", shfs.ErrIsDirectory, o)
	}
	src, ok := h.files[o]
	if !ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, o)
	}
	if h.dirs[n] || h.files[n] != nil {
		return nil, fmt.Errorf("%w: %s", shfs.ErrAlreadyExists, n)
	}
	if _, ok := h.links[n]; ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrAlreadyExists, n)
	}
	h.ensureParentsLocked(n)
	lf := &pcFile{data: append([]byte(nil), src.data...), mode: src.mode, uid: src.uid, gid: src.gid, mtime: h.tick(), atime: src.atime}
	lf.ino = h.fileInoLocked(src)
	h.files[n] = lf
	return &storhub.FileMetadata{Size: int64(len(src.data)), Mode: src.mode, Inode: lf.ino}, nil
}

// WriteFileAtContext enforces --expectedrevision against the file's
// ChangedAt token (the adapter's Revision source): a token from before
// any intervening mutation fails with ErrPreconditionFailed.
func (h *pcFakeHub) WriteFileAtContext(_ context.Context, _, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*storhub.FileMetadata, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(filePath, "/")
	if h.dirs[p] {
		return nil, fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	f, ok := h.files[p]
	if !ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	if rev := shfs.ApplyMutateOptions(opts).ExpectedRevision(); rev != "" && rev != strconv.FormatInt(f.mtime, 10) {
		return nil, fmt.Errorf("%w: revision %s does not match %d", shfs.ErrPreconditionFailed, rev, f.mtime)
	}
	if offset < 0 {
		return nil, errors.New("write offset must be non-negative")
	}
	if len(data) > 0 {
		end := offset + int64(len(data))
		if end > int64(len(f.data)) {
			nb := make([]byte, end)
			copy(nb, f.data)
			f.data = nb
		}
		copy(f.data[offset:], data)
		f.mode &^= 0o6000
		f.mtime = h.tick()
	}
	return &storhub.FileMetadata{Size: int64(len(f.data)), Mode: f.mode, Inode: h.fileInoLocked(f)}, nil
}

// RenameContext enforces RENAME_NOREPLACE inside the fake's mutex (no
// TOCTOU), mirroring the real transaction check; plain renames replace.
func (h *pcFakeHub) RenameContext(_ context.Context, _, oldPath, newPath string, opts ...shfs.MutateOption) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := strings.TrimPrefix(oldPath, "/")
	n := strings.TrimPrefix(newPath, "/")
	dstExists := h.files[n] != nil || h.dirs[n]
	if _, ok := h.links[n]; ok {
		dstExists = true
	}
	if shfs.ApplyMutateOptions(opts).NoReplace() && dstExists {
		return fmt.Errorf("%w: %s", shfs.ErrAlreadyExists, n)
	}
	return h.renameLocked(o, n)
}

// renameLocked is the pre-existing replace rename, extracted so the
// NoReplace branch above shares it.
func (h *pcFakeHub) renameLocked(o, n string) error {
	if h.dirs[o] {
		return fmt.Errorf("pcFakeHub: directory rename not implemented: %s", o)
	}
	f, ok := h.files[o]
	if !ok {
		if target, isLink := h.links[o]; isLink {
			if h.dirs[n] {
				return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, n)
			}
			delete(h.links, n)
			delete(h.linkIno, n)
			delete(h.files, n)
			h.ensureParentsLocked(n)
			h.links[n] = target
			if ino := h.linkIno[o]; ino != 0 {
				h.linkIno[n] = ino
			}
			delete(h.links, o)
			delete(h.linkIno, o)
			return nil
		}
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, o)
	}
	if h.dirs[n] {
		return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, n)
	}
	delete(h.files, n)
	delete(h.links, n)
	delete(h.linkIno, n)
	h.ensureParentsLocked(n)
	h.files[n] = f
	delete(h.files, o)
	return nil
}

func (h *pcFakeHub) ListMetadataRevisionsContext(_ context.Context, _ string) ([]storhub.MetadataRevision, error) {
	return nil, nil
}

func (h *pcFakeHub) RollbackMetadataContext(_ context.Context, _, _ string) error {
	return errors.New("pcFakeHub: rollback not implemented")
}

func (h *pcFakeHub) PruneContext(_ context.Context, _, scope string, _ int, dryRun bool) (*storhub.PruneResult, error) {
	return &storhub.PruneResult{Scope: storhub.PruneScope(scope), DryRun: dryRun}, nil
}
func (h *pcFakeHub) DegradedProjects() []string {
	return nil
}
func (h *pcFakeHub) ReEnableProject(_ string) error {
	return nil
}
func (h *pcFakeHub) PressureSnapshot() storhub.PressureSnapshot {
	return storhub.PressureSnapshot{}
}
func (h *pcFakeHub) PressureFailureStreak(_ string) uint64 { return 0 }
func (h *pcFakeHub) PressurePendingDepth(_ string) int     { return 0 }

func (h *pcFakeHub) DeleteProject(_ string) error { return nil }

func (h *pcFakeHub) NewFUSE(_ string, _ storhub.FUSEOptions) (fuseMount, error) {
	return nil, errors.New("pcFakeHub: FUSE not supported")
}

func (h *pcFakeHub) Shutdown(_ context.Context) error { return nil }

// DrainProjectContext is a no-op here: the conformance fake journals
// nothing, and the CLI conformance surface has no fsync equivalent.
func (h *pcFakeHub) DrainProjectContext(_ context.Context, _ string) error { return nil }

// ---------------------------------------------------------------------------
// Surface adapter driving the CLI.
// ---------------------------------------------------------------------------
