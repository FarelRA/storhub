package cli

// POSIX conformance adapter: drives the real cobra CLI (App) in-process so
// the shared table in internal/posixconform executes against CLI commands.
//
// Mapping (honest, gaps fail loudly):
//   CreateFile      -> `stat --json` + `upload` of an empty temp file (no
//                      truncate when present; exclusive is check-then-act,
//                      NOT atomic; perm bits ignored, CLI has no mode flag)
//   Open            -> `stat --json` (+ `upload` empty to create missing for
//                      every mode except OpenReadOnly); OpenTruncate has no
//                      CLI equivalent and reports a gap error
//   Handle cursors  -> adapter state (allowed by the harness contract);
//                      PRead/Read via `cat`, Write/PWrite via `write`,
//                      append-mode Write via `append`
//   Stat            -> `stat --json` parsed back into posixconform.Stat
//   ReadRange       -> `cat` plus client-side windowing (the CLI exposes no
//                      ranged-read flag); range error semantics enforced here
//   Append          -> `append` (missing path stays ErrNotFound, like real)
//   Unlink/Rename   -> `rm` / `mv` (no --no-replace flag, so noReplace=true
//                      reports a gap error)
//   Mkdir/Rmdir     -> `mkdir` / `rm -r`
//   Truncate/Chmod/Chown/Utimens/Symlink/Readlink/Sync/Revision/
//   CompareAndWrite -> explicit gap errors: the CLI command tree has no
//                      truncate, chmod, chown, utimens/touch, symlink,
//                      readlink, fsync, or CAS/etag facility, so these are
//                      reported, never simulated.
//
// Backend: the package-global one-shot hub seam (newHubFromFlagsFn, the same
// seam app_test.go swaps) is pointed at an in-memory fake for the duration
// of the test. Every byte the adapter sees still travels through a real
// cobra command RunE path (upload/write/append/cat/stat/rm/mv/mkdir); the
// fake only stands in for the networked GitHub storage behind the CLI.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/posixconform"
	"github.com/FarelRA/storhub/storhub"
)

var (
	_ posixconform.Surface = (*cliPOSIXSurface)(nil)
	_ posixconform.Handle  = (*cliHandle)(nil)
)

// ---------------------------------------------------------------------------
// In-memory fake hub behind the CLI seam.
// ---------------------------------------------------------------------------

// pcFile is one stored regular file.
type pcFile struct {
	data  []byte
	mode  uint32
	uid   uint32
	gid   uint32
	mtime int64
}

// pcFakeHub implements hubClient with textbook in-memory semantics and
// shfs/syscall-style errors, mirroring what the real backend reports
// (NotFound on missing append/write targets, AlreadyExists on duplicate
// mkdir, ELOOP-free since symlinks cannot be created through hubClient).
type pcFakeHub struct {
	mu    sync.Mutex
	files map[string]*pcFile
	dirs  map[string]bool
	clock int64
}

func newPCFakeHub() *pcFakeHub {
	return &pcFakeHub{
		files: make(map[string]*pcFile),
		dirs:  map[string]bool{"": true},
		clock: 1700000000000000000,
	}
}

func (h *pcFakeHub) tick() int64 {
	h.clock++
	return h.clock
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

func (h *pcFakeHub) UploadFile(project, remotePath, localPath string) (*storhub.FileMetadata, error) {
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
		return &storhub.FileMetadata{Size: int64(len(cp)), Mode: f.mode, Inode: 1}, nil
	}
	cp := append([]byte(nil), data...)
	h.files[p] = &pcFile{data: cp, mode: 0o644, mtime: h.tick()}
	return &storhub.FileMetadata{Size: int64(len(cp)), Mode: 0o644, Inode: 1}, nil
}

func (h *pcFakeHub) ReplaceFile(project, remotePath, localPath string) (*storhub.FileMetadata, error) {
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
	return h.UploadFile(project, remotePath, localPath)
}

func (h *pcFakeHub) DownloadFile(project, remotePath, localPath string) error {
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

func (h *pcFakeHub) ReadDir(project, dir string) ([]storhub.DirEntry, error) {
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

func (h *pcFakeHub) StatPath(project, targetPath string) (*storhub.EntryInfo, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(targetPath, "/")
	if h.dirs[p] {
		return &storhub.EntryInfo{Path: targetPath, IsDir: true, Mode: 0o755, ModifiedAt: h.clock}, nil
	}
	f, ok := h.files[p]
	if !ok {
		return nil, fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	return &storhub.EntryInfo{
		Path: targetPath, Size: int64(len(f.data)), Mode: f.mode,
		UID: f.uid, GID: f.gid, NLink: 1, Inode: 1, ModifiedAt: f.mtime,
		AccessedAt: f.mtime, ChangedAt: f.mtime,
	}, nil
}

func (h *pcFakeHub) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
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

func (h *pcFakeHub) Mkdir(project, dirPath string) error {
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

func (h *pcFakeHub) DeleteFile(project, filePath string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := strings.TrimPrefix(filePath, "/")
	if h.dirs[p] {
		return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	if _, ok := h.files[p]; !ok {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
	}
	delete(h.files, p)
	return nil
}

func (h *pcFakeHub) Rmdir(project, dirPath string) error {
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

func (h *pcFakeHub) Rename(project, oldPath, newPath string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := strings.TrimPrefix(oldPath, "/")
	n := strings.TrimPrefix(newPath, "/")
	if h.dirs[o] {
		return fmt.Errorf("pcFakeHub: directory rename not implemented: %s", o)
	}
	f, ok := h.files[o]
	if !ok {
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, o)
	}
	if h.dirs[n] {
		return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, n)
	}
	delete(h.files, n)
	h.ensureParentsLocked(n)
	h.files[n] = f
	delete(h.files, o)
	return nil
}

func (h *pcFakeHub) AppendFile(project, filePath string, data []byte) (*storhub.FileMetadata, error) {
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
	return &storhub.FileMetadata{Size: int64(len(f.data)), Mode: f.mode, Inode: 1}, nil
}

func (h *pcFakeHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*storhub.FileMetadata, error) {
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
		f.mode &^= 0o4000 | 0o2000
		f.mtime = h.tick()
	}
	return &storhub.FileMetadata{Size: int64(len(f.data)), Mode: f.mode, Inode: 1}, nil
}

func (h *pcFakeHub) PatchFile(project, filePath string, offset, deleteSize int64, edit []byte) (*storhub.FileMetadata, error) {
	return nil, errors.New("pcFakeHub: patch not implemented")
}

func (h *pcFakeHub) ListMetadataRevisions(project string) ([]storhub.MetadataRevision, error) {
	return nil, nil
}

func (h *pcFakeHub) RollbackMetadataContext(ctx context.Context, project, commitSHA string) error {
	return errors.New("pcFakeHub: rollback not implemented")
}

func (h *pcFakeHub) PurgeUntrackedContext(ctx context.Context, project string) (*storhub.PurgeResult, error) {
	return &storhub.PurgeResult{}, nil
}

func (h *pcFakeHub) PruneContext(ctx context.Context, project, scope string, keep int, dryRun bool) (*storhub.PruneResult, error) {
	return &storhub.PruneResult{Scope: storhub.PruneScope(scope), DryRun: dryRun}, nil
}

func (h *pcFakeHub) DeleteProject(project string) error { return nil }

func (h *pcFakeHub) NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error) {
	return nil, errors.New("pcFakeHub: FUSE not supported")
}

func (h *pcFakeHub) Shutdown(ctx context.Context) error { return nil }

// ---------------------------------------------------------------------------
// Surface adapter driving the CLI.
// ---------------------------------------------------------------------------

// cliPOSIXSurface implements posixconform.Surface by invoking real CLI
// commands on a fresh App per operation.
type cliPOSIXSurface struct {
	project string
}

// runCLI executes one CLI invocation and returns what the command wrote to
// stdout. A fresh App per call keeps cobra flag state isolated under the
// concurrent scenarios.
func (s *cliPOSIXSurface) runCLI(args []string) ([]byte, error) {
	app := New()
	var stdout, stderr bytes.Buffer
	app.stdout = &stdout
	app.stderr = &stderr
	if err := app.Run(args); err != nil {
		return nil, err
	}
	return stdout.Bytes(), nil
}

// cliPath validates an absolute Surface path and maps it to the remote-path
// spelling the CLI accepts.
func cliPath(p string) (string, error) {
	if len(p) < 2 || p[0] != '/' {
		return "", posixconform.ErrInvalid
	}
	return strings.TrimPrefix(p, "/"), nil
}

// pcTranslateErr maps backend/CLI failures onto the posixconform sentinels
// so callers can match with errors.Is. Unrecognized errors pass through
// untouched so scenarios fail with the raw CLI error attached.
func pcTranslateErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, shfs.ErrNotFound):
		return fmt.Errorf("%w (cli: %v)", posixconform.ErrNotFound, err)
	case errors.Is(err, shfs.ErrAlreadyExists):
		return fmt.Errorf("%w (cli: %v)", posixconform.ErrExists, err)
	case errors.Is(err, shfs.ErrIsDirectory):
		return fmt.Errorf("%w (cli: %v)", posixconform.ErrIsDir, err)
	case errors.Is(err, shfs.ErrNotDirectory):
		return fmt.Errorf("%w (cli: %v)", posixconform.ErrNotDir, err)
	case errors.Is(err, shfs.ErrNotEmpty):
		return fmt.Errorf("%w (cli: %v)", posixconform.ErrNotEmpty, err)
	case errors.Is(err, syscall.ELOOP):
		return fmt.Errorf("%w (cli: %v)", posixconform.ErrLoop, err)
	}
	for _, sentinel := range []error{
		posixconform.ErrNotFound, posixconform.ErrExists, posixconform.ErrIsDir,
		posixconform.ErrNotDir, posixconform.ErrNotEmpty, posixconform.ErrLoop,
		posixconform.ErrUnsatisfiableRange, posixconform.ErrClosed,
		posixconform.ErrAccess, posixconform.ErrInvalid,
	} {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	return err
}

// statViaCLI runs `stat --json` and decodes the entry.
func (s *cliPOSIXSurface) statViaCLI(path string) (*storhub.EntryInfo, error) {
	rel, err := cliPath(path)
	if err != nil {
		return nil, err
	}
	out, err := s.runCLI([]string{"stat", "--json", "--token", "x", s.project, rel})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	var entry storhub.EntryInfo
	if err := json.Unmarshal(out, &entry); err != nil {
		return nil, fmt.Errorf("cli stat output decode: %w", err)
	}
	return &entry, nil
}

func (s *cliPOSIXSurface) CreateFile(path string, perm uint32, exclusive bool) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	// Existence probe through the CLI. Exclusive create is NOT mapped onto
	// upload: the CLI offers no O_CREAT|O_EXCL primitive, so any
	// check-then-act here would be racy and could fake atomicity under the
	// exclusive-race scenario. An exclusive create over an existing file
	// truthfully reports ErrExists; an exclusive create of a missing file
	// reports the gap instead of pretending.
	if exclusive {
		if _, statErr := s.statViaCLI(path); statErr == nil {
			return fmt.Errorf("%w: cli stat shows %s already exists", posixconform.ErrExists, path)
		} else if !errors.Is(statErr, posixconform.ErrNotFound) {
			return statErr
		}
		return fmt.Errorf("cli: exclusive CreateFile of %s has no CLI equivalent (upload has no O_EXCL flag; check-then-act would fake atomicity); refusing to fake it", path)
	}
	if _, statErr := s.statViaCLI(path); statErr == nil {
		return nil
	} else if !errors.Is(statErr, posixconform.ErrNotFound) {
		return statErr
	}
	tmp, err := os.CreateTemp("", "storhub-pc-create-*.bin")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := s.runCLI([]string{"upload", "--token", "x", s.project, rel, tmpName}); err != nil {
		if errors.Is(err, shfs.ErrAlreadyExists) {
			return fmt.Errorf("%w: cli upload raced on %s", posixconform.ErrExists, path)
		}
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Open(path string, mode posixconform.OpenMode) (posixconform.Handle, error) {
	switch mode {
	case posixconform.OpenReadOnly, posixconform.OpenWriteOnly,
		posixconform.OpenReadWrite, posixconform.OpenAppend, posixconform.OpenTruncate:
	default:
		return nil, posixconform.ErrInvalid
	}
	if _, err := cliPath(path); err != nil {
		return nil, err
	}
	entry, err := s.statViaCLI(path)
	if err != nil {
		if !errors.Is(err, posixconform.ErrNotFound) {
			return nil, err
		}
		if mode == posixconform.OpenReadOnly {
			return nil, err
		}
		// Every other mode creates a missing file, so materialize it
		// through the CLI before handing out the cursor.
		if cErr := s.CreateFile(path, 0o644, false); cErr != nil {
			return nil, cErr
		}
	} else if entry.IsDir {
		return nil, fmt.Errorf("%w: cli stat shows %s is a directory", posixconform.ErrIsDir, path)
	} else if mode == posixconform.OpenTruncate {
		return nil, errors.New("cli: OpenTruncate has no CLI equivalent (no truncate/ftruncate command); refusing to fake it")
	}
	h := &cliHandle{surface: s, path: path, mode: mode}
	if mode == posixconform.OpenAppend {
		st, err := s.statViaCLI(path)
		if err != nil {
			return nil, err
		}
		h.cursor = st.Size
	}
	return h, nil
}

func (s *cliPOSIXSurface) Stat(path string) (posixconform.Stat, error) {
	entry, err := s.statViaCLI(path)
	if err != nil {
		return posixconform.Stat{}, err
	}
	if entry.IsSymlink {
		// No readlink resolution is attempted here; without symlink
		// creation through the CLI this arm only documents intent.
		return posixconform.Stat{}, errors.New("cli: Stat through symlink has no CLI equivalent (no symlink/readlink commands)")
	}
	if entry.IsDir {
		return posixconform.Stat{}, fmt.Errorf("%w: cli stat shows %s is a directory", posixconform.ErrIsDir, path)
	}
	return posixconform.Stat{
		Size: entry.Size, Mode: entry.Mode, UID: entry.UID, GID: entry.GID, MTime: entry.ModifiedAt,
	}, nil
}

func (s *cliPOSIXSurface) Truncate(path string, size int64) error {
	if _, err := cliPath(path); err != nil {
		return err
	}
	if size < 0 {
		return posixconform.ErrInvalid
	}
	return errors.New("cli: Truncate has no CLI equivalent (no truncate command; patch cannot express grow-with-zero-fill); refusing to fake it")
}

func (s *cliPOSIXSurface) Chmod(path string, mode uint32) error {
	if _, err := cliPath(path); err != nil {
		return err
	}
	return errors.New("cli: Chmod has no CLI equivalent (no chmod command); refusing to fake it")
}

func (s *cliPOSIXSurface) Chown(path string, uid, gid uint32) error {
	if _, err := cliPath(path); err != nil {
		return err
	}
	return errors.New("cli: Chown has no CLI equivalent (no chown command, so setuid/setgid clearing on chown is unobservable); refusing to fake it")
}

func (s *cliPOSIXSurface) Utimens(path string, mtime int64) error {
	if _, err := cliPath(path); err != nil {
		return err
	}
	return errors.New("cli: Utimens has no CLI equivalent (no touch/utimens command); refusing to fake it")
}

func (s *cliPOSIXSurface) Unlink(path string) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := s.runCLI([]string{"rm", "--token", "x", s.project, rel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Rename(oldPath, newPath string, noReplace bool) error {
	oldRel, err := cliPath(oldPath)
	if err != nil {
		return err
	}
	newRel, err := cliPath(newPath)
	if err != nil {
		return err
	}
	if noReplace {
		return errors.New("cli: Rename with noReplace has no CLI equivalent (mv has no --no-replace flag); refusing to fake atomicity")
	}
	if _, err := s.runCLI([]string{"mv", "--token", "x", s.project, oldRel, newRel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Mkdir(path string, perm uint32) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := s.runCLI([]string{"mkdir", "--token", "x", s.project, rel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Rmdir(path string) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := s.runCLI([]string{"rm", "-r", "--token", "x", s.project, rel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Symlink(target, linkPath string) error {
	if _, err := cliPath(linkPath); err != nil {
		return err
	}
	if strings.TrimSpace(target) == "" {
		return posixconform.ErrInvalid
	}
	return errors.New("cli: Symlink has no CLI equivalent (no symlink/ln command); refusing to fake it")
}

func (s *cliPOSIXSurface) Readlink(linkPath string) (string, error) {
	if _, err := cliPath(linkPath); err != nil {
		return "", err
	}
	return "", errors.New("cli: Readlink has no CLI equivalent (no readlink command); refusing to fake it")
}

// ReadRange fetches through `cat` (the CLI has no ranged-read flag) and
// windows client-side. Existence, type, and size come from `stat --json`,
// so missing paths, directories, and loops surface their CLI errors while
// the unsatisfiable/negative range rules are enforced here.
func (s *cliPOSIXSurface) ReadRange(path string, offset, length int64) ([]byte, error) {
	if _, err := cliPath(path); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, posixconform.ErrInvalid
	}
	entry, err := s.statViaCLI(path)
	if err != nil {
		return nil, err
	}
	if entry.IsDir {
		return nil, fmt.Errorf("%w: cli stat shows %s is a directory", posixconform.ErrIsDir, path)
	}
	if offset >= entry.Size {
		return nil, fmt.Errorf("%w: offset %d at or past size %d", posixconform.ErrUnsatisfiableRange, offset, entry.Size)
	}
	rel, _ := cliPath(path)
	data, err := s.runCLI([]string{"cat", "--token", "x", s.project, rel})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	if offset > int64(len(data)) {
		return nil, fmt.Errorf("%w: file shrank under read", posixconform.ErrUnsatisfiableRange)
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return append([]byte(nil), data[offset:end]...), nil
}

func (s *cliPOSIXSurface) Append(path string, data []byte) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := s.runCLI([]string{"append", "--token", "x", s.project, rel, string(data)}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Sync(path string) error {
	if _, err := cliPath(path); err != nil {
		return err
	}
	return errors.New("cli: Sync has no CLI equivalent (no fsync command); returning success would fake durability, so this fails")
}

func (s *cliPOSIXSurface) CompareAndWrite(path string, offset int64, data []byte, token uint64) error {
	if _, err := cliPath(path); err != nil {
		return err
	}
	if offset < 0 {
		return posixconform.ErrInvalid
	}
	return errors.New("cli: CompareAndWrite has no CLI equivalent (write/append/patch expose no etag/if-match guard); refusing to fake CAS")
}

func (s *cliPOSIXSurface) Revision(path string) (uint64, error) {
	if _, err := cliPath(path); err != nil {
		return 0, err
	}
	return 0, errors.New("cli: Revision has no CLI equivalent (revisions lists metadata commits, not per-file CAS tokens); refusing to fake it")
}

// ---------------------------------------------------------------------------
// Handle with adapter-owned cursor.
// ---------------------------------------------------------------------------

type cliHandle struct {
	mu      sync.Mutex
	surface *cliPOSIXSurface
	path    string
	mode    posixconform.OpenMode
	cursor  int64
	closed  bool
}

func (h *cliHandle) readable() bool {
	return h.mode == posixconform.OpenReadOnly || h.mode == posixconform.OpenReadWrite
}

func (h *cliHandle) writable() bool {
	return h.mode == posixconform.OpenWriteOnly || h.mode == posixconform.OpenReadWrite ||
		h.mode == posixconform.OpenAppend || h.mode == posixconform.OpenTruncate
}

// catLocked streams the whole file through `cat`; caller holds h.mu.
func (h *cliHandle) catLocked() ([]byte, error) {
	rel, err := cliPath(h.path)
	if err != nil {
		return nil, err
	}
	data, err := h.surface.runCLI([]string{"cat", "--token", "x", h.surface.project, rel})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	return data, nil
}

func (h *cliHandle) PRead(offset int64, length int) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, posixconform.ErrClosed
	}
	if !h.readable() {
		return nil, posixconform.ErrAccess
	}
	if offset < 0 || length < 0 {
		return nil, posixconform.ErrInvalid
	}
	data, err := h.catLocked()
	if err != nil {
		return nil, err
	}
	if offset >= int64(len(data)) || length == 0 {
		return []byte{}, nil
	}
	end := offset + int64(length)
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return append([]byte(nil), data[offset:end]...), nil
}

func (h *cliHandle) PWrite(offset int64, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, posixconform.ErrClosed
	}
	if !h.writable() {
		return 0, posixconform.ErrAccess
	}
	if offset < 0 {
		return 0, posixconform.ErrInvalid
	}
	rel, err := cliPath(h.path)
	if err != nil {
		return 0, err
	}
	if _, err := h.surface.runCLI([]string{"write", "--token", "x", h.surface.project, rel, strconv.FormatInt(offset, 10), string(data)}); err != nil {
		return 0, pcTranslateErr(err)
	}
	return len(data), nil
}

func (h *cliHandle) Read(length int) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, posixconform.ErrClosed
	}
	if !h.readable() {
		return nil, posixconform.ErrAccess
	}
	if length < 0 {
		return nil, posixconform.ErrInvalid
	}
	data, err := h.catLocked()
	if err != nil {
		return nil, err
	}
	if h.cursor >= int64(len(data)) || length == 0 {
		return []byte{}, nil
	}
	end := h.cursor + int64(length)
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	out := append([]byte(nil), data[h.cursor:end]...)
	h.cursor = end
	return out, nil
}

func (h *cliHandle) Write(data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, posixconform.ErrClosed
	}
	if !h.writable() {
		return 0, posixconform.ErrAccess
	}
	rel, err := cliPath(h.path)
	if err != nil {
		return 0, err
	}
	if h.mode == posixconform.OpenAppend {
		// O_APPEND forces the cursor to the end on every cursor write.
		st, err := h.surface.statViaCLI(h.path)
		if err != nil {
			return 0, err
		}
		h.cursor = st.Size
		if _, err := h.surface.runCLI([]string{"append", "--token", "x", h.surface.project, rel, string(data)}); err != nil {
			return 0, pcTranslateErr(err)
		}
		h.cursor += int64(len(data))
		return len(data), nil
	}
	if _, err := h.surface.runCLI([]string{"write", "--token", "x", h.surface.project, rel, strconv.FormatInt(h.cursor, 10), string(data)}); err != nil {
		return 0, pcTranslateErr(err)
	}
	h.cursor += int64(len(data))
	return len(data), nil
}

func (h *cliHandle) Truncate(size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return posixconform.ErrClosed
	}
	if !h.writable() {
		return posixconform.ErrAccess
	}
	if size < 0 {
		return posixconform.ErrInvalid
	}
	return errors.New("cli: Handle.Truncate has no CLI equivalent (no ftruncate command); refusing to fake it")
}

func (h *cliHandle) Sync() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return posixconform.ErrClosed
	}
	return errors.New("cli: Handle.Sync has no CLI equivalent (no fsync command); returning success would fake durability, so this fails")
}

func (h *cliHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	return nil
}

// ---------------------------------------------------------------------------
// Entrypoint.
// ---------------------------------------------------------------------------

func TestPosixConformCLI(t *testing.T) {
	if os.Getenv("STORHUB_CONFORMANCE") == "" {
		t.Skip("conformance suite runs only with STORHUB_CONFORMANCE=1 (Phase 0 RED: known deviations open)")
	}
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := newPCFakeHub()
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return fake, nil
	}

	adapter := &cliPOSIXSurface{project: "pc"}
	results := posixconform.Run(adapter, posixconform.Table)

	t.Logf("POSIX conformance via CLI: %d scenarios", len(results))
	for _, r := range results {
		if r.Pass {
			t.Logf("PASS %s", r.Name)
		} else {
			t.Logf("FAIL %s: %s", r.Name, r.Error)
		}
	}
	passed, failed := posixconform.Summary(results)
	t.Logf("posixconform CLI: %d passed, %d failed, %d total", passed, failed, len(results))
	if failed > 0 {
		t.Fatalf("%d scenario(s) failed against the CLI surface", failed)
	}
}
