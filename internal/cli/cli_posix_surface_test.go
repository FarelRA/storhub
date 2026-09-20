package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/FarelRA/storhub/internal/test"
	"github.com/FarelRA/storhub/storhub"
)

// cliPOSIXSurface implements test.Surface by invoking real CLI
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
		return "", test.ErrInvalid
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
		return fmt.Errorf("%w (cli: %v)", test.ErrNotFound, err)
	case errors.Is(err, shfs.ErrAlreadyExists):
		return fmt.Errorf("%w (cli: %v)", test.ErrExists, err)
	case errors.Is(err, shfs.ErrIsDirectory):
		return fmt.Errorf("%w (cli: %v)", test.ErrIsDir, err)
	case errors.Is(err, shfs.ErrNotDirectory):
		return fmt.Errorf("%w (cli: %v)", test.ErrNotDir, err)
	case errors.Is(err, shfs.ErrNotEmpty):
		return fmt.Errorf("%w (cli: %v)", test.ErrNotEmpty, err)
	case errors.Is(err, syscall.ELOOP):
		return fmt.Errorf("%w (cli: %v)", test.ErrLoop, err)
	case errors.Is(err, storage.ErrSessionLinked):
		return fmt.Errorf("%w (cli: %v)", test.ErrExists, err)
	case errors.Is(err, storage.ErrStaleSession):
		return fmt.Errorf("%w (cli: %v)", test.ErrStale, err)
	}
	for _, sentinel := range []error{
		test.ErrNotFound, test.ErrExists, test.ErrIsDir,
		test.ErrNotDir, test.ErrNotEmpty, test.ErrLoop,
		test.ErrUnsatisfiableRange, test.ErrClosed,
		test.ErrAccess, test.ErrInvalid, test.ErrStale,
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

func (s *cliPOSIXSurface) CreateFile(path string, _ uint32, exclusive bool) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	// touch never truncates, so non-exclusive create is idempotent, and
	// --exclusive gates on the atomic storage create (exactly one
	// concurrent winner), never check-then-act.
	args := []string{"touch", "--token", "x", s.project, rel}
	if exclusive {
		args = append(args, "--exclusive")
	}
	if _, err := s.runCLI(args); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Open(path string, mode test.OpenMode) (test.Handle, error) {
	switch mode {
	case test.OpenReadOnly, test.OpenWriteOnly,
		test.OpenReadWrite, test.OpenAppend, test.OpenTruncate:
	default:
		return nil, test.ErrInvalid
	}
	if _, err := cliPath(path); err != nil {
		return nil, err
	}
	// Open follows a final symlink like open(2): resolve first so the
	// handle addresses the target, and loops surface ErrLoop here.
	resolved, entry, err := s.followLinks(path)
	if err != nil {
		if !errors.Is(err, test.ErrNotFound) {
			return nil, err
		}
		if mode == test.OpenReadOnly {
			return nil, err
		}
		// Every other mode creates a missing file, so materialize it
		// through the CLI before handing out the cursor.
		if cErr := s.CreateFile(resolved, 0o644, false); cErr != nil {
			return nil, cErr
		}
		if resolved, entry, err = s.followLinks(resolved); err != nil {
			return nil, err
		}
	}
	if entry.IsDir {
		return nil, fmt.Errorf("%w: cli stat shows %s is a directory", test.ErrIsDir, path)
	}
	if mode == test.OpenTruncate {
		rel, _ := cliPath(resolved)
		if _, err := s.runCLI([]string{"truncate", "--token", "x", s.project, rel, "0"}); err != nil {
			return nil, pcTranslateErr(err)
		}
		if resolved, entry, err = s.followLinks(resolved); err != nil {
			return nil, err
		}
	}
	h := &cliHandle{surface: s, path: resolved, mode: mode}
	if mode == test.OpenAppend {
		st, err := s.statViaCLI(resolved)
		if err != nil {
			return nil, err
		}
		h.cursor = st.Size
	}
	// Every handle also opens a server-side session: the open-file
	// description behind close commit/discard/follow and detached IO.
	// Failing here fails the open loudly instead of silently dropping
	// POSIX close semantics.
	rel, _ := cliPath(resolved)
	id, err := s.sessionOpen(rel, cliSessionMode(mode), "")
	if err != nil {
		return nil, err
	}
	h.session = id
	h.ino = entry.Inode
	return h, nil
}

// maxFollowHops caps adapter-side symlink resolution, matching the
// backend's own loop bound: past it the path reports ELOOP, never a hang.
const maxFollowHops = 40

// followLinks resolves path through a final symlink chain like open(2),
// returning the resolved absolute path and its entry. Dangling targets
// report ErrNotFound; chains past maxFollowHops report ErrLoop.
func (s *cliPOSIXSurface) followLinks(path string) (string, *storhub.EntryInfo, error) {
	current := path
	for i := 0; i < maxFollowHops; i++ {
		entry, err := s.statViaCLI(current)
		if err != nil {
			return current, nil, err
		}
		if !entry.IsSymlink {
			return current, entry, nil
		}
		target := entry.SymlinkTarget
		if strings.HasPrefix(target, "/") {
			current = target
			continue
		}
		dir := current[:strings.LastIndex(current, "/")]
		current = dir + "/" + target
	}
	return current, nil, fmt.Errorf("%w: too many levels resolving %s", test.ErrLoop, path)
}

func (s *cliPOSIXSurface) Stat(path string) (test.Stat, error) {
	_, entry, err := s.followLinks(path)
	if err != nil {
		return test.Stat{}, err
	}
	if entry.IsDir {
		return test.Stat{}, fmt.Errorf("%w: cli stat shows %s is a directory", test.ErrIsDir, path)
	}
	return test.Stat{
		Size: entry.Size, Mode: entry.Mode, UID: entry.UID, GID: entry.GID, MTime: entry.ModifiedAt,
	}, nil
}

func (s *cliPOSIXSurface) Truncate(path string, size int64) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if size < 0 {
		return test.ErrInvalid
	}
	if _, err := s.runCLI([]string{"truncate", "--token", "x", s.project, rel, strconv.FormatInt(size, 10)}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Chmod(path string, mode uint32) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := s.runCLI([]string{"chmod", "--token", "x", s.project, rel, strconv.FormatUint(uint64(mode&0o7777), 8)}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Chown(path string, uid, gid uint32) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := s.runCLI([]string{"chown", "--token", "x", s.project, rel, strconv.FormatUint(uint64(uid), 10), strconv.FormatUint(uint64(gid), 10)}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Utimens(path string, mtime int64) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	stamp := strconv.FormatInt(mtime, 10)
	if _, err := s.runCLI([]string{"touch", "--token", "x", s.project, rel, "--mtime-ns", stamp, "--atime-ns", stamp}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
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
	args := []string{"mv", "--token", "x", s.project, oldRel, newRel}
	if noReplace {
		args = append(args, "--no-replace")
	}
	if _, err := s.runCLI(args); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Mkdir(path string, _ uint32) error {
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
	rel, err := cliPath(linkPath)
	if err != nil {
		return err
	}
	if strings.TrimSpace(target) == "" {
		return test.ErrInvalid
	}
	if _, err := s.runCLI([]string{"symlink", "--token", "x", s.project, target, rel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (s *cliPOSIXSurface) Readlink(linkPath string) (string, error) {
	if _, err := cliPath(linkPath); err != nil {
		return "", err
	}
	// Classify first: missing paths report NotFound, non-links ErrInvalid,
	// and only true links reach the readlink command.
	entry, err := s.statViaCLI(linkPath)
	if err != nil {
		return "", err
	}
	if !entry.IsSymlink {
		return "", fmt.Errorf("%w: cli stat shows %s is not a symlink", test.ErrInvalid, linkPath)
	}
	rel, _ := cliPath(linkPath)
	out, err := s.runCLI([]string{"readlink", "--token", "x", s.project, rel})
	if err != nil {
		return "", pcTranslateErr(err)
	}
	return strings.TrimSuffix(string(out), "\n"), nil
}

// ReadRange resolves a final symlink (reads follow links) and fetches
// through `cat` (the CLI has no ranged-read flag), windowing client-side.
// Existence, type, and size come from `stat --json`, so missing paths,
// directories, and loops surface their CLI errors while the
// unsatisfiable/negative range rules are enforced here.
func (s *cliPOSIXSurface) ReadRange(path string, offset, length int64) ([]byte, error) {
	if _, err := cliPath(path); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, test.ErrInvalid
	}
	resolved, entry, err := s.followLinks(path)
	if err != nil {
		return nil, err
	}
	if entry.IsDir {
		return nil, fmt.Errorf("%w: cli stat shows %s is a directory", test.ErrIsDir, path)
	}
	if offset >= entry.Size {
		return nil, fmt.Errorf("%w: offset %d at or past size %d", test.ErrUnsatisfiableRange, offset, entry.Size)
	}
	rel, _ := cliPath(resolved)
	data, err := s.runCLI([]string{"cat", "--token", "x", s.project, rel})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	if offset > int64(len(data)) {
		return nil, fmt.Errorf("%w: file shrank under read", test.ErrUnsatisfiableRange)
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
	// The standalone sync command drains the project (the --sync flag's
	// standalone form), which is exactly fsync-class durability here.
	if _, err := s.runCLI([]string{"project", "sync", "--token", "x", s.project}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

// Revision reports the file's ChangedAt clock as the CAS token: every
// data or metadata mutation ticks it, so it advances exactly when the
// content a CompareAndWrite guards moves.
func (s *cliPOSIXSurface) Revision(path string) (uint64, error) {
	_, entry, err := s.followLinks(path)
	if err != nil {
		return 0, err
	}
	return uint64(entry.ChangedAt), nil
}

func (s *cliPOSIXSurface) CompareAndWrite(path string, offset int64, data []byte, token uint64) error {
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if offset < 0 {
		return test.ErrInvalid
	}
	rev := strconv.FormatUint(token, 10)
	if _, err := s.runCLI([]string{"write", "--token", "x", "--expected-revision", rev, s.project, rel, strconv.FormatInt(offset, 10), string(data)}); err != nil {
		if errors.Is(err, shfs.ErrPreconditionFailed) {
			current, statErr := s.Revision(path)
			if statErr != nil {
				return statErr
			}
			return test.ErrPrecondition{Expected: token, Actual: current}
		}
		return pcTranslateErr(err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Handle with adapter-owned cursor.
// ---------------------------------------------------------------------------

type cliHandle struct {
	mu      sync.Mutex
	surface *cliPOSIXSurface
	path    string
	mode    test.OpenMode
	cursor  int64
	closed  bool
	// ino pins the open-time inode: after unlink+recreate the path
	// names a new file, and only the pin tells them apart. Backends
	// preserve inode identity across commits, so the pin never needs
	// refreshing — adopting a new inode would rebind the handle to a
	// stranger's file.
	ino uint64
	// session is the server-side open-file description (session open).
	// One-shot verbs carry IO while the path is linked (immediate
	// publish, which is what cross-handle visibility and uncommitted
	// stat size observe); the session carries the pin plus close
	// commit/discard/follow, and serves IO staged after an unlink or
	// rename. Stateless commands plus stateful sessions are one CLI
	// surface, and open file descriptions live in the stateful half.
	session string
}

// cliSessionMode maps open modes to session fopen strings. Only "r" and
// "r+" are used: the adapter enforces fd legality locally, create and
// truncate happen through one-shot verbs first, and append cursors stay
// adapter-side.
func cliSessionMode(m test.OpenMode) string {
	if m == test.OpenReadOnly {
		return "r"
	}
	return "r+"
}

func (s *cliPOSIXSurface) sessionOpen(rel, mode, ttl string) (string, error) {
	args := []string{"session", "open", "--token", "x", s.project}
	if rel != "" {
		args = append(args, rel)
	}
	args = append(args, "--mode", mode)
	if ttl != "" {
		args = append(args, "--ttl", ttl)
	}
	out, err := s.runCLI(args)
	if err != nil {
		return "", pcTranslateErr(err)
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		return "", fmt.Errorf("session open %s: empty handle", rel)
	}
	return id, nil
}

func (h *cliHandle) sessionRead(offset int64, length int) ([]byte, error) {
	out, err := h.surface.runCLI([]string{"session", "read", "--token", "x", "--handle", h.session,
		"--offset", strconv.FormatInt(offset, 10), "--length", strconv.Itoa(length)})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	return out, nil
}

func (h *cliHandle) sessionWrite(offset int64, data []byte) (int, error) {
	// Text travels via argv, which is exact for the ASCII payloads the
	// table uses; binary callers use session write with "-" (stdin).
	if _, err := h.surface.runCLI([]string{"session", "write", "--token", "x", "--handle", h.session,
		strconv.FormatInt(offset, 10), string(data)}); err != nil {
		return 0, pcTranslateErr(err)
	}
	return len(data), nil
}

func (h *cliHandle) sessionSync() error {
	if _, err := h.surface.runCLI([]string{"session", "sync", "--token", "x", "--handle", h.session}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliHandle) sessionTruncate(size int64) error {
	if _, err := h.surface.runCLI([]string{"session", "truncate", "--token", "x", "--handle", h.session, strconv.FormatInt(size, 10)}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliHandle) sessionSize() (int64, error) {
	out, err := h.surface.runCLI([]string{"session", "stat", "--token", "x", "--handle", h.session, "--json"})
	if err != nil {
		return 0, pcTranslateErr(err)
	}
	var doc struct {
		Size int64 `json:"size"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		return 0, fmt.Errorf("session stat: decode size: %v", err)
	}
	return doc.Size, nil
}

func (h *cliHandle) readable() bool {
	return h.mode == test.OpenReadOnly || h.mode == test.OpenReadWrite
}

func (h *cliHandle) writable() bool {
	return h.mode == test.OpenWriteOnly || h.mode == test.OpenReadWrite ||
		h.mode == test.OpenAppend || h.mode == test.OpenTruncate
}

// statLinkedLocked stats h.path and reports whether it still names the
// open-time inode, returning the entry when it does. A recycled name
// (same path, new inode after unlink+recreate) must serve the pinned
// session, never the new file. Caller holds h.mu, matching catLocked.
func (h *cliHandle) statLinkedLocked() (entry *storhub.EntryInfo, linked bool, err error) {
	ino := h.ino
	entry, err = h.surface.statViaCLI(h.path)
	if err != nil {
		if errors.Is(err, test.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return entry, entry.Inode == ino, nil
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
		return nil, test.ErrClosed
	}
	if !h.readable() {
		return nil, test.ErrAccess
	}
	if offset < 0 || length < 0 {
		return nil, test.ErrInvalid
	}
	if _, linked, err := h.statLinkedLocked(); err != nil {
		return nil, err
	} else if !linked {
		// Detached (unlinked, renamed away, or recycled): serve the
		// session pin plus staged writes.
		sess, serr := h.sessionRead(offset, length)
		if serr != nil {
			return nil, serr
		}
		return sess, nil
	}
	data, err := h.catLocked()
	if err != nil {
		// The path is gone (unlinked or renamed away) but the open
		// description survives: serve the session pin plus staged
		// writes. Only NotFound falls back; anything else propagates.
		if !errors.Is(pcTranslateErr(err), test.ErrNotFound) {
			return nil, err
		}
		sess, serr := h.sessionRead(offset, length)
		if serr != nil {
			return nil, serr
		}
		return sess, nil
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
		return 0, test.ErrClosed
	}
	if !h.writable() {
		return 0, test.ErrAccess
	}
	if offset < 0 {
		return 0, test.ErrInvalid
	}
	// Staged through the session, then committed: the bytes publish
	// immediately (cross-handle visibility, stat size) and the pin
	// refreshes, so a later unlink still serves them from the session.
	wrote, err := h.sessionWrite(offset, data)
	if err != nil {
		return 0, err
	}
	if err := h.sessionSync(); err != nil {
		return 0, err
	}
	return wrote, nil
}

func (h *cliHandle) Read(length int) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, test.ErrClosed
	}
	if !h.readable() {
		return nil, test.ErrAccess
	}
	if length < 0 {
		return nil, test.ErrInvalid
	}
	if _, linked, err := h.statLinkedLocked(); err != nil {
		return nil, err
	} else if !linked {
		sess, serr := h.sessionRead(h.cursor, length)
		if serr != nil {
			return nil, serr
		}
		h.cursor += int64(len(sess))
		return sess, nil
	}
	data, err := h.catLocked()
	if err != nil {
		if !errors.Is(pcTranslateErr(err), test.ErrNotFound) {
			return nil, err
		}
		sess, serr := h.sessionRead(h.cursor, length)
		if serr != nil {
			return nil, serr
		}
		h.cursor += int64(len(sess))
		return sess, nil
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
		return 0, test.ErrClosed
	}
	if !h.writable() {
		return 0, test.ErrAccess
	}
	off := h.cursor
	if h.mode == test.OpenAppend {
		// O_APPEND forces the cursor to the end on every cursor write.
		// Detached mid-append (unlinked, renamed, or recycled), the
		// session size is the end.
		_, linked, err := h.statLinkedLocked()
		if err != nil {
			return 0, err
		}
		if !linked {
			sz, serr := h.sessionSize()
			if serr != nil {
				return 0, serr
			}
			off = sz
		} else {
			st, err := h.surface.statViaCLI(h.path)
			if err != nil {
				return 0, err
			}
			off = st.Size
		}
	}
	wrote, err := h.sessionWrite(off, data)
	if err != nil {
		return 0, err
	}
	if err := h.sessionSync(); err != nil {
		return 0, err
	}
	h.cursor = off + int64(wrote)
	return wrote, nil
}

func (h *cliHandle) Truncate(size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	if !h.writable() {
		return test.ErrAccess
	}
	if size < 0 {
		return test.ErrInvalid
	}
	// Staged through the session, then committed like writes: the pin
	// refreshes and a later unlink still serves the truncated view.
	if err := h.sessionTruncate(size); err != nil {
		return err
	}
	return h.sessionSync()
}

func (h *cliHandle) Sync() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	// Session sync commits, repins, and the close flag drains the
	// project; plain session sync is the fsync equivalent here because
	// every staged write already committed through write+sync.
	if err := h.sessionSync(); err != nil {
		return err
	}
	if _, err := h.surface.runCLI([]string{"project", "sync", "--token", "x", h.surface.project}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return test.ErrClosed
	}
	h.mu.Unlock()
	// Session close commits staged state (discarding when the path went
	// away, following a rename) with --sync draining, so close means
	// durable exactly like Sync.
	if _, err := h.surface.runCLI([]string{"session", "close", "--token", "x", "--handle", h.session, "--sync"}); err != nil {
		return pcTranslateErr(err)
	}
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}

// cliScratchHandle is a pathless open-file description over one
// server-side session: writes stage (no sync — an unlinked scratch has
// nothing to commit to), Link names the staged image, Close publishes
// or discards. A close that fails (taken target) leaves the description
// open for Relink, mirroring close(2) never consuming a failed close.
type cliScratchHandle struct {
	surface *cliPOSIXSurface
	session string
	mu      sync.Mutex
	cursor  int64
	closed  bool
}

func (s *cliPOSIXSurface) OpenScratch(ttl time.Duration) (test.ScratchSession, error) {
	ttlRaw := ""
	if ttl > 0 {
		ttlRaw = ttl.String()
	}
	id, err := s.sessionOpen("", "r+", ttlRaw)
	if err != nil {
		return nil, err
	}
	return &cliScratchHandle{surface: s, session: id}, nil
}

func (h *cliScratchHandle) checkClosed() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return test.ErrClosed
	}
	return nil
}

func (h *cliScratchHandle) PRead(offset int64, length int) ([]byte, error) {
	if err := h.checkClosed(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, test.ErrInvalid
	}
	if length == 0 {
		return []byte{}, nil
	}
	out, err := h.surface.runCLI([]string{"session", "read", "--token", "x", "--handle", h.session,
		"--offset", strconv.FormatInt(offset, 10), "--length", strconv.Itoa(length)})
	if err != nil {
		return nil, pcTranslateErr(err)
	}
	return out, nil
}

func (h *cliScratchHandle) PWrite(offset int64, data []byte) (int, error) {
	if err := h.checkClosed(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, test.ErrInvalid
	}
	if len(data) == 0 {
		return 0, nil
	}
	if _, err := h.surface.runCLI([]string{"session", "write", "--token", "x", "--handle", h.session,
		strconv.FormatInt(offset, 10), string(data)}); err != nil {
		return 0, pcTranslateErr(err)
	}
	return len(data), nil
}

func (h *cliScratchHandle) Read(length int) ([]byte, error) {
	if err := h.checkClosed(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	cursor := h.cursor
	h.mu.Unlock()
	if length < 0 {
		return nil, test.ErrInvalid
	}
	if length == 0 {
		return []byte{}, nil
	}
	got, err := h.PRead(cursor, length)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.cursor = cursor + int64(len(got))
	h.mu.Unlock()
	return got, nil
}

func (h *cliScratchHandle) Write(data []byte) (int, error) {
	if err := h.checkClosed(); err != nil {
		return 0, err
	}
	h.mu.Lock()
	cursor := h.cursor
	h.mu.Unlock()
	if len(data) == 0 {
		return 0, nil
	}
	wrote, err := h.PWrite(cursor, data)
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	h.cursor = cursor + int64(wrote)
	h.mu.Unlock()
	return wrote, nil
}

func (h *cliScratchHandle) Truncate(size int64) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	if size < 0 {
		return test.ErrInvalid
	}
	if _, err := h.surface.runCLI([]string{"session", "truncate", "--token", "x", "--handle", h.session, strconv.FormatInt(size, 10)}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliScratchHandle) Sync() error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	// Unlinked scratch has nothing to commit to; the staged bytes are
	// already visible to the description itself, so sync succeeds as a
	// no-op like fsync on an unwritten fd. Publishing is Close's job.
	return nil
}

func (h *cliScratchHandle) Link(path string) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := h.surface.runCLI([]string{"session", "link", "--token", "x", "--handle", h.session, rel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliScratchHandle) Relink(path string) error {
	if err := h.checkClosed(); err != nil {
		return err
	}
	rel, err := cliPath(path)
	if err != nil {
		return err
	}
	if _, err := h.surface.runCLI([]string{"session", "relink", "--token", "x", "--handle", h.session, rel}); err != nil {
		return pcTranslateErr(err)
	}
	return nil
}

func (h *cliScratchHandle) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return test.ErrClosed
	}
	h.mu.Unlock()
	if _, err := h.surface.runCLI([]string{"session", "close", "--token", "x", "--handle", h.session, "--sync"}); err != nil {
		return pcTranslateErr(err)
	}
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// Entrypoint.
// ---------------------------------------------------------------------------
