package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	shfs "github.com/FarelRA/storhub/internal/fs"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/FarelRA/storhub/internal/test"
	"github.com/FarelRA/storhub/storhub"
)

// cliPOSIXSurface implements test.Surface by invoking real CLI
// commands on a fresh App per operation.
type cliPOSIXSurface struct {
	project string
	mu      sync.Mutex
	// umask masks CreateFile modes (see test.UmaskSurface; zero
	// disables masking). Guarded by mu: scenarios run sequentially,
	// but concurrent workers inside one scenario read it.
	umask uint32
	// hub is the fake backend every fresh App in runCLI serves.
	// Set once by the entrypoint; runCLI injects it per-App.
	hub hubClient
}

var _ test.UmaskSurface = (*cliPOSIXSurface)(nil)

// SetUmask implements test.UmaskSurface.SetUmask.
func (s *cliPOSIXSurface) SetUmask(mask uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.umask = mask & 0o777
}

// runCLI executes one CLI invocation and returns what the command wrote to
// stdout. A fresh App per call keeps cobra flag state isolated under the
// concurrent scenarios.
func (s *cliPOSIXSurface) runCLI(args []string) ([]byte, error) {
	app := New()
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return s.hub, nil
	}
	var stdout, stderr bytes.Buffer
	app.stdout = &stdout
	app.stderr = &stderr
	if err := app.Run(args); err != nil {
		return nil, err
	}
	return stdout.Bytes(), nil
}

// cliPath validates an absolute Surface path and maps it to the remotepath
// spelling the CLI accepts.
func cliPath(p string) (string, error) {
	if len(p) < 2 || p[0] != '/' {
		return "", test.ErrInvalid
	}
	return strings.TrimPrefix(p, "/"), nil
}

// pcTranslateErr maps backend/CLI failures onto the posixconform sentinels
// so callers can match with errors.Is. The shared backend-to-sentinel
// knowledge lives in test.Translate; this delegate only adds the CLI
// context and the two storage-scoped lines (session linked/stale),
// which cannot move into internal/test without an import cycle.
// Unrecognized errors pass through untouched so scenarios fail with the
// raw CLI error attached.
func pcTranslateErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, storage.ErrSessionLinked):
		return fmt.Errorf("%w (cli: %v)", test.ErrExists, err)
	case errors.Is(err, storage.ErrStaleSession):
		return fmt.Errorf("%w (cli: %v)", test.ErrStale, err)
	}
	return test.Translate(err, func(mapped error) error {
		return fmt.Errorf("%w (cli: %v)", mapped, err)
	})
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
	// The CLI has no mode flag, so the fake always creates 0o644: apply
	// the umask here with an explicit chmod, but only when a mask is
	// configured, leaving the unmasked path (and every existing row)
	// byte-identical.
	s.mu.Lock()
	umask := s.umask
	s.mu.Unlock()
	if umask != 0 {
		if target := (perm & 0o7777) &^ (umask & 0o777); target != 0o644 {
			if st, err := s.statViaCLI(path); err == nil && st.Mode&0o7777 != target {
				return s.Chmod(path, target)
			}
		}
	}
	return nil
}

func (s *cliPOSIXSurface) Open(path string, mode test.OpenMode, disp test.CreateDisposition) (test.Handle, error) {
	switch mode {
	case test.OpenReadOnly, test.OpenWriteOnly,
		test.OpenReadWrite, test.OpenAppend, test.OpenTruncate:
	default:
		return nil, test.ErrInvalid
	}
	switch disp {
	case test.CreateNever, test.CreateIfMissing:
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
		// Creation intent travels in disp, like O_CREAT: CreateNever
		// reports the missing path, while CreateIfMissing materializes
		// it through the CLI before handing out the cursor. OpenReadOnly
		// never creates.
		if disp == test.CreateNever || mode == test.OpenReadOnly || mode == test.OpenPath {
			return nil, err
		}
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
	if _, err := s.runCLI([]string{"touch", "--token", "x", s.project, rel, "--mtimens", stamp, "--atimens", stamp}); err != nil {
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
		args = append(args, "--noreplace")
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
	if _, err := s.runCLI([]string{"write", "--token", "x", "--expectedrevision", rev, s.project, rel, strconv.FormatInt(offset, 10), string(data)}); err != nil {
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
