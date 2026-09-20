package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// LockFileName marks a project cache directory as owned by a live
// process; it stores the owning pid as text. Locks live in a sibling
// .locks/ directory so the project dir itself stays an exact mirror of
// remote state that go-git can clone into.
const (
	LockFileName    = ".storhub-lock"
	locksDirName    = ".locks"
	recoveryDirName = "recovery"
)

// legacyRunDirPattern matches pre-XDG roots dropped straight into the
// temp directory: storhub-git-<pid>.
var legacyRunDirPattern = regexp.MustCompile(`^storhub-git-(\d+)$`)

// pidAlive reports whether a process exists. EPERM means the process
// exists but belongs to another user - treat it as alive.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// projectLockPath is where a project's ownership marker lives.
func projectLockPath(base, project string) string {
	return filepath.Join(base, locksDirName, project+".lock")
}

// projectLockPid returns the pid recorded for a project, or zero when
// unclaimed or unreadable.
func projectLockPid(base, project string) int {
	data, err := os.ReadFile(projectLockPath(base, project))
	if err != nil {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return pid
}

// claimProjectLock records this process as the owner of a project cache
// directory. The create itself is the mutual-exclusion point (O_EXCL), so two
// processes racing to claim the same directory can never both win: the loser
// sees EEXIST and then honors whoever holds the lock. A directory held by a
// live foreign process refuses claims, which keeps concurrent mounts from
// corrupting each other's worktree; only a dead or stale lock may be
// reclaimed, and the reclaim is re-contested with O_EXCL so a racing
// reclaimer loses cleanly.
func claimProjectLock(base, project string) error {
	lockPath := projectLockPath(base, project)
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return fmt.Errorf("create locks dir: %w", err)
	}
	pid := os.Getpid()
	if err := writeLockExclusive(lockPath, pid); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	existing := projectLockPid(base, project)
	if existing == pid {
		// We already hold it (e.g. a re-entrant ensure on the same mount):
		// refresh the marker and succeed without disturbing the worktree.
		return os.WriteFile(lockPath, []byte(strconv.Itoa(pid)), 0o644)
	}
	if existing != 0 && pidAlive(existing) {
		return fmt.Errorf("cache dir for %s is held by live process %d", project, existing)
	}
	// Dead or unreadable lock: reclaim, re-contested so a concurrent
	// reclaimer cannot both proceed.
	_ = os.Remove(lockPath)
	if err := writeLockExclusive(lockPath, pid); err != nil {
		if errors.Is(err, os.ErrExist) {
			if holder := projectLockPid(base, project); holder != 0 && holder != pid && pidAlive(holder) {
				return fmt.Errorf("cache dir for %s is held by live process %d", project, holder)
			}
		}
		return err
	}
	return nil
}

// writeLockExclusive creates the lock file with O_CREATE|O_EXCL and writes the
// pid, returning os.ErrExist when another process already holds it.
func writeLockExclusive(path string, pid int) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.WriteString(strconv.Itoa(pid))
	return err
}

// releaseProjectLock drops ownership without deleting anything.
func releaseProjectLock(base, project string) {
	_ = os.Remove(projectLockPath(base, project))
}

// projectDirIsOrphan reports whether a project cache directory can be
// deleted: it carries no lock from a live process. Our own live claim
// counts as alive - the Shutdown path removes claimed dirs explicitly.
func projectDirIsOrphan(base, project string) bool {
	pid := projectLockPid(base, project)
	return pid == 0 || !pidAlive(pid)
}

// reapDirs is the single home of the reaper's ReadDir/RemoveAll/Warn/Info
// loop (audit 24): reapOrphaned (git mirrors + legacy roots) and
// reapOrphanedObjectCaches (lock-judged object dirs) shared only the shape
// through duplicated code. shouldReap decides per entry (returning keep =
// false skips); after runs on each successful removal (e.g. lock release).
// Best-effort: one undeletable entry never stops the rest. Returns the
// number of directories reclaimed.
func reapDirs(logger *slog.Logger, label, base string, shouldReap func(name string, entry os.DirEntry) (reap bool), after func(name string)) int {
	if base == "" {
		return 0
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return 0
	}
	reaped := 0
	for _, entry := range entries {
		if !shouldReap(entry.Name(), entry) {
			continue
		}
		dir := filepath.Join(base, entry.Name())
		if err := os.RemoveAll(dir); err != nil {
			if logger != nil {
				logger.Warn("cache reaper could not remove "+label, "dir", dir, "err", err)
			}
			continue
		}
		reaped++
		if after != nil {
			after(entry.Name())
		}
		if logger != nil {
			logger.Info("reaped "+label, "dir", dir)
		}
	}
	return reaped
}

// reapOrphanedProjects removes per-project cache directories whose owner
// is gone, plus legacy top-level storhub-git-<pid> roots from before the
// XDG layout. Best-effort: one undeletable entry never stops the rest.
// Returns the number of directories reclaimed.
func reapOrphaned(logger *slog.Logger, bases ...string) int {
	reaped := 0
	for _, base := range bases {
		if base == "" {
			continue
		}
		b := base
		reaped += reapDirs(logger, "orphaned cache entry", b, func(name string, entry os.DirEntry) bool {
			switch {
			case name == locksDirName:
				return false
			case entry.IsDir():
				// Project cache dir: orphan when unlocked or dead-locked.
				return projectDirIsOrphan(b, name)
			case legacyRunDirPattern.MatchString(name):
				// Legacy whole-root naming; pid decides liveness.
				m := legacyRunDirPattern.FindStringSubmatch(name)
				pid, _ := strconv.Atoi(m[1])
				return !pidAlive(pid)
			default:
				return false
			}
		}, func(name string) { releaseProjectLock(b, name) })
	}
	return reaped
}

// spoolOrphanAge bounds how long a crashed upload's spool file may linger:
// live spools exist only for one asset-window upload (bounded by the HTTP
// client's own timeout, minutes), so anything older died with its uploader
// (audit 31: ReapOrphanedCaches swept git/objects/legacy-tmp but never
// rest/upload-*).
const spoolOrphanAge = 720 * storcfg.PatienceUnit // 1 hour

// reapSpoolDir removes upload-* spool files older than maxAge by mtime in
// dir (flat layout, no per-upload dirs). Live uploads hold young files;
// only crash orphans age out. Best-effort; returns files reclaimed.
// A missing dir is not an error (nothing ever spooled there).
func reapSpoolDir(logger *slog.Logger, dir string, maxAge time.Duration) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	now := time.Now()
	reaped := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "upload-") {
			continue
		}
		p := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if now.Sub(info.ModTime()) < maxAge {
			continue
		}
		if err := os.Remove(p); err != nil {
			if logger != nil {
				logger.Warn("cache reaper could not remove orphaned spool file", "file", p, "err", err)
			}
			continue
		}
		reaped++
		if logger != nil {
			logger.Info("reaped orphaned spool file", "file", p)
		}
	}
	return reaped
}

// ReapOrphanedCaches reclaims storhub cache leftovers in the standard
// locations: the git base, the object cache base, crashed-upload spool
// files (by mtime), and the legacy temp-directory pattern. Used by hub
// startup and by `storhub cache purge`.
func ReapOrphanedCaches(logger *slog.Logger) int {
	return ReapOrphanedCachesForBase(logger, storcfg.CacheBase())
}

// ReapOrphanedCachesForBase reclaims every cache class beneath one base
// (git/objects/rest + the legacy storhub/rest spool shim): the
// test-injectable form of ReapOrphanedCaches, which passes the process
// CacheBase. Object liveness is judged by the git worktree lock, spool
// files by mtime (live spools are young).
func ReapOrphanedCachesForBase(logger *slog.Logger, base string) int {
	if base == "" {
		return 0
	}
	gitBase := filepath.Join(base, "git")
	objectsBase := filepath.Join(base, "objects")
	reaped := reapOrphaned(logger, gitBase, os.TempDir())
	reaped += reapOrphanedObjectCaches(logger, objectsBase, gitBase)
	// Spool sweep by mtime: current path plus the pre-fix double-storhub
	// shim (<base>/storhub/rest), which spoolBase() migrates at runtime
	// but may still hold files when the symlink was never created.
	reaped += reapSpoolDir(logger, filepath.Join(base, "rest"), spoolOrphanAge)
	reaped += reapSpoolDir(logger, filepath.Join(base, "storhub", "rest"), spoolOrphanAge)
	return reaped
}

// reapOrphanedObjectCaches sweeps CacheBase()/objects/<project> for projects
// deleted (or never mounted again) while their cache dir lingered. Object
// caches carry no lock of their own, so liveness is judged by the git
// worktree lock in lockBase: a git-backed mount is spared outright. A
// REST-only mount has no lock and may lose its cached bytes here — that is
// a performance event, never a correctness one: the cache verifies a cold
// entry's content address on first read and self-heals, and every miss
// simply refetches from the repo.
func reapOrphanedObjectCaches(logger *slog.Logger, objectsBase, lockBase string) int {
	return reapDirs(logger, "orphaned object cache", objectsBase, func(name string, entry os.DirEntry) bool {
		if !entry.IsDir() || name == locksDirName {
			return false
		}
		return projectDirIsOrphan(lockBase, name)
	}, nil)
}

// noSpaceError reports an out-of-space failure with the directory that
// actually ran out, so the fix is one glance away instead of buried in
// a go-git stack trace.
type noSpaceError struct {
	Dir string
	Err error
}

func (e *noSpaceError) Error() string {
	return fmt.Sprintf(
		"no space left in cache directory %s: %v (free space or point STORHUB_CACHE_DIR at a larger filesystem)",
		e.Dir, e.Err)
}

func (e *noSpaceError) Unwrap() error { return e.Err }

// wrapNoSpace annotates write failures from the local cache tree when
// the kernel says the device is full.
func wrapNoSpace(dir string, err error) error {
	if err == nil || !errors.Is(err, syscall.ENOSPC) {
		return err
	}
	return &noSpaceError{Dir: dir, Err: err}
}
