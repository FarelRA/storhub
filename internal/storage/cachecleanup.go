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
// directory. A directory held by a live foreign process refuses claims,
// which keeps concurrent mounts from corrupting each other's worktree.
func claimProjectLock(base, project string) error {
	lockPath := projectLockPath(base, project)
	if existing := projectLockPid(base, project); existing != 0 && existing != os.Getpid() && pidAlive(existing) {
		return fmt.Errorf("cache dir for %s is held by live process %d", project, existing)
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return fmt.Errorf("create locks dir: %w", err)
	}
	return os.WriteFile(lockPath, []byte(strconv.Itoa(os.Getpid())), 0o644)
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
		entries, err := os.ReadDir(base)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			dir := filepath.Join(base, entry.Name())
			switch {
			case entry.Name() == locksDirName:
				continue
			case entry.IsDir():
				// Project cache dir: orphan when unlocked or dead-locked.
				if !projectDirIsOrphan(base, entry.Name()) {
					continue
				}
			case legacyRunDirPattern.MatchString(entry.Name()):
				// Legacy whole-root naming; pid decides liveness.
				m := legacyRunDirPattern.FindStringSubmatch(entry.Name())
				pid, _ := strconv.Atoi(m[1])
				if pidAlive(pid) {
					continue
				}
			default:
				continue
			}
			if err := os.RemoveAll(dir); err != nil {
				if logger != nil {
					logger.Warn("cache reaper could not remove orphan", "dir", dir, "err", err)
				}
				continue
			}
			reaped++
			releaseProjectLock(base, entry.Name())
			if logger != nil {
				logger.Info("reaped orphaned cache entry", "dir", dir)
			}
		}
	}
	return reaped
}

// ReapOrphanedCaches reclaims storhub cache leftovers in the standard
// locations: the git base and the legacy temp-directory pattern. Used by
// hub startup and by `storhub cache prune`.
func ReapOrphanedCaches(logger *slog.Logger) int {
	return reapOrphaned(logger, storcfg.DefaultGitCacheBase(), os.TempDir())
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
