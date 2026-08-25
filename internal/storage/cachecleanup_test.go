package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

func TestCacheBasePrecedence(t *testing.T) {
	t.Run("explicit env wins", func(t *testing.T) {
		t.Setenv("STORHUB_CACHE_DIR", "/data/storhub-cache")
		if got := storcfg.CacheBase(); got != "/data/storhub-cache" {
			t.Fatalf("CacheBase=%q", got)
		}
	})
	t.Run("xdg cache dir is the default", func(t *testing.T) {
		t.Setenv("STORHUB_CACHE_DIR", "")
		xdg := t.TempDir()
		t.Setenv("XDG_CACHE_HOME", xdg)
		if got := storcfg.CacheBase(); got != filepath.Join(xdg, "storhub") {
			t.Fatalf("CacheBase=%q, want %q", got, filepath.Join(xdg, "storhub"))
		}
	})
}

func TestProjectLockLifecycle(t *testing.T) {
	base := t.TempDir()
	if err := claimProjectLock(base, "demo"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	pid := projectLockPid(base, "demo")
	if pid != os.Getpid() {
		t.Fatalf("lock pid=%d, want %d", pid, os.Getpid())
	}
	if projectDirIsOrphan(base, "demo") {
		t.Fatal("own lock must not look orphaned")
	}
	releaseProjectLock(base, "demo")
	if pid := projectLockPid(base, "demo"); pid != 0 {
		t.Fatalf("released lock still reads pid %d", pid)
	}
	if !projectDirIsOrphan(base, "demo") {
		t.Fatal("unclaimed project must be orphan")
	}
}

func TestClaimRefusesLiveForeignLock(t *testing.T) {
	base := t.TempDir()
	lockDir := filepath.Join(base, ".locks")
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// PID 1 always exists; simulate a foreign live owner.
	if err := os.WriteFile(filepath.Join(lockDir, "demo.lock"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := claimProjectLock(base, "demo")
	if err == nil || !strings.Contains(err.Error(), "live process") {
		t.Fatalf("expected live-holder refusal, got %v", err)
	}
}

func TestReapOrphanedRemovesDeadAndSparesLive(t *testing.T) {
	base := t.TempDir()

	// Orphan: unlocked project dir.
	orphan := filepath.Join(base, "orphan-project")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	// Dead-pid locked project dir.
	dead := filepath.Join(base, "dead-project")
	if err := os.MkdirAll(dead, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, ".locks"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A pid guaranteed not to exist: scan down until /proc agrees.
	deadPid := 4194303
	for pidAlive(deadPid) {
		deadPid--
	}
	if err := os.WriteFile(filepath.Join(base, ".locks", "dead-project.lock"), []byte(strconv.Itoa(deadPid)), 0o644); err != nil {
		t.Fatal(err)
	}
	// Live (this process) locked dir must survive.
	live := filepath.Join(base, "live-project")
	if err := os.MkdirAll(live, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := claimProjectLock(base, "live-project"); err != nil {
		t.Fatal(err)
	}
	// Legacy tmp root with dead pid.
	legacyBase := t.TempDir()
	legacy := filepath.Join(legacyBase, "storhub-git-"+strconv.Itoa(deadPid))
	if err := os.MkdirAll(legacy, 0o755); err != nil {
		t.Fatal(err)
	}

	reaped := reapOrphaned(nil, base, legacyBase)
	if reaped < 2 {
		t.Fatalf("expected orphan+dead+legacy reclaimed (>=3 minus none), got %d", reaped)
	}
	for _, gone := range []string{orphan, dead, legacy} {
		if _, err := os.Stat(gone); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s should be gone", gone)
		}
	}
	if _, err := os.Stat(live); err != nil {
		t.Errorf("live-locked dir must survive: %v", err)
	}
}

func TestGitRepoEnsureReclaimsStaleDirectory(t *testing.T) {
	base := t.TempDir()
	url := seedBareMetadataRepo(t)
	r := newGitRepo(base, "owner", "demo", "")
	r.remoteBase = url

	// Plant a stale directory as if a crashed run left it.
	stale := filepath.Join(base, "demo")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "garbage.txt"), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := r.ensure(context.Background()); err != nil {
		t.Fatalf("ensure after stale dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stale, "garbage.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale content must have been reclaimed")
	}
	if pid := projectLockPid(base, "demo"); pid != os.Getpid() {
		t.Fatalf("expected our lock, got %d", pid)
	}

	// Shutdown contract: release(true) removes the whole directory.
	if err := r.release(true); err != nil {
		t.Fatalf("release(remove): %v", err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("release(true) must remove the project dir")
	}
	if pid := projectLockPid(base, "demo"); pid != 0 {
		t.Fatalf("lock must be released, reads pid %d", pid)
	}
}

func TestWrapNoSpaceTyping(t *testing.T) {
	dir := "/cache/git"
	plain := errors.New("write failed")
	if wrapNoSpace(dir, plain) != plain {
		t.Fatal("non-ENOSPC errors must pass through untouched")
	}
	enospc := &os.PathError{Op: "write", Path: dir, Err: syscall.ENOSPC}
	wrapped := wrapNoSpace(dir, enospc)
	var nsErr *noSpaceError
	if !errors.As(wrapped, &nsErr) {
		t.Fatalf("ENOSPC must surface typed noSpaceError, got %T", wrapped)
	}
	if nsErr.Dir != dir || !errors.Is(wrapped, syscall.ENOSPC) {
		t.Fatalf("typed error lost details: %+v", nsErr)
	}
	if !strings.Contains(nsErr.Error(), "STORHUB_CACHE_DIR") {
		t.Fatal("error message must hint at the override knob")
	}
}
