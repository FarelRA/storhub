package fusefs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/test"
)

// pcScenarioBudget bounds one scenario. A wedged mount (for example the
// concurrent-append commit deadlock documented in the program notes) must
// surface as a loud per-scenario FAIL, never as a hung suite. Enforced by
// test.RunWithBudget; the timeout flag below drives lazy teardown.
const pcScenarioBudget = 45 * time.Second

// pcTeardownBudget bounds unmount plus server close after one scenario. A
// wedged mount can stall teardown behind the same stuck requests, so this
// (not the scenario budget) is what keeps the suite total predictable:
// worst case per scenario is budget plus lazy-detach plus this. Derived
// from the teardown ladder: worst-case sequential teardown is 18 patience
// units (rest shutdown 2 + unmount 6 + mount join 2 + rest join 2 + hub
// drain 6), so the harness budgets 24 units of margin above it.
const pcTeardownBudget = 24 * storcfg.PatienceUnit

// pcRunScenario mounts a fresh Hub through the real FUSE layer, runs one
// table scenario against real mount syscalls with a hang budget, and tears
// the mount down. A fresh mount per scenario keeps one wedged scenario
// from denying signal from the rest: scenario paths are unique per the
// harness, and so is the server behind them here.
func pcRunScenario(t *testing.T, index, total int, sc test.Scenario) test.Result {
	t.Helper()
	fail := func(format string, args ...any) test.Result {
		return test.Result{Name: sc.Name, Pass: false, Error: fmt.Sprintf(format, args...)}
	}
	t.Logf("scenario %d/%d %s: mounting", index+1, total, sc.Name)
	parent, err := os.MkdirTemp("", "pc-fuse-conform-*")
	if err != nil {
		return fail("mktemp: %v", err)
	}
	// Best-effort removal; a wedged mount can keep its mountpoint busy
	// until process exit.
	defer func() { _ = os.RemoveAll(parent) }()
	cacheDir := filepath.Join(parent, "cache")
	mountPoint := filepath.Join(parent, "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		return fail("mkdir mountpoint: %v", err)
	}
	// Same construction pattern as mustMount: New over a test Hub with a
	// fresh cache dir.
	fsys, err := New(newPCHub(), "demo", Options{CacheDir: cacheDir})
	if err != nil {
		return fail("new filesystem: %v", err)
	}
	if err := fsys.Mount(mountPoint); err != nil {
		_ = fsys.Close()
		return fail("mount: %v", err)
	}
	// The mount call returns once the mount is established, but wait for
	// the root to answer before running the scenario.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := os.Stat(mountPoint)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = fsys.Unmount()
			_ = fsys.Close()
			return fail("mount point never became ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	result := test.RunWithBudget(&pcSurface{mount: mountPoint}, []test.Scenario{sc}, pcScenarioBudget)[0]
	if result.TimedOut {
		pcLazyUnmount(mountPoint)
	}
	// Teardown itself runs bounded: on a wedged mount Unmount/Close can
	// block behind the same stuck requests that tripped the budget
	// (commit-path deadlock family, owned by the earlier stateful-handle work). The lazy detach
	// above already aborted the connection, so abandoning teardown only
	// leaks reaping to process exit, never hangs the suite.
	teardownDone := make(chan struct{})
	go func() {
		defer close(teardownDone)
		if err := fsys.Unmount(); err != nil && !result.TimedOut {
			t.Logf("scenario %s: unmount: %v", sc.Name, err)
		}
		if err := fsys.Close(); err != nil {
			t.Logf("scenario %s: close: %v", sc.Name, err)
		}
	}()
	select {
	case <-teardownDone:
	case <-time.After(pcTeardownBudget):
		if !result.TimedOut {
			pcLazyUnmount(mountPoint)
		}
		t.Logf("scenario %s: teardown stuck, abandoned (reaped at process exit)", sc.Name)
	}
	return result
}

// pcPreflight proves the environment can carry a FUSE mount at all. A
// failure here is environmental (no usable /dev/fuse), so the suite skips
// instead of failing every scenario on setup.
func pcPreflight(t *testing.T) {
	t.Helper()
	parent, err := os.MkdirTemp("", "pc-fuse-preflight-*")
	if err != nil {
		t.Skipf("posix conformance over FUSE needs a temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(parent) }()
	mountPoint := filepath.Join(parent, "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Skipf("posix conformance over FUSE needs a mountpoint: %v", err)
	}
	fsys, err := New(newPCHub(), "demo", Options{CacheDir: filepath.Join(parent, "cache")})
	if err != nil {
		t.Skipf("posix conformance over FUSE needs a filesystem: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	if err := fsys.Mount(mountPoint); err != nil {
		t.Skipf("posix conformance over FUSE needs a working mount, mount failed: %v", err)
	}
	defer func() { _ = fsys.Unmount() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(mountPoint); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Skipf("posix conformance over FUSE needs a ready mount: %v", err)
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestPosixConformFUSE runs the shared conformance table against real
// mount syscalls, one fresh mount per scenario. A fresh mount per scenario
// keeps one wedged scenario from denying signal from the rest. The test
// fails iff any scenario fails, and the per-scenario table prints loudly
// either way. Environments without a working FUSE skip instead of faking
// results.
func TestPosixConformFUSE(t *testing.T) {
	test.RequireConformance(t)
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("posix conformance over FUSE needs /dev/fuse: %v", err)
	}
	pcPreflight(t)
	table := test.Filter(test.Table, test.SurfaceFUSE)
	total := len(table)
	t.Logf("posix conformance over FUSE: %d scenarios, fresh mount each", total)
	// PC_ONLY focuses one scenario by exact name (e.g. debugging a single
	// RED case without paying for 30 fresh mounts). Empty runs the table.
	only := test.ConformOnly()
	results := make([]test.Result, 0, total)
	for i, sc := range table {
		if only != "" && sc.Name != only {
			continue
		}
		result := pcRunScenario(t, i, total, sc)
		results = append(results, result)
		if result.Pass {
			t.Logf("PASS %s", result.Name)
		} else {
			t.Errorf("FAIL %s: %s", result.Name, result.Error)
		}
	}
	passed, failed := test.Summary(results)
	t.Logf("posixconform FUSE: %d passed, %d failed, %d total", passed, failed, len(results))
	if failed > 0 {
		t.Fatalf("%d scenario(s) failed over the FUSE mount", failed)
	}
}
