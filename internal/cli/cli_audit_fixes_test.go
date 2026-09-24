package cli

import (
	"context"
	"strings"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/storhub"
)

// TestCpNeverWithExpectedRevisionIsUsageError pins R6: --reflink=never
// streams bytes through plain WriteFileAt with no revision plumbing, so
// pairing it with --expectedrevision would silently drop the CAS guard.
// The combination fails loud as a usage error instead.
func TestCpNeverWithExpectedRevisionIsUsageError(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("hello world")}}
	err := runCpWithFake(t, fake, []string{"cp", "--reflink=never", "--expectedrevision", "rev1", "demo", "a.txt", "b.txt"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("--reflink=never with --expectedrevision must be a usage error, got %v", err)
	}
	if fake.cloneCalls != 0 || fake.writeCalls != 0 {
		t.Fatalf("rejected combination must not copy: clone=%d write=%d", fake.cloneCalls, fake.writeCalls)
	}
}

// TestProjectPruneDryRunAlias pins N3: --dryrun keeps working and
// --dry-run is its alias; both report instead of deleting.
func TestProjectPruneDryRunAlias(t *testing.T) {
	for _, flag := range []string{"--dryrun", "--dry-run"} {
		fake := &fakeHub{t: t}
		app, _, stderr := newTestApp(t)
		app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
			return fake, nil
		}
		if err := app.Run([]string{"project", "prune", flag, "demo"}); err != nil {
			t.Fatalf("prune %s: %v", flag, err)
		}
		if out := stderr(); !strings.Contains(out, "would prune") {
			t.Fatalf("prune %s must report instead of deleting, got %q", flag, out)
		}
	}
}

// TestInvalidLogKnobsFailLoud pins F50 on the CLI half: an unknown
// --loglevel/--logformat (or STORHUB_LOG_LEVEL) fails loudly instead of
// running silently at the fallback verbosity.
func TestInvalidLogKnobsFailLoud(t *testing.T) {
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	err := app.Run([]string{"--loglevel", "loud", "stat", "--token", "x", "demo", "a.txt"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("unknown --loglevel must be a usage error, got %v", err)
	}
	app2, _, _ := newTestApp(t)
	app2.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	err = app2.Run([]string{"stat", "--token", "x", "--logformat", "yaml", "demo", "a.txt"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("unknown --logformat must be a usage error, got %v", err)
	}
}

// TestInvalidLogLevelEnvFailsLoud pins the env half: STORHUB_LOG_LEVEL
// garbage fails loudly at startup, not silently at info.
func TestInvalidLogLevelEnvFailsLoud(t *testing.T) {
	t.Setenv("STORHUB_LOG_LEVEL", "loud")
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	err := app.Run([]string{"stat", "--token", "x", "demo", "a.txt"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("unknown STORHUB_LOG_LEVEL must be a usage error, got %v", err)
	}
}

// TestTouchToleratesExistingCreate pins R10: touch creates first and
// tolerates AlreadyExists (a concurrent touch won the race), so the
// timestamp update always lands without check-then-act.
func TestTouchToleratesExistingCreate(t *testing.T) {
	fake := &touchExistsFake{}
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	if err := app.Run([]string{"touch", "--token", "x", "demo", "docs/f.txt"}); err != nil {
		t.Fatalf("touch on an existing path must succeed, got %v", err)
	}
	if !fake.stamped {
		t.Fatal("touch on an existing path must update timestamps")
	}
}

// touchExistsFake reports every path as already created: CreateFile always
// answers AlreadyExists so the test proves touch falls through to Chtimes.
type touchExistsFake struct {
	hubClient
	stamped bool
}

func (f *touchExistsFake) CreateFileContext(_ context.Context, _, filePath string) (*storhub.FileMetadata, error) {
	return nil, shfs.AlreadyExists(filePath)
}

func (f *touchExistsFake) ChtimesContext(_ context.Context, _, _ string, _, _ int64) error {
	f.stamped = true
	return nil
}

func (f *touchExistsFake) Shutdown(_ context.Context) error { return nil }
