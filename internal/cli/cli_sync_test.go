package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Phase 3 sync opt-in: every mutating command accepts --sync and drains the
// project's journal after success. Async stays the default: no drain runs
// unless --sync is passed.

// TestCLISyncDrainsAfterMkdir pins the core contract: --sync drains once
// for the project after a successful mutation.
func TestCLISyncDrainsAfterMkdir(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &fakeHub{t: t}
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, _ := newTestApp(t)
	if err := app.Run([]string{"mkdir", "--token", "x", "--sync", "demo", "docs"}); err != nil {
		t.Fatalf("mkdir --sync: %v", err)
	}
	if len(fake.drainCalls) != 1 || fake.drainCalls[0] != "demo" {
		t.Fatalf("expected one drain call for demo, got %v", fake.drainCalls)
	}
}

// TestCLIDefaultSkipsDrain pins the no-behavior-change half: without --sync
// no drain runs.
func TestCLIDefaultSkipsDrain(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &fakeHub{t: t}
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, _ := newTestApp(t)
	if err := app.Run([]string{"mkdir", "--token", "x", "demo", "docs"}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if len(fake.drainCalls) != 0 {
		t.Fatalf("default paths must not drain, got %v", fake.drainCalls)
	}
}

// TestCLISyncDrainFailureLoud pins the failure contract: a failed drain is
// a failed command (non-nil error) naming the project.
func TestCLISyncDrainFailureLoud(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &fakeHub{t: t, drainErr: errors.New("drain demo: commit failed")}
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, _ := newTestApp(t)
	err := app.Run([]string{"mkdir", "--token", "x", "--sync", "demo", "docs"})
	if err == nil {
		t.Fatal("drain failure must fail the command")
	}
	if !strings.Contains(err.Error(), "demo") {
		t.Fatalf("drain failure must name the project, got %v", err)
	}
}

// TestCLISyncCoversEveryMutation walks every mutating command with --sync
// and asserts exactly one drain call. Read-only commands (download, ls,
// stat, cat, revisions), long-running surfaces (mount, rest, serve), and
// local-only maintenance (cache prune) take no --sync flag by design.
func TestCLISyncCoversEveryMutation(t *testing.T) {
	localFile := filepath.Join(t.TempDir(), "upload.txt")
	if err := os.WriteFile(localFile, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	cases := []struct {
		name string
		args []string
	}{
		{name: "upload", args: []string{"upload", "--token", "x", "--sync", "demo", "docs/readme.txt", localFile}},
		{name: "replace", args: []string{"replace", "--token", "x", "--sync", "demo", "docs/readme.txt", localFile}},
		{name: "mkdir", args: []string{"mkdir", "--token", "x", "--sync", "demo", "docs"}},
		{name: "rm-file", args: []string{"rm", "--token", "x", "--sync", "demo", "docs/readme.txt"}},
		{name: "rm-dir", args: []string{"rm", "--token", "x", "--sync", "-r", "demo", "docs"}},
		{name: "mv", args: []string{"mv", "--token", "x", "--sync", "demo", "docs/a.txt", "docs/b.txt"}},
		{name: "append", args: []string{"append", "--token", "x", "--sync", "demo", "docs/log.txt", "tail"}},
		{name: "write", args: []string{"write", "--token", "x", "--sync", "demo", "docs/f.txt", "1", "x"}},
		{name: "patch", args: []string{"patch", "--token", "x", "--sync", "demo", "docs/f.txt", "1", "2", "x"}},
		{name: "rollback", args: []string{"rollback", "--token", "x", "--sync", "demo", "deadbeef"}},
		{name: "purge", args: []string{"purge", "--token", "x", "--sync", "demo"}},
		{name: "prune", args: []string{"prune", "--token", "x", "--sync", "demo", "objects"}},
		{name: "delete-project", args: []string{"delete-project", "--token", "x", "--sync", "--yes", "demo"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oldFactory := newHubFromFlagsFn
			t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
			fake := &fakeHub{t: t}
			newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
				return fake, nil
			}
			// A fresh App per case keeps cobra flag state isolated:
			// --sync must not leak between invocations.
			app, _, _ := newTestApp(t)
			if err := app.Run(tc.args); err != nil {
				t.Fatalf("%s --sync: %v", tc.name, err)
			}
			if len(fake.drainCalls) != 1 || fake.drainCalls[0] != "demo" {
				t.Fatalf("%s --sync must drain demo exactly once, got %v", tc.name, fake.drainCalls)
			}
		})
	}
}
