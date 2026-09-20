package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/storhub"
)

// posix_test.go: one test per POSIX command plus the modifier flags.
// Every test drives the real cobra RunE path with the hub seam pointed at
// a fake; the fake only stands in for networked storage.

// runPosixCLI runs one CLI invocation against fakeHub and returns stderr.
func runPosixCLI(t *testing.T, fake *fakeHub, args []string) string {
	t.Helper()
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, stderr := newTestApp(t)
	if err := app.Run(args); err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return stderr()
}

func TestTruncateCommand(t *testing.T) {
	fake := &fakeHub{t: t}
	out := runPosixCLI(t, fake, []string{"truncate", "--token", "x", "demo", "docs/f.txt", "0"})
	if !strings.Contains(out, "truncated") {
		t.Fatalf("truncate must report, got %q", out)
	}
	app, _, _ := newTestApp(t)
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	if err := app.Run([]string{"truncate", "--token", "x", "demo", "docs/f.txt", "-1"}); err == nil || !IsUsageError(err) {
		t.Fatalf("negative size must be a usage error, got %v", err)
	}
	if err := app.Run([]string{"truncate", "--token", "x", "demo", "docs/f.txt", "nope"}); err == nil || !IsUsageError(err) {
		t.Fatalf("non-numeric size must be a usage error, got %v", err)
	}
}

func TestTruncateSyncDrains(t *testing.T) {
	fake := &fakeHub{t: t}
	runPosixCLI(t, fake, []string{"truncate", "--token", "x", "--sync", "demo", "docs/f.txt", "3"})
	if len(fake.drainCalls) != 1 || fake.drainCalls[0] != "demo" {
		t.Fatalf("truncate --sync must drain demo once, got %v", fake.drainCalls)
	}
}

func TestChmodCommand(t *testing.T) {
	fake := &fakeHub{t: t}
	out := runPosixCLI(t, fake, []string{"chmod", "--token", "x", "demo", "docs/f.txt", "640"})
	if !strings.Contains(out, "0640") {
		t.Fatalf("chmod must report the mode, got %q", out)
	}
	app, _, _ := newTestApp(t)
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	for _, bad := range []string{"888", "10000", "abc", ""} {
		if err := app.Run([]string{"chmod", "--token", "x", "demo", "docs/f.txt", bad}); err == nil || !IsUsageError(err) {
			t.Fatalf("chmod %q must be a usage error, got %v", bad, err)
		}
	}
}

func TestChownCommand(t *testing.T) {
	fake := &fakeHub{t: t}
	out := runPosixCLI(t, fake, []string{"chown", "--token", "x", "demo", "docs/f.txt", "--", "-1", "100"})
	if !strings.Contains(out, "4294967295:100") {
		t.Fatalf("chown must report the -1 keep sentinel as uint32 max, got %q", out)
	}
	app, _, _ := newTestApp(t)
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	if err := app.Run([]string{"chown", "--token", "x", "demo", "docs/f.txt", "abc", "0"}); err == nil || !IsUsageError(err) {
		t.Fatalf("non-numeric uid must be a usage error, got %v", err)
	}
}

func TestTouchCommand(t *testing.T) {
	fake := &fakeHub{t: t}
	// fakeHub.StatPath always succeeds, so this exercises the
	// update-timestamps branch.
	out := runPosixCLI(t, fake, []string{"touch", "--token", "x", "demo", "docs/f.txt"})
	if !strings.Contains(out, "touched") {
		t.Fatalf("touch must report, got %q", out)
	}
	app, _, _ := newTestApp(t)
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	if err := app.Run([]string{"touch", "--token", "x", "demo", "docs/f.txt", "--mtime-ns", "-2"}); err == nil || !IsUsageError(err) {
		t.Fatalf("--mtime-ns -2 must be a usage error, got %v", err)
	}
}

func TestTouchNoCreateSkipsMissing(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &touchFake{statErr: shfs.ErrNotFound}
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, stderr := newTestApp(t)
	if err := app.Run([]string{"touch", "--token", "x", "--no-create", "demo", "docs/missing.txt"}); err != nil {
		t.Fatalf("touch --no-create on missing must succeed silently, got %v", err)
	}
	if fake.created || fake.stamped {
		t.Fatal("touch --no-create must neither create nor stamp a missing file")
	}
	if got := stderr(); strings.Contains(got, "touched") {
		t.Fatalf("touch --no-create must stay silent, got %q", got)
	}
}

func TestTouchCreatesMissing(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &touchFake{statErr: shfs.ErrNotFound}
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, _ := newTestApp(t)
	if err := app.Run([]string{"touch", "--token", "x", "demo", "docs/missing.txt"}); err != nil {
		t.Fatalf("touch on missing must create, got %v", err)
	}
	if !fake.created || !fake.stamped {
		t.Fatal("touch on missing must create and stamp")
	}
}

// touchFake observes the create/stamp sequence behind touch.
type touchFake struct {
	hubClient
	statErr error
	created bool
	stamped bool
}

func (f *touchFake) StatPathContext(_ context.Context, _, targetPath string) (*storhub.EntryInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	return &storhub.EntryInfo{Path: targetPath}, nil
}

func (f *touchFake) CreateFileContext(_ context.Context, _, _ string) (*storhub.FileMetadata, error) {
	f.created = true
	return &storhub.FileMetadata{}, nil
}

func (f *touchFake) ChtimesContext(_ context.Context, _, _ string, _, _ int64) error {
	f.stamped = true
	return nil
}

func (f *touchFake) Shutdown(_ context.Context) error { return nil }

func TestSymlinkReadlinkLinkCommands(t *testing.T) {
	fake := &fakeHub{t: t}
	out := runPosixCLI(t, fake, []string{"symlink", "--token", "x", "demo", "docs/f.txt", "docs/alias.txt"})
	if !strings.Contains(out, "symlinked") {
		t.Fatalf("symlink must report, got %q", out)
	}
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	app, stdout, _ := newTestApp(t)
	if err := app.Run([]string{"readlink", "--token", "x", "demo", "docs/alias.txt"}); err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if got := strings.TrimSpace(stdout()); got != "target" {
		t.Fatalf("readlink must print the bare target, got %q", got)
	}
	out = runPosixCLI(t, fake, []string{"link", "--token", "x", "demo", "docs/f.txt", "docs/hard.txt"})
	if !strings.Contains(out, "linked") {
		t.Fatalf("link must report, got %q", out)
	}
}

func TestSyncCommand(t *testing.T) {
	fake := &fakeHub{t: t}
	out := runPosixCLI(t, fake, []string{"project", "sync", "--token", "x", "demo"})
	if !strings.Contains(out, "synced demo") {
		t.Fatalf("sync must report the project, got %q", out)
	}
	if len(fake.drainCalls) != 1 || fake.drainCalls[0] != "demo" {
		t.Fatalf("sync must drain demo once, got %v", fake.drainCalls)
	}
}

func TestSyncCommandPropagatesDrainError(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &fakeHub{t: t, drainErr: errors.New("drain boom")}
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, _ := newTestApp(t)
	if err := app.Run([]string{"project", "sync", "--token", "x", "demo"}); err == nil || !strings.Contains(err.Error(), "drain boom") {
		t.Fatalf("drain failure must propagate, got %v", err)
	}
}

// casHub records the mutate options threaded by the modifier flags.
type casHub struct {
	hubClient
	appendRev, writeRev, patchRev, truncateRev, replaceRev, rmRev, rmdirRev, mvRev string
	noReplace                                                                      bool
}

func (h *casHub) Shutdown(_ context.Context) error { return nil }

func (h *casHub) AppendFileContext(_ context.Context, _, _ string, _ []byte, opts ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.appendRev = shfs.ApplyMutateOptions(opts).ExpectedRevision()
	return &storhub.FileMetadata{}, nil
}

func (h *casHub) WriteFileAtContext(_ context.Context, _, _ string, _ int64, _ []byte, opts ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.writeRev = shfs.ApplyMutateOptions(opts).ExpectedRevision()
	return &storhub.FileMetadata{}, nil
}

func (h *casHub) PatchFileContext(_ context.Context, _, _ string, _, _ int64, _ []byte, opts ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.patchRev = shfs.ApplyMutateOptions(opts).ExpectedRevision()
	return &storhub.FileMetadata{}, nil
}

func (h *casHub) TruncateFileContext(_ context.Context, _, _ string, _ int64, opts ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.truncateRev = shfs.ApplyMutateOptions(opts).ExpectedRevision()
	return &storhub.FileMetadata{}, nil
}

func (h *casHub) ReplaceFileContext(_ context.Context, _, _, _ string, opts ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.replaceRev = shfs.ApplyMutateOptions(opts).ExpectedRevision()
	return &storhub.FileMetadata{}, nil
}

func (h *casHub) DeleteFileContext(_ context.Context, _, _ string, opts ...storhub.MutateOption) error {
	h.rmRev = shfs.ApplyMutateOptions(opts).ExpectedRevision()
	return nil
}

func (h *casHub) RmdirContext(_ context.Context, _, _ string, opts ...storhub.MutateOption) error {
	h.rmdirRev = shfs.ApplyMutateOptions(opts).ExpectedRevision()
	return nil
}

func (h *casHub) RenameContext(_ context.Context, _, _, _ string, opts ...storhub.MutateOption) error {
	cfg := shfs.ApplyMutateOptions(opts)
	h.mvRev = cfg.ExpectedRevision()
	h.noReplace = cfg.NoReplace()
	return nil
}

func runWithCasHub(t *testing.T, hub *casHub, args []string) {
	t.Helper()
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return hub, nil
	}
	app, _, _ := newTestApp(t)
	if err := app.Run(args); err != nil {
		t.Fatalf("%v: %v", args, err)
	}
}

func TestExpectedRevisionThreadsToVerbs(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		rev  string
		want func(h *casHub) string
	}{
		{name: "append", args: []string{"append", "--token", "x", "--expected-revision", "rev-1", "demo", "f", "data"}, rev: "rev-1", want: func(h *casHub) string { return h.appendRev }},
		{name: "write", args: []string{"write", "--token", "x", "--expected-revision", "rev-2", "demo", "f", "0", "data"}, rev: "rev-2", want: func(h *casHub) string { return h.writeRev }},
		{name: "patch", args: []string{"patch", "--token", "x", "--expected-revision", "rev-3", "demo", "f", "0", "0", "data"}, rev: "rev-3", want: func(h *casHub) string { return h.patchRev }},
		{name: "truncate", args: []string{"truncate", "--token", "x", "--expected-revision", "rev-4", "demo", "f", "9"}, rev: "rev-4", want: func(h *casHub) string { return h.truncateRev }},
		{name: "rm", args: []string{"rm", "--token", "x", "--expected-revision", "rev-5", "demo", "f"}, rev: "rev-5", want: func(h *casHub) string { return h.rmRev }},
		{name: "rm-r", args: []string{"rm", "-r", "--token", "x", "--expected-revision", "rev-6", "demo", "d"}, rev: "rev-6", want: func(h *casHub) string { return h.rmdirRev }},
		{name: "mv", args: []string{"mv", "--token", "x", "--expected-revision", "rev-7", "demo", "a", "b"}, rev: "rev-7", want: func(h *casHub) string { return h.mvRev }},
	} {
		hub := &casHub{}
		runWithCasHub(t, hub, tc.args)
		if got := tc.want(hub); got != tc.rev {
			t.Fatalf("%s: revision: want %q, got %q", tc.name, tc.rev, got)
		}
	}
}

func TestExpectedRevisionRmBranches(t *testing.T) {
	hub := &casHub{}
	runWithCasHub(t, hub, []string{"rm", "--token", "x", "--expected-revision", "rev-5", "demo", "f"})
	if hub.rmRev != "rev-5" {
		t.Fatalf("rm revision: want rev-5, got %q", hub.rmRev)
	}
	hub2 := &casHub{}
	runWithCasHub(t, hub2, []string{"rm", "-r", "--token", "x", "--expected-revision", "rev-6", "demo", "d"})
	if hub2.rmdirRev != "rev-6" {
		t.Fatalf("rm -r revision: want rev-6, got %q", hub2.rmdirRev)
	}
}

func TestReplaceExpectedRevision(t *testing.T) {
	local := filepath.Join(t.TempDir(), "rep.txt")
	if err := os.WriteFile(local, []byte("v2"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	hub := &casHub{}
	runWithCasHub(t, hub, []string{"replace", "--token", "x", "--expected-revision", "rev-r", "demo", "f", local})
	if hub.replaceRev != "rev-r" {
		t.Fatalf("replace revision: want rev-r, got %q", hub.replaceRev)
	}
}

func TestNoReplaceThreadsToRename(t *testing.T) {
	hub := &casHub{}
	runWithCasHub(t, hub, []string{"mv", "--token", "x", "--no-replace", "demo", "a", "b"})
	if !hub.noReplace {
		t.Fatal("mv --no-replace must thread WithNoReplace to RenameContext")
	}
}

func TestUploadExclusiveGate(t *testing.T) {
	// Exclusive upload gates on the atomic create: an existing path fails
	// before any bytes move.
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	gate := &exclusiveHub{exists: true}
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return gate, nil
	}
	local := filepath.Join(t.TempDir(), "payload.txt")
	if err := os.WriteFile(local, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	app, _, _ := newTestApp(t)
	if err := app.Run([]string{"upload", "--token", "x", "--exclusive", "demo", "docs/f.txt", local}); err == nil {
		t.Fatal("exclusive upload over an existing path must fail")
	}
	if gate.uploaded {
		t.Fatal("failed exclusive gate must not upload bytes")
	}
	gate.exists = false
	app2, _, _ := newTestApp(t)
	if err := app2.Run([]string{"upload", "--token", "x", "--exclusive", "demo", "docs/f.txt", local}); err != nil {
		t.Fatalf("exclusive upload of a missing path must succeed, got %v", err)
	}
	if !gate.uploaded {
		t.Fatal("exclusive upload of a missing path must upload bytes")
	}
}

// exclusiveHub fails CreateFile when the path exists, like real storage.
type exclusiveHub struct {
	hubClient
	exists   bool
	uploaded bool
}

func (h *exclusiveHub) CreateFileContext(_ context.Context, _, filePath string) (*storhub.FileMetadata, error) {
	if h.exists {
		return nil, shfs.AlreadyExists(filePath)
	}
	return &storhub.FileMetadata{}, nil
}

func (h *exclusiveHub) UploadFileContext(_ context.Context, _, _, _ string) (*storhub.FileMetadata, error) {
	h.uploaded = true
	return &storhub.FileMetadata{}, nil
}

func (h *exclusiveHub) Shutdown(_ context.Context) error { return nil }

func TestParseChmodMode(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want uint32
	}{
		{"640", 0o640},
		{"0640", 0o640},
		{"4755", 0o4755},
		{"7777", 0o7777},
		{"0", 0},
	} {
		got, err := parseChmodMode(tc.raw)
		if err != nil || got != tc.want {
			t.Fatalf("parseChmodMode(%q) = %#o, %v; want %#o", tc.raw, got, err, tc.want)
		}
	}
	for _, bad := range []string{"", "888", "10000", "abc", "-1"} {
		if _, err := parseChmodMode(bad); err == nil || !IsUsageError(err) {
			t.Fatalf("parseChmodMode(%q) must be a usage error, got %v", bad, err)
		}
	}
}

func TestParseChownID(t *testing.T) {
	got, err := parseChownID("-1", "uid")
	if err != nil || got != ^uint32(0) {
		t.Fatalf("parseChownID(-1) = %d, %v; want max uint32", got, err)
	}
	got, err = parseChownID("1000", "uid")
	if err != nil || got != 1000 {
		t.Fatalf("parseChownID(1000) = %d, %v", got, err)
	}
	for _, bad := range []string{"", "abc", "-2", "4294967296"} {
		if _, err := parseChownID(bad, "uid"); err == nil || !IsUsageError(err) {
			t.Fatalf("parseChownID(%q) must be a usage error, got %v", bad, err)
		}
	}
}

func TestSessionStatSurfacesStale(t *testing.T) {
	// The Stale bit has not landed from the parallel change yet, so the
	// output must carry stale:false today and stale:true automatically
	// once SessionStat gains the field (via the reflection shim).
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &fakeHub{t: t}
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	app, stdout, _ := newTestApp(t)
	if err := app.Run([]string{"session", "open", "--token", "x", "demo", "--mode", "w"}); err != nil {
		t.Fatalf("open: %v", err)
	}
	handle := strings.TrimSpace(stdout())
	if handle == "" {
		t.Fatal("open printed no handle")
	}
	statApp, statOut, _ := newTestApp(t)
	if err := statApp.Run([]string{"session", "stat", "--token", "x", "--json", "--handle", handle}); err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := statOut(); !strings.Contains(got, `"stale":false`) {
		t.Fatalf("session stat json must carry the stale bit, got %s", got)
	}
}
