package cli

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/FarelRA/storhub/storhub"
)

func TestAppRunHelpUnknownAndUsageErrors(t *testing.T) {
	app, stdout, stderr := newTestApp(t)
	if err := app.Run(nil); err != nil {
		t.Fatalf("run root help: %v", err)
	}
	if !strings.Contains(stdout(), "StorHub CLI") {
		t.Fatalf("expected root help, got %q", stdout())
	}
	if err := app.Run([]string{"unknown"}); err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected unknown command error, got %v", err)
	}
	if err := app.Run([]string{"upload"}); err == nil || !strings.Contains(err.Error(), "accepts") {
		t.Fatalf("expected upload arg error, got %v", err)
	}
	if err := app.Run([]string{"write", "--token", "x", "project", "file", "nope", "data"}); err == nil || !strings.Contains(err.Error(), "invalid offset") {
		t.Fatalf("expected invalid offset error, got %v", err)
	}
	if err := app.Run([]string{"patch", "--token", "x", "project", "file", "1", "bad", "data"}); err == nil || !strings.Contains(err.Error(), "invalid deletesize") {
		t.Fatalf("expected invalid deletesize error, got %v", err)
	}
	if err := app.Run([]string{"download"}); err == nil || !strings.Contains(err.Error(), "accepts") {
		t.Fatalf("expected download arg error, got %v", err)
	}
	_ = stderr
}

func TestHelpersAndRendering(t *testing.T) {
	restore := setWarnOutput(io.Discard)
	t.Cleanup(restore)
	if formatTime(0) != "-" || !strings.Contains(formatTime(1), "1970") {
		t.Fatal("unexpected formatted time")
	}
	if _, err := newHubFromFlags(context.Background(), "", "", 0, false, logSettings{}); err == nil {
		t.Fatal("expected missing token error")
	}
	hub, err := newHubFromFlags(context.Background(), "token", "https://example.test/api/", 64<<20, true, logSettings{})
	if err != nil || hub == nil {
		t.Fatalf("newHubFromFlags: %v", err)
	}
	_ = hub
	var buf bytes.Buffer
	printFileSummary(&buf, "uploaded", nil)
	if !strings.Contains(buf.String(), "uploaded") {
		t.Fatalf("unexpected nil file summary: %q", buf.String())
	}
	buf.Reset()
	printFileSummary(&buf, "uploaded", &storhub.FileMetadata{Size: 3, Inode: 1, Mode: 0o644})
	if !strings.Contains(buf.String(), "size: 3 bytes") {
		t.Fatalf("unexpected file summary: %q", buf.String())
	}
	buf.Reset()
	printDirEntries(&buf, nil, false)
	if buf.String() != "" {
		t.Fatalf("empty dir listing must print nothing, got %q", buf.String())
	}
	buf.Reset()
	printDirEntries(&buf, []storhub.DirEntry{{Name: "b", Path: "b"}, {Name: "a", Path: "a", IsDir: true}, {Name: "c", Path: "c", IsSymlink: true}}, true)
	if !strings.Contains(buf.String(), "dir") || !strings.Contains(buf.String(), "symlink") {
		t.Fatalf("unexpected long dir listing: %q", buf.String())
	}
	buf.Reset()
	printEntryInfo(&buf, nil)
	if strings.TrimSpace(buf.String()) != "not found" {
		t.Fatalf("unexpected nil entry info: %q", buf.String())
	}
	buf.Reset()
	printEntryInfo(&buf, &storhub.EntryInfo{Path: "docs/a", IsSymlink: true, Inode: 1, Size: 3, Mode: 0o777, UID: 1, GID: 2, NLink: 1, ModifiedAt: 1, AccessedAt: 2, ChangedAt: 3, SymlinkTarget: "target"})
	if !strings.Contains(buf.String(), "target: target") || entryKind(&storhub.EntryInfo{IsDir: true}) != "directory" || entryKind(&storhub.EntryInfo{IsSymlink: true}) != "symlink" || entryKind(&storhub.EntryInfo{}) != "file" {
		t.Fatalf("unexpected entry rendering: %q", buf.String())
	}
	buf.Reset()
	printRevisions(&buf, nil)
	// Silence is golden: empty history renders nothing, like ls(1).
	if buf.String() != "" {
		t.Fatalf("unexpected empty revisions: %q", buf.String())
	}
	buf.Reset()
	printRevisions(&buf, []storhub.MetadataRevision{{CommitSHA: "1234567890abcdef", Message: "msg", CommittedAt: 4}})
	if !strings.Contains(buf.String(), "1234567890") || !strings.Contains(buf.String(), "msg") {
		t.Fatalf("unexpected revisions rendering: %q", buf.String())
	}
}

func TestAppSmokeForTokenValidationAcrossCommands(t *testing.T) {
	app, _, _ := newTestApp(t)
	checks := []struct {
		name string
		args []string
		want string
	}{
		{name: "ls", args: []string{"ls", "project"}, want: "missing GitHub token"},
		{name: "stat", args: []string{"stat", "project", "path"}, want: "missing GitHub token"},
		{name: "cat", args: []string{"cat", "project", "path"}, want: "missing GitHub token"},
		{name: "mkdir", args: []string{"mkdir", "project", "path"}, want: "missing GitHub token"},
		{name: "rm", args: []string{"rm", "project", "path"}, want: "missing GitHub token"},
		{name: "mv", args: []string{"mv", "project", "old", "new"}, want: "missing GitHub token"},
		{name: "append", args: []string{"append", "project", "path", "text"}, want: "missing GitHub token"},
		{name: "write", args: []string{"write", "project", "path", "0", "text"}, want: "missing GitHub token"},
		{name: "patch", args: []string{"patch", "project", "path", "0", "0", "text"}, want: "missing GitHub token"},
		{name: "truncate", args: []string{"truncate", "project", "path", "0"}, want: "missing GitHub token"},
		{name: "chmod", args: []string{"chmod", "project", "path", "640"}, want: "missing GitHub token"},
		{name: "chown", args: []string{"chown", "project", "path", "1", "2"}, want: "missing GitHub token"},
		{name: "touch", args: []string{"touch", "project", "path"}, want: "missing GitHub token"},
		{name: "symlink", args: []string{"symlink", "project", "target", "link"}, want: "missing GitHub token"},
		{name: "readlink", args: []string{"readlink", "project", "path"}, want: "missing GitHub token"},
		{name: "link", args: []string{"link", "project", "old", "new"}, want: "missing GitHub token"},
		{name: "sync", args: []string{"project", "sync", "project"}, want: "missing GitHub token"},
		{name: "revisions", args: []string{"project", "revisions", "project"}, want: "missing GitHub token"},
		{name: "rollback", args: []string{"project", "rollback", "project", "sha"}, want: "invalid commit SHA"},
		{name: "rest", args: []string{"rest"}, want: "missing GitHub token"},
		{name: "serve", args: []string{"serve", "project", t.TempDir()}, want: "missing GitHub token"},
		{name: "mount", args: []string{"mount", "project", t.TempDir()}, want: "missing GitHub token"},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := app.Run(check.args); err == nil || !strings.Contains(err.Error(), check.want) {
				t.Fatalf("expected %q, got %v", check.want, err)
			}
		})
	}
}

func TestPrintRootHelp(t *testing.T) {
	app, stdout, _ := newTestApp(t)
	app.rootCmd.SetOut(app.stdout)
	_ = app.rootCmd.Help()
	if !strings.Contains(stdout(), "StorHub CLI") {
		t.Fatalf("unexpected root help output: %q", stdout())
	}
}

func TestListAcceptsAbsolutePath(t *testing.T) {
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return &fakeHub{t: t, assertReadDirPath: "docs/readme.txt"}, nil
	}
	if err := app.Run([]string{"ls", "--token", "x", "demo", "/docs/readme.txt"}); err != nil {
		t.Fatalf("run ls with absolute path: %v", err)
	}
}

func TestHelpAndCompletionWorkWithoutToken(t *testing.T) {
	t.Run("root --help", func(t *testing.T) {
		app, stdout, _ := newTestApp(t)
		if err := app.Run([]string{}); err != nil {
			t.Fatalf("root with no args should show help: %v", err)
		}
		if !strings.Contains(stdout(), "StorHub CLI") {
			t.Fatalf("expected help output, got %q", stdout())
		}
	})

	t.Run("help subcommand", func(t *testing.T) {
		app, stdout, _ := newTestApp(t)
		if err := app.Run([]string{"help"}); err != nil {
			t.Fatalf("help subcommand: %v", err)
		}
		if !strings.Contains(stdout(), "StorHub CLI") {
			t.Fatalf("expected help output, got %q", stdout())
		}
	})

	t.Run("command --help", func(t *testing.T) {
		app, stdout, _ := newTestApp(t)
		if err := app.Run([]string{"upload", "--help"}); err != nil {
			t.Fatalf("command --help: %v", err)
		}
		if !strings.Contains(stdout(), "Upload") {
			t.Fatalf("expected upload help output, got %q", stdout())
		}
	})

	t.Run("completion subcommand", func(t *testing.T) {
		app, stdout, _ := newTestApp(t)
		if err := app.Run([]string{"completion", "bash"}); err != nil {
			t.Fatalf("completion subcommand: %v", err)
		}
		if !strings.Contains(stdout(), "#") {
			t.Fatalf("expected bash completion output, got %q", stdout())
		}
	})
}

func TestFlagParsingAcrossCommands(t *testing.T) {
	t.Run("flags before subcommand", func(t *testing.T) {
		t.Setenv("GITHUB_TOKEN", "envtoken")
		app, _, _ := newTestApp(t)
		app.seams.newHub = func(_ context.Context, token, apiBase string, _ int64, _ bool, _ logSettings) (hubClient, error) {
			if token != "envtoken" {
				t.Fatalf("expected token envtoken, got %q", token)
			}
			if apiBase != "" {
				t.Fatalf("expected empty apibase, got %q", apiBase)
			}
			return &fakeHub{t: t}, nil
		}
		if err := app.Run([]string{"ls", "demo"}); err != nil {
			t.Fatalf("ls with env token: %v", err)
		}
	})

	t.Run("persistent flag overrides env", func(t *testing.T) {
		app, _, _ := newTestApp(t)
		app.seams.newHub = func(_ context.Context, token, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
			if token != "override" {
				t.Fatalf("expected token override, got %q", token)
			}
			return &fakeHub{t: t}, nil
		}
		t.Setenv("GITHUB_TOKEN", "envtoken")
		if err := app.Run([]string{"ls", "--token", "override", "demo"}); err != nil {
			t.Fatalf("ls with override token: %v", err)
		}
	})

	t.Run("local flags", func(t *testing.T) {
		app, stdout, _ := newTestApp(t)
		app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
			return &fakeHub{t: t}, nil
		}
		if err := app.Run([]string{"ls", "--token", "x", "-l", "demo"}); err != nil {
			t.Fatalf("ls -l with token: %v", err)
		}
		if !strings.Contains(stdout(), "file") {
			t.Fatalf("expected long listing, got %q", stdout())
		}
	})
}

func TestArgValidationErrors(t *testing.T) {
	app, _, _ := newTestApp(t)

	t.Run("exact args", func(t *testing.T) {
		err := app.Run([]string{"upload"})
		if err == nil || !strings.Contains(err.Error(), "accepts 3 arg(s), received 0") {
			t.Fatalf("expected arg count error, got %v", err)
		}
	})

	t.Run("range args", func(t *testing.T) {
		err := app.Run([]string{"ls"})
		if err == nil || !strings.Contains(err.Error(), "accepts between 1 and 2 arg(s), received 0") {
			t.Fatalf("expected arg range error, got %v", err)
		}
	})

	t.Run("no args", func(t *testing.T) {
		err := app.Run([]string{"rest", "extra"})
		if err == nil || !strings.Contains(err.Error(), "unknown command") {
			t.Fatalf("expected unknown command error, got %v", err)
		}
	})

	t.Run("unknown flag", func(t *testing.T) {
		err := app.Run([]string{"upload", "--bogus"})
		if err == nil || !strings.Contains(err.Error(), "unknown flag") {
			t.Fatalf("expected unknown flag error, got %v", err)
		}
	})
}
