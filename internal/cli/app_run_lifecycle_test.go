package cli

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FarelRA/storhub/storhub"
)

func TestAppCommandSuccessPathsWithMockHub(t *testing.T) {
	t.Cleanup(func() {
	})
	app, stdout, stderr := newTestApp(t)
	localFile := filepath.Join(t.TempDir(), "upload.txt")
	if err := os.WriteFile(localFile, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	downloadFile := filepath.Join(t.TempDir(), "download.txt")
	mountDir := t.TempDir()
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	app.seams.newMountHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	app.seams.newRESTHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	app.seams.newREST = func(_ *storhub.StorHub, _ storhub.RESTOptions) (http.Handler, error) {
		return http.NewServeMux(), nil
	}
	app.seams.listenServe = func(_ *http.Server) error {
		return nil
	}
	checks := [][]string{
		{"upload", "--token", "x", "demo", "docs/readme.txt", localFile},
		{"replace", "--token", "x", "demo", "docs/readme.txt", localFile},
		{"download", "--token", "x", "demo", "docs/readme.txt", downloadFile},
		{"ls", "--token", "x", "demo", "docs"},
		{"stat", "--token", "x", "demo", "docs/readme.txt"},
		{"cat", "--token", "x", "demo", "docs/readme.txt"},
		{"mkdir", "--token", "x", "demo", "docs"},
		{"rm", "--token", "x", "demo", "docs/readme.txt"},
		{"rm", "--token", "x", "-r", "demo", "docs"},
		{"mv", "--token", "x", "demo", "docs/readme.txt", "docs/final.txt"},
		{"append", "--token", "x", "demo", "docs/readme.txt", "tail"},
		{"write", "--token", "x", "demo", "docs/readme.txt", "1", "x"},
		{"patch", "--token", "x", "demo", "docs/readme.txt", "1", "2", "x"},
		{"truncate", "--token", "x", "demo", "docs/readme.txt", "3"},
		{"chmod", "--token", "x", "demo", "docs/readme.txt", "640"},
		{"chown", "--token", "x", "demo", "docs/readme.txt", "1", "2"},
		{"touch", "--token", "x", "demo", "docs/readme.txt"},
		{"symlink", "--token", "x", "demo", "docs/readme.txt", "docs/alias.txt"},
		{"readlink", "--token", "x", "demo", "docs/alias.txt"},
		{"link", "--token", "x", "demo", "docs/readme.txt", "docs/hard.txt"},
		{"project", "sync", "--token", "x", "demo"},
		{"project", "revisions", "--token", "x", "demo"},
		{"project", "rollback", "--token", "x", "demo", "deadbeef"},
		{"project", "prune", "--token", "x", "demo", "objects", "--dry-run"},
		{"rest", "--token", "x", "--listen", "127.0.0.1:0", "--allowanonymous"},
		{"mount", "--token", "x", "demo", mountDir},
		{"serve", "--token", "x", "demo", mountDir, "--allowanonymous"},
	}
	for _, args := range checks {
		if err := app.Run(args); err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
	}
	// Status chatter belongs on stderr; stdout carries only data.
	chatter := stderr()
	for _, want := range []string{"uploaded", "replaced", "downloaded docs/readme.txt", "created directory docs", "removed docs/readme.txt", "moved docs/readme.txt -> docs/final.txt", "appended", "written", "patched", "truncated", "changed mode of docs/readme.txt to 0640", "changed ownership of docs/readme.txt to 1:2", "touched docs/readme.txt", "symlinked", "linked", "synced demo", "rolled back demo to deadbeef", "would prune demo (objects)", "serving REST API on 127.0.0.1:0/api/v1 without auth", "mounted demo at ", "mounted demo at ", "serving REST API on :8080/api/v1 without auth"} {
		if !strings.Contains(chatter, want) {
			t.Fatalf("expected %q on stderr %q", want, chatter)
		}
	}
	if data := stdout(); !strings.Contains(data, "hello world") {
		t.Fatalf("expected cat data on stdout, got %q", data)
	}
	data, err := os.ReadFile(downloadFile)
	if err != nil || string(data) != "downloaded" {
		t.Fatalf("download file contents: %q %v", data, err)
	}
}

// TestRunDrainsHubOncePerCommand is the regression guard for the silent
// data-loss bug: one-shot CLI commands exited without draining the async
// metadata writer, so mutations never reached GitHub. Every command that
// creates a hub must be followed by exactly one Shutdown from Run.
func TestRunDrainsHubOncePerCommand(t *testing.T) {
	app, _, _ := newTestApp(t)
	fake := &fakeHub{t: t}
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	if err := app.Run([]string{"mkdir", "--token", "x", "demo", "docs"}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if fake.shutdowns != 1 {
		t.Fatalf("expected exactly 1 hub shutdown after mkdir, got %d", fake.shutdowns)
	}
	if err := app.Run([]string{"ls", "--token", "x", "demo", "docs"}); err != nil {
		t.Fatalf("ls: %v", err)
	}
	if fake.shutdowns != 2 {
		t.Fatalf("expected 2 hub shutdowns after second command, got %d", fake.shutdowns)
	}
}

// TestRunWithoutHubSkipsShutdown pins the flip side: paths that never
// create a hub (help, usage errors) must not invent one to drain.
func TestRunWithoutHubSkipsShutdown(t *testing.T) {
	app, stdout, _ := newTestApp(t)
	fake := &fakeHub{t: t}
	var created int
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		created++
		return fake, nil
	}
	if err := app.Run(nil); err != nil {
		t.Fatalf("root help: %v", err)
	}
	if err := app.Run([]string{"nosuchcommand"}); err == nil {
		t.Fatal("expected unknown command error")
	}
	if created != 0 || fake.shutdowns != 0 {
		t.Fatalf("no hub should exist on help/usage paths (created=%d shutdowns=%d)", created, fake.shutdowns)
	}
	if !strings.Contains(stdout(), "StorHub CLI") {
		t.Fatalf("expected root help output")
	}
}

func TestDeleteProjectRequiresYes(t *testing.T) {
	app, _, stderr := newTestApp(t)
	fake := &fakeHub{t: t}
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	err := app.Run([]string{"project", "delete", "demo"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("refusal must be a usage error naming --yes, got %v", err)
	}
	if fake.shutdowns != 0 {
		t.Fatalf("refusal must not create or drain a hub, shutdowns=%d", fake.shutdowns)
	}
	if err := app.Run([]string{"project", "delete", "--yes", "demo"}); err != nil {
		t.Fatalf("confirmed delete: %v", err)
	}
	if !strings.Contains(stderr(), "deleted project demo") {
		t.Fatalf("expected confirmation chatter on stderr")
	}
	if fake.shutdowns != 1 {
		t.Fatalf("confirmed delete must drain once, shutdowns=%d", fake.shutdowns)
	}
}

// TestRunPropagatesFlushFailure is the sabotage check: a failed
// metadata flush is a failed commit point and must surface as an error
// from Run (main exits non-zero), never as a warning with exit 0.
func TestRunPropagatesFlushFailure(t *testing.T) {
	app, _, _ := newTestApp(t)
	fake := &fakeHub{t: t, shutdownErr: errors.New("flush boom")}
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	err := app.Run([]string{"mkdir", "--token", "x", "demo", "docs"})
	if err == nil || !strings.Contains(err.Error(), "metadata flush failed") || !strings.Contains(err.Error(), "flush boom") {
		t.Fatalf("flush failure must propagate as the exit error, got %v", err)
	}
	if fake.shutdowns != 1 {
		t.Fatalf("expected one drain, got %d", fake.shutdowns)
	}
	// A command that already failed keeps its own error primary, but the
	// flush failure must still be visible, not swallowed.
	fake2 := &fakeHub{t: t, shutdownErr: errors.New("flush boom"), readDirErr: errors.New("ls boom")}
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake2, nil
	}
	app2, _, stderr2 := newTestApp(t)
	app2.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake2, nil
	}
	err = app2.Run([]string{"ls", "--token", "x", "demo"})
	if err == nil || !strings.Contains(err.Error(), "ls boom") {
		t.Fatalf("command error must stay primary, got %v", err)
	}
	if !strings.Contains(stderr2(), "flush boom") {
		t.Fatalf("secondary flush failure must be reported, not swallowed: %q", stderr2())
	}
}
