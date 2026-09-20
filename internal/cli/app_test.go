package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	rest "github.com/FarelRA/storhub/rest"
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
	if err := app.Run([]string{"patch", "--token", "x", "project", "file", "1", "bad", "data"}); err == nil || !strings.Contains(err.Error(), "invalid delete-size") {
		t.Fatalf("expected invalid delete-size error, got %v", err)
	}
	if err := app.Run([]string{"download"}); err == nil || !strings.Contains(err.Error(), "accepts") {
		t.Fatalf("expected download arg error, got %v", err)
	}
	_ = stderr
}

func TestHelpersAndRendering(t *testing.T) {
	oldWarn := warnOutput
	warnOutput = io.Discard
	t.Cleanup(func() { warnOutput = oldWarn })
	if formatTime(0) != "-" || !strings.Contains(formatTime(1), "1970") {
		t.Fatal("unexpected formatted time")
	}
	if _, err := newHubFromFlags("", "", 0, false, logSettings{}); err == nil {
		t.Fatal("expected missing token error")
	}
	hub, err := newHubFromFlags("token", "https://example.test/api/", 64, true, logSettings{})
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

func TestAppCommandSuccessPathsWithMockHub(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	oldMountFactory := newMountHubFromFlagsFn
	oldRESTFactory := newRESTHubFromFlagsFn
	oldRESTHandler := newRESTHandlerFn
	oldRESTListen := restListenAndServeFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	t.Cleanup(func() { newMountHubFromFlagsFn = oldMountFactory })
	t.Cleanup(func() {
		newRESTHubFromFlagsFn = oldRESTFactory
		newRESTHandlerFn = oldRESTHandler
		restListenAndServeFn = oldRESTListen
	})
	app, stdout, stderr := newTestApp(t)
	localFile := filepath.Join(t.TempDir(), "upload.txt")
	if err := os.WriteFile(localFile, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	downloadFile := filepath.Join(t.TempDir(), "download.txt")
	mountDir := t.TempDir()
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	newMountHubFromFlagsFn = func(token, apiBase string, log logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	newRESTHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	newRESTHandlerFn = func(hub *storhub.StorHub, opts rest.Options) (http.Handler, error) {
		return http.NewServeMux(), nil
	}
	restListenAndServeFn = func(server *http.Server) error {
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
		{"rest", "--token", "x", "--listen", "127.0.0.1:0", "--allow-anonymous"},
		{"mount", "--token", "x", "demo", mountDir},
		{"serve", "--token", "x", "demo", mountDir, "--allow-anonymous"},
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
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	app, _, _ := newTestApp(t)
	fake := &fakeHub{t: t}
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
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
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	app, stdout, _ := newTestApp(t)
	fake := &fakeHub{t: t}
	var created int
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		created++
		return fake, nil
	}
	if err := app.Run(nil); err != nil {
		t.Fatalf("root help: %v", err)
	}
	if err := app.Run([]string{"no-such-command"}); err == nil {
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
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	app, _, stderr := newTestApp(t)
	fake := &fakeHub{t: t}
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
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

func TestServeRESTLoadsAuthFile(t *testing.T) {
	oldHubFactory := newRESTHubFromFlagsFn
	oldMountFactory := newMountHubFromFlagsFn
	oldHandlerFactory := newRESTHandlerFn
	oldListen := restListenAndServeFn
	t.Cleanup(func() {
		newRESTHubFromFlagsFn = oldHubFactory
		newMountHubFromFlagsFn = oldMountFactory
		newRESTHandlerFn = oldHandlerFactory
		restListenAndServeFn = oldListen
	})
	app, _, stderr := newTestApp(t)
	authFile := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"realm":"demo","token_signing_key":"secret-key","users":[{"username":"admin","password":"pass","uid":0,"primary_gid":0,"admin":true}]}`), 0o644); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	newRESTHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	newRESTHandlerFn = func(hub *storhub.StorHub, opts rest.Options) (http.Handler, error) {
		if opts.Auth == nil || opts.Auth.Realm != "demo" || len(opts.Auth.Users) != 1 || opts.Auth.Users[0].Username != "admin" {
			t.Fatalf("unexpected auth opts: %+v", opts.Auth)
		}
		return http.NewServeMux(), nil
	}
	restListenAndServeFn = func(server *http.Server) error {
		if server.Addr != "127.0.0.1:9090" || server.Handler == nil {
			return fmt.Errorf("unexpected serve args: addr=%q handler=%v", server.Addr, server.Handler)
		}
		// ReadHeaderTimeout guards slow-loris; Read/WriteTimeout stay 0 so
		// large uploads and slow multi-gigabyte downloads are never amputated
		// mid-transfer (size bounds live in the content handlers).
		if server.ReadHeaderTimeout <= 0 || server.ReadTimeout != 0 || server.WriteTimeout != 0 || server.IdleTimeout <= 0 {
			return fmt.Errorf("expected REST server timeouts (ReadHeader>0 Read=0 Write=0 Idle>0), got ReadHeader=%v Read=%v Write=%v Idle=%v", server.ReadHeaderTimeout, server.ReadTimeout, server.WriteTimeout, server.IdleTimeout)
		}
		return errors.New("stop")
	}
	err := app.Run([]string{"rest", "--token", "x", "--listen", "127.0.0.1:9090", "--auth-file", authFile})
	if err == nil || err.Error() != "stop" {
		t.Fatalf("expected stop error, got %v", err)
	}
	if !strings.Contains(stderr(), "with auth") {
		t.Fatalf("unexpected stderr: %q", stderr())
	}
}

func TestNormalizeCLIChunkSizeFloorsSmallValues(t *testing.T) {
	t.Parallel()
	if got := normalizeCLIChunkSize(0); got != 0 {
		t.Fatalf("expected zero chunk size to remain unset, got %d", got)
	}
	if got := normalizeCLIChunkSize(-1); got != -1 {
		t.Fatalf("expected negative chunk size to pass through for usage-error rejection, got %d", got)
	}
	if got := normalizeCLIChunkSize(1024); got != minCLIChunkSize {
		t.Fatalf("expected small chunk size to clamp to %d, got %d", minCLIChunkSize, got)
	}
	if got := normalizeCLIChunkSize(64 << 20); got != 64<<20 {
		t.Fatalf("expected larger chunk size to remain unchanged, got %d", got)
	}
}

// TestNormalizeCLIChunkSizeCeilingClamp pins the ceiling contract: a --chunk-size
// above the GitHub release-asset ceiling must clamp DOWN, so the chunker's
// plan and the uploader's windows agree instead of failing mid-upload.
func TestNormalizeCLIChunkSizeCeilingClamp(t *testing.T) {
	t.Parallel()
	if got := normalizeCLIChunkSize(9999999999); got != chunking.MaxReleaseAssetSize {
		t.Fatalf("expected ceiling clamp to %d, got %d", chunking.MaxReleaseAssetSize, got)
	}
	if got := normalizeCLIChunkSize(chunking.MaxReleaseAssetSize); got != chunking.MaxReleaseAssetSize {
		t.Fatalf("ceiling value must pass through, got %d", got)
	}
	if got := normalizeCLIChunkSize(chunking.MaxReleaseAssetSize + 1); got != chunking.MaxReleaseAssetSize {
		t.Fatalf("expected ceiling clamp to %d, got %d", chunking.MaxReleaseAssetSize, got)
	}
}

// TestHubConfigClampWarnsThroughSeam pins that the clamp warning goes
// through the warnOutput seam (capturable), not straight to os.Stderr.
func TestHubConfigClampWarnsThroughSeam(t *testing.T) {
	oldWarn := warnOutput
	var buf bytes.Buffer
	warnOutput = &buf
	t.Cleanup(func() { warnOutput = oldWarn })
	cfg := newHubConfig("", 1024, false, logSettings{}, false)
	if cfg.ChunkSize != minCLIChunkSize {
		t.Fatalf("expected floor clamp, got %d", cfg.ChunkSize)
	}
	if !strings.Contains(buf.String(), "warning") || !strings.Contains(buf.String(), "--chunk-size") {
		t.Fatalf("expected clamp warning on warnOutput, got %q", buf.String())
	}
	buf.Reset()
	cfg = newHubConfig("", 9999999999, false, logSettings{}, false)
	if cfg.ChunkSize != chunking.MaxReleaseAssetSize {
		t.Fatalf("expected ceiling clamp, got %d", cfg.ChunkSize)
	}
	if !strings.Contains(buf.String(), "--chunk-size") {
		t.Fatalf("expected ceiling clamp warning, got %q", buf.String())
	}
}

// TestHubConfigRatePolicyWiring pins the documented contract: one-shot
// = fail-fast (negative max wait), long-running (mount, rest,
// serve) = pause up to the reset (positive max wait).
func TestHubConfigRatePolicyWiring(t *testing.T) {
	if got := newHubConfig("", 0, false, logSettings{}, false).RateMaxWait; got >= 0 {
		t.Fatalf("one-shot RateMaxWait = %v, want negative (fail-fast)", got)
	}
	if got := newHubConfig("", 0, false, logSettings{}, true).RateMaxWait; got <= 0 {
		t.Fatalf("long-running RateMaxWait = %v, want positive pause", got)
	}
}

// TestRatePolicyCommandRouting pins which commands reach which constructor:
// download is one-shot (standard hub), mount is long-running (mount hub).
func TestRatePolicyCommandRouting(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	oldMountFactory := newMountHubFromFlagsFn
	t.Cleanup(func() {
		newHubFromFlagsFn = oldFactory
		newMountHubFromFlagsFn = oldMountFactory
	})
	var oneShot, longRunning int
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		oneShot++
		return &fakeHub{t: t}, nil
	}
	newMountHubFromFlagsFn = func(token, apiBase string, log logSettings) (hubClient, error) {
		longRunning++
		return &fakeHub{t: t}, nil
	}
	app, _, _ := newTestApp(t)
	downloadFile := filepath.Join(t.TempDir(), "out.bin")
	if err := app.Run([]string{"download", "--token", "x", "demo", "f", downloadFile}); err != nil {
		t.Fatalf("download: %v", err)
	}
	if oneShot != 1 || longRunning != 0 {
		t.Fatalf("download must use the one-shot hub (oneShot=%d longRunning=%d)", oneShot, longRunning)
	}
	if err := app.Run([]string{"mount", "--token", "x", "demo", t.TempDir()}); err != nil {
		t.Fatalf("mount: %v", err)
	}
	if oneShot != 1 || longRunning != 1 {
		t.Fatalf("mount must use the long-running hub (oneShot=%d longRunning=%d)", oneShot, longRunning)
	}
}

// TestHubConfigAppliesLogSettingsAndJournal pins that every CLI hub
// (including the one-shot download path) must carry the --log-* flags and
// a journal directory beneath the cache base.
func TestHubConfigAppliesLogSettingsAndJournal(t *testing.T) {
	log := logSettings{level: "error", format: "text", color: false}
	for _, longRunning := range []bool{false, true} {
		cfg := newHubConfig("", 0, false, log, longRunning)
		if cfg.LogLevel != "error" || cfg.LogFormat != "text" || cfg.LogColor {
			t.Fatalf("log settings not applied (longRunning=%v): %+v", longRunning, cfg)
		}
		want := filepath.Join(storcfg.CacheBase(), "journal")
		if cfg.JournalDir != want {
			t.Fatalf("JournalDir = %q, want %q", cfg.JournalDir, want)
		}
	}
}

// TestNegativeChunkSizeIsUsageError pins that --chunk-size -1 must exit as
// a usage error (class 2), not silently run with defaults.
func TestNegativeChunkSizeIsUsageError(t *testing.T) {
	app, _, _ := newTestApp(t)
	localFile := filepath.Join(t.TempDir(), "f.bin")
	if err := os.WriteFile(localFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	err := app.Run([]string{"upload", "--token", "x", "--chunk-size", "-1", "demo", "f", localFile})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--chunk-size") {
		t.Fatalf("negative --chunk-size must be a usage error naming the flag, got %v", err)
	}
}

// TestPruneScopeAndKeepAreUsageErrors pins that bad scope and keep < 1 must
// be rejected by the CLI as usage errors (exit 2) before any hub exists.
func TestPruneScopeAndKeepAreUsageErrors(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	var created int
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		created++
		return &fakeHub{t: t}, nil
	}
	app, _, _ := newTestApp(t)
	err := app.Run([]string{"project", "prune", "--token", "x", "demo", "bogus"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "prune scope") {
		t.Fatalf("bad scope must be a usage error, got %v", err)
	}
	err = app.Run([]string{"project", "prune", "--token", "x", "demo", "objects", "--keep", "0"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--keep") {
		t.Fatalf("--keep 0 must be a usage error, got %v", err)
	}
	err = app.Run([]string{"project", "prune", "--token", "x", "demo", "all", "--keep", "-3"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--keep") {
		t.Fatalf("negative --keep must be a usage error, got %v", err)
	}
	if created != 0 {
		t.Fatalf("usage errors must not build a hub, created=%d", created)
	}
	if err := app.Run([]string{"project", "prune", "--token", "x", "demo", "history", "--keep", "2"}); err != nil {
		t.Fatalf("valid prune must still run: %v", err)
	}
}

// TestWritePatchNegativeArgsAreUsageErrors pins that negative offsets and
// delete-sizes are command-line mistakes (exit 2), not storage failures.
func TestWritePatchNegativeArgsAreUsageErrors(t *testing.T) {
	app, _, _ := newTestApp(t)
	cases := [][]string{
		{"write", "--token", "x", "demo", "f", "-1", "data"},
		{"patch", "--token", "x", "demo", "f", "-1", "0", "data"},
		{"patch", "--token", "x", "demo", "f", "0", "-5", "data"},
	}
	for _, args := range cases {
		err := app.Run(args)
		if err == nil || !IsUsageError(err) {
			t.Fatalf("%v must be a usage error, got %v", args[0], err)
		}
	}
	// Parse failures are usage errors too (exit 2, not 1).
	err := app.Run([]string{"write", "--token", "x", "demo", "f", "nope", "data"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "invalid offset") {
		t.Fatalf("unparseable offset must classify as usage error, got %v", err)
	}
}

// TestRunPropagatesFlushFailure is the sabotage check: a failed
// metadata flush is a failed commit point and must surface as an error
// from Run (main exits non-zero), never as a warning with exit 0.
func TestRunPropagatesFlushFailure(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	app, _, _ := newTestApp(t)
	fake := &fakeHub{t: t, shutdownErr: errors.New("flush boom")}
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
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
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return fake2, nil
	}
	app2, _, stderr2 := newTestApp(t)
	err = app2.Run([]string{"ls", "--token", "x", "demo"})
	if err == nil || !strings.Contains(err.Error(), "ls boom") {
		t.Fatalf("command error must stay primary, got %v", err)
	}
	if !strings.Contains(stderr2(), "flush boom") {
		t.Fatalf("secondary flush failure must be reported, not swallowed: %q", stderr2())
	}
}

// TestFlexDurationRejectsNonFinite pins the NaN/Inf nit: JSON's extended
// number grammar parses NaN/Infinity cleanly but they are nonsense TTLs.
func TestFlexDurationRejectsNonFinite(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"NaN", "Infinity", "-Infinity", "-30", `"1h"`} {
		var d flexDuration
		err := d.UnmarshalJSON([]byte(raw))
		if raw == `"1h"` {
			if err != nil || d.Duration() != time.Hour {
				t.Fatalf("valid duration string rejected: %v %v", err, d)
			}
			continue
		}
		if err == nil {
			t.Fatalf("%s must be rejected as a TTL, got %v", raw, d)
		}
	}
}

// TestServeRejectsAnonymousWithAuthFile pins the contradictory-flags nit:
// --allow-anonymous alongside an auth file must fail as a usage error
// instead of being silently ignored.
func TestServeRejectsAnonymousWithAuthFile(t *testing.T) {
	oldFactory := newRESTHubFromFlagsFn
	t.Cleanup(func() { newRESTHubFromFlagsFn = oldFactory })
	newRESTHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	app, _, _ := newTestApp(t)
	authFile := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"token_signing_key":"k"}`), 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	err := app.Run([]string{"rest", "--token", "x", "--auth-file", authFile, "--allow-anonymous"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--allow-anonymous") {
		t.Fatalf("contradictory auth flags must be a usage error, got %v", err)
	}
}

// TestAuthFileRejectsUnknownFields pins the typo nit: a misspelled key in
// the auth JSON (e.g. token_ttl) must fail loudly, not silently default.
func TestAuthFileRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(path, []byte(`{"token_signing_key":"k","token_ttl":"1h","users_typo":[]}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := loadRESTAuthOptions(path); err == nil || !strings.Contains(err.Error(), "users_typo") {
		t.Fatalf("unknown field must fail decoding, got %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"token_signing_key":"k","token_ttl":3600}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	auth, err := loadRESTAuthOptions(path)
	if err != nil || auth.TokenTTL != time.Hour {
		t.Fatalf("valid numeric TTL must decode to one hour, got %v %v", auth, err)
	}
}

func TestListAcceptsAbsolutePath(t *testing.T) {
	app, _, _ := newTestApp(t)
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
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
		t.Setenv("GITHUB_TOKEN", "env-token")
		app, _, _ := newTestApp(t)
		oldFactory := newHubFromFlagsFn
		t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
		newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
			if token != "env-token" {
				t.Fatalf("expected token env-token, got %q", token)
			}
			if apiBase != "" {
				t.Fatalf("expected empty api-base, got %q", apiBase)
			}
			return &fakeHub{t: t}, nil
		}
		if err := app.Run([]string{"ls", "demo"}); err != nil {
			t.Fatalf("ls with env token: %v", err)
		}
	})

	t.Run("persistent flag overrides env", func(t *testing.T) {
		app, _, _ := newTestApp(t)
		oldFactory := newHubFromFlagsFn
		t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
		newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
			if token != "override" {
				t.Fatalf("expected token override, got %q", token)
			}
			return &fakeHub{t: t}, nil
		}
		t.Setenv("GITHUB_TOKEN", "env-token")
		if err := app.Run([]string{"ls", "--token", "override", "demo"}); err != nil {
			t.Fatalf("ls with override token: %v", err)
		}
	})

	t.Run("local flags", func(t *testing.T) {
		app, stdout, _ := newTestApp(t)
		oldFactory := newHubFromFlagsFn
		t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
		newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
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

type fakeHub struct {
	t                 *testing.T
	assertReadDirPath string
	assertStatPath    string
	readDirErr        error
	shutdowns         int
	shutdownErr       error
	drainCalls        []string
	drainErr          error
	sess              *cliSessionStore
}

// DrainProjectContext records the call so sync tests can assert draining
// happened (or did not); a configured drainErr simulates a failed commit.
func (h *fakeHub) DrainProjectContext(ctx context.Context, project string) error {
	h.drainCalls = append(h.drainCalls, project)
	return h.drainErr
}

// Shutdown records every drain so tests can prove App.Run closed what a
// command opened - the regression guard for the silent-data-loss bug. A
// configured shutdownErr simulates a failed commit point.
func (h *fakeHub) Shutdown(ctx context.Context) error {
	h.shutdowns++
	return h.shutdownErr
}

func (h *fakeHub) DeleteProject(project string) error { return nil }

func (h *fakeHub) UploadFile(project, remotePath, localPath string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: 11, Inode: 1, Mode: 0o644}, nil
}
func (h *fakeHub) ReplaceFile(project, remotePath, localPath string) (*storhub.FileMetadata, error) {
	return h.UploadFile(project, remotePath, localPath)
}
func (h *fakeHub) DownloadFile(project, remotePath, localPath string) error {
	return os.WriteFile(localPath, []byte("downloaded"), 0o644)
}
func (h *fakeHub) ReadDir(project, dir string) ([]storhub.DirEntry, error) {
	if h.readDirErr != nil {
		return nil, h.readDirErr
	}
	if h.assertReadDirPath != "" {
		got, err := shfs.NormalizePath(dir)
		if err != nil {
			h.t.Fatalf("normalize read dir path: %v", err)
		}
		if got != h.assertReadDirPath {
			h.t.Fatalf("unexpected read dir path: %q", dir)
		}
	}
	return []storhub.DirEntry{{Name: "readme.txt", Path: "docs/readme.txt", Size: 11, Mode: 0o644}}, nil
}
func (h *fakeHub) StatPath(project, targetPath string) (*storhub.EntryInfo, error) {
	if h.assertStatPath != "" {
		got, err := shfs.NormalizePath(targetPath)
		if err != nil {
			h.t.Fatalf("normalize stat path: %v", err)
		}
		if got != h.assertStatPath {
			h.t.Fatalf("unexpected stat path: %q", targetPath)
		}
	}
	return &storhub.EntryInfo{Path: targetPath, Size: 11, Mode: 0o644, Inode: 1, UID: 1, GID: 2, NLink: 1, ModifiedAt: 1, AccessedAt: 2, ChangedAt: 3}, nil
}
func (h *fakeHub) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	return []byte("hello world"), nil
}
func (h *fakeHub) Mkdir(project, dirPath string) error           { return nil }
func (h *fakeHub) DeleteFile(project, filePath string) error     { return nil }
func (h *fakeHub) Rmdir(project, dirPath string) error           { return nil }
func (h *fakeHub) Rename(project, oldPath, newPath string) error { return nil }
func (h *fakeHub) AppendFile(project, filePath string, data []byte) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: int64(len(data)), Inode: 2, Mode: 0o644}, nil
}
func (h *fakeHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: offset + int64(len(data)), Inode: 3, Mode: 0o644}, nil
}
func (h *fakeHub) PatchFile(project, filePath string, offset, deleteSize int64, edit []byte) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: 9, Inode: 4, Mode: 0o644}, nil
}
func (h *fakeHub) CreateFile(project, filePath string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: 0, Inode: 5, Mode: 0o644}, nil
}
func (h *fakeHub) TruncateFile(project, filePath string, size int64) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: size, Inode: 6, Mode: 0o644}, nil
}
func (h *fakeHub) Chmod(project, targetPath string, mode uint32) error          { return nil }
func (h *fakeHub) Chown(project, targetPath string, uid, gid uint32) error      { return nil }
func (h *fakeHub) Chtimes(project, targetPath string, atime, mtime int64) error { return nil }
func (h *fakeHub) Symlink(project, target, linkPath string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: int64(len(target)), Inode: 7, Mode: 0o777}, nil
}
func (h *fakeHub) Readlink(project, linkPath string) (string, error) { return "target", nil }
func (h *fakeHub) Link(project, existingPath, newPath string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: 11, Inode: 1, Mode: 0o644}, nil
}
func (h *fakeHub) ListMetadataRevisions(project string) ([]storhub.MetadataRevision, error) {
	return []storhub.MetadataRevision{{CommitSHA: "deadbeefcafebabe", Message: "demo", CommittedAt: 1}}, nil
}
func (h *fakeHub) RollbackMetadataContext(ctx context.Context, project, commitSHA string) error {
	return nil
}
func (h *fakeHub) PruneContext(ctx context.Context, project, scope string, keep int, dryRun bool) (*storhub.PruneResult, error) {
	return &storhub.PruneResult{Scope: storhub.PruneScope(scope), DryRun: dryRun}, nil
}
func (h *fakeHub) DegradedProjects() []string {
	return nil
}
func (h *fakeHub) ReEnableProject(project string) error { return nil }
func (h *fakeHub) PressureSnapshot() storhub.PressureSnapshot {
	return storhub.PressureSnapshot{}
}
func (h *fakeHub) PressureFailureStreak(project string) uint64 { return 0 }
func (h *fakeHub) PressurePendingDepth(project string) int     { return 0 }
func (h *fakeHub) NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error) {
	return fakeMount{}, nil
}

type fakeMount struct{}

func (fakeMount) Mount(string) error { return nil }
func (fakeMount) Unmount() error     { return nil }
func (fakeMount) Wait()              {}
func (fakeMount) Close() error       { return nil }

func newTestApp(t *testing.T) (*App, func() string, func() string) {
	t.Helper()
	app := New()
	stdoutFile, stdout := tempCaptureFile(t)
	stderrFile, stderr := tempCaptureFile(t)
	app.stdout = stdoutFile
	app.stderr = stderrFile
	// Route package-level configuration warnings through the same capture
	// seam so tests can assert on them instead of polluting real stderr.
	oldWarn := warnOutput
	warnOutput = stderrFile
	t.Cleanup(func() { warnOutput = oldWarn })
	return app, stdout, stderr
}

// TestMountUmaskFlagMasksCreatedModes pins the mount --umask wiring:
// the flag exists with a 022 default, values parse as octal masks,
// misuse is a usage error, and the wired FUSEOptions mask created modes
// through the same ApplyCreateMode the server applies at create time.
func TestMountUmaskFlagMasksCreatedModes(t *testing.T) {
	app := New()
	mount, _, err := app.rootCmd.Find([]string{"mount"})
	if err != nil || mount == nil {
		t.Fatalf("find mount command: %v", err)
	}
	def, err := mount.Flags().GetString("umask")
	if err != nil {
		t.Fatalf("mount --umask flag missing: %v", err)
	}
	if def != "022" {
		t.Fatalf("mount --umask default: want %q, got %q", "022", def)
	}
	for _, tc := range []struct {
		raw      string
		wantMask uint32
		wantMode uint32
	}{
		{raw: "022", wantMask: 0o022, wantMode: 0o644},
		{raw: "027", wantMask: 0o027, wantMode: 0o640},
		{raw: "077", wantMask: 0o077, wantMode: 0o600},
		{raw: "0", wantMask: 0, wantMode: 0o666},
	} {
		if err := mount.Flags().Set("umask", tc.raw); err != nil {
			t.Fatalf("set --umask %q: %v", tc.raw, err)
		}
		raw, _ := mount.Flags().GetString("umask")
		mask, err := parseMountUmask(raw)
		if err != nil {
			t.Fatalf("parse --umask %q: %v", tc.raw, err)
		}
		if mask != tc.wantMask {
			t.Fatalf("parse --umask %q: want %#o, got %#o", tc.raw, tc.wantMask, mask)
		}
		opts := storhub.DefaultFUSEOptions()
		opts.Umask = mask
		opts.UmaskSet = true
		if got := opts.EffectiveUmask(); got != tc.wantMask {
			t.Fatalf("--umask %q: effective: want %#o, got %#o", tc.raw, tc.wantMask, got)
		}
		ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 1000, GID: 1000, Umask: opts.EffectiveUmask()})
		ctx = shfs.WithCreateMode(ctx, 0o666)
		if got := shfs.ApplyCreateMode(ctx, 0o666); got != tc.wantMode {
			t.Fatalf("--umask %q: created mode: want %#o, got %#o", tc.raw, tc.wantMode, got)
		}
	}
	if got := storhub.DefaultFUSEOptions().EffectiveUmask(); got != 0o022 {
		t.Fatalf("default effective umask: want %#o, got %#o", 0o022, got)
	}
	for _, bad := range []string{"", "abc", "888", "1000", "-1"} {
		if _, err := parseMountUmask(bad); err == nil || !IsUsageError(err) {
			t.Fatalf("parse --umask %q: want usage error, got %v", bad, err)
		}
	}
}

func tempCaptureFile(t *testing.T) (*os.File, func() string) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "storhub-cli-*.txt")
	if err != nil {
		t.Fatalf("create temp capture file: %v", err)
	}
	return file, func() string {
		if err := file.Sync(); err != nil {
			t.Fatalf("sync capture file: %v", err)
		}
		data, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatalf("read capture file: %v", err)
		}
		return string(data)
	}
}
