package cli

import (
	"bytes"
	"errors"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if err := app.Run([]string{"upload"}); err == nil || !strings.Contains(err.Error(), "usage: storhub upload") {
		t.Fatalf("expected upload usage error, got %v", err)
	}
	if err := app.Run([]string{"write", "--token", "x", "project", "file", "nope", "data"}); err == nil || !strings.Contains(err.Error(), "invalid offset") {
		t.Fatalf("expected invalid offset error, got %v", err)
	}
	if err := app.Run([]string{"patch", "--token", "x", "project", "file", "1", "bad", "data"}); err == nil || !strings.Contains(err.Error(), "invalid delete-size") {
		t.Fatalf("expected invalid delete-size error, got %v", err)
	}
	if err := app.Run([]string{"download"}); err == nil || !strings.Contains(err.Error(), "usage: storhub download") {
		t.Fatalf("expected download usage error, got %v", err)
	}
	_ = stderr
}

func TestHelpersAndRendering(t *testing.T) {
	app, _, _ := newTestApp(t)
	fs := app.parseCommand("ls", "storhub ls [flags] <project>")
	if err := fs.Parse([]string{"--", "demo"}); err != nil || fs == nil || len(fs.Args()) != 1 || fs.Args()[0] != "demo" {
		t.Fatalf("unexpected parseCommand result: fs=%v rest=%v err=%v", fs != nil, fs.Args(), err)
	}
	if err := app.parseCommand("ls", "storhub ls").Parse([]string{"--bad"}); err == nil {
		t.Fatal("expected parse error")
	}
	if got := ternary(true, "a", "b"); got != "a" || ternary(false, 1, 2) != 2 {
		t.Fatal("unexpected ternary result")
	}
	if defaultDownloadPath("docs/readme.txt") != "readme.txt" || defaultDownloadPath("/") != "downloaded-file" {
		t.Fatal("unexpected download path")
	}
	if formatTime(time.Time{}) != "-" || !strings.Contains(formatTime(time.Unix(1, 0)), "1970") {
		t.Fatal("unexpected formatted time")
	}
	if _, err := newHubFromFlags("", "", 0, 0, false); err == nil {
		t.Fatal("expected missing token error")
	}
	hub, err := newHubFromFlags("token", "https://example.test/api/", 64, 3, true)
	if err != nil || hub == nil {
		t.Fatalf("newHubFromFlags: %v", err)
	}
	t.Setenv("GITHUB_TOKEN", "env-token")
	t.Setenv("STORHUB_API_BASE_URL", "https://env.example.test")
	fsForHub := flag.NewFlagSet("test", flag.ContinueOnError)
	if hub, err := app.newHub(fsForHub); err != nil || hub == nil {
		t.Fatalf("newHub with env defaults: %v", err)
	}
	var buf bytes.Buffer
	printFileSummary(&buf, "uploaded", nil)
	if !strings.Contains(buf.String(), "uploaded") {
		t.Fatalf("unexpected nil file summary: %q", buf.String())
	}
	buf.Reset()
	printFileSummary(&buf, "uploaded", &storhub.FileMetadata{Name: "docs/a.txt", Size: 3, Release: "v1", Inode: 1, Mode: 0o644, NLink: 1})
	if !strings.Contains(buf.String(), "docs/a.txt") || strings.Contains(buf.String(), "crc32c") {
		t.Fatalf("unexpected file summary: %q", buf.String())
	}
	buf.Reset()
	printDirEntries(&buf, nil, false)
	if strings.TrimSpace(buf.String()) != "empty" {
		t.Fatalf("unexpected empty dir listing: %q", buf.String())
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
	printEntryInfo(&buf, &storhub.EntryInfo{Path: "docs/a", IsSymlink: true, Inode: 1, Size: 3, Mode: 0o777, UID: 1, GID: 2, NLink: 1, ModifiedAt: time.Unix(1, 0), AccessedAt: time.Unix(2, 0), ChangedAt: time.Unix(3, 0), SymlinkTarget: "target"})
	if !strings.Contains(buf.String(), "target: target") || entryKind(&storhub.EntryInfo{IsDir: true}) != "directory" || entryKind(&storhub.EntryInfo{IsSymlink: true}) != "symlink" || entryKind(&storhub.EntryInfo{}) != "file" {
		t.Fatalf("unexpected entry rendering: %q", buf.String())
	}
	buf.Reset()
	printRevisions(&buf, nil)
	if !strings.Contains(buf.String(), "no metadata revisions") {
		t.Fatalf("unexpected empty revisions: %q", buf.String())
	}
	buf.Reset()
	printRevisions(&buf, []storhub.MetadataRevision{{CommitSHA: "1234567890abcdef", Message: "msg", CommittedAt: time.Unix(4, 0)}})
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
		{name: "revisions", args: []string{"revisions", "project"}, want: "missing GitHub token"},
		{name: "rollback", args: []string{"rollback", "project", "sha"}, want: "missing GitHub token"},
		{name: "serve-rest", args: []string{"serve-rest"}, want: "missing GitHub token"},
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
	app.printRootHelp()
	if !strings.Contains(stdout(), rootHelp) {
		t.Fatalf("unexpected root help output: %q", stdout())
	}
}

func TestAppCommandSuccessPathsWithMockHub(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	oldRESTFactory := newRESTHubFromFlagsFn
	oldRESTHandler := newRESTHandlerFn
	oldRESTListen := restListenAndServeFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	t.Cleanup(func() {
		newRESTHubFromFlagsFn = oldRESTFactory
		newRESTHandlerFn = oldRESTHandler
		restListenAndServeFn = oldRESTListen
	})
	app, stdout, _ := newTestApp(t)
	localFile := filepath.Join(t.TempDir(), "upload.txt")
	if err := os.WriteFile(localFile, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	downloadFile := filepath.Join(t.TempDir(), "download.txt")
	mountDir := t.TempDir()
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, concurrency int, public bool) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	newRESTHubFromFlagsFn = func(token, apiBase string, chunkSize int64, concurrency int, public bool) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	newRESTHandlerFn = func(hub *storhub.StorHub, opts rest.Options) (http.Handler, error) {
		return http.NewServeMux(), nil
	}
	restListenAndServeFn = func(addr string, handler http.Handler) error {
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
		{"revisions", "--token", "x", "demo"},
		{"rollback", "--token", "x", "demo", "deadbeef"},
		{"serve-rest", "--token", "x", "--listen", "127.0.0.1:0"},
		{"mount", "--token", "x", "demo", mountDir},
	}
	for _, args := range checks {
		if err := app.Run(args); err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
	}
	output := stdout()
	for _, want := range []string{"uploaded", "replaced", "downloaded docs/readme.txt", "docs/readme.txt", "created directory docs", "removed docs/readme.txt", "moved docs/readme.txt -> docs/final.txt", "appended", "written", "patched", "rolled back demo to deadbeef", "serving REST API on 127.0.0.1:0/api/v1 without auth", "mounted demo at "} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected %q in output %q", want, output)
		}
	}
	data, err := os.ReadFile(downloadFile)
	if err != nil || string(data) != "downloaded" {
		t.Fatalf("download file contents: %q %v", data, err)
	}
}

func TestServeRESTLoadsAuthFile(t *testing.T) {
	oldHubFactory := newRESTHubFromFlagsFn
	oldHandlerFactory := newRESTHandlerFn
	oldListen := restListenAndServeFn
	t.Cleanup(func() {
		newRESTHubFromFlagsFn = oldHubFactory
		newRESTHandlerFn = oldHandlerFactory
		restListenAndServeFn = oldListen
	})
	app, stdout, _ := newTestApp(t)
	authFile := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"realm":"demo","token_signing_key":"secret-key","users":[{"username":"admin","password":"pass","uid":0,"primary_gid":0,"admin":true}]}`), 0o644); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	newRESTHubFromFlagsFn = func(token, apiBase string, chunkSize int64, concurrency int, public bool) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	newRESTHandlerFn = func(hub *storhub.StorHub, opts rest.Options) (http.Handler, error) {
		if opts.Auth == nil || opts.Auth.Realm != "demo" || len(opts.Auth.Users) != 1 || opts.Auth.Users[0].Username != "admin" {
			t.Fatalf("unexpected auth opts: %+v", opts.Auth)
		}
		return http.NewServeMux(), nil
	}
	restListenAndServeFn = func(addr string, handler http.Handler) error {
		if addr != "127.0.0.1:9090" || handler == nil {
			t.Fatalf("unexpected serve args: addr=%q handler=%v", addr, handler)
		}
		return errors.New("stop")
	}
	err := app.Run([]string{"serve-rest", "--token", "x", "--listen", "127.0.0.1:9090", "--auth-file", authFile})
	if err == nil || err.Error() != "stop" {
		t.Fatalf("expected stop error, got %v", err)
	}
	if !strings.Contains(stdout(), "with auth") {
		t.Fatalf("unexpected stdout: %q", stdout())
	}
}

func TestNormalizeCLIChunkSizeFloorsSmallValues(t *testing.T) {
	if got := normalizeCLIChunkSize(0); got != 0 {
		t.Fatalf("expected zero chunk size to remain unset, got %d", got)
	}
	if got := normalizeCLIChunkSize(1024); got != minCLIChunkSize {
		t.Fatalf("expected small chunk size to clamp to %d, got %d", minCLIChunkSize, got)
	}
	if got := normalizeCLIChunkSize(64 << 20); got != 64<<20 {
		t.Fatalf("expected larger chunk size to remain unchanged, got %d", got)
	}
}

func TestListAcceptsAbsolutePath(t *testing.T) {
	app, _, _ := newTestApp(t)
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, concurrency int, public bool) (hubClient, error) {
		return &fakeHub{t: t, assertReadDirPath: "docs/readme.txt"}, nil
	}
	if err := app.Run([]string{"ls", "--token", "x", "demo", "/docs/readme.txt"}); err != nil {
		t.Fatalf("run ls with absolute path: %v", err)
	}
}

type fakeHub struct {
	t                 *testing.T
	assertReadDirPath string
	assertStatPath    string
}

func (h *fakeHub) UploadFile(project, remotePath, localPath string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Name: remotePath, Size: 11, Release: "v1", Inode: 1, Mode: 0o644, NLink: 1}, nil
}
func (h *fakeHub) ReplaceFile(project, remotePath, localPath string) (*storhub.FileMetadata, error) {
	return h.UploadFile(project, remotePath, localPath)
}
func (h *fakeHub) DownloadFile(project, remotePath, localPath string) error {
	return os.WriteFile(localPath, []byte("downloaded"), 0o644)
}
func (h *fakeHub) ReadDir(project, dir string) ([]storhub.DirEntry, error) {
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
	return &storhub.EntryInfo{Path: targetPath, Size: 11, Mode: 0o644, Inode: 1, UID: 1, GID: 2, NLink: 1, ModifiedAt: time.Unix(1, 0), AccessedAt: time.Unix(2, 0), ChangedAt: time.Unix(3, 0)}, nil
}
func (h *fakeHub) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	return []byte("hello world"), nil
}
func (h *fakeHub) Mkdir(project, dirPath string) error           { return nil }
func (h *fakeHub) DeleteFile(project, filePath string) error     { return nil }
func (h *fakeHub) Rmdir(project, dirPath string) error           { return nil }
func (h *fakeHub) Rename(project, oldPath, newPath string) error { return nil }
func (h *fakeHub) AppendFile(project, filePath string, data []byte) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Name: filePath, Size: int64(len(data)), Release: "v1", Inode: 2, Mode: 0o644, NLink: 1}, nil
}
func (h *fakeHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Name: filePath, Size: offset + int64(len(data)), Release: "v1", Inode: 3, Mode: 0o644, NLink: 1}, nil
}
func (h *fakeHub) PatchFile(project, filePath string, offset, deleteSize int64, edit []byte) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Name: filePath, Size: 9, Release: "v1", Inode: 4, Mode: 0o644, NLink: 1}, nil
}
func (h *fakeHub) ListMetadataRevisions(project string) ([]storhub.MetadataRevision, error) {
	return []storhub.MetadataRevision{{CommitSHA: "deadbeefcafebabe", Message: "demo", CommittedAt: time.Unix(1, 0)}}, nil
}
func (h *fakeHub) RollbackMetadata(project, commitSHA string) error { return nil }
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
	return app, stdout, stderr
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
