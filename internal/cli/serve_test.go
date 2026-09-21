package cli

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/FarelRA/storhub/storhub"
)

// recordingMount counts lifecycle calls so tests can prove serve tears
// the mount down exactly once on every exit path. Wait blocks until the
// first Unmount - like the real FUSE filesystem - so exit paths stay
// deterministic under instant stubs.
type recordingMount struct {
	mounts, unmounts, closes int
	releaseOnce              sync.Once
	waitRelease              chan struct{}
}

func newRecordingMount() *recordingMount {
	return &recordingMount{waitRelease: make(chan struct{})}
}

func (m *recordingMount) Mount(string) error { m.mounts++; return nil }

func (m *recordingMount) Unmount() error {
	m.unmounts++
	m.releaseOnce.Do(func() { close(m.waitRelease) })
	return nil
}

func (m *recordingMount) Wait() { <-m.waitRelease }
func (m *recordingMount) Close() error {
	m.closes++
	return nil
}

// stubServeSeams swaps every external dependency of runServe on app and
// returns the recording mount it installed. Per-App seams need no
// restore: each test owns its App.
func stubServeSeams(t *testing.T, app *App) *recordingMount {
	t.Helper()
	app.seams.newRESTHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	mount := newRecordingMount()
	app.seams.newFUSE = func(_ *storhub.StorHub, _ string, _ storhub.FUSEOptions) (fuseMount, error) {
		return mount, nil
	}
	app.seams.newREST = func(_ *storhub.StorHub, _ storhub.RESTOptions) (http.Handler, error) {
		return http.NewServeMux(), nil
	}
	// Mimic a clean server stop without signals: returning
	// ErrServerClosed is what a real ListenAndServe does when Shutdown
	// runs, and it is the only arm of runServe's select that a test can
	// fire deterministically.
	app.seams.listenServe = func(_ *http.Server) error {
		return http.ErrServerClosed
	}
	return mount
}

func writeTempAuthFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "auth.json")
	content := `{"token_signing_key":"testsigningkey0123456789abcdef0000","realm":"storhub"}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	return path
}

func TestServeTearsDownMountWhenListenerDies(t *testing.T) {
	app, _, stderr := newTestApp(t)
	mount := stubServeSeams(t, app)
	app.seams.listenServe = func(server *http.Server) error {
		if server.Addr != "127.0.0.1:9090" || server.Handler == nil {
			t.Fatalf("unexpected serve args: addr=%q handler=%v", server.Addr, server.Handler)
		}
		return errors.New("listen boom")
	}
	err := app.Run([]string{"serve", "--token", "x", "--listen", "127.0.0.1:9090", "--allowanonymous", "demo", t.TempDir()})
	if err == nil || err.Error() != "listen boom" {
		t.Fatalf("expected listener error to surface, got %v", err)
	}
	if mount.mounts != 1 || mount.unmounts != 1 || mount.closes != 1 {
		t.Fatalf("mount lifecycle mounts=%d unmounts=%d closes=%d", mount.mounts, mount.unmounts, mount.closes)
	}
	for _, want := range []string{"mounted demo at ", "serving REST API on 127.0.0.1:9090/api/v1 without auth"} {
		if !strings.Contains(stderr(), want) {
			t.Fatalf("expected %q on stderr %q", want, stderr())
		}
	}
}

func TestServeRefusesOpenAPIAndUnmounts(t *testing.T) {
	app, _, _ := newTestApp(t)
	mount := stubServeSeams(t, app)
	err := app.Run([]string{"serve", "--token", "x", "demo", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "refusing to serve unauthenticated REST API") {
		t.Fatalf("expected open-API refusal, got %v", err)
	}
	if mount.unmounts != 1 || mount.closes != 1 {
		t.Fatalf("failed setup must unmount+close, got unmounts=%d closes=%d", mount.unmounts, mount.closes)
	}
}

func TestServeHonorsAuthFile(t *testing.T) {
	app, _, stderr := newTestApp(t)
	mount := stubServeSeams(t, app)
	var gotOpts storhub.RESTOptions
	app.seams.newREST = func(_ *storhub.StorHub, opts storhub.RESTOptions) (http.Handler, error) {
		gotOpts = opts
		return http.NewServeMux(), nil
	}
	if err := app.Run([]string{"serve", "--token", "x", "--authfile", writeTempAuthFile(t), "--basepath", "/api/v2", "demo", t.TempDir()}); err != nil {
		t.Fatalf("serve with auth file: %v", err)
	}
	if gotOpts.Auth == nil {
		t.Fatal("expected auth options to be wired into the handler")
	}
	if gotOpts.BasePath != "/api/v2" {
		t.Fatalf("base path not honored: %q", gotOpts.BasePath)
	}
	if mount.mounts != 1 || mount.unmounts != 1 || mount.closes != 1 {
		t.Fatalf("clean run must mount once and stop cleanly, got mounts=%d unmounts=%d closes=%d",
			mount.mounts, mount.unmounts, mount.closes)
	}
	if !strings.Contains(stderr(), "with auth") {
		t.Fatalf("expected 'with auth' chatter, got %q", stderr())
	}
}
