package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	storage "github.com/FarelRA/storhub/internal/storage"
)

// cli_sessions_test.go: Phase 2B session manager over the CLI.
//
// fakeHub gains an in-memory session table (pinned bytes at open,
// own-writes-visible reads, commit on sync/close, unlinked scratch with
// link/discard) so the command sequence is exercised end to end through
// real cobra RunE paths. pcFakeHub gets stubs: it never speaks sessions,
// but it must satisfy the extended hubClient contract to keep compiling.

// cliSession is one emulated open handle behind the CLI fake.
type cliSession struct {
	project string
	path    string
	mode    storage.OpenMode
	data    []byte
	dirty   bool
}

type cliSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*cliSession
	next     int
}

func (s *cliSessionStore) live(id string) (*cliSession, error) {
	sess, ok := s.sessions[id]
	if !ok {
		return nil, &storage.StaleSessionError{HandleID: id, Reason: "unknown handle"}
	}
	return sess, nil
}

func (h *fakeHub) sessionStore() *cliSessionStore {
	if h.sess == nil {
		h.sess = &cliSessionStore{sessions: map[string]*cliSession{}}
	}
	return h.sess
}

func (h *fakeHub) OpenSession(ctx context.Context, project, path string, mode storage.OpenMode, opts ...storage.SessionOption) (string, error) {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	store.next++
	id := fmt.Sprintf("clisess-%d", store.next)
	store.sessions[id] = &cliSession{project: project, path: path, mode: mode}
	return id, nil
}

func (h *fakeHub) ReadSession(ctx context.Context, handleID string, offset, length int64) ([]byte, error) {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	sess, err := store.live(handleID)
	if err != nil {
		return nil, err
	}
	if offset >= int64(len(sess.data)) || length == 0 {
		return []byte{}, nil
	}
	end := offset + length
	if end < offset || end > int64(len(sess.data)) {
		end = int64(len(sess.data))
	}
	return append([]byte(nil), sess.data[offset:end]...), nil
}

func (h *fakeHub) WriteSession(ctx context.Context, handleID string, offset int64, data []byte) (int, error) {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	sess, err := store.live(handleID)
	if err != nil {
		return 0, err
	}
	content := append([]byte(nil), sess.data...)
	if offset > int64(len(content)) {
		content = append(content, make([]byte, offset-int64(len(content)))...)
	}
	end := offset + int64(len(data))
	if end > int64(len(content)) {
		grown := make([]byte, end)
		copy(grown, content)
		content = grown
	}
	copy(content[offset:end], data)
	sess.data = content
	sess.dirty = true
	return len(data), nil
}

func (h *fakeHub) TruncateSession(ctx context.Context, handleID string, size int64) error {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	sess, err := store.live(handleID)
	if err != nil {
		return err
	}
	if int64(len(sess.data)) > size {
		sess.data = append([]byte(nil), sess.data[:size]...)
	} else {
		sess.data = append(append([]byte(nil), sess.data...), make([]byte, size-int64(len(sess.data)))...)
	}
	sess.dirty = true
	return nil
}

func (h *fakeHub) StatSession(ctx context.Context, handleID string) (storage.SessionStat, error) {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	sess, err := store.live(handleID)
	if err != nil {
		return storage.SessionStat{}, err
	}
	return storage.SessionStat{
		Project: sess.project,
		Path:    sess.path,
		Size:    int64(len(sess.data)),
		Dirty:   sess.dirty,
		Mode:    sess.mode,
	}, nil
}

func (h *fakeHub) SyncSession(ctx context.Context, handleID string) error {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	sess, err := store.live(handleID)
	if err != nil {
		return err
	}
	if sess.path == "" {
		return fmt.Errorf("commit session %s: %w", handleID, storage.ErrSessionUnlinked)
	}
	sess.dirty = false
	return nil
}

func (h *fakeHub) LinkSession(ctx context.Context, handleID, path string) error {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	sess, err := store.live(handleID)
	if err != nil {
		return err
	}
	if sess.path != "" {
		return fmt.Errorf("link session %s: %w", handleID, storage.ErrSessionLinked)
	}
	sess.path = path
	sess.dirty = true
	return nil
}

func (h *fakeHub) CloseSession(ctx context.Context, handleID string) error {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, err := store.live(handleID); err != nil {
		return err
	}
	delete(store.sessions, handleID)
	return nil
}

// pcFakeHub never speaks sessions; stubs keep the extended contract
// compiling while conformance keeps failing loudly on use.
func errPCSession() error { return errors.New("pcFakeHub: sessions not supported") }

func (h *pcFakeHub) OpenSession(ctx context.Context, project, path string, mode storage.OpenMode, opts ...storage.SessionOption) (string, error) {
	return "", errPCSession()
}

func (h *pcFakeHub) ReadSession(ctx context.Context, handleID string, offset, length int64) ([]byte, error) {
	return nil, errPCSession()
}

func (h *pcFakeHub) WriteSession(ctx context.Context, handleID string, offset int64, data []byte) (int, error) {
	return 0, errPCSession()
}

func (h *pcFakeHub) TruncateSession(ctx context.Context, handleID string, size int64) error {
	return errPCSession()
}

func (h *pcFakeHub) StatSession(ctx context.Context, handleID string) (storage.SessionStat, error) {
	return storage.SessionStat{}, errPCSession()
}

func (h *pcFakeHub) SyncSession(ctx context.Context, handleID string) error {
	return errPCSession()
}

func (h *pcFakeHub) LinkSession(ctx context.Context, handleID, path string) error {
	return errPCSession()
}

func (h *pcFakeHub) CloseSession(ctx context.Context, handleID string) error {
	return errPCSession()
}

// runSessionCLI executes one CLI invocation against the shared fake and
// returns stdout. A fresh App per call keeps cobra flag state isolated.
func runSessionCLI(t *testing.T, args []string) string {
	t.Helper()
	app, stdout, _ := newTestApp(t)
	if err := app.Run(args); err != nil {
		t.Fatalf("run %v: %v", args, err)
	}
	return stdout()
}

// TestCLISessionSequence drives open/write/read/append/truncate/stat/sync/
// link/close through real cobra paths against one shared fake hub.
func TestCLISessionSequence(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &fakeHub{t: t}
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return fake, nil
	}

	opened := runSessionCLI(t, []string{"session", "open", "--token", "x", "demo", "--mode", "w"})
	handle := strings.TrimSpace(opened)
	if handle == "" {
		t.Fatalf("open printed no handle: %q", opened)
	}

	runSessionCLI(t, []string{"session", "write", "--token", "x", "--handle", handle, "0", "hello"})
	if got := runSessionCLI(t, []string{"session", "read", "--token", "x", "--handle", handle}); got != "hello" {
		t.Fatalf("read = %q, want %q", got, "hello")
	}

	runSessionCLI(t, []string{"session", "append", "--token", "x", "--handle", handle, " world"})
	if got := runSessionCLI(t, []string{"session", "read", "--token", "x", "--handle", handle, "--offset", "0", "--length", "11"}); got != "hello world" {
		t.Fatalf("read after append = %q, want %q", got, "hello world")
	}

	runSessionCLI(t, []string{"session", "truncate", "--token", "x", "--handle", handle, "5"})
	if got := runSessionCLI(t, []string{"session", "read", "--token", "x", "--handle", handle}); got != "hello" {
		t.Fatalf("read after truncate = %q, want %q", got, "hello")
	}

	runSessionCLI(t, []string{"session", "link", "--token", "x", "--handle", handle, "docs/new.txt"})
	statJSON := runSessionCLI(t, []string{"session", "stat", "--token", "x", "--json", "--handle", handle})
	if !strings.Contains(statJSON, `"path":"docs/new.txt"`) || !strings.Contains(statJSON, `"size":5`) || !strings.Contains(statJSON, `"dirty":true`) {
		t.Fatalf("unexpected stat json: %s", statJSON)
	}

	runSessionCLI(t, []string{"session", "sync", "--token", "x", "--handle", handle})
	statJSON = runSessionCLI(t, []string{"session", "stat", "--token", "x", "--json", "--handle", handle})
	if !strings.Contains(statJSON, `"dirty":false`) {
		t.Fatalf("sync must clear dirty: %s", statJSON)
	}

	runSessionCLI(t, []string{"session", "close", "--token", "x", "--sync", "--handle", handle})
	if len(fake.drainCalls) != 1 || fake.drainCalls[0] != "demo" {
		t.Fatalf("close --sync must drain demo once, got %v", fake.drainCalls)
	}

	app, _, _ := newTestApp(t)
	if err := app.Run([]string{"session", "read", "--token", "x", "--handle", handle}); err == nil {
		t.Fatal("read after close must fail on a stale handle")
	}
}

// TestCLISessionRequiresHandle pins that every subcommand except open
// refuses to run without --handle as a usage error.
func TestCLISessionRequiresHandle(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	for _, args := range [][]string{
		{"session", "read", "--token", "x"},
		{"session", "write", "--token", "x", "0", "data"},
		{"session", "append", "--token", "x", "data"},
		{"session", "truncate", "--token", "x", "5"},
		{"session", "stat", "--token", "x"},
		{"session", "sync", "--token", "x"},
		{"session", "link", "--token", "x", "path"},
		{"session", "close", "--token", "x"},
	} {
		app, _, _ := newTestApp(t)
		err := app.Run(args)
		if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--handle") {
			t.Fatalf("%v must be a --handle usage error, got %v", args[1], err)
		}
	}
}

// TestCLISessionOpenRejectsBadMode pins mode/TTL validation as usage errors
// before any hub exists.
func TestCLISessionOpenRejectsBadMode(t *testing.T) {
	app, _, _ := newTestApp(t)
	err := app.Run([]string{"session", "open", "--token", "x", "demo", "--mode", "zzz"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("bad mode must be a usage error, got %v", err)
	}
	app2, _, _ := newTestApp(t)
	err = app2.Run([]string{"session", "open", "--token", "x", "demo", "--ttl", "nope"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("bad ttl must be a usage error, got %v", err)
	}
}
