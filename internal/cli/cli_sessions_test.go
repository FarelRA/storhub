package cli

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/FarelRA/storhub/internal/test"
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

func (h *fakeHub) OpenSession(_ context.Context, project, path string, mode storage.OpenMode, _ ...storage.SessionOption) (string, error) {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	store.next++
	id := fmt.Sprintf("clisess-%d", store.next)
	store.sessions[id] = &cliSession{project: project, path: path, mode: mode}
	return id, nil
}

func (h *fakeHub) ReadSession(_ context.Context, handleID string, offset, length int64) ([]byte, error) {
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

func (h *fakeHub) WriteSession(_ context.Context, handleID string, offset int64, data []byte) (int, error) {
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

func (h *fakeHub) TruncateSession(_ context.Context, handleID string, size int64) error {
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

func (h *fakeHub) StatSession(_ context.Context, handleID string) (storage.SessionStat, error) {
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

func (h *fakeHub) SyncSession(_ context.Context, handleID string) error {
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

func (h *fakeHub) LinkSession(_ context.Context, handleID, path string) error {
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

func (h *fakeHub) RelinkSession(_ context.Context, handleID, path string) error {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	sess, err := store.live(handleID)
	if err != nil {
		return err
	}
	sess.path = path
	sess.dirty = true
	return nil
}

func (h *fakeHub) CloseSession(_ context.Context, handleID string) error {
	store := h.sessionStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, err := store.live(handleID); err != nil {
		return err
	}
	delete(store.sessions, handleID)
	return nil
}

// pcSession is one emulated open handle: a pinned byte snapshot plus
// staged writes, committed on sync/close. Commits resolve the publish
// target by open-time inode identity, so a rename is followed and an
// unlink discards, mirroring the real session manager.
type pcSession struct {
	path  string
	mode  storage.OpenMode
	data  []byte
	dirty bool
	ino   uint64
	// linked marks handles named by LinkSession (vs opened with a
	// path): their commit creates, so a taken target fails like
	// UploadFileContext instead of silently overwriting. expires
	// bounds the description like the product idle TTL; every
	// operation past it fails stale.
	linked  bool
	expires time.Time
}

func pcSessionReadable(mode storage.OpenMode) bool {
	return mode&storage.SessionReadOnly != 0 || mode&storage.SessionReadWrite != 0
}

func pcSessionWritable(mode storage.OpenMode) bool {
	return mode&storage.SessionWriteOnly != 0 || mode&storage.SessionReadWrite != 0
}

// resolvePCCommitLocked maps a dirty handle to its publish path: the open
// path while it still names the open-time inode, a surviving name after
// a rename (sorted, deterministic), or "" when the inode lost its last
// name after open. Created handles (ino 0) keep the open path. Caller
// holds h.mu.
func (h *pcFakeHub) resolvePCCommitLocked(s *pcSession) string {
	if s.ino == 0 {
		return s.path
	}
	if f, ok := h.files[s.path]; ok && f.ino == s.ino {
		return s.path
	}
	var survivors []string
	for path, f := range h.files {
		if f.ino == s.ino {
			survivors = append(survivors, path)
		}
	}
	if len(survivors) == 0 {
		return ""
	}
	sort.Strings(survivors)
	return survivors[0]
}

// commitPCSessionLocked publishes staged bytes like the content verbs:
// overwrite-or-create, privilege bits cleared, mtime moved. Caller holds
// h.mu.
func (h *pcFakeHub) commitPCSessionLocked(s *pcSession, target string) {
	f, ok := h.files[target]
	if !ok {
		h.ensureParentsLocked(target)
		f = &pcFile{mode: 0o644, uid: 1000, gid: 1000, mtime: h.clock, atime: h.clock}
		h.files[target] = f
	}
	f.data = append([]byte(nil), s.data...)
	f.mode &^= 0o6000
	f.mtime = h.tick()
	f.atime = f.mtime
}

func (h *pcFakeHub) OpenSession(_ context.Context, project, path string, mode storage.OpenMode, opts ...storage.SessionOption) (string, error) {
	_ = project
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sessions == nil {
		h.sessions = map[string]*pcSession{}
	}
	// Honor requested TTLs through the shared clamp (default when
	// unset); every operation past expiry fails stale, checked in
	// livePCSessionLocked.
	ttl := test.ClampTTL(storage.RequestedTTL(opts), 10*time.Minute, time.Hour)
	expires := time.Now().Add(ttl)
	var data []byte
	var ino uint64
	if path != "" {
		p := strings.TrimPrefix(path, "/")
		path = p
		if f, ok := h.files[p]; ok {
			if f.ino == 0 {
				h.nextIno++
				f.ino = h.nextIno
			}
			ino = f.ino
			data = append([]byte(nil), f.data...)
		} else {
			if h.dirs[p] {
				return "", fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
			}
			if mode&storage.SessionCreate == 0 {
				return "", fmt.Errorf("%w: %s", shfs.ErrNotFound, p)
			}
			data = []byte{}
		}
	}
	h.nextSession++
	id := fmt.Sprintf("pcsess-%d", h.nextSession)
	h.sessions[id] = &pcSession{path: path, mode: mode, data: data, ino: ino, expires: expires}
	return id, nil
}

func (h *pcFakeHub) livePCSessionLocked(id string) (*pcSession, error) {
	s, ok := h.sessions[id]
	if !ok {
		// Unknown ids answer stale like expired ones, mirroring the
		// product (and the sibling fakes): an id that was never
		// issued and one that lapsed are indistinguishable, so every
		// operation past the first expiry keeps reporting stale
		// instead of degrading to NotFound.
		return nil, &storage.StaleSessionError{HandleID: id, Reason: "unknown handle"}
	}
	if test.Expired(s.expires, time.Now()) {
		delete(h.sessions, id)
		return nil, &storage.StaleSessionError{HandleID: id, Reason: "expired"}
	}
	return s, nil
}

func (h *pcFakeHub) ReadSession(_ context.Context, handleID string, offset, length int64) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.livePCSessionLocked(handleID)
	if err != nil {
		return nil, err
	}
	if !pcSessionReadable(s.mode) {
		return nil, fmt.Errorf("read session: %w", syscall.EBADF)
	}
	if length == 0 || offset >= int64(len(s.data)) {
		return []byte{}, nil
	}
	end := offset + length
	if end < offset || end > int64(len(s.data)) {
		end = int64(len(s.data))
	}
	return append([]byte(nil), s.data[offset:end]...), nil
}

func (h *pcFakeHub) WriteSession(_ context.Context, handleID string, offset int64, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.livePCSessionLocked(handleID)
	if err != nil {
		return 0, err
	}
	if !pcSessionWritable(s.mode) {
		return 0, fmt.Errorf("write session: %w", syscall.EBADF)
	}
	if len(data) == 0 {
		return 0, nil
	}
	if s.mode&storage.SessionAppend != 0 {
		offset = int64(len(s.data))
	}
	content := append([]byte(nil), s.data...)
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
	s.data = content
	s.dirty = true
	return len(data), nil
}

func (h *pcFakeHub) TruncateSession(_ context.Context, handleID string, size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.livePCSessionLocked(handleID)
	if err != nil {
		return err
	}
	if !pcSessionWritable(s.mode) {
		return fmt.Errorf("truncate session: %w", syscall.EBADF)
	}
	if size < 0 {
		return fmt.Errorf("%w: negative size", syscall.EINVAL)
	}
	if int64(len(s.data)) > size {
		s.data = append([]byte(nil), s.data[:size]...)
	} else {
		s.data = append(append([]byte(nil), s.data...), make([]byte, size-int64(len(s.data)))...)
	}
	s.dirty = true
	return nil
}

func (h *pcFakeHub) StatSession(_ context.Context, handleID string) (storage.SessionStat, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.livePCSessionLocked(handleID)
	if err != nil {
		return storage.SessionStat{}, err
	}
	return storage.SessionStat{Project: "pc", Path: s.path, Size: int64(len(s.data)), Dirty: s.dirty, Mode: s.mode}, nil
}

func (h *pcFakeHub) SyncSession(_ context.Context, handleID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.livePCSessionLocked(handleID)
	if err != nil {
		return err
	}
	if s.path == "" {
		return fmt.Errorf("commit session: %w", storage.ErrSessionUnlinked)
	}
	if !s.dirty {
		return nil
	}
	// Linked scratch commits with create semantics: a taken target
	// fails instead of overwriting, like CloseSession.
	if s.linked {
		if _, ok := h.files[s.path]; ok {
			return fmt.Errorf("%w: %s", shfs.AlreadyExists(s.path), s.path)
		}
		if h.dirs[s.path] {
			return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, s.path)
		}
		if _, ok := h.links[s.path]; ok {
			return fmt.Errorf("%w: %s", shfs.AlreadyExists(s.path), s.path)
		}
	}
	// An inode unlinked after open has nowhere to publish: retain the
	// staged bytes (fsync equivalent) without publishing.
	target := h.resolvePCCommitLocked(s)
	if target == "" {
		return nil
	}
	h.commitPCSessionLocked(s, target)
	s.dirty = false
	return nil
}

func (h *pcFakeHub) LinkSession(_ context.Context, handleID, path string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.livePCSessionLocked(handleID)
	if err != nil {
		return err
	}
	if s.path != "" {
		return fmt.Errorf("link session %s to %s: %w", handleID, path, storage.ErrSessionLinked)
	}
	p := strings.TrimPrefix(path, "/")
	if p == "" {
		return fmt.Errorf("link session: %w: empty path", syscall.EINVAL)
	}
	if _, ok := h.files[p]; ok {
		return fmt.Errorf("%w: %s", shfs.AlreadyExists(path), p)
	}
	if h.dirs[p] {
		return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	if _, ok := h.links[p]; ok {
		return fmt.Errorf("%w: %s", shfs.AlreadyExists(path), p)
	}
	if parent := pcParentOf(p); parent != "" {
		if !h.dirs[parent] {
			return fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
		}
	}
	s.path = p
	s.linked = true
	s.dirty = true
	return nil
}

func (h *pcFakeHub) RelinkSession(_ context.Context, handleID, path string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.livePCSessionLocked(handleID)
	if err != nil {
		return err
	}
	p := strings.TrimPrefix(path, "/")
	if p == "" {
		return fmt.Errorf("relink session: %w: empty path", syscall.EINVAL)
	}
	if _, ok := h.files[p]; ok {
		return fmt.Errorf("%w: %s", shfs.AlreadyExists(path), p)
	}
	if h.dirs[p] {
		return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, p)
	}
	if _, ok := h.links[p]; ok {
		return fmt.Errorf("%w: %s", shfs.AlreadyExists(path), p)
	}
	if parent := pcParentOf(p); parent != "" {
		if !h.dirs[parent] {
			return fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
		}
	}
	s.path = p
	s.linked = true
	s.dirty = true
	return nil
}

func (h *pcFakeHub) CloseSession(_ context.Context, handleID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.livePCSessionLocked(handleID)
	if err != nil {
		return err
	}
	if s.path == "" {
		delete(h.sessions, handleID)
		return nil
	}
	if s.linked {
		// Linked scratch commits with create semantics (like
		// UploadFileContext): a target taken since link fails instead
		// of overwriting, leaving the description open for Relink.
		if _, ok := h.files[s.path]; ok {
			return fmt.Errorf("%w: %s", shfs.AlreadyExists(s.path), s.path)
		}
		if h.dirs[s.path] {
			return fmt.Errorf("%w: %s", shfs.ErrIsDirectory, s.path)
		}
		if _, ok := h.links[s.path]; ok {
			return fmt.Errorf("%w: %s", shfs.AlreadyExists(s.path), s.path)
		}
		if s.dirty {
			h.commitPCSessionLocked(s, s.path)
		}
		delete(h.sessions, handleID)
		return nil
	}
	if s.dirty {
		// An inode unlinked after open discards with success (POSIX
		// close); a rename is followed to the surviving name.
		target := h.resolvePCCommitLocked(s)
		if target == "" {
			delete(h.sessions, handleID)
			return nil
		}
		h.commitPCSessionLocked(s, target)
	}
	delete(h.sessions, handleID)
	return nil
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
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
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
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
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

func TestCLISessionRelinkRetargetsHandle(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	fake := &fakeHub{t: t}
	newHubFromFlagsFn = func(_, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	opened := runSessionCLI(t, []string{"session", "open", "--token", "x", "demo", "--mode", "w"})
	handle := strings.TrimSpace(opened)
	if handle == "" {
		t.Fatalf("open printed no handle: %q", opened)
	}
	runSessionCLI(t, []string{"session", "write", "--token", "x", "--handle", handle, "0", "hello"})
	runSessionCLI(t, []string{"session", "link", "--token", "x", "--handle", handle, "docs/a.txt"})
	runSessionCLI(t, []string{"session", "relink", "--token", "x", "--handle", handle, "docs/b.txt"})
	statJSON := runSessionCLI(t, []string{"session", "stat", "--token", "x", "--json", "--handle", handle})
	if !strings.Contains(statJSON, `"path":"docs/b.txt"`) {
		t.Fatalf("relink must retarget the handle: %s", statJSON)
	}
	app, _, _ := newTestApp(t)
	if err := app.Run([]string{"session", "relink", "--token", "x", "only-path"}); err == nil {
		t.Fatal("relink without --handle must fail")
	}
}
