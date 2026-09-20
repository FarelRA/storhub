package rest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/FarelRA/storhub/internal/test"
)

// rest_sessions_test.go: Phase 2B session manager over REST.
//
// The fake client below emulates the manager contract in memory (pinned
// snapshot at open, own-writes-visible reads, commit on sync/close, owner
// checks from ctx identity, stale and busy errors) so the handler mapping,
// invisibility, and snapshot stability are exercised through HTTP. The fake
// returns the real storage sentinel errors, so status mapping is genuine.

// fakeSession is one emulated open handle.
type fakeSession struct {
	project  string
	path     string
	mode     storage.OpenMode
	data     []byte
	dirty    bool
	ownerUID uint32
	hasOwner bool
	expires  time.Time
	// linked marks handles named by LinkSession (vs opened with a path):
	// their commit creates, so a taken target fails like UploadFileContext
	// instead of silently overwriting.
	linked bool
	// inode is the open-time file identity (0 when the file was missing
	// at open). Commits resolve the publish target by identity, so a
	// rename is followed and an unlink discards, mirroring the real
	// session manager's resolveCommitPathLocked.
	inode uint64
}

// resolveFakeCommitLocked maps a dirty fake handle to its publish path:
// the open path while it still names the open-time inode, a surviving
// name after a rename (sorted, deterministic), or "" when the inode lost
// its last name after open (close discards, sync retains). Created
// handles (inode 0) keep the open path. Caller holds c.mu.
func (c *fakeRESTClient) resolveFakeCommitLocked(s *fakeSession) string {
	if s.inode == 0 {
		return s.path
	}
	p := c.project(s.project)
	if node, ok := p.files[s.path]; ok && node.entry.Inode == s.inode {
		return s.path
	}
	var survivors []string
	for path, node := range p.files {
		if node.entry.Inode == s.inode {
			survivors = append(survivors, path)
		}
	}
	if len(survivors) == 0 {
		return ""
	}
	sort.Strings(survivors)
	return survivors[0]
}

func fakeSessionReadable(mode storage.OpenMode) bool {
	return mode&storage.SessionReadOnly != 0 || mode&storage.SessionReadWrite != 0
}

func fakeSessionWritable(mode storage.OpenMode) bool {
	return mode&storage.SessionWriteOnly != 0 || mode&storage.SessionReadWrite != 0
}

func shortFakeHandle(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// fakeStaleSession builds the typed stale error for the shared core: the
// core owns the unknown/expired rules, this one line owns the error value
// so lookups keep asserting errors.Is against the storage sentinel.
func fakeStaleSession(handleID, reason string) error {
	return &storage.StaleSessionError{HandleID: handleID, Reason: reason}
}

// liveFakeSessionLocked resolves a handle, sweeping it when expired.
// Lookup and lazy reap live in the shared core; owner checks stay here.
// Caller holds c.mu.
func (c *fakeRESTClient) liveFakeSessionLocked(id string) (*fakeSession, error) {
	return test.LiveSession(c.sessions, id, func(s *fakeSession) time.Time { return s.expires }, time.Now(), fakeStaleSession)
}

// authorizeFakeSessionLocked mirrors the manager: no identity or admin
// bypasses, otherwise the caller UID must match the opener.
func (c *fakeRESTClient) authorizeFakeSessionLocked(ctx context.Context, id string, s *fakeSession) error {
	if !shfs.IdentityPresent(ctx) {
		return nil
	}
	caller := shfs.IdentityFromContext(ctx)
	if caller.Admin {
		return nil
	}
	if !s.hasOwner || caller.UID == s.ownerUID {
		return nil
	}
	return fmt.Errorf("session %s: %w (owner uid %d): %w", shortFakeHandle(id), storage.ErrSessionOwnerMismatch, s.ownerUID, syscall.EPERM)
}

// sweepFakeSessionsLocked reaps expired handles via the shared core.
// Caller holds c.mu.
func (c *fakeRESTClient) sweepFakeSessionsLocked(now time.Time) {
	test.SweepExpiredSessions(c.sessions, func(s *fakeSession) time.Time { return s.expires }, now)
}

func (c *fakeRESTClient) fakeSessionTTL() time.Duration {
	if c.sessionTTL > 0 {
		return c.sessionTTL
	}
	return storage.DefaultSessionIdleTTL
}

func (c *fakeRESTClient) fakeSessionCaps() (perProject, perUser int) {
	perProject, perUser = c.maxSessProject, c.maxSessUser
	if perProject <= 0 {
		perProject = storage.MaxSessionsPerProject
	}
	if perUser <= 0 {
		perUser = storage.MaxSessionsPerUser
	}
	return perProject, perUser
}

// commitFakeSessionLocked publishes staged bytes to the fake tree.
// Caller holds c.mu. Like every fake content verb, a publish clears
// setuid/setgid (the fake has no admin concept by design; admin truth
// lives in storage tests).
func (c *fakeRESTClient) commitFakeSessionLocked(project, path string, data []byte) {
	p := c.project(project)
	node, ok := p.files[path]
	if !ok {
		now := c.tick()
		node = &fakeRESTNode{
			entry: &EntryInfo{Path: path, Size: 0, Inode: c.allocInode(), Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now},
			xattr: map[string][]byte{},
			data:  &fakeRESTData{bytes: []byte{}, nlink: 1, kind: NodeKindFile},
		}
		p.files[path] = node
	}
	node.data.bytes = append([]byte(nil), data...)
	node.entry.Mode &^= 0o6000
	c.touchDataLocked(p, node.data, c.tick())
}

func (c *fakeRESTClient) OpenSession(ctx context.Context, project, path string, mode storage.OpenMode, opts ...storage.SessionOption) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessions == nil {
		c.sessions = map[string]*fakeSession{}
	}
	now := time.Now()
	c.sweepFakeSessionsLocked(now)
	// Honor requested TTLs through the shared core (default knob when
	// unset); every operation past expiry fails stale in
	// liveFakeSessionLocked.
	expires := test.SessionExpiryAt(now, storage.RequestedTTL(opts), c.fakeSessionTTL(), time.Hour)
	ownerUID := shfs.IdentityFromContext(ctx).UID
	maxProject, maxUser := c.fakeSessionCaps()
	projectCount, userCount := 0, 0
	for _, s := range c.sessions {
		if s.project == project {
			projectCount++
		}
		if s.ownerUID == ownerUID {
			userCount++
		}
	}
	if projectCount >= maxProject {
		return "", fmt.Errorf("open session %s: %w (cap %d)", project, storage.ErrSessionProjectBusy, maxProject)
	}
	if userCount >= maxUser {
		return "", fmt.Errorf("open session: %w (cap %d)", storage.ErrSessionUserBusy, maxUser)
	}
	var data []byte
	if path != "" {
		clean, err := cleanRESTPath(path)
		if err != nil {
			return "", err
		}
		path = clean
		p := c.project(project)
		if node, ok := p.files[path]; ok {
			if mode&storage.SessionExclusive != 0 {
				return "", shfs.AlreadyExists(path)
			}
			data = append([]byte(nil), node.data.bytes...)
		} else {
			if _, ok := p.dirs[path]; ok {
				return "", shfs.IsDirectory(path)
			}
			if mode&storage.SessionCreate == 0 {
				return "", shfs.NotFound(path)
			}
			data = []byte{}
		}
	}
	c.nextSession++
	id := fmt.Sprintf("fakesess-%d", c.nextSession)
	sess := &fakeSession{
		project:  project,
		path:     path,
		mode:     mode,
		data:     data,
		ownerUID: ownerUID,
		hasOwner: shfs.IdentityPresent(ctx),
		expires:  expires,
	}
	if path != "" {
		if node, ok := c.project(project).files[path]; ok {
			sess.inode = node.entry.Inode
		}
	}
	c.sessions[id] = sess
	return id, nil
}

func (c *fakeRESTClient) ReadSession(ctx context.Context, handleID string, offset, length int64) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.liveFakeSessionLocked(handleID)
	if err != nil {
		return nil, err
	}
	if err := c.authorizeFakeSessionLocked(ctx, handleID, s); err != nil {
		return nil, err
	}
	if !fakeSessionReadable(s.mode) {
		return nil, fmt.Errorf("read session %s: handle not open for reading: %w", shortFakeHandle(handleID), syscall.EBADF)
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

func (c *fakeRESTClient) WriteSession(ctx context.Context, handleID string, offset int64, data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.liveFakeSessionLocked(handleID)
	if err != nil {
		return 0, err
	}
	if err := c.authorizeFakeSessionLocked(ctx, handleID, s); err != nil {
		return 0, err
	}
	if !fakeSessionWritable(s.mode) {
		return 0, fmt.Errorf("write session %s: handle not open for writing: %w", shortFakeHandle(handleID), syscall.EBADF)
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

func (c *fakeRESTClient) TruncateSession(ctx context.Context, handleID string, size int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.liveFakeSessionLocked(handleID)
	if err != nil {
		return err
	}
	if err := c.authorizeFakeSessionLocked(ctx, handleID, s); err != nil {
		return err
	}
	if !fakeSessionWritable(s.mode) {
		return fmt.Errorf("truncate session %s: handle not open for writing: %w", shortFakeHandle(handleID), syscall.EBADF)
	}
	if size == int64(len(s.data)) {
		return nil
	}
	if int64(len(s.data)) > size {
		s.data = append([]byte(nil), s.data[:size]...)
	} else {
		s.data = append(append([]byte(nil), s.data...), make([]byte, size-int64(len(s.data)))...)
	}
	s.dirty = true
	return nil
}

func (c *fakeRESTClient) StatSession(ctx context.Context, handleID string) (storage.SessionStat, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.liveFakeSessionLocked(handleID)
	if err != nil {
		return storage.SessionStat{}, err
	}
	if err := c.authorizeFakeSessionLocked(ctx, handleID, s); err != nil {
		return storage.SessionStat{}, err
	}
	return storage.SessionStat{
		Project: s.project,
		Path:    s.path,
		Size:    int64(len(s.data)),
		Dirty:   s.dirty,
		Mode:    s.mode,
	}, nil
}

func (c *fakeRESTClient) SyncSession(ctx context.Context, handleID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.liveFakeSessionLocked(handleID)
	if err != nil {
		return err
	}
	if err := c.authorizeFakeSessionLocked(ctx, handleID, s); err != nil {
		return err
	}
	if s.path == "" {
		return fmt.Errorf("commit session %s: %w", shortFakeHandle(handleID), storage.ErrSessionUnlinked)
	}
	if !s.dirty {
		return nil
	}
	// Linked scratch commits with create semantics: a taken target
	// fails instead of overwriting, like CloseSession.
	if s.linked {
		p := c.project(s.project)
		if _, ok := p.files[s.path]; ok {
			return shfs.AlreadyExists(s.path)
		}
	}
	// An inode unlinked after open has nowhere to publish: sync keeps
	// the staged bytes readable (fsync equivalent) without publishing.
	target := c.resolveFakeCommitLocked(s)
	if target == "" {
		return nil
	}
	c.commitFakeSessionLocked(s.project, target, s.data)
	s.dirty = false
	return nil
}

func (c *fakeRESTClient) LinkSession(ctx context.Context, handleID, path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.liveFakeSessionLocked(handleID)
	if err != nil {
		return err
	}
	if err := c.authorizeFakeSessionLocked(ctx, handleID, s); err != nil {
		return err
	}
	if s.path != "" {
		return fmt.Errorf("link session %s to %s: %w", shortFakeHandle(handleID), path, storage.ErrSessionLinked)
	}
	return c.nameFakeSessionLocked(s, path)
}

// RelinkSession retargets a linked handle (rescue path); unlike Link it
// accepts an already-named handle. Mirrors the manager contract.
func (c *fakeRESTClient) RelinkSession(ctx context.Context, handleID, path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.liveFakeSessionLocked(handleID)
	if err != nil {
		return err
	}
	if err := c.authorizeFakeSessionLocked(ctx, handleID, s); err != nil {
		return err
	}
	return c.nameFakeSessionLocked(s, path)
}

// nameFakeSessionLocked validates and names a scratch handle (shared by
// link and relink): parent must exist with nothing at the target. Naming
// stages creation, so linked=true marks create-semantics for the commit.
func (c *fakeRESTClient) nameFakeSessionLocked(s *fakeSession, path string) error {
	clean, err := cleanRESTPath(path)
	if err != nil {
		return err
	}
	p := c.project(s.project)
	if _, ok := p.files[clean]; ok {
		return shfs.AlreadyExists(clean)
	}
	if _, ok := p.dirs[clean]; ok {
		return shfs.AlreadyExists(clean)
	}
	if parent := parentPath(clean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return test.MissingSessionParent(parent)
		}
	}
	s.path = clean
	s.linked = true
	s.dirty = true
	return nil
}

func (c *fakeRESTClient) CloseSession(ctx context.Context, handleID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.liveFakeSessionLocked(handleID)
	if err != nil {
		return err
	}
	if err := c.authorizeFakeSessionLocked(ctx, handleID, s); err != nil {
		return err
	}
	if s.path == "" {
		delete(c.sessions, handleID)
		return nil
	}
	if s.dirty {
		// Linked scratch commits with create semantics (like
		// UploadFileContext): a target taken since link fails instead
		// of overwriting. Opened-with-path handles overwrite (replace
		// semantics) as before.
		if s.linked {
			p := c.project(s.project)
			if _, ok := p.files[s.path]; ok {
				return shfs.AlreadyExists(s.path)
			}
		}
		// An inode unlinked after open discards with success (POSIX
		// close); a rename is followed to the surviving name.
		target := c.resolveFakeCommitLocked(s)
		if target == "" {
			delete(c.sessions, handleID)
			return nil
		}
		c.commitFakeSessionLocked(s.project, target, s.data)
	}
	delete(c.sessions, handleID)
	return nil
}

// --- test helpers ---

func openSessionHTTP(t *testing.T, handler http.Handler, project, path, mode string) string {
	t.Helper()
	payload := map[string]string{"project": project, "mode": mode}
	if path != "" {
		payload["path"] = path
	}
	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", payload, http.StatusCreated)
	var opened sessionOpenResponse
	decodeJSONBody(t, resp, &opened)
	if opened.Handle == "" {
		t.Fatal("open returned an empty handle")
	}
	return opened.Handle
}

func readSessionHTTP(t *testing.T, handler http.Handler, handle string, offset, length int64) []byte {
	t.Helper()
	target := "/api/v1/handles/" + handle + "?offset=" + itoa(offset) + "&length=" + itoa(length)
	resp := mustRequest(t, handler, http.MethodGet, target, nil, nil, http.StatusOK)
	return readBody(t, resp)
}

func itoa(v int64) string {
	return fmt.Sprintf("%d", v)
}

func writeSessionHTTP(t *testing.T, handler http.Handler, handle string, offset int64, data string) {
	t.Helper()
	payload := map[string]any{"offset": offset, "data": base64.StdEncoding.EncodeToString([]byte(data))}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+handle+"/write", payload, http.StatusOK)
}

func statSessionHTTP(t *testing.T, handler http.Handler, handle string) sessionStatResponse {
	t.Helper()
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+handle, nil, nil, http.StatusOK)
	var stat sessionStatResponse
	decodeJSONBody(t, resp, &stat)
	return stat
}

func contentHTTP(t *testing.T, handler http.Handler, project, path string) string {
	t.Helper()
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/"+project+"/content?path="+path, nil, nil, http.StatusOK)
	return string(readBody(t, resp))
}

// TestRESTSessionLifecycle drives open/write/read/sync/close through HTTP:
// staged writes stay invisible to a second client until close, a concurrent
// committer never moves the pin, and unlinked scratch needs a link.
func TestRESTSessionLifecycle(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=data.txt", strings.NewReader("version-one"), nil, http.StatusCreated)

	handle := openSessionHTTP(t, handler, "demo", "data.txt", "r")
	if got := string(readSessionHTTP(t, handler, handle, 0, 64)); got != "version-one" {
		t.Fatalf("pinned read = %q, want %q", got, "version-one")
	}

	writer := openSessionHTTP(t, handler, "demo", "data.txt", "w+")
	writeSessionHTTP(t, handler, writer, 0, "version-two!")
	if got := string(readSessionHTTP(t, handler, writer, 0, 64)); got != "version-two!" {
		t.Fatalf("own staged read = %q, want %q", got, "version-two!")
	}
	if got := contentHTTP(t, handler, "demo", "data.txt"); got != "version-one" {
		t.Fatalf("second client sees %q before commit, want pinned %q", got, "version-one")
	}

	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=data.txt", strings.NewReader("RIVAL-replace"), nil, http.StatusOK)
	if got := string(readSessionHTTP(t, handler, writer, 0, 64)); got != "version-two!" {
		t.Fatalf("pin moved under concurrent committer: %q", got)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+writer+"/sync", map[string]any{}, http.StatusOK)
	if stat := statSessionHTTP(t, handler, writer); stat.Dirty {
		t.Fatalf("sync must clear dirty, got %+v", stat)
	}
	if got := contentHTTP(t, handler, "demo", "data.txt"); got != "version-two!" {
		t.Fatalf("second client sees %q after sync, want %q", got, "version-two!")
	}

	mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+writer+"/close?sync=1", nil, nil, http.StatusOK)
	client.mu.Lock()
	drained := append([]string(nil), client.drainCalls...)
	client.mu.Unlock()
	if len(drained) != 1 || drained[0] != "demo" {
		t.Fatalf("close?sync=1 must drain demo once, got %v", drained)
	}
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+writer, nil, nil, http.StatusGone)
	assertErrorCode(t, resp, "gone")
}

// TestRESTSessionUnlinkedScratch covers the link and discard paths: sync
// without a link is a 409, link-then-sync publishes, and close without a
// link discards with no commit.
func TestRESTSessionUnlinkedScratch(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	scratch := openSessionHTTP(t, handler, "demo", "", "w")
	writeSessionHTTP(t, handler, scratch, 0, "scratch bytes")
	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/sync", map[string]any{}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/link", map[string]string{"path": "scratch.txt"}, http.StatusOK)
	if stat := statSessionHTTP(t, handler, scratch); stat.Path != "scratch.txt" || !stat.Dirty {
		t.Fatalf("link must name the handle and stage creation: %+v", stat)
	}
	mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/close", nil, nil, http.StatusOK)
	if got := contentHTTP(t, handler, "demo", "scratch.txt"); got != "scratch bytes" {
		t.Fatalf("linked scratch committed %q, want %q", got, "scratch bytes")
	}

	// Linking twice is a 409.
	linked := openSessionHTTP(t, handler, "demo", "scratch.txt", "r+")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+linked+"/link", map[string]string{"path": "other.txt"}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")

	// Discard path: close unlinked scratch commits nothing.
	discard := openSessionHTTP(t, handler, "demo", "", "w")
	writeSessionHTTP(t, handler, discard, 0, "doomed")
	mustRequest(t, handler, http.MethodDelete, "/api/v1/handles/"+discard, nil, nil, http.StatusNoContent)
	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=doomed.txt", nil, nil, http.StatusNotFound)
	assertErrorCode(t, resp, "not_found")
}

// TestRESTSessionTruncateAndStat pins truncate staging and the stat shape.
func TestRESTSessionTruncateAndStat(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=f.txt", strings.NewReader("hello world"), nil, http.StatusCreated)

	handle := openSessionHTTP(t, handler, "demo", "f.txt", "w+")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+handle+"/truncate", map[string]int64{"size": 5}, http.StatusOK)
	stat := statSessionHTTP(t, handler, handle)
	if stat.Size != 5 || !stat.Dirty || stat.Project != "demo" || stat.Path != "f.txt" {
		t.Fatalf("unexpected stat after truncate: %+v", stat)
	}
	if got := string(readSessionHTTP(t, handler, handle, 0, 64)); got != "hello" {
		t.Fatalf("read after truncate = %q, want %q", got, "hello")
	}
}

// TestRESTSessionErrorMapping pins every status mapping: stale (unknown and
// expired) to 410 with the reason, busy to 429, bad input to 400, and mode
// violations to 400.
func TestRESTSessionErrorMapping(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/handles/no-such-handle", nil, nil, http.StatusGone)
	if code, body := decodeSessionError(t, resp); code != "gone" || !strings.Contains(body, "unknown handle") {
		t.Fatalf("stale mapping = code %q body %q, want gone + unknown handle", code, body)
	}

	client.sessionTTL = 30 * time.Millisecond
	expiring := openSessionHTTP(t, handler, "demo", "", "w")
	time.Sleep(80 * time.Millisecond)
	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+expiring, nil, nil, http.StatusGone)
	if code, body := decodeSessionError(t, resp); code != "gone" || !strings.Contains(body, "expired") {
		t.Fatalf("expired mapping = code %q body %q, want gone + expired", code, body)
	}
	client.sessionTTL = 0

	client.maxSessProject = 1
	first := openSessionHTTP(t, handler, "demo", "", "w")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", map[string]string{"project": "demo", "mode": "w"}, http.StatusTooManyRequests)
	assertErrorCode(t, resp, "rate_limited")
	mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+first+"/close", nil, nil, http.StatusOK)
	client.maxSessProject = 0

	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", map[string]string{"project": "demo", "mode": "zzz"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", map[string]string{"project": "demo", "mode": "r", "ttl": "not-a-duration"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", map[string]string{"mode": "r"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")

	readonly := openSessionHTTP(t, handler, "demo", "", "r")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+readonly+"/write",
		map[string]any{"offset": 0, "data": base64.StdEncoding.EncodeToString([]byte("x"))}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")

	bad := openSessionHTTP(t, handler, "demo", "", "w")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+bad+"/write",
		map[string]any{"offset": 0, "data": "!!!not-base64!!!"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
}

func decodeSessionError(t *testing.T, resp *http.Response) (code, message string) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var payload restError
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if payload.Error.Code == "" {
		t.Fatal("error body missing code")
	}
	return payload.Error.Code, payload.Error.Message
}

// TestRESTSessionOwnerMismatch pins identity continuity: a handle opened by
// one user fails closed for another (403) while an admin still passes.
func TestRESTSessionOwnerMismatch(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	opts := DefaultOptions()
	opts.ShareSigningKey = []byte("abcdef0123456789abcdef0123456789")
	opts.Auth = &AuthOptions{
		TokenSigningKey: []byte("test-signing-key-0123456789abcdef"),
		Users: []User{
			{Username: "alice", Password: "alice-pass", UID: 1001, PrimaryGID: 2001},
			{Username: "bob", Password: "bob-pass", UID: 1002, PrimaryGID: 2002},
			{Username: "root", Password: "root-pass", UID: 0, PrimaryGID: 0, Admin: true},
		},
	}
	handler, err := newHandlerForClient(client, opts)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	login := func(user, pass string) string {
		t.Helper()
		resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: user, Password: pass}, http.StatusOK)
		var logged restLoginResponse
		decodeJSONBody(t, resp, &logged)
		return logged.Token
	}
	alice, bob, root := login("alice", "alice-pass"), login("bob", "bob-pass"), login("root", "root-pass")

	// Alice opens: the manager records her UID as the owner.
	authed := map[string]string{"Authorization": "Bearer " + alice}
	openResp := mustJSONRequestAuthed(t, handler, http.MethodPost, "/api/v1/handles",
		map[string]string{"project": "demo", "mode": "w"}, authed, http.StatusCreated)
	var opened sessionOpenResponse
	decodeJSONBody(t, openResp, &opened)

	bobHeaders := map[string]string{"Authorization": "Bearer " + bob}
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+opened.Handle, nil, bobHeaders, http.StatusForbidden)
	assertErrorCode(t, resp, "forbidden")

	rootHeaders := map[string]string{"Authorization": "Bearer " + root}
	mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+opened.Handle, nil, rootHeaders, http.StatusOK)
	mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+opened.Handle, nil, authed, http.StatusOK)
}

func mustJSONRequestAuthed(t *testing.T, handler http.Handler, method, target string, payload any, headers map[string]string, wantStatus int) *http.Response {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	merged := map[string]string{"Content-Type": "application/json"}
	for key, value := range headers {
		merged[key] = value
	}
	return mustRequest(t, handler, method, target, strings.NewReader(string(body)), merged, wantStatus)
}

func TestRESTSessionRelinkRescuesTakenTarget(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	scratch := openSessionHTTP(t, handler, "demo", "", "w")
	writeSessionHTTP(t, handler, scratch, 0, "rescued bytes")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/link", map[string]string{"path": "taken.txt"}, http.StatusOK)
	// A concurrent writer takes the linked target through the plain
	// content endpoint.
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=taken.txt", strings.NewReader("rival"), nil, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/close", nil, nil, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")
	// Relink rescues the staged bytes at a free name; the rival keeps
	// its own content.
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/relink", map[string]string{"path": "rescued.txt"}, http.StatusOK)
	if stat := statSessionHTTP(t, handler, scratch); stat.Path != "rescued.txt" {
		t.Fatalf("relink must rename the handle target: %+v", stat)
	}
	mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/close", nil, nil, http.StatusOK)
	if got := contentHTTP(t, handler, "demo", "rescued.txt"); got != "rescued bytes" {
		t.Fatalf("relocated content = %q, want %q", got, "rescued bytes")
	}
	if got := contentHTTP(t, handler, "demo", "taken.txt"); got != "rival" {
		t.Fatalf("rival content disturbed: %q", got)
	}
	// Relink to a taken name fails loudly instead of stealing it.
	other := openSessionHTTP(t, handler, "demo", "", "w")
	writeSessionHTTP(t, handler, other, 0, "x")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+other+"/link", map[string]string{"path": "other.txt"}, http.StatusOK)
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+other+"/relink", map[string]string{"path": "taken.txt"}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")
}
