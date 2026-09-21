package rest

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
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
	// pending is the multi-name stage for linked scratch: Link
	// appends, Relink replaces the whole set, Close publishes to
	// every name after pre-validating all of them. path mirrors
	// the first pending name for the unlinked resolve paths.
	pending test.PendingNames
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
	// Linked scratch publishes to every pending name with create
	// semantics: pre-validate all names first, so a target taken
	// since link fails the whole commit with nothing published.
	if s.linked {
		p := c.project(s.project)
		for _, name := range s.pending.List() {
			if _, ok := p.files[name]; ok {
				return shfs.AlreadyExists(name)
			}
		}
		for _, name := range s.pending.List() {
			c.commitFakeSessionLocked(s.project, name, s.data)
		}
		s.dirty = false
		return nil
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
	if !s.linked && s.path != "" {
		return fmt.Errorf("link session %s to %s: %w", shortFakeHandle(handleID), path, storage.ErrSessionLinked)
	}
	clean, err := c.checkFakeSessionTargetLocked(s, path)
	if err != nil {
		return err
	}
	first := s.pending.Empty()
	if err := s.pending.Add(clean); err != nil {
		return fmt.Errorf("link session %s to %s: %w", shortFakeHandle(handleID), path, storage.ErrSessionLinked)
	}
	if first {
		s.path = clean
	}
	s.linked = true
	s.dirty = true
	return nil
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
	clean, err := c.checkFakeSessionTargetLocked(s, path)
	if err != nil {
		return err
	}
	s.pending.Replace(clean)
	s.path = clean
	s.linked = true
	s.dirty = true
	return nil
}

// checkFakeSessionTargetLocked validates a link target (shared by link
// and relink): parent must exist with nothing at the target. Naming
// stages creation, so linked=true marks create-semantics for the commit.
// Returns the clean name; mutation (pending set, path) stays with the
// caller, which owns append-vs-replace.
func (c *fakeRESTClient) checkFakeSessionTargetLocked(s *fakeSession, path string) (string, error) {
	clean, err := cleanRESTPath(path)
	if err != nil {
		return "", err
	}
	p := c.project(s.project)
	if _, ok := p.files[clean]; ok {
		return "", shfs.AlreadyExists(clean)
	}
	if _, ok := p.dirs[clean]; ok {
		return "", shfs.AlreadyExists(clean)
	}
	if parent := parentPath(clean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return "", test.MissingSessionParent(parent)
		}
	}
	return clean, nil
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
	if s.linked {
		// Linked scratch publishes to every pending name with create
		// semantics (like UploadFileContext): pre-validate all
		// names first, so a target taken since link fails instead
		// of overwriting, publishing nothing and leaving the
		// description open. Opened-with-path handles overwrite
		// (replace semantics) as before.
		p := c.project(s.project)
		for _, name := range s.pending.List() {
			if _, ok := p.files[name]; ok {
				return shfs.AlreadyExists(name)
			}
		}
		if s.dirty {
			for _, name := range s.pending.List() {
				c.commitFakeSessionLocked(s.project, name, s.data)
			}
		}
		delete(c.sessions, handleID)
		return nil
	}
	if s.dirty {
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
