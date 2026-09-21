package storage

// sessions.go: stateful open sessions (FUSE handles over HTTP), Phase 2B.
//
// A session is an open file handle held server-side. Open pins the content
// layout (revision SHA plus the file entry and chunk descriptors needed to
// re-read that snapshot through ReadPinnedFileContext), stages writes into
// a server-side temp file under the spool base with dirty-range tracking,
// and commits through the standard hub verbs only on Sync or Close.
//
// Visibility rules:
//   - Reads serve the pinned snapshot plus the handle's own staged writes
//     (own-writes-visible). A newer remote version is never served: the pin
//     moves only to the handle's own committed state on Sync, or on reopen.
//   - Staged writes are invisible to everyone else until Sync or Close
//     commits them, so multi-call sequences stay atomic to a second hub.
//   - DAC is resolved ONCE at open (read access for read modes, write
//     access for write modes, parent-write for creates) with the same Check
//     functions the fs layer uses. Every later operation re-validates handle
//     ownership (stored UID vs caller UID, admin bypass mirroring
//     checkOverlayCaller semantics) and fails closed on mismatch.
//
// Lifecycle rules:
//   - SyncSession commits staged state without closing, then drains the
//     commit it made (DrainProjectContext semantics: fail loud).
//   - CloseSession commits staged state and destroys the handle. Close with
//     no staged state is a no-op success. Close on unlinked scratch without
//     a prior link discards the temp with no commit.
//   - LinkSession appends a validated name to the handle's ordered
//     pending-name set (O_TMPFILE equivalent first link), DAC-checked at
//     link time like create. Linking stages the creation, so
//     link-then-close persists even an empty file. Linking an
//     already-pending name fails with ErrSessionLinked; RelinkSession
//     replaces the whole pending set with one validated path. Close and
//     Sync pre-validate every pending name, then publish the staged bytes
//     to each with create semantics: any taken name fails the whole
//     operation with AlreadyExists, publishing nothing, and the handle
//     stays open for Relink.
//   - Idle TTL defaults to 10 minutes with a configurable max cap. There is
//     no background goroutine: expired handles are swept when opening new
//     ones plus lazily on use. Expired and unknown ids answer StaleSessionError.
//   - State is in-memory only: a server restart drops everything. Scratch
//     temps orphaned that way (or by expiry) are quarantined under the spool
//     base for manual recovery, never auto-redriven. Use
//     QuarantineStaleSessionTemps at startup to collect them.
//   - Per-project and per-user handle caps return explicit busy errors.
//
// Concurrency: one mutex per hub guards the session table and every
// handle's staging file, including across the network calls a commit
// makes. Commits additionally serialize on a per-hub commit mutex so two
// closes cannot interleave their verb sequences.
//
// The registry is keyed by hub pointer (no StorHub struct changes: this
// file is additive only), so a fresh hub naturally holds no sessions.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
)

// Session capacity and lifetime policy. All bounds are documented here so a
// limit change has one edit site; per-hub overrides go through
// ConfigureSessions.
const (
	// DefaultSessionIdleTTL is the idle expiry for a handle: no successful
	// use within this window makes the next use (or the next open) reap it.
	DefaultSessionIdleTTL = 120 * storcfg.PatienceUnit
	// DefaultSessionMaxTTL caps how large any handle TTL may grow. A larger
	// per-open request is clamped, not rejected.
	DefaultSessionMaxTTL = 720 * storcfg.PatienceUnit
	// MaxSessionsPerProject bounds open handles naming one project.
	MaxSessionsPerProject = 64
	// MaxSessionsPerUser bounds open handles owned by one UID across projects.
	MaxSessionsPerUser = 128
)

var (
	// ErrStaleSession matches any expired or unknown handle id. Prefer
	// errors.As with *StaleSessionError when the reason matters.
	ErrStaleSession = errors.New("storhub: session handle is stale")
	// ErrSessionProjectBusy reports the per-project handle cap.
	ErrSessionProjectBusy = errors.New("storhub: too many open sessions for project")
	// ErrSessionUserBusy reports the per-user handle cap.
	ErrSessionUserBusy = errors.New("storhub: too many open sessions for user")
	// ErrSessionOwnerMismatch reports a handle driven by a UID other than
	// the opener (admins bypass). It wraps syscall.EPERM.
	ErrSessionOwnerMismatch = errors.New("storhub: session owned by another user")
	// ErrSessionUnlinked reports syncing (or committing) a scratch handle
	// that was never linked to a path.
	ErrSessionUnlinked = errors.New("storhub: session has no path: link it before sync or close")
	// ErrSessionLinked reports linking an already-pending name, or linking
	// a handle opened with a path (which keeps single-path replace
	// semantics and can only be retargeted with RelinkSession).
	ErrSessionLinked = errors.New("storhub: session already has a path")
	// ErrSessionPathGone reports a commit or sync for a handle whose
	// pinned inode lost its last name after open (unlinked or renamed
	// away with no surviving link). Close maps it to discard-success
	// (POSIX: closing an unlinked fd drops the data); sync maps it to
	// retain-and-succeed (POSIX: fsync on an unlinked fd succeeds, and
	// the staged bytes stay readable until close).
	ErrSessionPathGone = errors.New("storhub: session path unlinked after open")
)

// StaleSessionError is the typed stale-handle error answered for expired or
// unknown ids. Reason is "expired" or "unknown handle".
type StaleSessionError struct {
	HandleID string
	Reason   string
}

func (e *StaleSessionError) Error() string {
	return fmt.Sprintf("%s (handle %s): %s", ErrStaleSession, shortSHA(e.HandleID), e.Reason)
}

func (e *StaleSessionError) Unwrap() error { return ErrStaleSession }

func newStaleSessionError(handleID, reason string) *StaleSessionError {
	return &StaleSessionError{HandleID: handleID, Reason: reason}
}

// OpenMode is a bitmask describing how a session may be used. The access
// bits mirror POSIX open flags; Combine them, e.g. SessionWriteOnly |
// SessionCreate | SessionTruncate, or use ParseOpenMode for fopen-style
// strings ("r", "w", "a", "r+", "w+", "a+", optional "x" for exclusive).
type OpenMode int

const (
	// SessionReadOnly opens for reads only.
	SessionReadOnly OpenMode = 1 << iota
	// SessionWriteOnly opens for writes only.
	SessionWriteOnly
	// SessionReadWrite opens for reads and writes.
	SessionReadWrite
	// SessionCreate creates the file if missing (staged until commit).
	SessionCreate
	// SessionTruncate truncates an existing file to zero at open (staged
	// until commit). Requires a write bit.
	SessionTruncate
	// SessionAppend forces every write to the current end (staged until
	// commit). Requires a write bit.
	SessionAppend
	// SessionExclusive fails the open when the file already exists.
	// Requires SessionCreate.
	SessionExclusive
)

// String renders the mode as a compact flag set for debugging.
func (m OpenMode) String() string {
	out := ""
	if m&SessionReadOnly != 0 {
		out += "r"
	}
	if m&SessionWriteOnly != 0 {
		out += "w"
	}
	if m&SessionReadWrite != 0 {
		out += "rw"
	}
	if m&SessionCreate != 0 {
		out += "+create"
	}
	if m&SessionTruncate != 0 {
		out += "+trunc"
	}
	if m&SessionAppend != 0 {
		out += "+append"
	}
	if m&SessionExclusive != 0 {
		out += "+excl"
	}
	if out == "" {
		return "none"
	}
	return out
}

func (m OpenMode) readable() bool { return m&SessionReadOnly != 0 || m&SessionReadWrite != 0 }

func (m OpenMode) writable() bool { return m&SessionWriteOnly != 0 || m&SessionReadWrite != 0 }

// ParseOpenMode maps fopen-style strings to an OpenMode: "r" read,
// "r+" read/write, "w" write/create/truncate, "w+" read/write/create/
// truncate, "a" write/create/append, "a+" read/write/create/append.
// Appending "x" ("wx", "ax") adds create-exclusive.
func ParseOpenMode(s string) (OpenMode, error) {
	switch s {
	case "r":
		return SessionReadOnly, nil
	case "r+":
		return SessionReadWrite, nil
	case "w":
		return SessionWriteOnly | SessionCreate | SessionTruncate, nil
	case "w+":
		return SessionReadWrite | SessionCreate | SessionTruncate, nil
	case "a":
		return SessionWriteOnly | SessionCreate | SessionAppend, nil
	case "a+":
		return SessionReadWrite | SessionCreate | SessionAppend, nil
	case "wx", "xw":
		return SessionWriteOnly | SessionCreate | SessionTruncate | SessionExclusive, nil
	case "w+x", "wx+":
		return SessionReadWrite | SessionCreate | SessionTruncate | SessionExclusive, nil
	case "ax", "xa":
		return SessionWriteOnly | SessionCreate | SessionAppend | SessionExclusive, nil
	case "a+x", "xa+":
		return SessionReadWrite | SessionCreate | SessionAppend | SessionExclusive, nil
	default:
		return 0, fmt.Errorf("invalid open mode %q: want one of r r+ w w+ a a+ with optional x", s)
	}
}

// SessionOption decorates one OpenSession call.
type SessionOption func(*sessionOpenOptions)

type sessionOpenOptions struct {
	ttl time.Duration
}

// WithSessionTTL requests an idle TTL for the new handle. Non-positive
// means the hub default; anything above the hub max cap is clamped to it.
func WithSessionTTL(d time.Duration) SessionOption {
	return func(o *sessionOpenOptions) { o.ttl = d }
}

// RequestedTTL folds SessionOptions and reports the requested idle TTL:
// <=0 means "hub default". Conformance doubles (the oracle plus the
// CLI/REST fakes) honor it so the shared table can drive handle expiry;
// production clamps it to [default, max] at open. It lives in prod code
// only because SessionOption closes over unexported state that no
// external test package can decode; the clamping itself is owned by
// test.ClampTTL so fakes share one implementation.
func RequestedTTL(opts []SessionOption) time.Duration {
	var o sessionOpenOptions
	for _, fn := range opts {
		fn(&o)
	}
	return o.ttl
}

// SessionHubOption tunes one hub's session policy.
type SessionHubOption func(*sessionHubPolicy)

type sessionHubPolicy struct {
	maxPerProject int
	maxPerUser    int
	defaultTTL    time.Duration
	maxTTL        time.Duration
}

// WithSessionMaxPerProject overrides MaxSessionsPerProject for one hub.
// Non-positive values are ignored.
func WithSessionMaxPerProject(n int) SessionHubOption {
	return func(p *sessionHubPolicy) { p.maxPerProject = n }
}

// WithSessionMaxPerUser overrides MaxSessionsPerUser for one hub.
// Non-positive values are ignored.
func WithSessionMaxPerUser(n int) SessionHubOption {
	return func(p *sessionHubPolicy) { p.maxPerUser = n }
}

// WithSessionDefaultTTL overrides DefaultSessionIdleTTL for one hub.
// Non-positive values are ignored.
func WithSessionDefaultTTL(d time.Duration) SessionHubOption {
	return func(p *sessionHubPolicy) { p.defaultTTL = d }
}

// WithSessionMaxTTL overrides DefaultSessionMaxTTL for one hub, the cap
// per-open TTL requests clamp to. Non-positive values are ignored.
func WithSessionMaxTTL(d time.Duration) SessionHubOption {
	return func(p *sessionHubPolicy) { p.maxTTL = d }
}

// SessionStat describes a live handle.
type SessionStat struct {
	Project string
	Path    string
	Size    int64
	Dirty   bool
	Mode    OpenMode
	// Stale reports whether a newer revision was committed after this
	// handle pinned its snapshot (pin revision != current committed
	// revision). Reads intentionally keep serving the pin (snapshot
	// isolation); this flag only advertises that a reopen would see
	// newer content. It is computed from the resident cache entry, so
	// it never performs I/O: a missing or unhydrated entry reports
	// false rather than reloading remote truth inside a stat call.
	Stale bool
}

// openSession is one live handle. mu serializes operations on this handle
// ID so distinct handles proceed in parallel once the table lock is
// dropped across network I/O. Fields written after insert (path and
// pending on link/relink, pin/revision/sizes/staging/dirty/applied/ranges/
// lastUse/tmp) are guarded by mu. project/mode/ownerUID/hasOpener/ttl are
// immutable after insert and safe to read under the table lock. destroyed
// marks removal from the table; holders of mu check it after re-acquiring
// the table lock.
type openSession struct {
	mu      sync.Mutex
	id      string
	project string
	path    string
	// pending is the ordered set of names staged bytes publish to on
	// Close or Sync. Scratch handles grow it with LinkSession (append)
	// and replace it with RelinkSession (exactly one name). Handles
	// opened with a path keep it empty and commit through the single
	// open path with replace semantics. path always mirrors pending[0]
	// while pending is non-empty, so single-name readers (StatSession,
	// quarantine) keep working unchanged.
	pending   []string
	mode      OpenMode
	ownerUID  uint32
	hasOpener bool
	// opener pins the open-time caller identity for the commit path:
	// staged bytes are the opener's work, so ownership and
	// privilege-clearing follow the opener even when someone else
	// (necessarily the opener or an admin, per authorize) triggers the
	// commit. Without this an admin closing a user's session would
	// reassign its files to the admin.
	opener       shfs.Identity
	revision     string
	pinned       FileMeta
	pinnedChunks map[int64]ChunkInfo
	baseSize     int64
	tmp          *os.File
	tmpName      string
	curSize      int64
	staged       bool
	dirty        bool
	applied      bool
	created      bool
	fullImage    bool
	appendOnly   bool
	ranges       []byteRange
	lastUse      time.Time
	ttl          time.Duration
	destroyed    bool
}

// sessionHubState is one hub's session table plus policy.
type sessionHubState struct {
	mu            sync.Mutex
	commitMu      sync.Mutex
	byID          map[string]*openSession
	now           func() time.Time
	maxPerProject int
	maxPerUser    int
	defaultTTL    time.Duration
	maxTTL        time.Duration
}

// sessionHub returns the hub's session table, creating it on first use. A
// fresh hub therefore holds no sessions: restarts drop everything. The
// table lives on the hub (not in a global registry) so dead hubs take
// their sessions with them instead of leaking.
func (h *StorHub) sessionHub() *sessionHubState {
	h.sessionMu.Lock()
	defer h.sessionMu.Unlock()
	sh := h.sessions
	if sh == nil {
		now := h.config.Now
		if now == nil {
			now = time.Now
		}
		sh = &sessionHubState{
			byID:          make(map[string]*openSession),
			now:           now,
			maxPerProject: MaxSessionsPerProject,
			maxPerUser:    MaxSessionsPerUser,
			defaultTTL:    DefaultSessionIdleTTL,
			maxTTL:        DefaultSessionMaxTTL,
		}
		h.sessions = sh
	}
	return sh
}

// ConfigureSessions tunes the hub's session policy (caps and TTLs).
func (h *StorHub) ConfigureSessions(opts ...SessionHubOption) {
	sh := h.sessionHub()
	p := sessionHubPolicy{
		maxPerProject: sh.maxPerProject,
		maxPerUser:    sh.maxPerUser,
		defaultTTL:    sh.defaultTTL,
		maxTTL:        sh.maxTTL,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&p)
		}
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if p.maxPerProject > 0 {
		sh.maxPerProject = p.maxPerProject
	}
	if p.maxPerUser > 0 {
		sh.maxPerUser = p.maxPerUser
	}
	if p.maxTTL > 0 {
		sh.maxTTL = p.maxTTL
	}
	if p.defaultTTL > 0 {
		sh.defaultTTL = p.defaultTTL
	}
	if sh.defaultTTL > sh.maxTTL {
		sh.defaultTTL = sh.maxTTL
	}
}

// newSessionID mints an opaque capability-shaped handle id: 128 bits of
// crypto randomness rendered as hex.
func newSessionID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("mint session handle: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

// OpenSession opens a stateful session on project/path. An empty path
// opens unlinked scratch (O_TMPFILE equivalent) that must be named with
// LinkSession before it can commit. DAC is resolved once here: read access
// for read modes, write access for write modes (parent-write for creates),
// using the same Check functions the fs layer uses. The content layout is
// pinned as the revision SHA plus the file entry and chunk descriptors
// needed to re-read the snapshot via ReadPinnedFileContext.
func (h *StorHub) OpenSession(ctx context.Context, project, path string, mode OpenMode, opts ...SessionOption) (handleID string, err error) {
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(project), "session open start", "project", project, "path", path, "mode", mode.String())
	defer func() {
		elapsed := h.config.Now().UTC().Sub(started)
		switch {
		case err == nil:
			logging.Debug(h.projectLogger(project), "session open complete", "project", project, "path", path, "mode", mode.String(), "handle", shortSHA(handleID), "elapsed", elapsed)
		case errors.Is(err, ErrSessionProjectBusy) || errors.Is(err, ErrSessionUserBusy):
			logging.Warn(h.projectLogger(project), "session open refused: handle cap reached", "project", project, "path", path, "mode", mode.String(), "elapsed", elapsed, "err", err)
		default:
			logging.Error(h.projectLogger(project), "session open failed", "project", project, "path", path, "mode", mode.String(), "elapsed", elapsed, "err", err)
		}
	}()
	if err := validateProject(project); err != nil {
		return "", err
	}
	if !mode.readable() && !mode.writable() {
		return "", fmt.Errorf("open session: mode %s grants neither read nor write", mode)
	}
	if mode&SessionTruncate != 0 && !mode.writable() {
		return "", fmt.Errorf("open session: truncate requires a write mode")
	}
	if mode&SessionAppend != 0 && !mode.writable() {
		return "", fmt.Errorf("open session: append requires a write mode")
	}
	if mode&SessionExclusive != 0 && mode&SessionCreate == 0 {
		return "", fmt.Errorf("open session: exclusive requires create")
	}
	var openOpts sessionOpenOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&openOpts)
		}
	}

	sh := h.sessionHub()
	sh.mu.Lock()
	now := sh.now()
	sh.sweepExpiredLocked(now)
	id := shfs.IdentityFromContext(ctx)
	hasOpener := shfs.IdentityPresent(ctx)
	projectCount, userCount := 0, 0
	for _, s := range sh.byID {
		if s.project == project {
			projectCount++
		}
		if s.ownerUID == id.UID {
			userCount++
		}
	}
	maxPerProject, maxPerUser := sh.maxPerProject, sh.maxPerUser
	defaultTTL, maxTTL := sh.defaultTTL, sh.maxTTL
	sh.mu.Unlock()

	if projectCount >= maxPerProject {
		return "", fmt.Errorf("open session %s: %w (cap %d)", project, ErrSessionProjectBusy, maxPerProject)
	}
	if userCount >= maxPerUser {
		return "", fmt.Errorf("open session: %w (cap %d)", ErrSessionUserBusy, maxPerUser)
	}

	ttl := openOpts.ttl
	if ttl <= 0 {
		ttl = defaultTTL
	}
	if ttl > maxTTL {
		ttl = maxTTL
	}

	s := &openSession{
		project:    project,
		mode:       mode,
		ownerUID:   id.UID,
		hasOpener:  hasOpener,
		opener:     id,
		appendOnly: true,
		lastUse:    now,
		ttl:        ttl,
	}

	if path == "" {
		s.baseSize = 0
		s.curSize = 0
	} else {
		// Network I/O outside any session-table lock.
		if err := h.pinSessionTarget(ctx, s, path, mode); err != nil {
			return "", err
		}
	}

	tmp, err := newSessionTemp()
	if err != nil {
		return "", err
	}
	s.tmp = tmp.File
	s.tmpName = tmp.Name

	if mode&SessionTruncate != 0 && !s.created {
		s.mu.Lock()
		if herr := h.hydrateSessionLocked(ctx, s); herr != nil {
			s.mu.Unlock()
			_ = tmp.File.Close()
			_ = os.Remove(tmp.Name)
			return "", herr
		}
		if terr := s.tmp.Truncate(0); terr != nil {
			s.mu.Unlock()
			_ = tmp.File.Close()
			_ = os.Remove(tmp.Name)
			return "", fmt.Errorf("truncate staged session: %w", terr)
		}
		s.curSize = 0
		s.dirty = true
		s.fullImage = true
		s.appendOnly = false
		s.mu.Unlock()
	}

	handleID, herr := newSessionID()
	if herr != nil {
		_ = tmp.File.Close()
		_ = os.Remove(tmp.Name)
		err = herr
		return "", err
	}
	s.id = handleID
	sh.mu.Lock()
	// Re-check caps under the lock: concurrent opens may have filled the
	// table during the network window above. Sweep again (cheap, skips
	// busy sessions) then admit or fail without leaking the temp.
	sh.sweepExpiredLocked(sh.now())
	projectCount, userCount = 0, 0
	for _, other := range sh.byID {
		if other.project == project {
			projectCount++
		}
		if other.ownerUID == id.UID {
			userCount++
		}
	}
	if projectCount >= sh.maxPerProject || userCount >= sh.maxPerUser {
		sh.mu.Unlock()
		_ = tmp.File.Close()
		_ = os.Remove(tmp.Name)
		if projectCount >= sh.maxPerProject {
			return "", fmt.Errorf("open session %s: %w (cap %d)", project, ErrSessionProjectBusy, sh.maxPerProject)
		}
		return "", fmt.Errorf("open session: %w (cap %d)", ErrSessionUserBusy, sh.maxPerUser)
	}
	sh.byID[handleID] = s
	sh.mu.Unlock()
	return handleID, nil
}

// pinSessionTarget validates the path shape, resolves DAC once, and pins
// the content layout for a named open. No session-table lock is held;
// the target session is not yet published so no per-session lock is
// needed either.
func (h *StorHub) pinSessionTarget(ctx context.Context, s *openSession, path string, mode OpenMode) error {
	if err := shfs.ValidateAccessPathShape(path); err != nil {
		return err
	}
	if mode&SessionCreate != 0 {
		if err := h.ensureRepo(ctx, s.project); err != nil {
			return err
		}
	}
	live, sha, err := h.loadRepoMetadataReadonly(ctx, s.project)
	if err != nil {
		return err
	}
	cleanName, traversed, err := h.resolveAuthedPath(ctx, live, path, true)
	if err != nil {
		return err
	}
	if live.HasDirectory(cleanName) {
		return shfs.IsDirectory(cleanName)
	}
	existing := live.FindFile(cleanName)
	switch {
	case existing == nil && mode&SessionCreate == 0:
		return fmt.Errorf("%w: %s", shfs.ErrNotFound, cleanName)
	case existing != nil && mode&SessionExclusive != 0 && mode&SessionCreate != 0:
		return shfs.AlreadyExists(cleanName)
	}
	if mode.readable() && existing != nil {
		if err := shfs.CheckReadAccessResolved(ctx, live, cleanName, traversed); err != nil {
			return err
		}
	}
	if mode.writable() {
		if existing != nil {
			if err := shfs.CheckWriteAccessResolved(ctx, live, cleanName, traversed); err != nil {
				return err
			}
		} else {
			if err := shfs.RequireParentDirectory(live, cleanName); err != nil {
				return err
			}
			if err := shfs.CheckParentWriteResolved(ctx, live, cleanName, traversed); err != nil {
				return err
			}
		}
	}
	s.path = cleanName
	s.revision = sha
	if existing == nil {
		s.created = true
		s.dirty = true
		s.baseSize = 0
		s.curSize = 0
		s.pinnedChunks = make(map[int64]ChunkInfo)
		return nil
	}
	pinned := existing.Clone()
	s.pinned = pinned
	s.pinnedChunks = make(map[int64]ChunkInfo, len(pinned.Chunks))
	for _, chunkID := range pinned.Chunks {
		if chunk, ok := live.Chunks()[chunkID]; ok {
			s.pinnedChunks[chunkID] = chunk
		}
	}
	s.baseSize = pinned.Size
	s.curSize = pinned.Size
	return nil
}

// sessionTempFile is one staging temp under the spool base.
type sessionTempFile struct {
	File *os.File
	Name string
}

func newSessionTemp() (*sessionTempFile, error) {
	base, err := spoolBase()
	if err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(base, "session-*")
	if err != nil {
		return nil, fmt.Errorf("create session staging temp: %w", err)
	}
	return &sessionTempFile{File: f, Name: f.Name()}, nil
}

// hydrateSessionLocked materializes the pinned snapshot into the staging
// temp on first mutation, so later reads and a full-image commit serve the
// handle's own bytes. Reads before the first write serve the pin directly
// with no download beyond what they ask for. Caller holds s.mu; the
// download runs outside the table lock so other sessions proceed.
func (h *StorHub) hydrateSessionLocked(ctx context.Context, s *openSession) error {
	if s.staged {
		return nil
	}
	if s.baseSize > 0 {
		pinned := s.pinned.Clone()
		data, err := h.ReadPinnedFileContext(ctx, s.project, &pinned, s.pinnedChunks, 0, s.baseSize)
		if err != nil {
			return fmt.Errorf("hydrate session %s: %w", shortSHA(s.id), err)
		}
		if _, err := s.tmp.WriteAt(data, 0); err != nil {
			return fmt.Errorf("hydrate session %s: %w", shortSHA(s.id), err)
		}
	}
	s.staged = true
	return nil
}

// readStagedRange reads staged bytes from the temp.
func (s *openSession) readStagedRange(start, end int64) ([]byte, error) {
	if end < start {
		return nil, fmt.Errorf("read staged session %s: inverted range [%d,%d)", shortSHA(s.id), start, end)
	}
	buf := make([]byte, end-start)
	if len(buf) == 0 {
		return buf, nil
	}
	if _, err := io.ReadFull(io.NewSectionReader(s.tmp, start, int64(len(buf))), buf); err != nil {
		return nil, fmt.Errorf("read staged session %s: %w", shortSHA(s.id), err)
	}
	return buf, nil
}
