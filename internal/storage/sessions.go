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
//   - LinkSession names an unlinked scratch handle (O_TMPFILE equivalent),
//     DAC-checked at link time like create. Linking stages the creation, so
//     link-then-close persists even an empty file.
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
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// Session capacity and lifetime policy. All bounds are documented here so a
// limit change has one edit site; per-hub overrides go through
// ConfigureSessions.
const (
	// DefaultSessionIdleTTL is the idle expiry for a handle: no successful
	// use within this window makes the next use (or the next open) reap it.
	DefaultSessionIdleTTL = 10 * time.Minute
	// DefaultSessionMaxTTL caps how large any handle TTL may grow. A larger
	// per-open request is clamped, not rejected.
	DefaultSessionMaxTTL = time.Hour
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
	// ErrSessionLinked reports linking a handle that already has a path.
	ErrSessionLinked = errors.New("storhub: session already has a path")
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
// dropped across network I/O. Fields written after insert (path on link,
// pin/revision/sizes/staging/dirty/applied/ranges/lastUse/tmp) are guarded
// by mu. project/mode/ownerUID/hasOpener/ttl are immutable after insert
// and safe to read under the table lock. destroyed marks removal from the
// table; holders of mu check it after re-acquiring the table lock.
type openSession struct {
	mu        sync.Mutex
	id        string
	project   string
	path      string
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

// expired reports whether the handle has been idle past its TTL.
func (s *openSession) expired(now time.Time) bool {
	return now.Sub(s.lastUse) > s.ttl
}

// authorize re-validates handle ownership on every operation, mirroring
// checkOverlayCaller: requests without a server-side identity (the trusted
// local process) and admin callers bypass; otherwise the caller UID must
// match the opener. Mismatches fail closed.
func (s *openSession) authorize(ctx context.Context) error {
	if !shfs.IdentityPresent(ctx) {
		return nil
	}
	id := shfs.IdentityFromContext(ctx)
	if id.Admin {
		return nil
	}
	if !s.hasOpener || id.UID == s.ownerUID {
		return nil
	}
	return fmt.Errorf("session %s: %w (owner uid %d): %w", shortSHA(s.id), ErrSessionOwnerMismatch, s.ownerUID, syscall.EPERM)
}

// constantTimeIDEqual compares a presented handle id in constant time. The
// table lookup already selected the candidate; this keeps the acceptance
// itself free of early-exit byte comparison.
func constantTimeIDEqual(stored, presented string) bool {
	a, b := []byte(stored), []byte(presented)
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare(a, b) == 1
}

// getLiveLocked resolves a handle id to its live session, sweeping it when
// expired. Unknown, mismatched, and expired ids answer StaleSessionError.
// Caller holds sh.mu. On success the session mu is held (order sh then s);
// the caller must Unlock the session and must not take sh.mu while holding
// it (release s.mu first, then re-acquire in sh-then-s order).
func (sh *sessionHubState) getLiveLocked(handleID string, now time.Time) (*openSession, error) {
	s, ok := sh.byID[handleID]
	if !ok || !constantTimeIDEqual(s.id, handleID) {
		return nil, newStaleSessionError(handleID, "unknown handle")
	}
	s.mu.Lock()
	if s.destroyed {
		s.mu.Unlock()
		return nil, newStaleSessionError(handleID, "unknown handle")
	}
	if s.expired(now) {
		sh.destroyLocked(s, true)
		s.mu.Unlock()
		return nil, newStaleSessionError(handleID, "expired")
	}
	return s, nil
}

// destroyLocked closes the staging temp and forgets the handle.
// quarantine moves an unlinked scratch temp aside for manual recovery
// instead of deleting it; named-session temps are always removed.
// Caller holds sh.mu and s.mu (order sh then s).
func (sh *sessionHubState) destroyLocked(s *openSession, quarantine bool) {
	s.destroyed = true
	if s.tmp != nil {
		_ = s.tmp.Close()
		s.tmp = nil
	}
	if quarantine && s.path == "" && stagingHasBytes(s.tmpName) {
		if quarantineSessionTemp(s.tmpName, s.id) == nil {
			delete(sh.byID, s.id)
			return
		}
	}
	_ = os.Remove(s.tmpName)
	delete(sh.byID, s.id)
}

// sweepExpiredLocked reaps idle handles. Runs on every open (no background
// goroutine by design). Caller holds sh.mu. Busy sessions (TryLock fails)
// are skipped: the holder bumps lastUse on completion, so skipping never
// leaks an idle handle and never stalls the table behind a slow commit.
func (sh *sessionHubState) sweepExpiredLocked(now time.Time) {
	for _, s := range sh.byID {
		if !s.mu.TryLock() {
			continue
		}
		if s.destroyed {
			s.mu.Unlock()
			continue
		}
		if s.expired(now) {
			sh.destroyLocked(s, true)
			// destroyLocked leaves s.mu held; release for next entry.
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()
	}
}

// stagingHasBytes reports whether a staging temp holds anything worth
// quarantining.
func stagingHasBytes(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	return fi.Size() > 0
}

// quarantineSessionTemp moves a temp aside under the spool base for manual
// recovery. It never re-drives the bytes anywhere.
func quarantineSessionTemp(name, id string) error {
	base, err := spoolBase()
	if err != nil {
		return err
	}
	qdir := filepath.Join(base, "session-quarantine")
	if err := os.MkdirAll(qdir, 0o755); err != nil {
		return fmt.Errorf("create session quarantine dir: %w", err)
	}
	dest := filepath.Join(qdir, "session-"+shortSHA(id)+".staged")
	if err := os.Rename(name, dest); err != nil {
		return fmt.Errorf("quarantine session temp: %w", err)
	}
	return nil
}

// QuarantineStaleSessionTemps collects session staging temps older than
// maxAge into the spool quarantine dir for manual recovery, never
// auto-redriven. Call it at startup after a restart: live handles are
// younger than their idle TTL, so with maxAge at or above the max TTL only
// orphaned temps move. Returns how many files were quarantined.
func (h *StorHub) QuarantineStaleSessionTemps(maxAge time.Duration) (int, error) {
	base, err := spoolBase()
	if err != nil {
		return 0, err
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return 0, fmt.Errorf("list spool base: %w", err)
	}
	cutoff := time.Now().Add(-maxAge)
	moved := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) < 8 || name[:8] != "session-" {
			continue
		}
		full := filepath.Join(base, name)
		fi, err := os.Stat(full)
		if err != nil {
			continue
		}
		if fi.ModTime().After(cutoff) {
			continue
		}
		if err := quarantineSessionTemp(full, name); err != nil {
			continue
		}
		moved++
	}
	return moved, nil
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
func (h *StorHub) OpenSession(ctx context.Context, project, path string, mode OpenMode, opts ...SessionOption) (string, error) {
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

	handleID, err := newSessionID()
	if err != nil {
		_ = tmp.File.Close()
		_ = os.Remove(tmp.Name)
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

// ReadSession serves [offset, offset+length) from the pinned snapshot plus
// the handle's own staged writes. Reads at or past EOF return zero bytes
// with a nil error. Table lock covers lookup only; the pinned download
// runs under the per-session lock so a slow read never stalls other
// sessions.
func (h *StorHub) ReadSession(ctx context.Context, handleID string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("read session: offset and length must be non-negative")
	}
	sh := h.sessionHub()
	sh.mu.Lock()
	s, err := sh.getLiveLocked(handleID, sh.now())
	if err != nil {
		sh.mu.Unlock()
		return nil, err
	}
	sh.mu.Unlock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if !s.mode.readable() {
		return nil, fmt.Errorf("read session %s: handle not open for reading: %w", shortSHA(s.id), syscall.EBADF)
	}
	if length == 0 || offset >= s.curSize {
		s.lastUse = sh.now()
		return []byte{}, nil
	}
	end := offset + length
	if end < offset || end > s.curSize {
		end = s.curSize
	}
	var out []byte
	if s.staged {
		out, err = s.readStagedRange(offset, end)
		if err != nil {
			return nil, err
		}
	} else {
		pinned := s.pinned.Clone()
		pinnedChunks := s.pinnedChunks
		project := s.project
		// Release per-session lock across network? No: same-handle
		// serialization requires holding s.mu, but other sessions hold
		// different s.mu so they proceed. Table lock is already dropped.
		out, err = h.ReadPinnedFileContext(ctx, project, &pinned, pinnedChunks, offset, end-offset)
		if err != nil {
			return nil, err
		}
	}
	s.lastUse = sh.now()
	return out, nil
}

// WriteSession stages data at offset (or at the current end when opened
// with SessionAppend). Holes zero-fill. Staging is local only: no network,
// no journal, invisible to everyone else until Sync or Close. Table lock
// covers lookup only; hydrate plus staging run under the per-session lock.
func (h *StorHub) WriteSession(ctx context.Context, handleID string, offset int64, data []byte) (int, error) {
	sh := h.sessionHub()
	sh.mu.Lock()
	s, err := sh.getLiveLocked(handleID, sh.now())
	if err != nil {
		sh.mu.Unlock()
		return 0, err
	}
	sh.mu.Unlock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx); err != nil {
		return 0, err
	}
	if !s.mode.writable() {
		return 0, fmt.Errorf("write session %s: handle not open for writing: %w", shortSHA(s.id), syscall.EBADF)
	}
	if len(data) == 0 {
		s.lastUse = sh.now()
		return 0, nil
	}
	if s.mode&SessionAppend != 0 {
		offset = s.curSize
	}
	if offset < 0 {
		return 0, fmt.Errorf("write session: offset must be non-negative")
	}
	if err := h.hydrateSessionLocked(ctx, s); err != nil {
		return 0, err
	}
	curBefore := s.curSize
	if offset > s.curSize {
		zeros := make([]byte, offset-s.curSize)
		if _, err := s.tmp.WriteAt(zeros, s.curSize); err != nil {
			return 0, fmt.Errorf("write session %s: %w", shortSHA(s.id), err)
		}
		s.ranges = mergeByteRange(s.ranges, byteRange{start: s.curSize, end: offset})
		s.curSize = offset
		s.fullImage = true
		s.appendOnly = false
	}
	wrote := 0
	for wrote < len(data) {
		n, err := s.tmp.WriteAt(data[wrote:], offset+int64(wrote))
		if err != nil {
			return wrote, fmt.Errorf("write session %s: %w", shortSHA(s.id), err)
		}
		if n == 0 {
			return wrote, fmt.Errorf("write session %s: no progress", shortSHA(s.id))
		}
		wrote += n
	}
	if offset != curBefore {
		s.appendOnly = false
	}
	s.ranges = mergeByteRange(s.ranges, byteRange{start: offset, end: offset + int64(wrote)})
	if end := offset + int64(wrote); end > s.curSize {
		s.curSize = end
	}
	s.dirty = true
	s.applied = false
	s.lastUse = sh.now()
	return wrote, nil
}

// TruncateSession stages a resize. Shrinks and grows both commit as a full
// image; a no-op size is a no-op success. Table lock covers lookup only.
func (h *StorHub) TruncateSession(ctx context.Context, handleID string, size int64) error {
	if size < 0 {
		return fmt.Errorf("truncate session: size must be non-negative")
	}
	sh := h.sessionHub()
	sh.mu.Lock()
	s, err := sh.getLiveLocked(handleID, sh.now())
	if err != nil {
		sh.mu.Unlock()
		return err
	}
	sh.mu.Unlock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx); err != nil {
		return err
	}
	if !s.mode.writable() {
		return fmt.Errorf("truncate session %s: handle not open for writing: %w", shortSHA(s.id), syscall.EBADF)
	}
	if size == s.curSize {
		s.lastUse = sh.now()
		return nil
	}
	if err := h.hydrateSessionLocked(ctx, s); err != nil {
		return err
	}
	if err := s.tmp.Truncate(size); err != nil {
		return fmt.Errorf("truncate session %s: %w", shortSHA(s.id), err)
	}
	s.curSize = size
	s.dirty = true
	s.applied = false
	s.fullImage = true
	s.appendOnly = false
	s.lastUse = sh.now()
	return nil
}

// StatSession reports the handle's project, path, current size, dirty
// state, mode, and staleness against the current committed revision.
// Table lock covers lookup only; the cached revision read runs under the
// per-session lock.
func (h *StorHub) StatSession(ctx context.Context, handleID string) (SessionStat, error) {
	sh := h.sessionHub()
	sh.mu.Lock()
	s, err := sh.getLiveLocked(handleID, sh.now())
	if err != nil {
		sh.mu.Unlock()
		return SessionStat{}, err
	}
	sh.mu.Unlock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx); err != nil {
		return SessionStat{}, err
	}
	s.lastUse = sh.now()
	stat := SessionStat{
		Project: s.project,
		Path:    s.path,
		Size:    s.curSize,
		Dirty:   s.dirty,
		Mode:    s.mode,
	}
	// Staleness is a cached readonly comparison only: the resident entry's
	// committed SHA against the open-time pin. It takes metaMu then pm.mu
	// for reading, matching every other reader, and never triggers a
	// remote load (a stat that performs network I/O could fail a pure
	// local query on a backend outage). No session-table lock is held
	// here, only the per-session lock, so stats never stall commits.
	if _, curSHA, ok := h.cachedRepoMetadataReadonly(s.project); ok {
		stat.Stale = s.revision != curSHA
	}
	return stat, nil
}

// resolveLinkTarget validates a link/relink target without mutating:
// shape, resolve, walk, parent presence, parent write, kind conflicts,
// and target absence. Shared by LinkSession (unlinked handles only) and
// RelinkSession (rescues a handle whose target was taken by a concurrent
// writer). Caller holds s.mu; metadata reads run under it like the rest
// of the session slow path.
func (h *StorHub) resolveLinkTarget(ctx context.Context, s *openSession, path string) (string, error) {
	if err := shfs.ValidateAccessPathShape(path); err != nil {
		return "", err
	}
	if err := h.ensureRepo(ctx, s.project); err != nil {
		return "", err
	}
	live, _, err := h.loadRepoMetadataReadonly(ctx, s.project)
	if err != nil {
		return "", err
	}
	cleanName, traversed, err := shfs.ResolveAccessPath(live, path, false)
	if err != nil {
		return "", err
	}
	if cleanName == "" {
		return "", fmt.Errorf("link session: path is required")
	}
	if err := shfs.CheckWalkResolved(ctx, live, traversed); err != nil {
		return "", err
	}
	if err := shfs.RequireParentDirectory(live, cleanName); err != nil {
		return "", err
	}
	if err := shfs.CheckParentWriteResolved(ctx, live, cleanName, traversed); err != nil {
		return "", err
	}
	if live.HasDirectory(cleanName) {
		return "", shfs.IsDirectory(cleanName)
	}
	if live.FindFile(cleanName) != nil {
		return "", shfs.AlreadyExists(cleanName)
	}
	return cleanName, nil
}

// LinkSession names an unlinked scratch handle, DAC-checked at link time
// like create (parent must exist, parent-write required, target must not
// exist). Linking stages the creation, so link-then-close persists.
// Table lock covers lookup only; the metadata checks run under the
// per-session lock.
func (h *StorHub) LinkSession(ctx context.Context, handleID, path string) error {
	sh := h.sessionHub()
	sh.mu.Lock()
	s, err := sh.getLiveLocked(handleID, sh.now())
	if err != nil {
		sh.mu.Unlock()
		return err
	}
	sh.mu.Unlock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx); err != nil {
		return err
	}
	if s.path != "" {
		return fmt.Errorf("link session %s to %s: %w", shortSHA(s.id), path, ErrSessionLinked)
	}
	cleanName, err := h.resolveLinkTarget(ctx, s, path)
	if err != nil {
		return err
	}
	s.path = cleanName
	s.created = true
	s.dirty = true
	s.lastUse = sh.now()
	return nil
}

// RelinkSession retargets a handle to a new path: the rescue for a
// commit that failed with AlreadyExists because a concurrent writer
// took the linked target. Without it the handle is wedged (close fails
// on the taken target, link refuses the named handle) until TTL expiry.
// Same checks as LinkSession; the new target must be absent. Marks the
// handle dirty so the staged bytes commit at the new path on close.
func (h *StorHub) RelinkSession(ctx context.Context, handleID, path string) error {
	sh := h.sessionHub()
	sh.mu.Lock()
	s, err := sh.getLiveLocked(handleID, sh.now())
	if err != nil {
		sh.mu.Unlock()
		return err
	}
	sh.mu.Unlock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx); err != nil {
		return err
	}
	cleanName, err := h.resolveLinkTarget(ctx, s, path)
	if err != nil {
		return err
	}
	s.path = cleanName
	s.created = true
	s.dirty = true
	s.applied = false
	s.lastUse = sh.now()
	return nil
}

// commitSessionLocked commits one handle's staged state through the standard
// hub verbs, chosen per staged shape, then drains the commit it made:
//
//   - staged creation (missing at open, or linked scratch): full upload of
//     the staged image via UploadFileContext;
//   - full-image stages (truncate, holes): full replace via
//     ReplaceFileFromReaderContext with the staged image;
//   - pure appends: the appended tail via AppendFileContext;
//   - range overwrites: one batched range patch via PatchFileRangesContext.
//
// The verb phase runs exactly once per staged generation: it mutates local
// metadata immediately, so a retry after a failed drain must NOT re-run the
// verbs (that would apply the same bytes twice) and only re-drains. Any new
// staged write clears the applied marker. The per-hub commit mutex makes
// the verb sequence plus drain exclusive across sessions. DAC is
// re-validated against live state and fails loud.
// Caller holds s.mu (per-session); the table lock is never held across the
// verb plus drain network window, so one slow commit never stalls other
// sessions. Same-handle exclusion comes from s.mu, cross-session commit
// exclusion from commitMu (order s.mu then commitMu; hub verbs underneath
// take pm.mu, never session locks, so no cycle).
func (h *StorHub) commitSessionLocked(ctx context.Context, sh *sessionHubState, s *openSession) error {
	if !s.dirty {
		return nil
	}
	if s.path == "" {
		return fmt.Errorf("commit session %s: %w", shortSHA(s.id), ErrSessionUnlinked)
	}
	// Commit as the opener (see the opener field): ownership and
	// privilege decisions follow whoever staged the bytes. The closer's
	// identity mattered only at authorize time (opener or admin may
	// trigger). DAC is rechecked below against live state, so a revoked
	// opener still fails loud instead of publishing.
	commitCtx := ctx
	if s.hasOpener {
		commitCtx = shfs.WithIdentity(ctx, s.opener)
	}
	if !s.staged && s.baseSize > 0 {
		if err := h.hydrateSessionLocked(commitCtx, s); err != nil {
			return err
		}
	}

	sh.commitMu.Lock()
	defer sh.commitMu.Unlock()

	if err := h.recheckSessionDAC(commitCtx, s); err != nil {
		return err
	}

	if !s.applied {
		var err error
		switch {
		case s.created:
			_, err = h.UploadFileContext(commitCtx, s.project, s.path, s.tmpName)
		case s.fullImage:
			var staged *os.File
			staged, err = os.Open(s.tmpName)
			if err == nil {
				defer func() { _ = staged.Close() }()
				_, err = h.ReplaceFileFromReaderContext(commitCtx, s.project, s.path, staged, shfs.WithSize(s.curSize))
			}
			if err != nil {
				err = fmt.Errorf("commit session %s: %w", shortSHA(s.id), err)
			}
		case s.appendOnly && s.curSize > s.baseSize:
			var tail []byte
			tail, err = s.readStagedRange(s.baseSize, s.curSize)
			if err == nil {
				_, err = h.AppendFileContext(commitCtx, s.project, s.path, tail)
			}
		default:
			var edits []shfs.RangeEdit
			edits, err = h.sessionRangeEdits(s)
			if err == nil {
				if len(edits) == 0 {
					return fmt.Errorf("commit session %s: staged state with no dirty ranges", shortSHA(s.id))
				}
				_, err = h.PatchFileRangesContext(commitCtx, s.project, s.path, edits)
			}
		}
		if err != nil {
			return err
		}
		s.applied = true
	}
	if err := h.DrainProjectContext(commitCtx, s.project); err != nil {
		return err
	}
	return nil
}

// recheckSessionDAC re-validates write permission against live state before
// committing: the open-time check cannot see permission changes that landed
// while the handle was open. Failures fail loud and retain staged state.
func (h *StorHub) recheckSessionDAC(ctx context.Context, s *openSession) error {
	live, _, err := h.loadRepoMetadataReadonly(ctx, s.project)
	if err != nil {
		return err
	}
	cleanName, traversed, err := h.resolveAuthedPath(ctx, live, s.path, true)
	if err != nil {
		return err
	}
	if live.FindFile(cleanName) != nil {
		if err := shfs.CheckWriteAccessResolved(ctx, live, cleanName, traversed); err != nil {
			return err
		}
		return nil
	}
	if err := shfs.RequireParentDirectory(live, cleanName); err != nil {
		return err
	}
	return shfs.CheckParentWriteResolved(ctx, live, cleanName, traversed)
}

// sessionRangeEdits renders merged dirty ranges as one ascending batch of
// range edits against pinned coordinates: the overlapped prefix replaces
// old bytes, the extended suffix is pure insert.
func (h *StorHub) sessionRangeEdits(s *openSession) ([]shfs.RangeEdit, error) {
	edits := make([]shfs.RangeEdit, 0, len(s.ranges))
	for _, r := range s.ranges {
		data, err := s.readStagedRange(r.start, r.end)
		if err != nil {
			return nil, err
		}
		deleteSize := int64(0)
		if r.start < s.baseSize {
			deleteSize = r.end - r.start
			if r.end > s.baseSize {
				deleteSize = s.baseSize - r.start
			}
		}
		edits = append(edits, shfs.RangeEdit{Start: r.start, DeleteSize: deleteSize, Data: data})
	}
	return edits, nil
}

// repinSessionLocked refreshes the pin to the just-committed live state and
// clears staging. Caller holds s.mu; the metadata load runs outside the
// table lock.
func (h *StorHub) repinSessionLocked(ctx context.Context, s *openSession) error {
	live, sha, err := h.loadRepoMetadataReadonly(ctx, s.project)
	if err != nil {
		return err
	}
	entry := live.FindFile(s.path)
	if entry == nil {
		return fmt.Errorf("repin session %s: %w: %s", shortSHA(s.id), shfs.ErrNotFound, s.path)
	}
	pinned := entry.Clone()
	chunks := make(map[int64]ChunkInfo, len(pinned.Chunks))
	for _, chunkID := range pinned.Chunks {
		if chunk, ok := live.Chunks()[chunkID]; ok {
			chunks[chunkID] = chunk
		}
	}
	s.pinned = pinned
	s.pinnedChunks = chunks
	s.revision = sha
	s.baseSize = pinned.Size
	s.curSize = pinned.Size
	s.staged = false
	s.dirty = false
	s.applied = false
	s.created = false
	s.fullImage = false
	s.appendOnly = true
	s.ranges = nil
	if s.tmp != nil {
		if err := s.tmp.Truncate(0); err != nil {
			return fmt.Errorf("repin session %s: %w", shortSHA(s.id), err)
		}
	}
	return nil
}

// SyncSession commits staged state without closing, then re-pins to the
// committed state. It drains the commit it makes and fails loud, retaining
// staged state for retry. Sync with no staged state is a no-op success.
// Table lock covers lookup only; commit plus repin run under the
// per-session lock.
func (h *StorHub) SyncSession(ctx context.Context, handleID string) error {
	sh := h.sessionHub()
	sh.mu.Lock()
	s, err := sh.getLiveLocked(handleID, sh.now())
	if err != nil {
		sh.mu.Unlock()
		return err
	}
	sh.mu.Unlock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx); err != nil {
		return err
	}
	if !s.dirty {
		s.lastUse = sh.now()
		return nil
	}
	if err := h.commitSessionLocked(ctx, sh, s); err != nil {
		return err
	}
	if err := h.repinSessionLocked(ctx, s); err != nil {
		return err
	}
	s.lastUse = sh.now()
	return nil
}

// CloseSession commits staged state through the standard hub verbs inside
// one exclusive transaction, then publishes once (the commit drains before
// the handle is destroyed, so close means durable). Close with no staged
// state is a no-op success. Close on unlinked scratch without a prior link
// discards the temp with no commit. A failed commit retains the handle and
// its staged state for retry. Table lock covers lookup and final destroy
// only; the commit runs under the per-session lock.
func (h *StorHub) CloseSession(ctx context.Context, handleID string) error {
	sh := h.sessionHub()
	sh.mu.Lock()
	s, err := sh.getLiveLocked(handleID, sh.now())
	if err != nil {
		sh.mu.Unlock()
		return err
	}
	sh.mu.Unlock()
	// getLiveLocked returns with the per-session lock held across the
	// commit so two closes of the same ID still serialize; distinct
	// handles hold different locks.
	if err := s.authorize(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	if s.destroyed {
		s.mu.Unlock()
		return newStaleSessionError(handleID, "unknown handle")
	}
	if s.path == "" || !s.dirty {
		// Destroy needs the table lock in sh-then-s order: release s.mu
		// first, then re-acquire both and re-validate.
		s.mu.Unlock()
		sh.mu.Lock()
		victim, verr := sh.getLiveLocked(handleID, sh.now())
		if verr != nil {
			sh.mu.Unlock()
			return verr
		}
		// getLiveLocked holds s.mu and sh.mu; destroy then release both.
		sh.destroyLocked(victim, false)
		victim.mu.Unlock()
		sh.mu.Unlock()
		return nil
	}
	if err := h.commitSessionLocked(ctx, sh, s); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	sh.mu.Lock()
	victim, verr := sh.getLiveLocked(handleID, sh.now())
	if verr != nil {
		sh.mu.Unlock()
		return verr
	}
	sh.destroyLocked(victim, false)
	victim.mu.Unlock()
	sh.mu.Unlock()
	return nil
}
