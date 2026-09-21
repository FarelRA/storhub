package test

// session_fake.go: one shared session-table core for the fakes.
//
// Boundary: production owns session semantics (internal/storage); the
// oracle (mem_scratch.go) owns POSIX scratch semantics over test-local
// sentinels; the CLI map-store fake and the REST project-store fake own
// their file layouts, owner checks, and caps. What they must not each
// reimplement is the handle-table mechanics: open/TTL/expiry,
// unknown-is-stale lookup with lazy reap, and the identically worded
// missing-parent error. Those live here, store-agnostic, over
// caller-supplied tables and callbacks so neither fake changes its file
// layout.
//
// Deliberately NOT unified here (kept per fake, reasons inline at each
// call site): staged read/write/truncate bodies, link/relink taken-target
// checks, and linked create-semantics on sync/close. The CLI fake reports
// a directory at a link target as ErrIsDirectory and consults a links
// map; the REST fake reports it as AlreadyExists and has no links map
// (symlinks are files with a kind). The REST fake additionally enforces
// owner identity and per-project/per-user caps, and its truncate has a
// same-size no-op the CLI fake lacks. Collapsing any of these would
// change observable error values, so they stay duplicated.

import (
	"fmt"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// StaleConstructor builds the typed stale-handle error for one fake. The
// core stays leaf-only (no storage import, so no test-binary import
// cycle): each adapter passes a one-line constructor returning its own
// typed error, keeping the unknown/expired rules unified here while the
// error values keep asserting errors.Is against the storage sentinels.
type StaleConstructor func(handleID, reason string) error

// StaleUnknownHandle answers an id that was never issued. Unknown and
// expired ids are indistinguishable by design (mirroring production), so
// every operation on either reports the typed stale error.
func StaleUnknownHandle(id string, newStale StaleConstructor) error {
	return newStale(id, "unknown handle")
}

// StaleExpiredHandle answers an id whose TTL lapsed. The entry is
// already reaped by LiveSession when this is returned.
func StaleExpiredHandle(id string, newStale StaleConstructor) error {
	return newStale(id, "expired")
}

// SessionExpiryAt folds a requested TTL into [def, limit] (defaults and
// cap from the caller: 10m default for the CLI fake, the fakeSessionTTL
// knob for the REST fake, one hour cap for both) and returns the
// deadline measured from now. One implementation so the fakes agree with
// production clamping (see ClampTTL).
func SessionExpiryAt(now time.Time, requested, def, limit time.Duration) time.Time {
	return now.Add(ClampTTL(requested, def, limit))
}

// LiveSession resolves id in table with lazy expiry, the
// action-triggered sweep shared by the fakes: no goroutines, no timers.
// An unknown id answers StaleUnknownHandle; an expired entry is deleted
// under the caller lock and answers StaleExpiredHandle, so the next use
// of the same id reports unknown. Caller holds the store lock; expires
// extracts the handle deadline without exposing the handle shape, which
// keeps this core store-agnostic.
func LiveSession[S any](table map[string]*S, id string, expires func(*S) time.Time, now time.Time, newStale StaleConstructor) (*S, error) {
	s, ok := table[id]
	if !ok {
		return nil, StaleUnknownHandle(id, newStale)
	}
	if Expired(expires(s), now) {
		delete(table, id)
		return nil, StaleExpiredHandle(id, newStale)
	}
	return s, nil
}

// SweepExpiredSessions reaps every entry past its TTL. The REST fake runs
// it on open (production sweeps on open the same way); the CLI fake never
// swept and still does not. Caller holds the store lock.
func SweepExpiredSessions[S any](table map[string]*S, expires func(*S) time.Time, now time.Time) {
	for id, s := range table {
		if Expired(expires(s), now) {
			delete(table, id)
		}
	}
}

// MissingSessionParent reports a link target whose parent directory is
// absent. Both fakes word it identically (wrapped shfs.ErrNotFound);
// the oracle uses bare test.ErrNotFound and stays on its own spelling.
func MissingSessionParent(parent string) error {
	return fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
}

// PendingNames is the multi-name stage for one scratch description.
// Link appends (an already-pending name fails with ErrExists, like
// linking onto an occupied name); Relink replaces the whole set with
// one path; Close publishes to List after the caller pre-validates
// every name. The oracle uses it directly; the CLI and REST fakes wire
// it into their session tables with the hunks reported alongside this
// change, so all three share one append/replace/list semantic.
type PendingNames struct {
	names []string
}

// Add appends path to the pending set, failing with ErrExists when path
// is already pending. Absence validation against the store (taken names,
// missing parents) stays with the caller, which owns its file layout.
func (p *PendingNames) Add(path string) error {
	for _, pending := range p.names {
		if pending == path {
			return ErrExists
		}
	}
	p.names = append(p.names, path)
	return nil
}

// Replace discards the whole pending set in favor of one path, the
// rescue for a close wedged on a taken name.
func (p *PendingNames) Replace(path string) {
	p.names = []string{path}
}

// List returns the pending names in link order. The caller must
// pre-validate every name before publishing any of them.
func (p *PendingNames) List() []string {
	return p.names
}

// Empty reports whether no name is staged (close discards).
func (p *PendingNames) Empty() bool {
	return len(p.names) == 0
}
