package storage

import (
	"context"
	"crypto/subtle"
	"fmt"
	"os"
	"syscall"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// sessions_auth.go: handle ownership, expiry, and table lookup.

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
