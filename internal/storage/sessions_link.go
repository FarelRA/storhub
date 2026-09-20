package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// sessions_link.go: link, relink, and unlinked-scratch quarantine.

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
