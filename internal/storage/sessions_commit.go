package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
)

// sessions_commit.go: session commit path: resolve, commit, recheck, sync, close.

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
// resolveCommitPathLocked maps a dirty handle to the path its staged state
// must publish to. The open path wins while it still names the pinned
// inode; after a rename the first surviving name (sorted, deterministic)
// wins, so close follows the rename instead of resurrecting the old name;
// when the inode lost its last name after open it returns "" and the
// caller maps that to discard (close) or retain (sync). Created handles
// keep the open path: a concurrent creator wins or loses through the
// normal create verb, exactly as before. Callers must hold s.mu; the live
// tree read is the same snapshot the verbs will contend with.
func (h *StorHub) resolveCommitPathLocked(ctx context.Context, s *openSession) (string, error) {
	if s.created {
		return s.path, nil
	}
	live, _, err := h.loadRepoMetadataReadonly(ctx, s.project)
	if err != nil {
		return "", err
	}
	paths := live.FindFilesByInode(s.pinned.Inode)
	for _, p := range paths {
		if p == s.path {
			return s.path, nil
		}
	}
	if len(paths) == 0 {
		return "", nil
	}
	sort.Strings(paths)
	return paths[0], nil
}

// resolveCommitPathsLocked maps a dirty handle to every path its staged
// state must publish to, in publish order. Linked handles (a non-empty
// pending set from Link/Relink) publish the staged bytes to each pending
// name; opened-with-path handles keep the single-path replace resolution
// below. Callers must hold s.mu.
func (h *StorHub) resolveCommitPathsLocked(ctx context.Context, s *openSession) ([]string, error) {
	if len(s.pending) > 0 {
		return append([]string(nil), s.pending...), nil
	}
	single, err := h.resolveCommitPathLocked(ctx, s)
	if err != nil || single == "" {
		return nil, err
	}
	return []string{single}, nil
}

// commitSessionLocked commits one handle's staged state: linked handles
// (a non-empty pending set) fan out through commitLinkedSessionLocked,
// every other handle commits its single resolved path below. It returns
// the path the staged state was published to (the resolved commit path,
// or the first pending name for linked handles): the caller repins to
// exactly that path so a commit that followed a rename does not repin to
// the stale open-time path.
func (h *StorHub) commitSessionLocked(ctx context.Context, sh *sessionHubState, s *openSession) (string, error) {
	if !s.dirty {
		return "", nil
	}
	if len(s.pending) > 0 {
		return h.commitLinkedSessionLocked(ctx, sh, s)
	}
	if s.path == "" {
		return "", fmt.Errorf("commit session %s: %w", shortSHA(s.id), ErrSessionUnlinked)
	}
	// Map the handle to the path its staged state must publish to: the
	// open path while it still names the pinned inode, a surviving name
	// after a rename, or gone when the inode was unlinked after open.
	commitPath, err := h.resolveCommitPathLocked(ctx, s)
	if err != nil {
		return "", err
	}
	if commitPath == "" {
		return "", fmt.Errorf("commit session %s: %w", shortSHA(s.id), ErrSessionPathGone)
	}
	if commitPath != s.path {
		logging.Warn(h.projectLogger(s.project), "session-commit followed rename", "handle", shortSHA(s.id), "project", s.project, "path", s.path, "commit_path", commitPath)
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
			return "", err
		}
	}

	sh.commitMu.Lock()
	defer sh.commitMu.Unlock()

	if err := h.recheckSessionDAC(commitCtx, s, commitPath); err != nil {
		return "", err
	}

	if !s.applied {
		var err error
		switch {
		case s.created:
			_, err = h.UploadFileContext(commitCtx, s.project, commitPath, s.tmpName)
		case s.fullImage:
			var staged *os.File
			staged, err = os.Open(s.tmpName)
			if err == nil {
				defer func() { _ = staged.Close() }()
				_, err = h.ReplaceFileFromReaderContext(commitCtx, s.project, commitPath, staged, shfs.WithSize(s.curSize))
			}
			if err != nil {
				err = fmt.Errorf("commit session %s: %w", shortSHA(s.id), err)
			}
		case s.appendOnly && s.curSize > s.baseSize:
			var tail []byte
			tail, err = s.readStagedRange(s.baseSize, s.curSize)
			if err == nil {
				_, err = h.AppendFileContext(commitCtx, s.project, commitPath, tail)
			}
		default:
			var edits []shfs.RangeEdit
			edits, err = h.sessionRangeEdits(s)
			if err == nil {
				if len(edits) == 0 {
					return "", fmt.Errorf("commit session %s: staged state with no dirty ranges", shortSHA(s.id))
				}
				_, err = h.PatchFileRangesContext(commitCtx, s.project, commitPath, edits)
			}
		}
		if err != nil {
			return "", err
		}
		s.applied = true
	}
	if err := h.DrainProjectContext(commitCtx, s.project); err != nil {
		return "", err
	}
	return commitPath, nil
}

// commitLinkedSessionLocked publishes one linked handle's staged bytes to
// every pending name in link order, then drains once. A linked creation
// pre-validates ALL pending names for absence before publishing anything:
// any name taken by another writer fails the whole operation with
// AlreadyExists, publishing nothing, and the handle stays open for Relink.
// Names this handle already published in an earlier partial attempt (a
// previous call that failed mid-fan-out with applied still false) skip the
// absence check and republish idempotently, so the retry completes the
// remaining names instead of wedging on its own prior publish. A linked
// handle that already published once (created cleared by repin on Sync)
// republishes its full staged image per name with replace-or-create
// semantics, so a second Sync after more writes keeps working. DAC is
// re-validated per name against live state. Like the single-path commit,
// the verb phase runs once per staged generation (applied marker) and a
// failed drain retains staged state for retry. Caller holds s.mu.
func (h *StorHub) commitLinkedSessionLocked(ctx context.Context, sh *sessionHubState, s *openSession) (string, error) {
	pending, err := h.resolveCommitPathsLocked(ctx, s)
	if err != nil {
		return "", err
	}
	if len(pending) == 0 {
		return "", fmt.Errorf("commit session %s: %w", shortSHA(s.id), ErrSessionUnlinked)
	}
	// Commit as the opener, like the single-path commit.
	commitCtx := ctx
	if s.hasOpener {
		commitCtx = shfs.WithIdentity(ctx, s.opener)
	}
	if !s.staged && s.baseSize > 0 {
		if err := h.hydrateSessionLocked(commitCtx, s); err != nil {
			return "", err
		}
	}

	sh.commitMu.Lock()
	defer sh.commitMu.Unlock()

	live, _, err := h.loadRepoMetadataReadonly(commitCtx, s.project)
	if err != nil {
		return "", err
	}
	if s.created {
		for _, name := range pending {
			if s.published[name] {
				continue
			}
			if live.FindFile(name) != nil {
				return "", shfs.AlreadyExists(name)
			}
		}
	}
	for _, name := range pending {
		if err := h.recheckSessionDAC(commitCtx, s, name); err != nil {
			return "", err
		}
	}

	if !s.applied {
		if s.created {
			for _, name := range pending {
				if s.published[name] {
					// Own prior publish from a failed attempt: the
					// staged bytes are unchanged, so republish
					// idempotently instead of re-creating.
					if err := h.publishSessionImageLocked(commitCtx, s, name, live); err != nil {
						return "", err
					}
					continue
				}
				if _, err := h.UploadFileContext(commitCtx, s.project, name, s.tmpName); err != nil {
					return "", err
				}
				if s.published == nil {
					s.published = make(map[string]bool, len(pending))
				}
				s.published[name] = true
			}
		} else {
			for _, name := range pending {
				if err := h.publishSessionImageLocked(commitCtx, s, name, live); err != nil {
					return "", err
				}
			}
		}
		s.applied = true
	}
	if err := h.DrainProjectContext(commitCtx, s.project); err != nil {
		return "", err
	}
	return pending[0], nil
}

// publishSessionImageLocked publishes the full staged image of a linked
// handle that already owns its names (a post-Sync update) to one name:
// replace when the name exists, create when it does not. live is the
// pre-publish snapshot read under commitMu; the verbs re-check existence
// themselves, so a name created concurrently still lands exactly once.
func (h *StorHub) publishSessionImageLocked(ctx context.Context, s *openSession, name string, live *RepoMetadata) error {
	if live.FindFile(name) != nil {
		staged, err := os.Open(s.tmpName)
		if err != nil {
			return fmt.Errorf("commit session %s: %w", shortSHA(s.id), err)
		}
		defer func() { _ = staged.Close() }()
		if _, err := h.ReplaceFileFromReaderContext(ctx, s.project, name, staged, shfs.WithSize(s.curSize)); err != nil {
			return fmt.Errorf("commit session %s: %w", shortSHA(s.id), err)
		}
		return nil
	}
	_, err := h.UploadFileContext(ctx, s.project, name, s.tmpName)
	return err
}

// recheckSessionDAC re-validates write permission against live state before
// committing: the open-time check cannot see permission changes that landed
// while the handle was open. commitPath is the resolved publish target
// (open path, or a surviving name after a rename), so the check follows
// renames instead of authorizing against a stale name. Failures fail loud
// and retain staged state.
func (h *StorHub) recheckSessionDAC(ctx context.Context, s *openSession, commitPath string) error {
	live, _, err := h.loadRepoMetadataReadonly(ctx, s.project)
	if err != nil {
		return err
	}
	cleanName, traversed, err := h.resolveAuthedPath(ctx, live, commitPath, true)
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
// clears staging. commitPath is the path the staged state was actually
// published to (the resolved commit path, which follows a rename): the pin
// moves there and s.path tracks it, so a Sync that followed a rename does
// not repin to the stale open-time path and report failure for a durable
// commit. Caller holds s.mu; the metadata load runs outside the table lock.
func (h *StorHub) repinSessionLocked(ctx context.Context, s *openSession, commitPath string) error {
	if commitPath == "" {
		commitPath = s.path
	}
	live, sha, err := h.loadRepoMetadataReadonly(ctx, s.project)
	if err != nil {
		return err
	}
	entry := live.FindFile(commitPath)
	if entry == nil {
		return fmt.Errorf("repin session %s: %w: %s", shortSHA(s.id), shfs.ErrNotFound, commitPath)
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
	s.path = commitPath
	s.baseSize = pinned.Size
	s.curSize = pinned.Size
	s.staged = false
	s.dirty = false
	s.applied = false
	s.created = false
	s.published = nil
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

// destroySession forgets one handle from the table. The caller holds s.mu
// and must not hold sh.mu (lock order sh-then-s: s.mu is released first,
// then both are re-acquired). The re-validation is by pointer, never by
// re-lookup alone: a concurrent sweep may have reaped this handle between
// the unlock and the re-lock (its lastUse predates a slow commit), and
// reporting Stale for already-durable state would be wrong. When the table
// no longer holds the id but the missing handle is this session (reaped
// after its commit already succeeded and drained), the close is already
// durable and destroy reports success. A different handle under this id is
// impossible (128-bit ids), but a handle that is not this session is never
// destroyed.
func (sh *sessionHubState) destroySession(handleID string, s *openSession) error {
	s.mu.Unlock()
	sh.mu.Lock()
	victim, verr := sh.getLiveLocked(handleID, sh.now())
	if verr != nil {
		sh.mu.Unlock()
		s.mu.Lock()
		reaped := s.destroyed
		s.mu.Unlock()
		if reaped {
			return nil
		}
		return verr
	}
	// getLiveLocked holds s.mu and sh.mu; destroy then release both.
	if victim != s {
		victim.mu.Unlock()
		sh.mu.Unlock()
		return newStaleSessionError(handleID, "unknown handle")
	}
	sh.destroyLocked(victim, false)
	victim.mu.Unlock()
	sh.mu.Unlock()
	return nil
}

// SyncSession commits staged state without closing, then re-pins to the
// committed state. It drains the commit it makes and fails loud, retaining
// staged state for retry. Sync with no staged state is a no-op success.
// Table lock covers lookup only; commit plus repin run under the
// per-session lock.
func (h *StorHub) SyncSession(ctx context.Context, handleID string) (err error) {
	sh := h.sessionHub()
	sh.mu.Lock()
	s, lerr := sh.getLiveLocked(handleID, sh.now())
	if lerr != nil {
		sh.mu.Unlock()
		logging.Error(h.logger, "session-sync lookup failed", "handle", shortSHA(handleID), "op", "sync", "err", lerr)
		return lerr
	}
	sh.mu.Unlock()
	defer s.mu.Unlock()
	if aerr := s.authorize(ctx); aerr != nil {
		logging.Error(h.projectLogger(s.project), "session auth failed", "handle", shortSHA(s.id), "project", s.project, "path", s.path, "op", "sync", "err", aerr)
		return aerr
	}
	started := h.config.Now().UTC()
	logging.Debug(h.projectLogger(s.project), "session-sync start", "handle", shortSHA(s.id), "project", s.project, "path", s.path)
	defer func() {
		elapsed := h.config.Now().UTC().Sub(started)
		if err != nil {
			logging.Error(h.projectLogger(s.project), "session-sync failed", "handle", shortSHA(s.id), "project", s.project, "path", s.path, "elapsed", elapsed, "err", err)
			return
		}
		logging.Debug(h.projectLogger(s.project), "session-sync complete", "handle", shortSHA(s.id), "project", s.project, "path", s.path, "elapsed", elapsed)
	}()
	if !s.dirty {
		s.lastUse = sh.now()
		return nil
	}
	commitPath, cerr := h.commitSessionLocked(ctx, sh, s)
	if cerr != nil {
		if errors.Is(cerr, ErrSessionPathGone) {
			// Pinned inode unlinked after open: the fsync
			// equivalent succeeds with nothing to publish, and
			// the staged bytes stay readable until close. There
			// is no live entry to repin to, so keep the pin.
			logging.Warn(h.projectLogger(s.project), "session-sync on unlinked path; retaining staged state", "handle", shortSHA(s.id), "project", s.project, "path", s.path)
			s.lastUse = sh.now()
			return nil
		}
		return cerr
	}
	if rerr := h.repinSessionLocked(ctx, s, commitPath); rerr != nil {
		return rerr
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
func (h *StorHub) CloseSession(ctx context.Context, handleID string) (err error) {
	sh := h.sessionHub()
	sh.mu.Lock()
	s, lerr := sh.getLiveLocked(handleID, sh.now())
	if lerr != nil {
		sh.mu.Unlock()
		logging.Error(h.logger, "session-close lookup failed", "handle", shortSHA(handleID), "op", "close", "err", lerr)
		return lerr
	}
	sh.mu.Unlock()
	// getLiveLocked returns with the per-session lock held across the
	// commit so two closes of the same ID still serialize; distinct
	// handles hold different locks.
	if aerr := s.authorize(ctx); aerr != nil {
		aproject, apath, ahandle := s.project, s.path, shortSHA(s.id)
		s.mu.Unlock()
		logging.Error(h.projectLogger(aproject), "session auth failed", "handle", ahandle, "project", aproject, "path", apath, "op", "close", "err", aerr)
		return aerr
	}
	started := h.config.Now().UTC()
	sproject, spath, shandle := s.project, s.path, shortSHA(s.id)
	logging.Debug(h.projectLogger(sproject), "session-close start", "handle", shandle, "project", sproject, "path", spath)
	defer func() {
		elapsed := h.config.Now().UTC().Sub(started)
		if err != nil {
			logging.Error(h.projectLogger(sproject), "session-close failed", "handle", shandle, "project", sproject, "path", spath, "elapsed", elapsed, "err", err)
			return
		}
		logging.Debug(h.projectLogger(sproject), "session-close complete", "handle", shandle, "project", sproject, "path", spath, "elapsed", elapsed)
	}()
	if s.destroyed {
		s.mu.Unlock()
		return newStaleSessionError(handleID, "unknown handle")
	}
	// Destroy needs the table lock in sh-then-s order: release s.mu
	// first, then re-acquire both and re-validate (see destroySession).
	destroy := func() error {
		return sh.destroySession(handleID, s)
	}
	if s.path == "" || !s.dirty {
		return destroy()
	}
	if _, err := h.commitSessionLocked(ctx, sh, s); err != nil {
		if errors.Is(err, ErrSessionPathGone) {
			// Pinned inode unlinked after open: POSIX close
			// discards the staged state with success.
			dproject, dpath, dhandle := s.project, s.path, shortSHA(s.id)
			if derr := destroy(); derr != nil {
				return derr
			}
			logging.Warn(h.projectLogger(dproject), "session-close-after-unlink discard", "handle", dhandle, "project", dproject, "path", dpath)
			return nil
		}
		s.mu.Unlock()
		return err
	}
	return destroy()
}
