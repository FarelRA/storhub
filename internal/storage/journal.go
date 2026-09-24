package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/FarelRA/storhub/internal/logging"
)

// The op journal is the crash-survival mirror of a project's pending op
// stack: every mutation appends its PRE-COALESCING DELTA as one JSON line
// with Times=1 BEFORE the next commit can lose it, and a successful commit
// rewrites the journal to exactly the still-pending ops (empty stack removes
// the file). On a cold start, a journal present for a never-hydrated project
// replays onto the freshly loaded remote state - acknowledged mutations survive a
// crash that the previous discard-on-conflict design would have dropped.
//
// Deltas, not tails: appendOpLocked journals the op exactly as appended
// (appendWithDelta), before stack coalescing rewrote it. foldOps then replays
// the identical append sequence through the identical rules and converges to
// exactly the live stack: including cross-transaction rename chains and
// rename-then-delete, which post-coalescing tails cannot reproduce. Times
// accumulates during the fold from per-delta Times=1 lines.
//
// Each delta carries its append-time generation (Op.Gen, `gen` key); the
// fold merges only within one generation, so a chain split by a commit
// freeze mid-chain refolds split exactly like the live stack. The old
// snapshot mark (`snap` key) is no longer written; lines carrying it
// (pre-generation journals) still parse and merge within the legacy
// generation.
//
// Deltas are kept deliberately (the A9 commit-time-formatting question):
// the rename-then-delete transform consumes a live rename that a
// tail-journaled line would already have rewritten away: journaling
// [rename A->B tail, del A tail] refolds to TWO ops while the live stack
// holds ONE, a divergence no generation tag can repair because the
// history the transform needed is gone. Tails cannot reproduce
// history-sensitive collapses; deltas replay the identical append
// sequence, so convergence is structural.
//
// The journal file is bounded by journalMaxBytes (64MiB): crossing the cap
// rewrites the journal to the folded survivors even though no commit
// succeeded (compaction, not acknowledgment): acknowledged ops are never
// dropped. The op stack's own byte cap (opStackMaxBytes) triggers the same
// rewrite, so either bound compacts the file.
//
// The journal is append-only and folded with the same coalescing rules as
// the live stack, so a torn final line (crash mid-write) costs only that
// append, and replaying a journal whose ops already committed is a no-op
// (ops are full-state assertions).

const (
	journalLineInitialBytes = 64 * 1024
	journalLineMaxBytes     = 16 * 1024 * 1024
	// journalMaxBytes bounds one project's journal file (64MiB, mirroring
	// the opStackMaxBytes bound on the live stack). Crossing it compacts via
	// journalRewrite to the folded survivors; ops are never dropped for
	// crossing (drop-never: only a commit clears them).
	journalMaxBytes = 64 << 20
	// journalRewriteSampleEvery bounds how often the cap is probed: a Stat
	// per append would double append syscalls, so appendOpLocked stats the
	// file only when the stack itself crossed its byte cap or every Nth
	// append (opStack.seq is the monotonic append counter).
	journalRewriteSampleEvery = 128
	// journalProbeLargeLine forces a cap stat when one appended line is
	// this big (1 MiB) even off the sample cadence: with 16 MiB max
	// lines, journal-only growth (large member lists, no stack-byte
	// crossing) could otherwise overshoot the 64 MiB cap by up to
	// 127 * 16 MiB before the next sample fires.
	journalProbeLargeLine = 1 << 20
	// No group-commit window: durability rides the commit loop
	// (ext4-ordered shape). Appends write immediately; every commit
	// attempt fsyncs the journal after snapshotting and before
	// publishing, so a closed batch is disk-durable before its manifest
	// CAS. A power loss can only drop ops estate never reached a commit
	// attempt: the same exposure as before minus the timer tail.
	// Committed state is unaffected (journalRewrite fsyncs before its
	// rename), and Shutdown flushes.
)

func (h *StorHub) journalPath(project string) string {
	if h.config.JournalDir == "" {
		return ""
	}
	return filepath.Join(h.config.JournalDir, project+".jsonl")
}

// journalAppend appends one op line. The op MUST be the pre-coalescing delta
// (appendWithDelta): journaling post-coalescing tails diverges the folded
// journal from the live stack on cross-transaction rename chains and
// rename-then-delete (see the header note: the transform's history is
// gone from tails). Times is forced to 1 defensively: accumulation
// happens in the fold, so a stored Times>1 (old post-coalescing journals,
// re-appended survivors) would inflate the folded total.
// Best-effort: a journal write failure is logged and never fails the
// mutation - the journal upgrades durability for acknowledged mutations, it
// must not downgrade availability. The write is durable to same-machine
// readers immediately (page cache); the fsync rides the next commit
// attempt, which always runs before anything publishes.
//
// It returns the marshaled line length (0 when nothing was written) so the
// caller can force a cap probe on large lines (see journalOverCap).
func (h *StorHub) journalAppend(project string, op Op) int {
	op.Times = 1
	path := h.journalPath(project)
	if path == "" {
		return 0
	}
	line, err := json.Marshal(op)
	if err != nil {
		logging.Warn(h.projectLogger(project), "op journal append failed", "op", string(op.Type), "err", err)
		return 0
	}
	h.journalMu.Lock()
	f, ok := h.journalFiles[project]
	if !ok {
		if err := os.MkdirAll(h.config.JournalDir, 0o755); err != nil {
			h.journalMu.Unlock()
			logging.Warn(h.projectLogger(project), "op journal append failed; will retry on next commit", "op", string(op.Type), "err", err)
			return 0
		}
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			h.journalMu.Unlock()
			logging.Warn(h.projectLogger(project), "op journal append failed; will retry on next commit", "op", string(op.Type), "err", err)
			return 0
		}
		h.journalFiles[project] = f
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		h.journalMu.Unlock()
		logging.Warn(h.projectLogger(project), "op journal append failed; will retry on next commit", "op", string(op.Type), "err", err)
		return 0
	}
	h.journalDirty[project] = true
	// No timer: durability rides the commit loop (ext4-ordered shape).
	// Every commit attempt fsyncs the journal after snapshotting and
	// before publishing, so a closed batch is disk-durable before its
	// manifest CAS; explicit flush points (fsync path, DrainProject,
	// Shutdown) cover the rest. An armed timer would only re-fsync
	// what the next commit already syncs.
	h.journalMu.Unlock()
	return len(line)
}

// flushJournals fsyncs every journal with pending appends. It snapshots
// the dirty handles under the lock and syncs outside it so a slow fsync
// never blocks concurrent appends. Called on every commit attempt after
// snapshotting (the ordered-commit data-first step) and at explicit
// durability points; never on a timer.
func (h *StorHub) flushJournals() {
	h.journalMu.Lock()
	if len(h.journalDirty) == 0 {
		h.journalMu.Unlock()
		return
	}
	handles := make([]*os.File, 0, len(h.journalDirty))
	for p := range h.journalDirty {
		if f, ok := h.journalFiles[p]; ok {
			handles = append(handles, f)
		}
		delete(h.journalDirty, p)
	}
	h.journalMu.Unlock()
	for _, f := range handles {
		if err := f.Sync(); err != nil {
			logging.Warn(h.logger, "op journal sync failed", "err", err)
		}
	}
}

// closeJournals flushes and releases every open journal handle. Called once
// at Shutdown after the commit loops have exited.
func (h *StorHub) closeJournals() {
	h.flushJournals()
	h.journalMu.Lock()
	defer h.journalMu.Unlock()
	for p, f := range h.journalFiles {
		_ = f.Close()
		delete(h.journalFiles, p)
		delete(h.journalDirty, p)
	}
}

// closeProjectJournal flushes, closes, and forgets one project's journal
// handle, and removes its journal file. Idempotent: a project with no open
// handle is a no-op (its cold-recovery file, if any, is left for
// journalReplayForLoad: only a held handle proves this process owns recent
// uncommitted appends). No-op when JournalDir == "". Takes journalMu only,
// never pm.mu or metaMu, so it can run under either.
//
// Contract for the eviction owner (workflows.go releaseProjectResidue):
// call h.closeProjectJournal(name) alongside the git/object/release/logger
// teardown so project churn cannot retain an open FD plus map entries per
// project after meta eviction. Safe for deleteRepo too (a deleted project's
// pending journal is moot).
func (h *StorHub) closeProjectJournal(project string) {
	if h.config.JournalDir == "" {
		return
	}
	path := h.journalPath(project)
	h.journalMu.Lock()
	f, ok := h.journalFiles[project]
	if ok {
		// Sync under the lock: eviction-time, rare, and the handle is
		// being retired: no append may interleave after the deletes.
		_ = f.Sync()
		_ = f.Close()
		delete(h.journalFiles, project)
		delete(h.journalDirty, project)
	}
	h.journalMu.Unlock()
	if ok {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logging.Warn(h.projectLogger(project), "op journal remove on close failed", "err", err)
		}
	}
}

// journalRead loads and folds the project's journal. Corrupt lines are
// skipped individually; a wholly unreadable journal yields nil (the crash
// loses those pending ops, matching the pre-journal durability contract).
// Every line's Times is reset to 1 before the fold: lines are deltas (one
// mutation each) and the fold accumulates, so a stored Times>1: from
// pre-delta journals that carried accumulated counts, or re-appended
// survivors: would inflate the folded total (cosmetic: commit message xN).
func (h *StorHub) journalRead(project string) []Op {
	path := h.journalPath(project)
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			logging.Warn(h.projectLogger(project), "op journal read failed; will retry on next load", "err", err)
		}
		return nil
	}
	defer func() { _ = f.Close() }()
	var ops []Op
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, journalLineInitialBytes), journalLineMaxBytes)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var op Op
		if err := json.Unmarshal(line, &op); err != nil {
			logging.Debug(h.projectLogger(project), "op journal line unreadable; skipping", "err", err)
			continue
		}
		op.Times = 1
		ops = append(ops, op)
	}
	if err := scanner.Err(); err != nil {
		logging.Warn(h.projectLogger(project), "op journal read failed; will retry on next load", "err", err)
		return nil
	}
	if len(ops) == 0 {
		return nil
	}
	return foldOps(ops)
}

// journalFileSize returns the on-disk size of the project's journal file,
// or 0 when journaling is disabled or the file is absent/unstatable. Read
// from the filesystem (no new hub state); callers sample it: see
// journalOverCap.
func (h *StorHub) journalFileSize(project string) int64 {
	path := h.journalPath(project)
	if path == "" {
		return 0
	}
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

// journalOverCap reports whether the project's journal file crossed
// journalMaxBytes. appendOpLocked calls this with the stack's monotonic
// append counter (opStack.seq) and the just-appended line length: the Stat
// runs on the sample cadence or when one line is large (at least
// journalProbeLargeLine), so cap probing never doubles append syscalls on
// the small-line hot path while journal-only growth still converges.
// Crossing compacts via journalRewrite (caller-side), never drops.
func (h *StorHub) journalOverCap(project string, appendSeq uint64, lastLineBytes int) bool {
	if h.config.JournalDir == "" {
		return false
	}
	if lastLineBytes < journalProbeLargeLine && appendSeq%journalRewriteSampleEvery != 0 {
		return false
	}
	return h.journalFileSize(project) >= journalMaxBytes
}

// journalRewrite replaces the journal with exactly the still-pending ops.
// An empty list removes the file. Rewriting (instead of deleting) keeps the
// journal correct when mutations landed while a commit was in flight. It is
// also the cap-compaction path: appendOpLocked calls it when the op-stack or
// journal byte cap crosses even though no commit succeeded: the rewrite is
// compaction to the folded survivors, not acknowledgment.
//
// Locking: journalMu is held across the whole critical section (handle
// close plus tmp write plus fsync plus rename). Rewrite is rare (commit
// success plus cap compaction) and both call sites already hold pm.mu, so
// the order stays pm.mu then journalMu with no reverse path (journal code
// never takes pm.mu). Appenders only take journalMu briefly and block for
// the rewrite window instead of losing lines to the rename race (F2).
func (h *StorHub) journalRewrite(project string, ops []Op) {
	path := h.journalPath(project)
	if path == "" {
		return
	}
	h.journalMu.Lock()
	defer h.journalMu.Unlock()
	// Drop the open append handle first: the rename below replaces the file,
	// and a lingering handle would keep appending to the unlinked inode (and
	// its pending fsync would target the wrong file). The next append reopens
	// the new file.
	if f, ok := h.journalFiles[project]; ok {
		_ = f.Close()
		delete(h.journalFiles, project)
		delete(h.journalDirty, project)
	}
	if len(ops) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logging.Warn(h.projectLogger(project), "op journal clear failed", "ops", 0, "err", err)
		} else {
			logging.Debug(h.projectLogger(project), "op journal clear complete", "ops", 0)
		}
		return
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		logging.Warn(h.projectLogger(project), "op journal rewrite failed; will retry on next commit", "ops", len(ops), "err", err)
		return
	}
	ok := true
	for _, op := range ops {
		line, err := json.Marshal(op)
		if err != nil {
			ok = false
			break
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			ok = false
			break
		}
	}
	// Sync before the rename so a crash cannot leave the new name pointing
	// at un-flushed bytes (same durability contract as append).
	if ok {
		if err := f.Sync(); err != nil {
			ok = false
		}
	}
	closeErr := f.Close()
	if ok && closeErr == nil {
		if err := os.Rename(tmp, path); err != nil {
			logging.Warn(h.projectLogger(project), "op journal rewrite failed; will retry on next commit", "ops", len(ops), "err", err)
		} else {
			logging.Debug(h.projectLogger(project), "op journal rewrite complete", "ops", len(ops))
		}
		return
	}
	_ = os.Remove(tmp)
	if closeErr != nil {
		logging.Warn(h.projectLogger(project), "op journal rewrite failed; will retry on next commit", "ops", len(ops), "err", closeErr)
	} else if !ok {
		logging.Warn(h.projectLogger(project), "op journal rewrite failed; will retry on next commit", "ops", len(ops))
	}
}
