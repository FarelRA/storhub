package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/FarelRA/storhub/internal/logging"
)

// The op journal is the crash-survival mirror of a project's pending op
// stack: every mutation appends its (post-coalescing) op as one JSON line
// BEFORE the next commit can lose it, and a successful commit rewrites the
// journal to exactly the still-pending ops (empty stack removes the file).
// On a cold start, a journal present for a never-hydrated project replays
// onto the freshly loaded remote state - acknowledged mutations survive a
// crash that the previous discard-on-conflict design would have dropped.
//
// The journal is append-only and folded with the same coalescing rules as
// the live stack, so a torn final line (crash mid-write) costs only that
// append, and replaying a journal whose ops already committed is a no-op
// (ops are full-state assertions).

const (
	journalLineInitialBytes = 64 * 1024
	journalLineMaxBytes     = 16 * 1024 * 1024
	// journalGroupCommitWindow bounds how long an appended op may sit
	// un-fsynced. A per-op fsync was one syscall per appended op (a bulk
	// import = one fsync per file); appends now write immediately and a
	// short timer coalesces the fsync across the window. The tradeoff is
	// honest: a power loss inside the window can drop the last
	// journalGroupCommitWindow of appends. Committed state is unaffected
	// (journalRewrite fsyncs before its rename), and Shutdown flushes, so
	// the exposure is only acknowledged-but-not-yet-committed ops lost to a
	// hard crash within the window - the standard group-commit contract.
	journalGroupCommitWindow = 100 * time.Millisecond
)

func (h *StorHub) journalPath(project string) string {
	if h.config.JournalDir == "" {
		return ""
	}
	return filepath.Join(h.config.JournalDir, project+".jsonl")
}

// journalAppend appends one op line. Best-effort: a journal write failure
// is logged and never fails the mutation - the journal upgrades durability
// for acknowledged mutations, it must not downgrade availability. The write
// is durable to same-machine readers immediately (page cache); the fsync is
// coalesced by the group-commit timer.
func (h *StorHub) journalAppend(project string, op Op) {
	path := h.journalPath(project)
	if path == "" {
		return
	}
	line, err := json.Marshal(op)
	if err != nil {
		logging.Warn(h.projectLogger(project), "op journal marshal failed", "err", err)
		return
	}
	h.journalMu.Lock()
	f, ok := h.journalFiles[project]
	if !ok {
		if err := os.MkdirAll(h.config.JournalDir, 0o755); err != nil {
			h.journalMu.Unlock()
			logging.Warn(h.projectLogger(project), "op journal append failed", "err", err)
			return
		}
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			h.journalMu.Unlock()
			logging.Warn(h.projectLogger(project), "op journal append failed", "err", err)
			return
		}
		h.journalFiles[project] = f
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		h.journalMu.Unlock()
		logging.Warn(h.projectLogger(project), "op journal append failed", "err", err)
		return
	}
	h.journalDirty[project] = true
	if h.journalTimer == nil {
		h.journalTimer = time.AfterFunc(journalGroupCommitWindow, h.flushJournals)
	}
	h.journalMu.Unlock()
}

// flushJournals fsyncs every journal with pending appends (the group-commit
// point). It snapshots the dirty handles under the lock and syncs outside it
// so a slow fsync never blocks concurrent appends.
func (h *StorHub) flushJournals() {
	h.journalMu.Lock()
	h.journalTimer = nil
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
			logging.Warn(h.logger, "op journal group-commit sync failed", "err", err)
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

// journalRead loads and folds the project's journal. Corrupt lines are
// skipped individually; a wholly unreadable journal yields nil (the crash
// loses those pending ops, matching the pre-journal durability contract).
func (h *StorHub) journalRead(project string) []Op {
	path := h.journalPath(project)
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			logging.Warn(h.projectLogger(project), "op journal read failed", "err", err)
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
			logging.Warn(h.projectLogger(project), "op journal line unreadable; skipping", "err", err)
			continue
		}
		ops = append(ops, op)
	}
	if err := scanner.Err(); err != nil {
		logging.Warn(h.projectLogger(project), "op journal read failed", "err", err)
		return nil
	}
	if len(ops) == 0 {
		return nil
	}
	return foldOps(ops)
}

// journalRewrite replaces the journal with exactly the still-pending ops.
// An empty list removes the file. Rewriting (instead of deleting) keeps the
// journal correct when mutations landed while a commit was in flight.
func (h *StorHub) journalRewrite(project string, ops []Op) {
	path := h.journalPath(project)
	if path == "" {
		return
	}
	// Drop the open append handle first: the rename below replaces the file,
	// and a lingering handle would keep appending to the unlinked inode (and
	// its pending fsync would target the wrong file). The next append reopens
	// the new file.
	h.journalMu.Lock()
	if f, ok := h.journalFiles[project]; ok {
		_ = f.Close()
		delete(h.journalFiles, project)
		delete(h.journalDirty, project)
	}
	h.journalMu.Unlock()
	if len(ops) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logging.Warn(h.projectLogger(project), "op journal clear failed", "err", err)
		}
		return
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		logging.Warn(h.projectLogger(project), "op journal rewrite failed", "err", err)
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
			logging.Warn(h.projectLogger(project), "op journal rewrite failed", "err", err)
		}
		return
	}
	_ = os.Remove(tmp)
	if closeErr != nil {
		logging.Warn(h.projectLogger(project), "op journal rewrite failed", "err", closeErr)
	}
}
