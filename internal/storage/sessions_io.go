package storage

import (
	"context"
	"fmt"
	"syscall"
)

// sessions_io.go: session data verbs: read, write, truncate, stat.

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
