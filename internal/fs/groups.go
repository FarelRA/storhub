package fs

import (
	"log/slog"
	"os/user"
	"strconv"
	"sync"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/logging"
)

// Supplementary group resolution for multi-user surfaces.
//
// The FUSE protocol carries only uid/gid/pid per request, so callerContext
// must resolve supplementary groups itself; without them the DAC judges a
// multi-group caller by primary group alone and wrongly denies group
// access. REST fills groups from its user record instead and never calls
// here: FUSE is the only production caller (see fuse_state.go).
//
// Placement: the cache lives in fs rather than fusefs because the DAC check
// consuming Groups (identityInGroup) is here, and no other package needs a
// groups helper today. Relocating it to fusefs (or a dedicated groups
// package) remains an option if a second consumer appears; that move touches
// fusefs wiring and is tracked as a follow-up, not done here.
//
// Design: os/user (NSS-aware per build: cgo builds consult nsswitch, pure
// builds read /etc/group), a 5-minute TTL cache capped at 1024 entries
// with opportunistic cleanup, a mutex plus hand-rolled in-flight dedup so
// concurrent lookups for one uid share a single NSS round trip, and
// fail-OPEN semantics: resolution failure returns an error to the caller
// but the FUSE wiring ignores it, leaving Groups empty, which denies
// nothing the pre-groups code allowed.

const (
	// groupCacheTTL bounds how long a resolved membership stays valid.
	// Group changes propagate on this horizon; shorter would re-hit NSS
	// per FUSE op storm, longer would strand removed members.
	groupCacheTTL = 60 * storcfg.PatienceUnit
	// groupCacheMaxEntries bounds resident cache size. Eviction is
	// opportunistic (expired first, then arbitrary) at insert time.
	groupCacheMaxEntries = 1024
)

type groupCacheEntry struct {
	groups    []uint32
	expiresAt time.Time
}

// groupCall is one in-flight NSS resolution. Waiters block on done, then
// read groups/err, which are written before close under groupMu.
type groupCall struct {
	done   chan struct{}
	groups []uint32
	err    error
}

var (
	groupMu       sync.Mutex
	groupCache    = make(map[uint32]groupCacheEntry)
	groupInflight = make(map[uint32]*groupCall)
)

// LookupUserGroups returns the supplementary group IDs of uid, consulting
// os/user on a cache miss. Results are copies; callers may mutate them.
// A nil slice with a non-nil error means resolution failed: callers that
// must not newly deny (FUSE callerContext) treat that as "no supplementary
// groups" and carry on with the primary gid alone.
func LookupUserGroups(uid uint32) ([]uint32, error) {
	groupMu.Lock()
	if entry, ok := groupCache[uid]; ok && time.Now().Before(entry.expiresAt) {
		out := append([]uint32(nil), entry.groups...)
		groupMu.Unlock()
		return out, nil
	}
	if call, ok := groupInflight[uid]; ok {
		groupMu.Unlock()
		<-call.done
		return append([]uint32(nil), call.groups...), call.err
	}
	call := &groupCall{done: make(chan struct{})}
	groupInflight[uid] = call
	groupMu.Unlock()

	groups, err := resolveUserGroups(uid)

	groupMu.Lock()
	delete(groupInflight, uid)
	if err == nil {
		storeGroupCacheLocked(uid, groups)
		call.groups = append([]uint32(nil), groups...)
	} else {
		call.err = err
	}
	groupMu.Unlock()
	close(call.done)

	if err != nil {
		// Fail open with a debug line: the caller proceeds with the
		// primary gid only, exactly the pre-groups behavior, so a
		// broken NSS never newly denies access. slog.Default is passed
		// explicitly: there is no per-project logger this deep, and the
		// package loggers all derive from the process default anyway.
		logging.Debug(slog.Default(), "supplementary group lookup failed; continuing with primary group only", "uid", uid, "err", err)
		return nil, err
	}
	return append([]uint32(nil), groups...), nil
}

// resolveUserGroups performs one uncached NSS lookup. Unparseable group
// IDs are skipped (a stray non-numeric entry must not void the rest);
// LookupId/GroupIds errors fail the whole resolution fail-open above.
func resolveUserGroups(uid uint32) ([]uint32, error) {
	account, err := user.LookupId(strconv.FormatUint(uint64(uid), 10))
	if err != nil {
		return nil, err
	}
	raw, err := account.GroupIds()
	if err != nil {
		return nil, err
	}
	groups := make([]uint32, 0, len(raw))
	for _, id := range raw {
		parsed, perr := strconv.ParseUint(id, 10, 32)
		if perr != nil {
			continue
		}
		groups = append(groups, uint32(parsed))
	}
	return uniqueGIDs(groups), nil
}

// storeGroupCacheLocked inserts one entry, evicting opportunistically past
// the cap: expired entries first, then arbitrary ones until under cap.
// Callers must hold groupMu.
func storeGroupCacheLocked(uid uint32, groups []uint32) {
	if len(groupCache) >= groupCacheMaxEntries {
		now := time.Now()
		for id, entry := range groupCache {
			if now.After(entry.expiresAt) {
				delete(groupCache, id)
			}
		}
		for id := range groupCache {
			if len(groupCache) < groupCacheMaxEntries {
				break
			}
			delete(groupCache, id)
		}
	}
	groupCache[uid] = groupCacheEntry{
		groups:    append([]uint32(nil), groups...),
		expiresAt: time.Now().Add(groupCacheTTL),
	}
}

// clearUserGroupCache drops all cached memberships and in-flight markers.
// Test seam only: production code never resets the cache. Tests calling it
// must not run in parallel: the cache is process-global, so a parallel
// test would observe (or evict) entries belonging to another test.
func clearUserGroupCache() {
	groupMu.Lock()
	defer groupMu.Unlock()
	groupCache = make(map[uint32]groupCacheEntry)
	groupInflight = make(map[uint32]*groupCall)
}
