package fs

import (
	"os"
	"os/user"
	"strconv"
	"sync"
	"testing"
)

// TestLookupUserGroupsCurrentUser pins the resolver against the process
// uid: where the environment reports at least one group, the resolver must
// return them (non-empty, containing a reported gid). Where the environment
// reports nothing usable, the test skips honestly instead of asserting on
// fabricated membership.
func TestLookupUserGroupsCurrentUser(t *testing.T) {
	uid := uint32(os.Getuid())
	account, err := user.LookupId(strconv.FormatUint(uint64(uid), 10))
	if err != nil {
		t.Skipf("os/user cannot resolve uid %d in this environment: %v", uid, err)
	}
	raw, err := account.GroupIds()
	if err != nil || len(raw) == 0 {
		t.Skipf("environment reports no groups for uid %d (err=%v); refusing to fake membership", uid, err)
	}
	var want []uint32
	for _, id := range raw {
		parsed, perr := strconv.ParseUint(id, 10, 32)
		if perr != nil {
			continue
		}
		want = append(want, uint32(parsed))
	}
	if len(want) == 0 {
		t.Skipf("environment group ids for uid %d are all non-numeric; nothing to pin", uid)
	}

	clearUserGroupCache()
	got, err := LookupUserGroups(uid)
	if err != nil {
		t.Fatalf("LookupUserGroups(%d): %v", uid, err)
	}
	if len(got) == 0 {
		t.Fatalf("LookupUserGroups(%d) returned no groups while os/user reports %v", uid, raw)
	}
	seen := make(map[uint32]bool, len(got))
	for _, gid := range got {
		seen[gid] = true
	}
	matched := false
	for _, gid := range want {
		if seen[gid] {
			matched = true
			break
		}
	}
	if !matched {
		t.Fatalf("LookupUserGroups(%d) = %v, shares nothing with os/user %v", uid, got, want)
	}

	// A second call must hit the cache and agree exactly.
	again, err := LookupUserGroups(uid)
	if err != nil {
		t.Fatalf("second LookupUserGroups(%d): %v", uid, err)
	}
	if len(again) != len(got) {
		t.Fatalf("cached lookup diverged: first %v, second %v", got, again)
	}
	for i := range got {
		if again[i] != got[i] {
			t.Fatalf("cached lookup diverged: first %v, second %v", got, again)
		}
	}
}

// TestLookupUserGroupsUnknownUserFailsOpen pins fail-open: an unresolvable
// uid reports an error AND an empty membership, never a fabricated group
// that could newly grant (or, by odd paths, deny) access.
func TestLookupUserGroupsUnknownUserFailsOpen(t *testing.T) {
	clearUserGroupCache()
	const unknownUID = uint32(4294967294)
	groups, err := LookupUserGroups(unknownUID)
	if err == nil {
		t.Fatalf("LookupUserGroups(%d) = %v, want an error for an unresolvable uid", unknownUID, groups)
	}
	if len(groups) != 0 {
		t.Fatalf("LookupUserGroups(%d) returned groups %v alongside error %v; fail-open demands empty", unknownUID, groups, err)
	}
}

// TestLookupUserGroupsConcurrentDedup pins the hand-rolled singleflight:
// concurrent lookups for one uid all agree and none deadlocks.
func TestLookupUserGroupsConcurrentDedup(t *testing.T) {
	uid := uint32(os.Getuid())
	if _, err := user.LookupId(strconv.FormatUint(uint64(uid), 10)); err != nil {
		t.Skipf("os/user cannot resolve uid %d: %v", uid, err)
	}
	clearUserGroupCache()
	const workers = 16
	results := make([][]uint32, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = LookupUserGroups(uid)
		}(i)
	}
	wg.Wait()
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if len(results[i]) != len(results[0]) {
			t.Fatalf("worker %d diverged: %v vs %v", i, results[i], results[0])
		}
		for j := range results[i] {
			if results[i][j] != results[0][j] {
				t.Fatalf("worker %d diverged: %v vs %v", i, results[i], results[0])
			}
		}
	}
}

// TestGroupCacheCapEvicts pins the 1024-entry cap: overfilling the cache
// through the insert path leaves at most groupCacheMaxEntries entries.
func TestGroupCacheCapEvicts(t *testing.T) {
	clearUserGroupCache()
	groupMu.Lock()
	for i := uint32(0); i < groupCacheMaxEntries+64; i++ {
		storeGroupCacheLocked(30000+i, []uint32{i})
	}
	n := len(groupCache)
	groupMu.Unlock()
	if n > groupCacheMaxEntries {
		t.Fatalf("cache holds %d entries, cap is %d", n, groupCacheMaxEntries)
	}
	clearUserGroupCache()
}
