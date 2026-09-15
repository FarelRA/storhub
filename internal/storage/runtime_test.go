package storage

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestAtimeUpdateSurvivesDeleteDuringRevival pins the revival-window contract: the atime path reads
// the entry again AFTER markProjectDirtyLiveLocked, which drops pm.mu for
// the revival critical section. A concurrent stale-pointer delete can remove
// the entry in that window; the old code panicked dereferencing the nil
// result of GetDirectory/FindFile before calling Clone.
func TestAtimeUpdateSurvivesDeleteDuringRevival(t *testing.T) {
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	cfg.AtimePolicy = "strictatime"
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	project := "atime-race"
	seedMeta(t, hub, project, "docs", "a.txt", 1)

	pm := hub.getOrCreateProjectMeta(project)
	waitClean(t, pm)

	// Wedge the commit loop inside a commit so eviction leaves stoppedCh
	// open (the loop only closes it on exit).
	var wedged atomic.Bool
	release := make(chan struct{})
	entered := make(chan struct{})
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && wedged.CompareAndSwap(false, true) {
			close(entered)
			<-release
			http.Error(w, "injected wedge failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	if _, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		m.EnsureDirectory("wedge", 1700000050)
		return nil
	}, "storhub: wedge"); err != nil {
		t.Fatalf("wedge mkdir: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit loop never entered the blocked commit")
	}

	// Evict the instance (loop is stuck mid-commit, so stoppedCh stays
	// open), then put it back in the cache: the state a mutator observes
	// while a revival is pending.
	hub.metaMu.Lock()
	pm.mu.Lock()
	pm.stopped = true
	close(pm.stopCh)
	delete(hub.metaCache, project)
	pm.mu.Unlock()
	hub.metaMu.Unlock()
	hub.metaMu.Lock()
	hub.metaCache[project] = pm
	hub.metaMu.Unlock()

	panicCh := make(chan any, 1)
	go func() {
		defer func() { panicCh <- recover() }()
		hub.QueueAtimeUpdateContext(ctx, project, "docs", true, 1700000200)
		panicCh <- nil
	}()

	// Let the atime writer pass SetDirAtime and park in the revival
	// select (pm.mu released), then delete the directory through the same
	// pointer - the stale-pointer delete of the failure scenario.
	time.Sleep(200 * time.Millisecond)
	pm.mu.Lock()
	pm.meta.RemoveDirectory("docs")
	pm.mu.Unlock()
	close(release)

	select {
	case p := <-panicCh:
		if p != nil {
			t.Fatalf("atime update panicked across the revival window: %v", p)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("atime writer never completed")
	}

	pm.mu.RLock()
	dirty := pm.dirty
	var atimeOps int
	for _, op := range pm.opStack.ops {
		if op.Cause == "atime" {
			atimeOps++
		}
	}
	pm.mu.RUnlock()
	if !dirty {
		t.Fatal("fixture broken: the atime writer never reached the revival path")
	}
	if atimeOps != 0 {
		t.Fatalf("atime op recorded for a deleted entry: %+v", pm.opStack.ops)
	}
}
