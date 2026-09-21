package storage

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// Ordered-commit data-first: every commit attempt fsyncs the journal
// after snapshotting and before publishing — no timer involved. A failed
// push must leave the journal lines intact (for retry) but fsynced (for
// crash survival): dirty empty, lines present.
func TestFailedCommitFlushesJournalSynchronously(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	cfg.JournalDir = t.TempDir()
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	project := "projectfailedflush"

	if _, err := hub.UploadFileContext(ctx, project, "a.txt", writeTempFile(t, t.TempDir(), "a", []byte("a-content"))); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
	var fail atomic.Bool
	fail.Store(true)
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, testIndexPath) && fail.Load() {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	})
	pm := hub.getOrCreateProjectMeta(project)
	if err := hub.commitProjectMetadata(ctx, project, pm); err == nil {
		t.Fatal("commit under intercepted PUT must fail")
	}
	hub.journalMu.Lock()
	dirty := len(hub.journalDirty)
	hub.journalMu.Unlock()
	if dirty != 0 {
		t.Fatalf("failed commit left %d journals dirty; the flush must ride the attempt, not a timer", dirty)
	}
	if ops := hub.journalRead(project); len(ops) == 0 {
		t.Fatal("failed commit must retain journal lines for retry")
	}
}
