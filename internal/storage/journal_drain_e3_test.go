package storage

// Phase E3 (F5): DrainProjectContext is an explicit journal durability
// point. A journal dirtied outside any commit (the failure-path/leftover
// state: ops acknowledged and journalled whose commit never rewrote the
// journal) must be fsynced synchronously inside the drain, not within the
// 100ms group-commit window. No sleeps: the assertion runs immediately
// after the drain returns.

import (
	"context"
	"testing"
)

func TestDrainProjectFlushesJournalsSynchronously(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	cfg.JournalDir = t.TempDir()
	hub := backend.newClient(t, cfg)
	project := "project-drain-flush"

	hub.journalAppend(project, Op{Seq: 1, Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload", Timestamp: 1})
	if err := hub.DrainProjectContext(context.Background(), project); err != nil {
		t.Fatalf("drain: %v", err)
	}
	hub.journalMu.Lock()
	dirty := len(hub.journalDirty)
	timerArmed := hub.journalTimer != nil
	hub.journalMu.Unlock()
	if dirty != 0 {
		t.Fatalf("drain left %d journals dirty; the flush is still timer-deferred", dirty)
	}
	if timerArmed {
		t.Fatal("drain left the group-commit timer armed; the sync path still has a 100ms tail")
	}
}
