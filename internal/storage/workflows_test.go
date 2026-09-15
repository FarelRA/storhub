package storage

import (
	"testing"
)

// TestStoreRepoMetadataPreservesPendingWork pins the pending-work contract: a fresh remote load
// (reached from enforceExpectedRevision, RevisionContext, conflict recovery,
// purge, ...) must never clobber acknowledged-but-uncommitted local work.
// The old unconditional apply-back wiped the dirty flag, the op stack, and
// the crash journal - silently discarding mutations the callers were told
// had succeeded.
func TestStoreRepoMetadataPreservesPendingWork(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	seedMeta(t, hub, "guard", "docs", "a.txt", 1)

	pm := hub.getOrCreateProjectMeta("guard")
	pm.mu.Lock()
	// Local acknowledged work that has not committed yet.
	pm.meta.EnsureDirectory("pending", 1700000100)
	hub.appendOpLocked("guard", pm, Op{Type: OpMkdir, Paths: []string{"pending"}, Cause: "mkdir", Timestamp: 1700000100, Dir: &DirMeta{Inode: 99, Mode: 0o755, CreatedAt: 1700000100, ModifiedAt: 1700000100}})
	markProjectDirtyLocked(pm)
	versionBefore := pm.version
	shaBefore := pm.sha
	pm.mu.Unlock()

	remote := NewRepoMetadata("guard")
	remote.EnsureDirectory("docs", 1700000000)
	remote.Chunks[1] = ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 1}
	remote.UpsertFile("docs/a.txt", FileMeta{Size: 4, Mode: 0o644, Inode: 2, Chunks: []int64{1}, UploadedAt: 1700000000, ModifiedAt: 1700000000, AccessedAt: 1700000000, ChangedAt: 1700000000}, 1700000000)
	remote.EnsureRelease("v1", 1700000000)
	remote.Normalize("guard", 1700000000)

	hub.storeRepoMetadata("guard", *remote, "remote-sha-token", nil, 0)

	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.dirty {
		t.Fatal("remote store wiped the dirty flag (acknowledged work lost)")
	}
	if len(pm.opStack.ops) != 1 || pm.opStack.ops[0].Type != OpMkdir {
		t.Fatalf("remote store wiped the pending op stack: %+v", pm.opStack.ops)
	}
	if !pm.meta.HasDirectory("pending") {
		t.Fatal("remote store clobbered the local tree")
	}
	if pm.sha != shaBefore {
		t.Fatalf("remote store replaced the CAS token under pending work: %q -> %q", shaBefore, pm.sha)
	}
	if pm.version != versionBefore {
		t.Fatalf("version bumped without a shared-state replacement: %d -> %d", versionBefore, pm.version)
	}
}

// TestStoreRepoMetadataAppliesBackWhenClean pins the other half of that contract: with
// no pending work the remote truth applies back, hydrates, and bumps the
// version so an in-flight transaction's guard observes the swap.
func TestStoreRepoMetadataAppliesBackWhenClean(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	pm := hub.getOrCreateProjectMeta("cleanproj")
	pm.mu.Lock()
	versionBefore := pm.version
	pm.mu.Unlock()

	remote := NewRepoMetadata("cleanproj")
	remote.EnsureDirectory("docs", 1700000000)
	remote.Normalize("cleanproj", 1700000000)
	hub.storeRepoMetadata("cleanproj", *remote, "tok", nil, 7)

	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.hydrated {
		t.Fatal("clean store must hydrate the entry")
	}
	if !pm.meta.HasDirectory("docs") {
		t.Fatal("clean store must apply the remote tree back")
	}
	if pm.sha != "tok" || pm.objectCount != 7 {
		t.Fatalf("clean store must adopt token/count: sha=%q count=%d", pm.sha, pm.objectCount)
	}
	if pm.version == versionBefore {
		t.Fatal("replacing shared state must bump pm.version")
	}
}

// TestJournalReplaySkipsSupersededOps pins the journal-replay contract: a journal rewrite that failed
// after a commit leaves committed ops in the file; a cold replay must not
// re-assert them over newer remote state. Ops newer than the remote entry
// still replay (the crash-survival contract).
func TestJournalReplaySkipsSupersededOps(t *testing.T) {
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	cfg.JournalDir = t.TempDir()
	hub := backend.newClient(t, cfg)

	// Stale line: put a.txt (size 5) at t=100, while remote already has
	// size 9 from t=200.
	hub.journalAppend("stale", Op{Seq: 1, Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload", Timestamp: 100,
		File: &FileMeta{Size: 5, Mode: 0o644, Inode: 2, Chunks: []int64{}, UploadedAt: 100, ModifiedAt: 100, AccessedAt: 100, ChangedAt: 100}})
	meta := NewRepoMetadata("stale")
	meta.Normalize("stale", 200)
	meta.UpsertFile("a.txt", FileMeta{Size: 9, Mode: 0o644, Inode: 3, Chunks: []int64{}, UploadedAt: 200, ModifiedAt: 200, AccessedAt: 200, ChangedAt: 200}, 200)
	if ops := hub.journalReplayForLoad("stale", meta); len(ops) != 0 {
		t.Fatalf("superseded op must be skipped, got %+v", ops)
	}
	if f := meta.FindFile("a.txt"); f == nil || f.Size != 9 {
		t.Fatalf("newer remote entry was clobbered by replay: %+v", f)
	}

	// Live line: put b.txt at t=300 against a remote that never saw it -
	// the acknowledged mutation must survive the crash.
	hub.journalAppend("live", Op{Seq: 1, Type: OpPutFile, Paths: []string{"b.txt"}, Cause: "upload", Timestamp: 300,
		File: &FileMeta{Size: 1, Mode: 0o644, Inode: 2, Chunks: []int64{}, UploadedAt: 300, ModifiedAt: 300, AccessedAt: 300, ChangedAt: 300}})
	meta2 := NewRepoMetadata("live")
	meta2.Normalize("live", 200)
	ops := hub.journalReplayForLoad("live", meta2)
	if len(ops) != 1 {
		t.Fatalf("non-superseded op must replay, got %+v", ops)
	}
	if meta2.FindFile("b.txt") == nil {
		t.Fatal("expected replayed b.txt")
	}
}
