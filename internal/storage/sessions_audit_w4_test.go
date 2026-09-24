package storage

// sessions_audit_w4_test.go: regression tests for the verbs/sessions audit
// fixes (F1, F2, M1, M2, M5, M6, prune error contract, OpenMode rendering).
// Each test fails against the pre-fix behavior described in its comment.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// A far-offset write must stage a sparse hole, never a heap fill: the old
// code allocated offset-curSize bytes up front, so a 1 GiB sparse jump
// attempted a 1 GiB allocation and OOMed the host.
func TestSessionSparseWriteStagesHole(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionw4sparse"

	id := mustOpenSession(ctx, t, hub, proj, "", SessionReadWrite)
	const far = 1 << 30
	if _, err := hub.WriteSession(ctx, id, far, []byte("tail")); err != nil {
		t.Fatalf("far write: %v", err)
	}
	stat, err := hub.StatSession(ctx, id)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if stat.Size != far+4 {
		t.Fatalf("size = %d, want %d", stat.Size, far+4)
	}
	// The hole must be sparse on disk, not a heap fill written out: the
	// old code wrote offset-curSize zero bytes through the temp, so a 1
	// GiB hole cost 1 GiB of backing blocks.
	sh := hub.sessionHub()
	sh.mu.Lock()
	s, ok := sh.byID[id]
	sh.mu.Unlock()
	if !ok {
		t.Fatal("handle missing from table")
	}
	fi, err := os.Stat(s.tmpName)
	if err != nil {
		t.Fatalf("stat temp: %v", err)
	}
	if blocks := fi.Sys().(*syscall.Stat_t).Blocks; blocks > 1024 {
		t.Fatalf("staging temp uses %d 512-byte blocks for a 1 GiB hole, want a sparse hole", blocks)
	}
	// The hole reads back as zeros without ever being written.
	hole, err := hub.ReadSession(ctx, id, 0, 16)
	if err != nil {
		t.Fatalf("hole read: %v", err)
	}
	for i, b := range hole {
		if b != 0 {
			t.Fatalf("hole byte %d = %d, want 0", i, b)
		}
	}
	mid, err := hub.ReadSession(ctx, id, far-8, 8)
	if err != nil {
		t.Fatalf("pre-tail read: %v", err)
	}
	for i, b := range mid {
		if b != 0 {
			t.Fatalf("pre-tail byte %d = %d, want 0", i, b)
		}
	}
	got, err := hub.ReadSession(ctx, id, far, 4)
	if err != nil {
		t.Fatalf("tail read: %v", err)
	}
	if string(got) != "tail" {
		t.Fatalf("tail = %q, want %q", got, "tail")
	}
	// Close the unlinked scratch: the temp discards with no commit, so
	// this 1 GiB staging never passes through the upload path.
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A staged hole commits through the normal full-image path and reads back
// as zeros around the written tail.
func TestSessionSparseHoleCommits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	cfg := baseTestConfig()
	cfg.ChunkSize = 1 << 20
	hub := backend.newClient(t, cfg)
	proj := "projectsessionw4sparsecommit"

	id := mustOpenSession(ctx, t, hub, proj, "", SessionReadWrite)
	const far = 1<<20 + 100
	if _, err := hub.WriteSession(ctx, id, far, []byte("tail")); err != nil {
		t.Fatalf("far write: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "sparse.bin"); err != nil {
		t.Fatalf("link: %v", err)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
	got := freshSessionBytes(t, hub, proj, "sparse.bin")
	if len(got) != far+4 {
		t.Fatalf("size = %d, want %d", len(got), far+4)
	}
	for i := 0; i < 16; i++ {
		if got[i] != 0 {
			t.Fatalf("hole byte %d = %d, want 0", i, got[i])
		}
	}
	if string(got[far:]) != "tail" {
		t.Fatalf("tail = %q, want %q", got[far:], "tail")
	}
}

// Hydrate must stream a multi-window snapshot exactly: the old code
// materialized the whole pinned file in one allocation, the new code reads
// in bounded windows. A 2.5-window file pins both the window-boundary
// correctness and the exact-size tail.
func TestSessionHydrateStreamsAcrossWindows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	cfg := baseTestConfig()
	cfg.ChunkSize = 1 << 20
	hub := backend.newClient(t, cfg)
	proj := "projectsessionw4hydrate"

	size := int64(2*hydrateWindowSize + hydrateWindowSize/2)
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i*31 + 7)
	}
	setupSessionFile(ctx, t, hub, proj, "big.bin", content)

	id := mustOpenSession(ctx, t, hub, proj, "big.bin", SessionReadWrite)
	// First mutation hydrates; flip one byte per window including the tail.
	for _, off := range []int64{0, hydrateWindowSize - 1, hydrateWindowSize, size - 1} {
		if _, err := hub.WriteSession(ctx, id, off, []byte{0xAA}); err != nil {
			t.Fatalf("write at %d: %v", off, err)
		}
	}
	if err := hub.SyncSession(ctx, id); err != nil {
		t.Fatalf("sync: %v", err)
	}
	got := freshSessionBytes(t, hub, proj, "big.bin")
	if len(got) != len(content) {
		t.Fatalf("size = %d, want %d", len(got), len(content))
	}
	for i := range content {
		want := content[i]
		if i == 0 || i == int(hydrateWindowSize)-1 || i == int(hydrateWindowSize) || i == len(content)-1 {
			want = 0xAA
		}
		if got[i] != want {
			t.Fatalf("byte %d = %d, want %d", i, got[i], want)
		}
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Sync after a rename must commit to the surviving name and repin there:
// the old repin looked up the open-time path, found nothing, and reported
// NotFound for a durable commit (leaving dirty set, so every later Sync
// repeated the failure).
func TestSessionSyncFollowsRename(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionw4syncrename"
	setupSessionFile(ctx, t, hub, proj, "a.txt", []byte("v1"))

	id := mustOpenSession(ctx, t, hub, proj, "a.txt", SessionReadWrite)
	if err := hub.RenameContext(ctx, proj, "a.txt", "b.txt"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := hub.WriteSession(ctx, id, 0, []byte("v2")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := hub.SyncSession(ctx, id); err != nil {
		t.Fatalf("sync after rename: %v", err)
	}
	stat, err := hub.StatSession(ctx, id)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if stat.Path != "b.txt" {
		t.Fatalf("handle path = %q, want %q", stat.Path, "b.txt")
	}
	if stat.Dirty {
		t.Fatal("sync must clear dirty")
	}
	// A second generation commits through the repinned path without error.
	if _, err := hub.WriteSession(ctx, id, 0, []byte("v3")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	if err := hub.SyncSession(ctx, id); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if got := freshSessionBytes(t, hub, proj, "b.txt"); string(got) != "v3" {
		t.Fatalf("b.txt = %q, want %q", got, "v3")
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Destroy-by-pointer: when a sweep reaps the handle after its commit
// already succeeded and drained, destroy reports success for the durable
// state instead of Stale. The old code re-looked-up by id and returned the
// sweep's Stale error.
func TestDestroySessionByPointerAfterSweep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionw4destroy"
	setupSessionFile(ctx, t, hub, proj, "m2.txt", []byte("data"))

	id := mustOpenSession(ctx, t, hub, proj, "m2.txt", SessionReadWrite)
	sh := hub.sessionHub()
	sh.mu.Lock()
	s, ok := sh.byID[id]
	sh.mu.Unlock()
	if !ok {
		t.Fatal("handle missing from table")
	}
	// Force expiry and sweep while holding no locks the sweeper needs.
	s.mu.Lock()
	s.lastUse = sh.now().Add(-time.Hour)
	s.mu.Unlock()
	sh.mu.Lock()
	sh.sweepExpiredLocked(sh.now())
	if _, still := sh.byID[id]; still {
		sh.mu.Unlock()
		t.Fatal("sweep must reap the expired handle")
	}
	sh.mu.Unlock()
	// The commit already succeeded (nothing was staged); destroy must
	// report success for the reaped handle.
	s.mu.Lock()
	if err := sh.destroySession(id, s); err != nil {
		t.Fatalf("destroy of reaped self must succeed, got %v", err)
	}
	// A genuinely unknown id against a live handle still answers Stale.
	id2 := mustOpenSession(ctx, t, hub, proj, "m2.txt", SessionReadWrite)
	sh.mu.Lock()
	s2, ok := sh.byID[id2]
	sh.mu.Unlock()
	if !ok {
		t.Fatal("second handle missing from table")
	}
	s2.mu.Lock()
	if err := sh.destroySession("ffffffffffffffffffffffffffffffff", s2); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("destroy of unknown id must be stale, got %v", err)
	}
	if err := hub.CloseSession(ctx, id2); err != nil {
		t.Fatalf("close survivor: %v", err)
	}
}

// A linked fan-out that fails mid-loop must be resumable: names the handle
// already published republish idempotently instead of conflicting with
// themselves. The old code re-entered pre-validation, saw its own prior
// publish as AlreadyExists, and wedged the handle until Relink.
func TestLinkedCommitRetryCompletesPartialFanout(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionw4linkedretry"

	ctx, id := multilinkSetup(t, hub, proj, []byte("linked-payload"))
	if err := hub.LinkSession(ctx, id, "docs/a.txt"); err != nil {
		t.Fatalf("link a: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "docs/b.txt"); err != nil {
		t.Fatalf("link b: %v", err)
	}
	// Simulate the handle's own prior partial publish exactly the way the
	// fan-out loop performs it, then record it as this handle's publish
	// (what the fixed loop records per name before moving on).
	sh := hub.sessionHub()
	sh.mu.Lock()
	s, ok := sh.byID[id]
	sh.mu.Unlock()
	if !ok {
		t.Fatal("handle missing from table")
	}
	s.mu.Lock()
	tmpName := s.tmpName
	s.mu.Unlock()
	if _, err := hub.UploadFileContext(ctx, proj, "docs/a.txt", tmpName); err != nil {
		t.Fatalf("partial publish of a: %v", err)
	}
	if err := hub.DrainProjectContext(ctx, proj); err != nil {
		t.Fatalf("drain partial: %v", err)
	}
	s.mu.Lock()
	if s.published == nil {
		s.published = make(map[string]bool)
	}
	s.published["docs/a.txt"] = true
	s.mu.Unlock()
	// The retry must complete the remaining name, not wedge on its own
	// prior publish.
	if err := hub.SyncSession(ctx, id); err != nil {
		t.Fatalf("retry after partial fan-out: %v", err)
	}
	if got := multilinkFreshBytes(t, backend, proj, "docs/a.txt"); string(got) != "linked-payload" {
		t.Fatalf("a.txt = %q, want %q", got, "linked-payload")
	}
	if got := multilinkFreshBytes(t, backend, proj, "docs/b.txt"); string(got) != "linked-payload" {
		t.Fatalf("b.txt = %q, want %q", got, "linked-payload")
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A name taken by ANOTHER writer still fails the whole operation: the
// idempotent retry above must not weaken the all-or-nothing contract for
// foreign takes.
func TestLinkedCommitForeignTakeStillAborts(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionw4linkedforeign"

	ctx, id := multilinkSetup(t, hub, proj, []byte("staged-bytes"))
	if err := hub.LinkSession(ctx, id, "docs/a.txt"); err != nil {
		t.Fatalf("link a: %v", err)
	}
	if err := hub.LinkSession(ctx, id, "docs/b.txt"); err != nil {
		t.Fatalf("link b: %v", err)
	}
	setupSessionFile(ctx, t, hub, proj, "docs/b.txt", []byte("rival"))
	if err := hub.SyncSession(ctx, id); !errors.Is(err, shfs.ErrAlreadyExists) {
		t.Fatalf("sync over a rival-taken name must fail with AlreadyExists, got %v", err)
	}
}

// Quarantine destinations embed the full handle id: two temps quarantined
// under the same id must both survive. The old 48-bit truncated name let
// the second rename silently overwrite the first.
func TestQuarantineFullIDNoCollision(t *testing.T) {
	t.Parallel()
	base, err := spoolBase()
	if err != nil {
		t.Fatalf("spool base: %v", err)
	}
	qdir := filepath.Join(base, "session-quarantine")
	mktemp := func(content string) string {
		t.Helper()
		f, err := os.CreateTemp(base, "session-*")
		if err != nil {
			t.Fatalf("temp: %v", err)
		}
		if _, err := f.WriteString(content); err != nil {
			t.Fatalf("write temp: %v", err)
		}
		name := f.Name()
		if err := f.Close(); err != nil {
			t.Fatalf("close temp: %v", err)
		}
		return name
	}
	first, second := mktemp("first"), mktemp("second")
	const id = "a4w4quarantinetest00000000000001"
	t.Cleanup(func() {
		for _, e := range []string{
			filepath.Join(qdir, "session-"+id+".staged"),
			filepath.Join(qdir, "session-"+id+".2.staged"),
		} {
			_ = os.Remove(e)
		}
	})
	if err := quarantineSessionTemp(first, id); err != nil {
		t.Fatalf("quarantine first: %v", err)
	}
	if err := quarantineSessionTemp(second, id); err != nil {
		t.Fatalf("quarantine second: %v", err)
	}
	want := filepath.Join(qdir, "session-"+id+".staged")
	got, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("read %s: %v", want, err)
	}
	if string(got) != "first" {
		t.Fatalf("first quarantine = %q, want %q", got, "first")
	}
	entries, err := os.ReadDir(qdir)
	if err != nil {
		t.Fatalf("list quarantine: %v", err)
	}
	found := 0
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(qdir, e.Name()))
		if err != nil {
			continue
		}
		if string(data) == "second" {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("second quarantine must survive exactly once, found %d", found)
	}
}

// SessionReadWrite renders "rw" while the legacy ReadOnly+WriteOnly
// combination renders "r+w": debug output must distinguish them.
func TestOpenModeStringDistinguishesCombos(t *testing.T) {
	t.Parallel()
	cases := map[OpenMode]string{
		SessionReadOnly:                    "r",
		SessionWriteOnly:                   "w",
		SessionReadWrite:                   "rw",
		SessionReadOnly | SessionWriteOnly: "r+w",
		0:                                  "none",
		SessionReadWrite | SessionCreate:   "rw+create",
	}
	for mode, want := range cases {
		if got := mode.String(); got != want {
			t.Fatalf("mode %d String() = %q, want %q", int(mode), got, want)
		}
	}
}

// A failing chunks-scope prune reports nil with the error, like every
// other scope: callers branching on result != nil must see one shape.
func TestPruneChunksErrorReturnsNil(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "projectsessionw4prunecontract"
	// A live session refuses the compaction: the scan succeeds, the
	// collect fails loud.
	id := mustOpenSession(ctx, t, hub, proj, "", SessionReadWrite)
	defer func() { _ = hub.CloseSession(ctx, id) }()
	res, err := hub.Prune(ctx, proj, PruneChunks, 1, false)
	if err == nil {
		t.Fatal("compaction under a live session must fail")
	}
	if res != nil {
		t.Fatalf("failed prune must return nil result, got %+v", res)
	}
}
