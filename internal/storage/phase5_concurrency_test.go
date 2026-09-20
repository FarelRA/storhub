package storage

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// F2: concurrent appends during a rewrite must not be lost.
func TestPhase5JournalRewriteKeepsConcurrentAppends(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	cfg.JournalDir = t.TempDir()
	hub := backend.newClient(t, cfg)
	project := "project-phase5-journal"
	survivors := []Op{
		{Seq: 1, Type: OpPutFile, Paths: []string{"a.txt"}, Cause: "upload", Timestamp: 100},
	}
	// Seed the journal file with survivors via a first rewrite.
	hub.journalRewrite(project, survivors)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		hub.journalRewrite(project, survivors)
	}()
	// Appender races the rewrite I/O; with the lock held end to end it
	// blocks then lands in the new file instead of the unlinked old one.
	time.Sleep(10 * time.Millisecond)
	hub.journalAppend(project, Op{Seq: 2, Type: OpPutFile, Paths: []string{"b.txt"}, Cause: "upload", Timestamp: 200})
	wg.Wait()
	// Flush group-commit timer so the append is on disk before reading.
	hub.flushJournals()
	// Give the timer-driven sync a moment when racing.
	time.Sleep(150 * time.Millisecond)
	ops := hub.journalRead(project)
	foundA, foundB := false, false
	for _, op := range ops {
		if len(op.Paths) > 0 && op.Paths[0] == "a.txt" {
			foundA = true
		}
		if len(op.Paths) > 0 && op.Paths[0] == "b.txt" {
			foundB = true
		}
	}
	if !foundA || !foundB {
		t.Fatalf("rewrite lost concurrent append: foundA=%v foundB=%v ops=%v", foundA, foundB, ops)
	}
}

// F3: cold hydrate must initialize the rebase baseline.
func TestPhase5HydrateSetsBaseTree(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-phase5-basetree"
	seed := writeTempFile(t, t.TempDir(), "seed.bin", []byte("hi"))
	if _, err := hub.UploadFileContext(ctx, proj, "a.txt", seed); err != nil {
		t.Fatalf("upload: %v", err)
	}
	pm := hub.getOrCreateProjectMeta(proj)
	pm.mu.RLock()
	baseNil := pm.baseTree == nil
	hydrated := pm.hydrated
	pm.mu.RUnlock()
	if !hydrated {
		t.Fatalf("project must be hydrated after mutation")
	}
	if baseNil {
		t.Fatalf("baseTree must be non-nil after hydrate (nil degrades first conflict)")
	}
}

// F5: one slow commit must not stall other sessions.
func TestPhase5SlowCommitDoesNotStallOtherSessions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	proj := "project-phase5-sessions"
	setupSessionFile(ctx, t, hub, proj, "data.txt", []byte("hello"))

	idA := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, idA, 5, []byte(" slow")); err != nil {
		t.Fatalf("write A: %v", err)
	}
	idB := mustOpenSession(ctx, t, hub, proj, "data.txt", SessionReadOnly)

	// Gate commit PUTs (metadata contents writes); reads use GET/CDN and pass.
	gate := make(chan struct{})
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if (r.Method == http.MethodPut || r.Method == http.MethodPost) && strings.Contains(r.URL.Path, "/contents/") {
			<-gate
		}
		return false
	})
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- hub.CloseSession(ctx, idA)
	}()
	// Let the close enter the gated PUT.
	time.Sleep(200 * time.Millisecond)
	// Other sessions must proceed while A is parked in network I/O.
	readDone := make(chan struct{})
	var readErr error
	var readBytes []byte
	go func() {
		defer close(readDone)
		readBytes, readErr = hub.ReadSession(ctx, idB, 0, 64)
	}()
	select {
	case <-readDone:
		if readErr != nil {
			t.Fatalf("read B during slow commit: %v", readErr)
		}
		if string(readBytes) != "hello" {
			t.Fatalf("read B = %q want pinned %q", readBytes, "hello")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("slow commit stalled another session past 5s")
	}
	statDone := make(chan struct{})
	go func() {
		defer close(statDone)
		_, _ = hub.StatSession(ctx, idB)
	}()
	select {
	case <-statDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("slow commit stalled stat past 5s")
	}
	close(gate)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close A after ungate: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("close A did not finish after ungate")
	}
}
