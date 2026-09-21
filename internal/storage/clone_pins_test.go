package storage

// Clone-vs-overwrite and clone-of-staged-state pins (Phase 4B edge cases
// from the plan table). The named funcs did not exist in clone_test.go,
// so they live here, reusing the neighboring seams (mockGitHub,
// smallTransferTestConfig, cloneSeedFile, setupSessionFile,
// mustOpenSession) and the errCh style of the concurrent conformance
// scenarios: no t.Fatalf from worker goroutines.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestCloneVsOverwrite races clones against full-file overwrites. A clone
// is one atomic transaction over a pre-operation snapshot: it either lands
// byte-exact on a committed state (pre- or post-overwrite) or is rejected
// loudly for a concurrent change. Torn or short results are never
// acceptable, and a rejected clone commits nothing.
func TestCloneVsOverwrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectclonevsoverwrite"

	const size = 32
	before := bytes.Repeat([]byte("A"), size)
	after := bytes.Repeat([]byte("B"), size)
	cloneSeedFile(t, hub, project, "src.bin", before)

	// Pre-commit ordering: clone first, then overwrite. The clone carries
	// the pre-overwrite bytes; the overwrite leaves it alone.
	if _, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst-pre.bin", 0, size); err != nil {
		t.Fatalf("clone before overwrite: %v", err)
	}
	overInput := writeTempFile(t, t.TempDir(), "after.bin", after)
	if _, err := hub.ReplaceFileContext(ctx, project, "src.bin", overInput); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if got := cloneFileBytes(ctx, t, hub, project, "dst-pre.bin"); !bytes.Equal(got, before) {
		t.Fatalf("pre-overwrite clone = %q, want %q", got, before)
	}
	// Post-commit ordering: a clone after the overwrite sees the new bytes.
	if _, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst-post.bin", 0, size); err != nil {
		t.Fatalf("clone after overwrite: %v", err)
	}
	if got := cloneFileBytes(ctx, t, hub, project, "dst-post.bin"); !bytes.Equal(got, after) {
		t.Fatalf("post-overwrite clone = %q, want %q", got, after)
	}

	// Concurrent hammer: overwriters cycle a fixed payload set while
	// cloners copy to unique destinations. Payloads stay one size so the
	// clone range is always valid; sizes never change mid-race.
	writerPayloads := [][]byte{
		bytes.Repeat([]byte("C"), size),
		bytes.Repeat([]byte("D"), size),
		bytes.Repeat([]byte("E"), size),
		bytes.Repeat([]byte("F"), size),
	}
	inputs := make([]string, len(writerPayloads))
	for i, p := range writerPayloads {
		inputs[i] = writeTempFile(t, t.TempDir(), fmt.Sprintf("race-payload-%d.bin", i), p)
	}
	known := map[string]bool{string(before): true, string(after): true}
	for _, p := range writerPayloads {
		known[string(p)] = true
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 128)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 8; i++ {
				idx := (w + i) % len(inputs)
				if _, err := hub.ReplaceFileContext(ctx, project, "src.bin", inputs[idx]); err != nil {
					errCh <- fmt.Errorf("overwrite w%d i%d: %v", w, i, err)
					return
				}
			}
		}(w)
	}
	for c := 0; c < 16; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			dst := fmt.Sprintf("dst-race-%d.bin", c)
			if _, err := hub.CloneRange(ctx, project, "src.bin", 0, dst, 0, size); err != nil {
				// Loud loser: the snapshot moved under the clone, so
				// the transaction aborted instead of cloning stale
				// bytes. Nothing may have committed.
				if strings.Contains(err.Error(), "changed concurrently") {
					repo, _, lerr := hub.loadRepoMetadataReadonly(ctx, project)
					if lerr != nil {
						errCh <- fmt.Errorf("clone %d reject re-read: %v", c, lerr)
						return
					}
					if repo.FindFile(dst) != nil {
						errCh <- fmt.Errorf("clone %d rejected yet %s exists", c, dst)
					}
					return
				}
				errCh <- fmt.Errorf("clone %d: %v", c, err)
				return
			}
			repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
			if err != nil {
				errCh <- fmt.Errorf("clone %d re-read: %v", c, err)
				return
			}
			file := repo.FindFile(dst)
			if file == nil {
				errCh <- fmt.Errorf("clone %d succeeded but %s is missing", c, dst)
				return
			}
			got, err := hub.ReadFileAtContext(ctx, project, dst, 0, file.Size)
			if err != nil {
				errCh <- fmt.Errorf("clone %d read back: %v", c, err)
				return
			}
			if len(got) != size {
				errCh <- fmt.Errorf("clone %d tore: size %d, want %d", c, len(got), size)
				return
			}
			if !known[string(got)] {
				errCh <- fmt.Errorf("clone %d tore: %q matches no committed payload", c, got)
			}
		}(c)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if got := cloneFileBytes(ctx, t, hub, project, "src.bin"); !known[string(got)] {
		t.Fatalf("final source = %q, matches no committed payload", got)
	}
}

// TestCloneOfOverlaySeesCommitted pins the committed-state-only rule for
// clones: bytes staged in a stateful session (the storage-level analogue
// of the FUSE overlay, invisible to everyone else until close) must never
// leak into a clone. Only after the session commits do the bytes become
// clonable.
func TestCloneOfOverlaySeesCommitted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectcloneoverlaycommitted"

	committed := bytes.Repeat([]byte("C"), 32)
	staged := bytes.Repeat([]byte("S"), 32)
	setupSessionFile(ctx, t, hub, project, "src.bin", committed)

	id := mustOpenSession(ctx, t, hub, project, "src.bin", SessionReadWrite)
	if _, err := hub.WriteSession(ctx, id, 0, staged); err != nil {
		t.Fatalf("stage session write: %v", err)
	}
	if _, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst.bin", 0, int64(len(committed))); err != nil {
		t.Fatalf("clone over staged session: %v", err)
	}
	if got := cloneFileBytes(ctx, t, hub, project, "dst.bin"); !bytes.Equal(got, committed) {
		t.Fatalf("clone saw staged bytes: got %q, want committed %q", got, committed)
	}
	if err := hub.CloseSession(ctx, id); err != nil {
		t.Fatalf("close session: %v", err)
	}
	if _, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst2.bin", 0, int64(len(staged))); err != nil {
		t.Fatalf("clone after session commit: %v", err)
	}
	if got := cloneFileBytes(ctx, t, hub, project, "dst2.bin"); !bytes.Equal(got, staged) {
		t.Fatalf("post-commit clone = %q, want %q", got, staged)
	}
}
