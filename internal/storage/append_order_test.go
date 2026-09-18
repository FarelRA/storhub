package storage

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// TestConcurrentAppendByteExactness pins the append concurrency contract:
// each append patches at the size observed before its upload, so under
// concurrency at most the non-overlapping attempts win and every loser is
// rejected with an explicit "changed concurrently" error instead of
// interleaving bytes. The final image must therefore consist solely of
// whole winner payloads, each in one contiguous run: whole appends,
// never interleaved bytes. Which workers win is unspecified.
func TestConcurrentAppendByteExactness(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, singleChunkTestConfig())
	ctx := context.Background()
	const project = "project-append-order"

	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if _, err := hub.CreateFileContext(ctx, project, "docs/log.txt"); err != nil {
		t.Fatalf("create log: %v", err)
	}

	const workers = 8
	const perWorker = 64
	payloads := make([][]byte, workers)
	for i := range payloads {
		payloads[i] = []byte(strings.Repeat(string(rune('A'+i)), perWorker))
	}
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := hub.AppendFileContext(ctx, project, "docs/log.txt", payloads[i])
			errs[i] = err
		}(i)
	}
	wg.Wait()

	won := map[int]bool{}
	for i, err := range errs {
		switch {
		case err == nil:
			won[i] = true
		case strings.Contains(err.Error(), "changed concurrently"):
			// Documented loser path: explicit rejection, no partial write.
		default:
			t.Fatalf("worker %d: unexpected append error: %v", i, err)
		}
	}
	if len(won) == 0 {
		t.Fatal("at least one concurrent append must win")
	}

	stat, err := hub.StatPathContext(ctx, project, "docs/log.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if stat.Size != int64(len(won)*perWorker) {
		t.Fatalf("size: want %d (one payload per winner), got %d", len(won)*perWorker, stat.Size)
	}
	got, err := hub.ReadFileAtContext(ctx, project, "docs/log.txt", 0, stat.Size)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	// Split the image into runs: every run must be exactly one winner's
	// whole payload, and every winner must appear exactly once. Any
	// interleave (or partial winner) breaks the run structure.
	seen := map[int]int{}
	for i := 0; i < len(got); {
		matched := -1
		for w := range won {
			if len(got)-i >= perWorker && string(got[i:i+perWorker]) == string(payloads[w]) {
				matched = w
				break
			}
		}
		if matched < 0 {
			t.Fatalf("image not decomposable into whole payloads at offset %d: %q", i, got[i:min(i+16, len(got))])
		}
		seen[matched]++
		i += perWorker
	}
	for w := range won {
		if seen[w] != 1 {
			t.Fatalf("winner %d appears %d times, want exactly once", w, seen[w])
		}
	}
}
