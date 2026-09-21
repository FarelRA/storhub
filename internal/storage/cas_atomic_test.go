package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// TestRevisionGateRacingCASPinsAtomicity proves the compare-and-swap gate
// is atomic end to end: N goroutines racing with the SAME expected-revision
// token yield exactly one winner. The losers must fail synchronously with
// ErrPreconditionFailed and their bytes must never land (no TOCTOU between
// the revision check and the write, no partial application).
func TestRevisionGateRacingCASPinsAtomicity(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	cfg := Config{
		ChunkSize:         32 << 20,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	}
	hub := backend.newClient(t, cfg)
	ctx := context.Background()
	project := "projectcasrace"

	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	seed := writeTempFile(t, t.TempDir(), "seed.txt", []byte("seedseed"))
	if _, err := hub.UploadFileContext(ctx, project, "docs/race.txt", seed); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush seed: %v", err)
	}
	rev, err := hub.RevisionContext(ctx, project)
	if err != nil || rev == "" {
		t.Fatalf("revision: %q %v", rev, err)
	}

	const racers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	type outcome struct {
		idx int
		err error
	}
	outcomes := make([]outcome, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			// Same offset, same length, distinct bytes: without an
			// atomic gate every racer lands (last-writer-wins) and
			// no size guard trips to mask the TOCTOU.
			payload := []byte(fmt.Sprintf("racer-%02d", idx))
			_, err := hub.WriteFileAtContext(ctx, project, "docs/race.txt", 0, payload, shfs.WithExpectedRevision(rev))
			outcomes[idx] = outcome{idx: idx, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	var winnerPayload string
	for _, o := range outcomes {
		if o.err == nil {
			winners++
			winnerPayload = fmt.Sprintf("racer-%02d", o.idx)
			continue
		}
		if !errors.Is(o.err, shfs.ErrPreconditionFailed) {
			t.Fatalf("racer %d failed with %v, want nil or ErrPreconditionFailed", o.idx, o.err)
		}
	}
	if winners != 1 {
		t.Fatalf("racing CAS with one token yielded %d winners, want exactly 1", winners)
	}

	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush winner: %v", err)
	}
	got, err := hub.ReadFileAtContext(ctx, project, "docs/race.txt", 0, 1<<20)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	// Same-offset overwrite: the file holds exactly the winner's bytes.
	if string(got) != winnerPayload {
		t.Fatalf("content = %q, want %q (loser bytes must never land)", got, winnerPayload)
	}
}

// TestChunkPruneWireSpellingPinsOneWord pins the one-word Op wire value
// with no legacy spellings: pre-alpha cuts are clean.
func TestChunkPruneWireSpellingPinsOneWord(t *testing.T) {
	t.Parallel()
	if string(OpChunkPrune) != "chunkprune" {
		t.Fatalf("OpChunkPrune = %q, want %q", string(OpChunkPrune), "chunkprune")
	}
	raw, err := json.Marshal(Op{Type: OpChunkPrune, Cause: "test"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Op
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Type != OpChunkPrune {
		t.Fatalf("round trip type = %q, want %q", decoded.Type, OpChunkPrune)
	}
	var bad OpType
	if err := bad.UnmarshalText([]byte("chunk-prune")); err == nil {
		t.Fatalf("hyphenated spelling must not parse")
	}
	if err := bad.UnmarshalText([]byte("chunkprune ")); err == nil {
		t.Fatalf("padded spelling must not parse")
	}
}
