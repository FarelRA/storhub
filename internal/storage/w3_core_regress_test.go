package storage

// W3 storage-engine-core regression tests: each pins a behavior fix from
// the storage-core audit so a revert fails loudly. Pure unit scope where
// possible (opStack, stackPathKey, journal probe, logOp span, spool
// reaper); hub scope only where the fix lives on the hub (sweep backstop,
// degraded latch).

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// TestSnapshotPruneMergeIsolation pins the commit-snapshot contract through
// the real live mutation path: a prune merge appends through the live op's
// RemovedChunks backing array, and the snapshot taken before the merge must
// not observe it.
func TestSnapshotPruneMergeIsolation(t *testing.T) {
	t.Parallel()
	var st opStack
	rc := make([]int64, 0, 16)
	rc = append(rc, 7, 8)
	st.append(Op{Type: OpChunkPrune, Cause: "w3", Timestamp: 1, RemovedChunks: rc})
	snap := st.snapshot()
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d, want 1", len(snap))
	}
	st.append(Op{Type: OpChunkPrune, Cause: "w3", Timestamp: 2, RemovedChunks: []int64{9}})
	if len(snap[0].RemovedChunks) != 2 || snap[0].RemovedChunks[0] != 7 || snap[0].RemovedChunks[1] != 8 {
		t.Fatalf("snapshot tail changed by live merge: %v", snap[0].RemovedChunks)
	}
	full := snap[0].RemovedChunks[:cap(snap[0].RemovedChunks)]
	for _, v := range full[2:] {
		if v == 9 {
			t.Fatalf("live prune merge wrote through shared backing into the snapshot (cap %d)", cap(snap[0].RemovedChunks))
		}
	}
}

// TestSnapshotDeepCopiesPayloads pins per-field snapshot independence: an
// in-place edit of any live reference-backed payload (the hazard class
// behind collision remaps reading commit batches outside pm.mu) must not
// reach the snapshot.
func TestSnapshotDeepCopiesPayloads(t *testing.T) {
	t.Parallel()
	var st opStack
	f := FileMeta{Size: 3, Mode: 0o644, Chunks: []int64{11, 12}, XAttrs: metadata.XAttrMap{"k": []byte("v")}}
	d := DirMeta{Mode: 0o755, XAttrs: metadata.XAttrMap{"dk": []byte("dv")}}
	st.append(Op{
		Type: OpPutFile, Paths: []string{"a"}, Cause: "w3", Timestamp: 1,
		File: &f, Chunks: map[int64]ChunkInfo{11: {Size: 3}},
		Members: []string{"m1"}, Release: &ReleaseRef{AssetCount: 1},
		RemovedChunks: []int64{5},
	})
	st.append(Op{Type: OpMkdir, Paths: []string{"md"}, Cause: "w3", Timestamp: 1, Dir: &d})
	snap := st.snapshot()
	st.ops[0].File.Chunks[0] = 999
	st.ops[0].File.XAttrs["k"][0] = 'X'
	st.ops[0].Chunks[11] = ChunkInfo{Size: 12345}
	st.ops[0].Members[0] = "poisoned"
	st.ops[0].Release.AssetCount = 999
	st.ops[0].Paths[0] = "poisoned"
	st.ops[0].RemovedChunks[0] = 999
	st.ops[1].Dir.XAttrs["dk"][0] = 'X'
	got := snap[0]
	if got.File.Chunks[0] != 11 || string(got.File.XAttrs["k"]) != "v" {
		t.Fatalf("snapshot file payload aliased live edit: %+v", got.File)
	}
	if got.Chunks[11].Size != 3 || got.Members[0] != "m1" || got.Release.AssetCount != 1 {
		t.Fatalf("snapshot map/slice/pointer payload aliased live edit: %+v", got)
	}
	if got.Paths[0] != "a" || got.RemovedChunks[0] != 5 {
		t.Fatalf("snapshot slice payload aliased live edit: %+v", got)
	}
	if string(snap[1].Dir.XAttrs["dk"]) != "dv" {
		t.Fatalf("snapshot dir payload aliased live edit: %+v", snap[1].Dir)
	}
}

// TestStackPathKeyNilPayloads pins the record-less setattr rule: an
// OpSetattr/OpXattr carrying neither File nor Dir asserts nothing and must
// not coalesce with a dir-assert on the same path.
func TestStackPathKeyNilPayloads(t *testing.T) {
	t.Parallel()
	if _, ok := stackPathKey(Op{Type: OpSetattr, Paths: []string{"a"}}); ok {
		t.Fatal("record-less OpSetattr must not produce a byPath key")
	}
	if _, ok := stackPathKey(Op{Type: OpXattr, Paths: []string{"a"}}); ok {
		t.Fatal("record-less OpXattr must not produce a byPath key")
	}
	f := FileMeta{Size: 1}
	d := DirMeta{Mode: 0o755}
	if key, ok := stackPathKey(Op{Type: OpSetattr, Paths: []string{"a"}, File: &f}); !ok || key != "f:a" {
		t.Fatalf("file setattr key = %q, %v; want f:a, true", key, ok)
	}
	if key, ok := stackPathKey(Op{Type: OpSetattr, Paths: []string{"a"}, Dir: &d}); !ok || key != "d:a" {
		t.Fatalf("dir setattr key = %q, %v; want d:a, true", key, ok)
	}
}

// TestPressureSweepPokeBytesOnly pins the sweep backstop's byte-cap leg: a
// dirty project over the byte cap but far under the op-count cap still gets
// a sweep poke from sweepCachesOnce, counted apart from live pokes.
func TestPressureSweepPokeBytesOnly(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := pressureTestHub(t, backend)
	project := "pressure-sweep-bytes"

	pm := hub.getOrCreateProjectMeta(project)
	pm.mu.Lock()
	// One op sized past opStackMaxBytes via RemovedChunks (8 bytes each).
	n := int(opStackMaxBytes/8) + 16
	rc := make([]int64, n)
	for i := range rc {
		rc[i] = int64(i + 1)
	}
	pm.opStack.append(Op{Type: OpChunkPrune, Cause: "pressure-sweep-bytes", Timestamp: 1700000000, RemovedChunks: rc})
	if !pm.opStack.needsForceFlush() {
		pm.mu.Unlock()
		t.Fatal("test setup failed: stack must cross the byte cap")
	}
	if len(pm.opStack.ops) >= maxPendingOpsPerProject {
		pm.mu.Unlock()
		t.Fatal("test setup failed: stack must stay under the op-count cap")
	}
	markProjectDirtyLocked(pm)
	pm.mu.Unlock()

	before := hub.PressureSnapshot()
	hub.sweepCachesOnce()
	after := hub.PressureSnapshot()
	if got := after.SweepRetryPokes - before.SweepRetryPokes; got != 1 {
		t.Fatalf("sweep retry delta = %d, want 1 (byte-cap leg)", got)
	}
	if got := after.ForceRetryPokes - before.ForceRetryPokes; got != 0 {
		t.Fatalf("live force-retry delta = %d, want 0 (byte-cap leg)", got)
	}
}

// TestJournalOverCapLargeLine pins the large-line probe: an off-cadence
// append carrying a large line still stats the file, while a small line
// off-cadence does not.
func TestJournalOverCapLargeLine(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := pressureTestHub(t, backend)
	project := "journal-probe"
	path := hub.journalPath(project)
	if path == "" {
		t.Fatal("test setup failed: JournalDir must be set")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Sparse over-cap file: Stat reports the size with no 64 MiB of I/O.
	if err := f.Truncate(journalMaxBytes + 1); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	_ = f.Close()

	if hub.journalOverCap(project, 1, 0) {
		t.Fatal("small line off-cadence must not probe")
	}
	if !hub.journalOverCap(project, 1, journalProbeLargeLine) {
		t.Fatal("large line off-cadence must probe the cap")
	}
	if !hub.journalOverCap(project, journalRewriteSampleEvery, 0) {
		t.Fatal("cadence sample must still probe")
	}
}

// captureHandler is a level-gated slog.Handler recording every handled
// record for span-contract assertions.
type captureHandler struct {
	mu    sync.Mutex
	level *slog.LevelVar
	recs  []slog.Record
}

func (c *captureHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= c.level.Level()
}

func (c *captureHandler) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, r)
	return nil
}

func (c *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *captureHandler) WithGroup(string) slog.Handler      { return c }

func (c *captureHandler) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.recs))
	for _, r := range c.recs {
		out = append(out, r.Message)
	}
	return out
}

// TestLogOpFinishFailurePassThrough pins the L1 span contract at the
// default level: failures log even when Debug is off, successes stay
// silent (hot-path alloc parity).
func TestLogOpFinishFailurePassThrough(t *testing.T) {
	t.Parallel()
	var level slog.LevelVar
	level.Set(slog.LevelWarn)
	capH := &captureHandler{level: &level}
	h := &StorHub{config: DefaultConfig(), logger: slog.New(capH)}

	started := h.logOpStart("w3-span", "op-under-test")
	h.logOpFinish("w3-span", "op-under-test", started, errors.New("boom"))
	msgs := capH.messages()
	if len(msgs) != 1 || msgs[0] != "op-under-test failed" {
		t.Fatalf("failure must log exactly one Error line, got %q", msgs)
	}
	h.logOpFinish("w3-span", "op-under-test", started, nil)
	if msgs := capH.messages(); len(msgs) != 1 {
		t.Fatalf("success must stay silent above Debug, got %q", msgs)
	}
}

// TestLogOpStartUsesConfigClock pins the injectable clock on op spans: the
// returned start instant must equal the frozen config clock, not wall time.
func TestLogOpStartUsesConfigClock(t *testing.T) {
	t.Parallel()
	frozen := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cfg := DefaultConfig()
	cfg.Now = func() time.Time { return frozen }
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &StorHub{config: cfg, logger: discard}

	if got := h.logOpStart("w3-clock", "op"); !got.Equal(frozen) {
		t.Fatalf("logOpStart = %v, want frozen clock %v", got, frozen)
	}
}

// TestReapSpoolDirAtFrozenClock pins the reaper age boundary under a frozen
// clock: only files older than maxAge go, young live spools survive.
func TestReapSpoolDirAtFrozenClock(t *testing.T) {
	t.Parallel()
	frozen := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	old := filepath.Join(dir, "upload-old")
	young := filepath.Join(dir, "upload-young")
	other := filepath.Join(dir, "notspool")
	for _, p := range []string{old, young, other} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	past := frozen.Add(-2 * time.Hour)
	for _, p := range []string{old, other} {
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatal(err)
		}
	}
	if got := reapSpoolDirAt(nil, dir, time.Hour, frozen); got != 1 {
		t.Fatalf("reaped = %d, want 1 (only the old spool)", got)
	}
	if _, err := os.Stat(young); err != nil {
		t.Fatalf("young spool must survive: %v", err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old spool must be reaped, stat err = %v", err)
	}
}

// TestDegradedLatchIsPerHub pins latch parity after the registry removal:
// latching is per-hub, re-enable clears exactly one hub, and hubs never
// observe each other.
func TestDegradedLatchIsPerHub(t *testing.T) {
	t.Parallel()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := &StorHub{config: DefaultConfig(), logger: discard}
	b := &StorHub{config: DefaultConfig(), logger: discard}

	a.markProjectDegraded("p")
	if !a.isProjectDegraded("p") {
		t.Fatal("hub A must latch its own project")
	}
	if b.isProjectDegraded("p") {
		t.Fatal("hub B must not observe hub A's latch")
	}
	if got := a.DegradedProjects(); !reflect.DeepEqual(got, []string{"p"}) {
		t.Fatalf("DegradedProjects = %q, want [p]", got)
	}
	if err := a.ReEnableProject("p"); err != nil {
		t.Fatal(err)
	}
	if a.isProjectDegraded("p") {
		t.Fatal("re-enable must clear the latch")
	}
	if err := b.ReEnableProject("never-latched"); err != nil {
		t.Fatalf("re-enable of a healthy project must succeed: %v", err)
	}
}

// TestChunkpruneLabelUnify pins the wire==label rule for the prune class:
// the commit summary names the class chunkprune, matching OpChunkPrune.
func TestChunkpruneLabelUnify(t *testing.T) {
	t.Parallel()
	ops := []Op{{Type: OpChunkPrune, Cause: "w3", RemovedChunks: []int64{1}}}
	if got := opSummaryCounts(ops); got != "1 chunkprune" {
		t.Fatalf("summary = %q, want %q", got, "1 chunkprune")
	}
	msg := buildCommitMessage(ops, "")
	if !strings.Contains(msg, "chunkprune 1 chunk records") {
		t.Fatalf("commit body must name chunkprune, got:\n%s", msg)
	}
}
