package fusefs

// Phase E3 (F5): explicit journal flush wired into the FUSE
// fsync/Flush/Release drain sequence. Proven by event order on a
// recording hub, never by timing: commit, then journal-flush, then
// drain, all synchronously before the syscall returns (no 100ms tail).

import (
	"context"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// flushProbeHub is a drainProbeHub that additionally exposes the optional
// FlushJournals durability hook and records it in the shared event log.
type flushProbeHub struct {
	*drainProbeHub
}

func (f *flushProbeHub) FlushJournals() {
	f.record("journal-flush")
}

// flushFixture mirrors syncDrainFixture but serves a hub exposing
// FlushJournals, so the F5 wiring has something to call.
func flushFixture(t *testing.T, flags uint32) (*Filesystem, *flushProbeHub, *storhubHandle) {
	t.Helper()
	probe := &flushProbeHub{drainProbeHub: &drainProbeHub{stubHub: &stubHub{chunkSize: 64}}}
	probe.patchRanges = func(_ []shfs.RangeEdit) (*meta.FileMeta, error) {
		probe.record("patch")
		return &meta.FileMeta{}, nil
	}
	probe.statPath = func(_ context.Context, _, target string) (*shfs.EntryInfo, error) {
		return &shfs.EntryInfo{Path: target, Inode: 7, Size: 16, Mode: 0o644, UID: 1000, GID: 1000, NLink: 1}, nil
	}
	probe.loadReadonly = func(_ context.Context, _ string) (*meta.RepoMetadata, string, error) {
		repo := meta.NewRepoMetadata("demo")
		repo.RebuildIndexes()
		return repo, "sha", nil
	}
	fsys, err := New(probe, "demo", Options{CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new filesystem: %v", err)
	}
	t.Cleanup(func() { _ = fsys.Close() })
	h, err := fsys.newHandle(context.Background(), 7, "sync.bin", flags, &writeBootstrap{baseSize: 16})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	return fsys, probe, h
}

func assertCommitFlushDrainOrder(t *testing.T, probe *flushProbeHub) {
	t.Helper()
	events := probe.eventLog()
	patch, flush, drain := indexOf(events, "patch"), indexOf(events, "journal-flush"), indexOf(events, "drain")
	if patch < 0 || flush < 0 || drain < 0 {
		t.Fatalf("durability path must commit, flush journals, then drain; got events %v", events)
	}
	if patch < flush && flush < drain {
		return
	}
	t.Fatalf("journal flush belongs between commit and drain; got events %v", events)
}

func TestFsyncFlushesJournalBeforeDrain(t *testing.T) {
	t.Parallel()
	_, probe, h := flushFixture(t, syscall.O_WRONLY)
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := h.Fsync(context.Background(), 0); errno != 0 {
		t.Fatalf("fsync: %v", errno)
	}
	assertCommitFlushDrainOrder(t, probe)
}

func TestFlushFlushesJournalBeforeDrain(t *testing.T) {
	t.Parallel()
	_, probe, h := flushFixture(t, syscall.O_WRONLY)
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := h.Flush(context.Background()); errno != 0 {
		t.Fatalf("flush: %v", errno)
	}
	assertCommitFlushDrainOrder(t, probe)
}

func TestReleaseFlushesJournalBeforeDrain(t *testing.T) {
	t.Parallel()
	_, probe, h := flushFixture(t, syscall.O_WRONLY)
	if _, errno := h.Write(context.Background(), []byte("hello"), 16); errno != 0 {
		t.Fatalf("write: %v", errno)
	}
	if errno := h.Release(context.Background()); errno != 0 {
		t.Fatalf("release: %v", errno)
	}
	assertCommitFlushDrainOrder(t, probe)
}
