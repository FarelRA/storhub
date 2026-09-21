package storage

// Prod-side atime gap tests (item 2): clone counts as a read for atime on
// the source. The degraded-mode half rewrites TestDegradedAtimeSkipsSilently
// in degraded_test.go to pin queueing instead of skipping.
// RED-first: the clone test fails while CloneRange leaves source atime alone.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// CloneRange must refresh the source atime like a read does, while leaving
// source mtime/ctime alone. The destination keeps its own atime.
func TestCloneCountsAsReadForSourceAtime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	cfg := smallTransferTestConfig()
	cfg.AtimePolicy = "strictatime"
	// Atomic clock installed before the client exists: swapping the
	// config func after background commit loops start races the
	// reader in commitProjectMetadata. Advance via store, never by
	// reassigning the func.
	var nowUnix atomic.Int64
	nowUnix.Store(time.Unix(1700000000, 0).UTC().Unix())
	cfg.Now = func() time.Time { return time.Unix(nowUnix.Load(), 0).UTC() }
	hub := backend.newClient(t, cfg)
	project := "project-clone-source-atime"

	seed := writeTempFile(t, t.TempDir(), "src.bin", []byte("0123456789ABCDEF"))
	if _, err := hub.UploadFileContext(ctx, project, "src.bin", seed); err != nil {
		t.Fatalf("upload src: %v", err)
	}
	if err := hub.DrainProjectContext(ctx, project); err != nil {
		t.Fatalf("drain src: %v", err)
	}
	before, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load before: %v", err)
	}
	srcBefore := before.FindFile("src.bin")
	if srcBefore == nil {
		t.Fatal("src.bin missing after seed")
	}

	// Advance the hub clock so the atime stamp is observably new.
	later := time.Unix(1700003600, 0).UTC()
	nowUnix.Store(later.Unix())
	if _, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst.bin", 0, 16); err != nil {
		t.Fatalf("clone: %v", err)
	}
	if err := hub.FlushMetadata(ctx); err != nil {
		t.Fatalf("flush clone atime: %v", err)
	}

	observer := backend.newClient(t, smallTransferTestConfig())
	after, _, err := observer.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		t.Fatalf("fresh load: %v", err)
	}
	src := after.FindFile("src.bin")
	if src == nil {
		t.Fatal("src.bin missing after clone")
	}
	if src.AccessedAt != later.UnixNano() {
		t.Fatalf("clone must refresh source atime to %d, got %d", later.UnixNano(), src.AccessedAt)
	}
	if src.ModifiedAt != srcBefore.ModifiedAt || src.ChangedAt != srcBefore.ChangedAt {
		t.Fatalf("clone must not move source mtime/ctime: before %+v after %+v", srcBefore, src)
	}
	dst := after.FindFile("dst.bin")
	if dst == nil {
		t.Fatal("dst.bin missing after clone")
	}
}
