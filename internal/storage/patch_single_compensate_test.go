package storage

import (
	"context"
	"testing"
)

// TestSingleEditCompensationSparesReusedChunks pins the single-edit
// compensation invariant: buildPatchedChunksFresh's assembled playlist mixes
// reused committed chunks with fresh uploads, and only the fresh list may
// ever be compensated. Compensating the mixed playlist deletes live assets
// (same class as the production append 404 fixed for the batch builders).
func TestSingleEditCompensationSparesReusedChunks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectsinglecompensatesparesreused"

	seed := writeTempFile(t, t.TempDir(), "base.bin", []byte("0123456789ABCDEF"))
	if _, err := hub.UploadFileContext(ctx, project, "f.bin", seed); err != nil {
		t.Fatalf("upload base: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush base: %v", err)
	}
	repoMeta, _, err := hub.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	file := repoMeta.FindFile("f.bin")
	if file == nil {
		t.Fatal("seed file missing")
	}
	committed := map[int64]bool{}
	for _, cid := range file.Chunks {
		if ci, ok := repoMeta.Chunks()[cid]; ok {
			committed[ci.AssetID] = true
		}
	}
	if len(committed) == 0 {
		t.Fatal("seed file has no committed assets")
	}
	// Single-byte replacement at offset 0: the playlist must reference the
	// committed chunks (prefix/suffix views), but the compensatable set
	// must hold ONLY the fresh upload.
	assembled, fresh, _, err := hub.buildPatchedChunksFresh(ctx, project, repoMeta, *file, "f.bin", 0, 1, []byte("Z"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(assembled) == 0 {
		t.Fatal("playlist must reference existing plus fresh chunks")
	}
	if len(fresh) == 0 {
		t.Fatal("compensatable set must hold the fresh upload")
	}
	for _, c := range fresh {
		if committed[c.AssetID] {
			t.Fatalf("compensatable set references committed asset %d", c.AssetID)
		}
	}
	// The safety property itself: compensating the fresh set leaves the
	// committed assets downloadable.
	hub.compensateDeleteAssets(ctx, project, fresh)
	data, err := hub.ReadFileAtContext(ctx, project, "f.bin", 0, file.Size)
	if err != nil {
		t.Fatalf("committed content unreadable after compensating fresh uploads: %v", err)
	}
	if string(data) != "0123456789ABCDEF" {
		t.Fatalf("committed content corrupted: %q", data)
	}
}

// TestSingleEditLoserCompensationPreservesLiveContent exercises a real
// patchFileWithMetadataContext failure site end to end: a stale-snapshot
// patch loses the concurrent-size race, and the loser's compensation must
// delete only its own fresh upload, leaving the winner's content readable.
// With the pre-fix mixed-playlist compensation this read-back 404s.
func TestSingleEditLoserCompensationPreservesLiveContent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectsingleloserpreserveslive"

	seed := writeTempFile(t, t.TempDir(), "base.bin", []byte("0123456789ABCDEF"))
	if _, err := hub.UploadFileContext(ctx, project, "f.bin", seed); err != nil {
		t.Fatalf("upload base: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush base: %v", err)
	}
	repoMeta, _, err := hub.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	stale := repoMeta.FindFile("f.bin")
	if stale == nil {
		t.Fatal("seed file missing")
	}
	staleCopy := stale.Clone()
	// Winner lands first: the stale snapshot is now out of date.
	if _, err := hub.AppendFileContext(ctx, project, "f.bin", []byte("TAIL")); err != nil {
		t.Fatalf("winner append: %v", err)
	}
	// Loser builds on the stale snapshot and must lose the size race.
	if _, err := hub.patchFileWithMetadataContext(ctx, project, "f.bin", repoMeta, &staleCopy, 0, 1, []byte("Z")); err == nil {
		t.Fatal("stale patch must lose the concurrent-size race")
	}
	data, err := hub.ReadFileAtContext(ctx, project, "f.bin", 0, 20)
	if err != nil {
		t.Fatalf("winner content unreadable after loser compensation: %v", err)
	}
	if string(data) != "0123456789ABCDEFTAIL" {
		t.Fatalf("winner content corrupted: %q", data)
	}
}

// TestSingleEditCompensatingPlaylistOrphansLiveData is the sabotage proof
// for the invariant above: compensating the mixed playlist (the pre-fix
// behavior of the six patchFileWithMetadataContext failure sites) deletes a
// live asset and the committed content stops reading back.
func TestSingleEditCompensatingPlaylistOrphansLiveData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectsinglecompensateplaylistkillslive"

	seed := writeTempFile(t, t.TempDir(), "base.bin", []byte("0123456789ABCDEF"))
	if _, err := hub.UploadFileContext(ctx, project, "f.bin", seed); err != nil {
		t.Fatalf("upload base: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush base: %v", err)
	}
	repoMeta, _, err := hub.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	file := repoMeta.FindFile("f.bin")
	if file == nil {
		t.Fatal("seed file missing")
	}
	assembled, _, _, err := hub.buildPatchedChunksFresh(ctx, project, repoMeta, *file, "f.bin", 0, 1, []byte("Z"))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Sabotage the compensation target: the mixed playlist must contain a
	// live asset, and deleting it must break the committed read-back.
	hub.compensateDeleteAssets(ctx, project, assembled)
	if _, err := hub.ReadFileAtContext(ctx, project, "f.bin", 0, file.Size); err == nil {
		t.Fatal("compensating the mixed playlist must break committed read-back (sabotage proof)")
	}
}
