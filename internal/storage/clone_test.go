package storage

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// countUploads arms the mock intercept to count release-asset uploads
// (POST to the upload host). CloneRange must never trigger one.
func countUploads(backend *mockGitHub) *atomic.Int32 {
	var calls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			calls.Add(1)
		}
		return false
	})
	return &calls
}

func cloneSeedFile(t *testing.T, hub *StorHub, project, path string, data []byte) *FileMeta {
	t.Helper()
	input := writeTempFile(t, t.TempDir(), "seed.bin", data)
	meta, err := hub.UploadFile(project, path, input)
	if err != nil {
		t.Fatalf("seed upload %s: %v", path, err)
	}
	return meta
}

func cloneFileBytes(t *testing.T, hub *StorHub, ctx context.Context, project, path string) []byte {
	t.Helper()
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	file := repo.FindFile(path)
	if file == nil {
		t.Fatalf("file %s not found", path)
	}
	if file.Size == 0 {
		return []byte{}
	}
	data, err := hub.ReadFileAtContext(ctx, project, path, 0, file.Size)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func cloneChunkIDs(ids []int64) map[int64]struct{} {
	set := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func TestCloneRangeWholeFileByteExactFreshIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-whole"

	srcData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd")
	srcMeta := cloneSeedFile(t, hub, project, "src.bin", srcData)
	srcBefore := srcMeta.Clone()
	uploads := countUploads(backend)

	dstMeta, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst.bin", 0, int64(len(srcData)))
	if err != nil {
		t.Fatalf("clone whole file: %v", err)
	}
	if dstMeta.Size != int64(len(srcData)) {
		t.Fatalf("dst size = %d, want %d", dstMeta.Size, len(srcData))
	}
	if got := cloneFileBytes(t, hub, ctx, project, "dst.bin"); !reflect.DeepEqual(got, srcData) {
		t.Fatalf("dst content mismatch: %q", got)
	}
	if got := cloneFileBytes(t, hub, ctx, project, "src.bin"); !reflect.DeepEqual(got, srcData) {
		t.Fatalf("src content changed: %q", got)
	}
	// Fresh record IDs, never shared between files, pointing at the same
	// assets in order.
	for id := range cloneChunkIDs(srcMeta.Chunks) {
		if _, ok := cloneChunkIDs(dstMeta.Chunks)[id]; ok {
			t.Fatalf("chunk record %d shared between source and destination", id)
		}
	}
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	srcAssets := make([]int64, 0, len(srcMeta.Chunks))
	for _, id := range srcMeta.Chunks {
		rec, ok := repo.Chunks()[id]
		if !ok {
			t.Fatalf("src chunk %d missing", id)
		}
		srcAssets = append(srcAssets, rec.AssetID)
	}
	dstAssets := make([]int64, 0, len(dstMeta.Chunks))
	for _, id := range dstMeta.Chunks {
		rec, ok := repo.Chunks()[id]
		if !ok {
			t.Fatalf("dst chunk %d missing", id)
		}
		dstAssets = append(dstAssets, rec.AssetID)
	}
	if !reflect.DeepEqual(dstAssets, srcAssets) {
		t.Fatalf("dst assets %v do not re-link src assets %v", dstAssets, srcAssets)
	}
	// The source entry is fully untouched.
	repo2, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if got := repo2.FindFile("src.bin"); !reflect.DeepEqual(got, &srcBefore) {
		t.Fatalf("source entry changed by clone: %+v vs %+v", got, &srcBefore)
	}
	if uploads.Load() != 0 {
		t.Fatalf("clone uploaded %d assets, want zero", uploads.Load())
	}
}

func TestCloneRangePartialEdgeNarrowing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-edge"

	// Chunk size is 8: [5,27) spans a partial first chunk, two full
	// chunks, and a partial last chunk.
	srcData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUV")
	cloneSeedFile(t, hub, project, "src.bin", srcData)
	uploads := countUploads(backend)

	dstMeta, err := hub.CloneRange(ctx, project, "src.bin", 5, "dst.bin", 0, 22)
	if err != nil {
		t.Fatalf("clone partial range: %v", err)
	}
	if dstMeta.Size != 22 {
		t.Fatalf("dst size = %d, want 22", dstMeta.Size)
	}
	want := srcData[5:27]
	if got := cloneFileBytes(t, hub, ctx, project, "dst.bin"); !reflect.DeepEqual(got, want) {
		t.Fatalf("narrowed clone mismatch: got %q want %q", got, want)
	}
	if len(dstMeta.Chunks) != 4 {
		t.Fatalf("expected 4 narrowed records, got %d", len(dstMeta.Chunks))
	}
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	var offs []int64
	for _, id := range dstMeta.Chunks {
		rec := repo.Chunks()[id]
		offs = append(offs, rec.Offset)
	}
	wantOffs := []int64{0, 3, 11, 19}
	if !reflect.DeepEqual(offs, wantOffs) {
		t.Fatalf("dst record offsets %v, want %v", offs, wantOffs)
	}
	if uploads.Load() != 0 {
		t.Fatalf("clone uploaded %d assets, want zero", uploads.Load())
	}
}

func TestCloneRangeIntraFileOverlapForward(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-fwd"

	srcData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUV")
	cloneSeedFile(t, hub, project, "f.bin", srcData)

	// Forward overlap: destination starts inside the source range.
	if _, err := hub.CloneRange(ctx, project, "f.bin", 0, "f.bin", 8, 16); err != nil {
		t.Fatalf("forward overlap clone: %v", err)
	}
	model := append([]byte(nil), srcData...)
	snap := append([]byte(nil), model...)
	copy(model[8:24], snap[0:16])
	if got := cloneFileBytes(t, hub, ctx, project, "f.bin"); !reflect.DeepEqual(got, model) {
		t.Fatalf("forward overlap mismatch: got %q want %q", got, model)
	}
}

func TestCloneRangeIntraFileOverlapBackward(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-bwd"

	srcData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUV")
	cloneSeedFile(t, hub, project, "f.bin", srcData)

	// Backward overlap: destination starts before the source range.
	if _, err := hub.CloneRange(ctx, project, "f.bin", 8, "f.bin", 0, 16); err != nil {
		t.Fatalf("backward overlap clone: %v", err)
	}
	model := append([]byte(nil), srcData...)
	snap := append([]byte(nil), model...)
	copy(model[0:16], snap[8:24])
	if got := cloneFileBytes(t, hub, ctx, project, "f.bin"); !reflect.DeepEqual(got, model) {
		t.Fatalf("backward overlap mismatch: got %q want %q", got, model)
	}
}

func TestCloneRangeRejectsBeyondEOF(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-eof"

	cloneSeedFile(t, hub, project, "src.bin", []byte("0123456789ABCDEF"))

	cases := []struct {
		name   string
		srcOff int64
		length int64
	}{
		{"range past EOF", 10, 10},
		{"offset past EOF", 100, 1},
		{"offset at EOF with length", 16, 1},
		{"negative offset", -1, 4},
		{"negative length", 0, -1},
	}
	for _, tc := range cases {
		if _, err := hub.CloneRange(ctx, project, "src.bin", tc.srcOff, "fresh.bin", 0, tc.length); err == nil {
			t.Fatalf("%s: expected error, got success", tc.name)
		}
	}
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if repo.FindFile("fresh.bin") != nil {
		t.Fatal("rejected clone created the destination (not all-or-nothing)")
	}
	if _, err := hub.CloneRange(ctx, project, "missing.bin", 0, "fresh.bin", 0, 4); err == nil {
		t.Fatal("missing source: expected error, got success")
	}
}

func TestCloneRangeHoleOnlyYieldsHole(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-hole"

	srcData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUV")
	cloneSeedFile(t, hub, project, "src.bin", srcData)
	cloneSeedFile(t, hub, project, "dst.bin", []byte("xxxxxxxx"))
	uploads := countUploads(backend)

	// Punch a hole into the source: drop the records covering [8,24)
	// while keeping the size, so the span reads back as zeros.
	if _, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		file := m.FindFile("src.bin")
		if file == nil {
			return shfs.NotFound("src.bin")
		}
		kept := file.Chunks[:0:0]
		for _, id := range file.Chunks {
			rec, ok := m.Chunks()[id]
			if !ok {
				return shfs.NotFound("src.bin")
			}
			if rec.Offset >= 8 && rec.Offset+rec.Size <= 24 {
				continue
			}
			kept = append(kept, id)
		}
		updated := file.Clone()
		updated.Chunks = kept
		if !m.ReplaceFile("src.bin", updated) {
			return shfs.NotFound("src.bin")
		}
		return nil
	}, "storhub: punch test hole"); err != nil {
		t.Fatalf("punch hole: %v", err)
	}
	holey := cloneFileBytes(t, hub, ctx, project, "src.bin")
	wantHoley := append(append([]byte(nil), srcData[:8]...), append(make([]byte, 16), srcData[24:]...)...)
	if !reflect.DeepEqual(holey, wantHoley) {
		t.Fatalf("hole fixture mismatch: %q", holey)
	}

	repoBefore, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	dstBefore := repoBefore.FindFile("dst.bin").Clone()

	// Clone the hole-only range into an existing destination: no records
	// are minted, the destination IDs do not move, the span reads zeros.
	dstMeta, err := hub.CloneRange(ctx, project, "src.bin", 8, "dst.bin", 8, 16)
	if err != nil {
		t.Fatalf("hole-only clone: %v", err)
	}
	if dstMeta.Size != 24 {
		t.Fatalf("dst size = %d, want 24", dstMeta.Size)
	}
	if !reflect.DeepEqual(dstMeta.Chunks, dstBefore.Chunks) {
		t.Fatalf("hole-only clone minted records: %v vs %v", dstMeta.Chunks, dstBefore.Chunks)
	}
	got := cloneFileBytes(t, hub, ctx, project, "dst.bin")
	want := append([]byte("xxxxxxxx"), make([]byte, 16)...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hole clone mismatch: got %q want %q", got, want)
	}
	if uploads.Load() != 0 {
		t.Fatalf("clone uploaded %d assets, want zero", uploads.Load())
	}

	// Degenerate case: a hole-only range into a fresh destination would
	// need size with zero records, which the schema cannot represent, so
	// the clone is refused and nothing is created.
	if _, err := hub.CloneRange(ctx, project, "src.bin", 8, "new.bin", 0, 16); err == nil {
		t.Fatal("hole-only into fresh destination: expected refusal, got success")
	}
	repoAfter, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if repoAfter.FindFile("new.bin") != nil {
		t.Fatal("refused clone created the destination (not all-or-nothing)")
	}
}

func TestCloneRangeDestinationGapZeroFills(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-gap"

	cloneSeedFile(t, hub, project, "src.bin", []byte("0123456789ABCDEFGH"))
	cloneSeedFile(t, hub, project, "dst.bin", []byte("abcdefgh"))
	uploads := countUploads(backend)

	dstMeta, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst.bin", 24, 8)
	if err != nil {
		t.Fatalf("gap clone: %v", err)
	}
	if dstMeta.Size != 32 {
		t.Fatalf("dst size = %d, want 32", dstMeta.Size)
	}
	got := cloneFileBytes(t, hub, ctx, project, "dst.bin")
	want := append(append([]byte("abcdefgh"), make([]byte, 16)...), []byte("01234567")...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gap clone mismatch: got %q want %q", got, want)
	}
	// The gap carries no records: every destination record sits fully
	// outside [8,24).
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	for _, id := range dstMeta.Chunks {
		rec := repo.Chunks()[id]
		if rec.Offset < 24 && rec.Offset+rec.Size > 8 {
			t.Fatalf("record %d [%d,%d) covers the gap", id, rec.Offset, rec.Offset+rec.Size)
		}
	}
	if uploads.Load() != 0 {
		t.Fatalf("clone uploaded %d assets, want zero", uploads.Load())
	}
}

func TestCloneRangeSelfCloneIsAtomicNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-self"

	srcData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUV")
	cloneSeedFile(t, hub, project, "f.bin", srcData)

	meta, err := hub.CloneRange(ctx, project, "f.bin", 4, "f.bin", 4, 20)
	if err != nil {
		t.Fatalf("self clone: %v", err)
	}
	if meta.Size != int64(len(srcData)) {
		t.Fatalf("self clone size = %d, want %d", meta.Size, len(srcData))
	}
	if got := cloneFileBytes(t, hub, ctx, project, "f.bin"); !reflect.DeepEqual(got, srcData) {
		t.Fatalf("self clone changed bytes: %q", got)
	}
}

func TestCloneRangeZeroLengthNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-zero"

	cloneSeedFile(t, hub, project, "src.bin", []byte("0123456789ABCDEF"))
	cloneSeedFile(t, hub, project, "dst.bin", []byte("xy"))

	repoBefore, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	dstBefore := repoBefore.FindFile("dst.bin").Clone()

	meta, err := hub.CloneRange(ctx, project, "src.bin", 4, "dst.bin", 0, 0)
	if err != nil {
		t.Fatalf("zero-length clone: %v", err)
	}
	if !reflect.DeepEqual(meta, &dstBefore) {
		t.Fatalf("zero-length clone changed dst: %+v vs %+v", meta, &dstBefore)
	}
	if _, err := hub.CloneRange(ctx, project, "src.bin", 0, "missing.bin", 0, 0); err == nil {
		t.Fatal("zero-length into missing dst: expected NotFound, got success")
	}
}

func TestCloneRangeTimestamps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-time"

	cloneSeedFile(t, hub, project, "src.bin", []byte("0123456789ABCDEF"))
	const fixedNow = int64(1700000000000000000)
	// Pre-create the destination with stale timestamps through a direct
	// transaction so stamping is observable under the fixed test clock.
	if _, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		m.UpsertFile("dst.bin", FileMeta{Size: 4, Mode: 0o644, ModifiedAt: 1000, ChangedAt: 1000, AccessedAt: 1001, Chunks: []int64{}}, 1000)
		return nil
	}, "storhub: seed stale dst"); err != nil {
		t.Fatalf("seed stale dst: %v", err)
	}

	// The seeded destination is root-owned (direct transaction, no
	// identity); clone as admin so the test exercises timestamps, not
	// DAC (covered by TestCloneRangePermissions).
	adminCtx := shfs.WithIdentity(ctx, shfs.Identity{UID: 0, GID: 0, Admin: true})
	meta, err := hub.CloneRange(adminCtx, project, "src.bin", 0, "dst.bin", 0, 8)
	if err != nil {
		t.Fatalf("clone: %v", err)
	}
	if meta.ModifiedAt != fixedNow || meta.ChangedAt != fixedNow {
		t.Fatalf("dst mtime/ctime did not move: %+v", meta)
	}
	if meta.AccessedAt != 1001 {
		t.Fatalf("dst atime not preserved: %+v", meta)
	}
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	src := repo.FindFile("src.bin")
	if src.ModifiedAt != fixedNow || src.ChangedAt != fixedNow || src.AccessedAt != fixedNow {
		t.Fatalf("source timestamps touched: %+v", src)
	}
}

func TestCloneRangePermissions(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-dac"
	adminCtx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0})
	userCtx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 1001, GID: 1002})
	// Setup alone needs admin powers (project root is owned by the
	// process user); everything below runs as plain UID 0 to prove
	// ownership works without admin privilege.
	setupCtx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})

	if err := hub.MkdirContext(setupCtx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, project, "docs", 0o777); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	secret := writeTempFile(t, t.TempDir(), "secret.txt", []byte("topsecret!"))
	if _, err := hub.UploadFileContext(adminCtx, project, "docs/secret.bin", secret); err != nil {
		t.Fatalf("upload secret: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, project, "docs/secret.bin", 0o600); err != nil {
		t.Fatalf("chmod secret: %v", err)
	}
	public := writeTempFile(t, t.TempDir(), "public.txt", []byte("publicdata"))
	if _, err := hub.UploadFileContext(adminCtx, project, "docs/public.bin", public); err != nil {
		t.Fatalf("upload public: %v", err)
	}
	locked := writeTempFile(t, t.TempDir(), "locked.txt", []byte("locked!!"))
	if _, err := hub.UploadFileContext(adminCtx, project, "docs/locked.bin", locked); err != nil {
		t.Fatalf("upload locked: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, project, "docs/locked.bin", 0o400); err != nil {
		t.Fatalf("chmod locked: %v", err)
	}

	// Ownership hermeticity: the admin upload above must own the file as
	// UID 0 regardless of which OS user runs the test. Without this pin,
	// the test passes or fails depending on the runner's UID (it passed
	// locally as UID 1000 and failed on CI as UID 1001).
	ownerMeta, _, err := hub.loadRepoMetadataReadonly(context.Background(), project)
	if err != nil {
		t.Fatalf("load for ownership check: %v", err)
	}
	secretEntry := ownerMeta.FindFile("docs/secret.bin")
	if secretEntry == nil {
		t.Fatal("secret.bin missing after upload")
	}
	if secretEntry.UID != 0 || secretEntry.GID != 0 {
		t.Fatalf("admin upload owner: want 0/0, got %d/%d", secretEntry.UID, secretEntry.GID)
	}
	// No read on the source: denied.
	if _, err := hub.CloneRange(userCtx, project, "docs/secret.bin", 0, "docs/out.bin", 0, 4); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("unreadable source: expected EACCES, got %v", err)
	}
	// No write on the destination: denied.
	if _, err := hub.CloneRange(userCtx, project, "docs/public.bin", 0, "docs/locked.bin", 0, 4); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("unwritable destination: expected EACCES, got %v", err)
	}
	// Owner succeeds on both sides.
	if _, err := hub.CloneRange(adminCtx, project, "docs/public.bin", 0, "docs/out.bin", 0, 4); err != nil {
		t.Fatalf("admin clone: %v", err)
	}
}

func TestCloneRangeExpectedRevisionCAS(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-cas"

	cloneSeedFile(t, hub, project, "src.bin", []byte("0123456789ABCDEF"))
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush: %v", err)
	}
	rev, err := hub.RevisionContext(ctx, project)
	if err != nil {
		t.Fatalf("revision: %v", err)
	}
	if _, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst.bin", 0, 4, shfs.WithExpectedRevision("bogus")); !errors.Is(err, shfs.ErrPreconditionFailed) {
		t.Fatalf("stale revision: expected precondition failure, got %v", err)
	}
	repo, _, err := hub.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		t.Fatalf("load metadata: %v", err)
	}
	if repo.FindFile("dst.bin") != nil {
		t.Fatal("CAS-rejected clone created the destination (not all-or-nothing)")
	}
	if _, err := hub.CloneRange(ctx, project, "src.bin", 0, "dst.bin", 0, 4, shfs.WithExpectedRevision(rev)); err != nil {
		t.Fatalf("matching revision clone: %v", err)
	}
}

func TestCloneRangePurgeSafety(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-purge"

	// One asset: content fits in the 8-byte test chunk.
	seedMeta := cloneSeedFile(t, hub, project, "orig.bin", []byte("hello"))
	if _, err := hub.CloneRange(ctx, project, "orig.bin", 0, "copy.bin", 0, seedMeta.Size); err != nil {
		t.Fatalf("clone: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush clone: %v", err)
	}
	live, _, err := hub.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		t.Fatalf("load fresh: %v", err)
	}
	origRec, ok := live.Chunks()[seedMeta.Chunks[0]]
	if !ok {
		t.Fatal("seed chunk missing")
	}
	assetID := origRec.AssetID

	// Purge liveness keys off chunk-record asset references: delete one
	// clone and the asset must stay referenced.
	if err := hub.DeleteFileContext(ctx, project, "orig.bin"); err != nil {
		t.Fatalf("delete orig: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush delete: %v", err)
	}
	if _, tasks, err := hub.classifyUntracked(ctx, project); err != nil {
		t.Fatalf("classify: %v", err)
	} else {
		for _, task := range tasks {
			if task.id == assetID {
				t.Fatal("purge wants an asset still referenced by the surviving clone")
			}
		}
	}
	purged, err := hub.PruneContext(ctx, project, "assets", 0, false)
	if err != nil {
		t.Fatalf("purge with survivor: %v", err)
	}
	if purged.DeletedAssets != 0 {
		t.Fatalf("purge deleted %d assets while a clone still references them", purged.DeletedAssets)
	}
	if got := cloneFileBytes(t, hub, ctx, project, "copy.bin"); string(got) != "hello" {
		t.Fatalf("survivor unreadable after purge: %q", got)
	}

	// Delete all clones and the asset is freed.
	if err := hub.DeleteFileContext(ctx, project, "copy.bin"); err != nil {
		t.Fatalf("delete copy: %v", err)
	}
	if err := hub.FlushProjectContext(ctx, project); err != nil {
		t.Fatalf("flush delete all: %v", err)
	}
	if _, tasks, err := hub.classifyUntracked(ctx, project); err != nil {
		t.Fatalf("classify after delete all: %v", err)
	} else {
		found := false
		for _, task := range tasks {
			if task.id == assetID {
				found = true
			}
		}
		if !found {
			t.Fatal("orphaned asset not classified for purge after deleting all clones")
		}
	}
	purged, err = hub.PruneContext(ctx, project, "assets", 0, false)
	if err != nil {
		t.Fatalf("purge after delete all: %v", err)
	}
	if purged.DeletedAssets != 1 {
		t.Fatalf("purge deleted %d assets, want 1", purged.DeletedAssets)
	}
	backend.mu.Lock()
	_, stillThere := backend.findAssetLocked(assetID)
	backend.mu.Unlock()
	if stillThere {
		t.Fatal("asset still present after purge freed it")
	}
}

// TestCloneRangePropertySeeded pins byte-exactness over randomized ranges:
// offsets and lengths spanning chunk edges, holes, and EOF, applied both
// intra-file and cross-file against a memmove model. Deterministic seed,
// no timing dependence.
func TestCloneRangePropertySeeded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "project-clone-prop"

	rng := rand.New(rand.NewSource(0xC10E4B))
	pattern := func(n int) []byte {
		out := make([]byte, n)
		for i := range out {
			out[i] = byte('a' + rng.Intn(26))
		}
		return out
	}
	srcModel := pattern(96)
	dstModel := pattern(48)

	cloneSeedFile(t, hub, project, "src.bin", srcModel)
	cloneSeedFile(t, hub, project, "dst.bin", dstModel)

	// Sparse fixture: drop the records covering [24,64) from a copy of
	// the source model, keeping the size so the span is a hole.
	sparseModel := append([]byte(nil), srcModel...)
	cloneSeedFile(t, hub, project, "sparse.bin", srcModel)
	if _, err := hub.UpdateRepoMetadataContext(ctx, project, func(m *RepoMetadata) error {
		file := m.FindFile("sparse.bin")
		if file == nil {
			return shfs.NotFound("sparse.bin")
		}
		var kept []int64
		for _, id := range file.Chunks {
			rec, ok := m.Chunks()[id]
			if !ok {
				return shfs.NotFound("sparse.bin")
			}
			if rec.Offset >= 24 && rec.Offset+rec.Size <= 64 {
				continue
			}
			kept = append(kept, id)
		}
		updated := file.Clone()
		updated.Chunks = kept
		if !m.ReplaceFile("sparse.bin", updated) {
			return shfs.NotFound("sparse.bin")
		}
		return nil
	}, "storhub: punch sparse hole"); err != nil {
		t.Fatalf("punch sparse hole: %v", err)
	}
	for i := 24; i < 64; i++ {
		sparseModel[i] = 0
	}
	sparseDstModel := pattern(40)
	cloneSeedFile(t, hub, project, "sparse-dst.bin", sparseDstModel)
	uploads := countUploads(backend)

	models := map[string][]byte{
		"src.bin":        append([]byte(nil), srcModel...),
		"dst.bin":        append([]byte(nil), dstModel...),
		"sparse.bin":     append([]byte(nil), sparseModel...),
		"sparse-dst.bin": append([]byte(nil), sparseDstModel...),
	}
	sources := []string{"src.bin", "sparse.bin"}
	dests := []string{"src.bin", "dst.bin", "sparse.bin", "sparse-dst.bin"}

	applyModel := func(dst, src []byte, srcOff, dstOff, length int64) []byte {
		for int64(len(dst)) < dstOff+length {
			dst = append(dst, 0)
		}
		copy(dst[dstOff:dstOff+length], src[srcOff:srcOff+length])
		return dst
	}

	for trial := 0; trial < 80; trial++ {
		srcPath := sources[rng.Intn(len(sources))]
		dstPath := dests[rng.Intn(len(dests))]
		srcBytes := models[srcPath]
		dstBytes := models[dstPath]
		srcOff := int64(rng.Intn(len(srcBytes)))
		length := int64(1 + rng.Intn(len(srcBytes)-int(srcOff)))
		var dstOff int64
		if rng.Intn(4) == 0 {
			// Occasionally land past EOF to exercise gap zero-fill.
			dstOff = int64(len(dstBytes) + rng.Intn(17))
		} else {
			dstOff = int64(rng.Intn(len(dstBytes) + 1))
		}

		// Intra-file trials snapshot the pre-op model first (memmove).
		preSrc := append([]byte(nil), srcBytes...)
		if srcPath == dstPath {
			models[dstPath] = applyModel(dstBytes, preSrc, srcOff, dstOff, length)
		} else {
			models[dstPath] = applyModel(dstBytes, srcBytes, srcOff, dstOff, length)
		}

		if _, err := hub.CloneRange(ctx, project, srcPath, srcOff, dstPath, dstOff, length); err != nil {
			t.Fatalf("trial %d: clone %s [%d,%d) to %s at %d: %v", trial, srcPath, srcOff, srcOff+length, dstPath, dstOff, err)
		}
		got := cloneFileBytes(t, hub, ctx, project, dstPath)
		if !reflect.DeepEqual(got, models[dstPath]) {
			t.Fatalf("trial %d: %s content mismatch after clone [%d,%d) at %d: got %q want %q",
				trial, dstPath, srcOff, srcOff+length, dstOff, got, models[dstPath])
		}
	}
	if uploads.Load() != 0 {
		t.Fatalf("property clones uploaded %d assets, want zero", uploads.Load())
	}
}
