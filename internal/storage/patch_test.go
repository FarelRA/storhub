package storage

import (
	"context"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func TestSpliceEdit(t *testing.T) {
	mk := func(offset, size, assetOffset, assetID int64) ChunkInfo {
		return ChunkInfo{Offset: offset, Size: size, AssetOffset: assetOffset, AssetID: assetID, Release: "v1"}
	}
	tests := []struct {
		name        string
		chunks      []ChunkInfo
		patchOffset int64
		deleteSize  int64
		insertedLen int64
		inserted    []ChunkInfo
		wantOffsets []int64
		wantSizes   []int64
	}{
		{
			name:        "spanning chunk splits into prefix and shifted suffix views",
			chunks:      []ChunkInfo{mk(0, 10, 0, 1)},
			patchOffset: 3,
			deleteSize:  2,
			insertedLen: 4,
			inserted:    []ChunkInfo{mk(3, 4, 0, 2)},
			wantOffsets: []int64{0, 3, 7},
			wantSizes:   []int64{3, 4, 5},
		},
		{
			name:        "delete-only shrinks later chunks",
			chunks:      []ChunkInfo{mk(0, 5, 2, 1), mk(5, 5, 0, 2)},
			patchOffset: 4,
			deleteSize:  3,
			insertedLen: 0,
			wantOffsets: []int64{0, 4},
			wantSizes:   []int64{4, 3},
		},
		{
			name:        "insert at boundary shifts only later chunks",
			chunks:      []ChunkInfo{mk(0, 5, 0, 1), mk(5, 5, 0, 2)},
			patchOffset: 5,
			deleteSize:  0,
			insertedLen: 3,
			inserted:    []ChunkInfo{mk(5, 3, 0, 9)},
			wantOffsets: []int64{0, 5, 8},
			wantSizes:   []int64{5, 3, 5},
		},
		{
			name:        "suffix view keeps asset offset arithmetic",
			chunks:      []ChunkInfo{mk(0, 20, 7, 3)},
			patchOffset: 10,
			deleteSize:  20,
			insertedLen: 1,
			inserted:    []ChunkInfo{mk(10, 1, 0, 5)},
			wantOffsets: []int64{0, 10},
			wantSizes:   []int64{10, 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := spliceEdit(tc.chunks, tc.patchOffset, tc.deleteSize, tc.insertedLen, tc.inserted)
			sort.SliceStable(got, func(i, j int) bool { return got[i].Offset < got[j].Offset })
			if len(got) != len(tc.wantOffsets) {
				t.Fatalf("got %+v", got)
			}
			for i := range got {
				if got[i].Offset != tc.wantOffsets[i] || got[i].Size != tc.wantSizes[i] {
					t.Fatalf("chunk %d = {off:%d size:%d}, want {off:%d size:%d}", i, got[i].Offset, got[i].Size, tc.wantOffsets[i], tc.wantSizes[i])
				}
			}
		})
	}
}

func TestSpliceEditSuffixAssetOffset(t *testing.T) {
	chunk := ChunkInfo{Offset: 0, Size: 20, AssetOffset: 7, AssetID: 3}
	got := spliceEdit([]ChunkInfo{chunk}, 10, 5, 1, []ChunkInfo{{Offset: 10, Size: 1, AssetID: 5}})
	var suffix *ChunkInfo
	for i := range got {
		if got[i].AssetID == 3 && got[i].Offset == 11 {
			suffix = &got[i]
		}
	}
	if suffix == nil {
		t.Fatalf("expected shifted suffix view of original asset, got %+v", got)
	}
	if suffix.Size != 5 || suffix.AssetOffset != 22 {
		t.Fatalf("suffix must slice the original asset at %d for %d bytes, got %+v", 22, 5, suffix)
	}
}

// TestPatchFileRangesBatchUsesOneReleaseResolution pins the batching
// contract: N disjoint edits become ONE operation - one release reused,
// one new asset per edited chunk, one playlist rebuild - instead of one
// release-listing round trip per edit.
func TestPatchFileRangesBatchUsesOneReleaseResolution(t *testing.T) {
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 64, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	input := writeTempFile(t, t.TempDir(), "batch.txt", []byte("0123456789"))
	if _, err := hub.UploadFile("project-batch", "batch.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}

	var listCalls atomic.Int32
	backend.intercept.Store(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/releases") {
			listCalls.Add(1)
		}
		return false
	})

	patched, err := hub.PatchFileRangesContext(context.Background(), "project-batch", "batch.txt", []shfs.RangeEdit{
		{Start: 1, DeleteSize: 2, Data: []byte("XY")},
		{Start: 5, DeleteSize: 0, Data: []byte("!!")},
		{Start: 7, DeleteSize: 1},
	})
	if err != nil {
		t.Fatalf("batched patch: %v", err)
	}
	if patched.Size != 11 {
		t.Fatalf("final size must be 11 (10 -3 +4), got %d", patched.Size)
	}
	output := filepath.Join(t.TempDir(), "out.txt")
	if err := hub.DownloadFile("project-batch", "batch.txt", output); err != nil {
		t.Fatalf("download: %v", err)
	}
	assertFileContent(t, output, []byte("0XY34!!5689"))
	if listCalls.Load() > 2 {
		t.Fatalf("batch must resolve releases once, saw %d listing calls", listCalls.Load())
	}
	repo := backend.repo("project-batch")
	if len(repo.assets) != 3 {
		t.Fatalf("expected 1 original + 2 edit assets (delete-only needs none), got %d", len(repo.assets))
	}
}
