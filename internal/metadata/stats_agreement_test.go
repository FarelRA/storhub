package metadata

import (
	"reflect"
	"testing"
)

// TestMutatorStatsAgreementWithRecomputeStats drives every tracked mutator
// (plus the RemoveRelease+EnsureRelease and PutChunk-before-EnsureRelease
// drain cycles) and requires the incremental TotalFiles/TotalSize/AssetCount
// accounting to agree exactly with a from-scratch RecomputeStats walk — and
// with the sealed transaction. Any drift between incremental and full
// accounting fails here instead of passing silently through Validate.
func TestMutatorStatsAgreementWithRecomputeStats(t *testing.T) {
	t.Parallel()
	const now = int64(1700000000)
	base := func() *RepoMetadata {
		m := NewRepoMetadata("p")
		m.EnsureDirectory("docs", now)
		if err := m.PutChunk(1, ChunkInfo{Size: 4, Offset: 0, Release: "v1", AssetID: 11}); err != nil {
			panic(err)
		}
		m.UpsertFile("docs/a.txt", FileMeta{Size: 4, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1}}, now)
		if _, err := m.EnsureRelease("v1", now); err != nil {
			panic(err)
		}
		return m
	}
	type snapshot struct {
		files    int
		size     int64
		releases map[string]ReleaseRef
	}
	snap := func(m *RepoMetadata) snapshot {
		return snapshot{files: m.TotalFiles, size: m.TotalSize, releases: m.Releases()}
	}
	check := func(t *testing.T, name string, mutate func(m *RepoMetadata)) {
		t.Helper()
		m := base()
		mutate(m)
		want := snap(m)
		recomputed := m.Clone()
		recomputed.RecomputeStats()
		if got := snap(recomputed); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: incremental %+v != recomputed %+v", name, want, got)
		}
		sealed := m.Clone()
		sealed.SealTransaction("p", now+1)
		if got := snap(sealed); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: sealed %+v != incremental %+v", name, got, want)
		}
	}
	file2 := FileMeta{Size: 6, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{2}}
	cases := []struct {
		name   string
		mutate func(m *RepoMetadata)
	}{
		{"upsert file", func(m *RepoMetadata) {
			if err := m.PutChunk(2, ChunkInfo{Size: 6, Offset: 0, Release: "v1", AssetID: 12}); err != nil {
				panic(err)
			}
			m.UpsertFile("docs/b.txt", file2, now)
		}},
		{"remove file", func(m *RepoMetadata) { m.RemoveFile("docs/a.txt") }},
		{"ensure directory", func(m *RepoMetadata) { m.EnsureDirectory("photos", now) }},
		{"remove directory", func(m *RepoMetadata) { m.RemoveDirectory("docs") }},
		{"put chunk unreferenced", func(m *RepoMetadata) {
			if err := m.PutChunk(9, ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 99}); err != nil {
				panic(err)
			}
		}},
		{"delete chunk unreferenced", func(m *RepoMetadata) {
			if err := m.PutChunk(9, ChunkInfo{Size: 1, Offset: 0, Release: "v1", AssetID: 99}); err != nil {
				panic(err)
			}
			m.DeleteChunk(9)
		}},
		{"ensure release existing", func(m *RepoMetadata) {
			if _, err := m.EnsureRelease("v1", now); err != nil {
				panic(err)
			}
		}},
		{"put release new tag", func(m *RepoMetadata) {
			if err := m.PutRelease("v2", ReleaseRef{CreatedAt: now}); err != nil {
				panic(err)
			}
		}},
		{"remove release then re-ensure drains pending", func(m *RepoMetadata) {
			m.RemoveRelease("v1")
			if _, err := m.EnsureRelease("v1", now); err != nil {
				panic(err)
			}
		}},
		{"put chunk before ensure release drains on ensure", func(m *RepoMetadata) {
			if err := m.PutChunk(7, ChunkInfo{Size: 2, Offset: 0, Release: "v9", AssetID: 77}); err != nil {
				panic(err)
			}
			if _, err := m.EnsureRelease("v9", now); err != nil {
				panic(err)
			}
		}},
		{"replace file", func(m *RepoMetadata) {
			m.ReplaceFile("docs/a.txt", FileMeta{Size: 8, Mode: 0o600, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1}})
		}},
		{"write file direct", func(m *RepoMetadata) {
			m.WriteFileDirect("docs/a.txt", FileMeta{Size: 5, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{1}})
		}},
		{"write dir direct", func(m *RepoMetadata) {
			m.WriteDirDirect("docs", DirMeta{Mode: 0o755, CreatedAt: now, ModifiedAt: now})
		}},
		{"set file atime", func(m *RepoMetadata) { m.SetFileAtime("docs/a.txt", now+5) }},
		{"set dir atime", func(m *RepoMetadata) { m.SetDirAtime("docs", now+5) }},
		{"sort file chunks", func(m *RepoMetadata) {
			if err := m.PutChunk(2, ChunkInfo{Size: 6, Offset: 0, Release: "v1", AssetID: 12}); err != nil {
				panic(err)
			}
			m.UpsertFile("docs/b.txt", FileMeta{Size: 10, Mode: 0o644, UploadedAt: now, ModifiedAt: now, Chunks: []int64{2, 1}}, now)
			m.SortFileChunks("docs/b.txt")
		}},
	}
	for _, tc := range cases {
		check(t, tc.name, tc.mutate)
	}
}
