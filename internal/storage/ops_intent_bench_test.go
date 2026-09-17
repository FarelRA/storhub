package storage

import (
	"strconv"
	"testing"

	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// Benchmarks for intent op-synthesis over identical transactions. The
// intent path (synthesizeOpsFromIntents) only touches what the transaction
// recorded. A mutation on a large tree must therefore be flat in tree size,
// and a bulk import must stay linear in the number of imported files.

func benchTree(b *testing.B, files int) *RepoMetadata {
	b.Helper()
	m := newTestMeta("p")
	benchImportInto(b, m, files)
	return m
}

func benchImportInto(b *testing.B, m *RepoMetadata, files int) {
	b.Helper()
	m.EnsureDirectory("bulk", 1700000000)
	for i := 0; i < files; i++ {
		m.UpsertFile("bulk/f"+strconv.Itoa(i)+".txt", intentTestFile(m, 8, nil), 1700000000)
	}
}

func benchTouchOne(m *RepoMetadata) {
	f := m.FindFile("bulk/f0.txt").Clone()
	f.Mode = 0o600
	m.ReplaceFile("bulk/f0.txt", f)
}

func BenchmarkIntentSynthesisTouchLargeTree(b *testing.B) {
	for _, n := range []int{10000, 50000, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			before := benchTree(b, n)
			rec := metadata.NewIntentRecorder()
			candidate := before.Clone()
			candidate.AttachIntentRecorder(rec)
			benchTouchOne(candidate)
			candidate.DetachIntentRecorder()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				synthesizeOpsFromIntents(before, candidate, rec, "bench", intentTestNow)
			}
		})
	}
}

func BenchmarkIntentSynthesisBulkImport(b *testing.B) {
	for _, n := range []int{5000, 20000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			before := newTestMeta("p")
			rec := metadata.NewIntentRecorder()
			candidate := before.Clone()
			candidate.AttachIntentRecorder(rec)
			benchImportInto(b, candidate, n)
			candidate.DetachIntentRecorder()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				synthesizeOpsFromIntents(before, candidate, rec, "bench", intentTestNow)
			}
		})
	}
}
