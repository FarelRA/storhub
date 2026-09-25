package github

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"time"
)

// TestComputeGitBlobSHAGolden pins computeGitBlobSHA against git's
// well-known blob IDs: the header format ("blob <len>\0" + bytes) is an
// optimistic-concurrency precondition for GetFileContent, so a
// header-format regression must fail loudly here instead of silently
// corrupting SHA comparisons upstream.
func TestComputeGitBlobSHAGolden(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		data []byte
		want string
	}{
		{"empty blob", []byte{}, "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"},
		{"hello blob", []byte("hello"), "b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0"},
	}
	for _, tc := range cases {
		if got := computeGitBlobSHA(tc.data); got != tc.want {
			t.Errorf("%s: computeGitBlobSHA=%q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestFileCommitHistory contract: the revision list follows pagination,
// keeps wire order, and normalizes commit times to UTC so the git-path
// list (UnixNano stamps) and this REST-path list compare equal.
func TestFileCommitHistory(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/p/commits", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("path"); got != "dir/file.txt" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, `{"message":"bad path %q"}`, got)
			return
		}
		if n := hits.Add(1); n == 1 {
			// A full page forces the past-the-end probe: exact multiples
			// terminate on the following empty batch.
			out := `[`
			for i := 0; i < 100; i++ {
				if i > 0 {
					out += `,`
				}
				out += fmt.Sprintf(`{"sha":"sha-%03d","commit":{"message":"msg-%03d","author":{"date":"2026-09-01T12:00:00+02:00"}}}`, i, i)
			}
			_, _ = w.Write([]byte(out + `]`))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	c := NewClient("t", retryTaxonomyConfig(server, nil))
	commits, err := c.ListFileCommits(context.Background(), "o", "p", "dir/file.txt")
	if err != nil {
		t.Fatalf("list commits: %v", err)
	}
	if len(commits) != 100 {
		t.Fatalf("want 100 commits across pages, got %d", len(commits))
	}
	got := commits[0]
	if got.SHA != "sha-000" || got.Message != "msg-000" {
		t.Fatalf("wrong mapping: %+v", got)
	}
	if last := commits[99]; last.SHA != "sha-099" {
		t.Fatalf("wire order must be preserved, last=%+v", last)
	}
	want := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	if !got.CommittedAt.Equal(want) || got.CommittedAt.Location() != time.UTC {
		t.Fatalf("time must normalize to UTC %v, got %v", want, got.CommittedAt)
	}
	if n := hits.Load(); n != 2 {
		t.Fatalf("a short first page still probes the past-the-end page, hits=%d", n)
	}
}
