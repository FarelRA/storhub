package github

import "testing"

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
