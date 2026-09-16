package metadata

import (
	"strings"
	"testing"
)

// TestNormalizeStoredPathErrParity pins the checked constructor to the total
// wrapper: every input the conformance suite covers must normalize
// identically; only escaping paths gain a context-rich error.
func TestNormalizeStoredPathErrParity(t *testing.T) {
	t.Parallel()
	for _, in := range []string{
		"", " ", "/", "//", "docs", "/docs", " docs ", "docs/",
		"a/b/c", "./a", "a//b", "/a/b/../c", " .hidden ", "...",
	} {
		checked, err := normalizeStoredPathErr(in)
		if err != nil {
			t.Fatalf("input %q: unexpected error: %v", in, err)
		}
		if checked != normalizeStoredPath(in) {
			t.Errorf("input %q: checked %q != total %q", in, checked, normalizeStoredPath(in))
		}
	}
	for _, in := range []string{"..", "../esc", "a/../..", "/../esc"} {
		if _, err := normalizeStoredPathErr(in); err == nil {
			t.Errorf("input %q: expected escaping-path error", in)
		} else if !strings.Contains(err.Error(), in) && !strings.Contains(err.Error(), strings.Trim(in, "/")) {
			t.Errorf("input %q: error lacks path context: %v", in, err)
		}
		// The total wrapper stays total for Validate to reject downstream.
		if normalizeStoredPath(in) == "" {
			t.Errorf("input %q: total wrapper must pass escaping keys through", in)
		}
	}
}

// TestParentPathMatchesTotal pins parentPath outputs while it is reimplemented
// on the checked constructor: root stays root, files map to parents.
func TestParentPathMatchesTotal(t *testing.T) {
	t.Parallel()
	if got := parentPath("docs/guide.txt"); got != "docs" {
		t.Errorf("parentPath = %q, want docs", got)
	}
	if got := parentPath("guide.txt"); got != "" {
		t.Errorf("parentPath = %q, want empty", got)
	}
	if got := parentPath(""); got != "" {
		t.Errorf("parentPath = %q, want empty", got)
	}
}
