package fs

import (
	"errors"
	"strings"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Path resolution must implement PHYSICAL "..": resolvePathTracked pops the
// resolved stack AFTER symlink components have been spliced, not
// before. The old entry pre-cleaned the input through normalizeStoredPath
// (path.Clean), collapsing interior ".." lexically and making the physical
// ".." handling dead code: "a/link/.." (link -> b/c) resolved to "a"
// instead of the physical parent "a/b".

func physicalRepo() *meta.RepoMetadata {
	m := meta.NewRepoMetadata("demo")
	m.EnsureDirectory("a", 1)
	m.EnsureDirectory("a/b", 1)
	m.EnsureDirectory("a/b/c", 1)
	m.UpsertFile("a/b/c/f.txt", meta.FileMeta{Size: 5, Inode: 2}, 1)
	// Relative link: "a/link" -> "b/c" resolves against the link's own
	// directory, so it addresses a/b/c.
	m.UpsertFile("a/link", meta.FileMeta{Symlink: "b/c", Mode: 0o777, Inode: 3}, 1)
	// Absolute-target link: "abs" -> "/a/b/c".
	m.UpsertFile("abs", meta.FileMeta{Symlink: "/a/b/c", Mode: 0o777, Inode: 4}, 1)
	// Two-hop relative chain: "chain1" -> "chain2" -> "a/b/c".
	m.UpsertFile("chain1", meta.FileMeta{Symlink: "chain2", Mode: 0o777, Inode: 5}, 1)
	m.UpsertFile("chain2", meta.FileMeta{Symlink: "a/b/c", Mode: 0o777, Inode: 6}, 1)
	// Cycle for ELOOP.
	m.UpsertFile("loop-a", meta.FileMeta{Symlink: "loop-b", Mode: 0o777, Inode: 7}, 1)
	m.UpsertFile("loop-b", meta.FileMeta{Symlink: "loop-a", Mode: 0o777, Inode: 8}, 1)
	return m
}

func TestStatResolvePhysicalDotDotThroughSymlink(t *testing.T) {
	t.Parallel()
	m := physicalRepo()
	cases := []struct {
		in   string
		want string
	}{
		// The golden cases: ".." after a symlinked component pops
		// the RESOLVED stack, not the lexical one.
		{"a/link/..", "a/b"},
		{"a/link/./..", "a/b"},
		{"a/./link/..", "a/b"},
		{"a/link/../x", "a/b/x"},
		{"a/link/f.txt", "a/b/c/f.txt"},
		{"a/link/../c/f.txt", "a/b/c/f.txt"},
		// Absolute-target link: physical parent of the spliced target.
		{"abs/..", "a/b"},
		{"abs/../x", "a/b/x"},
		// Chains splice before the ".." pops.
		{"chain1/..", "a/b"},
		{"chain1/../x", "a/b/x"},
		// Symlink-free spellings keep matching their lexical form.
		{"a/b/..", "a"},
		{"a/b/../c/f.txt", "a/c/f.txt"},
		{"a/link", "a/b/c"},
	}
	for _, tc := range cases {
		got, _, err := StatResolveTracked(m, tc.in)
		if err != nil {
			t.Fatalf("StatResolve(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("StatResolve(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestStatResolveDotDotEscapeContract(t *testing.T) {
	t.Parallel()
	m := physicalRepo()
	for _, in := range []string{"../x", "a/../..", "a/link/../../../../x"} {
		_, _, err := StatResolveTracked(m, in)
		if err == nil || !strings.Contains(err.Error(), "path escapes root") {
			t.Fatalf("StatResolve(%q) = %v, want path-escapes-root error", in, err)
		}
	}
	// A ".." that pops exactly to the root is legal and yields "".
	got, _, err := StatResolveTracked(m, "a/..")
	if err != nil || got != "" {
		t.Fatalf("StatResolve(\"a/..\") = %q, %v, want root", got, err)
	}
}

func TestStatResolveSymlinkLoopStillELOOP(t *testing.T) {
	t.Parallel()
	m := physicalRepo()
	if _, _, err := StatResolveTracked(m, "loop-a"); !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("expected ELOOP for cyclic links, got %v", err)
	}
	if _, _, err := StatResolveTracked(m, "loop-a/.."); !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("expected ELOOP through cyclic chain, got %v", err)
	}
}

// TestStatResolveMatchesNormalizeForPlainPaths is the no-regression
// property: for symlink-free inputs the repo-aware resolver must agree
// with the pure canonicalizer NormalizePath on both value and error.
func TestStatResolveMatchesNormalizeForPlainPaths(t *testing.T) {
	t.Parallel()
	m := physicalRepo()
	inputs := []string{
		"a", "a/b", "a/b/c", "a/b/c/f.txt",
		"a/./b", "a//b", "/a/b", "a/b/", "a/b/..", "a/../b",
		".", "/", "a/..", "a/b/../c", "a/b/c/../..",
		"a/b/./../c/f.txt",
	}
	for _, in := range inputs {
		want, wantErr := NormalizePath(in)
		got, _, gotErr := StatResolveTracked(m, in)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("StatResolve(%q) err=%v, NormalizePath err=%v: error parity broken", in, gotErr, wantErr)
		}
		if wantErr != nil {
			continue
		}
		if got != want {
			t.Fatalf("StatResolve(%q) = %q, NormalizePath = %q", in, got, want)
		}
	}
	// The escape spellings must fail in BOTH, with the same contract.
	for _, in := range []string{"../x", "a/../.."} {
		if _, err := NormalizePath(in); err == nil {
			t.Fatalf("NormalizePath(%q) unexpectedly accepted escape", in)
		}
		if _, _, err := StatResolveTracked(m, in); err == nil || !strings.Contains(err.Error(), "path escapes root") {
			t.Fatalf("StatResolve(%q) = %v, want path-escapes-root error", in, err)
		}
	}
}

func TestStatResolveShapeValidation(t *testing.T) {
	t.Parallel()
	m := physicalRepo()
	// Whitespace-only is rejected exactly like NormalizePath rejects it.
	_, normErr := NormalizePath("   ")
	_, _, err := StatResolveTracked(m, "   ")
	if err == nil || normErr == nil || err.Error() != normErr.Error() {
		t.Fatalf("whitespace-only: StatResolve err=%v, NormalizePath err=%v, want identical", err, normErr)
	}
	// The empty path denotes the root (documented resolver contract).
	got, traversed, err := resolvePathTracked(m, "", true)
	if err != nil || got != "" || len(traversed) != 1 || traversed[0] != "" {
		t.Fatalf("empty path: got %q traversed=%v err=%v, want root", got, traversed, err)
	}
}
