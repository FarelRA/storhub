package fs

import (
	"fmt"
	"strings"
	"syscall"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// maxSymlinkHops bounds resolution; exceeding it reports ELOOP, matching
// Linux's SYMLOOP_MAX (40) so legitimate deep chains resolve.
const maxSymlinkHops = 40

// ResolvePath follows symlinks among the components of targetPath and
// returns the physical path they resolve to. When followFinal is false the
// trailing component is returned unresolved (lstat/readlink semantics).
// The empty path denotes the root.
func ResolvePath(repo *meta.RepoMetadata, targetPath string, followFinal bool) (string, error) {
	resolved, _, err := resolvePathTracked(repo, targetPath, followFinal)
	return resolved, err
}

// ResolveAccessPath is the single repo-aware entry point every
// path-taking operation must use to turn a USER path (which may contain
// ".", "..", and symlink components) into the concrete storage key it
// addresses, with POSIX physical resolution semantics. It returns the
// ordered list of directories the walk descended into alongside the key so
// DAC checks can consume the real traversal chain (an absolute link resets
// the cursor; checking only the final key's ancestors would miss the
// chain up to the link). followFinal selects stat/open semantics (true:
// the final symlink is followed) versus lstat/readlink/unlink semantics
// (false: the final component is returned unresolved). Pure key
// canonicalization of concrete paths stays with NormalizePath; this
// function never pre-canonicalizes its input.
func ResolveAccessPath(repo *meta.RepoMetadata, rawPath string, followFinal bool) (string, []string, error) {
	return resolvePathTracked(repo, rawPath, followFinal)
}

// resolvePathTracked is ResolvePath plus the ordered list of directories
// the walk actually descended into (root first). An absolute link resets
// the resolution cursor, so checking only the final path's ancestors would
// skip the chain up to the link itself - a 0700 directory containing an
// absolute link would then leak through the link. Permission checks must
// consume this list in walk order to reproduce POSIX's left-to-right
// EACCES-before-ENOENT/ENOTDIR ordering.
//
// The input is a USER path: it may contain "." and ".." spellings and
// symlink components, and it is deliberately NOT pre-canonicalized. A ".."
// pops the RESOLVED stack (physical parent, POSIX path-resolution
// semantics), so "a/link/.." with link -> b/c yields "a/b", not the
// lexical "a". Only the joined output is canonicalized, once, because the
// walk leaves no ".." or "." components behind. Shape validation is
// minimal: whitespace-only input is rejected with the same error
// NormalizePath returns; the empty path denotes the root.
func resolvePathTracked(repo *meta.RepoMetadata, targetPath string, followFinal bool) (string, []string, error) {
	if targetPath == "" {
		return "", []string{""}, nil
	}
	if err := ValidateAccessPathShape(targetPath); err != nil {
		return "", nil, err
	}
	queue := strings.Split(strings.TrimLeft(targetPath, "/"), "/")
	resolved := make([]string, 0, len(queue))
	traversed := []string{""}
	hops := 0
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		switch name {
		case "", ".":
			continue
		case "..":
			if len(resolved) == 0 {
				return "", nil, fmt.Errorf("path escapes root: %s", targetPath)
			}
			resolved = resolved[:len(resolved)-1]
			continue
		}
		current := joinComponents(resolved, name)
		file := repo.FindFile(current)
		atFinal := len(queue) == 0
		if file == nil || file.Symlink == "" || (!followFinal && atFinal) {
			if !atFinal && (file != nil || repo.HasDirectory(current)) {
				traversed = append(traversed, current)
			}
			resolved = append(resolved, name)
			continue
		}
		hops++
		if hops > maxSymlinkHops {
			return "", nil, syscall.ELOOP
		}
		// The link itself carries no permission weight; its target's
		// components are recorded as they are processed below.
		target := file.Symlink
		var spliced []string
		if strings.HasPrefix(target, "/") {
			spliced = strings.Split(strings.TrimLeft(target, "/"), "/")
			resolved = resolved[:0]
		} else {
			spliced = strings.Split(target, "/")
		}
		queue = append(spliced, queue...)
	}
	out := normalizeStoredPath(strings.Join(resolved, "/"))
	return out, traversed, nil
}

func joinComponents(parts []string, last string) string {
	if len(parts) == 0 {
		return last
	}
	return strings.Join(append(append([]string{}, parts...), last), "/")
}
