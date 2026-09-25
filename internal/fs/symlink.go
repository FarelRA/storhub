package fs

import (
	"strings"
	"syscall"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// maxSymlinkHops bounds resolution; exceeding it reports ELOOP, matching
// Linux's SYMLOOP_MAX (40) so legitimate deep chains resolve.
const maxSymlinkHops = 40

// LstatResolveTracked follows symlinks among the components of targetPath
// and returns the physical path they resolve to, leaving the trailing
// component unresolved (lstat/readlink/unlink semantics), plus the ordered
// list of directories the walk descended into for DAC consumption. The
// empty path denotes the root. It is the single lstat-style resolver: the
// untracked plain twin is deleted, callers that need no chain ignore it.
func LstatResolveTracked(repo *meta.RepoMetadata, targetPath string) (string, []string, error) {
	return resolvePathTracked(repo, targetPath, false)
}

// StatResolveTracked follows symlinks among the components of targetPath,
// including a final symlink (stat/open semantics), plus the ordered list
// of directories the walk descended into for DAC consumption. The empty
// path denotes the root. It is the single stat-style resolver.
func StatResolveTracked(repo *meta.RepoMetadata, targetPath string) (string, []string, error) {
	return resolvePathTracked(repo, targetPath, true)
}

// resolvePathTracked is the tracked-resolution core behind StatResolveTracked
// and LstatResolveTracked. It returns the ordered list of directories
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
	// prefixes[i] is the storage-key prefix of resolved[:i] with a
	// trailing slash ("" for the root), so each component's full path
	// costs one concatenation instead of a fresh slice copy plus join per
	// step. The invariant is len(prefixes) == len(resolved)+1.
	prefixes := make([]string, 1, len(queue)+1)
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
				return "", nil, EscapesRoot(targetPath)
			}
			resolved = resolved[:len(resolved)-1]
			prefixes = prefixes[:len(prefixes)-1]
			continue
		}
		prefix := prefixes[len(prefixes)-1]
		next := prefix + name + "/"
		current := next[:len(next)-1]
		file := repo.FindFile(current)
		atFinal := len(queue) == 0
		if file == nil || file.Symlink == "" || (!followFinal && atFinal) {
			if !atFinal && (file != nil || repo.HasDirectory(current)) {
				traversed = append(traversed, current)
			}
			resolved = append(resolved, name)
			prefixes = append(prefixes, next)
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
			prefixes = prefixes[:1]
		} else {
			spliced = strings.Split(target, "/")
		}
		queue = append(spliced, queue...)
	}
	out := normalizeStoredPath(strings.Join(resolved, "/"))
	return out, traversed, nil
}
