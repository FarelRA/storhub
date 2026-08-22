package fs

import (
	"strings"
	"syscall"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// maxSymlinkHops bounds resolution; exceeding it reports ELOOP, matching
// Linux's SYMLOOP limit behavior.
const maxSymlinkHops = 8

// ResolvePath follows symlinks among the components of targetPath and
// returns the physical path they resolve to. When followFinal is false the
// trailing component is returned unresolved (lstat/readlink semantics).
// The empty path denotes the root.
func ResolvePath(repo *meta.RepoMetadata, targetPath string, followFinal bool) (string, error) {
	clean := normalizeStoredPath(targetPath)
	if clean == "" {
		return "", nil
	}
	queue := strings.Split(clean, "/")
	resolved := make([]string, 0, len(queue))
	hops := 0
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		switch name {
		case "", ".":
			continue
		case "..":
			if len(resolved) > 0 {
				resolved = resolved[:len(resolved)-1]
			}
			continue
		}
		current := joinComponents(resolved, name)
		file := repo.FindFile(current)
		atFinal := len(queue) == 0
		if file == nil || file.Symlink == "" || (!followFinal && atFinal) {
			resolved = append(resolved, name)
			continue
		}
		hops++
		if hops > maxSymlinkHops {
			return "", syscall.ELOOP
		}
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
	out := strings.Join(resolved, "/")
	return out, nil
}

func joinComponents(parts []string, last string) string {
	if len(parts) == 0 {
		return last
	}
	return strings.Join(append(append([]string{}, parts...), last), "/")
}

// LookupNodeFollowed resolves targetPath following every component including
// the final one, then returns the resulting node's attributes.
func LookupNodeFollowed(repo *meta.RepoMetadata, targetPath string) (nodeAttrs, error) {
	resolved, err := ResolvePath(repo, targetPath, true)
	if err != nil {
		return nodeAttrs{}, err
	}
	return lookupNode(repo, resolved)
}
