package cli

import (
	"fmt"
	"io"
	"sort"

	"github.com/FarelRA/storhub/storhub"
)

func printFileSummary(w io.Writer, action string, meta *storhub.FileMetadata) {
	if meta == nil {
		_, _ = fmt.Fprintf(w, "%s\n", action)
		return
	}
	_, _ = fmt.Fprintf(w, "%s\n", action)
	_, _ = fmt.Fprintf(w, "  size: %d bytes\n", meta.Size)
	_, _ = fmt.Fprintf(w, "  inode: %d\n", meta.Inode)
	_, _ = fmt.Fprintf(w, "  mode: %#o\n", meta.Mode)
	_, _ = fmt.Fprintf(w, "  chunks: %d\n", len(meta.Chunks))
}

func printDirEntries(w io.Writer, entries []storhub.DirEntry, long bool) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	// Silence is golden: an empty directory prints nothing, like ls(1).
	for _, entry := range entries {
		kind := "file"
		if entry.IsDir {
			kind = "dir"
		} else if entry.IsSymlink {
			kind = "symlink"
		}
		if long {
			_, _ = fmt.Fprintf(w, "%s\t%#o\t%d\t%s\n", kind, entry.Mode, entry.Size, entry.Path)
			continue
		}
		_, _ = fmt.Fprintln(w, entry.Name)
	}
}

func printEntryInfo(w io.Writer, entry *storhub.EntryInfo) {
	// Defensive: production callers bail out on stat errors first, but the
	// renderer stays total so a nil can never panic a future caller.
	if entry == nil {
		_, _ = fmt.Fprintln(w, "not found")
		return
	}
	_, _ = fmt.Fprintf(w, "path: %s\n", entry.Path)
	_, _ = fmt.Fprintf(w, "kind: %s\n", entryKind(entry))
	_, _ = fmt.Fprintf(w, "inode: %d\n", entry.Inode)
	_, _ = fmt.Fprintf(w, "size: %d\n", entry.Size)
	_, _ = fmt.Fprintf(w, "mode: %#o\n", entry.Mode)
	_, _ = fmt.Fprintf(w, "uid/gid: %d/%d\n", entry.UID, entry.GID)
	_, _ = fmt.Fprintf(w, "links: %d\n", entry.NLink)
	_, _ = fmt.Fprintf(w, "modified: %s\n", formatTime(entry.ModifiedAt))
	_, _ = fmt.Fprintf(w, "accessed: %s\n", formatTime(entry.AccessedAt))
	_, _ = fmt.Fprintf(w, "changed: %s\n", formatTime(entry.ChangedAt))
	if entry.SymlinkTarget != "" {
		_, _ = fmt.Fprintf(w, "target: %s\n", entry.SymlinkTarget)
	}
}

func printRevisions(w io.Writer, revisions []storhub.MetadataRevision) {
	// Silence is golden: empty history prints nothing, like ls(1).
	for _, rev := range revisions {
		sha := rev.CommitSHA
		if len(sha) > 10 {
			sha = sha[:10]
		}
		committedAt := rev.CommittedAt
		_, _ = fmt.Fprintf(w, "%s  %s  %s\n", sha, formatTime(committedAt), rev.Message)
	}
}

func entryKind(entry *storhub.EntryInfo) string {
	if entry.IsDir {
		return "directory"
	}
	if entry.IsSymlink {
		return "symlink"
	}
	return "file"
}
