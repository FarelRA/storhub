package cli

import (
	"fmt"
	"io"
	"sort"

	"github.com/FarelRA/storhub/storhub"
)

func printFileSummary(w io.Writer, action string, meta *storhub.FileMetadata) {
	if meta == nil {
		fmt.Fprintf(w, "%s\n", action)
		return
	}
	fmt.Fprintf(w, "%s %s\n", action, meta.Name)
	fmt.Fprintf(w, "  size: %d bytes\n", meta.Size)
	fmt.Fprintf(w, "  release: %s\n", meta.Release)
	fmt.Fprintf(w, "  inode: %d\n", meta.Inode)
	fmt.Fprintf(w, "  mode: %#o\n", meta.Mode)
	fmt.Fprintf(w, "  links: %d\n", meta.NLink)
	fmt.Fprintf(w, "  chunks: %d\n", len(meta.Chunks))
	fmt.Fprintf(w, "  crc32c: %s\n", meta.CRC32C)
}

func printDirEntries(w io.Writer, entries []storhub.DirEntry, long bool) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	if len(entries) == 0 {
		fmt.Fprintln(w, "empty")
		return
	}
	for _, entry := range entries {
		kind := "file"
		if entry.IsDir {
			kind = "dir"
		} else if entry.IsSymlink {
			kind = "symlink"
		}
		if long {
			fmt.Fprintf(w, "%s\t%#o\t%d\t%s\n", kind, entry.Mode, entry.Size, entry.Path)
			continue
		}
		fmt.Fprintln(w, entry.Name)
	}
}

func printEntryInfo(w io.Writer, entry *storhub.EntryInfo) {
	if entry == nil {
		fmt.Fprintln(w, "not found")
		return
	}
	fmt.Fprintf(w, "path: %s\n", entry.Path)
	fmt.Fprintf(w, "kind: %s\n", entryKind(entry))
	fmt.Fprintf(w, "inode: %d\n", entry.Inode)
	fmt.Fprintf(w, "size: %d\n", entry.Size)
	fmt.Fprintf(w, "mode: %#o\n", entry.Mode)
	fmt.Fprintf(w, "uid/gid: %d/%d\n", entry.UID, entry.GID)
	fmt.Fprintf(w, "links: %d\n", entry.NLink)
	fmt.Fprintf(w, "modified: %s\n", formatTime(entry.ModifiedAt))
	fmt.Fprintf(w, "accessed: %s\n", formatTime(entry.AccessedAt))
	fmt.Fprintf(w, "changed: %s\n", formatTime(entry.ChangedAt))
	if entry.SymlinkTarget != "" {
		fmt.Fprintf(w, "target: %s\n", entry.SymlinkTarget)
	}
}

func printRevisions(w io.Writer, revisions []storhub.MetadataRevision) {
	if len(revisions) == 0 {
		fmt.Fprintln(w, "no metadata revisions found")
		return
	}
	for _, rev := range revisions {
		sha := rev.CommitSHA
		if len(sha) > 10 {
			sha = sha[:10]
		}
		fmt.Fprintf(w, "%s  %s  %s\n", sha, formatTime(rev.CommittedAt), rev.Message)
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
