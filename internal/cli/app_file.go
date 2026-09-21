package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

// newUploadOrReplaceCmd builds upload and replace: identical flags and the
// same RunE (which branches on cmd.Name()), differing only in Use/Short/
// Long. One factory so the two can never drift apart on wording.
func (a *App) newUploadOrReplaceCmd(name, short, long string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   name + " [flags] <project> <remotepath> <localpath>",
		Short: short,
		Long:  long,
		Args:  usageArgs(cobra.ExactArgs(3)),
		RunE:  a.runUploadOrReplace,
	}
	cmd.Flags().Int64("chunksize", 0, "Chunk size in bytes (32 MiB floor, 2 GiB ceiling; out-of-range values clamp)")
	cmd.Flags().Bool("public", false, "Create public repos instead of private")
	addSyncFlag(cmd)
	return cmd
}

func (a *App) newUploadCmd() *cobra.Command {
	cmd := a.newUploadOrReplaceCmd("upload", "Upload a file", `Upload copies a local file into the project, chunking it for
GitHub release-asset storage.

Examples:
  storhub upload docs-project docs/readme.txt ./README.md`)
	cmd.Flags().Bool("exclusive", false, "Fail if the remote path already exists instead of overwriting (atomic create gate)")
	return cmd
}

func (a *App) newReplaceCmd() *cobra.Command {
	cmd := a.newUploadOrReplaceCmd("replace", "Replace an existing file", `Replace overwrites a stored file with a local one, keeping the
same path and metadata identity.

Examples:
  storhub replace docs-project docs/readme.txt ./README.md`)
	addRevisionFlag(cmd)
	return cmd
}

func (a *App) newDownloadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "download [flags] <project> <remotepath> <localpath>",
		Short: "Download a file",
		Long: `Download reassembles a stored file's chunks into a local file.

Examples:
  storhub download docs-project docs/readme.txt ./README.md`,
		Args: usageArgs(cobra.ExactArgs(3)),
		RunE: a.runDownload,
	}
}

func (a *App) newListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls [flags] <project> [path]",
		Short: "List directory contents",
		Long: `List prints a directory's entries, one per line like ls(1).
An empty directory prints nothing. File bytes go to stdout.

Examples:
  storhub ls docs-project docs
  storhub ls -l docs-project docs`,
		Args: usageArgs(cobra.RangeArgs(1, 2)),
		RunE: a.runList,
	}
	cmd.Flags().BoolP("long", "l", false, "Show detailed listing")
	cmd.Flags().Bool("json", false, "Emit machine-readable JSON (array of entries)")
	return cmd
}

func (a *App) newStatCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stat [flags] <project> <path>",
		Short: "Show file/directory metadata",
		Long: `Stat prints a path's metadata (kind, size, mode, ownership,
timestamps) for humans, or one JSON object with --json.

Examples:
  storhub stat docs-project docs/readme.txt`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runStat,
	}
	cmd.Flags().Bool("json", false, "Emit machine-readable JSON object")
	return cmd
}

func (a *App) newCatCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cat [flags] <project> <path>",
		Short: "Print file contents to stdout",
		Long: `Cat streams a stored file's bytes to stdout, so it composes
with pipes like cat(1). Status chatter stays on stderr.

Examples:
  storhub cat docs-project docs/readme.txt`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runCat,
	}
}

func (a *App) newMkdirCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mkdir [flags] <project> <path>",
		Short: "Create a directory",
		Long: `Mkdir creates a directory and any missing parents, like mkdir -p.

Examples:
  storhub mkdir docs-project docs/2026`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runMkdir,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) newRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rm [flags] <project> <path>",
		Short: "Remove a file or directory",
		Long: `Rm removes a file, or an empty directory with --recursive.
Recursive removal of non-empty trees is not supported.

Examples:
  storhub rm docs-project docs/old.txt
  storhub rm -r docs-project docs/empty-dir`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runRemove,
	}
	cmd.Flags().BoolP("recursive", "r", false, "Remove directory instead of file")
	addSyncFlag(cmd)
	addRevisionFlag(cmd)
	return cmd
}

func (a *App) newMoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mv [flags] <project> <oldpath> <newpath>",
		Short: "Move or rename a file/directory",
		Long: `Mv renames or moves a path within the project, like mv(1).

Examples:
  storhub mv docs-project docs/old.txt docs/new.txt`,
		Args: usageArgs(cobra.ExactArgs(3)),
		RunE: a.runMove,
	}
	cmd.Flags().Bool("noreplace", false, "Fail if the destination already exists (RENAME_NOREPLACE, atomic)")
	addSyncFlag(cmd)
	addRevisionFlag(cmd)
	return cmd
}

func (a *App) newAppendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "append [flags] <project> <path> <text>",
		Short: "Append text to a file",
		Long: `Append adds bytes (or stdin with "-") to the end of a file
atomically: readers never see a torn intermediate state.

Examples:
  storhub append docs-project docs/log.txt "more"
   echo more | storhub append docs-project docs/log.txt -`,
		Args: usageArgs(cobra.ExactArgs(3)),
		RunE: a.runAppend,
	}
	addSyncFlag(cmd)
	addRevisionFlag(cmd)
	return cmd
}

func (a *App) newWriteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "write [flags] <project> <path> <offset> <text>",
		Short: "Write data at a byte offset",
		Long: `Write stores bytes (or stdin with "-") at an offset atomically.
Offsets are non-negative; misuse is a usage error (exit 2).

Examples:
   storhub write docs-project docs/f.txt 0 hello`,
		Args: usageArgs(cobra.ExactArgs(4)),
		RunE: a.runWrite,
	}
	addSyncFlag(cmd)
	addRevisionFlag(cmd)
	return cmd
}

func (a *App) newPatchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "patch [flags] <project> <path> <offset> <deletesize> <text>",
		Short: "Delete and insert at an offset",
		Long: `Patch deletes deletesize bytes at offset and inserts the new
bytes (or stdin with "-") in one atomic step.

Examples:
   storhub patch docs-project docs/f.txt 0 5 hello`,
		Args: usageArgs(cobra.ExactArgs(5)),
		RunE: a.runPatch,
	}
	addSyncFlag(cmd)
	addRevisionFlag(cmd)
	return cmd
}

func (a *App) runUploadOrReplace(cmd *cobra.Command, args []string) error {
	chunkSize, _ := cmd.Flags().GetInt64("chunksize")
	if chunkSize < 0 {
		return &usageError{fmt.Errorf("--chunksize must be positive, got %d", chunkSize)}
	}
	public, _ := cmd.Flags().GetBool("public")

	hub, err := a.mustCmdHub(cmd, chunkSize, public)
	if err != nil {
		return err
	}

	replace := cmd.Name() == "replace"
	project, remotePath, localPath := args[0], args[1], args[2]
	var meta *storhub.FileMetadata
	if replace {
		meta, err = replaceWithFlags(cmd.Context(), hub, project, remotePath, localPath, cmd)
	} else {
		meta, err = uploadWithFlags(cmd.Context(), hub, project, remotePath, localPath, cmd)
	}
	if err != nil {
		return err
	}
	action := "uploaded"
	if replace {
		action = "replaced"
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, project); err != nil {
		return err
	}
	printFileSummary(a.stderr, action, meta)
	return nil
}

func (a *App) runDownload(cmd *cobra.Command, args []string) error {
	// download is a one-shot command: it must get the fail-fast rate
	// policy (a script wants a quick "rate limited", not a 15-minute
	// pause), i.e. the standard command hub, not the long-running one.
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.DownloadFileContext(cmd.Context(), args[0], args[1], args[2]); err != nil {
		return err
	}
	// The size in the status line is a nicety; a failed stat of the file we
	// just wrote must not turn success into exit code 1.
	info, err := os.Stat(args[2])
	if err != nil {
		_, _ = fmt.Fprintf(a.stderr, "downloaded %s to %s\n", args[1], args[2])
		return nil
	}
	_, _ = fmt.Fprintf(a.stderr, "downloaded %s to %s (%d bytes)\n", args[1], args[2], info.Size())
	return nil
}

func (a *App) runList(cmd *cobra.Command, args []string) error {
	long, _ := cmd.Flags().GetBool("long")
	jsonOut, _ := cmd.Flags().GetBool("json")
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	dir := ""
	if len(args) == 2 {
		dir = args[1]
	}
	entries, err := hub.ReadDirContext(cmd.Context(), args[0], dir)
	if err != nil {
		return err
	}
	if jsonOut {
		if entries == nil {
			entries = []storhub.DirEntry{}
		}
		return json.NewEncoder(a.stdout).Encode(entries)
	}
	printDirEntries(a.stdout, entries, long)
	return nil
}

func (a *App) runStat(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	entry, err := hub.StatPathContext(cmd.Context(), args[0], args[1])
	if err != nil {
		return err
	}
	if jsonOut, _ := cmd.Flags().GetBool("json"); jsonOut {
		return json.NewEncoder(a.stdout).Encode(entry)
	}
	printEntryInfo(a.stdout, entry)
	return nil
}

func (a *App) runCat(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	entry, err := hub.StatPathContext(cmd.Context(), args[0], args[1])
	if err != nil {
		return err
	}
	return streamCopyToStdout(cmd.Context(), hub, a.stdout, args[0], args[1], entry.Size)
}

// catWindowSize bounds resident memory while cat streams a stored file:
// each iteration fetches at most this many bytes instead of buffering the
// whole object, so multi-gigabyte files cannot OOM the CLI.
const catWindowSize = 1 << 20

func streamCopyToStdout(ctx context.Context, hub hubClient, w io.Writer, project, path string, size int64) error {
	if size <= 0 {
		return nil
	}
	buf := make([]byte, catWindowSize)
	var off int64
	for off < size {
		want := int64(len(buf))
		if remaining := size - off; remaining < want {
			want = remaining
		}
		n, err := hub.ReadFileAtContext(ctx, project, path, off, want)
		if err != nil {
			return err
		}
		if len(n) == 0 {
			// The file shrank underneath us; serve what exists.
			break
		}
		if _, err := w.Write(n); err != nil {
			return err
		}
		off += int64(len(n))
	}
	return nil
}

func (a *App) runMkdir(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.MkdirContext(cmd.Context(), args[0], args[1]); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "created directory %s\n", args[1])
	return nil
}

func (a *App) runRemove(cmd *cobra.Command, args []string) error {
	recursive, _ := cmd.Flags().GetBool("recursive")
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := removeWithFlags(cmd.Context(), hub, args[0], args[1], recursive, cmd); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "removed %s\n", args[1])
	return nil
}

func (a *App) runMove(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := renameWithFlags(cmd.Context(), hub, args[0], args[1], args[2], cmd); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "moved %s -> %s\n", args[1], args[2])
	return nil
}

func (a *App) runAppend(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	payload, err := readDataArg(a, args[2])
	if err != nil {
		return err
	}
	meta, err := appendWithFlags(cmd.Context(), hub, args[0], args[1], payload, cmd)
	if err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	printFileSummary(a.stderr, "appended", meta)
	return nil
}

func (a *App) runWrite(cmd *cobra.Command, args []string) error {
	offset, err := parseNonNegativeArg(args[2], "offset")
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	payload, err := readDataArg(a, args[3])
	if err != nil {
		return err
	}
	meta, err := writeWithFlags(cmd.Context(), hub, args[0], args[1], offset, payload, cmd)
	if err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	printFileSummary(a.stderr, "written", meta)
	return nil
}

func (a *App) runPatch(cmd *cobra.Command, args []string) error {
	offset, err := parseNonNegativeArg(args[2], "offset")
	if err != nil {
		return err
	}
	deleteSize, err := parseNonNegativeArg(args[3], "deletesize")
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	edit, err := readDataArg(a, args[4])
	if err != nil {
		return err
	}
	meta, err := patchWithFlags(cmd.Context(), hub, args[0], args[1], offset, deleteSize, edit, cmd)
	if err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	printFileSummary(a.stderr, "patched", meta)
	return nil
}
