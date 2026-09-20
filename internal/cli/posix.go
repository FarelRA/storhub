package cli

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

// posix.go: the POSIX verb commands (truncate, chmod, chown, touch,
// symlink, readlink, link, sync) plus the create-exclusive, no-replace,
// and expected-revision modifiers on the create/write paths.
//
// Flag-to-verb routing rule: every verb is Context-first and every helper
// below threads the command context straight into it. Modifier flags
// (--expected-revision, --no-replace) become MutateOptions on the same
// call, so a set guard can never be silently dropped: there is no
// plain-method fallback path.

// addRevisionFlag registers the --expected-revision opt-in shared by every
// write-path command whose storage verb accepts it: the mutation applies
// only while the project metadata revision still matches, failing with a
// precondition error otherwise (compare-and-swap; refetch and retry).
func addRevisionFlag(cmd *cobra.Command) {
	cmd.Flags().String("expected-revision", "", "Only apply when the project metadata revision still matches (compare-and-swap)")
}

// revisionFlag reads --expected-revision ("" when unset).
func revisionFlag(cmd *cobra.Command) string {
	rev, _ := cmd.Flags().GetString("expected-revision")
	return strings.TrimSpace(rev)
}

// revisionOpts converts a set --expected-revision flag into storage
// options (nil when unset, so callers can branch on its presence).
func revisionOpts(cmd *cobra.Command) []storhub.MutateOption {
	if rev := revisionFlag(cmd); rev != "" {
		return []storhub.MutateOption{storhub.WithExpectedRevision(rev)}
	}
	return nil
}

// appendWithFlags routes append past the CAS guard when requested.
func appendWithFlags(ctx context.Context, hub hubClient, project, path string, data []byte, cmd *cobra.Command) (*storhub.FileMetadata, error) {
	return hub.AppendFileContext(ctx, project, path, data, revisionOpts(cmd)...)
}

// writeWithFlags routes write past the CAS guard when requested.
func writeWithFlags(ctx context.Context, hub hubClient, project, path string, offset int64, data []byte, cmd *cobra.Command) (*storhub.FileMetadata, error) {
	return hub.WriteFileAtContext(ctx, project, path, offset, data, revisionOpts(cmd)...)
}

// patchWithFlags routes patch past the CAS guard when requested.
func patchWithFlags(ctx context.Context, hub hubClient, project, path string, offset, deleteSize int64, edit []byte, cmd *cobra.Command) (*storhub.FileMetadata, error) {
	return hub.PatchFileContext(ctx, project, path, offset, deleteSize, edit, revisionOpts(cmd)...)
}

// truncateWithFlags routes truncate past the CAS guard when requested.
func truncateWithFlags(ctx context.Context, hub hubClient, project, path string, size int64, cmd *cobra.Command) (*storhub.FileMetadata, error) {
	return hub.TruncateFileContext(ctx, project, path, size, revisionOpts(cmd)...)
}

// renameWithFlags routes mv past --no-replace/--expected-revision when set.
// NoReplace is enforced inside the storage transaction (no TOCTOU); the
// plain path stays the default.
func renameWithFlags(ctx context.Context, hub hubClient, project, oldPath, newPath string, cmd *cobra.Command) error {
	noReplace, _ := cmd.Flags().GetBool("no-replace")
	opts := revisionOpts(cmd)
	if noReplace {
		opts = append(opts, shfs.WithNoReplace())
	}
	return hub.RenameContext(ctx, project, oldPath, newPath, opts...)
}

// removeWithFlags routes rm past the CAS guard when requested, on both the
// file and recursive branches.
func removeWithFlags(ctx context.Context, hub hubClient, project, path string, recursive bool, cmd *cobra.Command) error {
	opts := revisionOpts(cmd)
	if recursive {
		return hub.RmdirContext(ctx, project, path, opts...)
	}
	return hub.DeleteFileContext(ctx, project, path, opts...)
}

// replaceWithFlags routes replace past the CAS guard when requested.
func replaceWithFlags(ctx context.Context, hub hubClient, project, remotePath, localPath string, cmd *cobra.Command) (*storhub.FileMetadata, error) {
	return hub.ReplaceFileContext(ctx, project, remotePath, localPath, revisionOpts(cmd)...)
}

// uploadWithFlags gates upload on an atomic exclusive create when
// --exclusive is set: CreateFileContext fails with AlreadyExists inside
// the storage transaction when the path exists, so exactly one concurrent
// exclusive uploader wins. The content fill afterwards is a plain upload
// over the just-created empty file. Upload has no revision plumbing
// (create paths declare no expected revision), so --expected-revision is
// a replace/write/append/patch/truncate/mv/rm facility; upload offers no
// such flag.
func uploadWithFlags(ctx context.Context, hub hubClient, project, remotePath, localPath string, cmd *cobra.Command) (*storhub.FileMetadata, error) {
	exclusive, _ := cmd.Flags().GetBool("exclusive")
	if !exclusive {
		return hub.UploadFileContext(ctx, project, remotePath, localPath)
	}
	if _, err := hub.CreateFileContext(ctx, project, remotePath); err != nil {
		return nil, err
	}
	return hub.UploadFileContext(ctx, project, remotePath, localPath)
}

func (a *App) newTruncateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "truncate [flags] <project> <path> <size>",
		Short: "Resize a file",
		Long: `Truncate resizes a file to size bytes, zero-filling growth
like truncate(1). Size is non-negative; misuse is a usage error (exit 2).

Examples:
  storhub truncate docs-project docs/f.txt 0`,
		Args: usageArgs(cobra.ExactArgs(3)),
		RunE: a.runTruncate,
	}
	addSyncFlag(cmd)
	addRevisionFlag(cmd)
	return cmd
}

func (a *App) runTruncate(cmd *cobra.Command, args []string) error {
	size, err := parseNonNegativeArg(args[2], "size")
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	meta, err := truncateWithFlags(cmd.Context(), hub, args[0], args[1], size, cmd)
	if err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	printFileSummary(a.stderr, "truncated", meta)
	return nil
}

// parseChmodMode parses an octal permission mode (000-7777, so setuid,
// setgid, and sticky survive). Misuse is a usage error (exit 2).
func parseChmodMode(raw string) (uint32, error) {
	v, err := strconv.ParseUint(strings.TrimSpace(raw), 8, 32)
	if err != nil || v > 0o7777 {
		return 0, &usageError{fmt.Errorf("invalid mode %q: must be an octal mode 000-7777", raw)}
	}
	return uint32(v), nil
}

func (a *App) newChmodCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chmod [flags] <project> <path> <mode>",
		Short: "Change file mode bits",
		Long: `Chmod replaces a path's permission bits (octal 000-7777),
like chmod(1).

There is deliberately no --expected-revision flag here: the storage
Chmod verb takes no revision options, so a revision token could only be
a start-of-request check, never apply-time compare-and-swap. Failing
loud by documentation instead of offering a guard that does not guard.

Examples:
  storhub chmod docs-project docs/f.txt 640`,
		Args: usageArgs(cobra.ExactArgs(3)),
		RunE: a.runChmod,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runChmod(cmd *cobra.Command, args []string) error {
	mode, err := parseChmodMode(args[2])
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	// Chmod takes no revision options (see newChmodCmd), so the command
	// context is the only thing threaded here.
	if err := hub.ChmodContext(cmd.Context(), args[0], args[1], mode); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "changed mode of %s to %04o\n", args[1], mode)
	return nil
}

// parseChownID parses a chown uid/gid: "-1" keeps the current owner (the
// chown(2) leave-unchanged sentinel), anything else must be a
// non-negative 32-bit id. Misuse is a usage error (exit 2).
func parseChownID(raw, name string) (uint32, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "-1" {
		return ^uint32(0), nil
	}
	v, err := strconv.ParseUint(trimmed, 10, 32)
	if err != nil {
		return 0, &usageError{fmt.Errorf("invalid %s %q: must be a non-negative id or -1 to keep", name, raw)}
	}
	return uint32(v), nil
}

func (a *App) newChownCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chown [flags] <project> <path> <uid> <gid>",
		Short: "Change file owner and group",
		Long: `Chown replaces a path's owner and group like chown(1); -1
keeps the corresponding id (prefix with -- so -1 is not read as a flag).

There is deliberately no --expected-revision flag here: the storage
Chown verb takes no revision options, so a revision token could only be
a start-of-request check, never apply-time compare-and-swap. Failing
loud by documentation instead of offering a guard that does not guard.

Examples:
  storhub chown docs-project docs/f.txt 1000 1000
  storhub chown docs-project docs/f.txt -- -1 100`,
		Args: usageArgs(cobra.ExactArgs(4)),
		RunE: a.runChown,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runChown(cmd *cobra.Command, args []string) error {
	uid, err := parseChownID(args[2], "uid")
	if err != nil {
		return err
	}
	gid, err := parseChownID(args[3], "gid")
	if err != nil {
		return err
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	// Chown takes no revision options (see newChownCmd), so the command
	// context is the only thing threaded here.
	if err := hub.ChownContext(cmd.Context(), args[0], args[1], uid, gid); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "changed ownership of %s to %d:%d\n", args[1], uid, gid)
	return nil
}

func (a *App) newTouchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "touch [flags] <project> <path>",
		Short: "Set file timestamps, creating when missing",
		Long: `Touch sets a path's access and modification times to now
(or to --atime-ns/--mtime-ns nanoseconds since the epoch), creating an
empty file when missing unless --no-create is given, like touch(1).

There is deliberately no --expected-revision flag here: touch is a
create-plus-chtimes composition and neither storage verb takes revision
options, so a revision token could only be a start-of-request check,
never apply-time compare-and-swap. Failing loud by documentation
instead of offering a guard that does not guard.

Examples:
  storhub touch docs-project docs/f.txt
  storhub touch docs-project docs/f.txt --mtime-ns 1700000000123456789`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runTouch,
	}
	cmd.Flags().Bool("no-create", false, "Do not create the file when missing")
	cmd.Flags().Int64("atime-ns", -1, "Access time as nanoseconds since the epoch (-1 means now)")
	cmd.Flags().Int64("mtime-ns", -1, "Modification time as nanoseconds since the epoch (-1 means now)")
	cmd.Flags().Bool("exclusive", false, "Fail if the path already exists instead of updating it (atomic create gate)")
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runTouch(cmd *cobra.Command, args []string) error {
	noCreate, _ := cmd.Flags().GetBool("no-create")
	atimeFlag, _ := cmd.Flags().GetInt64("atime-ns")
	mtimeFlag, _ := cmd.Flags().GetInt64("mtime-ns")
	exclusive, _ := cmd.Flags().GetBool("exclusive")
	if atimeFlag < -1 || mtimeFlag < -1 {
		return &usageError{fmt.Errorf("--atime-ns/--mtime-ns must be >= 0 or -1 for now")}
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	now := time.Now().UnixNano()
	atime := now
	if atimeFlag >= 0 {
		atime = atimeFlag
	}
	mtime := now
	if mtimeFlag >= 0 {
		mtime = mtimeFlag
	}
	if exclusive {
		// Atomic create gate: exactly one concurrent exclusive touch wins.
		if _, err := hub.CreateFileContext(cmd.Context(), args[0], args[1]); err != nil {
			return err
		}
		if err := hub.ChtimesContext(cmd.Context(), args[0], args[1], atime, mtime); err != nil {
			return err
		}
		if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.stderr, "created %s\n", args[1])
		return nil
	}
	if noCreate {
		// No creation allowed: a read-only existence check decides. A
		// missing path is the touch(1) --no-create silent no-op; a live
		// path is stamped, tolerating NotFound when it vanishes under
		// us. The stat is read-only (no mutation TOCTOU); either race
		// outcome stamps or skips correctly.
		if _, err := hub.StatPathContext(cmd.Context(), args[0], args[1]); err != nil {
			if isNotFoundErr(err) {
				return nil
			}
			return err
		}
		if err := hub.ChtimesContext(cmd.Context(), args[0], args[1], atime, mtime); err != nil {
			if isNotFoundErr(err) {
				return nil
			}
			return err
		}
	} else {
		// Create-first, never check-then-act: attempt the create
		// directly and tolerate AlreadyExists (a concurrent touch won
		// the race) or IsDirectory (touching a live directory updates
		// its stamps), so the timestamp update below always lands.
		if _, err := hub.CreateFileContext(cmd.Context(), args[0], args[1]); err != nil {
			if !isAlreadyExistsErr(err) && !isIsDirErr(err) {
				return err
			}
		}
		if err := hub.ChtimesContext(cmd.Context(), args[0], args[1], atime, mtime); err != nil {
			return err
		}
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "touched %s\n", args[1])
	return nil
}

// isNotFoundErr/isAlreadyExistsErr/isIsDirErr classify hub errors.
// errors.Is keeps wrapped failures matching.
func isNotFoundErr(err error) bool {
	return err != nil && errors.Is(err, shfs.ErrNotFound)
}

func isAlreadyExistsErr(err error) bool {
	return err != nil && errors.Is(err, shfs.ErrAlreadyExists)
}

func isIsDirErr(err error) bool {
	return err != nil && errors.Is(err, shfs.ErrIsDirectory)
}

func (a *App) newSymlinkCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "symlink [flags] <project> <target> <link-path>",
		Short: "Create a symbolic link",
		Long: `Symlink creates link-path pointing at target (dangling
targets allowed), like ln -s.

There is deliberately no --expected-revision flag here: the storage
Symlink verb takes no revision options, so a revision token could only
be a start-of-request check, never apply-time compare-and-swap. Failing
loud by documentation instead of offering a guard that does not guard.

Examples:
  storhub symlink docs-project docs/f.txt docs/alias.txt`,
		Args: usageArgs(cobra.ExactArgs(3)),
		RunE: a.runSymlink,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runSymlink(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	// Symlink takes no revision options, so the command context is the
	// only thing threaded here.
	meta, err := hub.SymlinkContext(cmd.Context(), args[0], args[1], args[2])
	if err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	printFileSummary(a.stderr, "symlinked", meta)
	return nil
}

func (a *App) newReadlinkCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "readlink <project> <path>",
		Short: "Print a symlink's target",
		Long: `Readlink prints link-path's target to stdout, like readlink(1).

Examples:
  storhub readlink docs-project docs/alias.txt`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runReadlink,
	}
}

func (a *App) runReadlink(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	target, err := hub.ReadlinkContext(cmd.Context(), args[0], args[1])
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(a.stdout, target)
	return nil
}

func (a *App) newLinkCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "link [flags] <project> <existing-path> <new-path>",
		Short: "Create a hard link",
		Long: `Link creates new-path as a hard link to existing-path
(regular files only), like ln(1) without -s.

There is deliberately no --expected-revision flag here: the storage
Link verb takes no revision options, so a revision token could only be
a start-of-request check, never apply-time compare-and-swap. Failing
loud by documentation instead of offering a guard that does not guard.

Examples:
  storhub link docs-project docs/f.txt docs/hard.txt`,
		Args: usageArgs(cobra.ExactArgs(3)),
		RunE: a.runLink,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runLink(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	// Link takes no revision options, so the command context is the only
	// thing threaded here.
	meta, err := hub.LinkContext(cmd.Context(), args[0], args[1], args[2])
	if err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd.Context(), cmd, args[0]); err != nil {
		return err
	}
	printFileSummary(a.stderr, "linked", meta)
	return nil
}

func (a *App) newProjectSyncCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sync <project>",
		Short: "Drain a project until published state is durable",
		Long: `Sync drains the project's journal until everything published
before the call is durably committed (the storage fsync primitive). It is
the standalone form of the --sync flag every mutating command accepts.

Examples:
  storhub sync docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runProjectSync,
	}
}

func (a *App) runProjectSync(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.DrainProjectContext(cmd.Context(), args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "synced %s\n", args[0])
	return nil
}
