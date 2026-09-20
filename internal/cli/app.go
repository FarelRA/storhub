package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shlog "github.com/FarelRA/storhub/internal/logging"
	storage "github.com/FarelRA/storhub/internal/storage"
	shrest "github.com/FarelRA/storhub/rest"
	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

// version is injected at build time via -ldflags "-X ...version=x.y.z".
var version = "dev"

// logSettings carries the --log-* flags from one App instance into the hub
// constructors. Per-App state, not package globals: two Apps in one process
// (tests, embedders driving the CLI) must not race on shared flag targets.
type logSettings struct {
	level  string
	format string
	color  bool
}

func defaultLogSettings() logSettings {
	return logSettings{
		level:  envOrDefault("STORHUB_LOG_LEVEL", "info"),
		format: envOrDefault("STORHUB_LOG_FORMAT", "pretty"),
		color:  parseEnvBool("STORHUB_LOG_COLOR", true),
	}
}

// cliSeams injects every external constructor the CLI shells out to.
// Per-App state (not package globals) so parallel tests can drive isolated
// Apps without racing on shared seam vars. The package-level
// newHubFromFlagsFn et al. below remain as deprecated shims forwarding to
// defaultSeams for backward compatibility with existing tests.
type cliSeams struct {
	newHub      func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error)
	newRESTHub  func(token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error)
	newMountHub func(token, apiBase string, log logSettings) (hubClient, error)
	newFUSE     func(hub *storhub.StorHub, project string, opts storhub.FUSEOptions) (fuseMount, error)
	newREST     func(hub *storhub.StorHub, opts shrest.Options) (http.Handler, error)
	listenServe func(server *http.Server) error
}

func defaultCliSeams() cliSeams {
	return cliSeams{
		newHub: func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
			hub, err := newHubFromFlags(token, apiBase, chunkSize, public, log)
			if err != nil {
				return nil, err
			}
			return storhubClient{StorHub: hub}, nil
		},
		newRESTHub: newRESTHubFromFlags,
		newMountHub: func(token, apiBase string, log logSettings) (hubClient, error) {
			hub, err := newMountHubFromFlags(token, apiBase, log)
			if err != nil {
				return nil, err
			}
			return storhubClient{StorHub: hub}, nil
		},
		newFUSE: func(hub *storhub.StorHub, project string, opts storhub.FUSEOptions) (fuseMount, error) {
			return hub.NewFUSE(project, opts)
		},
		newREST: func(hub *storhub.StorHub, opts shrest.Options) (http.Handler, error) {
			return shrest.New(hub, opts)
		},
		listenServe: func(server *http.Server) error { return server.ListenAndServe() },
	}
}

type App struct {
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	rootCmd *cobra.Command
	hub     hubClient
	log     logSettings
	seams   cliSeams
}

type fuseMount interface {
	Mount(mountPoint string) error
	Unmount() error
	Wait()
	Close() error
}

type hubClient interface {
	UploadFile(project, remotePath, localPath string) (*storhub.FileMetadata, error)
	ReplaceFile(project, remotePath, localPath string) (*storhub.FileMetadata, error)
	DownloadFile(project, remotePath, localPath string) error
	ReadDir(project, dir string) ([]storhub.DirEntry, error)
	StatPath(project, targetPath string) (*storhub.EntryInfo, error)
	ReadFileAt(project, filePath string, offset, length int64) ([]byte, error)
	Mkdir(project, dirPath string) error
	DeleteFile(project, filePath string) error
	Rmdir(project, dirPath string) error
	Rename(project, oldPath, newPath string) error
	AppendFile(project, filePath string, data []byte) (*storhub.FileMetadata, error)
	WriteFileAt(project, filePath string, offset int64, data []byte) (*storhub.FileMetadata, error)
	PatchFile(project, filePath string, offset, deleteSize int64, edit []byte) (*storhub.FileMetadata, error)
	// POSIX verbs backing the truncate/chmod/chown/touch/symlink/readlink/
	// link/sync commands. Signatures mirror *storage.StorHub directly so
	// storhubClient satisfies them through its embedded hub.
	CreateFile(project, filePath string) (*storhub.FileMetadata, error)
	TruncateFile(project, filePath string, size int64) (*storhub.FileMetadata, error)
	Chmod(project, targetPath string, mode uint32) error
	Chown(project, targetPath string, uid, gid uint32) error
	Chtimes(project, targetPath string, atime, mtime int64) error
	Symlink(project, target, linkPath string) (*storhub.FileMetadata, error)
	Readlink(project, linkPath string) (string, error)
	Link(project, existingPath, newPath string) (*storhub.FileMetadata, error)
	ListMetadataRevisions(project string) ([]storhub.MetadataRevision, error)
	// The long one-shot maintenance operations take a context so a
	// Ctrl+C cancels them between units of work instead of killing the
	// process mid-delete-loop.
	RollbackMetadataContext(ctx context.Context, project, commitSHA string) error
	PurgeContext(ctx context.Context, project, scope string, keep int, dryRun bool) (*storhub.PurgeResult, error)
	// GC scans the live chunk catalog for orphans (ScanChunkGC, read-only)
	// and collects them (CompactOrphanChunks; dryRun previews). Refuses
	// while any session is live on the project.
	ScanChunkGC(ctx context.Context, project string) (*storhub.ChunkGCResult, error)
	CompactOrphanChunks(ctx context.Context, project string, dryRun bool) (*storhub.ChunkGCResult, error)
	// Degraded-mode operations: DegradedProjects lists latched projects,
	// ReEnableProject clears one latch (the only path back to healthy),
	// PressureSnapshot exposes the operator pressure ledger.
	DegradedProjects() []string
	ReEnableProject(project string) error
	PressureSnapshot() storhub.PressureSnapshot
	PressureFailureStreak(project string) uint64
	PressurePendingDepth(project string) int
	DeleteProject(project string) error
	NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error)

	// DrainProjectContext blocks until everything published before the call
	// lands in the remote commit (the storage fsync primitive). It backs
	// the --sync opt-in on every mutating command via drainIfSyncRequested.
	DrainProjectContext(ctx context.Context, project string) error

	// OpenSession opens a stateful file handle (Phase 2B sessions). The
	// signatures mirror *storage.StorHub directly so storhubClient satisfies
	// them through its embedded hub with no adapter; commands pass
	// cmd.Context() unchanged and never synthesize a caller identity.
	OpenSession(ctx context.Context, project, path string, mode storage.OpenMode, opts ...storage.SessionOption) (string, error)
	ReadSession(ctx context.Context, handleID string, offset, length int64) ([]byte, error)
	WriteSession(ctx context.Context, handleID string, offset int64, data []byte) (int, error)
	TruncateSession(ctx context.Context, handleID string, size int64) error
	StatSession(ctx context.Context, handleID string) (storage.SessionStat, error)
	SyncSession(ctx context.Context, handleID string) error
	LinkSession(ctx context.Context, handleID, path string) error
	RelinkSession(ctx context.Context, handleID, path string) error
	CloseSession(ctx context.Context, handleID string) error

	// Shutdown drains the asynchronous metadata writer. Part of the
	// contract on purpose: every implementation - including test fakes -
	// must be drainable, and App.Run is the single caller.
	Shutdown(ctx context.Context) error
}

type storhubClient struct {
	*storhub.StorHub
}

// warnOutput is the fallback sink for configuration warnings emitted from
// package-level constructors that run before any App exists (flag parsing,
// env layering). Once an App exists, warnings go to a.stderr (see App.warnf);
// warnf below prefers the App sink and falls back here only for pre-App
// callers. Tests swap it to capture warnings.
var warnOutput io.Writer = os.Stderr

// warnf prints a storhub-prefixed warning to warnOutput (pre-App fallback).
func warnf(format string, args ...any) {
	_, _ = fmt.Fprintf(warnOutput, "%s storhub: warning: "+format+"\n",
		append([]any{time.Now().UTC().Format(time.RFC3339)}, args...)...)
}

// warnf is the primary warning sink: App.stderr. Package-level warnf above
// remains only for pre-App constructors without an App handle.
func (a *App) warnf(format string, args ...any) {
	out := a.stderr
	if out == nil {
		out = warnOutput
	}
	_, _ = fmt.Fprintf(out, "%s storhub: warning: "+format+"\n",
		append([]any{time.Now().UTC().Format(time.RFC3339)}, args...)...)
}

func (c storhubClient) NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error) {
	return c.StorHub.NewFUSE(project, opts)
}

// Deprecated seam shims: kept so existing tests (which swap these globals)
// keep compiling. New code uses App.seams injected per-App.
var newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
	return defaultCliSeams().newHub(token, apiBase, chunkSize, public, log)
}

// newRESTHubFromFlags builds the hub for rest/serve: long-running
// surfaces, so it gets the pause-to-reset rate policy (see applyRateEnv).
func newRESTHubFromFlags(token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error) {
	token = resolveToken(token)
	if token == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	return storhub.NewStorHubWithConfig(token, newHubConfig(apiBase, chunkSize, public, log, true))
}

var newRESTHubFromFlagsFn = newRESTHubFromFlags

// newMountHubFromFlagsFn builds the hub for mount: a long-running
// interactive surface, so it gets the pause-to-reset rate policy.
// Deprecated shim: new code uses App.seams.newMountHub.
var newMountHubFromFlagsFn = func(token, apiBase string, log logSettings) (hubClient, error) {
	return defaultCliSeams().newMountHub(token, apiBase, log)
}

// Deprecated seam shims: kept so existing tests keep compiling.
var newFUSEFn = func(hub *storhub.StorHub, project string, opts storhub.FUSEOptions) (fuseMount, error) {
	return defaultCliSeams().newFUSE(hub, project, opts)
}
var newRESTHandlerFn = func(hub *storhub.StorHub, opts shrest.Options) (http.Handler, error) {
	return defaultCliSeams().newREST(hub, opts)
}
var restListenAndServeFn = func(server *http.Server) error {
	return defaultCliSeams().listenServe(server)
}

const minCLIChunkSize int64 = 32 * 1024 * 1024

// normalizeCLIChunkSize maps a --chunk-size flag value to the size actually
// used. Non-positive values pass through untouched (0 means "unset"; the
// command layer rejects negatives as usage errors before they get here).
// Values below the 32 MiB floor clamp up, and values above the GitHub
// release-asset ceiling clamp down through chunking.NormalizedSize - the
// single owner of the ceiling - so the chunker's plan and the uploader's
// windows can never disagree mid-upload.
func normalizeCLIChunkSize(size int64) int64 {
	if size <= 0 {
		return size
	}
	if size < minCLIChunkSize {
		return minCLIChunkSize
	}
	return chunking.NormalizedSize(size)
}

func New() *App {
	a := &App{stdin: os.Stdin, stdout: io.Writer(os.Stdout), stderr: io.Writer(os.Stderr), log: defaultLogSettings(), seams: defaultCliSeams()}
	a.buildRootCmd()
	return a
}

// Seam accessors: per-App injection point (parallel-safe) with fallback to
// the deprecated package globals. Until wave 2 migrates tests to set
// a.seams per-test, the globals win so existing stub-swapping tests keep
// working; new code should set a.seams explicitly for isolation.
func (a *App) seamHub() func(string, string, int64, bool, logSettings) (hubClient, error) {
	return newHubFromFlagsFn
}

func (a *App) seamRESTHub() func(string, string, int64, bool, logSettings) (*storhub.StorHub, error) {
	return newRESTHubFromFlagsFn
}

func (a *App) seamMountHub() func(string, string, logSettings) (hubClient, error) {
	return newMountHubFromFlagsFn
}

func (a *App) seamFUSE() func(*storhub.StorHub, string, storhub.FUSEOptions) (fuseMount, error) {
	return newFUSEFn
}

func (a *App) seamRESTHandler() func(*storhub.StorHub, shrest.Options) (http.Handler, error) {
	return newRESTHandlerFn
}

func (a *App) seamListen() func(*http.Server) error {
	return restListenAndServeFn
}

func (a *App) buildRootCmd() {
	rootCmd := &cobra.Command{
		Use:   "storhub",
		Short: "StorHub CLI - GitHub-backed chunked storage",
		Long: `StorHub CLI

Friendly commands for GitHub-backed chunked storage, REST serving, and FUSE mounting.

Authentication:
  Set GITHUB_TOKEN or pass --token.

Examples:
  storhub upload docs-project docs/readme.txt ./README.md
  storhub ls docs-project docs
  storhub stat docs-project docs/readme.txt
  storhub rest --listen :8080
  storhub mount docs-project ./mnt
  storhub serve docs-project ./mnt --listen :8080`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}
	rootCmd.SetVersionTemplate("storhub {{.Version}}\n")
	// Flag misuse is a usage error: main exits 2 and the shell knows the
	// difference between "bad command line" and "operation failed".
	rootCmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		return &usageError{err}
	})

	rootCmd.PersistentFlags().String("token", "", "GitHub token (falls back to $GITHUB_TOKEN; never shown in help)")
	rootCmd.PersistentFlags().String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL (env: STORHUB_API_BASE_URL)")
	rootCmd.PersistentFlags().StringVar(&a.log.level, "log-level", a.log.level, "Log level: debug, info, warn, error (env: STORHUB_LOG_LEVEL)")
	rootCmd.PersistentFlags().StringVar(&a.log.format, "log-format", a.log.format, "Log format: pretty, text (env: STORHUB_LOG_FORMAT)")
	rootCmd.PersistentFlags().BoolVar(&a.log.color, "log-color", a.log.color, "Enable ANSI colors in logs (env: STORHUB_LOG_COLOR)")

	rootCmd.AddCommand(a.newUploadCmd())
	rootCmd.AddCommand(a.newReplaceCmd())
	rootCmd.AddCommand(a.newDownloadCmd())
	rootCmd.AddCommand(a.newListCmd())
	rootCmd.AddCommand(a.newStatCmd())
	rootCmd.AddCommand(a.newCatCmd())
	rootCmd.AddCommand(a.newMkdirCmd())
	rootCmd.AddCommand(a.newRemoveCmd())
	rootCmd.AddCommand(a.newMoveCmd())
	rootCmd.AddCommand(a.newCpCmd())
	rootCmd.AddCommand(a.newAppendCmd())
	rootCmd.AddCommand(a.newWriteCmd())
	rootCmd.AddCommand(a.newPatchCmd())
	rootCmd.AddCommand(a.newTruncateCmd())
	rootCmd.AddCommand(a.newChmodCmd())
	rootCmd.AddCommand(a.newChownCmd())
	rootCmd.AddCommand(a.newTouchCmd())
	rootCmd.AddCommand(a.newSymlinkCmd())
	rootCmd.AddCommand(a.newReadlinkCmd())
	rootCmd.AddCommand(a.newLinkCmd())
	rootCmd.AddCommand(a.newSyncCmd())
	rootCmd.AddCommand(a.newRevisionsCmd())
	rootCmd.AddCommand(a.newRollbackCmd())
	rootCmd.AddCommand(a.newPurgeCmd())
	rootCmd.AddCommand(a.newGCCmd())
	rootCmd.AddCommand(a.newStatusCmd())
	rootCmd.AddCommand(a.newReEnableCmd())
	rootCmd.AddCommand(a.newDeleteProjectCmd())
	rootCmd.AddCommand(a.newCacheCmd())
	rootCmd.AddCommand(a.newSessionCmd())
	rootCmd.AddCommand(a.newMountCmd())
	rootCmd.AddCommand(a.newRestCmd())
	rootCmd.AddCommand(a.newServeCmd())

	// An unknown command is as much a command-line mistake as a bad flag:
	// both must classify as usage errors so main exits 2. Making the root
	// runnable routes every unmatched first word through the Args
	// validator, where it is wrapped as a usageError.
	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	}
	rootCmd.Args = usageArgs(func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
			return nil
		}
		for _, sub := range cmd.Commands() {
			if sub.Name() == args[0] || sub.HasAlias(args[0]) {
				return nil
			}
		}
		return fmt.Errorf("unknown command %q for %q", args[0], cmd.CommandPath())
	})

	a.rootCmd = rootCmd
}

// newUploadOrReplaceCmd builds upload and replace: identical flags and the
// same RunE (which branches on cmd.Name()), differing only in Use/Short/
// Long. One factory so the two can never drift apart on wording.
func (a *App) newUploadOrReplaceCmd(name, short, long string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   name + " [flags] <project> <remote-path> <local-path>",
		Short: short,
		Long:  long,
		Args:  usageArgs(cobra.ExactArgs(3)),
		RunE:  a.runUploadOrReplace,
	}
	cmd.Flags().Int64("chunk-size", 0, "Chunk size in bytes (32 MiB floor, 2 GiB ceiling; out-of-range values clamp)")
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
		Use:   "download [flags] <project> <remote-path> <local-path>",
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
		Use:   "mv [flags] <project> <old-path> <new-path>",
		Short: "Move or rename a file/directory",
		Long: `Mv renames or moves a path within the project, like mv(1).

Examples:
  storhub mv docs-project docs/old.txt docs/new.txt`,
		Args: usageArgs(cobra.ExactArgs(3)),
		RunE: a.runMove,
	}
	cmd.Flags().Bool("no-replace", false, "Fail if the destination already exists (RENAME_NOREPLACE, atomic)")
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
		Use:   "patch [flags] <project> <path> <offset> <delete-size> <text>",
		Short: "Delete and insert at an offset",
		Long: `Patch deletes delete-size bytes at offset and inserts the new
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

func (a *App) newRevisionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revisions [flags] <project>",
		Short: "List metadata revision history",
		Long: `Revisions lists the project's metadata commits, newest last.
An empty history prints nothing, like ls(1).

Examples:
  storhub revisions docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runRevisions,
	}
	cmd.Flags().Bool("json", false, "Emit machine-readable JSON (array of revisions)")
	return cmd
}

func (a *App) newRollbackCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollback [flags] <project> <commit-sha>",
		Short: "Rollback metadata to a commit",
		Long: `Rollback restores the project's metadata to a past commit SHA
(7-64 lowercase hex). A malformed SHA is a usage error (exit 2).

The serve-mode admin boundary covers the REST surface only: this
command runs with local-process trust and performs no admin check
(the REST rollback endpoint is admin-gated).

Examples:
  storhub rollback docs-project abc1234`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runRollback,
	}
	addSyncFlag(cmd)
	return cmd
}

func (a *App) newPurgeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "purge <project> [objects|assets|history|all]",
		Short: "Reclaim objects, release assets, or git history",
		Long: `Purge reclaims storage under the full-history retention policy.

  objects   delete content-addressed index objects referenced by no retained
            manifest (orphans from failed commits or after a history purge)
  assets    delete release assets tracked by no file (interrupted writes,
            manual interference)
  history   collapse old index manifests into a checkpoint (git backend only;
            on the REST backend GitHub owns history and the API cannot delete
            revisions, so this reports honestly instead of pretending)
  all       history (where possible) + objects + assets (the default)

Use --dry-run to see what would be reclaimed without deleting anything.
--keep bounds history compaction (manifests newer than keep are retained).

The serve-mode admin boundary covers the REST surface only: this
command runs with local-process trust and performs no admin check
(the REST purge endpoint is admin-gated).`,
		Args: usageArgs(cobra.RangeArgs(1, 2)),
		RunE: a.runPurge,
	}
	cmd.Flags().Bool("dry-run", false, "Report what would be reclaimed without deleting")
	cmd.Flags().Int("keep", 1, "History: number of recent manifests to retain")
	addSyncFlag(cmd)
	return cmd
}

func (a *App) newGCCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "gc [flags] <project>",
		Short: "Collect orphaned chunk records",
		Long: `GC scans the live chunk catalog for records no file and no
pending edit references, then collects them. A project with a live
session refuses outright (sessions pin chunks); use --dry-run to
preview. Nothing runs automatically: every collection is an explicit
operator act, logged per object.

Examples:
  storhub gc docs-project --dry-run
  storhub gc docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runGC,
	}
	cmd.Flags().Bool("dry-run", false, "Report orphans without collecting")
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runGC(cmd *cobra.Command, args []string) error {
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	hub, ctx, stop, err := a.mustCmdHubCtx(cmd, 0, false)
	if err != nil {
		return err
	}
	defer stop()
	if dryRun {
		res, err := hub.ScanChunkGC(ctx, args[0])
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(a.stderr, "would collect %s (%d chunks, %d bytes) of %d scanned\n",
			args[0], res.OrphanChunks, res.OrphanBytes, res.ScannedChunks)
		return nil
	}
	res, err := hub.CompactOrphanChunks(ctx, args[0], false)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "collected %s: %d chunks, %d bytes\n",
		args[0], res.CollectedChunks, res.CollectedBytes)
	return nil
}

func (a *App) newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status [flags] <project>",
		Short: "Show project health: degraded latch, streaks, pressure",
		Long: `Status reports operability in one place: whether the project
is latched degraded (and its consecutive-failure streak), the pending
op depth, and the hub pressure totals. A degraded project refuses new
mutations until re-enable clears the latch.

Examples:
  storhub status docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runStatus,
	}
	return cmd
}

func (a *App) runStatus(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	degraded := hub.DegradedProjects()
	streak := hub.PressureFailureStreak(args[0])
	depth := hub.PressurePendingDepth(args[0])
	snap := hub.PressureSnapshot()
	latched := false
	for _, name := range degraded {
		if name == args[0] {
			latched = true
			break
		}
	}
	state := "healthy"
	if latched {
		state = fmt.Sprintf("degraded (streak %d; re-enable with: storhub re-enable %s)", streak, args[0])
	}
	_, _ = fmt.Fprintf(a.stderr, "%s: %s, pending ops %d, commits %d ok / %d failed / %d rebased, cap-crosses %d\n",
		args[0], state, depth, snap.CommitSuccesses, snap.CommitFailures, snap.Rebases, snap.CapCrosses)
	return nil
}

func (a *App) newReEnableCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "re-enable [flags] <project>",
		Short: "Clear a project's degraded latch",
		Long: `Re-enable clears the degraded latch and admits mutations
again. It is the ONLY path back to healthy: commit successes never
clear the latch, so a sick backend cannot silently recover. Re-enabling
a healthy project succeeds as a no-op.

Examples:
  storhub re-enable docs-project`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runReEnable,
	}
	return cmd
}

func (a *App) runReEnable(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.ReEnableProject(args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "re-enabled %s\n", args[0])
	return nil
}

func (a *App) newDeleteProjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-project <project>",
		Short: "Delete an entire project repository",
		Long: `Delete-project removes the project's GitHub repository outright: every file,
directory, release, asset, and metadata revision is gone. This cannot be undone.

The serve-mode admin boundary covers the REST surface only: this
command runs with local-process trust and performs no admin check
(the REST project-delete endpoint is admin-gated).

The --yes flag is mandatory so a typo can never destroy a project.`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runDeleteProject,
	}
	cmd.Flags().Bool("yes", false, "Confirm deletion of the whole project")
	addSyncFlag(cmd)
	return cmd
}

func (a *App) runDeleteProject(cmd *cobra.Command, args []string) error {
	confirmed, _ := cmd.Flags().GetBool("yes")
	if !confirmed {
		return &usageError{fmt.Errorf("deleting project %q removes its repository, releases, and every file; pass --yes to confirm", args[0])}
	}
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	if err := hub.DeleteProject(args[0]); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd, cmd.Context(), args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "deleted project %s\n", args[0])
	return nil
}

func (a *App) newCacheCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Manage local cache directories",
		Args:  usageArgs(cobra.NoArgs),
	}
	cmd.AddCommand(a.newCachePurgeCmd())
	return cmd
}

func (a *App) newCachePurgeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "purge",
		Short: "Reclaim cache directories left by crashed processes",
		Long: `Cache purge removes storhub's local cache leftovers: per-project git worktrees
whose owning process is gone, and legacy pre-XDG temp roots. Directories held by
live processes are never touched. No network access, no token required.`,
		Args: usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			logger := shlog.NewLogger(shlog.Options{Level: shlog.LevelWarn, Format: shlog.FormatText, Output: a.stderr})
			reaped := storage.ReapOrphanedCaches(logger)
			unit := "entries"
			if reaped == 1 {
				unit = "entry"
			}
			_, _ = fmt.Fprintf(a.stderr, "reclaimed %d orphaned cache %s\n", reaped, unit)
			return nil
		},
	}
}

// addSyncFlag registers the --sync opt-in shared by every mutating
// command: after success the command drains the project's journal before
// exiting (fsync-class durability for scripts that delete the local source
// on success). One definition so the flag wording cannot drift per command.
func addSyncFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("sync", false, "Wait until the mutation is durably committed before exiting")
}

// drainIfSyncRequested honors --sync after a successful mutation: it drains
// the project's journal and surfaces drain errors loudly (a non-zero exit
// naming the project, via the returned error). Without --sync it is a
// no-op, so default paths never pay for durability they did not request.
func (a *App) drainIfSyncRequested(cmd *cobra.Command, ctx context.Context, project string) error {
	want, _ := cmd.Flags().GetBool("sync")
	if !want {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := a.hub.DrainProjectContext(ctx, project); err != nil {
		return fmt.Errorf("sync %s: %w", project, err)
	}
	return nil
}

// addFUSEFlags registers the flags every FUSE-mounting command shares.
// One definition: mount and serve must not drift apart on wording or
// defaults, because serve is mount plus REST over the same hub.
func addFUSEFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("allow-other", false, "Enable allow_other on the FUSE mount")
	cmd.Flags().Bool("debug", false, "Enable FUSE debug logging")
	cmd.Flags().String("cache-dir", "", "Optional cache directory")
	cmd.Flags().String("umask", "022", "Umask applied to created files and directories (octal 000-777); the FUSE protocol does not transmit the caller umask")
}

// addRESTFlags registers the flags every REST-serving command shares.
func addRESTFlags(cmd *cobra.Command) {
	cmd.Flags().String("listen", ":8080", "Listen address")
	cmd.Flags().String("base-path", "/api/v1", "REST API base path")
	cmd.Flags().String("auth-file", "", "Optional JSON auth config file (falls back to $STORHUB_REST_AUTH_FILE)")
	cmd.Flags().String("share-key", "", "Share signing key; defaults to one derived from the auth file's token_signing_key (falls back to $STORHUB_SHARE_SIGNING_KEY)")
	cmd.Flags().Bool("allow-anonymous", false, "Explicitly serve the API without authentication (insecure)")
}

func (a *App) newMountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mount [flags] <project> <mount-point>",
		Short: "FUSE mount a project",
		Long: `Mount exposes the project as a local filesystem over FUSE.
Press Ctrl+C to unmount; the unmount is retried while files stay open.

Examples:
  storhub mount docs-project ./mnt`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runMount,
	}
	addFUSEFlags(cmd)
	return cmd
}

func (a *App) newRestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rest [flags]",
		Short: "Start the REST API server",
		Long: `Rest serves the project's data over HTTP for the web console
and API clients. Serving without an auth file needs --allow-anonymous.

Examples:
  storhub rest --listen :8080 --allow-anonymous`,
		Args: usageArgs(cobra.NoArgs),
		RunE: a.runServeREST,
	}
	addRESTFlags(cmd)
	return cmd
}

func (a *App) Run(args []string) error {
	if args == nil {
		args = []string{}
	}
	a.rootCmd.SetArgs(args)
	a.rootCmd.SetOut(a.stdout)
	a.rootCmd.SetErr(a.stderr)
	_, err := a.rootCmd.ExecuteC()
	if flushErr := a.shutdownHub(); flushErr != nil {
		if err == nil {
			// The command succeeded but its commit point failed: exiting 0
			// here would be silent data loss (the `upload && rm ./local`
			// trap), so the flush error becomes the exit status.
			err = flushErr
		} else if a.stderr != nil {
			// The command already failed; keep its error primary but never
			// swallow the flush failure alongside it (via the primary
			// App warning sink).
			a.warnf("secondary flush failure: %v", flushErr)
		}
	}
	return err
}

// shutdownHub is the SINGLE owner of hub.Shutdown: the one and only
// place that drains the asynchronous metadata writer. Commands must
// never call Shutdown themselves - not rest, not mount, not serve - so
// there is exactly one drain point to reason about. Shutdown is part of
// the hubClient contract, so nothing reachable here can lack it. The
// writer is asynchronous, meaning a CLI mutation that exits without this
// loses data; that is precisely what the released-binary smoke test caught.
// A failed drain is returned, not printed-and-forgotten: a failed commit
// point must change the exit code.
func (a *App) shutdownHub() error {
	if a.hub == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), hubShutdownTimeout)
	defer cancel()
	if err := a.hub.Shutdown(ctx); err != nil {
		return fmt.Errorf("metadata flush failed: %w", err)
	}
	return nil
}

// Hub constructors record the client so Run can always flush it on exit.

// withSignalContext arms SIGINT/SIGTERM handling: a Ctrl+C cancels the
// returned context between units of work instead of killing the process
// mid-loop. Every long-running command shares this one arm point.
func withSignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// mustCmdHub resolves auth flags and builds the one-shot command hub in two
// lines at each run* call site: hub, err := a.mustCmdHub(cmd, 0, false).
func (a *App) mustCmdHub(cmd *cobra.Command, chunkSize int64, public bool) (hubClient, error) {
	token, apiBase := cmdAuth(cmd)
	return a.newCmdHub(resolveToken(token), apiBase, chunkSize, public)
}

// mustCmdHubCtx is mustCmdHub plus a signal context for the long one-shot
// maintenance operations (purge/rollback): hub, ctx, stop, err :=
// a.mustCmdHubCtx(cmd, 0, false); defer stop().
func (a *App) mustCmdHubCtx(cmd *cobra.Command, chunkSize int64, public bool) (hubClient, context.Context, context.CancelFunc, error) {
	hub, err := a.mustCmdHub(cmd, chunkSize, public)
	if err != nil {
		return nil, nil, nil, err
	}
	ctx, stop := withSignalContext()
	return hub, ctx, stop, nil
}

// parseNonNegativeArg parses a required non-negative int64 CLI argument,
// mirroring rest.parseNonNegativeInt but returning usageError (exit 2)
// instead of a 400. One helper so runWrite/runPatch never twin
// ParseInt+<0 blocks again.
func parseNonNegativeArg(s, name string) (int64, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, &usageError{fmt.Errorf("invalid %s %q: %w", name, s, err)}
	}
	if v < 0 {
		return 0, &usageError{fmt.Errorf("%s must be >= 0, got %d", name, v)}
	}
	return v, nil
}

func (a *App) newCmdHub(token, apiBase string, chunkSize int64, public bool) (hubClient, error) {
	hub, err := a.seamHub()(token, apiBase, chunkSize, public, a.log)
	if err == nil {
		a.hub = hub
	}
	return hub, err
}

// newCmdMountHub builds the hub for long-running interactive surfaces
// (mount): it records the client for Run's flush and uses the
// pause-to-reset rate policy.
func (a *App) newCmdMountHub(token, apiBase string) (hubClient, error) {
	hub, err := a.seamMountHub()(token, apiBase, a.log)
	if err == nil {
		a.hub = hub
	}
	return hub, err
}

func (a *App) newCmdRESTHub(token, apiBase string, chunkSize int64, public bool) (*storhub.StorHub, error) {
	hub, err := a.seamRESTHub()(token, apiBase, chunkSize, public, a.log)
	if err == nil {
		// rest/serve need the raw *StorHub for shrest.New; track the
		// wrapped form so Run can still flush pending metadata.
		a.hub = storhubClient{StorHub: hub}
	}
	return hub, err
}

func (a *App) logf(format string, args ...any) {
	if a.stderr == nil {
		return
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	_, _ = fmt.Fprintf(a.stderr, "%s storhub: %s\n", stamp, fmt.Sprintf(format, args...))
}

// readDataArg treats the literal "-" as "read the payload from stdin",
// matching standard filter convention.
func readDataArg(a *App, value string) ([]byte, error) {
	if value != "-" {
		return []byte(value), nil
	}
	data, err := io.ReadAll(a.stdin)
	if err != nil {
		return nil, fmt.Errorf("read stdin: %w", err)
	}
	return data, nil
}

// usageError marks flag/argument misuse so main can exit with 2 instead of 1.
type usageError struct{ err error }

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

// IsUsageError reports whether err stems from flag or argument misuse.
func IsUsageError(err error) bool {
	var u *usageError
	return errors.As(err, &u)
}

func usageArgs(validate cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := validate(cmd, args); err != nil {
			return &usageError{err}
		}
		return nil
	}
}

type restAuthFile struct {
	Realm           string        `json:"realm"`
	TokenSigningKey string        `json:"token_signing_key"`
	TokenTTL        flexDuration  `json:"token_ttl"`
	Users           []shrest.User `json:"users"`
}

// flexDuration is a Go duration string ("2h", "30m") for token_ttl.
// A bare JSON number (legacy: seconds) still decodes this release but logs
// a deprecation warning — time.Duration's own unmarshaler would read a
// number as nanoseconds (the difference between an hour and 3.6
// microseconds, silently), so numeric configs must migrate to strings.
type flexDuration time.Duration

func (d *flexDuration) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "null" {
		*d = 0
		return nil
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		parsed, err := time.ParseDuration(strings.Trim(raw, `"`))
		if err != nil {
			return fmt.Errorf("token_ttl: %w", err)
		}
		if parsed < 0 {
			return fmt.Errorf("token_ttl must not be negative, got %s", raw)
		}
		*d = flexDuration(parsed)
		return nil
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fmt.Errorf("token_ttl must be a duration string or seconds: %w", err)
	}
	// JSON numbers reach here as floats: NaN/Inf parse cleanly but convert
	// to garbage durations, and negatives are nonsense for a TTL. Reject
	// all three loudly instead of storing them.
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return fmt.Errorf("token_ttl must be finite, got %s", raw)
	}
	if seconds < 0 {
		return fmt.Errorf("token_ttl must not be negative, got %s", raw)
	}
	warnf("token_ttl as a bare number (%s) is deprecated; use a Go duration string like %q", raw, fmt.Sprintf("%ds", int64(seconds)))
	*d = flexDuration(time.Duration(seconds * float64(time.Second)))
	return nil
}

// Duration exposes the parsed value.
func (d flexDuration) Duration() time.Duration { return time.Duration(d) }

func (a *App) runUploadOrReplace(cmd *cobra.Command, args []string) error {
	chunkSize, _ := cmd.Flags().GetInt64("chunk-size")
	if chunkSize < 0 {
		return &usageError{fmt.Errorf("--chunk-size must be positive, got %d", chunkSize)}
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
	if err := a.drainIfSyncRequested(cmd, cmd.Context(), project); err != nil {
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
	if err := hub.DownloadFile(args[0], args[1], args[2]); err != nil {
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
	entries, err := hub.ReadDir(args[0], dir)
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
	entry, err := hub.StatPath(args[0], args[1])
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
	entry, err := hub.StatPath(args[0], args[1])
	if err != nil {
		return err
	}
	return streamCopyToStdout(hub, a.stdout, args[0], args[1], entry.Size)
}

// catWindowSize bounds resident memory while cat streams a stored file:
// each iteration fetches at most this many bytes instead of buffering the
// whole object, so multi-gigabyte files cannot OOM the CLI.
const catWindowSize = 1 << 20

func streamCopyToStdout(hub hubClient, w io.Writer, project, path string, size int64) error {
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
		n, err := hub.ReadFileAt(project, path, off, want)
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
	if err := hub.Mkdir(args[0], args[1]); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd, cmd.Context(), args[0]); err != nil {
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
	if err := a.drainIfSyncRequested(cmd, cmd.Context(), args[0]); err != nil {
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
	if err := a.drainIfSyncRequested(cmd, cmd.Context(), args[0]); err != nil {
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
	if err := a.drainIfSyncRequested(cmd, cmd.Context(), args[0]); err != nil {
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
	if err := a.drainIfSyncRequested(cmd, cmd.Context(), args[0]); err != nil {
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
	deleteSize, err := parseNonNegativeArg(args[3], "delete-size")
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
	if err := a.drainIfSyncRequested(cmd, cmd.Context(), args[0]); err != nil {
		return err
	}
	printFileSummary(a.stderr, "patched", meta)
	return nil
}

func (a *App) runRevisions(cmd *cobra.Command, args []string) error {
	hub, err := a.mustCmdHub(cmd, 0, false)
	if err != nil {
		return err
	}
	revs, err := hub.ListMetadataRevisions(args[0])
	if err != nil {
		return err
	}
	if v, _ := cmd.Flags().GetBool("json"); v {
		if revs == nil {
			revs = []storhub.MetadataRevision{}
		}
		return json.NewEncoder(a.stdout).Encode(revs)
	}
	printRevisions(a.stdout, revs)
	return nil
}

// commitSHAPattern matches git object ids: lowercase hex, 7 (shortest
// unambiguous abbreviation) through 64 characters — the same shape REST
// requireCommitSHA enforces, so a CLI typo is a usage error (exit 2),
// not a runtime failure (exit 1) deep inside storage.
var commitSHAPattern = regexp.MustCompile(`^[0-9a-f]{7,64}$`)

func (a *App) runRollback(cmd *cobra.Command, args []string) error {
	if !commitSHAPattern.MatchString(strings.TrimSpace(args[1])) {
		return &usageError{fmt.Errorf("invalid commit SHA %q: must be 7-64 lowercase hex characters", args[1])}
	}
	hub, ctx, stop, err := a.mustCmdHubCtx(cmd, 0, false)
	if err != nil {
		return err
	}
	defer stop()
	if err := hub.RollbackMetadataContext(ctx, args[0], args[1]); err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd, ctx, args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "rolled back %s to %s\n", args[0], args[1])
	return nil
}

func (a *App) runPurge(cmd *cobra.Command, args []string) error {
	scope := "all"
	if len(args) >= 2 {
		scope = args[1]
	}
	switch storhub.PurgeScope(scope) {
	case storhub.PurgeObjects, storhub.PurgeAssets, storhub.PurgeHistory, storhub.PurgeAll:
	default:
		return &usageError{fmt.Errorf("invalid purge scope %q (known: objects, assets, history, all)", scope)}
	}
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	keep, _ := cmd.Flags().GetInt("keep")
	if keep < 1 {
		return &usageError{fmt.Errorf("--keep must retain at least 1 manifest, got %d", keep)}
	}
	// Purging can run for minutes; a Ctrl+C must cancel it between delete
	// units instead of killing the process mid-loop.
	hub, ctx, stop, err := a.mustCmdHubCtx(cmd, 0, false)
	if err != nil {
		return err
	}
	defer stop()
	result, err := hub.PurgeContext(ctx, args[0], scope, keep, dryRun)
	if err != nil {
		return err
	}
	if err := a.drainIfSyncRequested(cmd, ctx, args[0]); err != nil {
		return err
	}
	verb := "purged"
	if dryRun {
		verb = "would purge"
	}
	_, _ = fmt.Fprintf(a.stderr, "%s %s (%s): %d objects, %d releases, %d assets",
		verb, args[0], result.Scope, result.DeletedObjects, result.DeletedReleases, result.DeletedAssets)
	if result.HistoryCompacted {
		_, _ = fmt.Fprint(a.stderr, ", history compacted")
	}
	_, _ = fmt.Fprintln(a.stderr)
	for _, note := range result.Notes {
		_, _ = fmt.Fprintf(a.stderr, "  note: %s\n", note)
	}
	return nil
}

// parseMountUmask parses the mount --umask flag value as an octal
// permission mask (000-777). Misuse is a usage error (exit 2), like the
// other flag validation in this file.
func parseMountUmask(raw string) (uint32, error) {
	v, err := strconv.ParseUint(strings.TrimSpace(raw), 8, 32)
	if err != nil || v > 0o777 {
		return 0, &usageError{fmt.Errorf("invalid --umask %q: must be an octal mode 000-777", raw)}
	}
	return uint32(v), nil
}

func (a *App) runMount(cmd *cobra.Command, args []string) error {
	token, apiBase := cmdAuth(cmd)
	allowOther, _ := cmd.Flags().GetBool("allow-other")
	debug, _ := cmd.Flags().GetBool("debug")
	cacheDir, _ := cmd.Flags().GetString("cache-dir")
	umaskRaw, _ := cmd.Flags().GetString("umask")
	umask, err := parseMountUmask(umaskRaw)
	if err != nil {
		return err
	}
	// mount is a long-running interactive surface: it must get the
	// pause-to-reset rate policy, not the one-shot fail-fast default.
	hub, err := a.newCmdMountHub(resolveToken(token), apiBase)
	if err != nil {
		return err
	}
	opts := storhub.DefaultFUSEOptions()
	opts.AllowOther = allowOther
	opts.Debug = debug
	opts.CacheDir = cacheDir
	opts.Umask = umask
	opts.UmaskSet = true
	// Arm signal handling before touching FUSE: an interrupt arriving during
	// mount setup must not fall through to the default disposition and kill
	// the process with a half-attached mount left behind.
	ctx, stop := withSignalContext()
	defer stop()
	fsys, err := a.setupServeMount(ctx, args[0], args[1], opts, hub.NewFUSE)
	if err != nil {
		return err
	}
	defer func() {
		if err := fsys.Close(); err != nil {
			_, _ = fmt.Fprintf(a.stderr, "warning: closing filesystem session: %v\n", err)
		}
	}()
	_, _ = fmt.Fprintf(a.stderr, "mounted %s at %s\n", args[0], args[1])
	_, _ = fmt.Fprintln(a.stderr, "press Ctrl+C to unmount")
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		fsys.Wait()
	}()
	select {
	case <-waitDone:
		// The server stopped on its own (unmounted externally).
		return nil
	case <-ctx.Done():
		// Restore the default signal disposition first: pressing Ctrl+C
		// again force-quits instead of queueing more polite unmounts.
		stop()
		unmountWithRetry(fsys, args[1], a.stderr)
		// fsys.Wait() only returns after a SUCCESSFUL unmount. When the
		// retry budget gave up, an unbounded join here would hang the CLI
		// forever instead of exiting; bound it and report failure loudly.
		if !joinWithin(waitDone, unmountJoinTimeout) {
			_, _ = fmt.Fprintf(a.stderr, "mount session did not end within %s after unmount; %s\n", unmountJoinTimeout, mayStillBeMounted(args[1]))
			return fmt.Errorf("unmount of %s did not complete; giving up on the mount session", args[1])
		}
		return nil
	}
}

// mayStillBeMounted renders the shared teardown suffix so mount, serve,
// and the unmount retry loop never drift apart on wording.
func mayStillBeMounted(target string) string {
	return fmt.Sprintf("%s may still be mounted", target)
}

// joinWithin waits for done to close, giving up after timeout. It reports
// whether the join completed. Teardown joins must never be unbounded: a
// wedged FUSE server or listener goroutine would otherwise turn a failed
// unmount into a hung process.
func joinWithin(done <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// unmountWithRetry retries the unmount until it succeeds or its retry budget
// runs out. An unmount fails with EBUSY while any file on the mount is still
// open, so holders get a grace period instead of either hanging forever or
// silently leaking the mount. Only the first failure prints the advisory;
// later attempts are silent (the outcome line always prints), so teardown
// of a busy mount does not spam. A Ctrl+C during the backoff sleep aborts
// the wait early via a signal-scoped context: the callers already consumed
// their signal context to reach teardown, so the loop arms its own SIGINT
// watch (SIGTERM keeps the default kill disposition).
func unmountWithRetry(fsys fuseMount, target string, report io.Writer) {
	sigCtx, sigStop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer sigStop()
	delay := unmountRetryBaseDelay
	deadline := time.Now().Add(unmountRetryBudget)
	for attempt := 1; ; attempt++ {
		err := fsys.Unmount()
		if err == nil {
			if attempt > 1 {
				_, _ = fmt.Fprintf(report, "unmounted %s\n", target)
			}
			return
		}
		if attempt == 1 {
			_, _ = fmt.Fprintf(report, "unmount failed (%v); close programs using %s and wait, or press Ctrl+C again to quit\n", err, target)
		}
		if time.Now().After(deadline) {
			_, _ = fmt.Fprintf(report, "giving up on unmount after %d attempts; %s\n", attempt, mayStillBeMounted(target))
			return
		}
		if err := storcfg.SleepWithContext(sigCtx, delay); err != nil {
			_, _ = fmt.Fprintf(report, "unmount interrupted; %s\n", mayStillBeMounted(target))
			return
		}
		if delay < unmountBackoffCap {
			delay *= 2
		}
	}
}

var (
	unmountRetryBaseDelay = storcfg.PatienceUnit / 5
	unmountRetryBudget    = 6 * storcfg.PatienceUnit
)

// Teardown join bounds. After unmountWithRetry returns (success or giving
// up), the session goroutines must end promptly; if they do not, the
// mount/listener is wedged and the process must exit non-zero instead of
// hanging on an unbounded channel receive.
const (
	unmountJoinTimeout = 2 * storcfg.PatienceUnit
	restJoinTimeout    = 2 * storcfg.PatienceUnit
)

func (a *App) runServeREST(cmd *cobra.Command, args []string) error {
	token, apiBase := cmdAuth(cmd)
	listen, _ := cmd.Flags().GetString("listen")
	hub, err := a.newCmdRESTHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	opts, err := serveAuthOptions(cmd)
	if err != nil {
		return err
	}
	handler, err := a.buildRESTHandler(hub, opts)
	if err != nil {
		return err
	}
	server := newRESTServer(listen, handler)
	_, _ = fmt.Fprintf(a.stderr, "serving REST API on %s%s %s\n", listen, opts.BasePath, describeRESTAuth(opts))
	return a.serveRESTUntilSignal(server)
}

// serveAuthOptions resolves the shared REST serving policy: base path and
// authentication. Running without an auth file is a deliberate choice -
// require the explicit opt-in flag so an open server never happens by
// accident.
func serveAuthOptions(cmd *cobra.Command) (shrest.Options, error) {
	basePath, _ := cmd.Flags().GetString("base-path")
	authFile, _ := cmd.Flags().GetString("auth-file")
	opts := shrest.DefaultOptions()
	opts.BasePath = basePath
	if authFile == "" {
		authFile = os.Getenv("STORHUB_REST_AUTH_FILE")
	}
	if strings.TrimSpace(authFile) != "" {
		if noAuth, _ := cmd.Flags().GetBool("allow-anonymous"); noAuth {
			// --allow-anonymous would be silently ignored here; contradictory
			// auth intent is a command-line mistake, not a runtime failure.
			return opts, &usageError{errors.New("--allow-anonymous has no effect when an auth file is supplied; drop one of them")}
		}
		auth, err := loadRESTAuthOptions(authFile)
		if err != nil {
			return opts, err
		}
		opts.Auth = auth
	} else {
		noAuth, _ := cmd.Flags().GetBool("allow-anonymous")
		if !noAuth {
			// Same misuse class as the contradictory-flags case above:
			// serving wide open without the explicit opt-in flag is a
			// command-line mistake, so usageError (exit 2), not exit 1.
			return opts, &usageError{fmt.Errorf("refusing to serve unauthenticated REST API; provide --auth-file or pass --allow-anonymous")}
		}
		opts.AllowAnonymous = true
	}
	shareKey := strings.TrimSpace(shareSigningKey(cmd))
	if shareKey != "" {
		opts.ShareSigningKey = []byte(shareKey)
	}
	return opts, nil
}

// shareSigningKey resolves the explicit share key: flag first, then env. An
// empty result lets the server derive one from the auth signing key.
func shareSigningKey(cmd *cobra.Command) string {
	if key, _ := cmd.Flags().GetString("share-key"); strings.TrimSpace(key) != "" {
		return key
	}
	return os.Getenv("STORHUB_SHARE_SIGNING_KEY")
}

// buildRESTHandler builds the REST handler. It returns the REST layer's
// own handler directly: request logging lives in exactly one layer
// (rest.requestLogging), so the former CLI loggingMiddleware wrapper was
// removed from this chain. The middleware method stays (deprecated) for
// non-HTTP chatter via App.logf and existing tests.
func (a *App) buildRESTHandler(hub *storhub.StorHub, opts shrest.Options) (http.Handler, error) {
	return a.seamRESTHandler()(hub, opts)
}

func describeRESTAuth(opts shrest.Options) string {
	if opts.Auth != nil {
		return "with auth"
	}
	return "without auth"
}

func newRESTServer(listen string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              listen,
		Handler:           handler,
		ReadHeaderTimeout: restReadHeaderBudget,
		// No whole-request ReadTimeout: it bounds body reads too, killing
		// large uploads mid-flight (a 400 MB file over a 15 MB/s link dies
		// at 30 s with "context canceled" cascading into GitHub). Slow-loris
		// protection lives in ReadHeaderTimeout; body abuse is bounded by
		// explicit size enforcement in the content handlers instead.
		ReadTimeout: 0,
		// Same argument applies to WriteTimeout: it bounds the whole
		// response body, so any fixed value kills a large or slow download
		// mid-transfer (a 5 GB file at 15 MB/s needs ~6 min; 400 MB at a
		// tenth that dies at 5 m). Response size and duration are bounded
		// by the content handlers, not by amputating the write deadline.
		WriteTimeout: 0,
		IdleTimeout:  restIdleBudget,
	}
}

// newServeCmd registers `storhub serve`: FUSE mount and REST API from one
// process over ONE shared hub. Sharing the hub is the point - both surfaces
// see each other's writes immediately, share one metadata writer and one
// cache claim; two hubs would fight over the git cache lock and double-flush
// metadata.
func (a *App) newServeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve <project> <mount-point>",
		Short: "Mount FUSE and serve the REST API together",
		Long: `Serve mounts the project over FUSE and exposes the same data through
the REST API, from one process over one shared hub: writes through the
mount are visible to REST clients immediately and vice versa.

On Ctrl+C the mount is unmounted and in-flight HTTP requests drained
before pending metadata is flushed.`,
		Args: usageArgs(cobra.ExactArgs(2)),
		RunE: a.runServe,
	}
	addFUSEFlags(cmd)
	addRESTFlags(cmd)
	return cmd
}

func (a *App) runServe(cmd *cobra.Command, args []string) error {
	token, apiBase := cmdAuth(cmd)
	allowOther, _ := cmd.Flags().GetBool("allow-other")
	debug, _ := cmd.Flags().GetBool("debug")
	cacheDir, _ := cmd.Flags().GetString("cache-dir")
	umaskRaw, _ := cmd.Flags().GetString("umask")
	listen, _ := cmd.Flags().GetString("listen")

	hub, err := a.newCmdRESTHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	fuseOpts := storhub.DefaultFUSEOptions()
	fuseOpts.AllowOther = allowOther
	fuseOpts.Debug = debug
	fuseOpts.CacheDir = cacheDir
	umask, err := parseMountUmask(umaskRaw)
	if err != nil {
		return err
	}
	fuseOpts.Umask = umask
	fuseOpts.UmaskSet = true

	// Arm signal handling before touching FUSE: an interrupt arriving during
	// setup must not kill the process with a half-attached mount left behind.
	ctx, stop := withSignalContext()
	defer stop()
	newFUSE := a.seamFUSE()
	fsys, err := a.setupServeMount(ctx, args[0], args[1], fuseOpts, func(project string, opts storhub.FUSEOptions) (fuseMount, error) {
		return newFUSE(hub, project, opts)
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := fsys.Close(); err != nil {
			_, _ = fmt.Fprintf(a.stderr, "warning: closing filesystem session: %v\n", err)
		}
	}()
	// Single owner of fsys.Wait: everything below joins through fsDone.
	fsDone := fsWait(fsys)
	// abort stops a half-started serve: the mount comes down before the
	// error surfaces, so neither surface is left running unpaired.
	abort := func(err error) error {
		stop()
		unmountWithRetry(fsys, args[1], a.stderr)
		if !joinWithin(fsDone, unmountJoinTimeout) {
			_, _ = fmt.Fprintf(a.stderr, "mount session did not end within %s after unmount; %s\n", unmountJoinTimeout, mayStillBeMounted(args[1]))
			return errors.Join(err, fmt.Errorf("unmount of %s did not complete", args[1]))
		}
		return err
	}
	server, opts, err := a.setupServeREST(cmd, hub, args[0], listen)
	if err != nil {
		return abort(err)
	}
	errCh := make(chan error, 1)
	errDone := make(chan struct{})
	go func() {
		defer close(errDone)
		errCh <- a.seamListen()(server)
	}()

	_, _ = fmt.Fprintf(a.stderr, "mounted %s at %s\n", args[0], args[1])
	_, _ = fmt.Fprintf(a.stderr, "serving REST API on %s%s %s\n", listen, opts.BasePath, describeRESTAuth(opts))
	_, _ = fmt.Fprintln(a.stderr, "press Ctrl+C to stop")

	return a.joinServe(ctx, stop, fsys, args[1], server, fsDone, errCh, errDone)
}

// setupServeMount creates the FUSE instance, attaches it at mountPoint, and
// fails closed on an interrupt arriving mid-setup. open creates the session
// (hub.NewFUSE for mount, the FUSE seam for serve) so both commands share
// the MkdirAll→Mount→interrupt-check flow instead of twinning it.
func (a *App) setupServeMount(ctx context.Context, project, mountPoint string, fuseOpts storhub.FUSEOptions, open func(string, storhub.FUSEOptions) (fuseMount, error)) (fuseMount, error) {
	fsys, err := open(project, fuseOpts)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(mountPoint, mountDirPerm); err != nil {
		_ = fsys.Close()
		return nil, err
	}
	if err := fsys.Mount(mountPoint); err != nil {
		_ = fsys.Close()
		return nil, err
	}
	if ctx.Err() != nil {
		if uerr := fsys.Unmount(); uerr != nil {
			_, _ = fmt.Fprintf(a.stderr, "warning: interrupted during mount; unmount failed (%v); %s\n", uerr, mayStillBeMounted(mountPoint))
		}
		_ = fsys.Close()
		return nil, errors.New("interrupted while mounting " + project)
	}
	return fsys, nil
}

// setupServeREST resolves auth policy and builds the REST server for serve.
// The REST surface is pinned to the served project.
func (a *App) setupServeREST(cmd *cobra.Command, hub *storhub.StorHub, project, listen string) (*http.Server, shrest.Options, error) {
	opts, err := serveAuthOptions(cmd)
	if err != nil {
		return nil, opts, err
	}
	// `serve <project> <mount>`: the REST surface (and thus the web console)
	// is pinned to that project - no free-form selector needed.
	opts.DefaultProject = project
	handler, err := a.buildRESTHandler(hub, opts)
	if err != nil {
		return nil, opts, err
	}
	return newRESTServer(listen, handler), opts, nil
}

// joinServe waits for the first surface to stop, then tears the other down
// in ORDER: drain HTTP before pulling the mount out from under in-flight
// readers, join both goroutines, and only then let Run flush metadata.
// A plain select with explicit joins keeps that contract visible (not errgroup).
func (a *App) joinServe(ctx context.Context, stop context.CancelFunc, fsys fuseMount, mountPoint string, server *http.Server, fsDone <-chan struct{}, errCh <-chan error, errDone <-chan struct{}) error {
	var serveErr error
	fsDown := false
	select {
	case <-fsDone:
		// The mount went away on its own (unmounted externally).
		fsDown = true
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			// The listener died; tear the mount down too so neither
			// surface outlives the other half-configured.
			serveErr = err
		}
	case <-ctx.Done():
	}
	// Both surfaces can end at once; fold whatever else is already
	// decided so a simultaneous failure is never swallowed.
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && serveErr == nil {
			serveErr = err
		}
	default:
	}
	if !fsDown {
		select {
		case <-fsDone:
			fsDown = true
		default:
		}
	}
	// Restore the default signal disposition first: pressing Ctrl+C again
	// force-quits instead of queueing more polite stops.
	stop()
	// Drain in-flight requests before pulling the mount out from under
	// any client that might still be reading through it.
	shutdownRESTServer(server, a.stderr)
	if !fsDown {
		unmountWithRetry(fsys, mountPoint, a.stderr)
	}
	// Both joins are bounded: fsys.Wait() only returns after a successful
	// unmount, and a wedged listener goroutine must not turn shutdown into
	// a hang. On timeout the process exits non-zero (loudly) instead of
	// blocking forever with the mount retained.
	var joinErr error
	if !joinWithin(fsDone, unmountJoinTimeout) {
		_, _ = fmt.Fprintf(a.stderr, "mount session did not end within %s after unmount; %s\n", unmountJoinTimeout, mayStillBeMounted(mountPoint))
		joinErr = errors.Join(joinErr, fmt.Errorf("unmount of %s did not complete", mountPoint))
	}
	// Join the listener goroutine so nothing outlives this function.
	if !joinWithin(errDone, restJoinTimeout) {
		_, _ = fmt.Fprintf(a.stderr, "REST listener did not stop within %s; abandoning the join\n", restJoinTimeout)
		joinErr = errors.Join(joinErr, fmt.Errorf("REST listener did not stop within %s", restJoinTimeout))
	}
	// Metadata draining is NOT done here: Run's shutdownHub is the single
	// owner of hub.Shutdown for every command.
	return errors.Join(serveErr, joinErr)
}

func shutdownRESTServer(server *http.Server, report io.Writer) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), restShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && report != nil {
		_, _ = fmt.Fprintf(report, "graceful shutdown failed: %v\n", err)
	}
}

// fsWait runs fsys.Wait once and yields a channel that closes when it
// returns, so teardown paths can join the mount goroutine without racing
// a second Wait call.
func fsWait(fsys fuseMount) <-chan struct{} {
	done := make(chan struct{})
	go func() { defer close(done); fsys.Wait() }()
	return done
}

// serveRESTUntilSignal runs the REST server and drains it cleanly on
// SIGINT/SIGTERM: in-flight requests finish within a bounded shutdown
// window, then pending metadata is flushed before exit. A clean stop is not
// reported as an error.
func (a *App) serveRESTUntilSignal(server *http.Server) error {
	ctx, stop := withSignalContext()
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- a.seamListen()(server) }()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			return err
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), restShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_, _ = fmt.Fprintf(a.stderr, "graceful shutdown failed: %v\n", err)
	}
	// Metadata draining is NOT done here: Run's shutdownHub is the single
	// owner of hub.Shutdown for every command. Draining after the HTTP
	// server stops is exactly the right order - in-flight requests can
	// still mutate metadata, and shutdownHub commits all of it.
	return nil
}

// httpLogsEnabled reports whether the configured level wants per-request
// HTTP lines; at error level they are pure noise.
func (a *App) httpLogsEnabled() bool {
	level := shlog.NormalizeLevel(a.log.level)
	return level == shlog.LevelDebug || level == shlog.LevelInfo || level == shlog.LevelWarn
}

// loggingMiddleware is deprecated: buildRESTHandler no longer wraps the
// REST handler with it (single log layer: rest.requestLogging). Kept for
// backward compatibility with existing tests; new code must not wire it
// into the HTTP chain. App.logf stays for non-HTTP chatter.
func (a *App) loggingMiddleware(next http.Handler) http.Handler {
	if next == nil {
		// A nil inner handler must never degrade into http.Server's
		// DefaultServeMux fallback: answer every request with a loud 500
		// instead of silently serving the wrong thing.
		next = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "rest handler unavailable", http.StatusInternalServerError)
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := shlog.NewHTTPRecorder(w)
		if a.httpLogsEnabled() {
			a.logf("http start: method=%s uri=%s remote=%s", r.Method, shlog.RedactRequestURI(r.URL.RequestURI()), r.RemoteAddr)
		}
		next.ServeHTTP(wrapped, r)
		if a.httpLogsEnabled() {
			a.logf("http done: method=%s uri=%s status=%d duration=%s", r.Method, shlog.RedactRequestURI(r.URL.RequestURI()), wrapped.Status(), time.Since(start).Round(time.Millisecond))
		}
	})
}

// statusRecorder was removed: status/byte capture lives in
// internal/logging (HTTPRecorder) so one package owns one job.
// See logging/http_recorder.go.

func loadRESTAuthOptions(filePath string) (*shrest.AuthOptions, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var file restAuthFile
	// Unknown fields are typos in security-relevant config (a misspelled
	// token_ttl silently keeps the default TTL), so reject them loudly.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("decode rest auth file: %w", err)
	}
	key := []byte(strings.TrimSpace(file.TokenSigningKey))
	if len(key) == 0 {
		return nil, errors.New("rest auth file requires token_signing_key")
	}
	return &shrest.AuthOptions{
		Realm:           file.Realm,
		Users:           file.Users,
		TokenSigningKey: key,
		TokenTTL:        file.TokenTTL.Duration(),
	}, nil
}

// newHubConfig builds the hub configuration every CLI constructor shares.
// longRunning selects the rate-limit policy documented on applyRateEnv:
// interactive/daemon surfaces (mount, rest, serve) pause up to the reset,
// one-shot commands fail fast. Log knobs, the journal location, and the
// chunk-size clamps are applied for ALL commands - a surface that silently
// ignores --log-level is a bug waiting to be filed.
func newHubConfig(apiBase string, chunkSize int64, public bool, log logSettings, longRunning bool) storcfg.Config {
	cfg := storhub.DefaultConfig()
	if strings.TrimSpace(apiBase) != "" {
		cfg.APIBaseURL = apiBase
	}
	if normalized := normalizeCLIChunkSize(chunkSize); normalized > 0 {
		if normalized != chunkSize {
			warnf("--chunk-size %d outside [%d, %d]; using %d",
				chunkSize, minCLIChunkSize, chunking.MaxReleaseAssetSize, normalized)
		}
		cfg.ChunkSize = normalized
	}
	cfg.CreatePublicRepo = public
	cfg.LogLevel = log.level
	cfg.LogFormat = log.format
	cfg.LogColor = log.color
	// The write-ahead journal lives beneath the cache base: crash-safe
	// replay is exactly what the mutating CLI surfaces need, so the CLI
	// opts in by default (config.go documents this defaulting).
	cfg.JournalDir = filepath.Join(storcfg.CacheBase(), "journal")
	applyRateEnv(&cfg, longRunning)
	return cfg
}

// newHubFromFlags builds the hub for one-shot commands: fail-fast rate
// policy, chunk-size and public-repo flags honored.
func newHubFromFlags(token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error) {
	token = resolveToken(token)
	if token == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	return storhub.NewStorHubWithConfig(token, newHubConfig(apiBase, chunkSize, public, log, false))
}

// newMountHubFromFlags builds the hub for the long-running mount surface:
// pause-to-reset rate policy so an interactive session rides out a
// secondary rate limit instead of dying instantly.
func newMountHubFromFlags(token, apiBase string, log logSettings) (*storhub.StorHub, error) {
	token = resolveToken(token)
	if token == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	return storhub.NewStorHubWithConfig(token, newHubConfig(apiBase, 0, false, log, true))
}

// applyRateEnv layers the rate-governor environment variables onto a hub
// config. One-shot commands fail fast on rate limits by default (a wait
// cannot help a script), while long-running commands - rest, mount, serve
// - may pause up to the documented reset before giving up.
func applyRateEnv(cfg *storcfg.Config, longRunning bool) {
	defaultMaxWait := -1 * time.Second // fail-fast sentinel, not a duration: do not unit-derive
	if longRunning {
		defaultMaxWait = 180 * storcfg.PatienceUnit
	}
	cfg.RateReserve = parseEnvInt64("STORHUB_RATE_RESERVE", cfg.RateReserve)
	cfg.RateMaxWait = parseEnvDuration("STORHUB_RATE_MAX_WAIT", defaultMaxWait)
	if _, set := os.LookupEnv("STORHUB_RATE_MAX_WAIT"); set && cfg.RateMaxWait == 0 {
		warnf("STORHUB_RATE_MAX_WAIT=0 means \"not configured\": the library default (15m) applies; use a negative duration such as -1s for fail-fast")
	}
	cfg.RatePointsPerMin = parseEnvInt64("STORHUB_RATE_POINTS_PER_MIN", cfg.RatePointsPerMin)
	cfg.RateContentPerMin = parseEnvInt64("STORHUB_RATE_CONTENT_PER_MIN", cfg.RateContentPerMin)
	cfg.MaxConcurrentRequests = parseEnvInt64("STORHUB_MAX_CONCURRENT", cfg.MaxConcurrentRequests)
	cfg.MaxConsecutiveCommitFailures = int(parseEnvInt64("STORHUB_MAX_CONSECUTIVE_FAILURES", int64(cfg.MaxConsecutiveCommitFailures)))
	cfg.TransferThroughput = parseEnvInt64("STORHUB_TRANSFER_THROUGHPUT", cfg.TransferThroughput)
	for _, neg := range []struct {
		key   string
		value int64
	}{
		{"STORHUB_RATE_POINTS_PER_MIN", cfg.RatePointsPerMin},
		{"STORHUB_RATE_CONTENT_PER_MIN", cfg.RateContentPerMin},
		{"STORHUB_MAX_CONCURRENT", cfg.MaxConcurrentRequests},
	} {
		if neg.value < 0 {
			warnf("%s=%d is negative; the rate governor silently replaces it with the library default", neg.key, neg.value)
		}
	}
}

// envOr is the single generic env reader: empty means fallback, parse
// errors warn uniformly via warnEnvParse and fall back. The typed wrappers
// below exist so call sites read as policy, not parse plumbing.
func envOr[T any](key string, fallback T, parse func(string) (T, error)) T {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := parse(value)
	if err != nil {
		warnEnvParse(key, value, err)
		return fallback
	}
	return parsed
}

func parseEnvInt64(key string, fallback int64) int64 {
	return envOr(key, fallback, func(s string) (int64, error) {
		return strconv.ParseInt(s, 10, 64)
	})
}

func parseEnvDuration(key string, fallback time.Duration) time.Duration {
	return envOr(key, fallback, time.ParseDuration)
}

// resolveToken prefers the explicit --token value and falls back to
// $GITHUB_TOKEN. The token is never rendered into help output.
func resolveToken(flagValue string) string {
	if strings.TrimSpace(flagValue) != "" {
		return flagValue
	}
	return os.Getenv("GITHUB_TOKEN")
}

func envOrDefault(key, fallback string) string {
	return envOr(key, fallback, func(s string) (string, error) { return s, nil })
}

func parseEnvBool(key string, fallback bool) bool {
	return envOr(key, fallback, strconv.ParseBool)
}

// warnEnvParse reports an invalid STORHUB_* value that fell back to its
// default. The fallback preserves behavior; the warning makes the
// misconfiguration visible instead of silent.
func warnEnvParse(key, value string, err error) {
	warnf("invalid %s=%q (%v); using default", key, value, err)
}

// cmdAuth extracts the auth flags every command shares: explicit --token
// (resolved against $GITHUB_TOKEN) plus --api-base.
func cmdAuth(cmd *cobra.Command) (token, apiBase string) {
	token, _ = cmd.Flags().GetString("token")
	apiBase, _ = cmd.Flags().GetString("api-base")
	return token, apiBase
}

// Named timeouts/budgets so call sites read as policy, not literals.
const (
	hubShutdownTimeout   = 6 * storcfg.PatienceUnit
	restShutdownTimeout  = 2 * storcfg.PatienceUnit
	restReadHeaderBudget = 1 * storcfg.PatienceUnit
	restIdleBudget       = 24 * storcfg.PatienceUnit
	unmountBackoffCap    = 2 * storcfg.PatienceUnit
	mountDirPerm         = 0o755
)

func formatTime(t int64) string {
	if t == 0 {
		return "-"
	}
	return time.Unix(0, t).Format(time.RFC3339)
}
