// Package cli is the cobra terminal surface: one binary, git-style
// subcommand groups (project, file verbs, session, mount, serve) with
// data on stdout, status chatter on stderr, and usage errors as exit 2.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
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

// App is one CLI instance with injected I/O, hub, and seams: two Apps in
// one process (tests, embedders) never share flag targets or sinks.
type App struct {
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	warnOut io.Writer
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
	UploadFileContext(ctx context.Context, project, remotePath, localPath string) (*storhub.FileMetadata, error)
	ReplaceFileContext(ctx context.Context, project, remotePath, localPath string, opts ...storhub.MutateOption) (*storhub.FileMetadata, error)
	DownloadFileContext(ctx context.Context, project, remotePath, localPath string) error
	ReadDirContext(ctx context.Context, project, dir string) ([]storhub.DirEntry, error)
	StatPathContext(ctx context.Context, project, targetPath string) (*storhub.EntryInfo, error)
	ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error)
	MkdirContext(ctx context.Context, project, dirPath string) error
	DeleteFileContext(ctx context.Context, project, filePath string, opts ...storhub.MutateOption) error
	RmdirContext(ctx context.Context, project, dirPath string, opts ...storhub.MutateOption) error
	RenameContext(ctx context.Context, project, oldPath, newPath string, opts ...storhub.MutateOption) error
	AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...storhub.MutateOption) (*storhub.FileMetadata, error)
	WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...storhub.MutateOption) (*storhub.FileMetadata, error)
	PatchFileContext(ctx context.Context, project, filePath string, offset, deleteSize int64, edit []byte, opts ...storhub.MutateOption) (*storhub.FileMetadata, error)
	// POSIX verbs backing the truncate/chmod/chown/touch/symlink/readlink/
	// link/sync commands. Signatures mirror *storage.StorHub directly so
	// storhubClient satisfies them through its embedded hub.
	CreateFileContext(ctx context.Context, project, filePath string) (*storhub.FileMetadata, error)
	TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...storhub.MutateOption) (*storhub.FileMetadata, error)
	ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error
	ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error
	ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error
	SymlinkContext(ctx context.Context, project, target, linkPath string) (*storhub.FileMetadata, error)
	ReadlinkContext(ctx context.Context, project, linkPath string) (string, error)
	LinkContext(ctx context.Context, project, existingPath, newPath string) (*storhub.FileMetadata, error)
	ListMetadataRevisionsContext(ctx context.Context, project string) ([]storhub.MetadataRevision, error)
	// The long one-shot maintenance operations take a context so a
	// Ctrl+C cancels them between units of work instead of killing the
	// process mid-delete-loop.
	RollbackMetadataContext(ctx context.Context, project, commitSHA string) error
	PruneContext(ctx context.Context, project, scope string, keep int, dryRun bool) (*storhub.PruneResult, error)
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
// callers. Atomic so parallel tests swapping the seam never race; tests
// must use setWarnOutput, never assign the variable directly.
var warnOutput atomic.Pointer[io.Writer]

// setWarnOutput swaps the pre-App warning sink, returning a restore func.
// Test-only seam: prod never calls it.
func setWarnOutput(w io.Writer) func() {
	old, ok := warnOutput.Load(), true
	if w == nil {
		v := io.Writer(os.Stderr)
		w = v
	}
	warnOutput.Store(&w)
	if !ok {
		return func() {}
	}
	return func() { warnOutput.Store(old) }
}

func warnSink() io.Writer {
	if p := warnOutput.Load(); p != nil && *p != nil {
		return *p
	}
	return os.Stderr
}

// warnf prints a storhub-prefixed warning to warnOutput (pre-App fallback).
func warnf(format string, args ...any) {
	_, _ = fmt.Fprintf(warnSink(), "%s storhub: warning: "+format+"\n",
		append([]any{time.Now().UTC().Format(time.RFC3339)}, args...)...)
}

// warnf is the primary warning sink: App.warnOut (stderr by default,
// swappable per-App in tests). Package-level warnf above remains only
// for pre-App constructors without an App handle.
func (a *App) warnf(format string, args ...any) {
	out := a.warnOut
	if out == nil {
		out = a.stderr
	}
	if out == nil {
		out = warnSink()
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

// New returns an App wired to process stdio with default settings.
func New() *App {
	a := &App{stdin: os.Stdin, stdout: io.Writer(os.Stdout), stderr: io.Writer(os.Stderr), warnOut: io.Writer(os.Stderr), log: defaultLogSettings(), seams: defaultCliSeams()}
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
	rootCmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
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
	rootCmd.AddCommand(a.newProjectCmd())
	rootCmd.AddCommand(a.newCacheCmd())
	rootCmd.AddCommand(a.newSessionCmd())
	rootCmd.AddCommand(a.newMountCmd())
	rootCmd.AddCommand(a.newRestCmd())
	rootCmd.AddCommand(a.newServeCmd())

	// An unknown command is as much a command-line mistake as a bad flag:
	// both must classify as usage errors so main exits 2. Making the root
	// runnable routes every unmatched first word through the Args
	// validator, where it is wrapped as a usageError.
	rootCmd.RunE = func(cmd *cobra.Command, _ []string) error {
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
func (a *App) drainIfSyncRequested(ctx context.Context, cmd *cobra.Command, project string) error {
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

// Run executes args against the App: usage errors (exit 2 shapes) stay
// distinguishable from operation failures (exit 1 shapes) via IsUsageError.
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
