// Package cli is the cobra terminal surface: one binary, git-style
// subcommand groups (project, file verbs, session, mount, serve) with
// data on stdout, status chatter on stderr, and usage errors as exit 2.
package cli

import (
	"context"
	"errors"
	"fmt"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shlog "github.com/FarelRA/storhub/internal/logging"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
		level:  envOrDefault(storcfg.EnvLogLevel, "info"),
		format: envOrDefault(storcfg.EnvLogFormat, "pretty"),
		color:  parseEnvBool(storcfg.EnvLogColor, true),
	}
}

// cliSeams injects every external constructor the CLI shells out to.
// Per-App state (not package globals) so parallel tests drive isolated
// Apps without racing on shared seam vars. Hub constructors take ctx
// first: hub lifetime binds to the caller's scope.
type cliSeams struct {
	newHub      func(ctx context.Context, token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error)
	newRESTHub  func(ctx context.Context, token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error)
	newMountHub func(ctx context.Context, token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error)
	newFUSE     func(hub *storhub.StorHub, project string, opts storhub.FUSEOptions) (fuseMount, error)
	newREST     func(hub *storhub.StorHub, opts storhub.RESTOptions) (http.Handler, error)
	listenServe func(server *http.Server) error
}

func defaultCliSeams() cliSeams {
	return cliSeams{
		newHub: func(ctx context.Context, token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
			hub, err := newHubFromFlags(ctx, token, apiBase, chunkSize, public, log)
			if err != nil {
				return nil, err
			}
			return storhubClient{StorHub: hub}, nil
		},
		newRESTHub: newLongRunningHubFromFlags,
		newMountHub: func(ctx context.Context, token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
			hub, err := newLongRunningHubFromFlags(ctx, token, apiBase, chunkSize, public, log)
			if err != nil {
				return nil, err
			}
			return storhubClient{StorHub: hub}, nil
		},
		newFUSE: func(hub *storhub.StorHub, project string, opts storhub.FUSEOptions) (fuseMount, error) {
			return hub.NewFUSE(project, opts)
		},
		newREST: func(hub *storhub.StorHub, opts storhub.RESTOptions) (http.Handler, error) {
			return storhub.NewRESTHandler(hub, opts)
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
	// configFile is the --config persistent flag value: path to a JSON
	// config file with client defaults. Empty means no file.
	configFile string
	seams      cliSeams
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

	// OpenSession opens a stateful file handle (server-held handles). The
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
// env layering). Once an App exists, warnings go through the App sink
// chain. warnfWithAttrs below prefers the App sink and falls back here
// only for pre-App callers. Atomic so parallel tests swapping the seam
// never race; tests must use setWarnOutput, never assign directly.
var warnOutput atomic.Pointer[io.Writer]

// setWarnOutput swaps the pre-App warning sink, returning a restore func.
// Test-only seam: prod never calls it.
func setWarnOutput(w io.Writer) func() {
	old := warnOutput.Load()
	if w == nil {
		v := io.Writer(os.Stderr)
		w = v
	}
	warnOutput.Store(&w)
	return func() { warnOutput.Store(old) }
}

func warnSink() io.Writer {
	if p := warnOutput.Load(); p != nil && *p != nil {
		return *p
	}
	return os.Stderr
}

// warnfWithAttrs is the structured warning sink for pre-App callers: it
// renders a fixed storhub-prefixed human message with all varying values
// carried as structured slog attrs, never interpolated into the message.
func warnfWithAttrs(out io.Writer, msg string, attrs []any) {
	if out == nil {
		out = warnSink()
	}
	shlog.Warn(cliWarnLogger(out, shlog.FormatPretty, false), "storhub: warning: "+msg, attrs...)
}

// cliWarnLogger builds a Warn-level slog logger writing to out.
func cliWarnLogger(out io.Writer, format string, color bool) *slog.Logger {
	if out == nil {
		out = warnSink()
	}
	base := shlog.NewLogger(shlog.Options{Level: shlog.LevelWarn, Format: format, Color: color, Output: out})
	return shlog.WithComponent(base, "cli")
}

// logger returns the App leveled slog logger derived from the --log-*
// settings, writing to stderr and tagged with component=cli. Stdout stays
// pipeable data; all logs go to stderr only.
func (a *App) logger() *slog.Logger {
	out := a.stderr
	if out == nil {
		out = warnSink()
	}
	base := shlog.NewLogger(shlog.Options{Level: a.log.level, Format: a.log.format, Color: a.log.color, Output: out})
	return shlog.WithComponent(base, "cli")
}

// warnfWithAttrs is the App structured warning sink: it renders a fixed
// storhub-prefixed human message with all varying values as structured
// attrs, logged with the App log format and color settings. A nil out
// resolves through the usual App sink chain.
func (a *App) warnfWithAttrs(out io.Writer, msg string, attrs []any) {
	if out == nil {
		out = a.warnOut
	}
	if out == nil {
		out = a.stderr
	}
	if out == nil {
		out = warnSink()
	}
	shlog.Warn(cliWarnLogger(out, a.log.format, a.log.color), "storhub: warning: "+msg, attrs...)
}

func (c storhubClient) NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error) {
	return c.StorHub.NewFUSE(project, opts)
}

// New returns an App wired to process stdio with default settings.
func New() *App {
	a := &App{stdin: os.Stdin, stdout: io.Writer(os.Stdout), stderr: io.Writer(os.Stderr), warnOut: io.Writer(os.Stderr), log: defaultLogSettings(), seams: defaultCliSeams()}
	a.buildRootCmd()
	return a
}

// Seam accessors: per-App injection point (parallel-safe). Tests set
// a.seams fields directly for isolation; there are no package globals.
func (a *App) seamHub() func(context.Context, string, string, int64, bool, logSettings) (hubClient, error) {
	return a.seams.newHub
}

func (a *App) seamRESTHub() func(context.Context, string, string, int64, bool, logSettings) (*storhub.StorHub, error) {
	return a.seams.newRESTHub
}

func (a *App) seamMountHub() func(context.Context, string, string, int64, bool, logSettings) (hubClient, error) {
	return a.seams.newMountHub
}

func (a *App) seamFUSE() func(*storhub.StorHub, string, storhub.FUSEOptions) (fuseMount, error) {
	return a.seams.newFUSE
}

func (a *App) seamRESTHandler() func(*storhub.StorHub, storhub.RESTOptions) (http.Handler, error) {
	return a.seams.newREST
}

func (a *App) seamListen() func(*http.Server) error {
	return a.seams.listenServe
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
	// Log knobs fail loud here, before any logger is built: the logger
	// constructor falls back to info on unknown values, so without this
	// gate a --loglevel typo would run silently at the wrong verbosity.
	rootCmd.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		if !shlog.ValidLevel(a.log.level) {
			return &usageError{fmt.Errorf("invalid --loglevel %q: must be debug, info, warn, or error", a.log.level)}
		}
		if !shlog.ValidFormat(a.log.format) {
			return &usageError{fmt.Errorf("invalid --logformat %q: must be pretty or text", a.log.format)}
		}
		if err := strictEnvBool(storcfg.EnvLogColor); err != nil {
			return err
		}
		return nil
	}

	rootCmd.PersistentFlags().String("token", "", "GitHub token (falls back to $GITHUB_TOKEN; never shown in help)")
	rootCmd.PersistentFlags().String("apibase", os.Getenv(storcfg.EnvAPIBaseURL), "Optional GitHub API base URL (env: STORHUB_API_BASE_URL)")
	rootCmd.PersistentFlags().StringVar(&a.log.level, "loglevel", a.log.level, "Log level: debug, info, warn, error (env: STORHUB_LOG_LEVEL)")
	rootCmd.PersistentFlags().StringVar(&a.log.format, "logformat", a.log.format, "Log format: pretty, text (env: STORHUB_LOG_FORMAT)")
	rootCmd.PersistentFlags().BoolVar(&a.log.color, "logcolor", a.log.color, "Enable ANSI colors in logs (env: STORHUB_LOG_COLOR)")
	rootCmd.PersistentFlags().StringVar(&a.configFile, "config", "", "Path to a JSON config file with client defaults (flags and $STORHUB_* override file values)")

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
			if sub.Name() == args[0] {
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
	name := ""
	if cmd != nil {
		name = cmd.Name()
	}
	start := time.Now()
	shlog.Debug(a.logger(), "sync drain start", "command", name, "project", project)
	if err := a.hub.DrainProjectContext(ctx, project); err != nil {
		shlog.Error(a.logger(), "sync drain failed", "command", name, "project", project, "elapsed", time.Since(start), "err", err)
		return fmt.Errorf("sync %s: %w", project, err)
	}
	shlog.Debug(a.logger(), "sync drain complete", "command", name, "project", project, "elapsed", time.Since(start))
	return nil
}

// addFUSEFlags registers the flags every FUSE-mounting command shares.
// One definition: mount and serve must not drift apart on wording or
// defaults, because serve is mount plus REST over the same hub.
func addFUSEFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("allowother", false, "Enable allow_other on the FUSE mount")
	cmd.Flags().Bool("debug", false, "Enable FUSE debug logging")
	cmd.Flags().String("cachedir", "", "Optional cache directory")
	cmd.Flags().String("umask", "022", "Umask applied to created files and directories (octal 000-777); the FUSE protocol does not transmit the caller umask")
}

// addRESTFlags registers the flags every REST-serving command shares.
func addRESTFlags(cmd *cobra.Command) {
	cmd.Flags().String("listen", ":8080", "Listen address")
	cmd.Flags().String("basepath", "/api/v1", "REST API base path")
	cmd.Flags().String("authfile", "", "Optional JSON auth config file (falls back to $STORHUB_REST_AUTH_FILE)")
	cmd.Flags().String("sharekey", "", "Share signing key; defaults to one derived from the auth file's token_signing_key (falls back to $STORHUB_SHARE_SIGNING_KEY)")
	cmd.Flags().Bool("allowanonymous", false, "Explicitly serve the API without authentication (insecure)")
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
			a.warnfWithAttrs(nil, "secondary flush failure", []any{"err", flushErr})
		}
	}
	return err
}

// mustCmdHub resolves auth flags and builds the one-shot command hub in two
// lines at each run* call site: hub, err := a.mustCmdHub(cmd, 0, false).
func (a *App) mustCmdHub(cmd *cobra.Command, chunkSize int64, public bool) (hubClient, error) {
	token, apiBase := cmdAuth(cmd)
	return a.newCmdHub(cmd.Context(), resolveToken(token), apiBase, chunkSize, public, cmd.Flags().Changed("public"))
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

// withFileConfig layers the --config file under the flag and env values:
// flags win, then env, then file, then defaults. Callers pass their
// already-resolved flag values plus whether the public flag was explicitly
// set; empty/zero/unset means "unset" so the file may supply it, while any
// explicit value keeps precedence. File create_public_repo assigns (it can
// turn the repo private again), unlike the old OR-merge that could only
// ever turn it on. Log knobs additionally yield to the environment, which
// defaultLogSettings folded into a.log at construction. A missing file is
// a loud error: an explicit --config path is a contract, and running with
// silently wrong defaults after a typo is exactly what DisallowUnknownFields
// rejects for auth files. Anything else wrong with the file fails loudly.
func (a *App) withFileConfig(apiBase string, chunkSize int64, public, publicSet bool) (string, int64, bool, logSettings, error) {
	log := a.log
	path := strings.TrimSpace(a.configFile)
	if path == "" {
		return apiBase, chunkSize, public, log, nil
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "", 0, false, log, fmt.Errorf("load --config %q: file not found", path)
	}
	fc, err := storcfg.ReadFileConfig(path)
	if err != nil {
		return "", 0, false, log, fmt.Errorf("load --config %q: %w", path, err)
	}
	if apiBase == "" && fc.APIBaseURL != nil {
		apiBase = *fc.APIBaseURL
	}
	if chunkSize == 0 && fc.ChunkSize != nil {
		chunkSize = *fc.ChunkSize
	}
	if fc.CreatePublicRepo != nil && !publicSet {
		public = *fc.CreatePublicRepo
	}
	if fc.LogLevel != nil && !a.flagChanged("loglevel") && envUnset(storcfg.EnvLogLevel) {
		log.level = *fc.LogLevel
	}
	if fc.LogFormat != nil && !a.flagChanged("logformat") && envUnset(storcfg.EnvLogFormat) {
		log.format = *fc.LogFormat
	}
	if fc.LogColor != nil && !a.flagChanged("logcolor") && envUnset(storcfg.EnvLogColor) {
		log.color = *fc.LogColor
	}
	return apiBase, chunkSize, public, log, nil
}

// flagChanged reports whether the named persistent flag was explicitly
// set on the command line. A nil root (hand-built App in tests) counts
// as unchanged so file values still apply there.
func (a *App) flagChanged(name string) bool {
	if a.rootCmd == nil {
		return false
	}
	return a.rootCmd.PersistentFlags().Changed(name)
}

func (a *App) newCmdHub(ctx context.Context, token, apiBase string, chunkSize int64, public, publicSet bool) (hubClient, error) {
	start := time.Now()
	shlog.Debug(a.logger(), "hub creation start", "op", "newCmdHub")
	apiBase, chunkSize, public, log, err := a.withFileConfig(apiBase, chunkSize, public, publicSet)
	if err != nil {
		shlog.Error(a.logger(), "hub creation failed", "op", "newCmdHub", "elapsed", time.Since(start), "err", err)
		return nil, err
	}
	hub, err := a.seamHub()(ctx, token, apiBase, chunkSize, public, log)
	if err != nil {
		shlog.Error(a.logger(), "hub creation failed", "op", "newCmdHub", "elapsed", time.Since(start), "err", err)
		return hub, err
	}
	a.hub = hub
	shlog.Debug(a.logger(), "hub creation complete", "op", "newCmdHub", "elapsed", time.Since(start))
	return hub, err
}

// newCmdMountHub builds the hub for long-running interactive surfaces
// (mount): it records the client for Run's flush and uses the
// pause-to-reset rate policy. File-supplied chunk_size/public flow through
// like every other surface: mount no longer silently drops file keys.
func (a *App) newCmdMountHub(ctx context.Context, token, apiBase string) (hubClient, error) {
	start := time.Now()
	shlog.Debug(a.logger(), "hub creation start", "op", "newCmdMountHub")
	apiBase, chunkSize, public, log, err := a.withFileConfig(apiBase, 0, false, false)
	if err != nil {
		shlog.Error(a.logger(), "hub creation failed", "op", "newCmdMountHub", "elapsed", time.Since(start), "err", err)
		return nil, err
	}
	hub, err := a.seamMountHub()(ctx, token, apiBase, chunkSize, public, log)
	if err != nil {
		shlog.Error(a.logger(), "hub creation failed", "op", "newCmdMountHub", "elapsed", time.Since(start), "err", err)
		return hub, err
	}
	a.hub = hub
	shlog.Debug(a.logger(), "hub creation complete", "op", "newCmdMountHub", "elapsed", time.Since(start))
	return hub, err
}

func (a *App) newCmdRESTHub(ctx context.Context, token, apiBase string, chunkSize int64, public bool) (*storhub.StorHub, error) {
	start := time.Now()
	shlog.Debug(a.logger(), "hub creation start", "op", "newCmdRESTHub")
	apiBase, chunkSize, public, log, err := a.withFileConfig(apiBase, chunkSize, public, false)
	if err != nil {
		shlog.Error(a.logger(), "hub creation failed", "op", "newCmdRESTHub", "elapsed", time.Since(start), "err", err)
		return nil, err
	}
	hub, err := a.seamRESTHub()(ctx, token, apiBase, chunkSize, public, log)
	if err != nil {
		shlog.Error(a.logger(), "hub creation failed", "op", "newCmdRESTHub", "elapsed", time.Since(start), "err", err)
		return hub, err
	}
	// rest/serve need the raw *StorHub for storhub.NewRESTHandler; track the
	// wrapped form so Run can still flush pending metadata.
	a.hub = storhubClient{StorHub: hub}
	shlog.Debug(a.logger(), "hub creation complete", "op", "newCmdRESTHub", "elapsed", time.Since(start))
	return hub, err
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
	start := time.Now()
	shlog.Debug(a.logger(), "mount setup start", "project", project, "mountpoint", mountPoint)
	fsys, err := open(project, fuseOpts)
	if err != nil {
		shlog.Error(a.logger(), "mount setup failed", "project", project, "mountpoint", mountPoint, "elapsed", time.Since(start), "err", err)
		return nil, err
	}
	if err := os.MkdirAll(mountPoint, mountDirPerm); err != nil {
		_ = fsys.Close()
		shlog.Error(a.logger(), "mount setup failed", "project", project, "mountpoint", mountPoint, "elapsed", time.Since(start), "err", err)
		return nil, err
	}
	if err := fsys.Mount(mountPoint); err != nil {
		_ = fsys.Close()
		shlog.Error(a.logger(), "mount setup failed", "project", project, "mountpoint", mountPoint, "elapsed", time.Since(start), "err", err)
		return nil, err
	}
	if ctx.Err() != nil {
		if uerr := fsys.Unmount(); uerr != nil {
			_, _ = fmt.Fprintf(a.stderr, "warning: interrupted during mount; unmount failed (%v); %s\n", uerr, mayStillBeMounted(mountPoint))
		}
		_ = fsys.Close()
		shlog.Error(a.logger(), "mount setup failed", "project", project, "mountpoint", mountPoint, "elapsed", time.Since(start), "err", ctx.Err())
		return nil, errors.New("interrupted while mounting " + project)
	}
	shlog.Debug(a.logger(), "mount setup complete", "project", project, "mountpoint", mountPoint, "elapsed", time.Since(start))
	return fsys, nil
}

// newHubFromFlags builds the hub for one-shot commands: fail-fast rate
// policy, chunksize and public-repo flags honored.
func newHubFromFlags(ctx context.Context, token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error) {
	token = resolveToken(token)
	if token == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	cfg, err := newHubConfig(apiBase, chunkSize, public, log, false)
	if err != nil {
		return nil, err
	}
	return storhub.NewStorHubWithContext(ctx, token, cfg)
}

// newLongRunningHubFromFlags is the single hub constructor for the
// long-running interactive surfaces (mount, rest, serve): pause-to-reset
// rate policy so a session rides out a secondary rate limit instead of
// dying instantly. Mount, rest, and serve test doubles inject at this one
// seam shape.
func newLongRunningHubFromFlags(ctx context.Context, token, apiBase string, chunkSize int64, public bool, log logSettings) (*storhub.StorHub, error) {
	token = resolveToken(token)
	if token == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	cfg, err := newHubConfig(apiBase, chunkSize, public, log, true)
	if err != nil {
		return nil, err
	}
	return storhub.NewStorHubWithContext(ctx, token, cfg)
}

// cmdAuth extracts the auth flags every command shares: explicit --token
// (resolved against $GITHUB_TOKEN) plus --apibase.
func cmdAuth(cmd *cobra.Command) (token, apiBase string) {
	token, _ = cmd.Flags().GetString("token")
	apiBase, _ = cmd.Flags().GetString("apibase")
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
