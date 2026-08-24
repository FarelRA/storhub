package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shlog "github.com/FarelRA/storhub/internal/logging"
	shrest "github.com/FarelRA/storhub/rest"
	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

// version is injected at build time via -ldflags "-X ...version=x.y.z".
var version = "dev"

type App struct {
	stdin   io.Reader
	stdout  *os.File
	stderr  *os.File
	rootCmd *cobra.Command
	hub     hubClient
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
	ListMetadataRevisions(project string) ([]storhub.MetadataRevision, error)
	RollbackMetadata(project, commitSHA string) error
	PurgeUntracked(project string) (*storhub.PurgeResult, error)
	DeleteProject(project string) error
	NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error)

	// Shutdown drains the asynchronous metadata writer. Part of the
	// contract on purpose: every implementation - including test fakes -
	// must be drainable, and App.Run is the single caller.
	Shutdown(ctx context.Context) error
}

type storhubClient struct {
	*storhub.StorHub
}

var (
	cliLogLevel  = envOrDefault("STORHUB_LOG_LEVEL", "info")
	cliLogFormat = envOrDefault("STORHUB_LOG_FORMAT", "pretty")
	cliLogColor  = parseEnvBool("STORHUB_LOG_COLOR", true)
)

func (c storhubClient) NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error) {
	return c.StorHub.NewFUSE(project, opts)
}

var newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool) (hubClient, error) {
	hub, err := newHubFromFlags(token, apiBase, chunkSize, public)
	if err != nil {
		return nil, err
	}
	return storhubClient{StorHub: hub}, nil
}

var newRESTHubFromFlagsFn = newHubFromFlags
var newMountHubFromFlagsFn = func(token, apiBase string) (hubClient, error) {
	hub, err := newMountHubFromFlags(token, apiBase)
	if err != nil {
		return nil, err
	}
	return storhubClient{StorHub: hub}, nil
}
var newRESTHandlerFn = func(hub *storhub.StorHub, opts shrest.Options) (http.Handler, error) {
	return shrest.New(hub, opts)
}
var restListenAndServeFn = func(server *http.Server) error {
	return server.ListenAndServe()
}

const minCLIChunkSize int64 = 32 * 1024 * 1024

func normalizeCLIChunkSize(size int64) int64 {
	if size <= 0 {
		return size
	}
	if size < minCLIChunkSize {
		return minCLIChunkSize
	}
	return size
}

func New() *App {
	a := &App{stdin: os.Stdin, stdout: os.Stdout, stderr: os.Stderr}
	a.buildRootCmd()
	return a
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
  storhub serve-rest --listen :8080
  storhub mount docs-project ./mnt`,
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
	rootCmd.PersistentFlags().StringVar(&cliLogLevel, "log-level", cliLogLevel, "Log level: debug, info, warn, error (env: STORHUB_LOG_LEVEL)")
	rootCmd.PersistentFlags().StringVar(&cliLogFormat, "log-format", cliLogFormat, "Log format: pretty, text (env: STORHUB_LOG_FORMAT)")
	rootCmd.PersistentFlags().BoolVar(&cliLogColor, "log-color", cliLogColor, "Enable ANSI colors in logs (env: STORHUB_LOG_COLOR)")

	rootCmd.AddCommand(a.newUploadCmd())
	rootCmd.AddCommand(a.newReplaceCmd())
	rootCmd.AddCommand(a.newDownloadCmd())
	rootCmd.AddCommand(a.newListCmd())
	rootCmd.AddCommand(a.newStatCmd())
	rootCmd.AddCommand(a.newCatCmd())
	rootCmd.AddCommand(a.newMkdirCmd())
	rootCmd.AddCommand(a.newRemoveCmd())
	rootCmd.AddCommand(a.newMoveCmd())
	rootCmd.AddCommand(a.newAppendCmd())
	rootCmd.AddCommand(a.newWriteCmd())
	rootCmd.AddCommand(a.newPatchCmd())
	rootCmd.AddCommand(a.newRevisionsCmd())
	rootCmd.AddCommand(a.newRollbackCmd())
	rootCmd.AddCommand(a.newPurgeCmd())
	rootCmd.AddCommand(a.newDeleteProjectCmd())
	rootCmd.AddCommand(a.newMountCmd())
	rootCmd.AddCommand(a.newServeRESTCmd())

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

func (a *App) newUploadCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload [flags] <project> <remote-path> <local-path>",
		Short: "Upload a file",
		Args:  usageArgs(cobra.ExactArgs(3)),
		RunE:  a.runUploadOrReplace,
	}
	cmd.Flags().Int64("chunk-size", 0, "Chunk size in bytes")
	cmd.Flags().Bool("public", false, "Create public repos instead of private")
	return cmd
}

func (a *App) newReplaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replace [flags] <project> <remote-path> <local-path>",
		Short: "Replace an existing file",
		Args:  usageArgs(cobra.ExactArgs(3)),
		RunE:  a.runUploadOrReplace,
	}
	cmd.Flags().Int64("chunk-size", 0, "Chunk size in bytes")
	cmd.Flags().Bool("public", false, "Create public repos instead of private")
	return cmd
}

func (a *App) newDownloadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "download [flags] <project> <remote-path> <local-path>",
		Short: "Download a file",
		Args:  usageArgs(cobra.ExactArgs(3)),
		RunE:  a.runDownload,
	}
}

func (a *App) newListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls [flags] <project> [path]",
		Short: "List directory contents",
		Args:  usageArgs(cobra.RangeArgs(1, 2)),
		RunE:  a.runList,
	}
	cmd.Flags().BoolP("long", "l", false, "Show detailed listing")
	cmd.Flags().Bool("json", false, "Emit machine-readable JSON (array of entries)")
	return cmd
}

func (a *App) newStatCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stat [flags] <project> <path>",
		Short: "Show file/directory metadata",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE:  a.runStat,
	}
	cmd.Flags().Bool("json", false, "Emit machine-readable JSON object")
	return cmd
}

func (a *App) newCatCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cat [flags] <project> <path>",
		Short: "Print file contents to stdout",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE:  a.runCat,
	}
}

func (a *App) newMkdirCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mkdir [flags] <project> <path>",
		Short: "Create a directory",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE:  a.runMkdir,
	}
}

func (a *App) newRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rm [flags] <project> <path>",
		Short: "Remove a file or directory",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE:  a.runRemove,
	}
	cmd.Flags().BoolP("recursive", "r", false, "Remove directory instead of file")
	return cmd
}

func (a *App) newMoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mv [flags] <project> <old-path> <new-path>",
		Short: "Move or rename a file/directory",
		Args:  usageArgs(cobra.ExactArgs(3)),
		RunE:  a.runMove,
	}
}

func (a *App) newAppendCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "append [flags] <project> <path> <text>",
		Short: "Append text to a file",
		Args:  usageArgs(cobra.ExactArgs(3)),
		RunE:  a.runAppend,
	}
}

func (a *App) newWriteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "write [flags] <project> <path> <offset> <text>",
		Short: "Write data at a byte offset",
		Args:  usageArgs(cobra.ExactArgs(4)),
		RunE:  a.runWrite,
	}
}

func (a *App) newPatchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "patch [flags] <project> <path> <offset> <delete-size> <text>",
		Short: "Delete and insert at an offset",
		Args:  usageArgs(cobra.ExactArgs(5)),
		RunE:  a.runPatch,
	}
}

func (a *App) newRevisionsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revisions [flags] <project>",
		Short: "List metadata revision history",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE:  a.runRevisions,
	}
	cmd.Flags().Bool("json", false, "Emit machine-readable JSON (array of revisions)")
	return cmd
}

func (a *App) newRollbackCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rollback [flags] <project> <commit-sha>",
		Short: "Rollback metadata to a commit",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE:  a.runRollback,
	}
}

func (a *App) newPurgeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "purge <project>",
		Short: "Delete untracked releases and assets",
		Long: `Purge deletes GitHub releases and assets that are not tracked in the project metadata.

This cleans up orphaned releases and assets (e.g. from interrupted writes or manual interference).`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runPurge,
	}
}

func (a *App) newDeleteProjectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-project <project>",
		Short: "Delete an entire project repository",
		Long: `Delete-project removes the project's GitHub repository outright: every file,
directory, release, asset, and metadata revision is gone. This cannot be undone.

The --yes flag is mandatory so a typo can never destroy a project.`,
		Args: usageArgs(cobra.ExactArgs(1)),
		RunE: a.runDeleteProject,
	}
	cmd.Flags().Bool("yes", false, "Confirm deletion of the whole project")
	return cmd
}

func (a *App) runDeleteProject(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	confirmed, _ := cmd.Flags().GetBool("yes")
	if !confirmed {
		return &usageError{fmt.Errorf("deleting project %q removes its repository, releases, and every file; pass --yes to confirm", args[0])}
	}
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	if err := hub.DeleteProject(args[0]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "deleted project %s\n", args[0])
	return nil
}

func (a *App) newMountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mount [flags] <project> <mount-point>",
		Short: "FUSE mount a project",
		Args:  usageArgs(cobra.ExactArgs(2)),
		RunE:  a.runMount,
	}
	cmd.Flags().Bool("allow-other", false, "Enable allow_other on the FUSE mount")
	cmd.Flags().Bool("debug", false, "Enable FUSE debug logging")
	cmd.Flags().String("cache-dir", "", "Optional cache directory")
	return cmd
}

func (a *App) newServeRESTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve-rest [flags]",
		Short: "Start the REST API server",
		Args:  usageArgs(cobra.NoArgs),
		RunE:  a.runServeREST,
	}
	cmd.Flags().String("listen", ":8080", "Listen address")
	cmd.Flags().String("base-path", "/api/v1", "REST API base path")
	cmd.Flags().String("auth-file", "", "Optional JSON auth config file (falls back to $STORHUB_REST_AUTH_FILE)")
	cmd.Flags().Bool("allow-anonymous", false, "Explicitly serve the API without authentication (insecure)")
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
	a.shutdownHub()
	return err
}

// shutdownHub is the SINGLE owner of hub.Shutdown: the one and only
// place that drains the asynchronous metadata writer. Commands must
// never call Shutdown themselves - not serve-rest, not mount - so there
// is exactly one drain point to reason about. Shutdown is part of the
// hubClient contract, so nothing reachable here can lack it. The writer
// is asynchronous, meaning a CLI mutation that exits without this loses
// data; that is precisely what the released-binary smoke test caught.
func (a *App) shutdownHub() {
	if a.hub == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.hub.Shutdown(ctx); err != nil && a.stderr != nil {
		_, _ = fmt.Fprintf(a.stderr, "warning: metadata flush failed: %v\n", err)
	}
}

// Hub constructors record the client so Run can always flush it on exit.

func (a *App) newCmdHub(token, apiBase string, chunkSize int64, public bool) (hubClient, error) {
	hub, err := newHubFromFlagsFn(token, apiBase, chunkSize, public)
	if err == nil {
		a.hub = hub
	}
	return hub, err
}

func (a *App) newCmdMountHub(token, apiBase string) (hubClient, error) {
	hub, err := newMountHubFromFlagsFn(token, apiBase)
	if err == nil {
		a.hub = hub
	}
	return hub, err
}

func (a *App) newCmdRESTHub(token, apiBase string, chunkSize int64, public bool) (*storhub.StorHub, error) {
	hub, err := newRESTHubFromFlagsFn(token, apiBase, chunkSize, public)
	if err == nil {
		// serve-rest needs the raw *StorHub for shrest.New; track the
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

// flexDuration accepts either a Go duration string ("2h", "30m") or a bare
// JSON number meaning SECONDS. time.Duration's own unmarshaler would read a
// number as nanoseconds — the difference between an hour and 3.6
// microseconds, silently.
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
		*d = flexDuration(parsed)
		return nil
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fmt.Errorf("token_ttl must be a duration string or seconds: %w", err)
	}
	*d = flexDuration(time.Duration(seconds * float64(time.Second)))
	return nil
}

// Duration exposes the parsed value.
func (d flexDuration) Duration() time.Duration { return time.Duration(d) }

func (a *App) runUploadOrReplace(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	chunkSize, _ := cmd.Flags().GetInt64("chunk-size")
	public, _ := cmd.Flags().GetBool("public")

	hub, err := a.newCmdHub(resolveToken(token), apiBase, chunkSize, public)
	if err != nil {
		return err
	}

	replace := cmd.Name() == "replace"
	project, remotePath, localPath := args[0], args[1], args[2]
	var meta *storhub.FileMetadata
	if replace {
		meta, err = hub.ReplaceFile(project, remotePath, localPath)
	} else {
		meta, err = hub.UploadFile(project, remotePath, localPath)
	}
	if err != nil {
		return err
	}
	printFileSummary(a.stderr, ternary(replace, "replaced", "uploaded"), meta)
	return nil
}

func (a *App) runDownload(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdMountHub(resolveToken(token), apiBase)
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
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	long, _ := cmd.Flags().GetBool("long")
	jsonOut, _ := cmd.Flags().GetBool("json")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
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
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	entry, err := hub.StatPath(args[0], args[1])
	if err != nil {
		return err
	}
	if jsonOutStat(cmd) {
		return json.NewEncoder(a.stdout).Encode(entry)
	}
	printEntryInfo(a.stdout, entry)
	return nil
}

func jsonOutStat(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("json")
	return v
}

func (a *App) runCat(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
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
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	if err := hub.Mkdir(args[0], args[1]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "created directory %s\n", args[1])
	return nil
}

func (a *App) runRemove(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	recursive, _ := cmd.Flags().GetBool("recursive")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	if recursive {
		err = hub.Rmdir(args[0], args[1])
	} else {
		err = hub.DeleteFile(args[0], args[1])
	}
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "removed %s\n", args[1])
	return nil
}

func (a *App) runMove(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	if err := hub.Rename(args[0], args[1], args[2]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "moved %s -> %s\n", args[1], args[2])
	return nil
}

func (a *App) runAppend(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	payload, err := readDataArg(a, args[2])
	if err != nil {
		return err
	}
	meta, err := hub.AppendFile(args[0], args[1], payload)
	if err != nil {
		return err
	}
	printFileSummary(a.stderr, "appended", meta)
	return nil
}

func (a *App) runWrite(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	offset, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset %q: %w", args[2], err)
	}
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	payload, err := readDataArg(a, args[3])
	if err != nil {
		return err
	}
	meta, err := hub.WriteFileAt(args[0], args[1], offset, payload)
	if err != nil {
		return err
	}
	printFileSummary(a.stderr, "written", meta)
	return nil
}

func (a *App) runPatch(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	offset, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset %q: %w", args[2], err)
	}
	deleteSize, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid delete-size %q: %w", args[3], err)
	}
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	edit, err := readDataArg(a, args[4])
	if err != nil {
		return err
	}
	meta, err := hub.PatchFile(args[0], args[1], offset, deleteSize, edit)
	if err != nil {
		return err
	}
	printFileSummary(a.stderr, "patched", meta)
	return nil
}

func (a *App) runRevisions(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
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

func (a *App) runRollback(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	if err := hub.RollbackMetadata(args[0], args[1]); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "rolled back %s to %s\n", args[0], args[1])
	return nil
}

func (a *App) runPurge(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	result, err := hub.PurgeUntracked(args[0])
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(a.stderr, "purged %s: %d releases, %d assets deleted\n",
		args[0], result.DeletedReleases, result.DeletedAssets)
	return nil
}

func (a *App) runMount(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	allowOther, _ := cmd.Flags().GetBool("allow-other")
	debug, _ := cmd.Flags().GetBool("debug")
	cacheDir, _ := cmd.Flags().GetString("cache-dir")
	hub, err := a.newCmdHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	opts := storhub.DefaultFUSEOptions()
	opts.AllowOther = allowOther
	opts.Debug = debug
	opts.CacheDir = cacheDir
	// Arm signal handling before touching FUSE: an interrupt arriving during
	// mount setup must not fall through to the default disposition and kill
	// the process with a half-attached mount left behind.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fsys, err := hub.NewFUSE(args[0], opts)
	if err != nil {
		return err
	}
	defer func() { _ = fsys.Close() }()
	if err := os.MkdirAll(args[1], 0o755); err != nil {
		return err
	}
	if err := fsys.Mount(args[1]); err != nil {
		return err
	}
	if ctx.Err() != nil {
		if err := fsys.Unmount(); err != nil {
			_, _ = fmt.Fprintf(a.stderr, "warning: interrupted during mount; unmount failed (%v); %s may still be mounted\n", err, args[1])
		}
		return errors.New("interrupted while mounting " + args[0])
	}
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
		<-waitDone
		return nil
	}
}

// unmountWithRetry retries the unmount until it succeeds or its retry budget
// runs out. An unmount fails with EBUSY while any file on the mount is still
// open, so holders get a grace period instead of either hanging forever or
// silently leaking the mount.
func unmountWithRetry(fsys fuseMount, target string, report io.Writer) {
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
		_, _ = fmt.Fprintf(report, "unmount failed (%v); close programs using %s and wait, or press Ctrl+C again to quit\n", err, target)
		if time.Now().After(deadline) {
			_, _ = fmt.Fprintf(report, "giving up on unmount after %d attempts; %s may still be mounted\n", attempt, target)
			return
		}
		time.Sleep(delay)
		if delay < 8*time.Second {
			delay *= 2
		}
	}
}

var (
	unmountRetryBaseDelay = time.Second
	unmountRetryBudget    = 30 * time.Second
)

func (a *App) runServeREST(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	listen, _ := cmd.Flags().GetString("listen")
	basePath, _ := cmd.Flags().GetString("base-path")
	authFile, _ := cmd.Flags().GetString("auth-file")
	if authFile == "" {
		authFile = os.Getenv("STORHUB_REST_AUTH_FILE")
	}
	hub, err := a.newCmdRESTHub(resolveToken(token), apiBase, 0, false)
	if err != nil {
		return err
	}
	opts := shrest.DefaultOptions()
	opts.BasePath = basePath
	if strings.TrimSpace(authFile) != "" {
		auth, err := loadRESTAuthOptions(authFile)
		if err != nil {
			return err
		}
		opts.Auth = auth
	} else {
		// Running without an auth file is a deliberate choice: require the
		// explicit opt-in flag so an open server never happens by accident.
		noAuth, _ := cmd.Flags().GetBool("allow-anonymous")
		if !noAuth {
			return fmt.Errorf("refusing to serve unauthenticated REST API; provide --auth-file or pass --allow-anonymous")
		}
		opts.AllowAnonymous = true
	}
	handler, err := newRESTHandlerFn(hub, opts)
	if err != nil {
		return err
	}
	handler = a.loggingMiddleware(handler)
	mode := "without auth"
	if opts.Auth != nil {
		mode = "with auth"
	}
	_, _ = fmt.Fprintf(a.stderr, "serving REST API on %s%s %s\n", listen, opts.BasePath, mode)
	server := &http.Server{
		Addr:              listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
	return a.serveRESTUntilSignal(server, hub)
}

// serveRESTUntilSignal runs the REST server and drains it cleanly on
// SIGINT/SIGTERM: in-flight requests finish within a bounded shutdown
// window, then pending metadata is flushed before exit. A clean stop is not
// reported as an error.
func (a *App) serveRESTUntilSignal(server *http.Server, hub *storhub.StorHub) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() { errCh <- restListenAndServeFn(server) }()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			return err
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_, _ = fmt.Fprintf(a.stderr, "graceful shutdown failed: %v\n", err)
	}
	// Metadata draining is NOT done here: Run's flushHub is the single
	// owner of hub.Shutdown for every command. Draining after the HTTP
	// server stops is exactly the right order - in-flight requests can
	// still mutate metadata, and flushHub commits all of it.
	return nil
}

// httpLogsEnabled reports whether the configured level wants per-request
// HTTP lines; at error level they are pure noise.
func httpLogsEnabled() bool {
	level := shlog.NormalizeLevel(cliLogLevel)
	return level == shlog.LevelDebug || level == shlog.LevelInfo || level == shlog.LevelWarn
}

func (a *App) loggingMiddleware(next http.Handler) http.Handler {
	if next == nil {
		return nil
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		if httpLogsEnabled() {
			a.logf("http start: method=%s uri=%s remote=%s", r.Method, shlog.RedactRequestURI(r.URL.RequestURI()), r.RemoteAddr)
		}
		next.ServeHTTP(wrapped, r)
		if httpLogsEnabled() {
			a.logf("http done: method=%s uri=%s status=%d duration=%s", r.Method, shlog.RedactRequestURI(r.URL.RequestURI()), wrapped.status, time.Since(start).Round(time.Millisecond))
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func loadRESTAuthOptions(filePath string) (*shrest.AuthOptions, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var file restAuthFile
	if err := json.Unmarshal(data, &file); err != nil {
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

func newHubFromFlags(token, apiBase string, chunkSize int64, public bool) (*storhub.StorHub, error) {
	token = resolveToken(token)
	if token == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	cfg := storhub.DefaultConfig()
	if strings.TrimSpace(apiBase) != "" {
		cfg.APIBaseURL = apiBase
	}
	if normalized := normalizeCLIChunkSize(chunkSize); normalized > 0 {
		if normalized != chunkSize {
			fmt.Fprintf(os.Stderr, "%s storhub: warning: --chunk-size %d below floor %d; using %d\n",
				time.Now().UTC().Format(time.RFC3339), chunkSize, minCLIChunkSize, normalized)
		}
		cfg.ChunkSize = normalized
	}
	cfg.CreatePublicRepo = public
	cfg.LogLevel = cliLogLevel
	cfg.LogFormat = cliLogFormat
	cfg.LogColor = cliLogColor
	return storhub.NewStorHubWithConfig(token, cfg)
}

func newMountHubFromFlags(token, apiBase string) (*storhub.StorHub, error) {
	token = resolveToken(token)
	if token == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	cfg := storhub.DefaultConfig()
	if strings.TrimSpace(apiBase) != "" {
		cfg.APIBaseURL = apiBase
	}
	cfg.AtimePolicy = storcfg.AtimeNo
	return storhub.NewStorHubWithConfig(token, cfg)
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
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func parseEnvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func ternary[T any](cond bool, left, right T) T {
	if cond {
		return left
	}
	return right
}

func formatTime(t int64) string {
	if t == 0 {
		return "-"
	}
	return time.Unix(t, 0).Format(time.RFC3339)
}
