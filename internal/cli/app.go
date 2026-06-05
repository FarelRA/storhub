package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shrest "github.com/FarelRA/storhub/rest"
	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

type App struct {
	stdout  *os.File
	stderr  *os.File
	rootCmd *cobra.Command
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
	NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error)
}

type storhubClient struct {
	*storhub.StorHub
}

var (
	cliLogLevel  = envOrDefault("STORHUB_LOG_LEVEL", "debug")
	cliLogFormat = envOrDefault("STORHUB_LOG_FORMAT", "pretty")
	cliLogColor  = parseEnvBool("STORHUB_LOG_COLOR", true)
)

func (c storhubClient) NewFUSE(project string, opts storhub.FUSEOptions) (fuseMount, error) {
	return c.StorHub.NewFUSE(project, opts)
}

var newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, concurrency int, public bool) (hubClient, error) {
	hub, err := newHubFromFlags(token, apiBase, chunkSize, concurrency, public)
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
	a := &App{stdout: os.Stdout, stderr: os.Stderr}
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
	}

	rootCmd.PersistentFlags().String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	rootCmd.PersistentFlags().String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	rootCmd.PersistentFlags().StringVar(&cliLogLevel, "log-level", cliLogLevel, "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().StringVar(&cliLogFormat, "log-format", cliLogFormat, "Log format: pretty, text")
	rootCmd.PersistentFlags().BoolVar(&cliLogColor, "log-color", cliLogColor, "Enable ANSI colors in logs")

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
	rootCmd.AddCommand(a.newMountCmd())
	rootCmd.AddCommand(a.newServeRESTCmd())

	a.rootCmd = rootCmd
}

func (a *App) newUploadCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload [flags] <project> <remote-path> <local-path>",
		Short: "Upload a file",
		Args:  cobra.ExactArgs(3),
		RunE:  a.runUploadOrReplace,
	}
	cmd.Flags().Int64("chunk-size", 0, "Chunk size in bytes")
	cmd.Flags().Int("concurrency", 0, "Max concurrent transfers")
	cmd.Flags().Bool("public", false, "Create public repos instead of private")
	return cmd
}

func (a *App) newReplaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replace [flags] <project> <remote-path> <local-path>",
		Short: "Replace an existing file",
		Args:  cobra.ExactArgs(3),
		RunE:  a.runUploadOrReplace,
	}
	cmd.Flags().Int64("chunk-size", 0, "Chunk size in bytes")
	cmd.Flags().Int("concurrency", 0, "Max concurrent transfers")
	cmd.Flags().Bool("public", false, "Create public repos instead of private")
	return cmd
}

func (a *App) newDownloadCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "download [flags] <project> <remote-path> <local-path>",
		Short: "Download a file",
		Args:  cobra.ExactArgs(3),
		RunE:  a.runDownload,
	}
}

func (a *App) newListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls [flags] <project> [path]",
		Short: "List directory contents",
		Args:  cobra.RangeArgs(1, 2),
		RunE:  a.runList,
	}
	cmd.Flags().BoolP("long", "l", false, "Show detailed listing")
	return cmd
}

func (a *App) newStatCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stat [flags] <project> <path>",
		Short: "Show file/directory metadata",
		Args:  cobra.ExactArgs(2),
		RunE:  a.runStat,
	}
}

func (a *App) newCatCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cat [flags] <project> <path>",
		Short: "Print file contents to stdout",
		Args:  cobra.ExactArgs(2),
		RunE:  a.runCat,
	}
}

func (a *App) newMkdirCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mkdir [flags] <project> <path>",
		Short: "Create a directory",
		Args:  cobra.ExactArgs(2),
		RunE:  a.runMkdir,
	}
}

func (a *App) newRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rm [flags] <project> <path>",
		Short: "Remove a file or directory",
		Args:  cobra.ExactArgs(2),
		RunE:  a.runRemove,
	}
	cmd.Flags().BoolP("recursive", "r", false, "Remove directory instead of file")
	return cmd
}

func (a *App) newMoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mv [flags] <project> <old-path> <new-path>",
		Short: "Move or rename a file/directory",
		Args:  cobra.ExactArgs(3),
		RunE:  a.runMove,
	}
}

func (a *App) newAppendCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "append [flags] <project> <path> <text>",
		Short: "Append text to a file",
		Args:  cobra.ExactArgs(3),
		RunE:  a.runAppend,
	}
}

func (a *App) newWriteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "write [flags] <project> <path> <offset> <text>",
		Short: "Write data at a byte offset",
		Args:  cobra.ExactArgs(4),
		RunE:  a.runWrite,
	}
}

func (a *App) newPatchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "patch [flags] <project> <path> <offset> <delete-size> <text>",
		Short: "Delete and insert at an offset",
		Args:  cobra.ExactArgs(5),
		RunE:  a.runPatch,
	}
}

func (a *App) newRevisionsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "revisions [flags] <project>",
		Short: "List metadata revision history",
		Args:  cobra.ExactArgs(1),
		RunE:  a.runRevisions,
	}
}

func (a *App) newRollbackCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rollback [flags] <project> <commit-sha>",
		Short: "Rollback metadata to a commit",
		Args:  cobra.ExactArgs(2),
		RunE:  a.runRollback,
	}
}

func (a *App) newMountCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mount [flags] <project> <mount-point>",
		Short: "FUSE mount a project",
		Args:  cobra.ExactArgs(2),
		RunE:  a.runMount,
	}
	cmd.Flags().Bool("allow-other", false, "Enable allow_other on the FUSE mount")
	cmd.Flags().Bool("debug", true, "Enable FUSE debug logging")
	cmd.Flags().String("cache-dir", "", "Optional cache directory")
	return cmd
}

func (a *App) newServeRESTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve-rest [flags]",
		Short: "Start the REST API server",
		Args:  cobra.NoArgs,
		RunE:  a.runServeREST,
	}
	cmd.Flags().String("listen", ":8080", "Listen address")
	cmd.Flags().String("base-path", "/api/v1", "REST API base path")
	cmd.Flags().String("auth-file", os.Getenv("STORHUB_REST_AUTH_FILE"), "Optional JSON auth config file")
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
	return err
}

func (a *App) logf(format string, args ...any) {
	if a.stderr == nil {
		return
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	_, _ = fmt.Fprintf(a.stderr, "%s storhub: %s\n", stamp, fmt.Sprintf(format, args...))
}

type restAuthFile struct {
	Realm           string        `json:"realm"`
	TokenSigningKey string        `json:"token_signing_key"`
	TokenTTL        time.Duration `json:"token_ttl"`
	Users           []shrest.User `json:"users"`
}

func (a *App) runUploadOrReplace(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	chunkSize, _ := cmd.Flags().GetInt64("chunk-size")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	public, _ := cmd.Flags().GetBool("public")

	hub, err := newHubFromFlagsFn(token, apiBase, chunkSize, concurrency, public)
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
	printFileSummary(a.stdout, ternary(replace, "replaced", "uploaded"), meta)
	return nil
}

func (a *App) runDownload(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := newMountHubFromFlagsFn(token, apiBase)
	if err != nil {
		return err
	}
	if err := hub.DownloadFile(args[0], args[1], args[2]); err != nil {
		return err
	}
	info, err := os.Stat(args[2])
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "downloaded %s to %s (%d bytes)\n", args[1], args[2], info.Size())
	return nil
}

func (a *App) runList(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	long, _ := cmd.Flags().GetBool("long")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
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
	printDirEntries(a.stdout, entries, long)
	return nil
}

func (a *App) runStat(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	entry, err := hub.StatPath(args[0], args[1])
	if err != nil {
		return err
	}
	printEntryInfo(a.stdout, entry)
	return nil
}

func (a *App) runCat(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	entry, err := hub.StatPath(args[0], args[1])
	if err != nil {
		return err
	}
	data, err := hub.ReadFileAt(args[0], args[1], 0, entry.Size)
	if err != nil {
		return err
	}
	_, err = a.stdout.Write(data)
	return err
}

func (a *App) runMkdir(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	if err := hub.Mkdir(args[0], args[1]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "created directory %s\n", args[1])
	return nil
}

func (a *App) runRemove(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	recursive, _ := cmd.Flags().GetBool("recursive")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
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
	fmt.Fprintf(a.stdout, "removed %s\n", args[1])
	return nil
}

func (a *App) runMove(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	if err := hub.Rename(args[0], args[1], args[2]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "moved %s -> %s\n", args[1], args[2])
	return nil
}

func (a *App) runAppend(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	meta, err := hub.AppendFile(args[0], args[1], []byte(args[2]))
	if err != nil {
		return err
	}
	printFileSummary(a.stdout, "appended", meta)
	return nil
}

func (a *App) runWrite(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	offset, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset %q: %w", args[2], err)
	}
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	meta, err := hub.WriteFileAt(args[0], args[1], offset, []byte(args[3]))
	if err != nil {
		return err
	}
	printFileSummary(a.stdout, "written", meta)
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
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	meta, err := hub.PatchFile(args[0], args[1], offset, deleteSize, []byte(args[4]))
	if err != nil {
		return err
	}
	printFileSummary(a.stdout, "patched", meta)
	return nil
}

func (a *App) runRevisions(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	revs, err := hub.ListMetadataRevisions(args[0])
	if err != nil {
		return err
	}
	printRevisions(a.stdout, revs)
	return nil
}

func (a *App) runRollback(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	if err := hub.RollbackMetadata(args[0], args[1]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "rolled back %s to %s\n", args[0], args[1])
	return nil
}

func (a *App) runMount(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	allowOther, _ := cmd.Flags().GetBool("allow-other")
	debug, _ := cmd.Flags().GetBool("debug")
	cacheDir, _ := cmd.Flags().GetString("cache-dir")
	hub, err := newHubFromFlagsFn(token, apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	opts := storhub.DefaultFUSEOptions()
	opts.AllowOther = allowOther
	opts.Debug = debug
	opts.CacheDir = cacheDir
	fsys, err := hub.NewFUSE(args[0], opts)
	if err != nil {
		return err
	}
	defer fsys.Close()
	if err := os.MkdirAll(args[1], 0o755); err != nil {
		return err
	}
	if err := fsys.Mount(args[1]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "mounted %s at %s\n", args[0], args[1])
	fmt.Fprintln(a.stdout, "press Ctrl+C to unmount")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = fsys.Unmount()
	}()
	fsys.Wait()
	return nil
}

func (a *App) runServeREST(cmd *cobra.Command, args []string) error {
	token, _ := cmd.Flags().GetString("token")
	apiBase, _ := cmd.Flags().GetString("api-base")
	listen, _ := cmd.Flags().GetString("listen")
	basePath, _ := cmd.Flags().GetString("base-path")
	authFile, _ := cmd.Flags().GetString("auth-file")
	hub, err := newRESTHubFromFlagsFn(token, apiBase, 0, 0, false)
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
	fmt.Fprintf(a.stdout, "serving REST API on %s%s %s\n", listen, opts.BasePath, mode)
	server := &http.Server{
		Addr:              listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
	return restListenAndServeFn(server)
}

func (a *App) loggingMiddleware(next http.Handler) http.Handler {
	if next == nil {
		return nil
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		a.logf("http start: method=%s path=%s remote=%s", r.Method, r.URL.RequestURI(), r.RemoteAddr)
		next.ServeHTTP(wrapped, r)
		a.logf("http done: method=%s path=%s status=%d duration=%s", r.Method, r.URL.RequestURI(), wrapped.status, time.Since(start).Round(time.Millisecond))
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
		TokenTTL:        file.TokenTTL,
	}, nil
}

func newHubFromFlags(token, apiBase string, chunkSize int64, concurrency int, public bool) (*storhub.StorHub, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	cfg := storhub.DefaultConfig()
	if strings.TrimSpace(apiBase) != "" {
		cfg.APIBaseURL = apiBase
	}
	if normalized := normalizeCLIChunkSize(chunkSize); normalized > 0 {
		cfg.ChunkSize = normalized
	}
	if concurrency > 0 {
		cfg.MaxConcurrentTransfers = concurrency
	}
	cfg.CreatePublicRepo = public
	cfg.LogLevel = cliLogLevel
	cfg.LogFormat = cliLogFormat
	cfg.LogColor = cliLogColor
	return storhub.NewStorHubWithConfig(token, cfg)
}

func newMountHubFromFlags(token, apiBase string) (*storhub.StorHub, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	cfg := storhub.DefaultConfig()
	if strings.TrimSpace(apiBase) != "" {
		cfg.APIBaseURL = apiBase
	}
	cfg.AtimePolicy = storcfg.AtimeNo
	return storhub.NewStorHubWithConfig(token, cfg)
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

func defaultDownloadPath(remotePath string) string {
	if base := filepath.Base(remotePath); base != "." && base != "/" {
		return base
	}
	return "downloaded-file"
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}
