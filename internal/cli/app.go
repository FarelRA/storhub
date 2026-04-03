package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	shrest "github.com/FarelRA/storhub/rest"
	"github.com/FarelRA/storhub/storhub"
)

type App struct {
	stdout *os.File
	stderr *os.File
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
var newRESTHandlerFn = func(hub *storhub.StorHub, opts shrest.Options) (http.Handler, error) {
	return shrest.New(hub, opts)
}
var restListenAndServeFn = func(addr string, handler http.Handler) error {
	return http.ListenAndServe(addr, handler)
}

func New() *App {
	return &App{stdout: os.Stdout, stderr: os.Stderr}
}

func (a *App) Run(args []string) error {
	if len(args) == 0 {
		a.printRootHelp()
		return nil
	}
	cmd := args[0]
	rest := args[1:]
	a.logf("command start: %s %s", cmd, strings.Join(rest, " "))
	var err error
	switch cmd {
	case "help", "-h", "--help":
		a.printRootHelp()
		return nil
	case "upload":
		err = a.runUpload(rest, false)
	case "replace":
		err = a.runUpload(rest, true)
	case "download":
		err = a.runDownload(rest)
	case "ls":
		err = a.runList(rest)
	case "stat":
		err = a.runStat(rest)
	case "cat":
		err = a.runCat(rest)
	case "mkdir":
		err = a.runMkdir(rest)
	case "rm":
		err = a.runRemove(rest)
	case "mv":
		err = a.runMove(rest)
	case "append":
		err = a.runAppend(rest)
	case "write":
		err = a.runWrite(rest)
	case "patch":
		err = a.runPatch(rest)
	case "revisions":
		err = a.runRevisions(rest)
	case "rollback":
		err = a.runRollback(rest)
	case "mount":
		err = a.runMount(rest)
	case "serve-rest":
		err = a.runServeREST(rest)
	default:
		err = fmt.Errorf("unknown command %q\n\n%s", cmd, rootHelp)
	}
	if err != nil {
		a.logf("command failed: %s err=%v", cmd, err)
		return err
	}
	a.logf("command complete: %s", cmd)
	return nil
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

func (a *App) newHub(fs *flag.FlagSet) (hubClient, error) {
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token; defaults to GITHUB_TOKEN")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	chunkSize := fs.Int64("chunk-size", 0, "Chunk size in bytes")
	concurrency := fs.Int("concurrency", 0, "Max concurrent transfers")
	public := fs.Bool("public", false, "Create public repos instead of private")
	fs.SetOutput(a.stderr)
	fs.Usage = func() {
		fmt.Fprintln(a.stderr, fs.Name()+" usage:")
		fs.PrintDefaults()
	}
	parse := func(args []string) error { return fs.Parse(args) }
	_ = parse
	if strings.TrimSpace(*token) == "" {
		return nil, errors.New("missing GitHub token; pass --token or set GITHUB_TOKEN")
	}
	cfg := storhub.DefaultConfig()
	if strings.TrimSpace(*apiBase) != "" {
		cfg.APIBaseURL = *apiBase
	}
	if normalized := normalizeCLIChunkSize(*chunkSize); normalized > 0 {
		cfg.ChunkSize = normalized
	}
	if *concurrency > 0 {
		cfg.MaxConcurrentTransfers = *concurrency
	}
	cfg.CreatePublicRepo = *public
	hub, err := storhub.NewStorHubWithConfig(*token, cfg)
	if err != nil {
		return nil, err
	}
	return storhubClient{StorHub: hub}, nil
}

func (a *App) parseCommand(name, usage string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	fs.Usage = func() {
		fmt.Fprintf(a.stderr, "Usage: %s\n\n", usage)
		fs.PrintDefaults()
	}
	return fs
}

func (a *App) runUpload(args []string, replace bool) error {
	usage := "storhub " + ternary(replace, "replace", "upload") + " [flags] <project> <remote-path> <local-path>"
	fs := flag.NewFlagSet(ternary(replace, "replace", "upload"), flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	chunkSize := fs.Int64("chunk-size", 0, "Chunk size in bytes")
	concurrency := fs.Int("concurrency", 0, "Max concurrent transfers")
	public := fs.Bool("public", false, "Create public repos")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 3 {
		return fmt.Errorf("usage: %s", usage)
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, *chunkSize, *concurrency, *public)
	if err != nil {
		return err
	}
	project, remotePath, localPath := fs.Arg(0), fs.Arg(1), fs.Arg(2)
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

func (a *App) runDownload(args []string) error {
	fs := a.parseCommand("download", "storhub download [flags] <project> <remote-path> <local-path>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 3 {
		return fmt.Errorf("usage: storhub download [flags] <project> <remote-path> <local-path>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	if err := hub.DownloadFile(rest[0], rest[1], rest[2]); err != nil {
		return err
	}
	info, err := os.Stat(rest[2])
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "downloaded %s to %s (%d bytes)\n", rest[1], rest[2], info.Size())
	return nil
}

func (a *App) runList(args []string) error {
	fs := a.parseCommand("ls", "storhub ls [flags] <project> [path]")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	long := fs.Bool("l", false, "Show detailed listing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) < 1 || len(rest) > 2 {
		return fmt.Errorf("usage: storhub ls [flags] <project> [path]")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	dir := ""
	if len(rest) == 2 {
		dir = rest[1]
	}
	entries, err := hub.ReadDir(rest[0], dir)
	if err != nil {
		return err
	}
	printDirEntries(a.stdout, entries, *long)
	return nil
}

func (a *App) runStat(args []string) error {
	fs := a.parseCommand("stat", "storhub stat [flags] <project> <path>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return fmt.Errorf("usage: storhub stat [flags] <project> <path>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	entry, err := hub.StatPath(rest[0], rest[1])
	if err != nil {
		return err
	}
	printEntryInfo(a.stdout, entry)
	return nil
}

func (a *App) runCat(args []string) error {
	fs := a.parseCommand("cat", "storhub cat [flags] <project> <path>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return fmt.Errorf("usage: storhub cat [flags] <project> <path>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	entry, err := hub.StatPath(rest[0], rest[1])
	if err != nil {
		return err
	}
	data, err := hub.ReadFileAt(rest[0], rest[1], 0, entry.Size)
	if err != nil {
		return err
	}
	_, err = a.stdout.Write(data)
	return err
}

func (a *App) runMkdir(args []string) error {
	fs := a.parseCommand("mkdir", "storhub mkdir [flags] <project> <path>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return fmt.Errorf("usage: storhub mkdir [flags] <project> <path>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	if err := hub.Mkdir(rest[0], rest[1]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "created directory %s\n", rest[1])
	return nil
}

func (a *App) runRemove(args []string) error {
	fs := a.parseCommand("rm", "storhub rm [flags] <project> <path>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	recursive := fs.Bool("r", false, "Remove directory instead of file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return fmt.Errorf("usage: storhub rm [flags] <project> <path>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	if *recursive {
		err = hub.Rmdir(rest[0], rest[1])
	} else {
		err = hub.DeleteFile(rest[0], rest[1])
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "removed %s\n", rest[1])
	return nil
}

func (a *App) runMove(args []string) error {
	fs := a.parseCommand("mv", "storhub mv [flags] <project> <old-path> <new-path>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 3 {
		return fmt.Errorf("usage: storhub mv [flags] <project> <old-path> <new-path>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	if err := hub.Rename(rest[0], rest[1], rest[2]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "moved %s -> %s\n", rest[1], rest[2])
	return nil
}

func (a *App) runAppend(args []string) error {
	fs := a.parseCommand("append", "storhub append [flags] <project> <path> <text>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 3 {
		return fmt.Errorf("usage: storhub append [flags] <project> <path> <text>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	meta, err := hub.AppendFile(rest[0], rest[1], []byte(rest[2]))
	if err != nil {
		return err
	}
	printFileSummary(a.stdout, "appended", meta)
	return nil
}

func (a *App) runWrite(args []string) error {
	fs := a.parseCommand("write", "storhub write [flags] <project> <path> <offset> <text>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 4 {
		return fmt.Errorf("usage: storhub write [flags] <project> <path> <offset> <text>")
	}
	offset, err := strconv.ParseInt(rest[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset %q: %w", rest[2], err)
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	meta, err := hub.WriteFileAt(rest[0], rest[1], offset, []byte(rest[3]))
	if err != nil {
		return err
	}
	printFileSummary(a.stdout, "written", meta)
	return nil
}

func (a *App) runPatch(args []string) error {
	fs := a.parseCommand("patch", "storhub patch [flags] <project> <path> <offset> <delete-size> <text>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 5 {
		return fmt.Errorf("usage: storhub patch [flags] <project> <path> <offset> <delete-size> <text>")
	}
	offset, err := strconv.ParseInt(rest[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset %q: %w", rest[2], err)
	}
	deleteSize, err := strconv.ParseInt(rest[3], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid delete-size %q: %w", rest[3], err)
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	meta, err := hub.PatchFile(rest[0], rest[1], offset, deleteSize, []byte(rest[4]))
	if err != nil {
		return err
	}
	printFileSummary(a.stdout, "patched", meta)
	return nil
}

func (a *App) runRevisions(args []string) error {
	fs := a.parseCommand("revisions", "storhub revisions [flags] <project>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("usage: storhub revisions [flags] <project>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	revs, err := hub.ListMetadataRevisions(rest[0])
	if err != nil {
		return err
	}
	printRevisions(a.stdout, revs)
	return nil
}

func (a *App) runRollback(args []string) error {
	fs := a.parseCommand("rollback", "storhub rollback [flags] <project> <commit-sha>")
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return fmt.Errorf("usage: storhub rollback [flags] <project> <commit-sha>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	if err := hub.RollbackMetadata(rest[0], rest[1]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "rolled back %s to %s\n", rest[0], rest[1])
	return nil
}

func (a *App) runMount(args []string) error {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	allowOther := fs.Bool("allow-other", false, "Enable allow_other on the FUSE mount")
	debug := fs.Bool("debug", true, "Enable FUSE debug logging")
	cacheDir := fs.String("cache-dir", "", "Optional cache directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return fmt.Errorf("usage: storhub mount [flags] <project> <mount-point>")
	}
	hub, err := newHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	opts := storhub.DefaultFUSEOptions()
	opts.AllowOther = *allowOther
	opts.Debug = *debug
	opts.CacheDir = *cacheDir
	fsys, err := hub.NewFUSE(rest[0], opts)
	if err != nil {
		return err
	}
	defer fsys.Close()
	if err := os.MkdirAll(rest[1], 0o755); err != nil {
		return err
	}
	if err := fsys.Mount(rest[1]); err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "mounted %s at %s\n", rest[0], rest[1])
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

func (a *App) runServeREST(args []string) error {
	fs := flag.NewFlagSet("serve-rest", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	token := fs.String("token", os.Getenv("GITHUB_TOKEN"), "GitHub token")
	apiBase := fs.String("api-base", os.Getenv("STORHUB_API_BASE_URL"), "Optional GitHub API base URL")
	listen := fs.String("listen", ":8080", "Listen address")
	basePath := fs.String("base-path", "/api/v1", "REST API base path")
	authFile := fs.String("auth-file", os.Getenv("STORHUB_REST_AUTH_FILE"), "Optional JSON auth config file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: storhub serve-rest [flags]")
	}
	hub, err := newRESTHubFromFlagsFn(*token, *apiBase, 0, 0, false)
	if err != nil {
		return err
	}
	opts := shrest.DefaultOptions()
	opts.BasePath = *basePath
	if strings.TrimSpace(*authFile) != "" {
		auth, err := loadRESTAuthOptions(*authFile)
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
	fmt.Fprintf(a.stdout, "serving REST API on %s%s %s\n", *listen, opts.BasePath, mode)
	return restListenAndServeFn(*listen, handler)
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
	return storhub.NewStorHubWithConfig(token, cfg)
}

func ternary[T any](cond bool, left, right T) T {
	if cond {
		return left
	}
	return right
}

const rootHelp = `StorHub CLI

Friendly commands for GitHub-backed chunked storage, REST serving, and FUSE mounting.

Common commands:
  storhub upload <project> <remote-path> <local-path>
  storhub download <project> <remote-path> <local-path>
  storhub ls <project> [path]
  storhub stat <project> <path>
  storhub mkdir <project> <path>
  storhub mv <project> <old-path> <new-path>
  storhub rm <project> <path>
  storhub append <project> <path> <text>
  storhub write <project> <path> <offset> <text>
  storhub patch <project> <path> <offset> <delete-size> <text>
  storhub revisions <project>
  storhub rollback <project> <commit-sha>
  storhub serve-rest [flags]
  storhub mount <project> <mount-point>

Authentication:
  Set GITHUB_TOKEN or pass --token.

Examples:
  storhub upload docs-project docs/readme.txt ./README.md
  storhub ls docs-project docs
  storhub stat docs-project docs/readme.txt
  storhub serve-rest --listen :8080
  storhub mount docs-project ./mnt
`

func (a *App) printRootHelp() {
	fmt.Fprint(a.stdout, rootHelp)
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
