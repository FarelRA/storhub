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
	"strconv"
	"strings"
	"time"

	shlog "github.com/FarelRA/storhub/internal/logging"
	"github.com/FarelRA/storhub/storhub"
	"github.com/spf13/cobra"
)

func (a *App) newRestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rest [flags]",
		Short: "Start the REST API server",
		Long: `Rest serves the project's data over HTTP for the web console
and API clients. Serving without an auth file needs --allowanonymous.

Examples:
  storhub rest --listen :8080 --allowanonymous`,
		Args: usageArgs(cobra.NoArgs),
		RunE: a.runServeREST,
	}
	addRESTFlags(cmd)
	return cmd
}

type restAuthFile struct {
	Realm           string             `json:"realm"`
	TokenSigningKey string             `json:"token_signing_key"`
	TokenTTL        flexDuration       `json:"token_ttl"`
	Users           []storhub.RESTUser `json:"users"`
}

// flexDuration is a Go duration string ("2h", "30m") for token_ttl.
// A bare JSON number (legacy: seconds) still decodes this release but logs
// a deprecation warning : time.Duration's own unmarshaler would read a
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

func (a *App) runServeREST(cmd *cobra.Command, _ []string) error {
	token, apiBase := cmdAuth(cmd)
	listen, _ := cmd.Flags().GetString("listen")
	shlog.Debug(a.logger(), "rest serve start", "command", "rest", "listen", listen)
	hub, err := a.newCmdRESTHub(cmd.Context(), resolveToken(token), apiBase, 0, false)
	if err != nil {
		shlog.Error(a.logger(), "rest serve failed", "command", "rest", "listen", listen, "err", err)
		return err
	}
	opts, err := serveAuthOptions(cmd)
	if err != nil {
		shlog.Error(a.logger(), "rest serve failed", "command", "rest", "listen", listen, "err", err)
		return err
	}
	handler, err := a.buildRESTHandler(hub, opts)
	if err != nil {
		shlog.Error(a.logger(), "rest serve failed", "command", "rest", "listen", listen, "err", err)
		return err
	}
	server := newRESTServer(listen, handler)
	_, _ = fmt.Fprintf(a.stderr, "serving REST API on %s%s %s\n", listen, opts.BasePath, describeRESTAuth(opts))
	shlog.Info(a.logger(), "rest serving", "command", "rest", "listen", listen, "basepath", opts.BasePath, "auth", describeRESTAuth(opts))
	return a.serveRESTUntilSignal(server)
}

// serveAuthOptions resolves the shared REST serving policy: base path and
// authentication. Running without an auth file is a deliberate choice -
// require the explicit opt-in flag so an open server never happens by
// accident.
func serveAuthOptions(cmd *cobra.Command) (storhub.RESTOptions, error) {
	basePath, _ := cmd.Flags().GetString("basepath")
	authFile, _ := cmd.Flags().GetString("authfile")
	opts := storhub.DefaultRESTOptions()
	opts.BasePath = basePath
	if authFile == "" {
		authFile = os.Getenv("STORHUB_REST_AUTH_FILE")
	}
	if strings.TrimSpace(authFile) != "" {
		if noAuth, _ := cmd.Flags().GetBool("allowanonymous"); noAuth {
			// --allowanonymous would be silently ignored here; contradictory
			// auth intent is a command-line mistake, not a runtime failure.
			return opts, &usageError{errors.New("--allowanonymous has no effect when an auth file is supplied; drop one of them")}
		}
		auth, err := loadRESTAuthOptions(authFile)
		if err != nil {
			return opts, err
		}
		opts.Auth = auth
	} else {
		noAuth, _ := cmd.Flags().GetBool("allowanonymous")
		if !noAuth {
			// Same misuse class as the contradictory-flags case above:
			// serving wide open without the explicit opt-in flag is a
			// command-line mistake, so usageError (exit 2), not exit 1.
			return opts, &usageError{fmt.Errorf("refusing to serve unauthenticated REST API; provide --authfile or pass --allowanonymous")}
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
	if key, _ := cmd.Flags().GetString("sharekey"); strings.TrimSpace(key) != "" {
		return key
	}
	return os.Getenv("STORHUB_SHARE_SIGNING_KEY")
}

// buildRESTHandler builds the REST handler. It returns the REST layer's
// own handler directly: request logging lives in exactly one layer
// (rest.requestLogging), so the former CLI loggingMiddleware wrapper was
// removed from this chain. The middleware method stays (deprecated) for
// existing tests.
func (a *App) buildRESTHandler(hub *storhub.StorHub, opts storhub.RESTOptions) (http.Handler, error) {
	return a.seamRESTHandler()(hub, opts)
}

func describeRESTAuth(opts storhub.RESTOptions) string {
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
		Use:   "serve <project> <mountpoint>",
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
	allowOther, _ := cmd.Flags().GetBool("allowother")
	debug, _ := cmd.Flags().GetBool("debug")
	cacheDir, _ := cmd.Flags().GetString("cachedir")
	umaskRaw, _ := cmd.Flags().GetString("umask")
	listen, _ := cmd.Flags().GetString("listen")
	shlog.Debug(a.logger(), "serve start", "command", "serve", "project", args[0], "mountpoint", args[1], "listen", listen)

	hub, err := a.newCmdRESTHub(cmd.Context(), resolveToken(token), apiBase, 0, false)
	if err != nil {
		shlog.Error(a.logger(), "serve failed", "command", "serve", "project", args[0], "mountpoint", args[1], "listen", listen, "err", err)
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
		shlog.Error(a.logger(), "serve failed", "command", "serve", "project", args[0], "mountpoint", args[1], "listen", listen, "err", err)
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
		shlog.Error(a.logger(), "serve failed", "command", "serve", "project", args[0], "mountpoint", args[1], "listen", listen, "err", err)
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
	shlog.Info(a.logger(), "serve listening", "command", "serve", "project", args[0], "mountpoint", args[1], "listen", listen, "basepath", opts.BasePath, "auth", describeRESTAuth(opts))

	return a.joinServe(ctx, stop, fsys, args[1], server, fsDone, errCh, errDone)
}

// setupServeREST resolves auth policy and builds the REST server for serve.
// The REST surface is pinned to the served project.
func (a *App) setupServeREST(cmd *cobra.Command, hub *storhub.StorHub, project, listen string) (*http.Server, storhub.RESTOptions, error) {
	shlog.Debug(a.logger(), "rest setup start", "command", "serve", "project", project, "listen", listen)
	opts, err := serveAuthOptions(cmd)
	if err != nil {
		shlog.Error(a.logger(), "rest setup failed", "command", "serve", "project", project, "listen", listen, "err", err)
		return nil, opts, err
	}
	// `serve <project> <mount>`: the REST surface (and thus the web console)
	// is pinned to that project - no free-form selector needed.
	opts.DefaultProject = project
	handler, err := a.buildRESTHandler(hub, opts)
	if err != nil {
		shlog.Error(a.logger(), "rest setup failed", "command", "serve", "project", project, "listen", listen, "err", err)
		return nil, opts, err
	}
	shlog.Debug(a.logger(), "rest setup complete", "command", "serve", "project", project, "listen", listen)
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
	if serveErr != nil {
		shlog.Error(a.logger(), "serve failed", "mountpoint", mountPoint, "err", serveErr)
	}
	if joinErr != nil {
		shlog.Error(a.logger(), "serve teardown failed", "mountpoint", mountPoint, "err", joinErr)
	}
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
			shlog.Error(a.logger(), "rest serve failed", "command", "rest", "listen", server.Addr, "err", err)
			return err
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), restShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_, _ = fmt.Fprintf(a.stderr, "graceful shutdown failed: %v\n", err)
		shlog.Warn(a.logger(), "graceful shutdown failed", "command", "rest", "listen", server.Addr, "err", err)
	}
	// Metadata draining is NOT done here: Run's shutdownHub is the single
	// owner of hub.Shutdown for every command. Draining after the HTTP
	// server stops is exactly the right order - in-flight requests can
	// still mutate metadata, and shutdownHub commits all of it.
	return nil
}

// loggingMiddleware is deprecated: buildRESTHandler no longer wraps the
// REST handler with it (single log layer: rest.requestLogging). Kept for
// backward compatibility with existing tests; new code must not wire it
// into the HTTP chain.
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
		uri := shlog.RedactRequestURI(r.URL.RequestURI())
		shlog.Debug(a.logger(), "http start", "method", r.Method, "uri", uri, "remote", r.RemoteAddr)
		next.ServeHTTP(wrapped, r)
		shlog.Info(a.logger(), "http done", "method", r.Method, "uri", uri, "remote", r.RemoteAddr, "status", wrapped.Status(), "duration", time.Since(start).Round(time.Millisecond).String())
	})
}

// statusRecorder was removed: status/byte capture lives in
// internal/logging (HTTPRecorder) so one package owns one job.
// See logging/http_recorder.go.

func loadRESTAuthOptions(filePath string) (*storhub.RESTAuthOptions, error) {
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
	return &storhub.RESTAuthOptions{
		Realm:           file.Realm,
		Users:           file.Users,
		TokenSigningKey: key,
		TokenTTL:        file.TokenTTL.Duration(),
	}, nil
}
