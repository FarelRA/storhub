package config

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/FarelRA/storhub/internal/chunking"
	"github.com/FarelRA/storhub/internal/logging"
)

const (
	defaultAPIBaseURL      = "https://api.github.com"
	defaultAPIVersion      = "2022-11-28"
	defaultRequestTimeout  = 5 * time.Minute
	defaultRepoDescription = "StorHub storage project"
	// DefaultChunkSize and DefaultBufferSize are aliases of the chunking
	// package's constants: the GitHub release-asset ceiling has one owner,
	// so a limit change cannot drift between the two spellings.
	DefaultChunkSize  = chunking.DefaultChunkSize
	DefaultBufferSize = chunking.DefaultBufferSize
)

type AtimePolicy string

const (
	AtimeRelatime AtimePolicy = "relatime"
	AtimeStrict   AtimePolicy = "strictatime"
	AtimeNo       AtimePolicy = "noatime"
)

type Config struct {
	// Logger, when set, is the logger. Supplying it together with any of
	// the LogLevel/LogFormat/LogColor/LogOutput knobs fails Validate
	// loudly: silent precedence between overlapping mechanisms hides
	// configuration mistakes. Those knobs exist only to build a logger
	// when none was supplied.
	Logger *slog.Logger
	// LogOutput is the destination used when building a default logger.
	LogOutput io.Writer
	// LogLevel/LogFormat are normalized (trimmed, lowercased) by
	// WithDefaults and validated by Validate; unknown values fail loudly
	// instead of silently mapping to something else.
	LogLevel  string
	LogFormat string
	LogColor  bool

	APIBaseURL       string
	APIVersion       string
	HTTPClient       *http.Client
	ChunkSize        int64
	BufferSize       int
	RepoDescription  string
	CreatePublicRepo bool
	MaxRetries       int
	BaseRetryDelay   time.Duration
	MaxRetryDelay    time.Duration

	// RateReserve keeps this many requests of the hourly GitHub budget
	// unspent as headroom for recovery operations. Negative restores the
	// client default.
	RateReserve int64
	// RateMaxWait bounds any single rate-limit wait the client may take.
	// Zero takes the library default (15m, for long-running processes);
	// a negative value opts into fail-fast behavior.
	RateMaxWait time.Duration
	// RatePointsPerMin caps secondary rate-limit points per minute
	// (GET=1, writes=5; GitHub's documented ceiling is 900).
	RatePointsPerMin int64
	// RateContentPerMin caps content-generating requests per minute
	// such as release-asset uploads (documented ceiling: 80).
	RateContentPerMin int64
	// MaxConcurrentRequests bounds in-flight API requests (GitHub's
	// documented ceiling is 100).
	MaxConcurrentRequests int64

	// TransferThroughput is the conservative bytes-per-second assumption
	// used to size upload/download deadlines; large transfers get
	// size/throughput seconds instead of a fixed timeout. Zero takes the
	// library default (1 MiB/s).
	TransferThroughput int64

	AtimePolicy AtimePolicy
	// MaxTrackedProjects bounds how many projects' metadata stay resident
	// in memory. The cap is applied when a new project joins: the least-
	// recently-used clean entry is evicted (dirty entries always survive).
	// Each entry pins one RepoMetadata clone - potentially MBs for large
	// repos - plus one parked goroutine.
	MaxTrackedProjects int
	GitCacheDir        string
	DisableGitBackend  bool
	Now                func() time.Time
	Sleep              func(context.Context, time.Duration) error
}

func Default() Config {
	return Config{
		APIBaseURL:            defaultAPIBaseURL,
		APIVersion:            defaultAPIVersion,
		HTTPClient:            newDefaultHTTPClient(),
		ChunkSize:             DefaultChunkSize,
		BufferSize:            DefaultBufferSize,
		RepoDescription:       defaultRepoDescription,
		CreatePublicRepo:      false,
		MaxRetries:            4,
		BaseRetryDelay:        500 * time.Millisecond,
		MaxRetryDelay:         8 * time.Second,
		RateReserve:           25,
		RateMaxWait:           15 * time.Minute,
		RatePointsPerMin:      720,
		RateContentPerMin:     60,
		MaxConcurrentRequests: 16,
		TransferThroughput:    1 << 20,
		LogOutput:             os.Stderr,
		LogLevel:              logging.LevelDebug,
		LogFormat:             logging.FormatPretty,
		LogColor:              true,
		AtimePolicy:           AtimeNo,
		// 64 entries is a generous working set for interactive use while
		// bounding worst-case residency; embedders touching thousands of
		// projects should size it deliberately.
		MaxTrackedProjects: 64,
		GitCacheDir:        defaultGitCacheDir(),
		Now:                time.Now,
		Sleep:              SleepWithContext,
	}
}

// WithDefaults fills every unset field from Default(). There is no
// zero-config fast path: field-by-field filling is cheap and a shortcut
// that forgets a field silently discards user configuration.
func (c Config) WithDefaults() Config {
	defaults := Default()
	if c.APIBaseURL == "" {
		c.APIBaseURL = defaults.APIBaseURL
	}
	if c.APIVersion == "" {
		c.APIVersion = defaults.APIVersion
	}
	if c.HTTPClient == nil {
		c.HTTPClient = defaults.HTTPClient
	}
	if c.ChunkSize <= 0 {
		c.ChunkSize = defaults.ChunkSize
	}
	if c.BufferSize <= 0 {
		c.BufferSize = defaults.BufferSize
	}
	if c.RepoDescription == "" {
		c.RepoDescription = defaults.RepoDescription
	}
	if c.BaseRetryDelay <= 0 {
		c.BaseRetryDelay = defaults.BaseRetryDelay
	}
	if c.MaxRetryDelay <= 0 {
		c.MaxRetryDelay = defaults.MaxRetryDelay
	}
	if c.LogOutput == nil {
		c.LogOutput = defaults.LogOutput
	}
	// Normalize case/whitespace but never map unknown values to something
	// else: Validate rejects them loudly.
	c.LogLevel = strings.ToLower(strings.TrimSpace(c.LogLevel))
	if c.LogLevel == "" {
		c.LogLevel = defaults.LogLevel
	}
	c.LogFormat = strings.ToLower(strings.TrimSpace(c.LogFormat))
	if c.LogFormat == "" {
		c.LogFormat = defaults.LogFormat
	}
	if c.Logger == nil {
		c.Logger = logging.NewLogger(logging.Options{
			Level:  c.LogLevel,
			Format: c.LogFormat,
			Color:  c.LogColor,
			Output: c.LogOutput,
		})
		// The knobs have been consumed into the logger; clearing them
		// keeps the single-mechanism invariant (Validate rejects Logger+
		// knobs) true for every config that went through WithDefaults.
		c.LogLevel, c.LogFormat, c.LogColor, c.LogOutput = "", "", false, nil
	}
	if c.AtimePolicy == "" {
		c.AtimePolicy = defaults.AtimePolicy
	}
	if c.MaxTrackedProjects <= 0 {
		c.MaxTrackedProjects = defaults.MaxTrackedProjects
	}
	if c.GitCacheDir == "" {
		c.GitCacheDir = defaults.GitCacheDir
	}
	if c.Now == nil {
		c.Now = defaults.Now
	}
	if c.Sleep == nil {
		c.Sleep = defaults.Sleep
	}
	return c
}

// defaultGitCacheDir returns a per-process cache directory. A shared
// /tmp/storhub would let concurrent processes (or other users on multi-user
// machines) collide on the same git workspaces.
// CacheBase returns the root directory for storhub's local caches:
// $STORHUB_CACHE_DIR when set, otherwise the platform user cache dir
// (~/.cache/storhub on Linux per XDG), falling back to a temp
// directory when no home is available. Component caches live beneath
// it: git/ for backend working repos, fuse/<project>/ for overlays.
func CacheBase() string {
	if custom := strings.TrimSpace(os.Getenv("STORHUB_CACHE_DIR")); custom != "" {
		return custom
	}
	if userCache, err := os.UserCacheDir(); err == nil && userCache != "" {
		return filepath.Join(userCache, "storhub")
	}
	return filepath.Join(os.TempDir(), "storhub")
}

func defaultGitCacheDir() string {
	return filepath.Join(CacheBase(), "git")
}

// DefaultGitCacheBase exposes the git cache base for callers that create
// per-process roots beneath it.
func DefaultGitCacheBase() string {
	return filepath.Join(CacheBase(), "git")
}

func newDefaultHTTPClient() *http.Client {
	// Clone the default transport when possible; never assume its concrete
	// type, a custom DefaultTransport must not panic the process.
	var transport *http.Transport
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = t.Clone()
	} else {
		transport = &http.Transport{}
	}
	// Transfers are strictly sequential: a couple of pooled connections
	// per host is plenty, and small pools avoid socket buildup.
	transport.MaxIdleConns = 4
	transport.MaxIdleConnsPerHost = 2
	transport.MaxConnsPerHost = 4
	transport.IdleConnTimeout = 90 * time.Second
	return &http.Client{Timeout: defaultRequestTimeout, Transport: transport}
}

// SleepWithContext is the default config Sleep implementation.
func SleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Validate rejects configurations that would otherwise fail silently or
// behave surprisingly at operation time. Empty optional fields are fine;
// unknown values are not.
func (c Config) Validate() error {
	if c.Logger != nil {
		var conflicts []string
		if c.LogLevel != "" {
			conflicts = append(conflicts, "LogLevel")
		}
		if c.LogFormat != "" {
			conflicts = append(conflicts, "LogFormat")
		}
		if c.LogColor {
			conflicts = append(conflicts, "LogColor")
		}
		if c.LogOutput != nil {
			conflicts = append(conflicts, "LogOutput")
		}
		if len(conflicts) > 0 {
			return fmt.Errorf("logger and log knobs %v are mutually exclusive: configure one mechanism", conflicts)
		}
	}
	if !logging.ValidLevel(c.LogLevel) {
		return fmt.Errorf("invalid log level %q (known: %v)", c.LogLevel, logging.KnownLevels())
	}
	if !logging.ValidFormat(c.LogFormat) {
		return fmt.Errorf("invalid log format %q (known: %v)", c.LogFormat, logging.KnownFormats())
	}
	switch c.AtimePolicy {
	case "", AtimeRelatime, AtimeStrict, AtimeNo:
	default:
		return fmt.Errorf("invalid atime policy %q (known: %s, %s, %s)", c.AtimePolicy, AtimeRelatime, AtimeStrict, AtimeNo)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("MaxRetries must be >= 0, got %d", c.MaxRetries)
	}
	if c.MaxTrackedProjects < 0 {
		return fmt.Errorf("MaxTrackedProjects must be >= 0, got %d", c.MaxTrackedProjects)
	}
	if c.ChunkSize < 0 {
		return fmt.Errorf("ChunkSize must be >= 0, got %d", c.ChunkSize)
	}
	if c.BufferSize < 0 {
		return fmt.Errorf("BufferSize must be >= 0, got %d", c.BufferSize)
	}
	return nil
}
