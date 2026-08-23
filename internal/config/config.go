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

	APIBaseURL             string
	APIVersion             string
	HTTPClient             *http.Client
	ChunkSize              int64
	BufferSize             int
	RepoDescription        string
	CreatePublicRepo       bool
	MaxRetries             int
	BaseRetryDelay         time.Duration
	MaxRetryDelay          time.Duration
	AtimePolicy            AtimePolicy
	MetadataCommitInterval time.Duration
	GitCacheDir            string
	DisableGitBackend      bool
	Now                    func() time.Time
	Sleep                  func(context.Context, time.Duration) error
}

func Default() Config {
	return Config{
		APIBaseURL:             defaultAPIBaseURL,
		APIVersion:             defaultAPIVersion,
		HTTPClient:             newDefaultHTTPClient(),
		ChunkSize:              DefaultChunkSize,
		BufferSize:             DefaultBufferSize,
		RepoDescription:        defaultRepoDescription,
		CreatePublicRepo:       false,
		MaxRetries:             4,
		BaseRetryDelay:         500 * time.Millisecond,
		MaxRetryDelay:          8 * time.Second,
		LogOutput:              os.Stderr,
		LogLevel:               logging.LevelDebug,
		LogFormat:              logging.FormatPretty,
		LogColor:               true,
		AtimePolicy:            AtimeNo,
		MetadataCommitInterval: 10 * time.Second,
		GitCacheDir:            defaultGitCacheDir(),
		Now:                    time.Now,
		Sleep:                  SleepWithContext,
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
	if c.MetadataCommitInterval <= 0 {
		c.MetadataCommitInterval = defaults.MetadataCommitInterval
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
func defaultGitCacheDir() string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("storhub-git-%d", os.Getpid()))
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
			return fmt.Errorf("Logger and log knobs %v are mutually exclusive: configure one mechanism", conflicts)
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
	if c.ChunkSize < 0 {
		return fmt.Errorf("ChunkSize must be >= 0, got %d", c.ChunkSize)
	}
	if c.BufferSize < 0 {
		return fmt.Errorf("BufferSize must be >= 0, got %d", c.BufferSize)
	}
	return nil
}
