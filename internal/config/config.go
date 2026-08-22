package config

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/FarelRA/storhub/internal/logging"
)

const (
	defaultAPIBaseURL      = "https://api.github.com"
	defaultAPIVersion      = "2022-11-28"
	defaultRequestTimeout  = 5 * time.Minute
	defaultRepoDescription = "StorHub storage project"
	DefaultChunkSize       = int64(2*1024*1024*1024) - 1
	DefaultBufferSize      = 1 * 1024 * 1024
)

type AtimePolicy string

const (
	AtimeRelatime AtimePolicy = "relatime"
	AtimeStrict   AtimePolicy = "strictatime"
	AtimeNo       AtimePolicy = "noatime"
)

type Config struct {
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
	Logger                 *slog.Logger
	LogOutput              io.Writer
	LogLevel               string
	LogFormat              string
	LogColor               bool
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
		Sleep:                  sleepWithContext,
	}
}

func (c Config) WithDefaults() Config {
	if isZeroConfig(c) {
		return Default()
	}
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
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
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
	c.LogLevel = logging.NormalizeLevel(c.LogLevel)
	if c.LogLevel == "" {
		c.LogLevel = defaults.LogLevel
	}
	c.LogFormat = logging.NormalizeFormat(c.LogFormat)
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

func isZeroConfig(c Config) bool {
	return c.APIBaseURL == "" &&
		c.APIVersion == "" &&
		c.HTTPClient == nil &&
		c.ChunkSize == 0 &&
		c.BufferSize == 0 &&
		c.RepoDescription == "" &&
		!c.CreatePublicRepo &&
		c.MaxRetries == 0 &&
		c.BaseRetryDelay == 0 &&
		c.MaxRetryDelay == 0 &&
		c.Logger == nil &&
		c.LogOutput == nil &&
		c.LogLevel == "" &&
		c.LogFormat == "" &&
		!c.LogColor &&
		c.AtimePolicy == "" &&
		c.Now == nil &&
		c.Sleep == nil
}

func defaultGitCacheDir() string {
	return filepath.Join(os.TempDir(), "storhub")
}

func newDefaultHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 128
	transport.MaxIdleConnsPerHost = 32
	transport.MaxConnsPerHost = 64
	transport.IdleConnTimeout = 90 * time.Second
	return &http.Client{Timeout: defaultRequestTimeout, Transport: transport}
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
