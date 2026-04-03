package config

import (
	"context"
	"net/http"
	"time"
)

const (
	defaultAPIBaseURL      = "https://api.github.com"
	defaultAPIVersion      = "2022-11-28"
	defaultRequestTimeout  = 5 * time.Minute
	defaultRepoDescription = "StorHub storage project"
	DefaultChunkSize       = int64(32 * 1024 * 1024)
	DefaultBufferSize      = 1 * 1024 * 1024
	DefaultMaxTransfers    = 8
)

type Config struct {
	APIBaseURL             string
	APIVersion             string
	HTTPClient             *http.Client
	ChunkSize              int64
	BufferSize             int
	MaxConcurrentTransfers int
	RepoDescription        string
	CreatePublicRepo       bool
	MaxRetries             int
	BaseRetryDelay         time.Duration
	MaxRetryDelay          time.Duration
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
		MaxConcurrentTransfers: DefaultMaxTransfers,
		RepoDescription:        defaultRepoDescription,
		CreatePublicRepo:       false,
		MaxRetries:             4,
		BaseRetryDelay:         500 * time.Millisecond,
		MaxRetryDelay:          8 * time.Second,
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
	if c.MaxConcurrentTransfers <= 0 {
		c.MaxConcurrentTransfers = defaults.MaxConcurrentTransfers
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
		c.MaxConcurrentTransfers == 0 &&
		c.RepoDescription == "" &&
		!c.CreatePublicRepo &&
		c.MaxRetries == 0 &&
		c.BaseRetryDelay == 0 &&
		c.MaxRetryDelay == 0 &&
		c.Now == nil &&
		c.Sleep == nil
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
