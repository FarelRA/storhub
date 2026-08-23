package config

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultConfigProvidesUsableDefaults(t *testing.T) {
	cfg := Default()
	if cfg.APIBaseURL != defaultAPIBaseURL {
		t.Fatalf("unexpected api base: %q", cfg.APIBaseURL)
	}
	if cfg.APIVersion != defaultAPIVersion {
		t.Fatalf("unexpected api version: %q", cfg.APIVersion)
	}
	if cfg.HTTPClient == nil || cfg.HTTPClient.Transport == nil {
		t.Fatal("expected default http client")
	}
	if cfg.HTTPClient.Timeout != defaultRequestTimeout {
		t.Fatalf("unexpected timeout: %v", cfg.HTTPClient.Timeout)
	}
	if cfg.ChunkSize != DefaultChunkSize || cfg.BufferSize != DefaultBufferSize {
		t.Fatalf("unexpected transfer defaults: %+v", cfg)
	}
	if cfg.RepoDescription != defaultRepoDescription || cfg.CreatePublicRepo {
		t.Fatalf("unexpected repo defaults: %+v", cfg)
	}
	if cfg.MaxRetries != 4 || cfg.BaseRetryDelay != 500*time.Millisecond || cfg.MaxRetryDelay != 8*time.Second {
		t.Fatalf("unexpected retry defaults: %+v", cfg)
	}
	if cfg.Now == nil || cfg.Sleep == nil {
		t.Fatal("expected injected helpers")
	}
}

func TestWithDefaultsPreservesExplicitValuesAndFillsGaps(t *testing.T) {
	now := time.Unix(123, 0).UTC()
	sleep := func(context.Context, time.Duration) error { return nil }
	customClient := newDefaultHTTPClient()
	cfg := Config{
		APIBaseURL:       "https://example.test/api",
		HTTPClient:       customClient,
		ChunkSize:        64,
		BufferSize:       128,
		CreatePublicRepo: true,
		MaxRetries:       0,
		Now:              func() time.Time { return now },
		Sleep:            sleep,
	}
	got := cfg.WithDefaults()
	if got.APIBaseURL != cfg.APIBaseURL || got.HTTPClient != customClient {
		t.Fatalf("expected explicit values preserved: %+v", got)
	}
	if got.APIVersion != defaultAPIVersion || got.RepoDescription != defaultRepoDescription {
		t.Fatalf("expected defaults filled: %+v", got)
	}
	if got.ChunkSize != 64 || got.BufferSize != 128 {
		t.Fatalf("unexpected transfer values: %+v", got)
	}
	if !got.CreatePublicRepo || got.MaxRetries != 0 || got.Now() != now || got.Sleep == nil || got.Sleep(context.Background(), 0) != nil {
		t.Fatalf("unexpected preserved flags/helpers: %+v", got)
	}
}

func TestWithDefaultsHandlesZeroAndNegativeValues(t *testing.T) {
	got := (Config{MaxRetries: -2}).WithDefaults()
	// Negative values are preserved for Validate to reject loudly instead
	// of being silently clamped.
	if got.MaxRetries != -2 {
		t.Fatalf("expected negative retries preserved for validation, got %d", got.MaxRetries)
	}
	if err := got.Validate(); err == nil {
		t.Fatal("expected negative retries to fail validation")
	}
	if got.BaseRetryDelay != Default().BaseRetryDelay || got.MaxRetryDelay != Default().MaxRetryDelay {
		t.Fatalf("expected default retry delays, got %+v", got)
	}
	zero := (Config{}).WithDefaults()
	if zero.APIBaseURL != Default().APIBaseURL || zero.BufferSize != Default().BufferSize {
		t.Fatalf("expected full zero-config defaults, got %+v", zero)
	}
}

func TestSleepWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := SleepWithContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context, got %v", err)
	}
	if err := SleepWithContext(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("expected timer completion, got %v", err)
	}
}

// TestWithDefaultsFillsEachFieldIndependently walks every field: starting
// from a zero Config, each field must end up at its default value, so a
// partially populated config can never silently drop a knob.
func TestWithDefaultsFillsEachFieldIndependently(t *testing.T) {
	filled := Config{}.WithDefaults()
	defaults := Default()
	checks := map[string]struct{ got, want any }{
		"APIBaseURL":             {filled.APIBaseURL, defaults.APIBaseURL},
		"APIVersion":             {filled.APIVersion, defaults.APIVersion},
		"ChunkSize":              {filled.ChunkSize, defaults.ChunkSize},
		"BufferSize":             {filled.BufferSize, defaults.BufferSize},
		"RepoDescription":        {filled.RepoDescription, defaults.RepoDescription},
		"MaxRetries":             {filled.MaxRetries, 0},
		"BaseRetryDelay":         {filled.BaseRetryDelay, defaults.BaseRetryDelay},
		"MaxRetryDelay":          {filled.MaxRetryDelay, defaults.MaxRetryDelay},
		"LogLevel":               {filled.LogLevel, defaults.LogLevel},
		"LogFormat":              {filled.LogFormat, defaults.LogFormat},
		"LogOutput":              {(filled.LogOutput != nil), true},
		"Logger":                 {(filled.Logger != nil), true},
		"HTTPClient":             {(filled.HTTPClient != nil), true},
		"AtimePolicy":            {filled.AtimePolicy, defaults.AtimePolicy},
		"MetadataCommitInterval": {filled.MetadataCommitInterval, defaults.MetadataCommitInterval},
		"GitCacheDir":            {(filled.GitCacheDir != ""), true},
		"Now":                    {(filled.Now != nil), true},
		"Sleep":                  {(filled.Sleep != nil), true},
	}
	for name, check := range checks {
		if check.got != check.want {
			t.Fatalf("%s not defaulted: got %v want %v", name, check.got, check.want)
		}
	}
}

// TestValidateRejectsUnknownLogSettings pins the loud-failure contract for
// unknown log levels and formats after normalization.
func TestValidateRejectsUnknownLogSettings(t *testing.T) {
	defaulted := Config{}.WithDefaults()
	if err := defaulted.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	cfg := Config{}.WithDefaults()
	cfg.LogLevel = "loud"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unknown level must fail validation")
	}
	cfg = Config{}.WithDefaults()
	cfg.LogFormat = "yaml"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unknown format must fail validation")
	}
}
