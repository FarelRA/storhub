package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFileConfig writes content to a temp file and returns its path.
func writeFileConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "storhub.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

// assertDefaultEqual fails when got differs from Default() on any scalar
// field. Func and pointer fields (Now, Sleep, HTTPClient, Logger) are
// compared by presence so DeepEqual's func-value rules cannot false-fail.
func assertDefaultEqual(t *testing.T, got Config) {
	t.Helper()
	want := Default()
	if got.APIBaseURL != want.APIBaseURL ||
		got.APIVersion != want.APIVersion ||
		got.ChunkSize != want.ChunkSize ||
		got.BufferSize != want.BufferSize ||
		got.RepoDescription != want.RepoDescription ||
		got.CreatePublicRepo != want.CreatePublicRepo ||
		got.MaxRetries != want.MaxRetries ||
		got.BaseRetryDelay != want.BaseRetryDelay ||
		got.MaxRetryDelay != want.MaxRetryDelay ||
		got.RevivalTimeout != want.RevivalTimeout ||
		got.RateReserve != want.RateReserve ||
		got.RateMaxWait != want.RateMaxWait ||
		got.RatePointsPerMin != want.RatePointsPerMin ||
		got.RateContentPerMin != want.RateContentPerMin ||
		got.MaxConcurrentRequests != want.MaxConcurrentRequests ||
		got.TransferThroughput != want.TransferThroughput ||
		got.LogLevel != want.LogLevel ||
		got.LogFormat != want.LogFormat ||
		got.LogColor != want.LogColor ||
		got.AtimePolicy != want.AtimePolicy ||
		got.MaxTrackedProjects != want.MaxTrackedProjects ||
		got.GitCacheDir != want.GitCacheDir ||
		got.JournalDir != want.JournalDir ||
		got.StrictConflicts != want.StrictConflicts ||
		got.DisableGitBackend != want.DisableGitBackend ||
		got.ObjectCacheMaxEntries != want.ObjectCacheMaxEntries ||
		got.HistoryWarnObjects != want.HistoryWarnObjects ||
		got.MaxConsecutiveCommitFailures != want.MaxConsecutiveCommitFailures {
		t.Fatalf("config differs from Default(): got %+v want %+v", got, want)
	}
	if (got.HTTPClient == nil) != (want.HTTPClient == nil) ||
		(got.Logger == nil) != (want.Logger == nil) ||
		(got.LogOutput == nil) != (want.LogOutput == nil) ||
		(got.Now == nil) != (want.Now == nil) ||
		(got.Sleep == nil) != (want.Sleep == nil) {
		t.Fatalf("config presence differs from Default(): got %+v want %+v", got, want)
	}
}

func TestLoadFileMissingIsNoopSuccess(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"", filepath.Join(t.TempDir(), "does-not-exist.json")} {
		got, err := LoadFile(path)
		if err != nil {
			t.Fatalf("LoadFile(%q) must succeed, got %v", path, err)
		}
		assertDefaultEqual(t, got)
	}
}

func TestLoadFileNoFileEqualsDefault(t *testing.T) {
	t.Parallel()
	got, err := LoadFile("")
	if err != nil {
		t.Fatalf("LoadFile(\"\") must succeed, got %v", err)
	}
	assertDefaultEqual(t, got)
}

func TestLoadFileUnknownKeyErrors(t *testing.T) {
	t.Parallel()
	path := writeFileConfig(t, `{"bogus_key": 1}`)
	if _, err := LoadFile(path); err == nil {
		t.Fatal("unknown file key must fail loudly")
	}
	if _, err := ReadFileConfig(path); err == nil {
		t.Fatal("unknown sparse file key must fail loudly")
	}
}

func TestLoadFileAppliesValues(t *testing.T) {
	t.Parallel()
	path := writeFileConfig(t, `{
		"api_base_url": "https://file.test/api",
		"chunk_size": 67108864,
		"create_public_repo": true,
		"log_level": "debug",
		"log_format": "text",
		"log_color": false
	}`)
	got, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if got.APIBaseURL != "https://file.test/api" {
		t.Fatalf("api_base_url not applied: %q", got.APIBaseURL)
	}
	if got.ChunkSize != 67108864 {
		t.Fatalf("chunk_size not applied: %d", got.ChunkSize)
	}
	if !got.CreatePublicRepo {
		t.Fatal("create_public_repo not applied")
	}
	if got.LogLevel != "debug" || got.LogFormat != "text" || got.LogColor {
		t.Fatalf("log knobs not applied: %+v", got)
	}
	// Untouched keys keep their defaults.
	if defaults := Default(); got.APIVersion != defaults.APIVersion || got.MaxRetries != defaults.MaxRetries {
		t.Fatalf("unrelated keys must keep defaults: %+v", got)
	}
}

func TestReadFileConfigIsSparse(t *testing.T) {
	t.Parallel()
	path := writeFileConfig(t, `{"log_level": "debug"}`)
	fc, err := ReadFileConfig(path)
	if err != nil {
		t.Fatalf("ReadFileConfig: %v", err)
	}
	if fc.LogLevel == nil || *fc.LogLevel != "debug" {
		t.Fatalf("log_level not captured: %+v", fc)
	}
	if fc.APIBaseURL != nil || fc.ChunkSize != nil || fc.CreatePublicRepo != nil ||
		fc.LogFormat != nil || fc.LogColor != nil {
		t.Fatalf("unset keys must stay nil: %+v", fc)
	}
	empty, err := ReadFileConfig("")
	if err != nil {
		t.Fatalf("ReadFileConfig(\"\") must succeed, got %v", err)
	}
	if empty != (FileConfig{}) {
		t.Fatalf("empty path must yield zero overlay, got %+v", empty)
	}
}

func TestLoadFileEnvOverridesFile(t *testing.T) {
	path := writeFileConfig(t, `{
		"api_base_url": "https://file.test/api",
		"log_level": "debug"
	}`)
	t.Setenv("STORHUB_API_BASE_URL", "https://env.test/api")
	t.Setenv("STORHUB_LOG_LEVEL", "error")
	got, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if got.APIBaseURL != "https://env.test/api" {
		t.Fatalf("env must override file api_base_url, got %q", got.APIBaseURL)
	}
	if got.LogLevel != "error" {
		t.Fatalf("env must override file log_level, got %q", got.LogLevel)
	}
}

func TestLoadFileMalformedErrors(t *testing.T) {
	t.Parallel()
	for _, content := range []string{`{not json`, `{"log_level": "debug",}`, `["log_level"]`} {
		path := writeFileConfig(t, content)
		if _, err := LoadFile(path); err == nil {
			t.Fatalf("malformed content %q must fail, got nil error", content)
		}
	}
}

func TestLoadFileEmptyFileIsNoop(t *testing.T) {
	t.Parallel()
	got, err := LoadFile(writeFileConfig(t, ``))
	if err != nil {
		t.Fatalf("empty file must succeed, got %v", err)
	}
	assertDefaultEqual(t, got)
}
