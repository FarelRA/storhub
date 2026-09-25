package config

import "testing"

// TestLoadFileNoFileStillAppliesEnv pins the single precedence chain:
// env wins over defaults even when no file is given.
func TestLoadFileNoFileStillAppliesEnv(t *testing.T) {
	t.Setenv(EnvLogLevel, "error")
	got, err := LoadFile("")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if got.LogLevel != "error" {
		t.Fatalf("LoadFile(\"\") must apply env, got LogLevel %q", got.LogLevel)
	}
}

// TestLoadFileMissingStillAppliesEnv pins the same for a missing path.
func TestLoadFileMissingStillAppliesEnv(t *testing.T) {
	t.Setenv(EnvAPIBaseURL, "https://env.test/api")
	got, err := LoadFile("/nonexistent/storhub.json")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if got.APIBaseURL != "https://env.test/api" {
		t.Fatalf("missing file must still apply env, got %q", got.APIBaseURL)
	}
}
