package config

import (
	"os"
	"path/filepath"
	"reflect"
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

// assertDefaultEqual fails when got differs from Default() on any field.
// It walks the Config struct with reflection so a future field is checked
// automatically: scalar kinds compare by value, while func, pointer, and
// interface fields (Now, Sleep, HTTPClient, Logger, LogOutput) compare by
// presence, since func values have no meaningful equality.
func assertDefaultEqual(t *testing.T, got Config) {
	t.Helper()
	want := Default()
	gv, wv := reflect.ValueOf(got), reflect.ValueOf(want)
	typ := gv.Type()
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		g, w := gv.Field(i), wv.Field(i)
		k := g.Kind()
		if k == reflect.Func || k == reflect.Pointer || k == reflect.Interface {
			if g.IsNil() != w.IsNil() {
				t.Fatalf("field %s presence differs from Default(): got %+v want %+v", name, got, want)
			}
			continue
		}
		if !reflect.DeepEqual(g.Interface(), w.Interface()) {
			t.Fatalf("field %s differs from Default(): got %v want %v", name, g.Interface(), w.Interface())
		}
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

// TestReadFileConfigRejectsOversize pins the read backstop: a config file
// past maxFileConfigBytes fails loudly instead of buffering whole.
func TestReadFileConfigRejectsOversize(t *testing.T) {
	t.Parallel()
	big := make([]byte, maxFileConfigBytes+1)
	for i := range big {
		big[i] = 'x'
	}
	path := writeFileConfig(t, string(big))
	if _, err := ReadFileConfig(path); err == nil {
		t.Fatal("over-cap config file must fail loudly")
	}
	if _, err := LoadFile(path); err == nil {
		t.Fatal("over-cap config file must fail loudly via LoadFile")
	}
}
