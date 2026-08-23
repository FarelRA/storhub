package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestNormalizeLevelAndFormat(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", LevelInfo},
		{"DEBUG", LevelDebug},
		{"  warn ", LevelWarn},
		{"warning", LevelWarn},
		{"error", LevelError},
		{"bogus", LevelInfo},
	}
	for _, tc := range cases {
		if got := NormalizeLevel(tc.in); got != tc.want {
			t.Fatalf("NormalizeLevel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	for _, tc := range []struct{ in, want string }{{"", FormatPretty}, {"text", FormatText}, {"PRETTY", FormatPretty}, {"nope", FormatPretty}} {
		if got := NormalizeFormat(tc.in); got != tc.want {
			t.Fatalf("NormalizeFormat(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestValidLevelAndFormat(t *testing.T) {
	if !ValidLevel("") || !ValidLevel("debug") || ValidLevel("loud") {
		t.Fatal("level validation broken")
	}
	if !ValidFormat("") || !ValidFormat("text") || ValidFormat("json") {
		t.Fatal("format validation broken")
	}
}

func TestNewLoggerHonorsLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(Options{Level: LevelError, Format: FormatText, Output: &buf})
	logger.Info("dropped")
	if buf.Len() != 0 {
		t.Fatalf("info recorded at error level: %q", buf.String())
	}
	logger.Error("kept")
	if !strings.Contains(buf.String(), "kept") {
		t.Fatalf("error not recorded: %q", buf.String())
	}
}

func TestNilLoggerWrappersNeverDrop(t *testing.T) {
	// The nil-logger fallback is slog.Default(); these calls must not
	// panic and must not silently discard the record.
	Info(nil, "via default")
	Warn(nil, "via default")
	Error(nil, "via default")
	Debug(nil, "via default")
}

func TestWithComponentTagsRecords(t *testing.T) {
	var buf bytes.Buffer
	base := NewLogger(Options{Level: LevelDebug, Format: FormatText, Output: &buf})
	tagged := WithComponent(base, "rest")
	tagged.Info("hello")
	if !strings.Contains(buf.String(), "component=rest") {
		t.Fatalf("component tag missing: %q", buf.String())
	}
	// A nil logger falls back to the process default instead of dropping.
	if WithComponent(nil, "") == nil {
		t.Fatal("nil logger fallback returned nil")
	}
	if WithComponent(base, "  ") != base {
		t.Fatal("empty component should return the base logger unchanged")
	}
}

func TestResolveFallsBackToDefault(t *testing.T) {
	if resolve(nil) != slog.Default() {
		t.Fatal("nil logger must resolve to slog.Default()")
	}
}
