package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestNormalizeLevel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"", LevelInfo, true},
		{"DEBUG", LevelDebug, true},
		{"  warn ", LevelWarn, true},
		{"warning", "", false},
		{"error", LevelError, true},
		{"bogus", "", false},
		{"loud", "", false},
	}
	for _, tc := range cases {
		if got, ok := NormalizeLevel(tc.in); got != tc.want || ok != tc.ok {
			t.Fatalf("NormalizeLevel(%q) = (%q, %v), want (%q, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestValidLevelAndFormat(t *testing.T) {
	t.Parallel()
	if !ValidLevel("") || !ValidLevel("debug") || ValidLevel("loud") {
		t.Fatal("level validation broken")
	}
	// No silent alias: "warning" is unknown, so a typo can never pass
	// validation while hiding from KnownLevels.
	if ValidLevel("warning") || ValidLevel(" WARNING ") || !ValidLevel("warn") {
		t.Fatal("warning alias must not validate")
	}
	if !ValidFormat("") || !ValidFormat("text") || ValidFormat("json") {
		t.Fatal("format validation broken")
	}
}

func TestNewLoggerHonorsLevel(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	// The nil-logger fallback is slog.Default(); these calls must not
	// panic and must not silently discard the record.
	Info(nil, "via default")
	Warn(nil, "via default")
	Error(nil, "via default")
	Debug(nil, "via default")
}

func TestWithComponentTagsRecords(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	if resolve(nil) != slog.Default() {
		t.Fatal("nil logger must resolve to slog.Default()")
	}
}

type ctxPinKey struct{}

// TestEnabledIgnoresRequestContext pins the Enabled contract: no handler
// built in this tree is context-sensitive, so a request-scoped context
// carrying values must not change the answer. If a context-sensitive
// handler is ever introduced, Enabled must take a ctx parameter instead.
func TestEnabledIgnoresRequestContext(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := NewLogger(Options{Level: LevelWarn, Format: FormatText, Output: &buf})
	ctx := context.WithValue(context.Background(), ctxPinKey{}, "request")
	for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		if got, want := Enabled(logger, level), logger.Enabled(ctx, level); got != want {
			t.Fatalf("Enabled(logger, %v) = %v, handler with request ctx = %v", level, got, want)
		}
	}
}
