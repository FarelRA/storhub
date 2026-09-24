package config

import (
	"bytes"
	"os"
	"testing"

	"github.com/FarelRA/storhub/internal/logging"
)

// TestWithDefaultsPreservesSuppliedLogger pins the logger-construction split:
// a caller-supplied logger passes through untouched, with knobs intact for
// Validate's single-mechanism check.
func TestWithDefaultsPreservesSuppliedLogger(t *testing.T) {
	t.Parallel()
	supplied := logging.NewLogger(logging.Options{Output: os.Stderr})
	got := Config{Logger: supplied}.WithDefaults()
	if got.Logger != supplied {
		t.Fatal("WithDefaults must not replace a supplied logger")
	}
	if got.APIBaseURL == "" || got.Now == nil || got.Sleep == nil {
		t.Fatal("non-logger defaults must still fill around a supplied logger")
	}
}

// TestWithDefaultsSuppliedLoggerStaysQuiet pins Warn discipline: applying
// routine defaults is not a recoverable condition, so a stock
// Config{Logger: l}.WithDefaults() must emit zero Warn lines. The old code
// warned once per defaulted key (~15 lines), training operators to ignore
// Warn.
func TestWithDefaultsSuppliedLoggerStaysQuiet(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	supplied := logging.NewLogger(logging.Options{Level: logging.LevelWarn, Output: &buf})
	_ = Config{Logger: supplied}.WithDefaults()
	if buf.Len() != 0 {
		t.Fatalf("expected zero Warn output for routine defaults, got %q", buf.String())
	}
}
