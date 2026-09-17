package config

import (
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
	var _ = got.Logger
}
