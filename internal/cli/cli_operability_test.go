package cli

import (
	"strings"
	"testing"
)

// Operability surfaces (GC, status, re-enable) must be usable end to end
// through the CLI: every storage capability gets a command, no Go-only
// orphans.
func TestOperabilityCommandsSuccessPaths(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	for _, args := range [][]string{
		{"gc", "--token", "x", "demo", "--dry-run"},
		{"gc", "--token", "x", "demo"},
		{"status", "--token", "x", "demo"},
		{"re-enable", "--token", "x", "demo"},
	} {
		app, _, _ := newTestApp(t)
		if err := app.Run(args); err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
	}
	app, _, stderr := newTestApp(t)
	if err := app.Run([]string{"gc", "--token", "x", "demo", "--dry-run"}); err != nil {
		t.Fatalf("gc dry-run: %v", err)
	}
	if out := stderr(); !strings.Contains(out, "would collect demo") {
		t.Fatalf("gc dry-run must preview, got %q", out)
	}
	if err := app.Run([]string{"status", "--token", "x", "demo"}); err != nil {
		t.Fatalf("status: %v", err)
	}
	if out := stderr(); !strings.Contains(out, "demo: healthy") {
		t.Fatalf("status must report healthy, got %q", out)
	}
	if err := app.Run([]string{"re-enable", "--token", "x", "demo"}); err != nil {
		t.Fatalf("re-enable: %v", err)
	}
	if out := stderr(); !strings.Contains(out, "re-enabled demo") {
		t.Fatalf("re-enable must confirm, got %q", out)
	}
}
