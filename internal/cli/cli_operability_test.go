package cli

import (
	"context"
	"strings"
	"testing"
)

// Operability surfaces (prune incl. chunks scope, status, enable) must
// be usable end to end through the CLI under the project group: every
// storage capability gets a command, no Go-only orphans.
func TestOperabilityCommandsSuccessPaths(t *testing.T) {
	seed := func(app *App) {
		app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
			return &fakeHub{t: t}, nil
		}
	}
	for _, args := range [][]string{
		{"project", "prune", "--token", "x", "demo", "chunks", "--dry-run"},
		{"project", "prune", "--token", "x", "demo", "chunks"},
		{"project", "status", "--token", "x", "demo"},
		{"project", "enable", "--token", "x", "demo"},
	} {
		app, _, _ := newTestApp(t)
		seed(app)
		if err := app.Run(args); err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
	}
	app, _, stderr := newTestApp(t)
	seed(app)
	if err := app.Run([]string{"project", "prune", "--token", "x", "demo", "chunks", "--dry-run"}); err != nil {
		t.Fatalf("prune chunks dryrun: %v", err)
	}
	if out := stderr(); !strings.Contains(out, "would prune demo (chunks)") {
		t.Fatalf("prune chunks dryrun must preview, got %q", out)
	}
	if err := app.Run([]string{"project", "status", "--token", "x", "demo"}); err != nil {
		t.Fatalf("status: %v", err)
	}
	if out := stderr(); !strings.Contains(out, "demo: healthy") {
		t.Fatalf("status must report healthy, got %q", out)
	}
	if err := app.Run([]string{"project", "enable", "--token", "x", "demo"}); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if out := stderr(); !strings.Contains(out, "enabled demo") {
		t.Fatalf("enable must confirm, got %q", out)
	}
}
