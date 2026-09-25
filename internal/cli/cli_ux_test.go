package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/FarelRA/storhub/internal/chunking"
)

func TestReadDataArgReadsStdinDash(t *testing.T) {
	t.Parallel()
	app := &App{stdin: strings.NewReader("from stdin")}
	got, err := readDataArg(app, "-")
	if err != nil {
		t.Fatalf("read stdin: %v", err)
	}
	if string(got) != "from stdin" {
		t.Fatalf("unexpected payload %q", got)
	}
	literal, err := readDataArg(app, "literal")
	if err != nil || string(literal) != "literal" {
		t.Fatalf("literal passthrough broken: %q %v", literal, err)
	}
}

func TestUsageErrorsAreClassified(t *testing.T) {
	t.Parallel()
	app := New()
	var stderr bytes.Buffer
	app.rootCmd.SetErr(&stderr)
	app.rootCmd.SetOut(&bytes.Buffer{})
	err := app.Run([]string{"upload", "--no-such-flag"})
	if err == nil {
		t.Fatal("expected flag error")
	}
	if !IsUsageError(err) {
		t.Fatalf("flag misuse should classify as usage error: %v", err)
	}
	if IsUsageError(errRuntimeShape()) {
		t.Fatal("plain errors are not usage errors")
	}
}

func TestChunkSizeClampWarns(t *testing.T) {
	t.Parallel()
	if _, err := normalizeCLIChunkSize(1024); err == nil || !IsUsageError(err) {
		t.Fatalf("below-floor size must fail loud, got %v", err)
	}
	if got, err := normalizeCLIChunkSize(minCLIChunkSize * 2); err != nil || got != minCLIChunkSize*2 {
		t.Fatalf("valid size must pass through: %d err %v", got, err)
	}
	if got, err := normalizeCLIChunkSize(9999999999); err != nil || got != chunking.MaxReleaseAssetSize {
		t.Fatalf("ceiling clamp broken: %d err %v", got, err)
	}
}

type runtimeErr struct{}

func (runtimeErr) Error() string { return "runtime failure" }

func errRuntimeShape() error { return runtimeErr{} }

func TestJSONOutputContracts(t *testing.T) {
	newSeededApp := func() (*App, func() string, func() string) {
		app, stdout, stderr := newTestApp(t)
		app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
			return &fakeHub{t: t}, nil
		}
		return app, stdout, stderr
	}
	// stat --json: stable object with the documented keys.
	app, stdout, _ := newSeededApp()
	if err := app.Run([]string{"stat", "--json", "demo", "docs"}); err != nil {
		t.Fatalf("stat --json: %v", err)
	}
	var statObj map[string]any
	if err := json.Unmarshal([]byte(stdout()), &statObj); err != nil {
		t.Fatalf("stat json decode: %v (%s)", err, stdout())
	}
	for _, key := range []string{"path", "is_dir", "size"} {
		if _, ok := statObj[key]; !ok {
			t.Fatalf("stat json missing key %q: %v", key, statObj)
		}
	}

	// ls --json: array (never null); entries expose name.
	app, stdout, _ = newSeededApp()
	if err := app.Run([]string{"ls", "--json", "demo"}); err != nil {
		t.Fatalf("ls --json: %v", err)
	}
	var list []map[string]any
	if err := json.Unmarshal([]byte(stdout()), &list); err != nil {
		t.Fatalf("ls json decode: %v (%s)", err, stdout())
	}
	if len(list) == 0 {
		t.Fatalf("expected non-empty entry array, got %q", stdout())
	}
	if _, ok := list[0]["name"]; !ok {
		t.Fatalf("ls json entries must expose name: %v", list[0])
	}

	// revisions --json: array shape.
	app, stdout, _ = newSeededApp()
	if err := app.Run([]string{"project", "revisions", "--json", "demo"}); err != nil {
		t.Fatalf("revisions --json: %v", err)
	}
	out := strings.TrimSpace(stdout())
	var revs []map[string]any
	if err := json.Unmarshal([]byte(out), &revs); err != nil {
		t.Fatalf("revisions json decode: %v (%s)", err, out)
	}
	if len(revs) == 0 || revs[0]["commit_sha"] == "" {
		t.Fatalf("revisions json missing commit_sha: %v", revs)
	}
}
