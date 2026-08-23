package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestReadDataArgReadsStdinDash(t *testing.T) {
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
	if got := normalizeCLIChunkSize(1024); got != minCLIChunkSize {
		t.Fatalf("clamp broken: %d", got)
	}
	if got := normalizeCLIChunkSize(minCLIChunkSize * 2); got != minCLIChunkSize*2 {
		t.Fatalf("valid size must pass through: %d", got)
	}
}

type runtimeErr struct{}

func (runtimeErr) Error() string { return "runtime failure" }

func errRuntimeShape() error { return runtimeErr{} }

func TestJSONOutputContracts(t *testing.T) {
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	// stat --json: stable object with the documented keys.
	app, stdout, _ := newTestApp(t)
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
	app, stdout, _ = newTestApp(t)
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
	app, stdout, _ = newTestApp(t)
	if err := app.Run([]string{"revisions", "--json", "demo"}); err != nil {
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
