package cli

import (
	"bytes"
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
