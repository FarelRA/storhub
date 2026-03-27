package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainRequiresToken(t *testing.T) {
	out := runRESTAuthHelper(t, nil)
	if !strings.Contains(out, "GITHUB_TOKEN environment variable not set") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestMainRequiresPassword(t *testing.T) {
	out := runRESTAuthHelper(t, []string{"GITHUB_TOKEN=test-token"})
	if !strings.Contains(out, "STORHUB_REST_ADMIN_PASSWORD environment variable not set") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestMainRequiresSigningKey(t *testing.T) {
	out := runRESTAuthHelper(t, []string{"GITHUB_TOKEN=test-token", "STORHUB_REST_ADMIN_PASSWORD=secret"})
	if !strings.Contains(out, "STORHUB_REST_SIGNING_KEY environment variable not set") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func runRESTAuthHelper(t *testing.T, extraEnv []string) string {
	t.Helper()
	helper := os.Args[0]
	cmd := exec.Command(helper, "-test.run=TestRESTAuthHelperProcess", "--", "main")
	cmd.Env = append(os.Environ(), "GO_WANT_REST_AUTH_HELPER=1")
	cmd.Env = append(cmd.Env, extraEnv...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected rest-auth example to fail in helper")
	}
	return string(out)
}

func TestRESTAuthHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_REST_AUTH_HELPER") != "1" {
		return
	}
	main()
	os.Exit(0)
}
