package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainRequiresToken(t *testing.T) {
	helper := os.Args[0]
	cmd := exec.Command(helper, "-test.run=TestRESTHelperProcess", "--", "main")
	cmd.Env = append(os.Environ(), "GO_WANT_REST_HELPER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected rest example to fail without token")
	}
	if !strings.Contains(string(out), "GITHUB_TOKEN environment variable not set") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestRESTHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_REST_HELPER") != "1" {
		return
	}
	main()
	os.Exit(0)
}
