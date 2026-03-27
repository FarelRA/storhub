package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainRequiresToken(t *testing.T) {
	helper := os.Args[0]
	cmd := exec.Command(helper, "-test.run=TestRevisionsHelperProcess", "--", "main")
	cmd.Env = append(os.Environ(), "GO_WANT_REVISIONS_HELPER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected revisions example to fail without token")
	}
	if !strings.Contains(string(out), "GITHUB_TOKEN environment variable not set") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestRevisionsHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_REVISIONS_HELPER") != "1" {
		return
	}
	main()
	os.Exit(0)
}
