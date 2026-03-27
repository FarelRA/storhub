package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainRequiresToken(t *testing.T) {
	out := runFuseHelper(t, nil)
	if !strings.Contains(out, "GITHUB_TOKEN environment variable not set") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestMainRequiresMountPoint(t *testing.T) {
	out := runFuseHelper(t, []string{"GITHUB_TOKEN=test-token"})
	if !strings.Contains(out, "STORHUB_MOUNT_POINT environment variable not set") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestMainRequiresProject(t *testing.T) {
	out := runFuseHelper(t, []string{"GITHUB_TOKEN=test-token", "STORHUB_MOUNT_POINT=/tmp/demo"})
	if !strings.Contains(out, "STORHUB_PROJECT environment variable not set") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func runFuseHelper(t *testing.T, extraEnv []string) string {
	t.Helper()
	helper := os.Args[0]
	cmd := exec.Command(helper, "-test.run=TestFuseMountHelperProcess", "--", "main")
	cmd.Env = append(os.Environ(), "GO_WANT_FUSE_MOUNT_HELPER=1")
	cmd.Env = append(cmd.Env, extraEnv...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected fuse-mount example to fail in helper")
	}
	return string(out)
}

func TestFuseMountHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_FUSE_MOUNT_HELPER") != "1" {
		return
	}
	main()
	os.Exit(0)
}
