package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainHelpAndErrorExit(t *testing.T) {
	helper := os.Args[0]
	help := exec.Command(helper, "-test.run=TestMainHelperProcess", "--", "--help")
	help.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	out, err := help.CombinedOutput()
	if err != nil {
		t.Fatalf("run help helper: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "StorHub CLI") {
		t.Fatalf("expected help output, got %q", out)
	}

	bad := exec.Command(helper, "-test.run=TestMainHelperProcess", "--", "unknown")
	bad.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	out, err = bad.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit for unknown command")
	}
	if !strings.Contains(string(out), "error: unknown command") {
		t.Fatalf("expected formatted error output, got %q", out)
	}
}

func TestMainHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" {
			os.Args = append([]string{"storhub"}, args[i+1:]...)
			main()
			return
		}
	}
	os.Exit(2)
}
