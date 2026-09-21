package cli

import (
	"testing"

	"github.com/FarelRA/storhub/internal/test"
)

func TestPosixConformCLI(t *testing.T) {
	test.RequireConformance(t)
	fake := newPCFakeHub()
	adapter := &cliPOSIXSurface{project: "pc", hub: fake}
	results := test.Run(adapter, test.Filter(test.Table, test.SurfaceCLI))

	t.Logf("POSIX conformance via CLI: %d scenarios", len(results))
	for _, r := range results {
		if r.Pass {
			t.Logf("PASS %s", r.Name)
		} else {
			t.Logf("FAIL %s: %s", r.Name, r.Error)
		}
	}
	passed, failed := test.Summary(results)
	t.Logf("posixconform CLI: %d passed, %d failed, %d total", passed, failed, len(results))
	if failed > 0 {
		t.Fatalf("%d scenario(s) failed against the CLI surface", failed)
	}
}
