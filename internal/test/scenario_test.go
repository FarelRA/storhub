package test

import (
	"testing"
)

// TestTableRunsAgainstOracle runs the whole conformance table against the
// in-memory oracle and fails loudly with scenario names on any mismatch.
func TestTableRunsAgainstOracle(t *testing.T) {
	if len(Table) < 25 {
		t.Fatalf("conformance table has %d scenarios, need at least 25", len(Table))
	}
	seen := make(map[string]bool, len(Table))
	for _, sc := range Table {
		if sc.Name == "" {
			t.Fatalf("conformance table has a scenario with an empty name")
		}
		if seen[sc.Name] {
			t.Fatalf("duplicate scenario name %q", sc.Name)
		}
		seen[sc.Name] = true
		if sc.Run == nil {
			t.Fatalf("scenario %q has a nil Run function", sc.Name)
		}
	}
	results := Run(NewMemSurface(), Table)
	passed, failed := Summary(results)
	for _, r := range results {
		if !r.Pass {
			t.Errorf("scenario %q FAILED: %s", r.Name, r.Error)
		}
	}
	t.Logf("posixconform: %d passed, %d failed, %d total", passed, failed, len(results))
	if failed > 0 {
		t.Fatalf("%d scenario(s) failed against MemSurface", failed)
	}
}
