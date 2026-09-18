package posixconform

import (
	"testing"
	"time"
)

// TestTableAgainstMemSurface runs the whole conformance table against the
// in-memory oracle and fails loudly with scenario names on any mismatch.
func TestTableAgainstMemSurface(t *testing.T) {
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

func TestRunWithBudgetPassThrough(t *testing.T) {
	results := RunWithBudget(NewMemSurface(), Table, 30*time.Second)
	if len(results) != len(Table) {
		t.Fatalf("got %d results for %d scenarios", len(results), len(Table))
	}
	if _, failed := Summary(results); failed != 0 {
		t.Fatalf("%d scenarios failed against MemSurface under budget", failed)
	}
	for _, r := range results {
		if r.TimedOut {
			t.Fatalf("scenario %q wrongly marked timed out", r.Name)
		}
	}
}

func TestRunWithBudgetAbandonsStuckScenario(t *testing.T) {
	release := make(chan struct{})
	stuck := Scenario{Name: "stuck", Surfaces: SurfaceAll, Run: func(s Surface) error {
		<-release
		return nil
	}}
	after := Scenario{Name: "after", Surfaces: SurfaceAll, Run: func(s Surface) error { return nil }}
	results := RunWithBudget(NewMemSurface(), []Scenario{stuck, after}, 50*time.Millisecond)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].Pass || !results[0].TimedOut {
		t.Fatalf("stuck scenario: Pass=%v TimedOut=%v, want false/true", results[0].Pass, results[0].TimedOut)
	}
	if !results[1].Pass || results[1].TimedOut {
		t.Fatalf("scenario after stuck: Pass=%v TimedOut=%v, want true/false", results[1].Pass, results[1].TimedOut)
	}
	close(release)
}

func TestRunWithBudgetRecoversPanic(t *testing.T) {
	bad := Scenario{Name: "panics", Surfaces: SurfaceAll, Run: func(s Surface) error {
		panic("boom")
	}}
	results := RunWithBudget(NewMemSurface(), []Scenario{bad}, 30*time.Second)
	if len(results) != 1 || results[0].Pass || results[0].TimedOut {
		t.Fatalf("panic scenario: %+v, want one non-pass non-timeout failure", results)
	}
}
