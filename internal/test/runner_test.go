package test

import (
	"testing"
	"time"
)

// TestRunnerBudgetPassthrough runs the table under a generous budget: every
// scenario passes and none is marked timed out.
func TestRunnerBudgetPassthrough(t *testing.T) {
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

// TestRunnerBudgetAbandonsStuck proves the budget abandons a wedged
// scenario instead of wedging the suite: the stuck row times out while
// the row behind it still runs green.
func TestRunnerBudgetAbandonsStuck(t *testing.T) {
	release := make(chan struct{})
	stuck := Scenario{Name: "stuck", Surfaces: SurfaceAll, Run: func(_ Surface) error {
		<-release
		return nil
	}}
	after := Scenario{Name: "after", Surfaces: SurfaceAll, Run: func(_ Surface) error { return nil }}
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

// TestRunnerBudgetRecoversPanic proves a panicking scenario fails that row
// without taking down the runner.
func TestRunnerBudgetRecoversPanic(t *testing.T) {
	bad := Scenario{Name: "panics", Surfaces: SurfaceAll, Run: func(_ Surface) error {
		panic("boom")
	}}
	results := RunWithBudget(NewMemSurface(), []Scenario{bad}, 30*time.Second)
	if len(results) != 1 || results[0].Pass || results[0].TimedOut {
		t.Fatalf("panic scenario: %+v, want one non-pass non-timeout failure", results)
	}
}
