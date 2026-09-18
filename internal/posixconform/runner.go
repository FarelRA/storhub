package posixconform

import (
	"fmt"
	"time"
)

// Result records the outcome of one scenario.
type Result struct {
	Name  string
	Pass  bool
	Error string
	// TimedOut reports a budget abandonment (only RunWithBudget sets it):
	// the scenario may still be running, so the caller must tear down
	// around it (unmount, detach) rather than reuse the surface.
	TimedOut bool
}

// Run executes every scenario in table order against s, recovering panics per
// scenario and recording them as failures.
func Run(s Surface, table []Scenario) []Result {
	results := make([]Result, 0, len(table))
	for _, sc := range table {
		results = append(results, runOne(s, sc))
	}
	return results
}

func runOne(s Surface, sc Scenario) (r Result) {
	r.Name = sc.Name
	defer func() {
		if v := recover(); v != nil {
			r.Pass = false
			r.Error = fmt.Sprintf("panic: %v", v)
		}
	}()
	if err := sc.Run(s); err != nil {
		r.Pass = false
		r.Error = err.Error()
		return r
	}
	r.Pass = true
	return r
}

// Summary counts passed and failed results.
func Summary(results []Result) (passed, failed int) {
	for _, r := range results {
		if r.Pass {
			passed++
		} else {
			failed++
		}
	}
	return passed, failed
}

// RunWithBudget executes like Run but abandons a scenario that does not
// finish within budget, recording a timeout failure. It exists because a
// stuck kernel mount has no error return: without a budget one wedged
// scenario hangs the suite. Surfaces that can wedge (FUSE mounts) must use
// this; in-process surfaces use Run. The abandoned scenario keeps running
// in the background (its result channel is buffered), so the caller must
// isolate around it: the FUSE adapter mounts fresh per scenario and lazily
// detaches on Result.TimedOut.
func RunWithBudget(s Surface, table []Scenario, budget time.Duration) []Result {
	results := make([]Result, 0, len(table))
	for _, sc := range table {
		results = append(results, runOneBudget(s, sc, budget))
	}
	return results
}

func runOneBudget(s Surface, sc Scenario, budget time.Duration) (r Result) {
	r.Name = sc.Name
	type outcome struct {
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				done <- outcome{err: fmt.Errorf("panic: %v", v)}
			}
		}()
		if err := sc.Run(s); err != nil {
			done <- outcome{err: err}
			return
		}
		done <- outcome{}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			r.Pass = false
			r.Error = o.err.Error()
			return r
		}
		r.Pass = true
		return r
	case <-time.After(budget):
		r.Pass = false
		r.TimedOut = true
		r.Error = fmt.Sprintf("timed out after %s with requests unanswered; scenario abandoned", budget)
		return r
	}
}
