package posixconform

import "fmt"

// Result records the outcome of one scenario.
type Result struct {
	Name  string
	Pass  bool
	Error string
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
