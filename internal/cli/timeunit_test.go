package cli

import (
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// The teardown ladder derives from one patience unit, budgeted by the
// autonomy of the waited party: kernel propagation (1u) < local drains
// (2u) < human and network phases (6u). Absolute values pin current
// behavior; whole multiples pin the symmetry. Worst-case sequential
// teardown is 2+6+2+1+6 = 17 units, covered by the 24-unit harness
// budget (see pcTeardownBudget).
func TestTeardownLadderSymmetric(t *testing.T) {
	t.Parallel()
	u := storcfg.PatienceUnit
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"rest join", restJoinTimeout, 2 * u},
		{"mount join", unmountJoinTimeout, 2 * u},
		{"rest shutdown", restShutdownTimeout, 2 * u},
		{"hub shutdown", hubShutdownTimeout, 6 * u},
		{"unmount budget", unmountRetryBudget, 6 * u},
		{"unmount base delay", unmountRetryBaseDelay, u / 5},
		{"unmount backoff cap", unmountBackoffCap, 2 * u},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: must be %v, got %v", c.name, c.want, c.got)
		}
	}
	var total time.Duration
	for _, c := range []time.Duration{restShutdownTimeout, unmountRetryBudget, unmountJoinTimeout, restJoinTimeout, hubShutdownTimeout} {
		total += c
	}
	if total != 18*u {
		t.Fatalf("sequential teardown must total 18 units, got %v", total)
	}
}

// HTTP server budgets derive from the units like the teardown ladder.
func TestHTTPBudgetsSymmetric(t *testing.T) {
	t.Parallel()
	u := storcfg.PatienceUnit
	if restReadHeaderBudget != 1*u {
		t.Fatalf("restReadHeaderBudget must be 1 unit, got %v", restReadHeaderBudget)
	}
	if restIdleBudget != 24*u {
		t.Fatalf("restIdleBudget must be 24 units, got %v", restIdleBudget)
	}
}
