package config

import (
	"testing"
	"time"
)

// The two time bases every timeout in the system derives from. Absolute
// values are pinned here: changing a base retunes the whole system on
// purpose, never by accident.
func TestTimeUnitsPinned(t *testing.T) {
	t.Parallel()
	if TickUnit != 50*time.Millisecond {
		t.Fatalf("TickUnit must be 50ms, got %v", TickUnit)
	}
	if PatienceUnit != 5*time.Second {
		t.Fatalf("PatienceUnit must be 5s, got %v", PatienceUnit)
	}
	d := Default()
	if d.BaseRetryDelay != 500*time.Millisecond {
		t.Fatalf("BaseRetryDelay must be 500ms, got %v", d.BaseRetryDelay)
	}
	if d.MaxRetryDelay != 8*time.Second {
		t.Fatalf("MaxRetryDelay must be 8s, got %v", d.MaxRetryDelay)
	}
	if d.RevivalTimeout != 5*time.Second {
		t.Fatalf("RevivalTimeout must be 5s, got %v", d.RevivalTimeout)
	}
	if d.RateMaxWait != 15*time.Minute {
		t.Fatalf("RateMaxWait must be 15m, got %v", d.RateMaxWait)
	}
}
