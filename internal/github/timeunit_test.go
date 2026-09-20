package github

import (
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// Server-dictated waits are still spelled in shared units (the server
// owns the resume instant; we own the spelling). Absolute values pin
// current behavior; whole multiples pin the symmetry.
func TestGithubTimeoutsSymmetric(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
		base time.Duration
	}{
		{"request timeout", defaultRequestTimeout, 5 * time.Minute, storcfg.PatienceUnit},
		{"client retry base", defaultBaseRetryDelay, 500 * time.Millisecond, storcfg.TickUnit},
		{"client retry cap", defaultMaxRetryDelay, 8 * time.Second, storcfg.TickUnit},
		{"secondary backoff base", secondaryBackoffBase, 60 * time.Second, storcfg.PatienceUnit},
		{"secondary backoff cap", secondaryBackoffCap, 15 * time.Minute, storcfg.PatienceUnit},
		{"governor max wait", defaultRateMaxWait, 15 * time.Minute, storcfg.PatienceUnit},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: must be %v, got %v", c.name, c.want, c.got)
		}
		if c.got%c.base != 0 {
			t.Errorf("%s: %v is not a whole multiple of its base %v", c.name, c.got, c.base)
		}
	}
}
