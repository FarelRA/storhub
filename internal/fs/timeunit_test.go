package fs

import (
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// Freshness TTLs derive from shared units: absolute values pin current
// behavior, whole multiples pin the symmetry.
func TestFreshnessTTLsSymmetric(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
		base time.Duration
	}{
		{"statfs TTL", statfsCacheTTL, 2 * time.Second, storcfg.TickUnit},
		{"group TTL", groupCacheTTL, 5 * time.Minute, storcfg.PatienceUnit},
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
