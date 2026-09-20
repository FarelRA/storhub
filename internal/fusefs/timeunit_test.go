package fusefs

import (
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// FUSE patience values derive from the shared unit: absolute values pin
// current behavior, whole multiples pin the symmetry.
func TestFusePatienceSymmetric(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
		base time.Duration
	}{
		{"close backstop", closeOpMuTimeout, 5 * time.Second, storcfg.PatienceUnit},
		{"notify warn threshold", notifySlotWarnThreshold, 5 * time.Second, storcfg.PatienceUnit},
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
