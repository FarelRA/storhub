package storage

import (
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// Every storage timeout derives from the shared time units (event-purity
// P0): absolute values below pin current behavior, and each must be a
// whole multiple of its tier's base. A value that cannot be expressed
// exactly is a design smell, caught here instead of in review.
func TestStorageTimeoutsSymmetric(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
		base time.Duration
	}{
		{"journal group-commit", journalGroupCommitWindow, 100 * time.Millisecond, storcfg.TickUnit},
		{"release cache TTL", releaseCacheTTL, 60 * time.Second, storcfg.PatienceUnit},
		{"metadata idle TTL", metaCacheIdleTTL, 30 * time.Minute, storcfg.PatienceUnit},
		{"session idle TTL", DefaultSessionIdleTTL, 10 * time.Minute, storcfg.PatienceUnit},
		{"session max TTL", DefaultSessionMaxTTL, time.Hour, storcfg.PatienceUnit},
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
