// Package sessiontest groups test-only session helpers in one place.
//
// Boundary: production owns session semantics (internal/storage); the
// CLI/REST conformance fakes must not each reimplement TTL clamping and
// expiry checks. Shared pure helpers live here, imported only by _test
// files, never by prod code.
package sessiontest

import "time"

// DefaultTTL mirrors the product default when callers pass <=0: the
// description lives until reaped, never forever.
const DefaultTTL = 10 * time.Minute

// MaxTTL mirrors the product cap: no handle outlives it.
const MaxTTL = time.Hour

// ClampTTL folds a requested TTL into [default, max]: <=0 takes the
// default, anything above max clamps to it. One implementation so the
// CLI fake, the REST fake, and the oracle agree with production.
func ClampTTL(requested, def, max time.Duration) time.Duration {
	if def <= 0 {
		def = DefaultTTL
	}
	if max <= 0 {
		max = MaxTTL
	}
	if requested <= 0 {
		return def
	}
	if requested > max {
		return max
	}
	return requested
}

// Expired reports whether a deadline computed as openedAt+ttl has lapsed
// at now. Lazy-expiry checks in every fake go through here so wall-clock
// comparisons stay in one documented spot.
func Expired(deadline, now time.Time) bool {
	return !now.Before(deadline)
}
