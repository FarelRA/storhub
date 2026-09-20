package test

// Session TTL helpers shared by the oracle and the CLI/REST fakes.
//
// Boundary: production owns session semantics (internal/storage); the
// fakes must not each reimplement TTL clamping and expiry checks.
// ClampTTL is the single implementation so the fakes and the oracle
// agree with production.

import "time"

// DefaultTTL mirrors the product default when callers pass <=0: the
// description lives until reaped, never forever.
const DefaultTTL = 10 * time.Minute

// MaxTTL mirrors the product cap: no handle outlives it.
const MaxTTL = time.Hour

// ClampTTL folds a requested TTL into [default, limit]: <=0 takes the
// default, anything above the limit clamps to it. One implementation so
// the CLI fake, the REST fake, and the oracle agree with production.
func ClampTTL(requested, def, limit time.Duration) time.Duration {
	if def <= 0 {
		def = DefaultTTL
	}
	if limit <= 0 {
		limit = MaxTTL
	}
	if requested <= 0 {
		return def
	}
	if requested > limit {
		return limit
	}
	return requested
}

// Expired reports whether a deadline computed as openedAt+ttl has lapsed
// at now. Lazy-expiry checks in every fake go through here so wall-clock
// comparisons stay in one documented spot.
func Expired(deadline, now time.Time) bool {
	return !now.Before(deadline)
}
