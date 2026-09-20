package test

import (
	"os"
	"strings"
	"testing"
)

// Central test environment dimensions.
//
// Every magic test env var lives here, so a new dimension is one helper
// plus one call site, not a fresh os.Getenv idiom per file. Production
// env (STORHUB_CACHE_DIR, STORHUB_API_BASE_URL, ...) stays in
// internal/config; only test gating lives here.
//
// Dimensions:
//   - STORHUB_CONFORMANCE (non-empty): run the shared conformance table.
//   - PC_ONLY (exact scenario name): focus one conformance scenario.
//   - STORHUB_BENCH=1: enforce benchmark budgets (CI job only).
//   - STORHUB_RUN_LIVE / STORHUB_RUN_LIVE_LARGE / STORHUB_RUN_LARGE /
//     STORHUB_RUN_FUSE (=1): live / large / FUSE smoke tests.
//   - GITHUB_TOKEN: secret for live tests, never logged.

// ConformanceEnabled reports whether the shared conformance table runs.
func ConformanceEnabled() bool {
	return os.Getenv("STORHUB_CONFORMANCE") != ""
}

// RequireConformance skips the caller unless the conformance table runs.
func RequireConformance(t *testing.T) {
	t.Helper()
	if !ConformanceEnabled() {
		t.Skip("conformance suite runs only with STORHUB_CONFORMANCE=1 (Phase 0 RED: known deviations open)")
	}
}

// ConformOnly focuses one scenario by exact name (e.g. debugging a single
// RED case without paying for 30 fresh mounts). Empty runs the table.
func ConformOnly() string {
	return os.Getenv("PC_ONLY")
}

// RequireBench skips the caller unless benchmark budget enforcement runs.
func RequireBench(t *testing.T) {
	t.Helper()
	if os.Getenv("STORHUB_BENCH") != "1" {
		t.Skip("budget enforcement needs STORHUB_BENCH=1 (CI benchmark-budgets job); table sanity is TestBudgetTableSane")
	}
}

// RequireFlag skips the caller unless env name equals "1".
func RequireFlag(t *testing.T, name string) {
	t.Helper()
	if os.Getenv(name) != "1" {
		t.Skipf("set %s=1 to run this smoke test", name)
	}
}

// RequireValue returns the trimmed env value or fails when blank.
func RequireValue(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required for this smoke test", name)
	}
	return value
}
