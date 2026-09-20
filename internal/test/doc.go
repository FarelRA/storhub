// Package test is the single home for test-support libraries.
//
// Boundary: anything imported only by _test files lives here, never as a
// standalone top-level internal/ package next to prod code. Production
// packages must not import internal/test; the dependency arrow points one
// way, tests -> internal/test.
//
// Contents, one generalized package:
//
//	contract  (surface.go)   - Surface/Handle/SessionSurface capability
//	  interfaces plus the Err* sentinels shared by every adapter.
//	table     (scenario.go)  - the shared POSIX + session scenario table.
//	runner    (runner.go)    - Filter/Run/RunWithBudget execution with
//	  per-scenario budgets.
//	oracle    (mem.go, mem_scratch.go) - in-memory Surface plus pathless
//	  scratch oracle; the expected behavior every adapter is held to.
//	session   (session.go)   - TTL clamp + expiry check shared by the
//	  oracle and the CLI/REST fakes.
//	budgets   (budgets_test.go) - benchmark ceilings enforced in CI.
//
// Naming convention for test files repo-wide: foo_test.go tests foo.go
// in the same package (scenario_test.go for scenario.go, never a
// catch-all conform_test.go); behavior-only gates keep a descriptive
// name (budgets_test.go). Test funcs read TestTypeBehavior
// (TestRunnerBudgetPassthrough, TestTableRunsAgainstOracle), never
// ticket tags (no TestAudit14/TestPhase5); regression pins name the
// behavior they pin.
package test
