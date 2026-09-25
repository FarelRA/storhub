package test

import (
	"errors"
	"testing"
)

// TestPreconditionMismatchCarriesTokens pins the CAS mismatch constructor:
// the value must match ErrPrecondition under errors.As with both tokens.
func TestPreconditionMismatchCarriesTokens(t *testing.T) {
	err := PreconditionMismatch(7, 9)
	var pre ErrPrecondition
	if !errors.As(err, &pre) {
		t.Fatalf("mismatch value: want ErrPrecondition, got %T", err)
	}
	if pre.Expected != 7 || pre.Actual != 9 {
		t.Fatalf("mismatch tokens: want {7 9}, got {%d %d}", pre.Expected, pre.Actual)
	}
}
