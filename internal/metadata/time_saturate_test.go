package metadata

import (
	"math"
	"testing"
	"time"
)

// TestTimeToUnixSaturates pins the overflow guard: stamps outside the
// UnixNano window saturate instead of wrapping, zero stays zero, and normal
// times pass through exactly.
func TestTimeToUnixSaturates(t *testing.T) {
	t.Parallel()
	if got := timeToUnix(time.Time{}); got != 0 {
		t.Fatalf("zero time: got %d, want 0", got)
	}
	normal := time.Unix(1700000000, 123)
	if got := timeToUnix(normal); got != normal.UnixNano() {
		t.Fatalf("normal time: got %d, want %d", got, normal.UnixNano())
	}
	ancient := time.Date(1500, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := timeToUnix(ancient); got != math.MinInt64 {
		t.Fatalf("pre-1678 time: got %d, want MinInt64", got)
	}
	future := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := timeToUnix(future); got != math.MaxInt64 {
		t.Fatalf("far-future time: got %d, want MaxInt64", got)
	}
}
