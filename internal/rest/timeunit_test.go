package rest

import (
	"testing"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

// Token and share TTLs derive from the units; absolute values pin
// behavior.
func TestAuthTTLsSymmetric(t *testing.T) {
	t.Parallel()
	if defaultRESTTokenTTL != 12*720*storcfg.PatienceUnit {
		t.Fatalf("defaultRESTTokenTTL must be 12h, got %v", defaultRESTTokenTTL)
	}
	if defaultRESTShareTTL != 7*24*720*storcfg.PatienceUnit {
		t.Fatalf("defaultRESTShareTTL must be 7d, got %v", defaultRESTShareTTL)
	}
}
