package rest

import (
	"testing"

	"github.com/FarelRA/storhub/internal/storage"
)

// TestPruneScopeAcceptsChunks pins the documented scope set: chunks is a
// real scope alongside objects, assets, history, and all.
func TestPruneScopeAcceptsChunks(t *testing.T) {
	t.Parallel()
	scope, err := parsePruneScope("chunks")
	if err != nil {
		t.Fatalf("chunks scope rejected: %v", err)
	}
	if scope != storage.PruneChunks {
		t.Fatalf("chunks scope = %q, want %q", scope, storage.PruneChunks)
	}
}
