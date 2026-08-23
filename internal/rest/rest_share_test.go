package rest

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestShareTTLDefaultClamp ensures the MaxShareTTL clamp is armed even when
// the embedder never sets it: a zero option value must mean "default bound",
// never "unbounded".
func TestShareTTLDefaultClamp(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{
		AllowAnonymous:  true,
		ShareSigningKey: []byte("test-signing-key-at-least-32-bytes!!"),
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("x"), nil, http.StatusCreated)

	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		shareRequest{Path: "docs/f.txt", ExpiresInSeconds: 100 * 365 * 24 * 3600}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, resp, &share)

	expiresAt, err := time.Parse(time.RFC3339, share.ExpiresAt)
	if err != nil {
		t.Fatalf("parse expires_at %q: %v", share.ExpiresAt, err)
	}
	bound := Options{}.withDefaults().MaxShareTTL
	if bound <= 0 {
		t.Fatal("withDefaults must arm a positive MaxShareTTL")
	}
	if until := time.Until(expiresAt); until > bound+time.Minute {
		t.Fatalf("share lifetime %v exceeds default clamp %v", until, bound)
	}
}

func TestSweepExpiredSharesBoundsRegistry(t *testing.T) {
	client := newFakeRESTClient()
	handler := &restHandler{
		client: client,
		opts:   Options{AllowAnonymous: true}.withDefaults(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	now := time.Now()
	for i := 0; i < shareSweepThreshold+10; i++ {
		handler.shares.items[fmt.Sprintf("stale-%d", i)] = &shareRecord{
			ID: "stale", Project: "demo", ExpiresAt: now.Add(-time.Hour),
		}
	}
	handler.sweepExpiredShares()
	if got := len(handler.shares.items); got != 0 {
		t.Fatalf("expected all expired records swept, got %d", got)
	}
}
