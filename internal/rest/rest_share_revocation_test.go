package rest

// rest_share_revocation_test.go: share revocation scope and expiry.

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// ---- Revocation state is per-handler and self-expiring ----

func TestRevocationIsPerHandler(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	newHandler := func() http.Handler {
		handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		return handler
	}
	first, second := newHandler(), newHandler()
	mustRequest(t, first, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hi"), nil, http.StatusCreated)
	share := mustDecodeShare(t, mustJSONRequest(t, first, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "hello.txt"}, http.StatusCreated))

	mustRequest(t, first, http.MethodDelete, "/api/v1/projects/demo/shares/"+share.ID, nil, nil, http.StatusNoContent)
	mustRequest(t, first, http.MethodGet, "/api/v1/shares/"+share.Token, nil, nil, http.StatusNotFound)
	// A sibling handler never saw the revocation: per-handler by design, no
	// package-level map shared across hubs or tests.
	mustRequest(t, second, http.MethodGet, "/api/v1/shares/"+share.Token, nil, nil, http.StatusOK)
}

func TestRevocationEntriesSelfExpire(t *testing.T) {
	t.Parallel()
	h := &restHandler{client: newFakeRESTClient(), opts: DefaultOptions(), shares: &shareRegistry{items: map[string]*shareRecord{}, revoked: map[string]time.Time{}}}
	h.revokeShare("gone", time.Now().Add(-time.Minute))
	if h.isRevoked("gone") {
		t.Fatal("revocation of an already-expired share must not report revoked")
	}
	if len(h.shares.revoked) != 0 {
		t.Fatalf("expired revocation entry must be dropped, %d remain", len(h.shares.revoked))
	}
	h.revokeShare("live", time.Now().Add(time.Hour))
	if !h.isRevoked("live") {
		t.Fatal("live revocation must be honored")
	}
}
