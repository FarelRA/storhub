package rest

// rest_session_scratch_test.go: unlinked scratch sessions, link, relink,
// and discard over REST.

import (
	"net/http"
	"strings"
	"testing"
)

// TestRESTSessionUnlinkedScratch covers the link and discard paths: sync
// without a link is a 409, link-then-sync publishes, and close without a
// link discards with no commit.

func TestRESTSessionUnlinkedScratch(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	scratch := openSessionHTTP(t, handler, "demo", "", "w")
	writeSessionHTTP(t, handler, scratch, 0, "scratch bytes")
	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/sync", map[string]any{}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/link", map[string]string{"path": "scratch.txt"}, http.StatusOK)
	if stat := statSessionHTTP(t, handler, scratch); stat.Path != "scratch.txt" || !stat.Dirty {
		t.Fatalf("link must name the handle and stage creation: %+v", stat)
	}
	mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/close", nil, nil, http.StatusOK)
	if got := contentHTTP(t, handler, "demo", "scratch.txt"); got != "scratch bytes" {
		t.Fatalf("linked scratch committed %q, want %q", got, "scratch bytes")
	}

	// Linking twice is a 409.
	linked := openSessionHTTP(t, handler, "demo", "scratch.txt", "r+")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+linked+"/link", map[string]string{"path": "other.txt"}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")

	// Discard path: close unlinked scratch commits nothing.
	discard := openSessionHTTP(t, handler, "demo", "", "w")
	writeSessionHTTP(t, handler, discard, 0, "doomed")
	mustRequest(t, handler, http.MethodDelete, "/api/v1/handles/"+discard, nil, nil, http.StatusNoContent)
	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=doomed.txt", nil, nil, http.StatusNotFound)
	assertErrorCode(t, resp, "not_found")
}

func TestRESTSessionRelinkRescuesTakenTarget(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	scratch := openSessionHTTP(t, handler, "demo", "", "w")
	writeSessionHTTP(t, handler, scratch, 0, "rescued bytes")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/link", map[string]string{"path": "taken.txt"}, http.StatusOK)
	// A concurrent writer takes the linked target through the plain
	// content endpoint.
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=taken.txt", strings.NewReader("rival"), nil, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/close", nil, nil, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")
	// Relink rescues the staged bytes at a free name; the rival keeps
	// its own content.
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/relink", map[string]string{"path": "rescued.txt"}, http.StatusOK)
	if stat := statSessionHTTP(t, handler, scratch); stat.Path != "rescued.txt" {
		t.Fatalf("relink must rename the handle target: %+v", stat)
	}
	mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/close", nil, nil, http.StatusOK)
	if got := contentHTTP(t, handler, "demo", "rescued.txt"); got != "rescued bytes" {
		t.Fatalf("relocated content = %q, want %q", got, "rescued bytes")
	}
	if got := contentHTTP(t, handler, "demo", "taken.txt"); got != "rival" {
		t.Fatalf("rival content disturbed: %q", got)
	}
	// Relink to a taken name fails loudly instead of stealing it.
	other := openSessionHTTP(t, handler, "demo", "", "w")
	writeSessionHTTP(t, handler, other, 0, "x")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+other+"/link", map[string]string{"path": "other.txt"}, http.StatusOK)
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+other+"/relink", map[string]string{"path": "taken.txt"}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")
}
