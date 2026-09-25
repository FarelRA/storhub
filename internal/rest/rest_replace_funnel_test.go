package rest

import (
	"net/http"
	"strings"
	"testing"
)

// TestReplaceDirectoryConflict pins the replace-only type rule: PUT
// /content at a directory answers 409 through the shared precondition
// funnel, never a storage write.
func TestReplaceDirectoryConflict(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs", strings.NewReader("hello"), nil, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")
}

// TestReplaceMissingWithStaleTokenFailsPrecondition pins the funnel rule
// for creates: If-Match on a missing resource answers 412 because there is
// no state to match.
func TestReplaceMissingWithStaleTokenFailsPrecondition(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	resp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/new.txt", strings.NewReader("hello"), map[string]string{"If-Match": `"staletoken"`}, http.StatusPreconditionFailed)
	assertErrorCode(t, resp, "precondition_failed")
}
