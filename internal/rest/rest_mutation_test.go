package rest

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

// TestRESTAppendRejectsOversizedBodies pins the atomic-mutation contract:
// append/write bodies are buffered whole and applied in a single call, so
// anything beyond the configured cap is refused with 413 instead of being
// written chunk-by-chunk with torn intermediate states.
func TestRESTAppendRejectsOversizedBodies(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, MaxPatchBodySize: 8})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/file.txt", strings.NewReader("seed"), nil, http.StatusCreated)

	resp := mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/file.txt&op=append", strings.NewReader("0123456789"), nil, http.StatusRequestEntityTooLarge)
	assertErrorCode(t, resp, "payload_too_large")

	nodeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/file.txt", nil, nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, nodeResp, &node)
	if node.Entry.Size != 4 {
		t.Fatalf("rejected append must not modify the file, size=%d", node.Entry.Size)
	}

	// At-cap bodies still succeed atomically.
	mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/file.txt&op=append", strings.NewReader("12345678"), nil, http.StatusOK)
	content := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/file.txt", nil, nil, http.StatusOK)
	if got := string(readBody(t, content)); got != "seed12345678" {
		t.Fatalf("unexpected content after capped append: %q", got)
	}
}

// TestRESTRemovesOrphanOnFailedReplace ensures a failed body transfer after
// create does not strand an empty placeholder file.
func TestRESTRemovesOrphanOnFailedReplace(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	client.failNextReplace(errors.New("upload exploded"))

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/new.txt", strings.NewReader("payload"), nil, http.StatusInternalServerError)
	assertErrorCode(t, resp, "internal_error")

	orphan := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/new.txt", nil, nil, http.StatusNotFound)
	assertErrorCode(t, orphan, "not_found")

	// A failed overwrite of an existing regular file must keep that file.
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/keep.txt", strings.NewReader("original"), nil, http.StatusCreated)
	client.failNextReplace(errors.New("upload exploded"))
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/keep.txt", strings.NewReader("replacement"), nil, http.StatusInternalServerError)
	content := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/keep.txt", nil, nil, http.StatusOK)
	if got := string(readBody(t, content)); got != "original" {
		t.Fatalf("failed replace must preserve original content, got %q", got)
	}
	_ = context.Background()
}
