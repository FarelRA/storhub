package rest

import (
	"net/http"
	"strings"
	"testing"
)

func TestRESTPreconditionsAndDeleteErrors(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/file.txt", strings.NewReader("payload"), nil, http.StatusCreated)

	resp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/file.txt", strings.NewReader("again"), map[string]string{"If-None-Match": "*"}, http.StatusPreconditionFailed)
	assertErrorCode(t, resp, "precondition_failed")

	nodeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/file.txt", nil, nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, nodeResp, &node)
	resp = mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/file.txt&op=append", strings.NewReader("!"), map[string]string{"If-Match": "\"wrong\""}, http.StatusPreconditionFailed)
	assertErrorCode(t, resp, "precondition_failed")

	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/file.txt", nil, map[string]string{"Range": "bytes=999-1000"}, http.StatusRequestedRangeNotSatisfiable)
	assertErrorCode(t, resp, "range_not_satisfiable")

	dirResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs", nil, nil, http.StatusOK)
	var dirNode nodeResponse
	decodeJSONBody(t, dirResp, &dirNode)
	resp = mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs", nil, map[string]string{"If-Match": dirNode.ETag}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")

	resp = mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs/file.txt", nil, map[string]string{"If-Match": node.ETag}, http.StatusNoContent)
	if body := strings.TrimSpace(string(readBody(t, resp))); body != "" {
		t.Fatalf("expected empty body on delete, got %q", body)
	}
	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/file.txt", nil, nil, http.StatusNotFound)
	assertErrorCode(t, resp, "not_found")
	resp = mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs", nil, nil, http.StatusNoContent)
	if body := strings.TrimSpace(string(readBody(t, resp))); body != "" {
		t.Fatalf("expected empty body on directory delete, got %q", body)
	}
}

func TestRESTProjectDeleteAndConditionalNodeRead(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	rootResp := mustRequest(t, handler, http.MethodGet, "/api/v1", nil, nil, http.StatusOK)
	var root map[string]any
	decodeJSONBody(t, rootResp, &root)
	if root["version"] != "v1" {
		t.Fatalf("unexpected root payload: %+v", root)
	}

	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hi"), nil, http.StatusCreated)
	nodeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=hello.txt", nil, nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, nodeResp, &node)
	notModified := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=hello.txt", nil, map[string]string{"If-None-Match": node.ETag}, http.StatusNotModified)
	if body := strings.TrimSpace(string(readBody(t, notModified))); body != "" {
		t.Fatalf("expected empty 304 body, got %q", body)
	}

	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo", nil, nil, http.StatusOK)
	deletedResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo", nil, nil, http.StatusNotFound)
	assertErrorCode(t, deletedResp, "not_found")
}
