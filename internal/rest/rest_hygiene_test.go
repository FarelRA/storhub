package rest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRequestBearerTokenPrecedence pins the one header-extraction helper:
// Authorization: Bearer wins inline whitespace, and callers keep their own
// header-vs-query precedence on top of it.
func TestRequestBearerTokenPrecedence(t *testing.T) {
	withHeader := httptest.NewRequest(http.MethodGet, "/x", nil)
	withHeader.Header.Set("Authorization", "Bearer   abc123  ")
	if got := requestBearerToken(withHeader); got != "abc123" {
		t.Fatalf("header token = %q, want abc123", got)
	}
	bare := httptest.NewRequest(http.MethodGet, "/x", nil)
	if got := requestBearerToken(bare); got != "" {
		t.Fatalf("missing header must yield empty token, got %q", got)
	}
	basic := httptest.NewRequest(http.MethodGet, "/x", nil)
	basic.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	if got := requestBearerToken(basic); got != "" {
		t.Fatalf("non-bearer scheme must yield empty token, got %q", got)
	}
}

// TestReadSizedBodyMessages pins the per-endpoint 413 wordings surviving the
// readMutationBody/readPatchBody merge.
func TestReadSizedBodyMessages(t *testing.T) {
	h := &restHandler{opts: DefaultOptions()}
	mutationMsg := "mutation body exceeds the configured limit of 8388608 bytes; use full-file PUT for large payloads"
	if _, err := h.readSizedBody(strings.NewReader(strings.Repeat("x", 9<<20)), mutationMsg); err == nil {
		t.Fatal("expected oversized mutation body to fail")
	} else if !strings.Contains(err.Error(), "use full-file PUT") {
		t.Fatalf("mutation 413 wording lost: %v", err)
	}
	if _, err := h.readSizedBody(strings.NewReader(strings.Repeat("x", 9<<20)), "patch payload exceeds the configured limit"); err == nil {
		t.Fatal("expected oversized patch body to fail")
	} else if !strings.Contains(err.Error(), "patch payload exceeds") {
		t.Fatalf("patch 413 wording lost: %v", err)
	}
	payload, err := h.readSizedBody(strings.NewReader("hello"), mutationMsg)
	if err != nil || string(payload) != "hello" {
		t.Fatalf("small body must pass through, got %q %v", payload, err)
	}
}

// TestServeUIDistErrorShapes pins the merged UI handler: the hashed-bundle
// route keeps its JSON 404, the public route keeps stock http.NotFound.
func TestServeUIDistErrorShapes(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	bundles := mustRequest(t, handler, http.MethodGet, "/_nuxt/%2e%2e/index.html", nil, nil, http.StatusNotFound)
	if body := string(readBody(t, bundles)); !strings.Contains(body, "not_found") {
		t.Fatalf("bundle traversal must stay JSON, got %q", body)
	}
}
