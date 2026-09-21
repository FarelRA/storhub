package rest

// rest_route_hardening_test.go: UI serving guards, base path fallback,
// query parsing, and mutation response shapes over REST.

import (
	"net/http"
	"strings"
	"testing"
)

// ---- UI guard precision + no directory listings ----

func TestSafeDistNameGuard(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, want string }{
		{"/_nuxt/index.html", "_nuxt/index.html"},
		{"/_nuxt/chunk..2.js", "_nuxt/chunk..2.js"}, // legitimate hashed name
		{"/favicon.svg", "favicon.svg"},
		{"/_nuxt/", ""},
		{"/_nuxt", "_nuxt"},
		{"/_nuxt/../secret", ""},
		{"/_nuxt/./x", ""},
		{"/_nuxt//x", ""},
		{"/_nuxt/..", ""},
		{"/a\\b", ""},
		{"/", ""},
	} {
		if got := safeDistName(tc.in); got != tc.want {
			t.Fatalf("safeDistName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestUIDirectoryListingDisabled(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	for _, target := range []string{"/_nuxt/", "/_nuxt", "/_nuxt/builds", "/_nuxt/builds/"} {
		resp := mustRequest(t, handler, http.MethodGet, target, nil, nil, http.StatusNotFound)
		_ = readBody(t, resp)
	}
}

// ---- Misc hardening ----

func TestBasePathSlashFallsBackToDefault(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, BasePath: "/"})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
}

func TestRecursiveQueryBoolParsing(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	for _, truthy := range []string{"true", "1", "yes", "TRUE"} {
		resp := mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs&recursive="+truthy, nil, nil, http.StatusNotImplemented)
		assertErrorCode(t, resp, "not_implemented")
	}
	resp := mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs&recursive=banana", nil, nil, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs&recursive=false", nil, nil, http.StatusNoContent)
}

func TestPruneAcceptsBodylessPOST(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/prune", nil, nil, http.StatusOK)
	var pr pruneResponse
	decodeJSONBody(t, resp, &pr)
	if pr.Scope != "assets" || pr.Status != "pruned" {
		t.Fatalf("unexpected prune response: %+v", pr)
	}
}

func TestPatchSuccessReturnsNode(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=f.txt", strings.NewReader("hello"), nil, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=f.txt&op=append", strings.NewReader("!"), nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, resp, &node)
	if node.Entry == nil || node.Entry.Path != "f.txt" || node.Entry.Size != 6 {
		t.Fatalf("PATCH success must return the fresh node: %+v", node)
	}
}
