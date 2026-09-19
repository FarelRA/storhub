package rest

import (
	"net/http"
	"strings"
	"testing"
)

// TestIfNoneMatchStarOnUpdates pins the R1 funnel extension: every
// update-path endpoint must honor If-None-Match *, answering 412 when the
// guard target exists. The 24-case matrix is 6 update endpoints x 4 header
// flavors:
//
//	plain    no precondition headers: the mutation applies (2xx).
//	star     If-None-Match *: 412, even though no If-Match was sent.
//	precede  If-None-Match * plus a matching revision If-Match: still 412
//	         (RFC 9110 precedence; the create-only guard is never skipped
//	         by a passing CAS token).
//	missing  If-None-Match * against a missing path: NOT 412. The guard
//	         only fires when the target exists; the handler answers its
//	         own create/404 outcome (PUT creates, the rest 404).
func TestIfNoneMatchStarOnUpdates(t *testing.T) {
	t.Parallel()
	const rev0 = `"rev-0"` // fake RevisionContext default, quoted on the wire.
	cases := []struct {
		name       string
		method     string
		target     string
		body       any
		wantPlain  int
		wantMiss   int
		missTarget string
		missBody   any
	}{
		{
			name: "put-replace", method: http.MethodPut,
			target: "/api/v1/projects/demo/content?path=docs/f.txt", body: "hello",
			wantPlain:  http.StatusOK,
			missTarget: "/api/v1/projects/demo/content?path=docs/fresh.txt", missBody: "hello",
			wantMiss: http.StatusCreated,
		},
		{
			name: "patch-append", method: http.MethodPatch,
			target: "/api/v1/projects/demo/content?path=docs/f.txt&op=append", body: "!",
			wantPlain:  http.StatusOK,
			missTarget: "/api/v1/projects/demo/content?path=docs/fresh.txt&op=append", missBody: "!",
			wantMiss: http.StatusNotFound,
		},
		{
			name: "patch-write", method: http.MethodPatch,
			target: "/api/v1/projects/demo/content?path=docs/f.txt&op=write&offset=0", body: "!",
			wantPlain:  http.StatusOK,
			missTarget: "/api/v1/projects/demo/content?path=docs/fresh.txt&op=write&offset=0", missBody: "!",
			wantMiss: http.StatusNotFound,
		},
		{
			name: "patch-truncate", method: http.MethodPatch,
			target: "/api/v1/projects/demo/content?path=docs/f.txt&op=truncate&size=1", body: nil,
			wantPlain:  http.StatusOK,
			missTarget: "/api/v1/projects/demo/content?path=docs/fresh.txt&op=truncate&size=1", missBody: nil,
			wantMiss: http.StatusNotFound,
		},
		{
			name: "delete-node", method: http.MethodDelete,
			target: "/api/v1/projects/demo/nodes?path=docs/f.txt", body: nil,
			wantPlain:  http.StatusNoContent,
			missTarget: "/api/v1/projects/demo/nodes?path=docs/fresh.txt", missBody: nil,
			wantMiss: http.StatusNotFound,
		},
		{
			name: "unlink", method: http.MethodPost,
			target: "/api/v1/projects/demo/ops/unlink", body: pathRequest{Path: "docs/f.txt"},
			wantPlain:  http.StatusNoContent,
			missTarget: "/api/v1/projects/demo/ops/unlink", missBody: pathRequest{Path: "docs/fresh.txt"},
			wantMiss: http.StatusNotFound,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/plain", func(t *testing.T) {
			t.Parallel()
			_, handler := seedIfNoneMatch(t)
			doUpdate(t, handler, tc.method, tc.target, tc.body, nil, tc.wantPlain)
		})
		t.Run(tc.name+"/star", func(t *testing.T) {
			t.Parallel()
			_, handler := seedIfNoneMatch(t)
			resp := doUpdate(t, handler, tc.method, tc.target, tc.body,
				map[string]string{"If-None-Match": "*"}, http.StatusPreconditionFailed)
			assertErrorCode(t, resp, "precondition_failed")
		})
		t.Run(tc.name+"/precedence", func(t *testing.T) {
			t.Parallel()
			_, handler := seedIfNoneMatch(t)
			resp := doUpdate(t, handler, tc.method, tc.target, tc.body,
				map[string]string{"If-None-Match": "*", "If-Match": rev0},
				http.StatusPreconditionFailed)
			assertErrorCode(t, resp, "precondition_failed")
		})
		t.Run(tc.name+"/missing", func(t *testing.T) {
			t.Parallel()
			_, handler := seedIfNoneMatch(t)
			resp := doUpdate(t, handler, tc.method, tc.missTarget, tc.missBody,
				map[string]string{"If-None-Match": "*"}, tc.wantMiss)
			if tc.wantMiss == http.StatusPreconditionFailed {
				assertErrorCode(t, resp, "precondition_failed")
			}
		})
	}
}

// seedIfNoneMatch builds a fresh handler with docs/f.txt ("hello") so every
// matrix cell starts from identical state.
func seedIfNoneMatch(t *testing.T) (*fakeRESTClient, http.Handler) {
	t.Helper()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("hello"), nil, http.StatusCreated)
	return client, handler
}

// doUpdate issues one matrix request with either a text or JSON body (nil
// means no body) and returns the response for code assertions.
func doUpdate(t *testing.T, handler http.Handler, method, target string, body any, headers map[string]string, wantStatus int) *http.Response {
	t.Helper()
	if body == nil {
		return mustRequest(t, handler, method, target, nil, headers, wantStatus)
	}
	if text, ok := body.(string); ok {
		return mustRequest(t, handler, method, target, strings.NewReader(text), headers, wantStatus)
	}
	return mustJSONRequestWithHeaders(t, handler, method, target, body, headers, wantStatus)
}
