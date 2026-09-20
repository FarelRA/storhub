package rest

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestPreconditionFunnelCoversMutatingOps pins the shared precondition
// funnel: EVERY mutating endpoint must honor If-Match, answering 412 on a
// stale token instead of applying. Each case seeds a fresh project so
// cases stay independent. A stale token can never match a live ETag or
// revision, so 412 is the only honest answer everywhere below.
func TestPreconditionFunnelCoversMutatingOps(t *testing.T) {
	t.Parallel()
	stale := map[string]string{"If-Match": `"stale-token"`}
	cases := []struct {
		name   string
		method string
		target string
		body   any
		seed   func(t *testing.T, handler http.Handler)
	}{
		{name: "delete-node", method: http.MethodDelete, target: "/api/v1/projects/demo/nodes?path=docs/f.txt"},
		{name: "put-content", method: http.MethodPut, target: "/api/v1/projects/demo/content?path=docs/f.txt", body: "hello"},
		{name: "patch-append", method: http.MethodPatch, target: "/api/v1/projects/demo/content?path=docs/f.txt&op=append", body: "!"},
		{name: "patch-write", method: http.MethodPatch, target: "/api/v1/projects/demo/content?path=docs/f.txt&op=write&offset=0", body: "!"},
		{name: "patch-patch", method: http.MethodPatch, target: "/api/v1/projects/demo/content?path=docs/f.txt&op=patch&offset=0&delete_size=0", body: "!"},
		{name: "patch-truncate", method: http.MethodPatch, target: "/api/v1/projects/demo/content?path=docs/f.txt&op=truncate&size=1"},
		{name: "create-file", method: http.MethodPost, target: "/api/v1/projects/demo/ops/create-file", body: pathRequest{Path: "docs/fresh.txt"}},
		{name: "mkdir", method: http.MethodPost, target: "/api/v1/projects/demo/ops/mkdir", body: pathRequest{Path: "docs/sub"}},
		{
			name: "rmdir", method: http.MethodPost, target: "/api/v1/projects/demo/ops/rmdir", body: pathRequest{Path: "docs/empty"},
			seed: func(t *testing.T, handler http.Handler) {
				mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs/empty"}, http.StatusCreated)
			},
		},
		{name: "unlink", method: http.MethodPost, target: "/api/v1/projects/demo/ops/unlink", body: pathRequest{Path: "docs/f.txt"}},
		{name: "rename", method: http.MethodPost, target: "/api/v1/projects/demo/ops/rename", body: renameRequest{OldPath: "docs/f.txt", NewPath: "docs/g.txt"}},
		{name: "copy", method: http.MethodPost, target: "/api/v1/projects/demo/ops/copy", body: copyRequest{SrcPath: "docs/f.txt", DstPath: "docs/h.txt"}},
		{name: "link", method: http.MethodPost, target: "/api/v1/projects/demo/ops/link", body: linkRequest{ExistingPath: "docs/f.txt", NewPath: "docs/hard.txt"}},
		{name: "symlink", method: http.MethodPost, target: "/api/v1/projects/demo/ops/symlink", body: symlinkRequest{Target: "docs/f.txt", LinkPath: "docs/soft.txt"}},
		{name: "chmod", method: http.MethodPost, target: "/api/v1/projects/demo/ops/chmod", body: chmodRequest{Path: "docs/f.txt", Mode: 0o640}},
		{name: "chown", method: http.MethodPost, target: "/api/v1/projects/demo/ops/chown", body: chownRequest{Path: "docs/f.txt", UID: 1000, GID: 1000}},
		{
			name: "utimes", method: http.MethodPost, target: "/api/v1/projects/demo/ops/utimes",
			body: utimesRequest{Path: "docs/f.txt", Atime: time.Unix(0, 1700000000123456789).UTC(), Mtime: time.Unix(0, 1700000000123456789).UTC()},
		},
		{name: "purge", method: http.MethodPost, target: "/api/v1/projects/demo/ops/purge"},
		{name: "prune", method: http.MethodPost, target: "/api/v1/projects/demo/ops/purge", body: purgeRequest{Scope: "all"}},
		{
			name: "xattr-put", method: http.MethodPut, target: "/api/v1/projects/demo/xattrs/value?path=docs/f.txt&name=key",
			body: "value",
		},
		{
			name: "xattr-delete", method: http.MethodDelete, target: "/api/v1/projects/demo/xattrs/value?path=docs/f.txt&name=key",
			seed: func(t *testing.T, handler http.Handler) {
				mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/xattrs/value?path=docs/f.txt&name=key", strings.NewReader("value"), nil, http.StatusNoContent)
			},
		},
		{name: "project-delete", method: http.MethodDelete, target: "/api/v1/projects/demo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeRESTClient()
			handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
			if err != nil {
				t.Fatalf("new handler: %v", err)
			}
			mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
			mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("hello"), nil, http.StatusCreated)
			if tc.seed != nil {
				tc.seed(t, handler)
			}
			var resp *http.Response
			if tc.body != nil {
				if text, ok := tc.body.(string); ok {
					resp = mustRequest(t, handler, tc.method, tc.target, strings.NewReader(text), stale, http.StatusPreconditionFailed)
				} else {
					resp = mustJSONRequestWithHeaders(t, handler, tc.method, tc.target, tc.body, stale, http.StatusPreconditionFailed)
				}
			} else {
				resp = mustRequest(t, handler, tc.method, tc.target, nil, stale, http.StatusPreconditionFailed)
			}
			assertErrorCode(t, resp, "precondition_failed")
		})
	}
}

// TestPreconditionFunnelRollback pins the project-op flavor: rollback
// guards on the current metadata revision, so a stale revision token
// answers 412 before any storage call. The SHA comes from the live
// revision history so validation cannot fail first.
func TestPreconditionFunnelRollback(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/revisions", nil, nil, http.StatusOK)
	var history revisionsResponse
	decodeJSONBody(t, resp, &history)
	if len(history.Revisions) == 0 {
		t.Fatal("need at least one revision to address the rollback at")
	}
	sha := history.Revisions[0].CommitSHA
	stale := map[string]string{"If-Match": `"rev-older` + sha + `"`}
	for _, tc := range []struct {
		name   string
		target string
		body   any
	}{
		{name: "rollback", target: "/api/v1/projects/demo/ops/rollback", body: rollbackRequest{CommitSHA: sha}},
		{name: "revert-path", target: "/api/v1/projects/demo/ops/revert-path", body: revertPathRequest{Path: "docs", CommitSHA: sha}},
	} {
		resp := mustJSONRequestWithHeaders(t, handler, http.MethodPost, tc.target, tc.body, stale, http.StatusPreconditionFailed)
		assertErrorCode(t, resp, "precondition_failed")
	}
}

// mustJSONRequestWithHeaders is mustJSONRequest with caller headers
// (If-Match) instead of none.
func mustJSONRequestWithHeaders(t *testing.T, handler http.Handler, method, target string, payload any, headers map[string]string, wantStatus int) *http.Response {
	t.Helper()
	raw := mustJSONMarshal(t, payload)
	out := map[string]string{"Content-Type": "application/json"}
	for k, v := range headers {
		out[k] = v
	}
	return mustRequest(t, handler, method, target, strings.NewReader(string(raw)), out, wantStatus)
}
