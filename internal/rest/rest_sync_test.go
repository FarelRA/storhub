package rest

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Phase 3 sync opt-in: mutating endpoints accept ?sync=1 and drain the
// project's journal before responding. Async stays the default: no drain
// runs unless sync is requested.

// TestRESTSyncOptInDrainsOnPut pins the core contract: PUT with ?sync=1
// records exactly one drain call for the project.
func TestRESTSyncOptInDrainsOnPut(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt&sync=1", strings.NewReader("hi"), nil, http.StatusCreated)
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.drainCalls) != 1 || client.drainCalls[0] != "demo" {
		t.Fatalf("expected one drain call for demo, got %v", client.drainCalls)
	}
}

// TestRESTAsyncDefaultSkipsDrain pins the no-behavior-change half: without
// ?sync the mutation succeeds and no drain runs on any default path.
func TestRESTAsyncDefaultSkipsDrain(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=a.txt", strings.NewReader("x"), nil, http.StatusCreated)
	mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=a.txt&op=append", strings.NewReader("y"), nil, http.StatusOK)
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chmod", chmodRequest{Path: "a.txt", Mode: 0o644}, http.StatusOK)
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.drainCalls) != 0 {
		t.Fatalf("default paths must not drain, got %v", client.drainCalls)
	}
}

// TestRESTSyncDrainFailureIs500 pins the failure contract: the mutation is
// already published and journaled, so a failed drain answers 500 naming the
// project (retry-or-verify, never silent loss).
func TestRESTSyncDrainFailureIs500(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	client.setDrainErr(errors.New("drain demo: commit failed"))
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	resp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt&sync=1", strings.NewReader("hi"), nil, http.StatusInternalServerError)
	body := strings.TrimSpace(string(readBody(t, resp)))
	if !strings.Contains(body, "demo") {
		t.Fatalf("drain failure must name the project, got %q", body)
	}
}

// syncCase is one mutating endpoint exercised with ?sync=1: setup builds
// any preconditions without sync, then the cased request must succeed and
// record exactly one drain call.
type syncCase struct {
	name       string
	setup      func(t *testing.T, handler http.Handler)
	method     string
	target     string
	body       string
	json       any
	wantStatus int
}

// TestRESTSyncCoversEveryMutation walks every mutating route in the router
// table and asserts ?sync=1 drains exactly once. Share routes are
// deliberately absent: shares are ephemeral registry state, not journaled
// metadata, so draining them would be meaningless.
func TestRESTSyncCoversEveryMutation(t *testing.T) {
	t.Parallel()
	cases := []syncCase{
		{
			name: "put-replace", method: http.MethodPut,
			target: "/api/v1/projects/demo/content?path=a.txt", body: "x",
			wantStatus: http.StatusCreated,
		},
		{
			name: "patch-append", method: http.MethodPatch,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/content?path=a.txt&op=append", body: "y",
			wantStatus: http.StatusOK,
		},
		{
			name: "patch-write", method: http.MethodPatch,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/content?path=a.txt&op=write&offset=0", body: "y",
			wantStatus: http.StatusOK,
		},
		{
			name: "patch-patch", method: http.MethodPatch,
			setup:  putFile("a.txt", "xy"),
			target: "/api/v1/projects/demo/content?path=a.txt&op=patch&offset=0&delete_size=1", body: "z",
			wantStatus: http.StatusOK,
		},
		{
			name: "patch-truncate", method: http.MethodPatch,
			setup:      putFile("a.txt", "xy"),
			target:     "/api/v1/projects/demo/content?path=a.txt&op=truncate&size=1",
			wantStatus: http.StatusOK,
		},
		{
			name: "node-delete-file", method: http.MethodDelete,
			setup:      putFile("gone.txt", "x"),
			target:     "/api/v1/projects/demo/nodes?path=gone.txt",
			wantStatus: http.StatusNoContent,
		},
		{
			name: "node-delete-dir", method: http.MethodDelete,
			setup: func(t *testing.T, handler http.Handler) {
				t.Helper()
				mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "emptyd"}, http.StatusCreated)
			},
			target:     "/api/v1/projects/demo/nodes?path=emptyd",
			wantStatus: http.StatusNoContent,
		},
		{
			name: "xattr-put", method: http.MethodPut,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/xattrs/value?path=a.txt&name=k", body: "v",
			wantStatus: http.StatusNoContent,
		},
		{
			name: "xattr-delete", method: http.MethodDelete,
			setup: func(t *testing.T, handler http.Handler) {
				t.Helper()
				putFile("a.txt", "x")(t, handler)
				mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/xattrs/value?path=a.txt&name=k", strings.NewReader("v"), nil, http.StatusNoContent)
			},
			target:     "/api/v1/projects/demo/xattrs/value?path=a.txt&name=k",
			wantStatus: http.StatusNoContent,
		},
		{
			name: "create-file", method: http.MethodPost,
			target: "/api/v1/projects/demo/ops/create", json: pathRequest{Path: "c.txt"},
			wantStatus: http.StatusCreated,
		},
		{
			name: "mkdir", method: http.MethodPost,
			target: "/api/v1/projects/demo/ops/mkdir", json: pathRequest{Path: "d"},
			wantStatus: http.StatusCreated,
		},
		{
			name: "rmdir", method: http.MethodPost,
			setup: func(t *testing.T, handler http.Handler) {
				t.Helper()
				mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "d"}, http.StatusCreated)
			},
			target: "/api/v1/projects/demo/ops/rmdir", json: pathRequest{Path: "d"},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "unlink", method: http.MethodPost,
			setup:  putFile("u.txt", "x"),
			target: "/api/v1/projects/demo/ops/unlink", json: pathRequest{Path: "u.txt"},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "rename", method: http.MethodPost,
			setup:  putFile("old.txt", "x"),
			target: "/api/v1/projects/demo/ops/rename", json: renameRequest{OldPath: "old.txt", NewPath: "new.txt"},
			wantStatus: http.StatusOK,
		},
		{
			name: "copy", method: http.MethodPost,
			setup:  putFile("src.txt", "x"),
			target: "/api/v1/projects/demo/ops/copy", json: copyRequest{SrcPath: "src.txt", DstPath: "dst.txt"},
			wantStatus: http.StatusCreated,
		},
		{
			name: "link", method: http.MethodPost,
			setup:  putFile("src.txt", "x"),
			target: "/api/v1/projects/demo/ops/link", json: linkRequest{ExistingPath: "src.txt", NewPath: "hard.txt"},
			wantStatus: http.StatusCreated,
		},
		{
			name: "symlink", method: http.MethodPost,
			target: "/api/v1/projects/demo/ops/symlink", json: symlinkRequest{Target: "a.txt", LinkPath: "ln.txt"},
			wantStatus: http.StatusCreated,
		},
		{
			name: "chmod", method: http.MethodPost,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/ops/chmod", json: chmodRequest{Path: "a.txt", Mode: 0o644},
			wantStatus: http.StatusOK,
		},
		{
			name: "chown", method: http.MethodPost,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/ops/chown", json: chownRequest{Path: "a.txt", UID: 1, GID: 2},
			wantStatus: http.StatusOK,
		},
		{
			name: "utimes", method: http.MethodPost,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/ops/utimes", json: utimesRequest{Path: "a.txt", Atime: time.Unix(0, 10000000001).UTC(), Mtime: time.Unix(0, 20000000002).UTC()},
			wantStatus: http.StatusOK,
		},
		{
			name: "revert-path", method: http.MethodPost,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/ops/revert", json: revertPathRequest{Path: "a.txt", CommitSHA: "deadbeef"},
			wantStatus: http.StatusOK,
		},
		{
			name: "purge", method: http.MethodPost,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/ops/prune", json: map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "purge-scoped", method: http.MethodPost,
			setup:  putFile("a.txt", "x"),
			target: "/api/v1/projects/demo/ops/prune", json: map[string]any{"scope": "objects", "dry_run": true},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete-project", method: http.MethodDelete,
			setup: func(t *testing.T, handler http.Handler) {
				t.Helper()
				mustRequest(t, handler, http.MethodPut, "/api/v1/projects/gone/content?path=a.txt", strings.NewReader("x"), nil, http.StatusCreated)
			},
			target:     "/api/v1/projects/gone",
			wantStatus: http.StatusOK,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeRESTClient()
			handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
			if err != nil {
				t.Fatalf("new handler: %v", err)
			}
			if tc.setup != nil {
				tc.setup(t, handler)
			}
			target := tc.target
			if strings.Contains(target, "?") {
				target += "&sync=1"
			} else {
				target += "?sync=1"
			}
			if tc.json != nil {
				mustJSONRequest(t, handler, tc.method, target, tc.json, tc.wantStatus)
			} else {
				var body *strings.Reader
				if tc.body != "" {
					body = strings.NewReader(tc.body)
				} else {
					body = strings.NewReader("")
				}
				mustRequest(t, handler, tc.method, target, body, nil, tc.wantStatus)
			}
			client.mu.Lock()
			defer client.mu.Unlock()
			if len(client.drainCalls) != 1 {
				t.Fatalf("?sync=1 on %s must drain exactly once, got %v", tc.name, client.drainCalls)
			}
		})
	}
}

// TestRESTSyncRollbackDrains covers rollback separately: it needs a genuine
// revision SHA from the fake's history, which the table cannot know upfront.
func TestRESTSyncRollbackDrains(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=a.txt", strings.NewReader("x"), nil, http.StatusCreated)
	client.mu.Lock()
	rev := client.projects["demo"].revisions[0].CommitSHA
	client.mu.Unlock()
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/rollback?sync=1",
		rollbackRequest{CommitSHA: rev}, http.StatusOK)
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.drainCalls) != 1 || client.drainCalls[0] != "demo" {
		t.Fatalf("rollback ?sync=1 must drain demo once, got %v", client.drainCalls)
	}
}

// putFile returns a setup step creating path with content via PUT.
func putFile(path, content string) func(t *testing.T, handler http.Handler) {
	return func(t *testing.T, handler http.Handler) {
		t.Helper()
		mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path="+path, strings.NewReader(content), nil, http.StatusCreated)
	}
}
