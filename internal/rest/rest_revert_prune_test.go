package rest

import (
	"net/http"
	"strings"
	"testing"
)

func TestRESTRevertPathAndPrune(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/readme.txt", strings.NewReader("hello"), nil, http.StatusCreated)

	// Per-path revert records the path + revision on the client.
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/revert",
		revertPathRequest{Path: "docs/readme.txt", CommitSHA: "deadbeef"}, http.StatusOK)
	client.mu.Lock()
	rec := append([]string(nil), client.revertPaths...)
	client.mu.Unlock()
	if len(rec) != 1 || rec[0] != "docs/readme.txt@deadbeef" {
		t.Fatalf("revert-path not recorded: %v", rec)
	}

	// Prune returns a typed result echoing scope + dry_run.
	pruneResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/prune",
		pruneRequest{Scope: "objects", DryRun: true}, http.StatusOK)
	var pr pruneResponse
	decodeJSONBody(t, pruneResp, &pr)
	if pr.Scope != "objects" || !pr.DryRun || pr.Status != "pruned" {
		t.Fatalf("unexpected prune response: %+v", pr)
	}
}
