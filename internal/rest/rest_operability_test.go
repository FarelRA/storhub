package rest

import (
	"net/http"
	"testing"
)

// Operability endpoints (prune incl. chunks scope, enable, status) must
// answer through HTTP like every other operator surface: no Go-only
// orphans.
func TestOperabilityEndpoints(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)

	chunkResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/prune",
		pruneRequest{Scope: "chunks", DryRun: true}, http.StatusOK)
	var chunk pruneResponse
	decodeJSONBody(t, chunkResp, &chunk)
	if chunk.Status != "pruned" || chunk.Scope != "chunks" || !chunk.DryRun {
		t.Fatalf("unexpected chunks prune response: %+v", chunk)
	}

	reResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/enable", nil, nil, http.StatusOK)
	var re ackResponse
	decodeJSONBody(t, reResp, &re)
	if re.Status != "enabled" {
		t.Fatalf("unexpected enable response: %+v", re)
	}

	stResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/ops/status", nil, nil, http.StatusOK)
	var st statusResponse
	decodeJSONBody(t, stResp, &st)
	if st.Project != "demo" || st.Degraded {
		t.Fatalf("unexpected status response: %+v", st)
	}
}
