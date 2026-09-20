package rest

import (
	"net/http"
	"testing"
)

// Operability endpoints (gc, re-enable, status) must answer through HTTP
// like every other operator surface: no Go-only orphans.
func TestOperabilityEndpoints(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)

	gcResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/gc",
		gcRequest{DryRun: true}, http.StatusOK)
	var gc gcResponse
	decodeJSONBody(t, gcResp, &gc)
	if gc.Status != "collected" || !gc.DryRun {
		t.Fatalf("unexpected gc response: %+v", gc)
	}

	reResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/re-enable", nil, nil, http.StatusOK)
	var re ackResponse
	decodeJSONBody(t, reResp, &re)
	if re.Status != "re-enabled" {
		t.Fatalf("unexpected re-enable response: %+v", re)
	}

	stResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/ops/status", nil, nil, http.StatusOK)
	var st statusResponse
	decodeJSONBody(t, stResp, &st)
	if st.Project != "demo" || st.Degraded {
		t.Fatalf("unexpected status response: %+v", st)
	}
}
