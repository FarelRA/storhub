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

// TestPressureStreakDepthRequireAdmin pins the pressure-lane gating: the
// failure streak and pending depth are operator counters like the sibling
// snapshot accessors, so non-admin principals get 403. The pre-fix
// forwarders answered nil error to anyone, so the denial assertions fail
// on the old code.
func TestPressureStreakDepthRequireAdmin(t *testing.T) {
	t.Parallel()
	fake := newFakeRESTClient()
	admin := &authorizedClient{base: fake, principal: &restPrincipal{Kind: "user", Username: "root", Admin: true}}
	user := &authorizedClient{base: fake, principal: &restPrincipal{Kind: "user", Username: "bob", UID: 1002, PrimaryGID: 3000}}
	if _, err := user.PressureFailureStreak("demo"); mappedStatus(err) != http.StatusForbidden {
		t.Fatalf("non-admin streak must be forbidden, got: %v", err)
	}
	if _, err := user.PressurePendingDepth("demo"); mappedStatus(err) != http.StatusForbidden {
		t.Fatalf("non-admin depth must be forbidden, got: %v", err)
	}
	if _, err := admin.PressureFailureStreak("demo"); err != nil {
		t.Fatalf("admin streak must pass through: %v", err)
	}
	if _, err := admin.PressurePendingDepth("demo"); err != nil {
		t.Fatalf("admin depth must pass through: %v", err)
	}
}
