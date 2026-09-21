package rest

// rest_ops_authorization_test.go: privileged ops authorization and owner
// chgrp conventions over REST.

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ---- HTTP-level authz gaps (rollback/revert-path/prune) ----

func TestOpsRoutesDenyNonAdminAndShareTokens(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)
	alice := loginBearer(t, handler, "alice", "alicepass")
	root := loginBearer(t, handler, "root", "rootpass")

	share := mustDecodeShare(t, mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared"})),
		map[string]string{"Authorization": root, "Content-Type": "application/json"}, http.StatusCreated))

	var revisions revisionsResponse
	decodeJSONBody(t, mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/revisions", nil, map[string]string{"Authorization": root}, http.StatusOK), &revisions)
	if len(revisions.Revisions) == 0 {
		t.Fatal("seed produced no revisions")
	}
	validSHA := revisions.Revisions[0].CommitSHA

	cases := []struct {
		name   string
		target string
		body   any
	}{
		{"rollback", "/api/v1/projects/demo/ops/rollback", rollbackRequest{CommitSHA: validSHA}},
		{"revert", "/api/v1/projects/demo/ops/revert", revertPathRequest{Path: "shared/readme.txt", CommitSHA: validSHA}},
		{"purge", "/api/v1/projects/demo/ops/prune", pruneRequest{Scope: "objects", DryRun: true}},
	}
	for _, tc := range cases {
		for _, caller := range []struct{ who, bearer string }{
			{"nonadmin", alice},
			{"sharetoken", "Bearer " + share.Token},
		} {
			t.Run(tc.name+"/"+caller.who, func(t *testing.T) {
				mustJSONRequestWithBearer(t, handler, tc.target, tc.body, caller.bearer, http.StatusForbidden)
			})
		}
		// The admin gate must not over-deny: root passes all three.
		t.Run(tc.name+"/admin", func(t *testing.T) {
			resp := mustJSONRequestWithBearer(t, handler, tc.target, tc.body, root, http.StatusOK)
			_ = readBody(t, resp)
		})
	}
}

func mustJSONRequestWithBearer(t *testing.T, handler http.Handler, target string, payload any, bearer string, wantStatus int) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewBuffer(mustJSONMarshal(t, payload)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", bearer)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Result().StatusCode != wantStatus {
		t.Fatalf("%s: got %d want %d body=%s", target, rec.Code, wantStatus, rec.Body.String())
	}
	return rec.Result()
}

// ---- Owner chgrp over REST follows CanChown; uid/gid omission keeps ----

func TestRESTOwnerChgrpAndKeepConvention(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	rootAuth := map[string]string{"Authorization": loginBearer(t, handler, "root", "rootpass"), "Content-Type": "application/json"}
	aliceAuth := map[string]string{"Authorization": loginBearer(t, handler, "alice", "alicepass"), "Content-Type": "application/json"}

	// Hand shared/readme.txt to alice so she is the file owner.
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBuffer(mustJSONMarshal(t, chownRequest{Path: "shared/readme.txt", UID: 1001, GID: 2001})), rootAuth, http.StatusOK)

	// Omitted uid means keep: owner chgrp to her own group succeeds.
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBufferString(`{"path":"shared/readme.txt","gid":2001}`), aliceAuth, http.StatusOK)
	var doc nodeResponse
	decodeJSONBody(t, resp, &doc)
	if doc.Entry.UID != 1001 || doc.Entry.GID != 2001 {
		t.Fatalf("owner keep-chgrp changed ownership: %+v", doc.Entry)
	}

	// Explicit -1 on both ids is the keep spelling shared with the CLI.
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBufferString(`{"path":"shared/readme.txt","uid":-1,"gid":-1}`), aliceAuth, http.StatusOK)

	// The owner still cannot hand the file away or move it out of her groups.
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBuffer(mustJSONMarshal(t, chownRequest{Path: "shared/readme.txt", UID: 1002, GID: 2001})), aliceAuth, http.StatusForbidden)
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBufferString(`{"path":"shared/readme.txt","gid":4000}`), aliceAuth, http.StatusForbidden)

	// Out-of-range ids fail closed with 400, not 500.
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBufferString(`{"path":"shared/readme.txt","uid":4294967296}`), rootAuth, http.StatusBadRequest)
}
