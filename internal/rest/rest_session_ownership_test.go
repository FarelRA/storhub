package rest

// rest_session_ownership_test.go: session owner continuity over REST.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestRESTSessionOwnerMismatch pins identity continuity: a handle opened by
// one user fails closed for another (403) while an admin still passes.

func TestRESTSessionOwnerMismatch(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	opts := DefaultOptions()
	opts.ShareSigningKey = []byte("abcdef0123456789abcdef0123456789")
	opts.Auth = &AuthOptions{
		TokenSigningKey: []byte("testsigningkey0123456789abcdef0000"),
		Users: []User{
			{Username: "alice", Password: "alicepass", UID: 1001, PrimaryGID: 2001},
			{Username: "bob", Password: "bobpass", UID: 1002, PrimaryGID: 2002},
			{Username: "root", Password: "rootpass", UID: 0, PrimaryGID: 0, Admin: true},
		},
	}
	handler, err := newHandlerForClient(client, opts)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	login := func(user, pass string) string {
		t.Helper()
		resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: user, Password: pass}, http.StatusOK)
		var logged restLoginResponse
		decodeJSONBody(t, resp, &logged)
		return logged.Token
	}
	alice, bob, root := login("alice", "alicepass"), login("bob", "bobpass"), login("root", "rootpass")

	// Alice opens: the manager records her UID as the owner.
	authed := map[string]string{"Authorization": "Bearer " + alice}
	openResp := mustJSONRequestAuthed(t, handler, http.MethodPost, "/api/v1/handles",
		map[string]string{"project": "demo", "mode": "w"}, authed, http.StatusCreated)
	var opened sessionOpenResponse
	decodeJSONBody(t, openResp, &opened)

	bobHeaders := map[string]string{"Authorization": "Bearer " + bob}
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+opened.Handle, nil, bobHeaders, http.StatusForbidden)
	assertErrorCode(t, resp, "forbidden")

	rootHeaders := map[string]string{"Authorization": "Bearer " + root}
	mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+opened.Handle, nil, rootHeaders, http.StatusOK)
	mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+opened.Handle, nil, authed, http.StatusOK)
}

func mustJSONRequestAuthed(t *testing.T, handler http.Handler, method, target string, payload any, headers map[string]string, wantStatus int) *http.Response {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	merged := map[string]string{"Content-Type": "application/json"}
	for key, value := range headers {
		merged[key] = value
	}
	return mustRequest(t, handler, method, target, strings.NewReader(string(body)), merged, wantStatus)
}
