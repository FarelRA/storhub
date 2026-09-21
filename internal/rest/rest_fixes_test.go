package rest

import (
	"net/http"
	"testing"
)

const testShareKey = "abcdef0123456789abcdef0123456789"

// ---- helpers ----

func loginBearer(t *testing.T, handler http.Handler, username, password string) string {
	t.Helper()
	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: username, Password: password}, http.StatusOK)
	var session restLoginResponse
	decodeJSONBody(t, resp, &session)
	if session.Token == "" {
		t.Fatal("login returned no token")
	}
	return "Bearer " + session.Token
}

func mustDecodeShare(t *testing.T, resp *http.Response) shareResponse {
	t.Helper()
	var share shareResponse
	decodeJSONBody(t, resp, &share)
	if share.ID == "" {
		t.Fatal("empty share id in response")
	}
	return share
}
