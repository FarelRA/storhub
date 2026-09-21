package rest

import (
	"net/http"
	"strings"
	"testing"
)

func TestRESTUISurfacesDocumentAndConfig(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	root := mustRequest(t, handler, http.MethodGet, "/", nil, nil, http.StatusOK)
	if body := string(readBody(t, root)); !strings.Contains(body, "StorHub Console") {
		t.Fatalf("unexpected ui body: %q", body)
	}
	mustRequest(t, handler, http.MethodGet, "/_nuxt/nope.js", nil, nil, http.StatusNotFound)
	config := mustRequest(t, handler, http.MethodGet, "/config.js", nil, nil, http.StatusOK)
	if body := string(readBody(t, config)); !strings.Contains(body, "authEnabled") || !strings.Contains(body, "/api/v1") {
		t.Fatalf("unexpected config body: %q", body)
	}
	authed, err := newHandlerForClient(client, Options{Auth: &AuthOptions{TokenSigningKey: []byte("0123456789abcdef0123456789abcdeg"), Users: []User{{Username: "admin", PasswordHash: testHashPass, UID: 0, PrimaryGID: 0, Admin: true}}}})
	if err != nil {
		t.Fatalf("new authed handler: %v", err)
	}
	authedConfig := mustRequest(t, authed, http.MethodGet, "/config.js", nil, nil, http.StatusOK)
	if body := string(readBody(t, authedConfig)); !strings.Contains(body, "true") {
		t.Fatalf("expected auth-enabled config: %q", body)
	}
}
