package rest

import (
	"io/fs"
	"net/http"
	"strings"
	"testing"
)

// uiBundleEmbedded reports whether the console bundle was embedded at
// build time. The dist is git-ignored and rebuilt by release jobs, so Go
// gate checkouts have no bundle: the contract under test is serve the
// bundle when embedded, graceful ui_not_built when not. Both branches
// run wherever their precondition holds.
func uiBundleEmbedded() bool {
	dist, err := distFS()
	if err != nil {
		return false
	}
	st, err := fs.Stat(dist, "index.html")
	return err == nil && st.Mode().IsRegular()
}

func TestRESTUISurfacesDocumentAndConfig(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	if uiBundleEmbedded() {
		root := mustRequest(t, handler, http.MethodGet, "/", nil, nil, http.StatusOK)
		if body := string(readBody(t, root)); !strings.Contains(body, "StorHub Console") {
			t.Fatalf("unexpected ui body: %q", body)
		}
		mustRequest(t, handler, http.MethodGet, "/_nuxt/nope.js", nil, nil, http.StatusNotFound)
	} else {
		missing := mustRequest(t, handler, http.MethodGet, "/", nil, nil, http.StatusNotFound)
		if body := string(readBody(t, missing)); !strings.Contains(body, "ui_not_built") {
			t.Fatalf("unexpected missing-ui body: %q", body)
		}
	}
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
