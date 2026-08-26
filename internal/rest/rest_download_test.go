package rest

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// seedDownloadFile creates a file with deterministic content on the fake
// backend so mint/redeem flows have something real to stream.
func seedDownloadFile(t *testing.T, client *fakeRESTClient, path, content string) {
	t.Helper()
	dir := ""
	for _, part := range strings.Split(path, "/")[:len(strings.Split(path, "/"))-1] {
		dir = part
		break
	}
	if dir != "" {
		if err := client.MkdirContext(context.Background(), "demo", dir); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if _, err := client.CreateFileContext(context.Background(), "demo", path); err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	if _, err := client.WriteFileAtContext(context.Background(), "demo", path, 0, []byte(content)); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestDownloadLinkLifecycle pins the whole native-download contract:
// authed mint -> anonymous browser-style redeem (attachment + exact bytes),
// HTTP Range resume through the same URL, and strict claim scoping.
func TestDownloadLinkLifecycle(t *testing.T) {
	const content = "hello native download\n"
	fake := newFakeRESTClient()
	seedDownloadFile(t, fake, "docs/report.txt", content)

	handler, bearer := newAuthedShareHandler(t, fake)
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/downloads",
		strings.NewReader(`{"path":"docs/report.txt"}`), map[string]string{"Authorization": bearer}, http.StatusCreated)
	var link downloadLinkResponse
	decodeJSONBody(t, resp, &link)
	if link.URL == "" || link.ExpiresIn <= 0 {
		t.Fatalf("mint response incomplete: %+v", link)
	}

	// Anonymous redeem - no Authorization header, like a real browser.
	got := mustRequest(t, handler, http.MethodGet, link.URL, nil, nil, http.StatusOK)
	if disp := got.Header.Get("Content-Disposition"); !strings.HasPrefix(disp, `attachment; filename="report.txt"`) {
		t.Fatalf("unexpected disposition: %q", disp)
	}
	if body := string(readBody(t, got)); body != content {
		t.Fatalf("body mismatch: %q", body)
	}

	// Range request: resumability is inherited from the streaming reader.
	partial := mustRequest(t, handler, http.MethodGet, link.URL,
		nil, map[string]string{"Range": "bytes=6-9"}, http.StatusPartialContent)
	if body := string(readBody(t, partial)); body != "nati" {
		t.Fatalf("range body mismatch: %q", body)
	}

	// Project mismatch between URL and signed claims is rejected.
	wrongProject := strings.Replace(link.URL, "/download/demo", "/download/other", 1)
	mustRequest(t, handler, http.MethodGet, wrongProject, nil, nil, http.StatusForbidden)

	// Garbage token.
	mustRequest(t, handler, http.MethodGet,
		"/api/v1/download/demo?token=garbage&path=docs/report.txt", nil, nil, http.StatusForbidden)

	// Directories are refused at mint time with guidance toward shares.
	if err := fake.MkdirContext(context.Background(), "demo", "docs/archive"); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}
	errResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/downloads",
		strings.NewReader(`{"path":"docs/archive"}`), map[string]string{"Authorization": bearer}, http.StatusConflict)
	if body := readBody(t, errResp); !strings.Contains(string(body), "is_directory") {
		t.Fatalf("expected is_directory error, got: %s", body)
	}
}

// TestDownloadLinkRequiresSigningKey: anonymous deployments without any key
// get the actionable 403, not a 500.
func TestDownloadLinkRequiresSigningKey(t *testing.T) {
	fake := newFakeRESTClient()
	seedDownloadFile(t, fake, "f.txt", "x")
	handler, err := newHandlerForClient(fake, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	errResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/downloads",
		strings.NewReader(`{"path":"f.txt"}`), nil, http.StatusForbidden)
	if body := readBody(t, errResp); !strings.Contains(string(body), "--share-key") {
		t.Fatalf("expected actionable message, got: %s", body)
	}
}

func newAuthedShareHandler(t *testing.T, client *fakeRESTClient) (http.Handler, string) {
	t.Helper()
	opts := DefaultOptions()
	opts.Auth = &AuthOptions{
		TokenSigningKey: []byte("download-test-signing-key-32bytes!"),
		Users:           []User{{Username: "admin", Password: "pass", UID: 0, PrimaryGID: 0, Admin: true}},
	}
	handler, err := newHandlerForClient(client, opts)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	login := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login",
		restLoginRequest{Username: "admin", Password: "pass"}, http.StatusOK)
	var session restLoginResponse
	decodeJSONBody(t, login, &session)
	return handler, "Bearer " + session.Token
}

var _ = fmt.Sprintf // placate imports during table tweaks
