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

// TestDownloadLinkLifecycle pins the whole share-based download contract:
// authed mint via POST /shares (download=true, 5m) -> anonymous redeem
// via GET /shares/{id}/download (attachment + exact bytes), Range resume,
// and strict claim scoping. Replaces the old POST /downloads + GET /download/{project}.
func TestDownloadLinkLifecycle(t *testing.T) {
	const content = "hello native download\n"
	fake := newFakeRESTClient()
	seedDownloadFile(t, fake, "docs/report.txt", content)

	handler, bearer := newAuthedShareHandler(t, fake)
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		strings.NewReader(`{"path":"docs/report.txt","expires_in_seconds":300}`), map[string]string{"Authorization": bearer}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, resp, &share)
	if share.DownloadURL == "" || share.Token == "" {
		t.Fatalf("mint response incomplete: %+v", share)
	}
	linkURL := share.DownloadURL

	// Anonymous redeem - no Authorization header, like a real browser.
	got := mustRequest(t, handler, http.MethodGet, linkURL, nil, nil, http.StatusOK)
	if disp := got.Header.Get("Content-Disposition"); !strings.HasPrefix(disp, `attachment; filename="report.txt"`) {
		t.Fatalf("unexpected disposition: %q", disp)
	}
	if body := string(readBody(t, got)); body != content {
		t.Fatalf("body mismatch: %q", body)
	}

	// Range request: resumability is inherited from the streaming reader.
	partial := mustRequest(t, handler, http.MethodGet, linkURL,
		nil, map[string]string{"Range": "bytes=6-9"}, http.StatusPartialContent)
	if body := string(readBody(t, partial)); body != "nati" {
		t.Fatalf("range body mismatch: %q", body)
	}

	// Garbage token is rejected (404 for unknown share ID or bad JWT)
	mustRequest(t, handler, http.MethodGet,
		"/api/v1/shares/invalid-id/download?token=garbage", nil, nil, http.StatusNotFound)
	tampered := linkURL
	if idx := strings.Index(tampered, "token="); idx != -1 {
		tampered = tampered[:idx+6] + "garbage" + tampered[idx+6:]
	}
	mustRequest(t, handler, http.MethodGet, tampered, nil, nil, http.StatusNotFound)

	// Directories via shares with download=true succeed but have no download_url (files only)
	if err := fake.MkdirContext(context.Background(), "demo", "docs/archive"); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}
	resp2 := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		strings.NewReader(`{"path":"docs/archive","expires_in_seconds":300}`), map[string]string{"Authorization": bearer}, http.StatusCreated)
	var share2 shareResponse
	decodeJSONBody(t, resp2, &share2)
	if share2.DownloadURL != "" {
		t.Fatalf("expected no download_url for folder share, got %+v", share2)
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
	errResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
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
