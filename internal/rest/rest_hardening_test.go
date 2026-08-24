package rest

import (
	"bytes"
	"errors"

	ghapi "github.com/FarelRA/storhub/internal/github"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPanickingHandlerReturns500 pins the recovery middleware: a panic in
// any handler becomes a clean internal_error, not a dropped connection.
func TestPanickingHandlerReturns500(t *testing.T) {
	handler := &restHandler{
		client: newFakeRESTClient(),
		opts:   Options{AllowAnonymous: true}.withDefaults(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(errors.New("boom"))
	})
	rec := httptest.NewRecorder()
	handler.recoverPanics(inner).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 after panic, got %d", rec.Code)
	}
	assertErrorCode(t, rec.Result(), "internal_error")
}

func TestOversizedJSONBodyRejectedWith413(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	big := strings.Repeat("a", int(maxRequestBodyMemory)+10)
	resp := mustRequest(t, handler, http.MethodPost,
		"/api/v1/projects/demo/ops/mkdir",
		strings.NewReader(`{"path":"`+big+`"}`), nil, http.StatusRequestEntityTooLarge)
	assertErrorCode(t, resp, "payload_too_large")
}

func TestUpstreamErrorsAreSanitized(t *testing.T) {
	// The mapped message for upstream failures must be generic; raw GitHub
	// error bodies can carry infrastructure details.
	if status := mappedStatus(&ghapi.APIError{StatusCode: 500}); status != http.StatusBadGateway {
		t.Fatalf("upstream errors should map to 502, got %d", status)
	}
	err := &ghapi.APIError{StatusCode: 500, Message: "secret hostname srv-9 failed"}
	handler := &restHandler{
		client: newFakeRESTClient(),
		opts:   Options{AllowAnonymous: true}.withDefaults(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	rec := httptest.NewRecorder()
	handler.writeMappedError(rec, err)
	body := rec.Body.String()
	if strings.Contains(body, "srv-9") || strings.Contains(body, "secret") {
		t.Fatalf("upstream details leaked to client: %s", body)
	}
	if !strings.Contains(body, "upstream GitHub request failed") {
		t.Fatalf("missing sanitized message: %s", body)
	}
}

func TestUpstreamNotFoundSanitizedAndCoded(t *testing.T) {
	err := &ghapi.APIError{StatusCode: 404, Message: `{"message":"Not Found","documentation_url":"https://docs.github.com"}`}
	if status := mappedStatus(err); status != http.StatusNotFound {
		t.Fatalf("upstream 404 should map to 404, got %d", status)
	}
	handler := &restHandler{
		client: newFakeRESTClient(),
		opts:   Options{AllowAnonymous: true}.withDefaults(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	rec := httptest.NewRecorder()
	handler.writeMappedError(rec, err)
	body := rec.Body.String()
	if strings.Contains(body, "documentation_url") || strings.Contains(body, "Not Found") {
		t.Fatalf("upstream body leaked to client: %s", body)
	}
	if !strings.Contains(body, `"code":"not_found"`) || !strings.Contains(body, "upstream GitHub request failed") {
		t.Fatalf("unexpected sanitized error envelope: %s", body)
	}
}

func TestMappedCodeNamesBadGateway(t *testing.T) {
	if code := mappedCode(http.StatusBadGateway); code != "bad_gateway" {
		t.Fatalf("502 must map to bad_gateway, got %q", code)
	}
}

func TestShareDownloadSupportsHead(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{
		AllowAnonymous:  true,
		ShareSigningKey: []byte("test-signing-key-at-least-32-bytes!!"),
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	download := true
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("payload"), nil, http.StatusCreated)
	createResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		shareRequest{Path: "docs/f.txt", Download: &download}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, createResp, &share)

	downloadURL := "/shares/" + share.ID + "/download"
	headResp := mustRequest(t, handler, http.MethodHead, downloadURL, nil, nil, http.StatusOK)
	if got := headResp.Header.Get("Content-Length"); got == "" || got == "0" {
		t.Fatalf("HEAD must advertise length, got %q", got)
	}
	if body := string(readBody(t, headResp)); body != "" {
		t.Fatalf("HEAD must have empty body, got %q", body)
	}
}

// TestShareLinksCarryShortIDs pins the round-4 registry change: URLs and
// listing IDs are short opaque identifiers (no JWT in links), the creation
// response alone carries the signed token for bearer use, deleted shares
// stay dead by ID, and legacy JWT-shaped segments still resolve to their
// live record.
func TestShareLinksCarryShortIDs(t *testing.T) {
	dl := true
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)
	auth := map[string]string{"Authorization": "Bearer " + login.Token, "Content-Type": "application/json"}

	shareReq := bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared", Download: &dl}))
	shareResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareReq, auth, http.StatusCreated)
	var created shareResponse
	decodeJSONBody(t, shareResp, &created)
	if created.ID == "" || strings.Contains(created.ID, ".") {
		t.Fatalf("share URL id must be a short opaque identifier, got %q", created.ID)
	}
	if created.Token == "" || !strings.Contains(created.Token, ".") {
		t.Fatalf("creation response must carry the signed token for bearer use")
	}
	if !strings.Contains(created.URL, created.ID) || strings.Contains(created.URL, created.Token) {
		t.Fatalf("share url must embed the short id only: %q", created.URL)
	}

	// Download by short ID works; a token in the URL is NOT a locator.
	mustRequest(t, handler, http.MethodGet, "/shares/"+created.ID+"/download?path=shared/readme.txt", nil, nil, http.StatusOK)
	mustRequest(t, handler, http.MethodGet, "/shares/"+created.Token+"/download?path=shared/readme.txt", nil, nil, http.StatusNotFound)

	// Listings never leak tokens.
	listResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/shares", nil, auth, http.StatusOK)
	if strings.Contains(string(readBody(t, listResp)), created.Token) {
		t.Fatal("share listing leaked the signed token")
	}

	// Revocation kills the link immediately.
	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/shares/"+created.ID, nil, auth, http.StatusNoContent)
	mustRequest(t, handler, http.MethodGet, "/shares/"+created.ID+"/download?path=shared/readme.txt", nil, nil, http.StatusNotFound)
}
