package rest

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
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
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("payload"), nil, http.StatusCreated)
	createResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		shareRequest{Path: "docs/f.txt"}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, createResp, &share)

	downloadURL := share.DownloadURL + "&path=docs%2Ff.txt"
	headResp := mustRequest(t, handler, http.MethodHead, downloadURL, nil, nil, http.StatusOK)
	if got := headResp.Header.Get("Content-Length"); got == "" || got == "0" {
		t.Fatalf("HEAD must advertise length, got %q", got)
	}
	if body := string(readBody(t, headResp)); body != "" {
		t.Fatalf("HEAD must have empty body, got %q", body)
	}
}

// TestShareRedemptionIsStateless pins the single-pathway contract: the
// signed token IS the share. URLs embed the token (restart-proof - the
// in-memory registry is only bookkeeping for listings/revocation), listings
// never leak it, and deletion revokes immediately for this process.
func TestShareRedemptionIsStateless(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)
	auth := map[string]string{"Authorization": "Bearer " + login.Token, "Content-Type": "application/json"}

	shareReq := bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared"}))
	shareResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareReq, auth, http.StatusCreated)
	var created shareResponse
	decodeJSONBody(t, shareResp, &created)
	if created.ID == "" || strings.Contains(created.ID, ".") {
		t.Fatalf("share URL id must be a short opaque identifier, got %q", created.ID)
	}
	if created.Token == "" || !strings.Contains(created.Token, ".") {
		t.Fatalf("creation response must carry the signed token")
	}
	if !strings.Contains(created.URL, created.Token) || strings.Contains(created.URL, "/shares/"+created.ID) {
		t.Fatalf("share url must embed the signed token: %q", created.URL)
	}

	// Stateless info redemption by token.
	info := mustRequest(t, handler, http.MethodGet, "/api/v1/shares/"+created.Token, nil, nil, http.StatusOK)
	var public shareResponse
	decodeJSONBody(t, info, &public)
	if public.Project != "demo" || public.Path != "shared" || public.Token != created.Token {
		t.Fatalf("unexpected stateless share info: %+v", public)
	}

	// Download via token-bearing URL under /api/v1.
	mustRequest(t, handler, http.MethodGet,
		"/api/v1/shares/"+created.ID+"/download?token="+url.QueryEscape(created.Token)+"&path=shared/readme.txt",
		nil, nil, http.StatusOK)

	// Listings never leak tokens or mintable URLs.
	listResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/shares", nil, auth, http.StatusOK)
	if strings.Contains(string(readBody(t, listResp)), created.Token) {
		t.Fatal("share listing leaked the signed token")
	}

	// Revocation kills the link immediately (same process).
	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/shares/"+created.ID, nil, auth, http.StatusNoContent)
	mustRequest(t, handler, http.MethodGet, "/api/v1/shares/"+created.Token, nil, nil, http.StatusNotFound)
}
