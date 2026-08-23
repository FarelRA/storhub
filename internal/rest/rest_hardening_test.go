package rest

import (
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
		shareRequest{Path: "docs/f.txt", Download: &download}, http.StatusOK)
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
