package rest

// rest_session_errors_test.go: session error and status mapping over REST.

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRESTSessionErrorMapping pins every status mapping: stale (unknown and
// expired) to 410 with the reason, busy to 429, bad input to 400, and mode
// violations to 400.

func TestRESTSessionErrorMapping(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/handles/no-such-handle", nil, nil, http.StatusGone)
	if code, body := decodeSessionError(t, resp); code != "gone" || !strings.Contains(body, "unknown handle") {
		t.Fatalf("stale mapping = code %q body %q, want gone + unknown handle", code, body)
	}

	client.sessionTTL = 30 * time.Millisecond
	expiring := openSessionHTTP(t, handler, "demo", "", "w")
	time.Sleep(80 * time.Millisecond)
	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+expiring, nil, nil, http.StatusGone)
	if code, body := decodeSessionError(t, resp); code != "gone" || !strings.Contains(body, "expired") {
		t.Fatalf("expired mapping = code %q body %q, want gone + expired", code, body)
	}
	client.sessionTTL = 0

	client.maxSessProject = 1
	first := openSessionHTTP(t, handler, "demo", "", "w")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", map[string]string{"project": "demo", "mode": "w"}, http.StatusTooManyRequests)
	assertErrorCode(t, resp, "rate_limited")
	mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+first+"/close", nil, nil, http.StatusOK)
	client.maxSessProject = 0

	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", map[string]string{"project": "demo", "mode": "zzz"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", map[string]string{"project": "demo", "mode": "r", "ttl": "notaduration"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles", map[string]string{"mode": "r"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")

	readonly := openSessionHTTP(t, handler, "demo", "", "r")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+readonly+"/write",
		map[string]any{"offset": 0, "data": base64.StdEncoding.EncodeToString([]byte("x"))}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")

	bad := openSessionHTTP(t, handler, "demo", "", "w")
	resp = mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+bad+"/write",
		map[string]any{"offset": 0, "data": "!!!not-base64!!!"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
}

func decodeSessionError(t *testing.T, resp *http.Response) (code, message string) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var payload restError
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if payload.Error.Code == "" {
		t.Fatal("error body missing code")
	}
	return payload.Error.Code, payload.Error.Message
}
