package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// panickingClient fails the test the moment any call reaches it: a share
// client must deny every mutation without delegating.
type panickingClient struct {
	Client
}

// TestRestrictedClientDeniesEverything exercises the chokepoint contract:
// every method of the Client interface, invoked with zero arguments, must
// return an access-denied error and never delegate to the underlying
// client. A future Client method that "forgets" to deny (and instead
// forwards) fails here with a nil-pointer panic from the embedded
// interface, and a read method whose zero-argument path escapes the share
// scope likewise trips the panicking underlying.
func TestRestrictedClientDeniesEverything(t *testing.T) {
	client := newRestrictedClient(panickingClient{}, "unshared-project", "shared")
	if client.allowedPath == "" {
		t.Fatal("test expects a non-root allowed path so zero args cannot match")
	}

	ifaceType := reflect.TypeOf((*Client)(nil)).Elem()
	restricted := reflect.ValueOf(client)
	for i := 0; i < ifaceType.NumMethod(); i++ {
		method := ifaceType.Method(i)
		m := restricted.MethodByName(method.Name)
		if !m.IsValid() {
			t.Fatalf("restrictedClient does not implement Client.%s", method.Name)
		}
		mt := m.Type()
		tail := mt.NumIn()
		if mt.IsVariadic() {
			// Call packs the variadic tail itself; supply none.
			tail--
		}
		args := make([]reflect.Value, 0, tail)
		for j := 0; j < tail; j++ {
			argType := mt.In(j)
			if argType == reflect.TypeOf((*context.Context)(nil)).Elem() {
				args = append(args, reflect.ValueOf(context.Background()))
				continue
			}
			args = append(args, reflect.Zero(argType))
		}
		results := m.Call(args)
		denied := false
		for _, res := range results {
			if err, ok := res.Interface().(error); ok && err != nil && strings.Contains(err.Error(), "access denied") {
				denied = true
			}
		}
		if !denied {
			t.Fatalf("Client.%s did not deny a zero-argument call: %v", method.Name, results)
		}
	}
}

// TestShareCreateLocationHeader pins the 201 creation convention.
func TestShareCreateLocationHeader(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/projects/demo/shares", bytes.NewReader(mustJSONMarshal(t, shareRequest{Path: "shared"})))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+login.Token)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("share create: got %d want 201 body=%s", rec.Code, rec.Body.String())
	}
	var share shareResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &share); err != nil {
		t.Fatalf("decode share: %v", err)
	}
	want := "/api/v1/projects/demo/shares/" + share.ID
	if got := rec.Header().Get("Location"); got != want {
		t.Fatalf("Location header: got %q want %q", got, want)
	}
}

// TestRevisionPreconditionEndToEnd drives the If-Match revision CAS through
// the HTTP surface: stat publishes X-StorHub-Revision, a mutation carrying
// that revision is enforced against the backend (stale revision fails 412),
// and classic attribute ETags keep their freshness semantics.
func TestRevisionPreconditionEndToEnd(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)
	auth := map[string]string{"Authorization": "Bearer " + login.Token}

	statResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=shared", nil, auth, http.StatusOK)
	rev := statResp.Header.Get("X-StorHub-Revision")
	if rev == "" {
		t.Fatal("stat response did not publish X-StorHub-Revision")
	}
	nodeETag := readETag(t, statResp)

	// Advance the remote behind the client's back; the old revision token
	// is now stale.
	client.SetRevision("rev-advanced")

	mustRequest(t, handler, http.MethodDelete,
		"/api/v1/projects/demo/nodes?path=shared/readme.txt",
		nil, map[string]string{"Authorization": "Bearer " + login.Token, "If-Match": rev}, http.StatusPreconditionFailed)

	// A CURRENT revision token enforces CAS at the backend and succeeds.
	fresh := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=shared", nil, auth, http.StatusOK)
	currentRev := fresh.Header.Get("X-StorHub-Revision")
	if currentRev != "rev-advanced" {
		t.Fatalf("stale revision header: %q", currentRev)
	}
	mustRequest(t, handler, http.MethodDelete,
		"/api/v1/projects/demo/nodes?path=shared/readme.txt",
		nil, map[string]string{"Authorization": "Bearer " + login.Token, "If-Match": currentRev}, http.StatusNoContent)

	// Classic attribute ETags keep working (freshness-only flow).
	mustRequest(t, handler, http.MethodDelete,
		"/api/v1/projects/demo/nodes?path=shared",
		nil, map[string]string{"Authorization": "Bearer " + login.Token, "If-Match": nodeETag}, http.StatusNoContent)
}

func readETag(t *testing.T, resp *http.Response) string {
	t.Helper()
	for _, v := range resp.Header.Values("X-StorHub-Node") {
		_ = v
	}
	var body struct {
		ETag string `json:"etag"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode node response: %v", err)
	}
	if body.ETag == "" {
		t.Fatal("node response missing etag")
	}
	return body.ETag
}

func quote(v string) string { return `"` + v + `"` }

// TestRevisionCASOnPatchFamily pins the round-4 ordering fix: a CURRENT
// revision token must upgrade PATCH-family mutations (append, write, patch,
// truncate) to backend CAS instead of being false-412'd by the attribute-
// ETag fast path, and a stale revision still fails 412 on every one of them.
func TestRevisionCASOnPatchFamily(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)

	currentRev := func() string {
		resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=shared/readme.txt", nil,
			map[string]string{"Authorization": "Bearer " + login.Token}, http.StatusOK)
		return resp.Header.Get("X-StorHub-Revision")
	}

	cases := []struct {
		name   string
		method string
		target string
		body   string
	}{
		{"append", http.MethodPatch, "/api/v1/projects/demo/content?path=shared/readme.txt&op=append", "more"},
		{"write", http.MethodPatch, "/api/v1/projects/demo/content?path=shared/readme.txt&op=write&offset=0", "swap"},
		{"patch", http.MethodPatch, "/api/v1/projects/demo/content?path=shared/readme.txt&op=patch&offset=0&delete_size=1", "X"},
		{"truncate", http.MethodPatch, "/api/v1/projects/demo/content?path=shared/readme.txt&op=truncate&size=2", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/stale", func(t *testing.T) {
			client.SetRevision("rev-stale-marker")
			mustRequest(t, handler, tc.method, tc.target, strings.NewReader(tc.body),
				map[string]string{"Authorization": "Bearer " + login.Token, "If-Match": quote("rev-older")}, http.StatusPreconditionFailed)
		})
		t.Run(tc.name+"/current", func(t *testing.T) {
			rev := currentRev()
			mustRequest(t, handler, tc.method, tc.target, strings.NewReader(tc.body),
				map[string]string{"Authorization": "Bearer " + login.Token, "If-Match": quote(rev)}, http.StatusOK)
		})
	}
}
