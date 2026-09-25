package rest

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// The share credential has one precedence: ?token= query, then the
// Authorization header, then the path segment. Query stays first because
// mailed console links carry it.

func seedFileShare(t *testing.T, handler http.Handler) shareResponse {
	t.Helper()
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("hello world"), nil, http.StatusCreated)
	return mustDecodeShare(t, mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		shareRequest{Path: "docs/f.txt"}, http.StatusCreated))
}

func TestShareCredentialQueryBeatsHeader(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	share := seedFileShare(t, handler)

	resp := mustRequest(t, handler, http.MethodGet, share.DownloadURL, nil,
		map[string]string{"Authorization": "Bearer garbage"}, http.StatusOK)
	if body := string(readBody(t, resp)); body != "hello world" {
		t.Fatalf("query credential must win over a bad header, body = %q", body)
	}
}

func TestShareCredentialQueryWinsOverHeader(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	share := seedFileShare(t, handler)

	target := "/api/v1/shares/" + share.ID + "/download?token=bogus"
	mustRequest(t, handler, http.MethodGet, target, nil,
		map[string]string{"Authorization": "Bearer " + share.Token}, http.StatusNotFound)
}

func TestShareCredentialExtractorOrder(t *testing.T) {
	t.Parallel()
	req := func(rawQuery, header string) *http.Request {
		r, err := http.NewRequest(http.MethodGet, "/x?"+rawQuery, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		return r
	}
	if got := shareTokenFromRequest(req("token=query-value", "Bearer header-value"), "path-value"); got != "query-value" {
		t.Fatalf("query must win, got %q", got)
	}
	if got := shareTokenFromRequest(req("", "Bearer header-value"), "path-value"); got != "header-value" {
		t.Fatalf("header must beat the path segment, got %q", got)
	}
	if got := shareTokenFromRequest(req("", ""), "path-value"); got != "path-value" {
		t.Fatalf("path segment is the last resort, got %q", got)
	}
	if got := shareTokenFromRequest(req("", ""), ""); got != "" {
		t.Fatalf("no credential must stay empty, got %q", got)
	}
}

// A deleted link stays dead on every lane: public redemption answers
// 404 and the credential lane answers 401, all from the one revocation
// check in the redemption builder.
func TestShareRevokedLinkStaysDead(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "rootpass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)
	auth := map[string]string{"Authorization": "Bearer " + login.Token}
	postJSON := func(target string, payload any, want int) *http.Response {
		t.Helper()
		headers := map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + login.Token}
		return mustRequest(t, handler, http.MethodPost, target, bytes.NewBuffer(mustJSONMarshal(t, payload)), headers, want)
	}

	postJSON("/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("hello world"), auth, http.StatusCreated)
	share := mustDecodeShare(t, postJSON("/api/v1/projects/demo/shares", shareRequest{Path: "docs/f.txt"}, http.StatusCreated))
	shareAuth := map[string]string{"Authorization": "Bearer " + share.Token}

	mustRequest(t, handler, http.MethodGet, share.DownloadURL, nil, nil, http.StatusOK)
	mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/f.txt", nil, shareAuth, http.StatusOK)

	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/shares/"+share.ID, nil, auth, http.StatusNoContent)

	mustRequest(t, handler, http.MethodGet, share.DownloadURL, nil, nil, http.StatusNotFound)
	mustRequest(t, handler, http.MethodGet, "/api/v1/shares/"+url.PathEscape(share.Token), nil, nil, http.StatusNotFound)
	mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/f.txt", nil, shareAuth, http.StatusUnauthorized)
}

// A derived share answers the same management address as a created one:
// the Location names /projects/{p}/shares/{id}, never the public
// redemption path (a bare ID there is not a credential and answers 404).
func TestShareDeriveLocationUsesManagementPath(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("data"), nil, http.StatusCreated)
	parent := mustDecodeShare(t, mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "docs"}, http.StatusCreated))

	derive := mustRequest(t, handler, http.MethodPost,
		"/api/v1/shares/"+parent.ID+"/derive?token="+url.QueryEscape(parent.Token),
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "docs/f.txt"})),
		map[string]string{"Content-Type": "application/json"}, http.StatusCreated)
	child := mustDecodeShare(t, derive)
	want := "/api/v1/projects/demo/shares/" + child.ID
	if got := derive.Header.Get("Location"); got != want {
		t.Fatalf("derive Location = %q, want %q", got, want)
	}
}
