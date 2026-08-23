package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestRESTAuthLoginAndPermissions(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "alice", Password: "alice-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)
	if login.Token == "" || login.Principal.Username != "alice" {
		t.Fatalf("unexpected login response: %+v", login)
	}

	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=private/secret.txt", nil, map[string]string{"Authorization": "Bearer " + login.Token}, http.StatusForbidden)
	assertErrorCode(t, resp, "forbidden")

	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=shared/readme.txt", nil, map[string]string{"Authorization": "Bearer " + login.Token}, http.StatusOK)
	if got := string(readBody(t, resp)); got != "shared data" {
		t.Fatalf("unexpected readable content: %q", got)
	}

	resp = mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=shared/readme.txt&op=append", bytes.NewBufferString("!"), map[string]string{"Authorization": "Bearer " + login.Token}, http.StatusForbidden)
	assertErrorCode(t, resp, "forbidden")

	resp = mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=public/note.txt&op=append", bytes.NewBufferString("!"), map[string]string{"Authorization": "Bearer " + login.Token}, http.StatusOK)
	if body := string(readBody(t, mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=public/note.txt", nil, map[string]string{"Authorization": "Bearer " + login.Token}, http.StatusOK))); body != "hello!" {
		t.Fatalf("unexpected updated public note: %q", body)
	}
}

func TestRESTAuthAdminAndInvalidBearer(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	unauth := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo", nil, nil, http.StatusUnauthorized)
	assertErrorCode(t, unauth, "unauthorized")

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)

	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown", bytes.NewBuffer(mustJSONMarshal(t, chownRequest{Path: "private/secret.txt", UID: 1002, GID: 2002})), map[string]string{"Authorization": "Bearer " + login.Token, "Content-Type": "application/json"}, http.StatusOK)
	_ = readBody(t, resp)

	resp = mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo", nil, map[string]string{"Authorization": "Bearer " + login.Token}, http.StatusOK)
	_ = readBody(t, resp)

	bad := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo", nil, map[string]string{"Authorization": "Bearer bad.token"}, http.StatusUnauthorized)
	assertErrorCode(t, bad, "unauthorized")
}

func TestRESTShareBearerRootDirectoryReadOnly(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)

	shareReq := bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: ""}))
	shareResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareReq, map[string]string{"Authorization": "Bearer " + login.Token, "Content-Type": "application/json"}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, shareResp, &share)

	childrenResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/children?path=", nil, map[string]string{"Authorization": "Bearer " + share.Token}, http.StatusOK)
	var children entriesResponse
	decodeJSONBody(t, childrenResp, &children)
	if len(children.Entries) == 0 {
		t.Fatal("expected shared root directory entries")
	}

	forbiddenProject := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo", nil, map[string]string{"Authorization": "Bearer " + share.Token}, http.StatusForbidden)
	assertErrorCode(t, forbiddenProject, "forbidden")

	forbiddenCreate := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/create-file", bytes.NewBuffer(mustJSONMarshal(t, pathRequest{Path: "new.txt"})), map[string]string{"Authorization": "Bearer " + share.Token, "Content-Type": "application/json"}, http.StatusForbidden)
	assertErrorCode(t, forbiddenCreate, "forbidden")
}

func TestRESTShareBearerCannotEscapeSharedPath(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)

	shareResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared"})), map[string]string{"Authorization": "Bearer " + login.Token, "Content-Type": "application/json"}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, shareResp, &share)

	forbiddenContent := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=shared/../private/secret.txt", nil, map[string]string{"Authorization": "Bearer " + share.Token}, http.StatusForbidden)
	assertErrorCode(t, forbiddenContent, "forbidden")

	forbiddenNode := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=shared/../private/secret.txt", nil, map[string]string{"Authorization": "Bearer " + share.Token}, http.StatusForbidden)
	assertErrorCode(t, forbiddenNode, "forbidden")
}

func TestRESTShareBearerCannotCreateNestedShares(t *testing.T) {
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	loginResp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "root", Password: "root-pass"}, http.StatusOK)
	var login restLoginResponse
	decodeJSONBody(t, loginResp, &login)

	download := false
	shareResp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared", Download: &download})), map[string]string{"Authorization": "Bearer " + login.Token, "Content-Type": "application/json"}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, shareResp, &share)

	forbiddenReshare := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared", Download: boolPtr(true)})), map[string]string{"Authorization": "Bearer " + share.Token, "Content-Type": "application/json"}, http.StatusForbidden)
	assertErrorCode(t, forbiddenReshare, "forbidden")
}

func TestHashPasswordAndTokenExpiry(t *testing.T) {
	hash, err := HashPassword("secret")
	if err != nil || !verifyPassword("secret", hash) || verifyPassword("wrong", hash) {
		t.Fatalf("unexpected password verification result: hash=%q err=%v", hash, err)
	}
	now := time.Unix(100, 0).UTC()
	auth, err := newAuthenticator(AuthOptions{TokenSigningKey: []byte("0123456789abcdef0123456789abcdef"), TokenTTL: time.Second, Now: func() time.Time { return now }, Users: []User{{Username: "u", Password: "p", UID: 1, PrimaryGID: 1}}})
	if err != nil {
		t.Fatalf("new authenticator: %v", err)
	}
	principal, token, _, err := auth.login("u", "p")
	if err != nil || principal.Username != "u" {
		t.Fatalf("login failed: %+v %v", principal, err)
	}
	if _, err := auth.parseToken(token); err != nil {
		t.Fatalf("parse token: %v", err)
	}
	auth.now = func() time.Time { return now.Add(2 * time.Second) }
	if _, err := auth.parseToken(token); err == nil {
		t.Fatal("expected expired token")
	}
}

func newAuthedTestHandler(t *testing.T, client *fakeRESTClient) http.Handler {
	t.Helper()
	opts := DefaultOptions()
	opts.ShareSigningKey = []byte("abcdef0123456789abcdef0123456789")
	opts.Auth = &AuthOptions{
		TokenSigningKey: []byte("test-signing-key-0123456789abcdef"),
		Users: []User{
			{Username: "alice", Password: "alice-pass", UID: 1001, PrimaryGID: 2001},
			{Username: "root", Password: "root-pass", UID: 0, PrimaryGID: 0, Admin: true},
		},
	}
	handler, err := newHandlerForClient(client, opts)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	return handler
}

func boolPtr(v bool) *bool {
	return &v
}

func seedProjectForAuth(t *testing.T, client *fakeRESTClient) {
	t.Helper()
	if err := client.MkdirContext(context.Background(), "demo", "public"); err != nil {
		t.Fatalf("mkdir public: %v", err)
	}
	if err := client.MkdirContext(context.Background(), "demo", "private"); err != nil {
		t.Fatalf("mkdir private: %v", err)
	}
	if _, err := client.CreateFileContext(context.Background(), "demo", "public/note.txt"); err != nil {
		t.Fatalf("create public file: %v", err)
	}
	if _, err := client.WriteFileAtContext(context.Background(), "demo", "public/note.txt", 0, []byte("hello")); err != nil {
		t.Fatalf("write public file: %v", err)
	}
	if _, err := client.CreateFileContext(context.Background(), "demo", "shared/readme.txt"); err == nil {
		t.Fatal("expected missing parent for shared/readme.txt before mkdir")
	}
	if err := client.MkdirContext(context.Background(), "demo", "shared"); err != nil {
		t.Fatalf("mkdir shared: %v", err)
	}
	if _, err := client.CreateFileContext(context.Background(), "demo", "shared/readme.txt"); err != nil {
		t.Fatalf("create shared file: %v", err)
	}
	if _, err := client.WriteFileAtContext(context.Background(), "demo", "shared/readme.txt", 0, []byte("shared data")); err != nil {
		t.Fatalf("write shared file: %v", err)
	}
	if _, err := client.CreateFileContext(context.Background(), "demo", "private/secret.txt"); err != nil {
		t.Fatalf("create secret file: %v", err)
	}
	if _, err := client.WriteFileAtContext(context.Background(), "demo", "private/secret.txt", 0, []byte("secret data")); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	if err := client.ChmodContext(context.Background(), "demo", "", 0o755); err != nil {
		t.Fatalf("chmod root: %v", err)
	}
	if err := client.ChmodContext(context.Background(), "demo", "public", 0o777); err != nil {
		t.Fatalf("chmod public dir: %v", err)
	}
	if err := client.ChmodContext(context.Background(), "demo", "public/note.txt", 0o666); err != nil {
		t.Fatalf("chmod public file: %v", err)
	}
	if err := client.ChmodContext(context.Background(), "demo", "shared", 0o755); err != nil {
		t.Fatalf("chmod shared dir: %v", err)
	}
	if err := client.ChownContext(context.Background(), "demo", "shared", 0, 2001); err != nil {
		t.Fatalf("chown shared dir: %v", err)
	}
	if err := client.ChmodContext(context.Background(), "demo", "shared/readme.txt", 0o640); err != nil {
		t.Fatalf("chmod shared file: %v", err)
	}
	if err := client.ChownContext(context.Background(), "demo", "shared/readme.txt", 0, 2001); err != nil {
		t.Fatalf("chown shared file: %v", err)
	}
	if err := client.ChmodContext(context.Background(), "demo", "private", 0o700); err != nil {
		t.Fatalf("chmod private dir: %v", err)
	}
	if err := client.ChownContext(context.Background(), "demo", "private", 0, 0); err != nil {
		t.Fatalf("chown private dir: %v", err)
	}
	if err := client.ChmodContext(context.Background(), "demo", "private/secret.txt", 0o600); err != nil {
		t.Fatalf("chmod secret file: %v", err)
	}
	if err := client.ChownContext(context.Background(), "demo", "private/secret.txt", 0, 0); err != nil {
		t.Fatalf("chown secret file: %v", err)
	}
}

func mustJSONMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return data
}
