package rest

// rest_auth_identity_test.go: client identity plumbing, auth middleware
// record checks, credential transport rules, and login work factor.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// ---- The clientFor wrapper fails closed; wrappers assert Client ----

func TestClientForFailsClosedOnForeignContextValue(t *testing.T) {
	t.Parallel()
	h := &restHandler{
		client: newFakeRESTClient(),
		opts:   DefaultOptions(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/demo", nil)
	req = req.WithContext(context.WithValue(req.Context(), clientCtxKey, "notaclient"))
	got, err := h.clientFor(req)
	if err == nil {
		t.Fatal("clientFor must fail closed with an error on a foreign context value, not fall back to the raw client")
	}
	if got != nil {
		t.Fatal("clientFor must not return a client on a foreign context value")
	}
}

func TestClientForKeepsAnonymousRawClient(t *testing.T) {
	t.Parallel()
	h := &restHandler{
		client: newFakeRESTClient(),
		opts:   DefaultOptions(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/demo", nil)
	got, err := h.clientFor(req)
	if err != nil {
		t.Fatalf("clientFor on an absent context value: %v", err)
	}
	if got != h.client {
		t.Fatal("an absent context value (anonymous mode) must keep the configured client")
	}
}

// ---- Auth tokens are revocable via the live user record ----

func TestAuthMiddlewareRechecksUserRecord(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	auth, err := newAuthenticator(AuthOptions{
		TokenSigningKey: []byte("testsigningkey0123456789abcdef0000"),
		Users: []User{
			{Username: "alice", PasswordHash: testHashAlicePass, UID: 1001, PrimaryGID: 2001},
			{Username: "root", PasswordHash: testHashRootPass, UID: 0, PrimaryGID: 0, Admin: true},
		},
	})
	if err != nil {
		t.Fatalf("new authenticator: %v", err)
	}
	h := &restHandler{client: client, opts: DefaultOptions(), shares: &shareRegistry{items: map[string]*shareRecord{}}}
	var seenPrincipal *restPrincipal
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ac, ok := r.Context().Value(clientCtxKey).(*authorizedClient); ok {
			seenPrincipal = ac.principal
		}
		w.WriteHeader(http.StatusTeapot)
	})
	mw := h.authMiddleware(auth, "/api/v1")

	serve := func(token string) int {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/demo", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		mw(inner).ServeHTTP(rec, req)
		return rec.Code
	}

	_, aliceToken, _, err := auth.login("alice", "alicepass")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if code := serve(aliceToken); code != http.StatusTeapot {
		t.Fatalf("live user must pass, got %d", code)
	}

	// Disabled account: the still-valid JWT must stop working.
	user := auth.users["alice"]
	user.Disabled = true
	auth.users["alice"] = user
	if code := serve(aliceToken); code != http.StatusUnauthorized {
		t.Fatalf("disabled user kept access: %d", code)
	}
	user.Disabled = false
	auth.users["alice"] = user

	// Demotion: the live record's admin bit wins over stale claims.
	_, rootToken, _, err := auth.login("root", "rootpass")
	if err != nil {
		t.Fatalf("root login: %v", err)
	}
	if code := serve(rootToken); code != http.StatusTeapot || seenPrincipal == nil || !seenPrincipal.Admin {
		t.Fatalf("admin token must carry admin, code=%d principal=%+v", code, seenPrincipal)
	}
	user = auth.users["root"]
	user.Admin = false
	auth.users["root"] = user
	if code := serve(rootToken); code != http.StatusTeapot {
		t.Fatalf("demoted user must still pass auth, got %d", code)
	}
	if seenPrincipal == nil || seenPrincipal.Admin {
		t.Fatalf("stale admin claim honored: %+v", seenPrincipal)
	}

	// Removed account: token dies with the record.
	delete(auth.users, "alice")
	if code := serve(aliceToken); code != http.StatusUnauthorized {
		t.Fatalf("removed user kept access: %d", code)
	}
}

// ---- Auth JWTs are not accepted via ?token= ----

func TestAuthTokenRejectedViaQuery(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)
	bearer := loginBearer(t, handler, "alice", "alicepass")
	token := strings.TrimPrefix(bearer, "Bearer ")

	mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo", nil, map[string]string{"Authorization": bearer}, http.StatusOK)
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo?token="+url.QueryEscape(token), nil, nil, http.StatusUnauthorized)
	assertErrorCode(t, resp, "unauthorized")
}

func TestShareTokenStillAcceptedViaQuery(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)
	root := loginBearer(t, handler, "root", "rootpass")
	share := mustDecodeShare(t, mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared"})),
		map[string]string{"Authorization": root, "Content-Type": "application/json"}, http.StatusCreated))

	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=shared/readme.txt&token="+url.QueryEscape(share.Token), nil, nil, http.StatusOK)
	if body := string(readBody(t, resp)); body != "shared data" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// ---- Constant-work login path ----

func TestLoginConstantWorkForUnknownUsers(t *testing.T) {
	t.Parallel()
	auth, err := newAuthenticator(AuthOptions{
		TokenSigningKey: []byte("0123456789abcdef0123456789abcdef"),
		Users:           []User{{Username: "alice", Password: "alicepass", UID: 1, PrimaryGID: 1}},
	})
	if err != nil {
		t.Fatalf("new authenticator: %v", err)
	}
	var calls int
	var lastHash string
	auth.verify = func(password, encoded string) bool {
		calls++
		lastHash = encoded
		return verifyPassword(password, encoded)
	}

	_, _, _, knownErr := auth.login("alice", "wrongpassword")
	if knownErr == nil {
		t.Fatal("wrong password must fail")
	}
	if calls != 1 {
		t.Fatalf("known-user path must perform exactly one bcrypt verify, got %d", calls)
	}

	calls, lastHash = 0, ""
	_, _, _, unknownErr := auth.login("mallorynotauser", "whatever")
	if unknownErr == nil {
		t.Fatal("unknown user must fail")
	}
	if calls != 1 {
		t.Fatalf("unknown-user path skipped the dummy bcrypt verify (timing oracle): %d verifies", calls)
	}
	if lastHash == "" {
		t.Fatal("unknown-user path must verify against the dummy hash")
	}
	if knownErr.Error() != unknownErr.Error() {
		t.Fatalf("error text differs between branches: %q vs %q", knownErr, unknownErr)
	}

	calls = 0
	if _, _, _, err := auth.login("alice", "alicepass"); err != nil || calls != 1 {
		t.Fatalf("correct login: err=%v verifies=%d", err, calls)
	}
}
