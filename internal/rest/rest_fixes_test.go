package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

const testShareKey = "abcdef0123456789abcdef0123456789"

// ---- GET /projects/{p}/shares/{id} must not leak the token ----

func TestProjectShareGetDoesNotLeakToken(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	bearer := loginBearer(t, handler, "root", "root-pass")
	auth := map[string]string{"Authorization": bearer, "Content-Type": "application/json"}

	created := mustDecodeShare(t, mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "private/secret.txt"})), auth, http.StatusCreated))
	if created.Token == "" {
		t.Fatal("creation must carry the token")
	}

	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/shares/"+created.ID, nil, auth, http.StatusOK)
	body := string(readBody(t, resp))
	if strings.Contains(body, created.Token) {
		t.Fatal("single-get leaked the signed share token")
	}
	var got shareResponse
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Token != "" || got.URL != "" || got.DownloadURL != "" {
		t.Fatalf("single-get must not carry credential or mintable URLs: %+v", got)
	}
	if got.ID != created.ID || got.Path != "private/secret.txt" {
		t.Fatalf("unexpected share view: %+v", got)
	}
}

// ---- The clientFor wrapper fails closed; wrappers assert Client ----

func TestClientForFailsClosedOnForeignContextValue(t *testing.T) {
	t.Parallel()
	h := &restHandler{
		client: newFakeRESTClient(),
		opts:   DefaultOptions(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/demo", nil)
	req = req.WithContext(context.WithValue(req.Context(), clientCtxKey, "not-a-client"))
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

// ---- Redemption routes run as nobody, scoped ----

func TestShareRedemptionRunsAsNobody(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hello world"), nil, http.StatusCreated)
	share := mustDecodeShare(t, mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "hello.txt"}, http.StatusCreated))

	_ = client.takeSeenIdentities() // discard setup noise
	resp := mustRequest(t, handler, http.MethodGet, share.DownloadURL, nil, nil, http.StatusOK)
	if body := string(readBody(t, resp)); body != "hello world" {
		t.Fatalf("unexpected download body: %q", body)
	}
	assertRedemptionIdentities(t, client)
}

func TestShareDeriveRunsAsNobody(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("data"), nil, http.StatusCreated)
	parent := mustDecodeShare(t, mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "docs"}, http.StatusCreated))

	_ = client.takeSeenIdentities()
	derive := mustRequest(t, handler, http.MethodPost,
		"/api/v1/shares/"+parent.ID+"/derive?token="+url.QueryEscape(parent.Token),
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "docs/f.txt"})),
		map[string]string{"Content-Type": "application/json"}, http.StatusCreated)
	child := mustDecodeShare(t, derive)
	if child.Path != "docs/f.txt" {
		t.Fatalf("unexpected derived share: %+v", child)
	}
	assertRedemptionIdentities(t, client)
}

// Derive also accepts the redemption spelling: the signed parent JWT in
// the path segment with no query or header credential.
func TestShareDeriveAcceptsJWTInPath(t *testing.T) {
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
		"/api/v1/shares/"+url.PathEscape(parent.Token)+"/derive",
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "docs/f.txt"})),
		map[string]string{"Content-Type": "application/json"}, http.StatusCreated)
	child := mustDecodeShare(t, derive)
	if child.Path != "docs/f.txt" {
		t.Fatalf("unexpected derived share: %+v", child)
	}

	// A mismatched ID in the path with a valid token credential stays denied.
	mustRequest(t, handler, http.MethodPost,
		"/api/v1/shares/deadbeef/derive?token="+url.QueryEscape(parent.Token),
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "docs/f.txt"})),
		map[string]string{"Content-Type": "application/json"}, http.StatusForbidden)
}

func assertRedemptionIdentities(t *testing.T, client *fakeRESTClient) {
	t.Helper()
	ids := client.takeSeenIdentities()
	if len(ids) == 0 {
		t.Fatal("redemption never reached the storage layer")
	}
	for i, id := range ids {
		if id.UID != nobodyUID || id.GID != nobodyGID || id.Admin {
			t.Fatalf("redemption call %d ran as %+v; share visitors must be the unprivileged nobody identity", i, id)
		}
	}
}

func TestShareRedemptionStaysScopedToSharedPath(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "other"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("x"), nil, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=other/g.txt", strings.NewReader("y"), nil, http.StatusCreated)
	share := mustDecodeShare(t, mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "docs/f.txt"}, http.StatusCreated))

	resp := mustRequest(t, handler, http.MethodGet, share.DownloadURL+"&path=other%2Fg.txt", nil, nil, http.StatusForbidden)
	assertErrorCode(t, resp, "forbidden")
}

// ---- Input validation answers 400, never storage 500s ----

func TestInputValidationReturnsBadRequest(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("hello"), nil, http.StatusCreated)

	cases := []struct {
		name   string
		method string
		target string
		body   any
	}{
		{"mkdir empty path", http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "  "}},
		{"create-file empty path", http.MethodPost, "/api/v1/projects/demo/ops/create", pathRequest{Path: ""}},
		{"rmdir empty path", http.MethodPost, "/api/v1/projects/demo/ops/rmdir", pathRequest{Path: ""}},
		{"unlink empty path", http.MethodPost, "/api/v1/projects/demo/ops/unlink", pathRequest{Path: ""}},
		{"rename empty new", http.MethodPost, "/api/v1/projects/demo/ops/rename", renameRequest{OldPath: "docs/f.txt", NewPath: ""}},
		{"link empty existing", http.MethodPost, "/api/v1/projects/demo/ops/link", linkRequest{ExistingPath: "", NewPath: "docs/l.txt"}},
		{"symlink empty target", http.MethodPost, "/api/v1/projects/demo/ops/symlink", symlinkRequest{Target: "", LinkPath: "docs/s.txt"}},
		{"chmod empty path", http.MethodPost, "/api/v1/projects/demo/ops/chmod", chmodRequest{Path: "", Mode: 0o644}},
		{"chown empty path", http.MethodPost, "/api/v1/projects/demo/ops/chown", chownRequest{Path: "", UID: 1, GID: 1}},
		{"utimes empty path", http.MethodPost, "/api/v1/projects/demo/ops/utimes", utimesRequest{Path: "", Atime: time.Unix(0, 1), Mtime: time.Unix(0, 1)}},
		{"utimes zero times", http.MethodPost, "/api/v1/projects/demo/ops/utimes", utimesRequest{Path: "docs/f.txt"}},
		{"rollback empty sha", http.MethodPost, "/api/v1/projects/demo/ops/rollback", rollbackRequest{}},
		{"rollback branch sha", http.MethodPost, "/api/v1/projects/demo/ops/rollback", rollbackRequest{CommitSHA: "refs/heads/main"}},
		{"rollback short sha", http.MethodPost, "/api/v1/projects/demo/ops/rollback", rollbackRequest{CommitSHA: "deadbe"}},
		{"revert-path empty sha", http.MethodPost, "/api/v1/projects/demo/ops/revert", revertPathRequest{Path: "docs/f.txt"}},
		{"prune unknown scope", http.MethodPost, "/api/v1/projects/demo/ops/prune", pruneRequest{Scope: "banana"}},
		{"prune negative keep", http.MethodPost, "/api/v1/projects/demo/ops/prune", pruneRequest{Scope: "objects", Keep: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := mustJSONRequest(t, handler, tc.method, tc.target, tc.body, http.StatusBadRequest)
			assertErrorCode(t, resp, "bad_request")
		})
	}

	patchCases := []struct {
		name   string
		target string
	}{
		{"negative offset", "/api/v1/projects/demo/content?path=docs/f.txt&op=write&offset=-5"},
		{"negative size", "/api/v1/projects/demo/content?path=docs/f.txt&op=truncate&size=-1"},
		{"negative patch offset", "/api/v1/projects/demo/content?path=docs/f.txt&op=patch&offset=-1&delete_size=0"},
		{"negative delete_size", "/api/v1/projects/demo/content?path=docs/f.txt&op=patch&offset=0&delete_size=-2"},
		{"empty path", "/api/v1/projects/demo/content?path=&op=append"},
	}
	for _, tc := range patchCases {
		t.Run("patch "+tc.name, func(t *testing.T) {
			resp := mustRequest(t, handler, http.MethodPatch, tc.target, strings.NewReader("x"), nil, http.StatusBadRequest)
			assertErrorCode(t, resp, "bad_request")
		})
	}
}

func TestFakePruneRejectsUnknownScope(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	if err := client.MkdirContext(context.Background(), "demo", "seed"); err != nil {
		t.Fatalf("seed project: %v", err)
	}
	if _, err := client.PruneContext(context.Background(), "demo", "banana", 0, true); err == nil || !strings.Contains(err.Error(), "unknown prune scope") {
		t.Fatalf("fake must mirror the storage scope contract, got: %v", err)
	}
	if _, err := client.PruneContext(context.Background(), "demo", "objects", 0, true); err != nil {
		t.Fatalf("valid scope must pass: %v", err)
	}
}

// ---- Mapped 5xx-class errors never echo internal wording ----

func TestInternalErrorsAreNotEchoed(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	client.failNextReplace(errors.New("dial tcp 10.0.3.7:8888 /var/lib/storhub/internal/srv-9 exploded"))
	resp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=f.txt", strings.NewReader("payload"), nil, http.StatusInternalServerError)
	body := string(readBody(t, resp))
	for _, leak := range []string{"10.0.3.7", "srv-9", "/var/lib", "exploded"} {
		if strings.Contains(body, leak) {
			t.Fatalf("internal detail %q leaked to the client: %s", leak, body)
		}
	}
	if !strings.Contains(body, "internal server error") {
		t.Fatalf("expected generic message, got: %s", body)
	}
}

func TestValidationErrorsKeepTheirMessage(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/prune", pruneRequest{Scope: "banana"}, http.StatusBadRequest)
	body := string(readBody(t, resp))
	if !strings.Contains(body, "objects") || !strings.Contains(body, "assets") {
		t.Fatalf("400-class validation guidance must survive sanitization: %s", body)
	}
}

// ---- The /content route never renders stored bytes as active content ----

func TestContentReadNeverInlineExecutable(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=evil.html", strings.NewReader("<script>alert(1)</script>"), nil, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=pic.svg", strings.NewReader("<svg onload=alert(1)></svg>"), nil, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=notes.txt", strings.NewReader("plain"), nil, http.StatusCreated)

	for _, target := range []string{"/api/v1/projects/demo/content?path=evil.html", "/api/v1/projects/demo/content?path=pic.svg"} {
		resp := mustRequest(t, handler, http.MethodGet, target, nil, nil, http.StatusOK)
		if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("%s: nosniff missing", target)
		}
		if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "html") || strings.Contains(ct, "svg") {
			t.Fatalf("%s: active content type served inline: %q", target, ct)
		}
		_ = readBody(t, resp)
	}
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=notes.txt", nil, nil, http.StatusOK)
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatal("nosniff missing on text response")
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("text/plain must stay inline, got %q", ct)
	}
	_ = readBody(t, resp)
}

func TestDetectContentTypeAllowlist(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ file, want string }{
		{"a.html", "application/octet-stream"},
		{"a.svg", "application/octet-stream"},
		{"a.js", "application/octet-stream"},
		{"a.txt", "text/plain; charset=utf-8"},
		{"a.png", "image/png"},
		{"a.unknownext", "application/octet-stream"},
	} {
		if got := detectContentType(tc.file); got != tc.want {
			t.Fatalf("detectContentType(%q) = %q, want %q", tc.file, got, tc.want)
		}
	}
}

// ---- Share management is creator ∪ admin ----

func TestShareManagementRequiresOwnershipOrAdmin(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	alice := loginBearer(t, handler, "alice", "alice-pass")
	root := loginBearer(t, handler, "root", "root-pass")

	aliceShare := mustDecodeShare(t, mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared/readme.txt"})),
		map[string]string{"Authorization": alice, "Content-Type": "application/json"}, http.StatusCreated))
	rootShare := mustDecodeShare(t, mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "private/secret.txt"})),
		map[string]string{"Authorization": root, "Content-Type": "application/json"}, http.StatusCreated))

	// Alice sees only her own share.
	var listing sharesResponse
	decodeJSONBody(t, mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/shares", nil, map[string]string{"Authorization": alice}, http.StatusOK), &listing)
	if len(listing.Shares) != 1 || listing.Shares[0].ID != aliceShare.ID {
		t.Fatalf("alice saw foreign shares: %+v", listing.Shares)
	}
	// Admin sees everything.
	var adminListing sharesResponse
	decodeJSONBody(t, mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/shares", nil, map[string]string{"Authorization": root}, http.StatusOK), &adminListing)
	if len(adminListing.Shares) != 2 {
		t.Fatalf("admin must see all project shares: %+v", adminListing.Shares)
	}

	// Alice cannot read or revoke root's share (404: no existence leak).
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/shares/"+rootShare.ID, nil, map[string]string{"Authorization": alice}, http.StatusNotFound)
	assertErrorCode(t, resp, "not_found")
	resp = mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/shares/"+rootShare.ID, nil, map[string]string{"Authorization": alice}, http.StatusNotFound)
	assertErrorCode(t, resp, "not_found")

	// The admin share survives the attempted revocation.
	mustRequest(t, handler, http.MethodGet, "/api/v1/shares/"+rootShare.Token, nil, nil, http.StatusOK)

	// Alice manages her own; admin can revoke alice's.
	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/shares/"+aliceShare.ID, nil, map[string]string{"Authorization": alice}, http.StatusNoContent)
	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/shares/"+rootShare.ID, nil, map[string]string{"Authorization": root}, http.StatusNoContent)
}

// ---- Auth tokens are revocable via the live user record ----

func TestAuthMiddlewareRechecksUserRecord(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	auth, err := newAuthenticator(AuthOptions{
		TokenSigningKey: []byte("test-signing-key-0123456789abcdef"),
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

	_, aliceToken, _, err := auth.login("alice", "alice-pass")
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
	_, rootToken, _, err := auth.login("root", "root-pass")
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
	bearer := loginBearer(t, handler, "alice", "alice-pass")
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
	root := loginBearer(t, handler, "root", "root-pass")
	share := mustDecodeShare(t, mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared"})),
		map[string]string{"Authorization": root, "Content-Type": "application/json"}, http.StatusCreated))

	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=shared/readme.txt&token="+url.QueryEscape(share.Token), nil, nil, http.StatusOK)
	if body := string(readBody(t, resp)); body != "shared data" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// ---- Revocation state is per-handler and self-expiring ----

func TestRevocationIsPerHandler(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	newHandler := func() http.Handler {
		handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		return handler
	}
	first, second := newHandler(), newHandler()
	mustRequest(t, first, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hi"), nil, http.StatusCreated)
	share := mustDecodeShare(t, mustJSONRequest(t, first, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "hello.txt"}, http.StatusCreated))

	mustRequest(t, first, http.MethodDelete, "/api/v1/projects/demo/shares/"+share.ID, nil, nil, http.StatusNoContent)
	mustRequest(t, first, http.MethodGet, "/api/v1/shares/"+share.Token, nil, nil, http.StatusNotFound)
	// A sibling handler never saw the revocation: per-handler by design, no
	// package-level map shared across hubs or tests.
	mustRequest(t, second, http.MethodGet, "/api/v1/shares/"+share.Token, nil, nil, http.StatusOK)
}

func TestRevocationEntriesSelfExpire(t *testing.T) {
	t.Parallel()
	h := &restHandler{client: newFakeRESTClient(), opts: DefaultOptions(), shares: &shareRegistry{items: map[string]*shareRecord{}, revoked: map[string]time.Time{}}}
	h.revokeShare("gone", time.Now().Add(-time.Minute))
	if h.isRevoked("gone") {
		t.Fatal("revocation of an already-expired share must not report revoked")
	}
	if len(h.shares.revoked) != 0 {
		t.Fatalf("expired revocation entry must be dropped, %d remain", len(h.shares.revoked))
	}
	h.revokeShare("live", time.Now().Add(time.Hour))
	if !h.isRevoked("live") {
		t.Fatal("live revocation must be honored")
	}
}

// ---- UI guard precision + no directory listings ----

func TestSafeDistNameGuard(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, want string }{
		{"/_nuxt/index.html", "_nuxt/index.html"},
		{"/_nuxt/chunk..2.js", "_nuxt/chunk..2.js"}, // legitimate hashed name
		{"/favicon.svg", "favicon.svg"},
		{"/_nuxt/", ""},
		{"/_nuxt", "_nuxt"},
		{"/_nuxt/../secret", ""},
		{"/_nuxt/./x", ""},
		{"/_nuxt//x", ""},
		{"/_nuxt/..", ""},
		{"/a\\b", ""},
		{"/", ""},
	} {
		if got := safeDistName(tc.in); got != tc.want {
			t.Fatalf("safeDistName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestUIDirectoryListingDisabled(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	for _, target := range []string{"/_nuxt/", "/_nuxt", "/_nuxt/builds", "/_nuxt/builds/"} {
		resp := mustRequest(t, handler, http.MethodGet, target, nil, nil, http.StatusNotFound)
		_ = readBody(t, resp)
	}
}

// ---- Misc hardening ----

func TestBasePathSlashFallsBackToDefault(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, BasePath: "/"})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
}

func TestRecursiveQueryBoolParsing(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	for _, truthy := range []string{"true", "1", "yes", "TRUE"} {
		resp := mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs&recursive="+truthy, nil, nil, http.StatusNotImplemented)
		assertErrorCode(t, resp, "not_implemented")
	}
	resp := mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs&recursive=banana", nil, nil, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs&recursive=false", nil, nil, http.StatusNoContent)
}

func TestPruneAcceptsBodylessPOST(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/prune", nil, nil, http.StatusOK)
	var pr pruneResponse
	decodeJSONBody(t, resp, &pr)
	if pr.Scope != "assets" || pr.Status != "pruned" {
		t.Fatalf("unexpected prune response: %+v", pr)
	}
}

func TestPatchSuccessReturnsNode(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=f.txt", strings.NewReader("hello"), nil, http.StatusCreated)
	resp := mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=f.txt&op=append", strings.NewReader("!"), nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, resp, &node)
	if node.Entry == nil || node.Entry.Path != "f.txt" || node.Entry.Size != 6 {
		t.Fatalf("PATCH success must return the fresh node: %+v", node)
	}
}

func TestCanonicalSharePathRejectsEscapes(t *testing.T) {
	t.Parallel()
	for _, escaping := range []string{"../x", "docs/../../etc/passwd", "..", "a/../../b", "  ../x  "} {
		if got, err := canonicalSharePath(escaping); err == nil {
			t.Fatalf("canonicalSharePath(%q) = %q, want escape rejection", escaping, got)
		}
	}
	for _, fine := range []struct{ in, want string }{
		{"", ""},
		{"docs", "docs"},
		{"docs/../hello.txt", "hello.txt"},
		{"/docs/x", "docs/x"},
		{"./x", "x"},
		{"/..", ""}, // absolute ".." resolves to the project root, not an escape
	} {
		if got, err := canonicalSharePath(fine.in); err != nil || got != fine.want {
			t.Fatalf("canonicalSharePath(%q) = %q, %v; want %q", fine.in, got, err, fine.want)
		}
	}
}

// ---- Constant-work login path ----

func TestLoginConstantWorkForUnknownUsers(t *testing.T) {
	t.Parallel()
	auth, err := newAuthenticator(AuthOptions{
		TokenSigningKey: []byte("0123456789abcdef0123456789abcdef"),
		Users:           []User{{Username: "alice", Password: "alice-pass", UID: 1, PrimaryGID: 1}},
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

	_, _, _, knownErr := auth.login("alice", "wrong-password")
	if knownErr == nil {
		t.Fatal("wrong password must fail")
	}
	if calls != 1 {
		t.Fatalf("known-user path must perform exactly one bcrypt verify, got %d", calls)
	}

	calls, lastHash = 0, ""
	_, _, _, unknownErr := auth.login("mallory-not-a-user", "whatever")
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
	if _, _, _, err := auth.login("alice", "alice-pass"); err != nil || calls != 1 {
		t.Fatalf("correct login: err=%v verifies=%d", err, calls)
	}
}

// ---- HTTP-level authz gaps (rollback/revert-path/prune) ----

func TestOpsRoutesDenyNonAdminAndShareTokens(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)
	alice := loginBearer(t, handler, "alice", "alice-pass")
	root := loginBearer(t, handler, "root", "root-pass")

	share := mustDecodeShare(t, mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		bytes.NewBuffer(mustJSONMarshal(t, shareRequest{Path: "shared"})),
		map[string]string{"Authorization": root, "Content-Type": "application/json"}, http.StatusCreated))

	var revisions revisionsResponse
	decodeJSONBody(t, mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/revisions", nil, map[string]string{"Authorization": root}, http.StatusOK), &revisions)
	if len(revisions.Revisions) == 0 {
		t.Fatal("seed produced no revisions")
	}
	validSHA := revisions.Revisions[0].CommitSHA

	cases := []struct {
		name   string
		target string
		body   any
	}{
		{"rollback", "/api/v1/projects/demo/ops/rollback", rollbackRequest{CommitSHA: validSHA}},
		{"revert", "/api/v1/projects/demo/ops/revert", revertPathRequest{Path: "shared/readme.txt", CommitSHA: validSHA}},
		{"purge", "/api/v1/projects/demo/ops/prune", pruneRequest{Scope: "objects", DryRun: true}},
	}
	for _, tc := range cases {
		for _, caller := range []struct{ who, bearer string }{
			{"non-admin", alice},
			{"share-token", "Bearer " + share.Token},
		} {
			t.Run(tc.name+"/"+caller.who, func(t *testing.T) {
				mustJSONRequestWithBearer(t, handler, tc.target, tc.body, caller.bearer, http.StatusForbidden)
			})
		}
		// The admin gate must not over-deny: root passes all three.
		t.Run(tc.name+"/admin", func(t *testing.T) {
			resp := mustJSONRequestWithBearer(t, handler, tc.target, tc.body, root, http.StatusOK)
			_ = readBody(t, resp)
		})
	}
}

// ---- helpers ----

func loginBearer(t *testing.T, handler http.Handler, username, password string) string {
	t.Helper()
	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: username, Password: password}, http.StatusOK)
	var session restLoginResponse
	decodeJSONBody(t, resp, &session)
	if session.Token == "" {
		t.Fatal("login returned no token")
	}
	return "Bearer " + session.Token
}

func mustDecodeShare(t *testing.T, resp *http.Response) shareResponse {
	t.Helper()
	var share shareResponse
	decodeJSONBody(t, resp, &share)
	if share.ID == "" {
		t.Fatal("empty share id in response")
	}
	return share
}

func mustJSONRequestWithBearer(t *testing.T, handler http.Handler, target string, payload any, bearer string, wantStatus int) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewBuffer(mustJSONMarshal(t, payload)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", bearer)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Result().StatusCode != wantStatus {
		t.Fatalf("%s: got %d want %d body=%s", target, rec.Code, wantStatus, rec.Body.String())
	}
	return rec.Result()
}

// ---- Owner chgrp over REST follows CanChown; uid/gid omission keeps ----

func TestRESTOwnerChgrpAndKeepConvention(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	rootAuth := map[string]string{"Authorization": loginBearer(t, handler, "root", "root-pass"), "Content-Type": "application/json"}
	aliceAuth := map[string]string{"Authorization": loginBearer(t, handler, "alice", "alice-pass"), "Content-Type": "application/json"}

	// Hand shared/readme.txt to alice so she is the file owner.
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBuffer(mustJSONMarshal(t, chownRequest{Path: "shared/readme.txt", UID: 1001, GID: 2001})), rootAuth, http.StatusOK)

	// Omitted uid means keep: owner chgrp to her own group succeeds.
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBufferString(`{"path":"shared/readme.txt","gid":2001}`), aliceAuth, http.StatusOK)
	var doc nodeResponse
	decodeJSONBody(t, resp, &doc)
	if doc.Entry.UID != 1001 || doc.Entry.GID != 2001 {
		t.Fatalf("owner keep-chgrp changed ownership: %+v", doc.Entry)
	}

	// Explicit -1 on both ids is the keep spelling shared with the CLI.
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBufferString(`{"path":"shared/readme.txt","uid":-1,"gid":-1}`), aliceAuth, http.StatusOK)

	// The owner still cannot hand the file away or move it out of her groups.
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBuffer(mustJSONMarshal(t, chownRequest{Path: "shared/readme.txt", UID: 1002, GID: 2001})), aliceAuth, http.StatusForbidden)
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBufferString(`{"path":"shared/readme.txt","gid":4000}`), aliceAuth, http.StatusForbidden)

	// Out-of-range ids fail closed with 400, not 500.
	mustRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown",
		bytes.NewBufferString(`{"path":"shared/readme.txt","uid":4294967296}`), rootAuth, http.StatusBadRequest)
}
