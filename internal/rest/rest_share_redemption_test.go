package rest

// rest_share_redemption_test.go: share issuance, redemption identity, path
// scoping, and share path rules over REST.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// ---- GET /projects/{p}/shares/{id} must not leak the token ----

func TestProjectShareGetDoesNotLeakToken(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	bearer := loginBearer(t, handler, "root", "rootpass")
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

// ---- Share management is creator ∪ admin ----

func TestShareManagementRequiresOwnershipOrAdmin(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	seedProjectForAuth(t, client)
	handler := newAuthedTestHandler(t, client)

	alice := loginBearer(t, handler, "alice", "alicepass")
	root := loginBearer(t, handler, "root", "rootpass")

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
