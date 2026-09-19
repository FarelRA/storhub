package rest

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestShareTTLDefaultClamp ensures the MaxShareTTL clamp is armed even when
// the embedder never sets it: a zero option value must mean "default bound",
// never "unbounded".
func TestShareTTLDefaultClamp(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{
		AllowAnonymous:  true,
		ShareSigningKey: []byte("test-signing-key-at-least-32-bytes!!"),
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("x"), nil, http.StatusCreated)

	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		shareRequest{Path: "docs/f.txt", ExpiresInSeconds: 100 * 365 * 24 * 3600}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, resp, &share)

	expiresAt, err := time.Parse(time.RFC3339, share.ExpiresAt)
	if err != nil {
		t.Fatalf("parse expires_at %q: %v", share.ExpiresAt, err)
	}
	bound := Options{}.withDefaults().MaxShareTTL
	if bound <= 0 {
		t.Fatal("withDefaults must arm a positive MaxShareTTL")
	}
	if until := time.Until(expiresAt); until > bound+time.Minute {
		t.Fatalf("share lifetime %v exceeds default clamp %v", until, bound)
	}
}

// TestShareKeyDerivedFromAuthSigningKey pins the zero-config behavior an
// auth-file deployment relies on: with no explicit ShareSigningKey, one is
// derived deterministically from the auth token signing key, so share
// creation works immediately AND existing links keep verifying after a
// restart or across handler instances.
func TestShareKeyDerivedFromAuthSigningKey(t *testing.T) {
	t.Parallel()
	auth := &AuthOptions{TokenSigningKey: []byte("0123456789abcdef0123456789abcdeg"), Users: []User{{Username: "admin", PasswordHash: testHashPass, UID: 0, PrimaryGID: 0, Admin: true}}}

	newAuthed := func(t *testing.T, client *fakeRESTClient) (http.Handler, string) {
		t.Helper()
		handler, err := newHandlerForClient(client, Options{Auth: auth})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		login := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/auth/login", restLoginRequest{Username: "admin", Password: "pass"}, http.StatusOK)
		var session restLoginResponse
		decodeJSONBody(t, login, &session)
		if session.Token == "" {
			t.Fatal("login returned no token")
		}
		return handler, "Bearer " + session.Token
	}

	fake := newFakeRESTClient()
	if err := fake.MkdirContext(context.Background(), "demo", "docs"); err != nil {
		t.Fatalf("seed docs dir: %v", err)
	}
	first, bearer := newAuthed(t, fake)
	resp := mustRequest(t, first, http.MethodPost, "/api/v1/projects/demo/shares", strings.NewReader(`{"path":"docs"}`), map[string]string{"Authorization": bearer}, http.StatusCreated)
	var share shareResponse
	decodeJSONBody(t, resp, &share)
	if share.ID == "" {
		t.Fatal("share creation must succeed when the key is derived from the auth signing key")
	}
	if share.Token == "" {
		t.Fatal("creation response must carry the signed share token")
	}

	// A brand-new handler over the same backend (simulated restart) must
	// still VERIFY the signed share token - that verification is exactly
	// what keeps ?share= links alive across process restarts.
	second, _ := newAuthed(t, fake)
	mustRequest(t, second, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs",
		nil, map[string]string{"Authorization": "Bearer " + share.Token}, http.StatusOK)

	// Anonymous deployments without any key still refuse to sign shares.
	anonFake := newFakeRESTClient()
	if err := anonFake.MkdirContext(context.Background(), "demo", "docs"); err != nil {
		t.Fatalf("seed anon docs dir: %v", err)
	}
	anon, err := newHandlerForClient(anonFake, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new anon handler: %v", err)
	}
	errResp := mustJSONRequest(t, anon, http.MethodPost, "/api/v1/projects/demo/shares", shareRequest{Path: "docs"}, http.StatusForbidden)
	body := readBody(t, errResp)
	if !strings.Contains(string(body), "share signing key not configured") {
		t.Fatalf("expected explicit not-configured error, got: %s", body)
	}
}

func TestSweepExpiredSharesBoundsRegistry(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler := &restHandler{
		client: client,
		opts:   Options{AllowAnonymous: true}.withDefaults(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	now := time.Now()
	for i := 0; i < shareSweepThreshold+10; i++ {
		handler.shares.items[fmt.Sprintf("stale-%d", i)] = &shareRecord{
			ID: "stale", Project: "demo", ExpiresAt: now.Add(-time.Hour),
		}
	}
	handler.sweepExpiredShares()
	if got := len(handler.shares.items); got != 0 {
		t.Fatalf("expected all expired records swept, got %d", got)
	}
}

// seedShareEscapeTree builds a share lane where public/ holds an absolute
// escape link, a relative escape link, a chained escape (in-scope hop to
// an escaping hop), an in-scope link, and a dangling in-scope link,
// alongside the outside world-readable target they point at.
func seedShareEscapeTree(t *testing.T) (*fakeRESTClient, *restrictedClient) {
	t.Helper()
	ctx := context.Background()
	client := newFakeRESTClient()
	for _, dir := range []string{"public", "private"} {
		if err := client.MkdirContext(ctx, "demo", dir); err != nil {
			t.Fatalf("seed %s: %v", dir, err)
		}
	}
	if _, err := client.CreateFileContext(ctx, "demo", "private/keys.txt"); err != nil {
		t.Fatalf("seed keys: %v", err)
	}
	if _, err := client.WriteFileAtContext(ctx, "demo", "private/keys.txt", 0, []byte("top-secret")); err != nil {
		t.Fatalf("write keys: %v", err)
	}
	if _, err := client.CreateFileContext(ctx, "demo", "public/doc.txt"); err != nil {
		t.Fatalf("seed doc: %v", err)
	}
	for link, target := range map[string]string{
		"public/abs":   "/private/keys.txt",
		"public/rel":   "../private/keys.txt",
		"public/mid":   "doc.txt",
		"public/chain": "hop",
		"public/hop":   "/private/keys.txt",
		"public/inner": "doc.txt",
		"public/dead":  "gone.txt",
	} {
		if _, err := client.SymlinkContext(ctx, "demo", target, link); err != nil {
			t.Fatalf("seed link %s: %v", link, err)
		}
	}
	return client, newRestrictedClient(client, "demo", "public")
}

func assertShareDenied(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: escaping symlink served, want denial", what)
	}
	if !strings.Contains(err.Error(), "not shared") && !strings.Contains(err.Error(), "symbolic links") {
		t.Fatalf("%s: wrong denial: %v", what, err)
	}
}

// A symlink inside the shared subtree pointing outside must not serve
// outside bytes, metadata, target strings, or xattrs to share visitors.
func TestShareSymlinkEscapeDenied(t *testing.T) {
	t.Parallel()
	_, share := seedShareEscapeTree(t)
	ctx := context.Background()
	for _, link := range []string{"public/abs", "public/rel", "public/chain"} {
		if _, err := share.ReadFileAtContext(ctx, "demo", link, 0, 64); err == nil || !strings.Contains(err.Error(), "not shared") {
			t.Fatalf("read %s: want scope denial, got %v", link, err)
		}
		assertShareDenied(t, func() error { _, err := share.StatPathContext(ctx, "demo", link); return err }(), "stat "+link)
		assertShareDenied(t, func() error { _, err := share.ReadlinkContext(ctx, "demo", link); return err }(), "readlink "+link)
		assertShareDenied(t, func() error { _, err := share.GetXAttrContext(ctx, "demo", link, "user.a"); return err }(), "getxattr "+link)
		assertShareDenied(t, func() error { _, err := share.ListXAttrContext(ctx, "demo", link); return err }(), "listxattr "+link)
		assertShareDenied(t, func() error { _, err := share.ReadDirContext(ctx, "demo", link); return err }(), "readdir "+link)
	}
}

// In-scope links keep working, and a dangling in-scope link still stats
// as itself (only escapes fail closed).
func TestShareSymlinkInScopeAllowed(t *testing.T) {
	t.Parallel()
	_, share := seedShareEscapeTree(t)
	ctx := context.Background()
	if target, err := share.ReadlinkContext(ctx, "demo", "public/inner"); err != nil || target != "doc.txt" {
		t.Fatalf("in-scope readlink: %q %v", target, err)
	}
	if entry, err := share.StatPathContext(ctx, "demo", "public/inner"); err != nil || !entry.IsSymlink {
		t.Fatalf("in-scope stat: %+v %v", entry, err)
	}
	if _, err := share.ReadFileAtContext(ctx, "demo", "public/inner", 0, 64); err != nil {
		t.Fatalf("in-scope read delegates: %v", err)
	}
	if entry, err := share.StatPathContext(ctx, "demo", "public/dead"); err != nil || !entry.IsSymlink {
		t.Fatalf("dangling in-scope stat must stay visible: %+v %v", entry, err)
	}
}
