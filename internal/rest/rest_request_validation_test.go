package rest

// rest_request_validation_test.go: input validation, error mapping, and
// content type guards over REST.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

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
	for _, leak := range []string{"10.0.3.7", "srv9", "/var/lib", "exploded"} {
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
