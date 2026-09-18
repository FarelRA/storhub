package rest

import (
	"net/http"
	"strings"
	"testing"
)

// TestContentRangeEdges pins every Range branch: suffix, open-ended,
// clamped, and full reads answer 206 with an exact Content-Range, while
// multi-range, bad units, unparseable values, and empty-file reads answer
// 416 carrying Content-Range bytes */size and the range_not_satisfiable
// code. If-None-Match still wins over Range (304, empty body), and HEAD
// with a Range answers headers only.
func TestContentRangeEdges(t *testing.T) {
	t.Parallel()
	newSeeded := func(t *testing.T) http.Handler {
		t.Helper()
		handler, err := newHandlerForClient(newFakeRESTClient(), Options{AllowAnonymous: true})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
		mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("0123456789"), nil, http.StatusCreated)
		mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/empty.txt", strings.NewReader(""), nil, http.StatusCreated)
		return handler
	}
	const base = "/api/v1/projects/demo/content?path=docs/f.txt"

	partial := []struct {
		name       string
		rangeHdr   string
		wantRange  string
		wantBody   string
		wantLength string
	}{
		{name: "suffix", rangeHdr: "bytes=-3", wantRange: "bytes 7-9/10", wantBody: "789", wantLength: "3"},
		{name: "suffix-larger-than-file", rangeHdr: "bytes=-100", wantRange: "bytes 0-9/10", wantBody: "0123456789", wantLength: "10"},
		{name: "open-ended", rangeHdr: "bytes=8-", wantRange: "bytes 8-9/10", wantBody: "89", wantLength: "2"},
		{name: "clamped-end", rangeHdr: "bytes=8-100", wantRange: "bytes 8-9/10", wantBody: "89", wantLength: "2"},
		{name: "single-byte", rangeHdr: "bytes=0-0", wantRange: "bytes 0-0/10", wantBody: "0", wantLength: "1"},
	}
	for _, tc := range partial {
		t.Run("partial/"+tc.name, func(t *testing.T) {
			t.Parallel()
			handler := newSeeded(t)
			resp := mustRequest(t, handler, http.MethodGet, base, nil, map[string]string{"Range": tc.rangeHdr}, http.StatusPartialContent)
			if got := resp.Header.Get("Content-Range"); got != tc.wantRange {
				t.Fatalf("Content-Range: want %q, got %q", tc.wantRange, got)
			}
			if got := resp.Header.Get("Content-Length"); got != tc.wantLength {
				t.Fatalf("Content-Length: want %q, got %q", tc.wantLength, got)
			}
			if got := string(readBody(t, resp)); got != tc.wantBody {
				t.Fatalf("body: want %q, got %q", tc.wantBody, got)
			}
		})
	}

	unsatisfiable := []struct {
		name     string
		path     string
		rangeHdr string
	}{
		{name: "start-past-eof", path: base, rangeHdr: "bytes=10-12"},
		{name: "multi-range", path: base, rangeHdr: "bytes=0-1,3-4"},
		{name: "bad-unit", path: base, rangeHdr: "items=0-1"},
		{name: "unparseable", path: base, rangeHdr: "bytes=abc"},
		{name: "inverted", path: base, rangeHdr: "bytes=5-3"},
		{name: "empty-file", path: "/api/v1/projects/demo/content?path=docs/empty.txt", rangeHdr: "bytes=0-"},
	}
	for _, tc := range unsatisfiable {
		t.Run("unsatisfiable/"+tc.name, func(t *testing.T) {
			t.Parallel()
			handler := newSeeded(t)
			resp := mustRequest(t, handler, http.MethodGet, tc.path, nil, map[string]string{"Range": tc.rangeHdr}, http.StatusRequestedRangeNotSatisfiable)
			size := "10"
			if tc.name == "empty-file" {
				size = "0"
			}
			if got := resp.Header.Get("Content-Range"); got != "bytes */"+size {
				t.Fatalf("Content-Range: want %q, got %q", "bytes */"+size, got)
			}
			assertErrorCode(t, resp, "range_not_satisfiable")
		})
	}

	t.Run("if-none-match-wins-over-range", func(t *testing.T) {
		t.Parallel()
		handler := newSeeded(t)
		nodeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/f.txt", nil, nil, http.StatusOK)
		var node nodeResponse
		decodeJSONBody(t, nodeResp, &node)
		resp := mustRequest(t, handler, http.MethodGet, base, nil,
			map[string]string{"Range": "bytes=0-3", "If-None-Match": node.ETag}, http.StatusNotModified)
		if body := strings.TrimSpace(string(readBody(t, resp))); body != "" {
			t.Fatalf("expected empty 304 body, got %q", body)
		}
	})

	t.Run("head-with-range", func(t *testing.T) {
		t.Parallel()
		handler := newSeeded(t)
		resp := mustRequest(t, handler, http.MethodHead, base, nil, map[string]string{"Range": "bytes=2-4"}, http.StatusPartialContent)
		if got := resp.Header.Get("Content-Range"); got != "bytes 2-4/10" {
			t.Fatalf("Content-Range: want %q, got %q", "bytes 2-4/10", got)
		}
		if body := string(readBody(t, resp)); body != "" {
			t.Fatalf("HEAD must have empty body, got %q", body)
		}
	})
}

// TestListingOrderIsStable pins stable pagination's foundation: repeated
// children and revision listings are byte-identical across reads and
// across unrelated mutations, so cursor-style clients never skip or
// repeat entries.
func TestListingOrderIsStable(t *testing.T) {
	t.Parallel()
	handler, err := newHandlerForClient(newFakeRESTClient(), Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	for _, name := range []string{"docs/c.txt", "docs/a.txt", "docs/b.txt"} {
		mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path="+name, strings.NewReader("x"), nil, http.StatusCreated)
	}
	children := func() string {
		resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/children?path=docs", nil, nil, http.StatusOK)
		return string(readBody(t, resp))
	}
	first, second := children(), children()
	if first != second {
		t.Fatalf("children listing unstable across reads:\n%s\n%s", first, second)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/z.txt", strings.NewReader("x"), nil, http.StatusCreated)
	third := children()
	if first == third {
		t.Fatalf("children listing did not reflect the new file")
	}
	if fourth := children(); fourth != third {
		t.Fatalf("children listing unstable after mutation:\n%s\n%s", third, fourth)
	}
	revisions := func() string {
		resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/revisions", nil, nil, http.StatusOK)
		return string(readBody(t, resp))
	}
	if a, b := revisions(), revisions(); a != b {
		t.Fatalf("revisions listing unstable across reads:\n%s\n%s", a, b)
	}
}

// TestPreconditionRevisionTokenThreadsToStorage pins the CAS flavor on the
// newly funneled endpoints: a current-revision If-Match proceeds (and
// reaches storage with the token), while a moved revision fails 412.
func TestPreconditionRevisionTokenThreadsToStorage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		method string
		target string
		body   any
	}{
		{name: "unlink", method: http.MethodPost, target: "/api/v1/projects/demo/ops/unlink", body: pathRequest{Path: "docs/f.txt"}},
		{name: "rename", method: http.MethodPost, target: "/api/v1/projects/demo/ops/rename", body: renameRequest{OldPath: "docs/f.txt", NewPath: "docs/g.txt"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newFakeRESTClient()
			handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
			if err != nil {
				t.Fatalf("new handler: %v", err)
			}
			mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
			mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("hello"), nil, http.StatusCreated)
			client.SetRevision("rev-1")
			current := map[string]string{"If-Match": `"rev-1"`}
			want := http.StatusOK
			if tc.name == "unlink" {
				want = http.StatusNoContent
			}
			mustJSONRequestWithHeaders(t, handler, tc.method, tc.target, tc.body, current, want)
			// The revision moved under the client: the same token is stale now.
			client.SetRevision("rev-2")
			mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("hello"), nil, http.StatusCreated)
			resp := mustJSONRequestWithHeaders(t, handler, tc.method, tc.target, tc.body, current, http.StatusPreconditionFailed)
			assertErrorCode(t, resp, "precondition_failed")
		})
	}
}
