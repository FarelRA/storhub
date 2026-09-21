package rest

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestSessionSubVerbsHonorSyncFlag pins FIX2: every mutating session verb
// (write, truncate, sync, link, relink) honors ?sync=1 by draining the
// handle's project, mirroring closeSession. Without the flag no drain runs.
func TestSessionSubVerbsHonorSyncFlag(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("hello"), nil, http.StatusCreated)

	drains := func() []string {
		t.Helper()
		client.mu.Lock()
		defer client.mu.Unlock()
		return append([]string(nil), client.drainCalls...)
	}

	fileHandle := openSessionHTTP(t, handler, "demo", "docs/f.txt", "r+")
	writePayload := map[string]any{"offset": 0, "data": base64.StdEncoding.EncodeToString([]byte("!"))}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+fileHandle+"/write?sync=1", writePayload, http.StatusOK)
	if got := drains(); len(got) != 1 || got[0] != "demo" {
		t.Fatalf("write?sync=1 must drain demo once, got %v", got)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+fileHandle+"/write", writePayload, http.StatusOK)
	if got := drains(); len(got) != 1 {
		t.Fatalf("write without sync must not drain, got %v", got)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+fileHandle+"/truncate?sync=1", map[string]any{"size": 2}, http.StatusOK)
	if got := drains(); len(got) != 2 {
		t.Fatalf("truncate?sync=1 must drain, got %v", got)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+fileHandle+"/sync?sync=1", map[string]any{}, http.StatusOK)
	if got := drains(); len(got) != 3 {
		t.Fatalf("sync?sync=1 must drain after commit, got %v", got)
	}

	scratch := openSessionHTTP(t, handler, "demo", "", "w+")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/link?sync=1", map[string]any{"path": "docs/a.txt"}, http.StatusOK)
	if got := drains(); len(got) != 4 {
		t.Fatalf("link?sync=1 must drain, got %v", got)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+scratch+"/relink?sync=1", map[string]any{"path": "docs/b.txt"}, http.StatusOK)
	if got := drains(); len(got) != 5 {
		t.Fatalf("relink?sync=1 must drain, got %v", got)
	}
}

// TestSessionEmptyReadOmitsRange pins R7: an empty session read (empty
// file, or offset at/past EOF) answers plain 200 with no Content-Range,
// never 206 with an invalid "bytes N-(N-1)/M" range.
func TestSessionEmptyReadOmitsRange(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/empty.txt", strings.NewReader(""), nil, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/small.txt", strings.NewReader("abc"), nil, http.StatusCreated)

	empty := openSessionHTTP(t, handler, "demo", "docs/empty.txt", "r")
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+empty+"?offset=0&length=10", nil, nil, http.StatusOK)
	if got := resp.Header.Get("Content-Range"); got != "" {
		t.Fatalf("empty-file read must not carry Content-Range, got %q", got)
	}
	if body := strings.TrimSpace(string(readBody(t, resp))); body != "" {
		t.Fatalf("empty-file read must be empty, got %q", body)
	}

	small := openSessionHTTP(t, handler, "demo", "docs/small.txt", "r")
	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+small+"?offset=3&length=10", nil, nil, http.StatusOK)
	if got := resp.Header.Get("Content-Range"); got != "" {
		t.Fatalf("at-EOF read must not carry Content-Range, got %q", got)
	}
	if body := strings.TrimSpace(string(readBody(t, resp))); body != "" {
		t.Fatalf("at-EOF read must be empty, got %q", body)
	}
}

// TestRevisionTokenFailsLoudOnNoCASVerbs pins R2: endpoints whose storage
// verb takes no mutate options (copy whole-file, link, chmod, chown,
// utimes, xattrs) fail a revision If-Match token loud with 412 instead of
// silently degrading to a start-of-request check. CAS-capable verbs (range
// clone, PATCH write) still enforce the same token and succeed.
func TestRevisionTokenFailsLoudOnNoCASVerbs(t *testing.T) {
	t.Parallel()
	const rev0 = `"rev0"` // fake RevisionContext default, quoted on the wire.
	loud := []struct {
		name   string
		method string
		target string
		body   any
	}{
		{name: "copy", method: http.MethodPost, target: "/api/v1/projects/demo/ops/copy", body: copyRequest{SrcPath: "docs/f.txt", DstPath: "docs/h.txt"}},
		{name: "link", method: http.MethodPost, target: "/api/v1/projects/demo/ops/link", body: linkRequest{ExistingPath: "docs/f.txt", NewPath: "docs/hard.txt"}},
		{name: "chmod", method: http.MethodPost, target: "/api/v1/projects/demo/ops/chmod", body: chmodRequest{Path: "docs/f.txt", Mode: 0o640}},
		{name: "chown", method: http.MethodPost, target: "/api/v1/projects/demo/ops/chown", body: chownRequest{Path: "docs/f.txt", UID: 1000, GID: 1000}},
		{name: "utimes", method: http.MethodPost, target: "/api/v1/projects/demo/ops/utimes", body: utimesRequest{Path: "docs/f.txt", Atime: nowForUtimes(), Mtime: nowForUtimes()}},
		{name: "xattrput", method: http.MethodPut, target: "/api/v1/projects/demo/xattrs/value?path=docs/f.txt&name=key", body: "value"},
	}
	for _, tc := range loud {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, handler := seedIfNoneMatch(t)
			headers := map[string]string{"If-Match": rev0}
			var resp *http.Response
			if text, ok := tc.body.(string); ok {
				resp = mustRequest(t, handler, tc.method, tc.target, strings.NewReader(text), headers, http.StatusPreconditionFailed)
			} else {
				resp = mustJSONRequestWithHeaders(t, handler, tc.method, tc.target, tc.body, headers, http.StatusPreconditionFailed)
			}
			assertErrorCode(t, resp, "precondition_failed")
		})
	}
	t.Run("xattrdelete", func(t *testing.T) {
		t.Parallel()
		_, handler := seedIfNoneMatch(t)
		mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/xattrs/value?path=docs/f.txt&name=key", strings.NewReader("value"), nil, http.StatusNoContent)
		resp := mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/xattrs/value?path=docs/f.txt&name=key", nil,
			map[string]string{"If-Match": rev0}, http.StatusPreconditionFailed)
		assertErrorCode(t, resp, "precondition_failed")
	})
	t.Run("rangeclonekeepscas", func(t *testing.T) {
		t.Parallel()
		_, handler := seedIfNoneMatch(t)
		body := copyRequest{SrcPath: "docs/f.txt", DstPath: "docs/h.txt", SrcOff: int64ptr(0)}
		mustJSONRequestWithHeaders(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy", body,
			map[string]string{"If-Match": rev0}, http.StatusCreated)
	})
	t.Run("patchwritekeepscas", func(t *testing.T) {
		t.Parallel()
		_, handler := seedIfNoneMatch(t)
		mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/f.txt&op=write&offset=0",
			strings.NewReader("!"), map[string]string{"If-Match": rev0}, http.StatusOK)
	})
}

// TestCopyAliasWithoutOffsetsUsesWholeFileCopy pins R13: the src/dst short
// aliases are name aliases only and never select the range variant. With
// the destination already present, whole-file CopyContext fails 409
// (create-only destination) where CloneRange would overwrite with 201.
func TestCopyAliasWithoutOffsetsUsesWholeFileCopy(t *testing.T) {
	t.Parallel()
	_, handler := seedIfNoneMatch(t)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/g.txt", strings.NewReader("world"), nil, http.StatusCreated)
	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy",
		copyRequest{Src: "docs/f.txt", Dst: "docs/g.txt"}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")

	// An explicit offset still routes to CloneRange and overwrites.
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy",
		copyRequest{SrcPath: "docs/f.txt", DstPath: "docs/g.txt", SrcOff: int64ptr(0)}, http.StatusCreated)
}

func nowForUtimes() time.Time {
	return time.Unix(0, 1700000000123456789).UTC()
}

func int64ptr(v int64) *int64 { return &v }
