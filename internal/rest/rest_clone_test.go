package rest

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// seedCloneFile puts content at path via PUT /content and returns nothing;
// failures fail the test immediately.
func seedCloneFile(t *testing.T, handler http.Handler, targetPath, content string) {
	t.Helper()
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path="+targetPath, strings.NewReader(content), nil, http.StatusCreated)
}

func readCloneContent(t *testing.T, handler http.Handler, targetPath string) string {
	t.Helper()
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path="+targetPath, nil, nil, http.StatusOK)
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read content %s: %v", targetPath, err)
	}
	return string(data)
}

func int64Ptr(v int64) *int64 { return &v }

// TestOpsCopyRangeFullClone pins the whole-file range clone: short src/dst
// names with no length resolve the source size and create the destination
// byte-exact, with zero bytes uploaded.
func TestOpsCopyRangeFullClone(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	seedCloneFile(t, handler, "docs/src.txt", "hello world")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy",
		copyRequest{Src: "docs/src.txt", Dst: "docs/full.txt"}, http.StatusCreated)
	if got := readCloneContent(t, handler, "docs/full.txt"); got != "hello world" {
		t.Fatalf("full clone bytes wrong, got %q", got)
	}
	if got := readCloneContent(t, handler, "docs/src.txt"); got != "hello world" {
		t.Fatalf("source must be untouched, got %q", got)
	}
}

// TestOpsCopyRangePartialAndSelfClone pins range overwrites and same-file
// memmove semantics through the range variant.
func TestOpsCopyRangePartialAndSelfClone(t *testing.T) {
	t.Parallel()
	t.Run("partial range", func(t *testing.T) {
		t.Parallel()
		client := newFakeRESTClient()
		handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
		seedCloneFile(t, handler, "docs/src.txt", "0123456789")
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy",
			copyRequest{SrcPath: "docs/src.txt", DstPath: "docs/part.txt", SrcOff: int64Ptr(4), DstOff: int64Ptr(0), Length: int64Ptr(3)}, http.StatusCreated)
		if got := readCloneContent(t, handler, "docs/part.txt"); got != "456" {
			t.Fatalf("range clone bytes wrong, got %q", got)
		}
	})
	t.Run("self overlap reads pre-op bytes", func(t *testing.T) {
		t.Parallel()
		client := newFakeRESTClient()
		handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
		seedCloneFile(t, handler, "docs/self.txt", "abcdefgh")
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy",
			copyRequest{SrcPath: "docs/self.txt", DstPath: "docs/self.txt", SrcOff: int64Ptr(0), DstOff: int64Ptr(2), Length: int64Ptr(4)}, http.StatusCreated)
		if got := readCloneContent(t, handler, "docs/self.txt"); got != "ababcdgh" {
			t.Fatalf("self-clone must read pre-op bytes, got %q", got)
		}
	})
	t.Run("negative offset is 400", func(t *testing.T) {
		t.Parallel()
		client := newFakeRESTClient()
		handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
		seedCloneFile(t, handler, "docs/src.txt", "hello")
		resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy",
			copyRequest{SrcPath: "docs/src.txt", DstPath: "docs/dst.txt", SrcOff: int64Ptr(-1)}, http.StatusBadRequest)
		assertErrorCode(t, resp, "bad_request")
	})
}

// TestOpsCopyRangePreconditionAndSync pins the Phase-4 funnel on the range
// variant: a stale If-Match answers 412 before any clone, and ?sync=1
// drains the project after the clone lands.
func TestOpsCopyRangePreconditionAndSync(t *testing.T) {
	t.Parallel()
	t.Run("stale CAS answers 412", func(t *testing.T) {
		t.Parallel()
		client := newFakeRESTClient()
		handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
		seedCloneFile(t, handler, "docs/src.txt", "hello")
		stale := map[string]string{"If-Match": `"stale-token"`}
		resp := mustJSONRequestWithHeaders(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy",
			copyRequest{SrcPath: "docs/src.txt", DstPath: "docs/dst.txt", Length: int64Ptr(5)}, stale, http.StatusPreconditionFailed)
		assertErrorCode(t, resp, "precondition_failed")
	})
	t.Run("sync drains after clone", func(t *testing.T) {
		t.Parallel()
		client := newFakeRESTClient()
		handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
		if err != nil {
			t.Fatalf("new handler: %v", err)
		}
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
		seedCloneFile(t, handler, "docs/src.txt", "hello")
		mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy?sync=1",
			copyRequest{Src: "docs/src.txt", Dst: "docs/synced.txt"}, http.StatusCreated)
		if got := readCloneContent(t, handler, "docs/synced.txt"); got != "hello" {
			t.Fatalf("synced clone bytes wrong, got %q", got)
		}
		client.mu.Lock()
		defer client.mu.Unlock()
		found := false
		for _, p := range client.drainCalls {
			if p == "demo" {
				found = true
			}
		}
		if !found {
			t.Fatalf("?sync=1 must drain the project, drainCalls=%v", client.drainCalls)
		}
	})
}
