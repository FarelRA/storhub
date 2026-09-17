package rest

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Content-lifecycle tests (split from the TestRESTFilesystemWorkflow
// mega-workflow): put/range/patch/truncate/xattr/chmod/chown/utimes shaped
// around one seeded file, sharing newHandlerForClient + newFakeRESTClient.

func TestRESTContentWorkflow(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	putResp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/readme.txt", strings.NewReader("hello"), nil, http.StatusCreated)
	var putNode nodeResponse
	decodeJSONBody(t, putResp, &putNode)
	if putNode.Entry == nil || putNode.Entry.Path != "docs/readme.txt" || putNode.Entry.Size != 5 {
		t.Fatalf("unexpected put node: %+v", putNode)
	}
	if putNode.ETag == "" {
		t.Fatal("expected etag on put response")
	}

	contentResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/readme.txt", nil, nil, http.StatusOK)
	if got := string(readBody(t, contentResp)); got != "hello" {
		t.Fatalf("unexpected content: %q", got)
	}
	if contentResp.Header.Get("ETag") == "" {
		t.Fatal("expected content etag")
	}

	rangeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/readme.txt", nil, map[string]string{"Range": "bytes=1-3"}, http.StatusPartialContent)
	if got := string(readBody(t, rangeResp)); got != "ell" {
		t.Fatalf("unexpected ranged content: %q", got)
	}
	if calls := client.takeReadCalls(); !reflect.DeepEqual(calls, []readCall{{path: "docs/readme.txt", offset: 0, length: 5}, {path: "docs/readme.txt", offset: 1, length: 3}}) {
		t.Fatalf("unexpected read calls: %+v", calls)
	}

	mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/readme.txt&op=write&offset=1", strings.NewReader("a"), map[string]string{"If-Match": putNode.ETag}, http.StatusOK)
	nodeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/readme.txt", nil, nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, nodeResp, &node)
	writeETag := node.ETag

	mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/readme.txt&op=append", strings.NewReader("!"), map[string]string{"If-Match": writeETag}, http.StatusOK)
	nodeResp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/readme.txt", nil, nil, http.StatusOK)
	decodeJSONBody(t, nodeResp, &node)
	appendETag := node.ETag

	mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/readme.txt&op=truncate&size=3", nil, map[string]string{"If-Match": appendETag}, http.StatusOK)
	contentResp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/readme.txt", nil, nil, http.StatusOK)
	if got := string(readBody(t, contentResp)); got != "hal" {
		t.Fatalf("unexpected truncated content: %q", got)
	}

	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/xattrs/value?path=docs/readme.txt&name=user.flag", strings.NewReader("warm"), nil, http.StatusNoContent)
	attrsResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/xattrs?path=docs/readme.txt", nil, nil, http.StatusOK)
	var attrs xattrListResponse
	decodeJSONBody(t, attrsResp, &attrs)
	if len(attrs.Names) != 1 || attrs.Names[0] != "user.flag" {
		t.Fatalf("unexpected xattrs: %+v", attrs)
	}
	attrValueResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/xattrs/value?path=docs/readme.txt&name=user.flag", nil, nil, http.StatusOK)
	if got := string(readBody(t, attrValueResp)); got != "warm" {
		t.Fatalf("unexpected xattr value: %q", got)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chmod", chmodRequest{Path: "docs/readme.txt", Mode: 0o600}, http.StatusOK)
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown", chownRequest{Path: "docs/readme.txt", UID: 7, GID: 9}, http.StatusOK)
	stamp := time.Unix(100, 0).UTC()
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/utimes", utimesRequest{Path: "docs/readme.txt", Atime: stamp, Mtime: stamp}, http.StatusOK)
}

func TestRESTLinksRevisionsWorkflow(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	putResp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/readme.txt", strings.NewReader("hello"), nil, http.StatusCreated)
	var putNode nodeResponse
	decodeJSONBody(t, putResp, &putNode)
	if putNode.Entry == nil || putNode.Entry.Path != "docs/readme.txt" {
		t.Fatalf("seed put failed: %+v", putNode)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/symlink", symlinkRequest{Target: "docs/readme.txt", LinkPath: "docs/link.txt"}, http.StatusCreated)
	symlinkContent := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/link.txt", nil, nil, http.StatusOK)
	if got := string(readBody(t, symlinkContent)); got != "docs/readme.txt" {
		t.Fatalf("unexpected symlink body: %q", got)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/link", linkRequest{ExistingPath: "docs/readme.txt", NewPath: "docs/hard.txt"}, http.StatusCreated)
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/rename", renameRequest{OldPath: "docs/hard.txt", NewPath: "docs/final.txt"}, http.StatusOK)

	childrenResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/children?path=docs", nil, nil, http.StatusOK)
	var children entriesResponse
	decodeJSONBody(t, childrenResp, &children)
	if len(children.Entries) != 3 {
		t.Fatalf("unexpected children count: %+v", children)
	}
	if names := []string{children.Entries[0].Name, children.Entries[1].Name, children.Entries[2].Name}; strings.Join(names, ",") != "final.txt,link.txt,readme.txt" {
		t.Fatalf("unexpected child names: %v", names)
	}

	revisionsResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/revisions", nil, nil, http.StatusOK)
	var revisions revisionsResponse
	decodeJSONBody(t, revisionsResp, &revisions)
	if len(revisions.Revisions) == 0 {
		t.Fatal("expected revisions")
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/rollback", rollbackRequest{CommitSHA: revisions.Revisions[0].CommitSHA}, http.StatusOK)

	projectResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo", nil, nil, http.StatusOK)
	var project projectResponse
	decodeJSONBody(t, projectResp, &project)
	if project.Stats == nil || project.Stats.Files != 3 || project.Stats.Directories != 2 {
		t.Fatalf("unexpected project stats: %+v", project)
	}
}
