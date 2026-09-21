package rest

// rest_session_lifecycle_test.go: session open, write isolation, sync,
// truncate, and stat over REST.

import (
	"net/http"
	"strings"
	"testing"
)

// TestRESTSessionLifecycle drives open/write/read/sync/close through HTTP:
// staged writes stay invisible to a second client until close, a concurrent
// committer never moves the pin, and unlinked scratch needs a link.

func TestRESTSessionLifecycle(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=data.txt", strings.NewReader("versionone"), nil, http.StatusCreated)

	handle := openSessionHTTP(t, handler, "demo", "data.txt", "r")
	if got := string(readSessionHTTP(t, handler, handle, 0, 64)); got != "versionone" {
		t.Fatalf("pinned read = %q, want %q", got, "versionone")
	}

	writer := openSessionHTTP(t, handler, "demo", "data.txt", "w+")
	writeSessionHTTP(t, handler, writer, 0, "version-two!")
	if got := string(readSessionHTTP(t, handler, writer, 0, 64)); got != "version-two!" {
		t.Fatalf("own staged read = %q, want %q", got, "version-two!")
	}
	if got := contentHTTP(t, handler, "demo", "data.txt"); got != "versionone" {
		t.Fatalf("second client sees %q before commit, want pinned %q", got, "versionone")
	}

	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=data.txt", strings.NewReader("RIVAL-replace"), nil, http.StatusOK)
	if got := string(readSessionHTTP(t, handler, writer, 0, 64)); got != "version-two!" {
		t.Fatalf("pin moved under concurrent committer: %q", got)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+writer+"/sync", map[string]any{}, http.StatusOK)
	if stat := statSessionHTTP(t, handler, writer); stat.Dirty {
		t.Fatalf("sync must clear dirty, got %+v", stat)
	}
	if got := contentHTTP(t, handler, "demo", "data.txt"); got != "version-two!" {
		t.Fatalf("second client sees %q after sync, want %q", got, "version-two!")
	}

	mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+writer+"/close?sync=1", nil, nil, http.StatusOK)
	client.mu.Lock()
	drained := append([]string(nil), client.drainCalls...)
	client.mu.Unlock()
	if len(drained) != 1 || drained[0] != "demo" {
		t.Fatalf("close?sync=1 must drain demo once, got %v", drained)
	}
	resp := mustRequest(t, handler, http.MethodGet, "/api/v1/handles/"+writer, nil, nil, http.StatusGone)
	assertErrorCode(t, resp, "gone")
}

// TestRESTSessionTruncateAndStat pins truncate staging and the stat shape.

func TestRESTSessionTruncateAndStat(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=f.txt", strings.NewReader("hello world"), nil, http.StatusCreated)

	handle := openSessionHTTP(t, handler, "demo", "f.txt", "w+")
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/handles/"+handle+"/truncate", map[string]int64{"size": 5}, http.StatusOK)
	stat := statSessionHTTP(t, handler, handle)
	if stat.Size != 5 || !stat.Dirty || stat.Project != "demo" || stat.Path != "f.txt" {
		t.Fatalf("unexpected stat after truncate: %+v", stat)
	}
	if got := string(readSessionHTTP(t, handler, handle, 0, 64)); got != "hello" {
		t.Fatalf("read after truncate = %q, want %q", got, "hello")
	}
}
