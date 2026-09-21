package rest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// gatedBody signals once the handler starts consuming it, then blocks until
// released - simulating a slow upload whose client vanished mid-body.
type gatedBody struct {
	entered chan struct{}
	release chan struct{}
	done    bool
}

func (b *gatedBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	select {
	case <-b.entered:
	default:
		close(b.entered)
	}
	<-b.release
	b.done = true
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = 'x'
	return 1, nil
}

// TestReplaceOutlivesCanceledRequestContext pins upload detachment: once the
// server accepts an upload it finishes even though the HTTP request's
// context was canceled (client vanished / connection deadline fired).
func TestReplaceOutlivesCanceledRequestContext(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	body := &gatedBody{entered: entered, release: release}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // client disconnected instantly

	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/demo/content?path=f.txt", body).WithContext(ctx)
	rec := httptest.NewRecorder()
	served := make(chan int)
	go func() {
		handler.ServeHTTP(rec, req)
		served <- rec.Code
	}()

	<-entered      // handler is inside the (ignoring-ctx) replace call
	cancel()       // transport-level cancellation fires
	close(release) // bytes finish arriving

	select {
	case code := <-served:
		if code != http.StatusCreated {
			t.Fatalf("status=%d want 201", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replace blocked on canceled request context")
	}
}

// TestClientForWrongTypeServes500 pins the fail-closed HTTP surface: a
// request carrying a foreign value under clientCtxKey must answer 500
// internal_error through the normal error writer, never panic and never
// fall back to the raw unrestricted client.
func TestClientForWrongTypeServes500(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/demo", nil)
	req = req.WithContext(context.WithValue(req.Context(), clientCtxKey, "not-a-client"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("got status %d, want 500", resp.StatusCode)
	}
	assertErrorCode(t, resp, "internal_error")
}
