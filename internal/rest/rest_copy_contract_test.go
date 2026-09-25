package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// TestCopyLegacyPathFieldsRejected pins the copy wire to its canonical
// field names: old_path/new_path were removed, so a copy body using them
// answers 400 instead of warn-but-work.
func TestCopyLegacyPathFieldsRejected(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	resp := mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/copy",
		map[string]string{"old_path": "docs/a.txt", "new_path": "docs/b.txt"}, http.StatusBadRequest)
	assertErrorCode(t, resp, "bad_request")
}

// TestCopyRangeEmitsSingleSpan pins one span per copy request: the range
// variant carries variant=range on the copy span instead of opening a
// second clone-range span on the same route.
func TestCopyRangeEmitsSingleSpan(t *testing.T) {
	t.Parallel()
	client := newFakeRESTClient()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := &restHandler{
		client: client,
		opts:   Options{AllowAnonymous: true}.withDefaults(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
		logger: logger,
	}
	ctx := context.Background()
	if err := client.MkdirContext(ctx, "demo", "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := client.CreateFileContext(ctx, "demo", "docs/src.txt"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := client.WriteFileAtContext(ctx, "demo", "docs/src.txt", 0, []byte("hello world")); err != nil {
		t.Fatalf("write: %v", err)
	}
	body, err := json.Marshal(copyRequest{SrcPath: "docs/src.txt", DstPath: "docs/dst.txt", SrcOff: int64Ptr(0), DstOff: int64Ptr(0), Length: int64Ptr(5)})
	if err != nil {
		t.Fatalf("marshal copy body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/projects/demo/ops/copy", bytes.NewReader(body))
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("project", "demo")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
	rec := httptest.NewRecorder()
	h.handleCopy(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("range copy status = %d, want %d (%s)", rec.Code, http.StatusCreated, rec.Body.String())
	}
	out := logs.String()
	if got := strings.Count(out, `"msg":"copy start"`); got != 1 {
		t.Fatalf("want exactly one copy start span, got %d in %s", got, out)
	}
	if strings.Contains(out, "clone-range") {
		t.Fatalf("range copy must not open a clone-range span in %s", out)
	}
	if got := strings.Count(out, `"msg":"copy complete"`); got != 1 {
		t.Fatalf("want exactly one copy complete span, got %d in %s", got, out)
	}
	if !strings.Contains(out, "variant") || !strings.Contains(out, "range") {
		t.Fatalf("copy span must carry the range variant in %s", out)
	}
}
