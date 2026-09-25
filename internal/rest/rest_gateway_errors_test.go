package rest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ghapi "github.com/FarelRA/storhub/internal/github"
)

// TestUpstreamConflictMapsToConflict pins the gateway contract for
// upstream compare-and-swap failures: a 409 from the backend answers 409
// conflict (sanitized), not the default 502.
func TestUpstreamConflictMapsToConflict(t *testing.T) {
	t.Parallel()
	err := &ghapi.APIError{StatusCode: http.StatusConflict, Message: "non-fast-forward"}
	if status := mappedStatus(err); status != http.StatusConflict {
		t.Fatalf("upstream 409 should map to 409, got %d", status)
	}
	handler := &restHandler{
		client: newFakeRESTClient(),
		opts:   Options{AllowAnonymous: true}.withDefaults(),
		shares: &shareRegistry{items: map[string]*shareRecord{}},
	}
	rec := httptest.NewRecorder()
	handler.writeMappedError(rec, err)
	if rec.Code != http.StatusConflict {
		t.Fatalf("envelope status = %d, want 409", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "non-fast-forward") {
		t.Fatalf("upstream detail leaked to client: %s", body)
	}
	if !strings.Contains(body, `"code":"conflict"`) {
		t.Fatalf("missing conflict code in %s", body)
	}
}
