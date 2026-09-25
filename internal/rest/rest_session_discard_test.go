package rest

import (
	"context"
	"net/http"
	"strings"
	"testing"

	storage "github.com/FarelRA/storhub/internal/storage"
)

// Discard forgets a handle without committing: the explicit verb for
// abandoning staged work, next to close which commits then destroys.

type discardStub struct {
	*fakeRESTClient
	discarded []string
}

func (s *discardStub) DiscardSession(ctx context.Context, handleID string) error {
	if _, err := s.StatSession(ctx, handleID); err != nil {
		return err
	}
	s.discarded = append(s.discarded, handleID)
	return nil
}

func TestSessionDiscardForgetsWithoutCommitting(t *testing.T) {
	t.Parallel()
	stub := &discardStub{fakeRESTClient: newFakeRESTClient()}
	handler, err := newHandlerForClient(stub, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=data.txt", strings.NewReader("versionone"), nil, http.StatusCreated)

	handle := openSessionHTTP(t, handler, "demo", "data.txt", "w+")
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/handles/"+handle+"/discard", nil, nil, http.StatusOK)
	var closed sessionCloseResponse
	decodeJSONBody(t, resp, &closed)
	if closed.Handle != handle || closed.Status != "discarded" {
		t.Fatalf("unexpected discard ack: %+v", closed)
	}
	if len(stub.discarded) != 1 || stub.discarded[0] != handle {
		t.Fatalf("discard must reach the backend once, got %v", stub.discarded)
	}
	if got := contentHTTP(t, handler, "demo", "data.txt"); got != "versionone" {
		t.Fatalf("discard must publish nothing, content = %q", got)
	}
}

func TestSessionDiscardUnknownHandleIsGone(t *testing.T) {
	t.Parallel()
	stub := &discardStub{fakeRESTClient: newFakeRESTClient()}
	handler, err := newHandlerForClient(stub, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	resp := mustRequest(t, handler, http.MethodPost, "/api/v1/handles/deadbeef/discard", nil, nil, http.StatusGone)
	assertErrorCode(t, resp, "gone")
}

func TestSessionDiscardPassthroughAndDenial(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stub := &discardStub{fakeRESTClient: newFakeRESTClient()}
	handle, err := stub.OpenSession(ctx, "demo", "", storage.SessionCreate|storage.SessionReadWrite)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	fwd := &authorizedClient{base: stub, principal: &restPrincipal{Kind: "auth", Username: "root", UID: 0, PrimaryGID: 0, Admin: true}}
	if err := fwd.DiscardSession(ctx, handle); err != nil {
		t.Fatalf("authorized discard must forward, got %v", err)
	}
	if len(stub.discarded) != 1 {
		t.Fatalf("authorized discard must reach the backend, got %v", stub.discarded)
	}
	if err := (readOnlyShare{}).DiscardSession(ctx, "abc"); !isAccessDenied(err) {
		t.Fatalf("share discard must be denied, got %v", err)
	}
}
