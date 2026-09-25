package rest

import (
	"context"
	"errors"
	"net/http"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// isAccessDenied reports a 403 denial from the authorization layer.
func isAccessDenied(err error) bool {
	var rerr *restStatusError
	return errors.As(err, &rerr) && rerr.status == http.StatusForbidden
}

// A drain waits for published work to land, so it needs the same
// readability gate as the stat surface: an unreadable root stays denied
// even though the drain itself moves no bytes.

type drainGateStub struct {
	Client
	root    *shfs.EntryInfo
	drained []string
}

func (s *drainGateStub) StatPathContext(_ context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	if project == "demo" && targetPath == "" {
		return s.root, nil
	}
	return nil, shfs.ErrNotFound
}

func (s *drainGateStub) DrainProjectContext(_ context.Context, project string) error {
	s.drained = append(s.drained, project)
	return nil
}

func TestDrainRequiresReadableRoot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	locked := &drainGateStub{root: &shfs.EntryInfo{IsDir: true, UID: 0, GID: 0, Mode: 0o700}}
	outsider := &authorizedClient{base: locked, principal: &restPrincipal{Kind: "auth", Username: "alice", UID: 1001, PrimaryGID: 2001}}
	if err := outsider.DrainProjectContext(ctx, "demo"); !isAccessDenied(err) {
		t.Fatalf("drain of an unreadable project must be denied, got %v", err)
	}
	if len(locked.drained) != 0 {
		t.Fatalf("denied drain must not reach the backend, got %v", locked.drained)
	}

	open := &drainGateStub{root: &shfs.EntryInfo{IsDir: true, UID: 0, GID: 0, Mode: 0o755}}
	operator := &authorizedClient{base: open, principal: &restPrincipal{Kind: "auth", Username: "root", UID: 0, PrimaryGID: 0, Admin: true}}
	if err := operator.DrainProjectContext(ctx, "demo"); err != nil {
		t.Fatalf("admin drain must pass, got %v", err)
	}
	if len(open.drained) != 1 || open.drained[0] != "demo" {
		t.Fatalf("allowed drain must reach the backend once, got %v", open.drained)
	}
}
