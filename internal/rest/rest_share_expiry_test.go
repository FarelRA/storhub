package rest

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Share lifetimes follow the share clock, not the wall clock: moving the
// clock past expiry must end redemption without touching config.

func TestShareLifetimeFollowsShareClock(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true, ShareSigningKey: []byte(testShareKey)})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/f.txt", strings.NewReader("data"), nil, http.StatusCreated)

	base := time.Now()
	restore := setShareClock(func() time.Time { return base })
	defer restore()

	parent := mustDecodeShare(t, mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/shares",
		shareRequest{Path: "docs/f.txt", ExpiresInSeconds: 3600}, http.StatusCreated))
	infoTarget := "/api/v1/shares/" + url.PathEscape(parent.Token)
	mustRequest(t, handler, http.MethodGet, infoTarget, nil, nil, http.StatusOK)

	restore()
	restore = setShareClock(func() time.Time { return base.Add(2 * time.Hour) })
	defer restore()
	mustRequest(t, handler, http.MethodGet, infoTarget, nil, nil, http.StatusNotFound)
}
