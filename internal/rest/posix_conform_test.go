package rest

import (
	"testing"

	"github.com/FarelRA/storhub/internal/test"
)

const pcProject = "demo"

func TestPosixConformREST(t *testing.T) {
	test.RequireConformance(t)
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{AllowAnonymous: true})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	adapter := &restConformAdapter{handler: handler, project: pcProject, etagBy: map[uint64]string{}}
	results := test.Run(adapter, test.Filter(test.Table, test.SurfaceREST))
	passed, failed := test.Summary(results)
	for _, r := range results {
		if r.Pass {
			t.Logf("PASS %s", r.Name)
		} else {
			t.Logf("FAIL %s: %s", r.Name, r.Error)
		}
	}
	t.Logf("summary: %d passed, %d failed", passed, failed)
	if failed > 0 {
		t.Fatalf("%d scenarios failed", failed)
	}
}
