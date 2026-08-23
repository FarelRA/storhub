package rest

import (
	_ "embed"
	"net/http"
)

// alpineJS is a vendored, pinned copy of Alpine.js 3.14.9. Serving it from
// the binary avoids a third-party CDN dependency: no availability risk, no
// supply-chain trust in jsdelivr at runtime, and a workable CSP.
//
//go:embed static/alpine.min.js
var alpineJS []byte

func (h *restHandler) serveAlpineJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	_, _ = w.Write(alpineJS)
}
