package rest

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// The console is a Nuxt SPA built ahead of time (`web/ -> bun run build:embed`)
// and embedded into the binary: no runtime CDN, no external asset trust, one
// self-contained artifact. The built dist is git-ignored so Go-only checkouts
// never see bundle churn; release workflows rebuild it from source first with
// a pinned bun. The bundle is not hermetic (chunk hashes drift by arch and
// toolchain, index.html embeds a random buildId), so byte-for-byte equality
// between a local dist and a fresh rebuild is NOT expected: CI only asserts
// the rebuilt bundle is non-empty.
//
// A committed placeholder.txt keeps `go:embed all:static/dist` compiling on
// Go-only checkouts (Go refuses to embed an empty directory). It is rejected
// by isDistFile and never served. Without a real index.html, serveUIRoot
// keeps its clean ui_not_built 404 path for exactly this case.
//
//go:embed all:static/dist
var uiDist embed.FS

// distFS opens the embedded dist root. A missing embed is reported as an
// error so handlers stay graceful: panicking inside a library handler would
// take down the whole server on a corrupt build, while serveUIRoot already
// has a clean ui_not_built 404 path for exactly this case.
func distFS() (fs.FS, error) {
	sub, err := fs.Sub(uiDist, "static/dist")
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// safeDistName validates a decoded URL path against the embedded dist root.
// A path.Clean equality check rejects every traversal form ("..", ".",
// empty segments, and their %2e%2e encodings once net/http has decoded
// them) WITHOUT rejecting legitimate hashed names that merely contain the
// two-character sequence ".." (e.g. "chunk..2.js").
func safeDistName(urlPath string) string {
	name := strings.TrimPrefix(urlPath, "/")
	if name == "" || strings.Contains(name, "\\") {
		return ""
	}
	if path.Clean("/"+name) != "/"+name {
		return ""
	}
	return name
}

// serveUIRoot serves the SPA entrypoint. Cache-busting lives in hashed
// /_nuxt/* filenames, so the shell itself is revalidated every load.
func (h *restHandler) serveUIRoot(w http.ResponseWriter, _ *http.Request) {
	dist, err := distFS()
	if err != nil {
		h.writeError(w, http.StatusNotFound, "ui_not_built", "console assets are not embedded in this binary")
		return
	}
	data, err := fs.ReadFile(dist, "index.html")
	if err != nil {
		h.writeError(w, http.StatusNotFound, "ui_not_built", "console assets are not embedded in this binary")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}

// serveDistFile serves one embedded file behind the traversal guard. The
// hashed-bundle and public routes differ only in their 404 shape (JSON API
// error vs plain text), so both delegate here instead of twinning the
// guard + FileServerFS pairing.
func (h *restHandler) serveDistFile(w http.ResponseWriter, r *http.Request, jsonNotFound bool) {
	dist, err := distFS()
	if err != nil || !isDistFile(dist, r.URL.Path) {
		if jsonNotFound {
			h.writeError(w, http.StatusNotFound, "not_found", "no such asset")
			return
		}
		http.NotFound(w, r)
		return
	}
	http.FileServerFS(dist).ServeHTTP(w, r)
}

// serveUIAssets serves hashed Vite bundles. The URL prefix /_nuxt/ mirrors the
// real directory inside the dist FS, so the mapping is identity - only the
// traversal guard matters. Directories are never served: http.FileServerFS
// would render a listing of the bundle folder.
func (h *restHandler) serveUIAssets(w http.ResponseWriter, r *http.Request) {
	h.serveDistFile(w, r, true)
}

// serveUIPublic serves non-hashed public files that sit at the dist root
// (favicon.svg today), regular files only.
func (h *restHandler) serveUIPublic(w http.ResponseWriter, r *http.Request) {
	h.serveDistFile(w, r, false)
}

// isDistFile reports whether the URL path names an existing REGULAR file
// inside the embedded dist: the traversal guard must pass and the target
// must not be a directory (FileServerFS renders directory listings).
// placeholder.txt (the committed go:embed placeholder for Go-only
// checkouts) is never a servable asset.
func isDistFile(dist fs.FS, urlPath string) bool {
	name := safeDistName(urlPath)
	if name == "" || name == "placeholder.txt" {
		return false
	}
	info, err := fs.Stat(dist, name)
	return err == nil && info.Mode().IsRegular()
}
