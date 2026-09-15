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
// self-contained artifact. The built dist is committed so Go-only checkouts
// and CI produce identical binaries; workflows that release artifacts rebuild
// it from source first.
//
//go:embed all:static/dist
var uiDist embed.FS

func distFS() fs.FS {
	sub, err := fs.Sub(uiDist, "static/dist")
	if err != nil {
		panic("rest: embedded dist missing: " + err.Error())
	}
	return sub
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
	data, err := fs.ReadFile(distFS(), "index.html")
	if err != nil {
		h.writeError(w, http.StatusNotFound, "ui_not_built", "console assets are not embedded in this binary")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}

// serveUIAssets serves hashed Vite bundles. The URL prefix /_nuxt/ mirrors the
// real directory inside the dist FS, so the mapping is identity - only the
// traversal guard matters. Directories are never served: http.FileServerFS
// would render a listing of the bundle folder.
func (h *restHandler) serveUIAssets(w http.ResponseWriter, r *http.Request) {
	if !isDistFile(r.URL.Path) {
		h.writeError(w, http.StatusNotFound, "not_found", "no such asset")
		return
	}
	http.FileServerFS(distFS()).ServeHTTP(w, r)
}

// serveUIPublic serves non-hashed public files that sit at the dist root
// (favicon.svg today), regular files only.
func (h *restHandler) serveUIPublic(w http.ResponseWriter, r *http.Request) {
	if !isDistFile(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	http.FileServerFS(distFS()).ServeHTTP(w, r)
}

// isDistFile reports whether the URL path names an existing REGULAR file
// inside the embedded dist: the traversal guard must pass and the target
// must not be a directory (FileServerFS renders directory listings).
func isDistFile(urlPath string) bool {
	name := safeDistName(urlPath)
	if name == "" {
		return false
	}
	info, err := fs.Stat(distFS(), name)
	return err == nil && info.Mode().IsRegular()
}
