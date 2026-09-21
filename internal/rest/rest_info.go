package rest

import (
	"encoding/json"
	"fmt"
	"github.com/FarelRA/storhub/internal/logging"
	"net/http"
)

func (h *restHandler) serveConfigJS(w http.ResponseWriter, _ *http.Request) {
	payload, err := json.Marshal(map[string]any{
		"basePath":    h.opts.BasePath,
		"authEnabled": h.opts.Auth != nil,
		"project":     h.opts.DefaultProject,
	})
	if err != nil {
		logging.Error(h.logger, "config serialization failed", "err", err)
		h.writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	_, _ = fmt.Fprintf(w, "window.STORHUB_UI_CONFIG = %s;", payload)
}

// handleAPIInfo answers GET <basePath>: service identity for the console.
// A handle* route (JSON document), not a serve* byte stream.
func (h *restHandler) handleAPIInfo(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]any{
		"service":   "storhubrest",
		"version":   "v1",
		"base_path": h.opts.BasePath,
		// Set by `storhub serve <project>`: the console auto-loads this and
		// hides the free-form project selector - one server, one project.
		"project": h.opts.DefaultProject,
	})
}
