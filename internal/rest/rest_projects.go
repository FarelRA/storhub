package rest

import (
	"net/http"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/go-chi/chi/v5"
)

type projectResponse struct {
	Project string        `json:"project"`
	Stats   *shfs.FSStats `json:"stats"`
}

// handleProjectGet serves GET /projects/{project}: filesystem stats.
func (h *restHandler) handleProjectGet(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	stats, err := h.clientFor(r).StatFSContext(r.Context(), project)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.setRevisionHeader(w, r, project)
	h.writeJSON(w, http.StatusOK, projectResponse{Project: project, Stats: stats})
}

// handleProjectDelete serves DELETE /projects/{project}.
func (h *restHandler) handleProjectDelete(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	if err := h.clientFor(r).DeleteProjectContext(r.Context(), project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	// Draining a deleted project is a cheap no-op on a clean (fresh) cache
	// entry, kept here so every mutating route shares one uniform pattern.
	if !h.maybeDrain(w, r, project) {
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "deleted"})
}
