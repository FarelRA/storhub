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

func (h *restHandler) handleProject(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	switch r.Method {
	case http.MethodGet:
		stats, err := h.clientFor(r).StatFSContext(r.Context(), project)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, projectResponse{Project: project, Stats: stats})
	case http.MethodDelete:
		if err := h.clientFor(r).DeleteProjectContext(r.Context(), project); err != nil {
			h.writeMappedError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "deleted"})
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodDelete)
	}
}
