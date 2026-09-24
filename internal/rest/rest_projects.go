package rest

import (
	"errors"
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
	var err error
	spanStarted := h.traceStart(r, "project-get", project, "")
	defer h.traceFinish(r, "project-get", project, "", spanStarted, &err)
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	stats, err := client.StatFSContext(r.Context(), project)
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
	var err error
	spanStarted := h.traceStart(r, "project-delete", project, "")
	defer h.traceFinish(r, "project-delete", project, "", spanStarted, &err)
	// Deleting the whole project has no target node: the guard is the
	// project revision (412 when the caller decided on a moved HEAD).
	if !h.preconditionForProjectOp(w, r, project) {
		err = errors.New("project precondition failed")
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err = client.DeleteProjectContext(r.Context(), project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	// Draining a deleted project is a cheap no-op on a clean (fresh) cache
	// entry, kept here so every mutating route shares one uniform pattern.
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "deleted"})
}
