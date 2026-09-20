package rest

import (
	"net/http"
	"strings"

	"github.com/FarelRA/storhub/internal/logging"
	"github.com/FarelRA/storhub/internal/storage"
	"github.com/go-chi/chi/v5"
)

type rollbackRequest struct {
	CommitSHA string `json:"commit_sha"`
}

type revertPathRequest struct {
	Path      string `json:"path"`
	CommitSHA string `json:"commit_sha"`
}

type purgeRequest struct {
	// Scope is one of objects|assets|history|all (empty means all).
	Scope  string `json:"scope,omitempty"`
	Keep   int    `json:"keep,omitempty"`
	DryRun bool   `json:"dry_run,omitempty"`
}

func (h *restHandler) handleRollback(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req rollbackRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireCommitSHA(req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	// Rollback republishes history without a target node: the guard is the
	// project revision (412 when the caller decided on a moved HEAD).
	if !h.preconditionForProjectOp(w, r, project) {
		return
	}
	if err := h.clientFor(r).RollbackMetadataContext(r.Context(), project, req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "rolled_back"})
}

// purgeResponse is the typed result of a purge operation; it replaces the
// former ad-hoc map so every endpoint returns a struct-shaped document.
type purgeResponse struct {
	Project          string   `json:"project"`
	Status           string   `json:"status"`
	Scope            string   `json:"scope"`
	DryRun           bool     `json:"dry_run"`
	DeletedObjects   int      `json:"deleted_objects"`
	DeletedReleases  int      `json:"deleted_releases"`
	DeletedAssets    int      `json:"deleted_assets"`
	HistoryCompacted bool     `json:"history_compacted"`
	Notes            []string `json:"notes,omitempty"`
}

func (h *restHandler) handlePurge(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req purgeRequest
	// A bodyless POST means the old bare purge: assets scope, keep=0,
	// dry_run=false. A body selects any scope (objects|assets|history|all).
	if err := h.decodeJSON(r, &req, true); err != nil {
		h.writeMappedError(w, err)
		return
	}
	scope := storage.PurgeAssets
	if strings.TrimSpace(req.Scope) != "" {
		var err error
		scope, err = parsePurgeScope(req.Scope)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
	}
	if req.Keep < 0 {
		h.writeMappedError(w, errBadRequest("keep must be non-negative"))
		return
	}
	// History compaction supports only keep<=1 (storage coerces keep<1 to
	// 1); keep>1 would pass through to a generic 500, so reject it as 400
	// here. Storage keeps its own backstop error.
	if req.Keep > 1 {
		h.writeMappedError(w, errBadRequest("keep must be <= 1: history compaction retains exactly one checkpoint"))
		return
	}
	if !h.preconditionForProjectOp(w, r, project) {
		return
	}
	result, err := h.clientFor(r).PurgeContext(r.Context(), project, string(scope), req.Keep, req.DryRun)
	if err != nil {
		logging.Error(h.logger, "purge failed", "project", project, "scope", scope, "err", err, "status", mappedStatus(err))
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		return
	}
	h.writeJSON(w, http.StatusOK, purgeResponse{
		Project:          project,
		Status:           "purged",
		Scope:            string(result.Scope),
		DryRun:           result.DryRun,
		DeletedObjects:   result.DeletedObjects,
		DeletedReleases:  result.DeletedReleases,
		DeletedAssets:    result.DeletedAssets,
		HistoryCompacted: result.HistoryCompacted,
		Notes:            result.Notes,
	})
}

func (h *restHandler) handleRevertPath(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req revertPathRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireCommitSHA(req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.preconditionForProjectOp(w, r, project) {
		return
	}
	if err := h.clientFor(r).RevertPathContext(r.Context(), project, req.Path, req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "reverted"})
}
