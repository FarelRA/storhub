package rest

import (
	"net/http"
	"strings"

	"github.com/FarelRA/storhub/internal/logging"
	"github.com/go-chi/chi/v5"
)

type rollbackRequest struct {
	CommitSHA string `json:"commit_sha"`
}

type revertPathRequest struct {
	Path      string `json:"path"`
	CommitSHA string `json:"commit_sha"`
}

type pruneRequest struct {
	// Scope is one of objects|assets|history|all (empty means all).
	Scope  string `json:"scope,omitempty"`
	Keep   int    `json:"keep,omitempty"`
	DryRun bool   `json:"dry_run,omitempty"`
}

func (h *restHandler) handleRollback(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req rollbackRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireCommitSHA(req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).RollbackMetadataContext(r.Context(), project, req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "rolled_back"})
}

// purgeResponse is the typed result of a purge operation; it replaces the
// former ad-hoc map so every endpoint returns a struct-shaped document.
type purgeResponse struct {
	Project         string `json:"project"`
	Status          string `json:"status"`
	DeletedReleases int    `json:"deleted_releases"`
	DeletedAssets   int    `json:"deleted_assets"`
}

func (h *restHandler) handlePurge(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	result, err := h.clientFor(r).PurgeUntrackedContext(r.Context(), project)
	if err != nil {
		logging.Error(h.logger, "purge failed", "project", project, "err", err, "status", mappedStatus(err))
		h.writeMappedError(w, err)
		return
	}
	logging.Info(h.logger, "purge complete", "project", project, "deleted_releases", result.DeletedReleases, "deleted_assets", result.DeletedAssets)
	h.writeJSON(w, http.StatusOK, purgeResponse{
		Project:         project,
		Status:          "purged",
		DeletedReleases: result.DeletedReleases,
		DeletedAssets:   result.DeletedAssets,
	})
}

func (h *restHandler) handleRevertPath(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req revertPathRequest
	if err := h.decodeJSON(r, &req); err != nil {
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
	if err := h.clientFor(r).RevertPathContext(r.Context(), project, req.Path, req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "reverted"})
}

// pruneResponse is the typed result of a granular prune.
type pruneResponse struct {
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

func (h *restHandler) handlePrune(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pruneRequest
	// Like purge, prune accepts a bodyless POST: an empty body means the
	// defaults (scope=all, keep=0, dry_run=false).
	if err := h.decodeJSONOptional(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	scope := strings.TrimSpace(req.Scope)
	if scope == "" {
		scope = "all"
	}
	if !validPruneScope(scope) {
		h.writeMappedError(w, errBadRequest(`prune scope must be one of "objects", "assets", "history", "all"`))
		return
	}
	if req.Keep < 0 {
		h.writeMappedError(w, errBadRequest("keep must be non-negative"))
		return
	}
	result, err := h.clientFor(r).PruneContext(r.Context(), project, scope, req.Keep, req.DryRun)
	if err != nil {
		logging.Error(h.logger, "prune failed", "project", project, "scope", scope, "err", err, "status", mappedStatus(err))
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, pruneResponse{
		Project:          project,
		Status:           "pruned",
		Scope:            string(result.Scope),
		DryRun:           result.DryRun,
		DeletedObjects:   result.DeletedObjects,
		DeletedReleases:  result.DeletedReleases,
		DeletedAssets:    result.DeletedAssets,
		HistoryCompacted: result.HistoryCompacted,
		Notes:            result.Notes,
	})
}
