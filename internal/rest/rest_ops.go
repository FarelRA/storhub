package rest

import (
	"errors"
	"net/http"
	"strings"
	"time"

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

type pruneRequest struct {
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
	var err error
	spanStarted := h.traceStart(r, "rollback", project, "")
	defer h.traceFinish(r, "rollback", project, "", spanStarted, &err)
	// Rollback republishes history without a target node: the guard is the
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
	if err = client.RollbackMetadataContext(r.Context(), project, req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "rolled_back"})
}

// pruneResponse is the typed result of a purge operation: every endpoint
// returns a struct-shaped document.
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
	// Chunk-GC tallies, populated by the chunks scope only.
	ScannedChunks   int   `json:"scanned_chunks,omitempty"`
	OrphanChunks    int   `json:"orphan_chunks,omitempty"`
	OrphanBytes     int64 `json:"orphan_bytes,omitempty"`
	CollectedChunks int   `json:"collected_chunks,omitempty"`
	CollectedBytes  int64 `json:"collected_bytes,omitempty"`
}

func (h *restHandler) handlePrune(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	started := time.Now().UTC()
	var req pruneRequest
	// A bodyless POST means the old bare purge: assets scope, keep=0,
	// dry_run=false. A body selects any scope (objects|assets|history|all).
	if err := h.decodeJSON(r, &req, true); err != nil {
		h.writeMappedError(w, err)
		return
	}
	scope := storage.PruneAssets
	if strings.TrimSpace(req.Scope) != "" {
		var err error
		scope, err = parsePruneScope(req.Scope)
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
	var err error
	spanStarted := h.traceStart(r, "prune", project, "", "scope", string(scope), "dry_run", req.DryRun)
	defer h.traceFinish(r, "prune", project, "", spanStarted, &err, "scope", string(scope), "dry_run", req.DryRun)
	if !h.preconditionForProjectOp(w, r, project) {
		err = errors.New("project precondition failed")
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	result, err := client.PruneContext(r.Context(), project, string(scope), req.Keep, req.DryRun)
	if err != nil {
		logging.Error(h.logger, "prune failed", "project", project, "scope", scope, "status", mappedStatus(err), "elapsed", time.Since(started), "err", err)
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
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
		ScannedChunks:    result.ScannedChunks,
		OrphanChunks:     result.OrphanChunks,
		OrphanBytes:      result.OrphanBytes,
		CollectedChunks:  result.CollectedChunks,
		CollectedBytes:   result.CollectedBytes,
	})
}

func (h *restHandler) handleEnable(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	started := time.Now().UTC()
	var err error
	spanStarted := h.traceStart(r, "enable", project, "")
	defer h.traceFinish(r, "enable", project, "", spanStarted, &err)
	if !h.preconditionForProjectOp(w, r, project) {
		err = errors.New("project precondition failed")
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err = client.ReEnableProject(project); err != nil {
		logging.Error(h.logger, "enable failed", "project", project, "status", mappedStatus(err), "elapsed", time.Since(started), "err", err)
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "enabled"})
}

// statusResponse reports project operability in one document: the
// degraded latch, the consecutive-failure streak, the pending depth,
// and the hub pressure totals.
type statusResponse struct {
	Project          string   `json:"project"`
	Degraded         bool     `json:"degraded"`
	FailureStreak    uint64   `json:"failure_streak"`
	PendingDepth     int      `json:"pending_depth"`
	CapCrosses       uint64   `json:"cap_crosses"`
	ForceRetryPokes  uint64   `json:"force_retry_pokes"`
	CommitSuccesses  uint64   `json:"commit_successes"`
	CommitFailures   uint64   `json:"commit_failures"`
	Rebases          uint64   `json:"rebases"`
	DegradedProjects []string `json:"degraded_projects"`
}

func (h *restHandler) handleStatus(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var err error
	spanStarted := h.traceStart(r, "status", project, "")
	defer h.traceFinish(r, "status", project, "", spanStarted, &err)
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	degraded, err := client.DegradedProjects()
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	snap, err := client.PressureSnapshot()
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	latched := false
	for _, name := range degraded {
		if name == project {
			latched = true
			break
		}
	}
	streak, err := client.PressureFailureStreak(project)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	depth, err := client.PressurePendingDepth(project)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, statusResponse{
		Project:          project,
		Degraded:         latched,
		FailureStreak:    streak,
		PendingDepth:     depth,
		CapCrosses:       snap.CapCrosses,
		ForceRetryPokes:  snap.ForceRetryPokes,
		CommitSuccesses:  snap.CommitSuccesses,
		CommitFailures:   snap.CommitFailures,
		Rebases:          snap.Rebases,
		DegradedProjects: degraded,
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
	var err error
	spanStarted := h.traceStart(r, "revert", project, req.Path)
	defer h.traceFinish(r, "revert", project, req.Path, spanStarted, &err)
	if !h.preconditionForProjectOp(w, r, project) {
		err = errors.New("project precondition failed")
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err = client.RevertPathContext(r.Context(), project, req.Path, req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "reverted"})
}
