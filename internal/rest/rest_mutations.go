package rest

import (
	"net/http"
	"strings"
	"time"

	"github.com/FarelRA/storhub/internal/logging"
	"github.com/go-chi/chi/v5"
)

// File mutations: create/mkdir/rmdir/unlink/rename/copy/link/symlink/
// chmod/chown/utimes. Split out of rest_content.go so content stays about
// reads (nodes/children/content/xattrs/revisions) and mutations read as
// one table.

type renameRequest struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

type linkRequest struct {
	ExistingPath string `json:"existing_path"`
	NewPath      string `json:"new_path"`
}

type symlinkRequest struct {
	Target   string `json:"target"`
	LinkPath string `json:"link_path"`
}

type chmodRequest struct {
	Path string `json:"path"`
	Mode uint32 `json:"mode"`
}

type chownRequest struct {
	Path string `json:"path"`
	UID  uint32 `json:"uid"`
	GID  uint32 `json:"gid"`
}

type utimesRequest struct {
	Path  string    `json:"path"`
	Atime time.Time `json:"atime"`
	Mtime time.Time `json:"mtime"`
}

func (h *restHandler) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath, err := h.decodePathRequest(r, "path")
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.clientFor(r).CreateFileContext(r.Context(), project, filePath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, filePath, http.StatusCreated)
}

func (h *restHandler) handleMkdir(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	dirPath, err := h.decodePathRequest(r, "path")
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).MkdirContext(r.Context(), project, dirPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, dirPath, http.StatusCreated)
}

func (h *restHandler) handleRmdir(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	dirPath, err := h.decodePathRequest(r, "path")
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).RmdirContext(r.Context(), project, dirPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleUnlink(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath, err := h.decodePathRequest(r, "path")
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).DeleteFileContext(r.Context(), project, filePath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleRename(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req renameRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("old_path", req.OldPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("new_path", req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).RenameContext(r.Context(), project, req.OldPath, req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.NewPath, http.StatusOK)
}

func (h *restHandler) handleCopy(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req copyRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if strings.TrimSpace(req.OldPath) != "" || strings.TrimSpace(req.NewPath) != "" {
		logging.Warn(h.logger, "deprecated copy fields old_path/new_path used; send src_path/dst_path", "project", project)
	}
	src, dst, err := copySrcDst(req)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).CopyContext(r.Context(), project, src, dst); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, dst, http.StatusCreated)
}

func (h *restHandler) handleLink(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req linkRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("existing_path", req.ExistingPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("new_path", req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.clientFor(r).LinkContext(r.Context(), project, req.ExistingPath, req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.NewPath, http.StatusCreated)
}

func (h *restHandler) handleSymlink(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req symlinkRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("target", req.Target); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("link_path", req.LinkPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.clientFor(r).SymlinkContext(r.Context(), project, req.Target, req.LinkPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.LinkPath, http.StatusCreated)
}

func (h *restHandler) handleChmod(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req chmodRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).ChmodContext(r.Context(), project, req.Path, req.Mode); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusOK)
}

func (h *restHandler) handleChown(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req chownRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).ChownContext(r.Context(), project, req.Path, req.UID, req.GID); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusOK)
}

func (h *restHandler) handleUtimes(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req utimesRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	// A zero time.Time would silently forward Unix() = -62135596800 to
	// storage; require both stamps to be present.
	if req.Atime.IsZero() || req.Mtime.IsZero() {
		h.writeMappedError(w, errBadRequest("atime and mtime are required"))
		return
	}
	if err := h.clientFor(r).ChtimesContext(r.Context(), project, req.Path, req.Atime.Unix(), req.Mtime.Unix()); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusOK)
}
