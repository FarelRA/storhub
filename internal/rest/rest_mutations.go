package rest

import (
	"net/http"
	"strings"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
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
	// NoReplace requests RENAME_NOREPLACE: fail with 409 when the
	// destination exists instead of replacing it. Absent means replace,
	// so existing clients are unaffected.
	NoReplace bool `json:"no_replace,omitempty"`
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
	if _, _, ok := h.preconditionForCreate(w, r, project, filePath); !ok {
		return
	}
	if _, err := h.clientFor(r).CreateFileContext(r.Context(), project, filePath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	if _, _, ok := h.preconditionForCreate(w, r, project, dirPath); !ok {
		return
	}
	if err := h.clientFor(r).MkdirContext(r.Context(), project, dirPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	revOpts, ok := h.preconditionForUpdate(w, r, project, dirPath)
	if !ok {
		return
	}
	if err := h.clientFor(r).RmdirContext(r.Context(), project, dirPath, revOpts...); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	revOpts, ok := h.preconditionForUpdate(w, r, project, filePath)
	if !ok {
		return
	}
	if err := h.clientFor(r).DeleteFileContext(r.Context(), project, filePath, revOpts...); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	revOpts, ok := h.preconditionForUpdate(w, r, project, req.OldPath)
	if !ok {
		return
	}
	if req.NoReplace {
		// Enforced inside the storage transaction (no TOCTOU), like the
		// CLI's mv --no-replace.
		revOpts = append(revOpts, shfs.WithNoReplace())
	}
	if err := h.clientFor(r).RenameContext(r.Context(), project, req.OldPath, req.NewPath, revOpts...); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	// Copy reads the source and creates the destination; the guard sits on
	// the source (CopyContext takes no mutate options, so a revision token
	// degrades to a start-of-request freshness check, documented below).
	if _, ok := h.preconditionForUpdate(w, r, project, src); !ok {
		return
	}
	if err := h.clientFor(r).CopyContext(r.Context(), project, src, dst); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	// Link reads the existing path and creates the new one; the guard sits
	// on the source (LinkContext takes no mutate options: freshness only).
	if _, ok := h.preconditionForUpdate(w, r, project, req.ExistingPath); !ok {
		return
	}
	if _, err := h.clientFor(r).LinkContext(r.Context(), project, req.ExistingPath, req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	// Symlink creation follows create-only semantics (SymlinkContext takes
	// no mutate options: freshness only).
	if _, _, ok := h.preconditionForCreate(w, r, project, req.LinkPath); !ok {
		return
	}
	if _, err := h.clientFor(r).SymlinkContext(r.Context(), project, req.Target, req.LinkPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	// ChmodContext takes no mutate options: a revision token degrades to a
	// start-of-request freshness check (documented on the helpers below).
	if _, ok := h.preconditionForUpdate(w, r, project, req.Path); !ok {
		return
	}
	if err := h.clientFor(r).ChmodContext(r.Context(), project, req.Path, req.Mode); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	// ChownContext takes no mutate options: freshness only, as for chmod.
	if _, ok := h.preconditionForUpdate(w, r, project, req.Path); !ok {
		return
	}
	if err := h.clientFor(r).ChownContext(r.Context(), project, req.Path, req.UID, req.GID); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
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
	// ChtimesContext takes no mutate options: freshness only, as for chmod.
	if _, ok := h.preconditionForUpdate(w, r, project, req.Path); !ok {
		return
	}
	if err := h.clientFor(r).ChtimesContext(r.Context(), project, req.Path, req.Atime.Unix(), req.Mtime.Unix()); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusOK)
}

// preconditionForUpdate enforces the request's If-Match for an operation
// that mutates an existing target, through the shared mutationPrecondition
// funnel (revision CAS where the header carries the current revision,
// classic ETag freshness otherwise). It returns the backend CAS options
// for client methods that accept them; methods that take no mutate options
// (copy, link, chmod, chown, utimes, xattrs) get a start-of-request
// freshness check only, without apply-time re-verification. ok=false means
// the handler already answered and must return without writing more.
func (h *restHandler) preconditionForUpdate(w http.ResponseWriter, r *http.Request, project, targetPath string) ([]shfs.MutateOption, bool) {
	revOpts, err := h.mutationPrecondition(r, project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return nil, false
	}
	return revOpts, true
}

// preconditionForCreate enforces create-only semantics for an operation
// that creates targetPath, mirroring the PUT replace funnel: a missing
// target proceeds (a non-empty If-Match on a missing resource fails 412),
// while an existing target keeps classic ETag freshness plus
// If-None-Match * rejection. exists reports whether the target already
// exists; revOpts carry backend CAS options as in preconditionForUpdate.
// ok=false means the handler already answered.
func (h *restHandler) preconditionForCreate(w http.ResponseWriter, r *http.Request, project, targetPath string) (exists bool, revOpts []shfs.MutateOption, ok bool) {
	entry, exists, err := h.lookupOptional(r, project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return false, nil, false
	}
	if !exists {
		// Like the PUT replace funnel: If-Match guards a state the
		// client saw, and there is no state to match on a missing
		// resource, so any If-Match fails 412 here.
		if strings.TrimSpace(r.Header.Get("If-Match")) != "" {
			h.writeMappedError(w, errPreconditionFailed("resource does not exist"))
			return false, nil, false
		}
		return false, nil, true
	}
	revOpts, revErr := h.mutationPrecondition(r, project, targetPath)
	if revErr != nil {
		h.writeMappedError(w, revErr)
		return false, nil, false
	}
	if revOpts == nil {
		if err := h.requireMatch(r.Header.Get("If-Match"), restEntryETag(entry)); err != nil {
			if !isPreconditionHeaderEmpty(err) {
				h.writeMappedError(w, err)
				return false, nil, false
			}
		}
		if matchEntityTag(r.Header.Get("If-None-Match"), "*") {
			h.writeMappedError(w, errPreconditionFailed("resource already exists"))
			return false, nil, false
		}
	}
	return true, revOpts, true
}

// preconditionForProjectOp enforces If-Match for path-less project
// operations (rollback, revert-path, purge, prune, project delete): the
// token must equal the project's current metadata revision, else 412.
// There is no target node, so the classic ETag flavor has nothing to
// compare against and any non-revision token fails. ok=false means the
// handler already answered.
func (h *restHandler) preconditionForProjectOp(w http.ResponseWriter, r *http.Request, project string) bool {
	ifMatch := strings.TrimSpace(r.Header.Get("If-Match"))
	if ifMatch == "" {
		return true
	}
	rev, err := h.clientFor(r).RevisionContext(r.Context(), project)
	if err != nil {
		h.writeMappedError(w, err)
		return false
	}
	if unquoteEntityTag(ifMatch) != rev {
		h.writeMappedError(w, errPreconditionFailed("project revision changed"))
		return false
	}
	return true
}
