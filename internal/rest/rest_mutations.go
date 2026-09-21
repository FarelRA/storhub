package rest

import (
	"encoding/json"
	"fmt"
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

// chownKeepID is the chown(2) leave-unchanged sentinel (all-ones, the
// wire encoding of (uid_t)-1), shared with shfs.CanChown and the CLI
// -1 convention. REST clients omit the field (or send -1) to keep the
// corresponding id, matching the CLI and FUSE partial-chown spellings.
const chownKeepID = ^uint32(0)

// UnmarshalJSON decodes a chown body with an explicit keep convention:
// an omitted uid/gid (or the signed -1 the CLI accepts) means keep,
// anything else must be a uint32 id. Out-of-range values fail closed.
// Unknown fields are rejected, preserving the strict decoding the
// default path applies under DisallowUnknownFields.
func (r *chownRequest) UnmarshalJSON(data []byte) error {
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		return err
	}
	for key := range keys {
		switch key {
		case "path", "uid", "gid":
		default:
			return fmt.Errorf("unknown field %q", key)
		}
	}
	var raw struct {
		Path string `json:"path"`
		UID  *int64 `json:"uid"`
		GID  *int64 `json:"gid"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	uid, err := chownIDOrKeep(raw.UID, "uid")
	if err != nil {
		return err
	}
	gid, err := chownIDOrKeep(raw.GID, "gid")
	if err != nil {
		return err
	}
	r.Path, r.UID, r.GID = raw.Path, uid, gid
	return nil
}

func chownIDOrKeep(value *int64, field string) (uint32, error) {
	if value == nil || *value == -1 {
		return chownKeepID, nil
	}
	if *value < 0 || *value > int64(chownKeepID) {
		return 0, fmt.Errorf("invalid %s %d: must be a non-negative id, -1, or omitted to keep", field, *value)
	}
	return uint32(*value), nil
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
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := client.CreateFileContext(r.Context(), project, filePath); err != nil {
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
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := client.MkdirContext(r.Context(), project, dirPath); err != nil {
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
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := client.RmdirContext(r.Context(), project, dirPath, revOpts...); err != nil {
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
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := client.DeleteFileContext(r.Context(), project, filePath, revOpts...); err != nil {
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
		// CLI's mv --noreplace.
		revOpts = append(revOpts, shfs.WithNoReplace())
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := client.RenameContext(r.Context(), project, req.OldPath, req.NewPath, revOpts...); err != nil {
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
	srcOff, dstOff, length, isRange, err := copyRangeParams(req)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if isRange {
		h.handleCloneRange(w, r, project, src, srcOff, dst, dstOff, length)
		return
	}
	// Copy reads the source and creates the destination; the guard sits on
	// the source. CopyContext takes no mutate options, so a revision token
	// cannot become apply-time compare-and-swap: it fails loud with 412
	// (preconditionForUpdateNoCAS) instead of silently degrading to a
	// start-of-request check. The range variant above keeps true CAS via
	// CloneRange options.
	if !h.preconditionForUpdateNoCAS(w, r, project, src, "copy") {
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := client.CopyContext(r.Context(), project, src, dst); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		return
	}
	h.respondWithNode(w, r, project, dst, http.StatusCreated)
}

// handleCloneRange serves the range variant of POST /ops/copy: one
// server-side CloneRange, zero bytes uploaded. Outcome matrix:
// full clone: absent length resolves to the source size from src_off
// (src_off 0 covers the whole file) and creates dst when missing;
// range: an explicit [src_off, src_off+length) overwrites the dst span
// at dst_off with pwrite semantics (gaps zero-fill by size accounting);
// self-clone: src and dst may name the same file, where the core's
// memmove snapshot semantics clone the pre-op bytes (cloning a span onto
// itself is an exact no-op duplicate, still applied atomically).
// The guard funnels through preconditionForUpdate on the source like the
// sibling mutating endpoints: a revision token becomes backend
// compare-and-swap options enforced inside the core transaction (412 when
// the project moved), any other token keeps start-of-request freshness.
func (h *restHandler) handleCloneRange(w http.ResponseWriter, r *http.Request, project, src string, srcOff int64, dst string, dstOff int64, length *int64) {
	revOpts, ok := h.preconditionForUpdate(w, r, project, src)
	if !ok {
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	resolved := length
	if resolved == nil {
		entry, err := client.StatPathContext(r.Context(), project, src)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		if entry.IsDir {
			h.writeMappedError(w, &restStatusError{status: http.StatusConflict, message: "clone source is a directory"})
			return
		}
		full := entry.Size - srcOff
		if full < 0 {
			full = 0
		}
		resolved = &full
	}
	if _, err := client.CloneRange(r.Context(), project, src, srcOff, dst, dstOff, *resolved, revOpts...); err != nil {
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
	// on the source. LinkContext takes no mutate options, so a revision
	// token fails loud with 412 (preconditionForUpdateNoCAS) instead of
	// silently degrading to a start-of-request check.
	if !h.preconditionForUpdateNoCAS(w, r, project, req.ExistingPath, "link") {
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := client.LinkContext(r.Context(), project, req.ExistingPath, req.NewPath); err != nil {
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
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := client.SymlinkContext(r.Context(), project, req.Target, req.LinkPath); err != nil {
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
	// ChmodContext takes no mutate options: a revision token cannot become
	// apply-time compare-and-swap, so it fails loud with 412
	// (preconditionForUpdateNoCAS) instead of silently degrading to a
	// start-of-request check. Classic ETag tokens keep freshness semantics.
	if !h.preconditionForUpdateNoCAS(w, r, project, req.Path, "chmod") {
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := client.ChmodContext(r.Context(), project, req.Path, req.Mode); err != nil {
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
	// ChownContext takes no mutate options: like chmod, a revision token
	// fails loud with 412 (preconditionForUpdateNoCAS) instead of silently
	// degrading to a start-of-request check.
	if !h.preconditionForUpdateNoCAS(w, r, project, req.Path, "chown") {
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := client.ChownContext(r.Context(), project, req.Path, req.UID, req.GID); err != nil {
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
	// A zero time.Time would silently forward UnixNano() garbage to
	// storage; require both stamps to be present.
	if req.Atime.IsZero() || req.Mtime.IsZero() {
		h.writeMappedError(w, errBadRequest("atime and mtime are required"))
		return
	}
	// ChtimesContext takes no mutate options: like chmod, a revision token
	// fails loud with 412 (preconditionForUpdateNoCAS) instead of silently
	// degrading to a start-of-request check.
	//
	// Precision note: req.Atime/req.Mtime forward as UnixNano()
	// nanoseconds; sub-second fractions survive end to end, matching the
	// nanosecond storage format (structural: FUSE/REST/CLI share it).
	if !h.preconditionForUpdateNoCAS(w, r, project, req.Path, "utimes") {
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := client.ChtimesContext(r.Context(), project, req.Path, req.Atime.UnixNano(), req.Mtime.UnixNano()); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusOK)
}

// preconditionForUpdateNoCAS enforces the request's preconditions for an
// operation whose storage verb takes no mutate options (copy whole-file,
// link, chmod, chown, utimes, xattrs): a revision If-Match token cannot
// become apply-time compare-and-swap there, so accepting it would silently
// degrade to a start-of-request freshness check. Fail loud with 412 naming
// the endpoint instead; callers needing CAS must use a CAS-capable verb.
// Classic ETag tokens keep start-of-request freshness. ok=false means the
// handler already answered and must return without writing more.
func (h *restHandler) preconditionForUpdateNoCAS(w http.ResponseWriter, r *http.Request, project, targetPath, endpoint string) bool {
	revOpts, err := h.mutationPrecondition(r, project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return false
	}
	if revOpts != nil {
		h.writeMappedError(w, errPreconditionFailed(endpoint+" does not enforce revision compare-and-swap; retry without the revision If-Match token"))
		return false
	}
	return true
}

// preconditionForUpdate enforces the request's If-Match for an operation
// that mutates an existing target, through the shared mutationPrecondition
// funnel (revision CAS where the header carries the current revision,
// classic ETag freshness otherwise, If-None-Match * rejection on existing
// targets in both flavors). It returns the backend CAS options for client
// methods that accept them. Verbs that take no mutate options must use
// preconditionForUpdateNoCAS instead so a revision token fails loud rather
// than silently degrading. ok=false means the handler already answered and
// must return without writing more.
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
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return false
	}
	rev, err := client.RevisionContext(r.Context(), project)
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
