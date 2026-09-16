package rest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	"github.com/go-chi/chi/v5"
)

type nodeResponse struct {
	Project string          `json:"project"`
	Entry   *shfs.EntryInfo `json:"entry"`
	ETag    string          `json:"etag,omitempty"`
}

type entriesResponse struct {
	Project string          `json:"project"`
	Path    string          `json:"path"`
	Entries []shfs.DirEntry `json:"entries"`
}

type xattrListResponse struct {
	Project string   `json:"project"`
	Path    string   `json:"path"`
	Names   []string `json:"names"`
}

type revisionsResponse struct {
	Project   string                      `json:"project"`
	Revisions []metadata.MetadataRevision `json:"revisions"`
}

type pathRequest struct {
	Path string `json:"path"`
}

type renameRequest struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
}

type copyRequest struct {
	SrcPath string `json:"src_path"`
	DstPath string `json:"dst_path"`
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

func (h *restHandler) handleNodes(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := r.URL.Query().Get("path")
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		eTag := restEntryETag(entry)
		h.setRevisionHeader(w, r, project)
		if h.ifNoneMatchSatisfied(r.Header.Get("If-None-Match"), eTag) {
			w.Header().Set("ETag", eTag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", eTag)
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		h.writeJSON(w, http.StatusOK, nodeResponse{Project: project, Entry: entry, ETag: eTag})
	case http.MethodDelete:
		entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		revOpts, perr := h.mutationPrecondition(r, project, targetPath)
		if perr != nil {
			h.writeMappedError(w, perr)
			return
		}
		if entry.IsDir {
			recursive, ok := parseQueryBool(r.URL.Query().Get("recursive"))
			if !ok {
				h.writeError(w, http.StatusBadRequest, "invalid_request", "recursive must be a boolean (true/false)")
				return
			}
			if recursive {
				h.writeError(w, http.StatusNotImplemented, "recursive_delete_unsupported", "recursive directory deletion is not supported")
				return
			}
		}
		if entry.IsDir {
			err = h.clientFor(r).RmdirContext(r.Context(), project, targetPath, revOpts...)
		} else {
			err = h.clientFor(r).DeleteFileContext(r.Context(), project, targetPath, revOpts...)
		}
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodHead, http.MethodDelete)
	}
}

func (h *restHandler) handleChildren(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	dirPath := r.URL.Query().Get("path")
	entries, err := h.clientFor(r).ReadDirContext(r.Context(), project, dirPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, entriesResponse{Project: project, Path: dirPath, Entries: entries})
}

func (h *restHandler) handleContentRead(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := r.URL.Query().Get("path")
	// User-stored bytes share the API origin with the console: never let a
	// browser sniff an uploaded file into an executable representation.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if entry.IsDir {
		h.writeError(w, http.StatusConflict, "is_directory", fmt.Sprintf("path is a directory: %s", filePath))
		return
	}
	if entry.IsSymlink {
		target, readErr := h.clientFor(r).ReadlinkContext(r.Context(), project, filePath)
		if readErr != nil {
			h.writeMappedError(w, readErr)
			return
		}
		w.Header().Set("Content-Type", "application/symlink-target")
		w.Header().Set("X-StorHub-Symlink-Target", target)
		w.Header().Set("Content-Length", strconv.Itoa(len(target)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.WriteString(w, target)
		return
	}

	eTag := restEntryETag(entry)
	if h.ifNoneMatchSatisfied(r.Header.Get("If-None-Match"), eTag) {
		w.Header().Set("ETag", eTag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	start, end, partial, err := parseByteRange(r.Header.Get("Range"), entry.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", entry.Size))
		h.writeError(w, http.StatusRequestedRangeNotSatisfiable, "range_not_satisfiable", err.Error())
		return
	}
	contentLength := end - start
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, entry.Size))
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("ETag", eTag)
	w.Header().Set("Content-Type", detectContentType(filePath))
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	w.WriteHeader(status)
	sent := int64(0)
	for offset := start; offset < end; {
		readLen := h.opts.StreamChunkSize
		if remaining := end - offset; remaining < readLen {
			readLen = remaining
		}
		chunk, readErr := h.clientFor(r).ReadFileAtContext(r.Context(), project, filePath, offset, readLen)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			logging.Error(h.logger, "stream aborted mid-response", "project", project, "path", filePath, "offset", offset, "sent", sent, "expected", end-start, "err", readErr)
			return
		}
		if len(chunk) == 0 {
			break
		}
		if _, writeErr := w.Write(chunk); writeErr != nil {
			return
		}
		// Advance by the bytes actually read: short reads must not skip
		// data (a concurrent shrink clamps ReadFileAt to live size).
		offset += int64(len(chunk))
		sent += int64(len(chunk))
	}
}

func (h *restHandler) handleContentReplace(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := r.URL.Query().Get("path")
	if err := requireNonEmptyPath("path", filePath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	entry, exists, err := h.lookupOptional(r, project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	replaceRevOpts, replaceRevErr := h.mutationPrecondition(r, project, filePath)
	if replaceRevErr != nil {
		h.writeMappedError(w, replaceRevErr)
		return
	}
	if exists {
		// Type conflicts hold under both If-Match flavors.
		if entry.IsDir {
			h.writeError(w, http.StatusConflict, "is_directory", fmt.Sprintf("path is a directory: %s", filePath))
			return
		}
		if replaceRevOpts == nil {
			if err := h.requireMatch(r.Header.Get("If-Match"), restEntryETag(entry)); err != nil {
				if !isPreconditionHeaderEmpty(err) {
					h.writeMappedError(w, err)
					return
				}
			}
			if h.ifNoneMatchStar(r.Header.Get("If-None-Match")) {
				h.writeMappedError(w, errPreconditionFailed("resource already exists"))
				return
			}
		}
	} else if r.Header.Get("If-Match") != "" {
		h.writeMappedError(w, errPreconditionFailed("resource does not exist"))
		return
	}

	created := !exists
	if !exists {
		if _, err := h.clientFor(r).CreateFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
	} else if entry.IsSymlink {
		// A symlink is replaced by a regular file, not followed; clear it
		// first because create refuses existing nodes.
		if err := h.clientFor(r).DeleteFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
		if _, err := h.clientFor(r).CreateFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
	}
	// The create call above leaves an empty placeholder behind when the path
	// did not exist (or clobbers a symlink). If the body transfer then
	// fails, remove the placeholder instead of stranding an orphan the
	// client believes was never created.
	replaceOpts := replaceRevOpts
	if r.ContentLength >= 0 {
		replaceOpts = append(replaceOpts, shfs.WithSize(r.ContentLength))
	}
	// Accepted uploads outlive their HTTP request: a client disconnect at
	// minute 25 of a 400 MB transfer must not orphan half-uploaded chunks
	// (and previously skipped compensating deletes, since those were bound
	// to the same dying context). WithoutCancel keeps identity/logging
	// values while dropping cancellation; the scaled transferDeadline inside
	// the GitHub client remains the real bound. If the client is gone, the
	// final response write simply fails silently.
	uploadCtx := context.WithoutCancel(r.Context())
	if _, err := h.clientFor(r).ReplaceFileFromReaderContext(uploadCtx, project, filePath, r.Body, replaceOpts...); err != nil {
		if created || (exists && entry.IsSymlink) {
			if cleanupErr := h.clientFor(r).DeleteFileContext(r.Context(), project, filePath); cleanupErr != nil {
				h.logger.Error("failed to clean up placeholder after failed replace", "project", project, "path", filePath, "err", cleanupErr)
			}
		}
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, filePath, ternaryStatus(created, http.StatusCreated, http.StatusOK))
}

func (h *restHandler) handleContentPatch(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := r.URL.Query().Get("path")
	if err := requireNonEmptyPath("path", filePath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	ifMatch := r.Header.Get("If-Match")
	if strings.TrimSpace(ifMatch) != "" {
		// Fail fast on a stale token before reading the body. Flavor is
		// resolved first so a current-revision CAS token is honored rather
		// than misjudged against attribute ETags; the authoritative check
		// runs again just before each mutation below.
		if _, ferr := h.mutationPrecondition(r, project, filePath); ferr != nil {
			h.writeMappedError(w, ferr)
			return
		}
	}
	op := strings.TrimSpace(r.URL.Query().Get("op"))
	var err error
	switch op {
	case "append":
		err = h.patchOpAppend(r, project, filePath)
	case "write":
		err = h.patchOpWrite(r, project, filePath)
	case "patch":
		err = h.patchOpPatch(r, project, filePath)
	case "truncate":
		err = h.patchOpTruncate(r, project, filePath)
	default:
		h.writeError(w, http.StatusBadRequest, "invalid_patch_op", "query parameter op must be one of append, write, patch, truncate")
		return
	}
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	// Same contract as every other mutation: answer with the fresh node so
	// clients can chain If-Match tokens without a separate stat.
	h.respondWithNode(w, r, project, filePath, http.StatusOK)
}

// patchOpAppend applies one atomic append from the request body.
func (h *restHandler) patchOpAppend(r *http.Request, project, filePath string) error {
	return h.streamAppendBody(r, project, filePath, r.Body)
}

// patchOpWrite applies one atomic write at the required offset.
func (h *restHandler) patchOpWrite(r *http.Request, project, filePath string) error {
	offset, err := parseRequiredInt64(r.URL.Query().Get("offset"), "offset")
	if err != nil {
		return err
	}
	if err := requireNonNegative("offset", offset); err != nil {
		return err
	}
	return h.streamWriteBody(r, project, filePath, r.Body, offset)
}

// patchOpPatch applies one range replacement (offset/delete_size/edit).
func (h *restHandler) patchOpPatch(r *http.Request, project, filePath string) error {
	offset, err := parseRequiredInt64(r.URL.Query().Get("offset"), "offset")
	if err != nil {
		return err
	}
	if err := requireNonNegative("offset", offset); err != nil {
		return err
	}
	deleteSize, err := parseRequiredInt64(r.URL.Query().Get("delete_size"), "delete_size")
	if err != nil {
		return err
	}
	if err := requireNonNegative("delete_size", deleteSize); err != nil {
		return err
	}
	edit, err := h.readSizedBody(r.Body, "patch payload exceeds the configured limit")
	if err != nil {
		return err
	}
	revOpts, err := h.mutationPrecondition(r, project, filePath)
	if err != nil {
		return err
	}
	_, err = h.clientFor(r).PatchFileContext(r.Context(), project, filePath, offset, deleteSize, edit, revOpts...)
	return err
}

// patchOpTruncate resizes the file to the required size.
func (h *restHandler) patchOpTruncate(r *http.Request, project, filePath string) error {
	size, err := parseRequiredInt64(r.URL.Query().Get("size"), "size")
	if err != nil {
		return err
	}
	if err := requireNonNegative("size", size); err != nil {
		return err
	}
	revOpts, err := h.mutationPrecondition(r, project, filePath)
	if err != nil {
		return err
	}
	_, err = h.clientFor(r).TruncateFileContext(r.Context(), project, filePath, size, revOpts...)
	return err
}

// enforceFreshPrecondition restates the client's If-Match requirement against
// the resource's current state immediately before a mutating call. Checking
// only a snapshot taken earlier in the request would apply mutations to a
// state the client never saw; failing closed with 412 here closes most of
// that window.
func (h *restHandler) enforceFreshPrecondition(r *http.Request, project, targetPath string) error {
	ifMatch := r.Header.Get("If-Match")
	if strings.TrimSpace(ifMatch) == "" {
		return nil
	}
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		return err
	}
	return h.requireMatch(ifMatch, restEntryETag(entry))
}

// revisionPrecondition implements the revision flavor of If-Match. When the
// header carries the project's CURRENT metadata revision, it is returned as
// an fs.WithExpectedRevision option so storage re-verifies against remote
// HEAD immediately before applying - true compare-and-swap. Header values
// that are not the current revision (e.g. classic attribute ETags from
// earlier clients) yield no options here; those flows keep their existing
// freshness semantics via enforceFreshPrecondition, which still answers 412
// on staleness. Absent If-Match yields no options.
func (h *restHandler) revisionPrecondition(r *http.Request, project string) (opts []shfs.MutateOption, matched bool, err error) {
	ifMatch := strings.TrimSpace(r.Header.Get("If-Match"))
	if ifMatch == "" {
		return nil, false, nil
	}
	rev, err := h.clientFor(r).RevisionContext(r.Context(), project)
	if err != nil {
		return nil, false, err
	}
	if rerr := h.requireMatch(unquoteEntityTag(ifMatch), rev); rerr != nil {
		// Not a revision token: defer to the attribute-ETag freshness check.
		return nil, false, nil
	}
	return []shfs.MutateOption{shfs.WithExpectedRevision(rev)}, true, nil
}

// unquoteEntityTag strips HTTP entity-tag quotes so tokens published via
// X-StorHub-Revision compare equal whether or not the client quoted them
// (RFC 9110 clients quote If-Match values).
func unquoteEntityTag(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		return v[1 : len(v)-1]
	}
	return v
}

// mutationPrecondition resolves the If-Match flavor ONCE and enforces it
// for a mutating flow: a token equal to the project's CURRENT metadata
// revision becomes backend compare-and-swap options (apply-time check,
// immune to attribute collisions), while any other non-empty token keeps
// classic attribute-ETag freshness against freshly-statted state. Every
// mutating endpoint funnels through here so neither flavor can be
// short-circuited by a stale fast path.
func (h *restHandler) mutationPrecondition(r *http.Request, project, filePath string) (opts []shfs.MutateOption, err error) {
	revOpts, revMatched, rerr := h.revisionPrecondition(r, project)
	if rerr != nil {
		return nil, rerr
	}
	if revMatched {
		return revOpts, nil
	}
	if err := h.enforceFreshPrecondition(r, project, filePath); err != nil {
		return nil, err
	}
	return nil, nil
}

func (h *restHandler) handleXAttrs(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := r.URL.Query().Get("path")
	names, err := h.clientFor(r).ListXAttrContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, xattrListResponse{Project: project, Path: targetPath, Names: names})
}

func (h *restHandler) handleXAttrValue(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := r.URL.Query().Get("path")
	name := r.URL.Query().Get("name")
	switch r.Method {
	case http.MethodGet:
		value, err := h.clientFor(r).GetXAttrContext(r.Context(), project, targetPath, name)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-StorHub-XAttr-Name", name)
		w.Header().Set("Content-Length", strconv.Itoa(len(value)))
		_, _ = w.Write(value)
	case http.MethodPut:
		payload, err := io.ReadAll(io.LimitReader(r.Body, h.opts.MaxPatchBodySize+1))
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		if int64(len(payload)) > h.opts.MaxPatchBodySize {
			h.writeError(w, http.StatusRequestEntityTooLarge, "xattr_too_large", "xattr value exceeds the configured limit")
			return
		}
		if err := h.clientFor(r).SetXAttrContext(r.Context(), project, targetPath, name, payload); err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := h.clientFor(r).RemoveXAttrContext(r.Context(), project, targetPath, name); err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func (h *restHandler) handleRevisions(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	revisions, err := h.clientFor(r).ListMetadataRevisionsContext(r.Context(), project)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, revisionsResponse{Project: project, Revisions: revisions})
}

func (h *restHandler) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.clientFor(r).CreateFileContext(r.Context(), project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusCreated)
}

func (h *restHandler) handleMkdir(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).MkdirContext(r.Context(), project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, r, project, req.Path, http.StatusCreated)
}

func (h *restHandler) handleRmdir(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).RmdirContext(r.Context(), project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleUnlink(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := requireNonEmptyPath("path", req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.clientFor(r).DeleteFileContext(r.Context(), project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleRename(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var req renameRequest
	if err := h.decodeJSON(r, &req); err != nil {
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
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	src := strings.TrimSpace(req.SrcPath)
	if src == "" {
		src = strings.TrimSpace(req.OldPath)
	}
	dst := strings.TrimSpace(req.DstPath)
	if dst == "" {
		dst = strings.TrimSpace(req.NewPath)
	}
	if src == "" || dst == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "src_path and dst_path are required")
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
	if err := h.decodeJSON(r, &req); err != nil {
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
	if err := h.decodeJSON(r, &req); err != nil {
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
	if err := h.decodeJSON(r, &req); err != nil {
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
	if err := h.decodeJSON(r, &req); err != nil {
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
	if err := h.decodeJSON(r, &req); err != nil {
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

func (h *restHandler) respondWithNode(w http.ResponseWriter, r *http.Request, project, targetPath string, status int) {
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, status, nodeResponse{Project: project, Entry: entry, ETag: restEntryETag(entry)})
}

func (h *restHandler) lookupOptional(r *http.Request, project, targetPath string) (*shfs.EntryInfo, bool, error) {
	entry, err := h.clientFor(r).StatPathContext(r.Context(), project, targetPath)
	if err == nil {
		return entry, true, nil
	}
	if mappedStatus(err) == http.StatusNotFound {
		return nil, false, nil
	}
	return nil, false, err
}

// streamWriteBody applies an entire write atomically by buffering the body
// up to MaxPatchBodySize and issuing a single WriteFileAt call. Chunked
// multi-call writes would expose torn intermediate states to concurrent
// readers and leave partial data committed on failure; bodies beyond the
// cap are rejected so clients fall back to the atomic full-file PUT.
func (h *restHandler) streamWriteBody(r *http.Request, project, filePath string, body io.Reader, offset int64) error {
	payload, err := h.readSizedBody(body, fmt.Sprintf("mutation body exceeds the configured limit of %d bytes; use full-file PUT for large payloads", h.opts.MaxPatchBodySize))
	if err != nil {
		return err
	}
	revOptsW, perrW := h.mutationPrecondition(r, project, filePath)
	if perrW != nil {
		return perrW
	}
	_, err = h.clientFor(r).WriteFileAtContext(r.Context(), project, filePath, offset, payload, revOptsW...)
	return err
}

// streamAppendBody mirrors streamWriteBody: one AppendFile call, or 413.
func (h *restHandler) streamAppendBody(r *http.Request, project, filePath string, body io.Reader) error {
	payload, err := h.readSizedBody(body, fmt.Sprintf("mutation body exceeds the configured limit of %d bytes; use full-file PUT for large payloads", h.opts.MaxPatchBodySize))
	if err != nil {
		return err
	}
	revOptsA, perrA := h.mutationPrecondition(r, project, filePath)
	if perrA != nil {
		return perrA
	}
	_, err = h.clientFor(r).AppendFileContext(r.Context(), project, filePath, payload, revOptsA...)
	return err
}

// readSizedBody buffers at most MaxPatchBodySize bytes from a mutation body;
// oversized payloads fail with the caller's 413 wording so the existing
// endpoint messages stay unchanged. It replaces the former readMutationBody /
// readPatchBody pair, which differed only in that message.
func (h *restHandler) readSizedBody(body io.Reader, tooLargeMessage string) ([]byte, error) {
	payload, err := readCappedBody(body, h.opts.MaxPatchBodySize)
	if err != nil {
		if errors.Is(err, errCappedBody) {
			return nil, errPayloadTooLarge(tooLargeMessage)
		}
		return nil, err
	}
	return payload, nil
}

// readCappedBody buffers at most limit+1 bytes; a nil return with errCapped
// signals the caller to map its own size-specific 413 message so existing
// endpoint wordings stay unchanged.
var errCappedBody = errPayloadTooLarge("body exceeds the configured limit")

func readCappedBody(body io.Reader, limit int64) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > limit {
		return nil, errCappedBody
	}
	return payload, nil
}

func (h *restHandler) ifNoneMatchStar(header string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.TrimSpace(part) == "*" {
			return true
		}
	}
	return false
}

func (h *restHandler) ifNoneMatchSatisfied(header, eTag string) bool {
	if strings.TrimSpace(header) == "" {
		return false
	}
	for _, part := range strings.Split(header, ",") {
		value := strings.TrimSpace(part)
		if value == "*" || value == eTag {
			return true
		}
	}
	return false
}

func (h *restHandler) requireMatch(header, eTag string) error {
	if strings.TrimSpace(header) == "" {
		return errEmptyPrecondition
	}
	for _, part := range strings.Split(header, ",") {
		value := strings.TrimSpace(part)
		if value == "*" || value == eTag {
			return nil
		}
	}
	return errPreconditionFailed("etag precondition failed")
}

// setRevisionHeader publishes the project's current metadata revision as a
// response header so clients can obtain CAS tokens for later If-Match use.
// Best effort: a revision fetch failure never fails the read.
func (h *restHandler) setRevisionHeader(w http.ResponseWriter, r *http.Request, project string) {
	rev, err := h.clientFor(r).RevisionContext(r.Context(), project)
	if err != nil || rev == "" {
		return
	}
	w.Header().Set("X-StorHub-Revision", rev)
}

func restEntryETag(entry *shfs.EntryInfo) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, entry.Path)
	_, _ = io.WriteString(hash, "|")
	_, _ = io.WriteString(hash, fmt.Sprintf("%d|%d|%d|%d|%d|%t|%t|%s|%s", entry.Inode, entry.Size, entry.Mode, entry.UID, entry.GID, entry.IsDir, entry.IsSymlink, time.Unix(entry.ModifiedAt, 0).UTC().Format(time.RFC3339Nano), time.Unix(entry.ChangedAt, 0).UTC().Format(time.RFC3339Nano)))
	if entry.SymlinkTarget != "" {
		_, _ = io.WriteString(hash, "|")
		_, _ = io.WriteString(hash, entry.SymlinkTarget)
	}
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
}

// detectContentType maps a file extension to an INLINE-SAFE media type.
// The API shares its origin with the console SPA, so a stored upload must
// never be rendered as active content: anything outside the allowlist
// (HTML, script, XML, and even SVG, which can carry scripts) is served as
// opaque bytes. Combined with X-Content-Type-Options: nosniff this removes
// the stored-XSS surface on /content.
func detectContentType(filePath string) string {
	if ext := path.Ext(filePath); ext != "" {
		if typ := mime.TypeByExtension(ext); typ != "" && inlineSafeContentType(typ) {
			return typ
		}
	}
	return "application/octet-stream"
}

func inlineSafeContentType(contentType string) bool {
	base, _, _ := strings.Cut(contentType, ";")
	base = strings.ToLower(strings.TrimSpace(base))
	switch {
	case base == "text/plain" || base == "application/pdf":
		return true
	case base == "image/svg+xml":
		return false // scripts live here
	case strings.HasPrefix(base, "image/"),
		strings.HasPrefix(base, "audio/"),
		strings.HasPrefix(base, "video/"):
		return true
	default:
		return false
	}
}

func parseByteRange(header string, size int64) (start, end int64, partial bool, err error) {
	if size < 0 {
		return 0, 0, false, fmt.Errorf("invalid object size")
	}
	if strings.TrimSpace(header) == "" {
		return 0, size, false, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, fmt.Errorf("range header must start with bytes=")
	}
	raw := strings.TrimPrefix(header, "bytes=")
	if strings.Contains(raw, ",") {
		return 0, 0, false, fmt.Errorf("multiple ranges are not supported")
	}
	parts := strings.SplitN(raw, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid byte range")
	}
	if size == 0 {
		return 0, 0, false, fmt.Errorf("range cannot be satisfied for an empty file")
	}
	if parts[0] == "" {
		suffix, parseErr := strconv.ParseInt(parts[1], 10, 64)
		if parseErr != nil || suffix <= 0 {
			return 0, 0, false, fmt.Errorf("invalid byte range")
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size, true, nil
	}
	start, parseErr := strconv.ParseInt(parts[0], 10, 64)
	if parseErr != nil || start < 0 {
		return 0, 0, false, fmt.Errorf("invalid byte range")
	}
	if start >= size {
		return 0, 0, false, fmt.Errorf("range start exceeds file size")
	}
	if parts[1] == "" {
		return start, size, true, nil
	}
	inclusiveEnd, parseErr := strconv.ParseInt(parts[1], 10, 64)
	if parseErr != nil || inclusiveEnd < start {
		return 0, 0, false, fmt.Errorf("invalid byte range")
	}
	if inclusiveEnd >= size {
		inclusiveEnd = size - 1
	}
	return start, inclusiveEnd + 1, true, nil
}

func parseRequiredInt64(raw, field string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, errBadRequest(field + " is required")
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, errBadRequest(field + " must be a valid integer")
	}
	return value, nil
}

// parseQueryBool interprets a query-string boolean. Absent means false;
// recognized truthy/falsey spellings resolve; anything else reports ok=false
// so the caller answers 400 instead of silently taking the other branch.
func parseQueryBool(raw string) (value bool, ok bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "false", "0", "no":
		return false, true
	case "true", "1", "yes":
		return true, true
	default:
		return false, false
	}
}

func ternaryStatus(cond bool, yes, no int) int {
	if cond {
		return yes
	}
	return no
}
