package rest

import (
	"context"
	"errors"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	"github.com/go-chi/chi/v5"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// serveContent serves GET/HEAD /content: raw file bytes. A serve* stream
// (not a handle* JSON route) because the body is user-stored bytes.
//
// Atime note: reads through this endpoint update atime relatime-gated
// in storage (ReadFileAtContext queues TouchFileAccessTime in
// verbs_fs.go, gated by the relatime ladder in fs/atime.go), while FUSE
// reads ride ReadPinnedFileContext (fuse_file.go, no atime queue) and its
// commit redrive reads pass WithSuppressedAtime (fuse_write_ranges.go),
// and session reads serve staged bytes with no atime queue
// (sessions_io.go). That matrix is deliberate per surface; unifying it
// further needs the FUSE and storage layers, not this handler.
func (h *restHandler) serveContent(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := queryFirstParam(r.URL.RawQuery, "path")
	var err error
	spanStarted := h.traceStart(r, "serve-content", project, filePath)
	defer h.traceFinish(r, "serve-content", project, filePath, spanStarted, &err)
	// User-stored bytes share the API origin with the console: never let a
	// browser sniff an uploaded file into an executable representation.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	entry, err := client.StatPathContext(r.Context(), project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if entry.IsDir {
		err = &restStatusError{status: http.StatusConflict, message: fmt.Sprintf("path is a directory: %s", filePath)}
		h.writeMappedError(w, err)
		return
	}
	if entry.IsSymlink {
		target, readErr := client.ReadlinkContext(r.Context(), project, filePath)
		if readErr != nil {
			err = readErr
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
	h.setRevisionHeader(w, r, project)
	if matchEntityTag(r.Header.Get("If-None-Match"), eTag) {
		w.Header().Set("ETag", eTag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	start, end, partial, rerr := parseByteRange(r.Header.Get("Range"), entry.Size)
	if rerr != nil {
		// Unmatched or unsatisfiable ranges fail loud with 416 plus a
		// Content-Range */size hint (never silent truncation to 200):
		// the caller asked for bytes that do not exist. The conformance
		// getRange helper treats 416 as an empty read at that offset.
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", entry.Size))
		err = &restStatusError{status: http.StatusRequestedRangeNotSatisfiable, message: rerr.Error()}
		h.writeMappedError(w, err)
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
	h.streamFileRange(w, r, project, filePath, start, end)
}

func (h *restHandler) handleContentReplace(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := queryFirstParam(r.URL.RawQuery, "path")
	if err := requireNonEmptyPath("path", filePath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	var err error
	spanStarted := h.traceStart(r, "replace", project, filePath)
	defer h.traceFinish(r, "replace", project, filePath, spanStarted, &err)
	entry, exists, revOpts, ok := h.checkReplacePreconditions(w, r, project, filePath)
	if !ok {
		err = errors.New("replace precondition failed")
		return
	}
	created, ok := h.createReplacePlaceholder(w, r, project, filePath, entry, exists)
	if !ok {
		if err == nil {
			err = errors.New("replace placeholder failed")
		}
		return
	}
	// The create call above leaves an empty placeholder behind when the path
	// did not exist (or clobbers a symlink). If the body transfer then
	// fails, remove the placeholder instead of stranding an orphan the
	// client believes was never created.
	// Copy the precondition slice before extending it: revOpts is owned
	// by the precondition helper, and appending to its backing array
	// would corrupt the helper's result if it ever gains spare capacity.
	replaceOpts := append([]shfs.MutateOption{}, revOpts...)
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
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err = client.ReplaceFileFromReaderContext(uploadCtx, project, filePath, r.Body, replaceOpts...); err != nil {
		if created || (exists && entry.IsSymlink) {
			if cleanupErr := client.DeleteFileContext(uploadCtx, project, filePath); cleanupErr != nil {
				logging.Error(h.logger, "failed to clean up placeholder after failed replace", "project", project, "path", filePath, "err", cleanupErr)
			}
		}
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	h.respondWithNode(w, r, project, filePath, status)
}

// checkReplacePreconditions stats the target and enforces If-Match /
// If-None-Match semantics. ok=false means the handler already answered.
func (h *restHandler) checkReplacePreconditions(w http.ResponseWriter, r *http.Request, project, filePath string) (entry *shfs.EntryInfo, exists bool, revOpts []shfs.MutateOption, ok bool) {
	entry, exists, err := h.lookupOptional(r, project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return nil, false, nil, false
	}
	revOpts, revErr := h.mutationPrecondition(r, project, filePath)
	if revErr != nil {
		h.writeMappedError(w, revErr)
		return nil, false, nil, false
	}
	if exists {
		// Type conflicts hold under both If-Match flavors.
		if entry.IsDir {
			h.writeMappedError(w, &restStatusError{status: http.StatusConflict, message: fmt.Sprintf("path is a directory: %s", filePath)})
			return nil, false, nil, false
		}
		if revOpts == nil {
			if err := h.requireMatch(r.Header.Get("If-Match"), restEntryETag(entry)); err != nil {
				if !isPreconditionHeaderEmpty(err) {
					h.writeMappedError(w, err)
					return nil, false, nil, false
				}
			}
			if matchEntityTag(r.Header.Get("If-None-Match"), "*") {
				h.writeMappedError(w, errPreconditionFailed("resource already exists"))
				return nil, false, nil, false
			}
		}
	} else if r.Header.Get("If-Match") != "" {
		h.writeMappedError(w, errPreconditionFailed("resource does not exist"))
		return nil, false, nil, false
	}
	return entry, exists, revOpts, true
}

// createReplacePlaceholder ensures a regular file exists for the body
// transfer. It reports whether the file is newly created (for status and
// failure cleanup). ok=false means the handler already answered.
func (h *restHandler) createReplacePlaceholder(w http.ResponseWriter, r *http.Request, project, filePath string, entry *shfs.EntryInfo, exists bool) (created, ok bool) {
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return false, false
	}
	if !exists {
		if _, err := client.CreateFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return false, false
		}
		return true, true
	}
	if entry.IsSymlink {
		// A symlink is replaced by a regular file, not followed; clear it
		// first because create refuses existing nodes.
		if err := client.DeleteFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return false, false
		}
		if _, err := client.CreateFileContext(r.Context(), project, filePath); err != nil {
			h.writeMappedError(w, err)
			return false, false
		}
	}
	return false, true
}

func (h *restHandler) handleContentPatch(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	filePath := queryFirstParam(r.URL.RawQuery, "path")
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
	op := strings.TrimSpace(queryFirstParam(r.URL.RawQuery, "op"))
	var err error
	spanStarted := h.traceStart(r, "patch", project, filePath, "op", op)
	defer h.traceFinish(r, "patch", project, filePath, spanStarted, &err, "op", op)
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
		err = errBadRequest("query parameter op must be one of append, write, patch, truncate")
		h.writeMappedError(w, err)
		return
	}
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	// Same contract as every other mutation: answer with the fresh node so
	// clients can chain If-Match tokens without a separate stat.
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	h.respondWithNode(w, r, project, filePath, http.StatusOK)
}

// patchOpAppend applies one atomic append from the request body.
func (h *restHandler) patchOpAppend(r *http.Request, project, filePath string) error {
	return h.streamAppendBody(r, project, filePath, r.Body)
}

// patchOpWrite applies one atomic write at the required offset.
func (h *restHandler) patchOpWrite(r *http.Request, project, filePath string) error {
	offset, err := parseNonNegativeInt(queryFirstParam(r.URL.RawQuery, "offset"), "offset")
	if err != nil {
		return err
	}
	return h.streamWriteBody(r, project, filePath, r.Body, offset)
}

// patchOpPatch applies one range replacement (offset/delete_size/edit).
func (h *restHandler) patchOpPatch(r *http.Request, project, filePath string) error {
	offset, err := parseNonNegativeInt(queryFirstParam(r.URL.RawQuery, "offset"), "offset")
	if err != nil {
		return err
	}
	deleteSize, err := parseNonNegativeInt(queryFirstParam(r.URL.RawQuery, "delete_size"), "delete_size")
	if err != nil {
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
	client, err := h.clientFor(r)
	if err != nil {
		return err
	}
	_, err = client.PatchFileContext(r.Context(), project, filePath, offset, deleteSize, edit, revOpts...)
	return err
}

// patchOpTruncate resizes the file to the required size.
func (h *restHandler) patchOpTruncate(r *http.Request, project, filePath string) error {
	size, err := parseNonNegativeInt(queryFirstParam(r.URL.RawQuery, "size"), "size")
	if err != nil {
		return err
	}
	revOpts, err := h.mutationPrecondition(r, project, filePath)
	if err != nil {
		return err
	}
	client, err := h.clientFor(r)
	if err != nil {
		return err
	}
	_, err = client.TruncateFileContext(r.Context(), project, filePath, size, revOpts...)
	return err
}
