package rest

import (
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

// handleNodeGet serves GET/HEAD /nodes: stat one node with ETag/304 support.
func (h *restHandler) handleNodeGet(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := queryFirstParam(r.URL.RawQuery, "path")
	var err error
	spanStarted := h.traceStart(r, "node-get", project, targetPath)
	defer h.traceFinish(r, "node-get", project, targetPath, spanStarted, &err)
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	entry, err := client.StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	eTag := restEntryETag(entry)
	h.setRevisionHeader(w, r, project)
	if matchEntityTag(r.Header.Get("If-None-Match"), eTag) {
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
}

// handleNodeDelete serves DELETE /nodes: remove one file or empty dir.
func (h *restHandler) handleNodeDelete(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := queryFirstParam(r.URL.RawQuery, "path")
	var err error
	spanStarted := h.traceStart(r, "node-delete", project, targetPath)
	defer h.traceFinish(r, "node-delete", project, targetPath, spanStarted, &err)
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	entry, err := client.StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	revOpts, perr := h.mutationPrecondition(r, project, targetPath)
	if perr != nil {
		err = perr
		h.writeMappedError(w, perr)
		return
	}
	if entry.IsDir {
		recursive, berr := parseBoolStrict(queryFirstParam(r.URL.RawQuery, "recursive"), "recursive")
		if berr != nil {
			err = berr
			h.writeMappedError(w, berr)
			return
		}
		if recursive {
			err = &restStatusError{status: http.StatusNotImplemented, message: "recursive directory deletion is not supported"}
			h.writeError(w, http.StatusNotImplemented, "not_implemented", "recursive directory deletion is not supported")
			return
		}
	}
	if entry.IsDir {
		err = client.RmdirContext(r.Context(), project, targetPath, revOpts...)
	} else {
		err = client.DeleteFileContext(r.Context(), project, targetPath, revOpts...)
	}
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleChildren(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	dirPath := queryFirstParam(r.URL.RawQuery, "path")
	var err error
	spanStarted := h.traceStart(r, "children", project, dirPath)
	defer h.traceFinish(r, "children", project, dirPath, spanStarted, &err)
	// Unbounded by design: no limit/offset parameters. Directory reads
	// resolve against the published tree in one storage call and the
	// response is one JSON document; adding pagination would need a
	// storage cursor to stay consistent across commits, which does not
	// exist. Clients needing bounded transfers page at a coarser grain
	// (per-path stat loops) instead.
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	entries, err := client.ReadDirContext(r.Context(), project, dirPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.setRevisionHeader(w, r, project)
	h.writeJSON(w, http.StatusOK, entriesResponse{Project: project, Path: dirPath, Entries: entries})
}

// streamFileRange streams [start,end) in StreamChunkSize windows. Shared by
// serveContent and share downloads so the short-read comment lives once.
func (h *restHandler) streamFileRange(w http.ResponseWriter, r *http.Request, project, filePath string, start, end int64) {
	started := time.Now().UTC()
	client, err := h.clientFor(r)
	if err != nil {
		// Headers are already on the wire (the caller wrote the status
		// before streaming), so no error document can follow: log and
		// truncate like a mid-response read failure.
		logging.Error(h.logger, "stream aborted before first byte", "project", project, "path", filePath, "elapsed", time.Since(started), "err", err)
		return
	}
	sent := int64(0)
	for offset := start; offset < end; {
		readLen := h.opts.StreamChunkSize
		if remaining := end - offset; remaining < readLen {
			readLen = remaining
		}
		chunk, readErr := client.ReadFileAtContext(r.Context(), project, filePath, offset, readLen)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			logging.Error(h.logger, "stream aborted mid-response", "project", project, "path", filePath, "offset", offset, "sent", sent, "expected", end-start, "elapsed", time.Since(started), "err", readErr)
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
	client, err := h.clientFor(r)
	if err != nil {
		return err
	}
	entry, err := client.StatPathContext(r.Context(), project, targetPath)
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
//
// Status contract: CAS divergence always surfaces as
// fs.ErrPreconditionFailed, mapped to 412. 409 stays reserved for resource
// conflicts (AlreadyExists, NoReplace, unlinked-handle states), so CAS
// clients only need the 412 arm; no CAS path emits 409.
func (h *restHandler) revisionPrecondition(r *http.Request, project string) (opts []shfs.MutateOption, matched bool, err error) {
	ifMatch := strings.TrimSpace(r.Header.Get("If-Match"))
	if ifMatch == "" {
		return nil, false, nil
	}
	client, err := h.clientFor(r)
	if err != nil {
		return nil, false, err
	}
	rev, err := client.RevisionContext(r.Context(), project)
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
//
// If-None-Match * is enforced here too (RFC 9110 precedence over
// If-Match): on an existing guard target the request fails 412, so the
// create-only guard can never be skipped by pairing * with a matching
// If-Match token. A missing target proceeds so the handler answers its
// own 404/create outcome.
func (h *restHandler) mutationPrecondition(r *http.Request, project, filePath string) (opts []shfs.MutateOption, err error) {
	revOpts, revMatched, rerr := h.revisionPrecondition(r, project)
	if rerr != nil {
		return nil, rerr
	}
	if matchEntityTag(r.Header.Get("If-None-Match"), "*") {
		if serr := h.rejectIfNoneMatchStar(r, project, filePath); serr != nil {
			return nil, serr
		}
	}
	if revMatched {
		return revOpts, nil
	}
	if err := h.enforceFreshPrecondition(r, project, filePath); err != nil {
		return nil, err
	}
	return nil, nil
}

// rejectIfNoneMatchStar enforces If-None-Match * against the guard target:
// existing answers 412 (create-only semantics), missing proceeds, and any
// other stat failure propagates so an unprovable state never applies.
func (h *restHandler) rejectIfNoneMatchStar(r *http.Request, project, filePath string) error {
	client, err := h.clientFor(r)
	if err != nil {
		return err
	}
	if _, err := client.StatPathContext(r.Context(), project, filePath); err == nil {
		return errPreconditionFailed("resource already exists")
	} else if mappedStatus(err) != http.StatusNotFound {
		return err
	}
	return nil
}

func (h *restHandler) handleXAttrs(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := queryFirstParam(r.URL.RawQuery, "path")
	var err error
	spanStarted := h.traceStart(r, "xattrs", project, targetPath)
	defer h.traceFinish(r, "xattrs", project, targetPath, spanStarted, &err)
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	names, err := client.ListXAttrContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.setRevisionHeader(w, r, project)
	h.writeJSON(w, http.StatusOK, xattrListResponse{Project: project, Path: targetPath, Names: names})
}

// handleXAttrGet serves GET /xattrs/value: raw xattr bytes.
//
// X-Content-Type-Options: nosniff is set (like /content) so a browser never
// sniffs stored xattr bytes into an executable representation. No
// Content-Disposition: attachment is set: xattr values are fetched via XHR
// for inline use (never navigated to), so a download affordance would break
// the console's read path without adding security beyond nosniff+octet-stream.
func (h *restHandler) handleXAttrGet(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := queryFirstParam(r.URL.RawQuery, "path")
	var err error
	spanStarted := h.traceStart(r, "xattr-get", project, targetPath)
	defer h.traceFinish(r, "xattr-get", project, targetPath, spanStarted, &err)
	name, ok := h.requireXAttrName(w, r)
	if !ok {
		err = errBadRequest("invalid xattr name")
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	value, err := client.GetXAttrContext(r.Context(), project, targetPath, name)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.setRevisionHeader(w, r, project)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-StorHub-XAttr-Name", name)
	w.Header().Set("Content-Length", strconv.Itoa(len(value)))
	_, _ = w.Write(value)
}

// handleXAttrPut serves PUT /xattrs/value.
func (h *restHandler) handleXAttrPut(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := queryFirstParam(r.URL.RawQuery, "path")
	var err error
	spanStarted := h.traceStart(r, "xattr-put", project, targetPath)
	defer h.traceFinish(r, "xattr-put", project, targetPath, spanStarted, &err)
	name, ok := h.requireXAttrName(w, r)
	if !ok {
		err = errBadRequest("invalid xattr name")
		return
	}
	// SetXAttrContext takes no mutate options: a revision token fails loud
	// with 412 (preconditionForUpdateNoCAS) instead of silently degrading
	// to a start-of-request check.
	if !h.preconditionForUpdateNoCAS(w, r, project, targetPath, "xattr-put") {
		err = errors.New("xattr precondition failed")
		return
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, h.opts.MaxPatchBodySize+1))
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if int64(len(payload)) > h.opts.MaxPatchBodySize {
		err = errPayloadTooLarge("xattr value exceeds the configured limit")
		h.writeMappedError(w, err)
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err = client.SetXAttrContext(r.Context(), project, targetPath, name, payload); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleXAttrDelete serves DELETE /xattrs/value.
func (h *restHandler) handleXAttrDelete(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	targetPath := queryFirstParam(r.URL.RawQuery, "path")
	var err error
	spanStarted := h.traceStart(r, "xattr-delete", project, targetPath)
	defer h.traceFinish(r, "xattr-delete", project, targetPath, spanStarted, &err)
	name, ok := h.requireXAttrName(w, r)
	if !ok {
		err = errBadRequest("invalid xattr name")
		return
	}
	// RemoveXAttrContext takes no mutate options: like xattr-put, a
	// revision token fails loud with 412 (preconditionForUpdateNoCAS).
	if !h.preconditionForUpdateNoCAS(w, r, project, targetPath, "xattr-delete") {
		err = errors.New("xattr precondition failed")
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err = client.RemoveXAttrContext(r.Context(), project, targetPath, name); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if !h.maybeDrain(w, r, project) {
		if err == nil {
			err = errors.New("drain failed")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// requireXAttrName validates the ?name= query: non-empty (like path) and
// free of CR/LF so Header.Set cannot split the response. ok=false means
// the handler already answered 400.
func (h *restHandler) requireXAttrName(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := queryFirstParam(r.URL.RawQuery, "name")
	if strings.TrimSpace(name) == "" {
		h.writeMappedError(w, errBadRequest("name is required"))
		return "", false
	}
	if strings.ContainsAny(name, "\r\n") {
		h.writeMappedError(w, errBadRequest("name must not contain CR or LF"))
		return "", false
	}
	return name, true
}

func (h *restHandler) handleRevisions(w http.ResponseWriter, r *http.Request) {
	project := chi.URLParam(r, "project")
	var err error
	spanStarted := h.traceStart(r, "revisions", project, "")
	defer h.traceFinish(r, "revisions", project, "", spanStarted, &err)
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	revisions, err := client.ListMetadataRevisionsContext(r.Context(), project)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.setRevisionHeader(w, r, project)
	h.writeJSON(w, http.StatusOK, revisionsResponse{Project: project, Revisions: revisions})
}

func (h *restHandler) respondWithNode(w http.ResponseWriter, r *http.Request, project, targetPath string, status int) {
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	entry, err := client.StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, status, nodeResponse{Project: project, Entry: entry, ETag: restEntryETag(entry)})
}

func (h *restHandler) lookupOptional(r *http.Request, project, targetPath string) (*shfs.EntryInfo, bool, error) {
	client, err := h.clientFor(r)
	if err != nil {
		return nil, false, err
	}
	entry, err := client.StatPathContext(r.Context(), project, targetPath)
	if err == nil {
		return entry, true, nil
	}
	if mappedStatus(err) == http.StatusNotFound {
		return nil, false, nil
	}
	return nil, false, err
}

// streamSizedBody buffers one mutation body up to MaxPatchBodySize and
// applies it atomically via fn. Chunked multi-call writes would expose torn
// intermediate states to concurrent readers and leave partial data committed
// on failure; bodies beyond the cap are rejected so clients fall back to
// the atomic full-file PUT. streamWriteBody/streamAppendBody are thin
// wrappers over it.
func (h *restHandler) streamSizedBody(r *http.Request, project, filePath string, body io.Reader, fn func(payload []byte, revOpts []shfs.MutateOption) error) error {
	payload, err := h.readSizedBody(body, fmt.Sprintf("mutation body exceeds the configured limit of %d bytes; use full-file PUT for large payloads", h.opts.MaxPatchBodySize))
	if err != nil {
		return err
	}
	revOpts, perr := h.mutationPrecondition(r, project, filePath)
	if perr != nil {
		return perr
	}
	return fn(payload, revOpts)
}

// streamWriteBody applies an entire write atomically: one WriteFileAt call.
func (h *restHandler) streamWriteBody(r *http.Request, project, filePath string, body io.Reader, offset int64) error {
	return h.streamSizedBody(r, project, filePath, body, func(payload []byte, revOpts []shfs.MutateOption) error {
		client, err := h.clientFor(r)
		if err != nil {
			return err
		}
		_, err = client.WriteFileAtContext(r.Context(), project, filePath, offset, payload, revOpts...)
		return err
	})
}

// streamAppendBody applies an entire append atomically: one AppendFile call.
func (h *restHandler) streamAppendBody(r *http.Request, project, filePath string, body io.Reader) error {
	return h.streamSizedBody(r, project, filePath, body, func(payload []byte, revOpts []shfs.MutateOption) error {
		client, err := h.clientFor(r)
		if err != nil {
			return err
		}
		_, err = client.AppendFileContext(r.Context(), project, filePath, payload, revOpts...)
		return err
	})
}

// readSizedBody buffers at most MaxPatchBodySize bytes from a mutation body;
// oversized payloads fail with the caller's 413 wording so the existing
// endpoint messages stay unchanged. It is the single capped-body reader
// for mutation payloads.
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

// matchEntityTag reports whether an If-* header list (comma-separated,
// whitespace-tolerant) matches eTag or the "*" wildcard. The single owner
// of entity-tag list parsing; requireMatch is its thin error wrapper.
func matchEntityTag(header, eTag string) bool {
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
	if matchEntityTag(header, eTag) {
		return nil
	}
	return errPreconditionFailed("etag precondition failed")
}

// setRevisionHeader publishes the project's current metadata revision as a
// response header so clients can obtain CAS tokens for later If-Match use.
// Best effort: a revision fetch failure never fails the read.
//
// Revision representation rule: headers carry concurrency tokens, bodies
// carry history pointers. If-Match takes attribute ETags or the current
// metadata revision (compare-and-swap); X-StorHub-Revision and ETag publish
// those tokens back in headers; commit_sha bodies name history commits for
// rollback and revert, never concurrency.
func (h *restHandler) setRevisionHeader(w http.ResponseWriter, r *http.Request, project string) {
	client, err := h.clientFor(r)
	if err != nil {
		return
	}
	rev, err := client.RevisionContext(r.Context(), project)
	if err != nil || rev == "" {
		return
	}
	w.Header().Set("X-StorHub-Revision", rev)
}

func restEntryETag(entry *shfs.EntryInfo) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, entry.Path)
	_, _ = io.WriteString(hash, "|")
	_, _ = io.WriteString(hash, fmt.Sprintf("%d|%d|%d|%d|%d|%t|%t|%s|%s", entry.Inode, entry.Size, entry.Mode, entry.UID, entry.GID, entry.IsDir, entry.IsSymlink, time.Unix(0, entry.ModifiedAt).UTC().Format(time.RFC3339Nano), time.Unix(0, entry.ChangedAt).UTC().Format(time.RFC3339Nano)))
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
	// Range spellings differ by endpoint by design: only GET /content
	// speaks the HTTP Range header (closed bytes=a-b on the wire, half-open
	// [start,end) in code). Session reads take offset/length query
	// parameters and copy-range takes src_off/dst_off/length body fields;
	// neither parses this header.
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

// NOTE: query parsers live in rest.go as the canonical three
// (parseNonNegativeInt/parseBoolStrict/parsePurgeScope); do not add
// handler-local variants here.
