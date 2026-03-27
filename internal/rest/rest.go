package rest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	ghapi "github.com/FarelRA/storhub/internal/github"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	storage "github.com/FarelRA/storhub/internal/storage"
)

const (
	defaultRESTBasePath      = "/api/v1"
	defaultRESTStreamChunk   = 1 << 20
	defaultRESTPatchBodySize = 8 << 20
	maxRequestBodyMemory     = 32 << 10
)

type Options struct {
	BasePath         string
	StreamChunkSize  int64
	MaxPatchBodySize int64
	Auth             *AuthOptions
}

type (
	FileMetadata     = metadata.FileMetadata
	MetadataRevision = metadata.MetadataRevision
	EntryInfo        = shfs.EntryInfo
	DirEntry         = shfs.DirEntry
	FSStats          = shfs.FSStats
	NodeKind         = metadata.NodeKind
)

const (
	NodeKindFile    = metadata.NodeKindFile
	NodeKindSymlink = metadata.NodeKindSymlink
)

var (
	ErrProjectNotFound = storage.ErrProjectNotFound
	ErrFileNotFound    = storage.ErrFileNotFound
)

type Client interface {
	CreateFile(project, filePath string) (*metadata.FileMetadata, error)
	Mkdir(project, dirPath string) error
	DeleteFile(project, filePath string) error
	Rmdir(project, dirPath string) error
	Rename(project, oldPath, newPath string) error
	TruncateFile(project, filePath string, size int64) (*metadata.FileMetadata, error)
	AppendFile(project, filePath string, data []byte) (*metadata.FileMetadata, error)
	WriteFileAt(project, filePath string, offset int64, data []byte) (*metadata.FileMetadata, error)
	PatchFile(project, filePath string, offset, deleteSize int64, edit []byte) (*metadata.FileMetadata, error)
	ReadFileAt(project, filePath string, offset, length int64) ([]byte, error)
	StatPath(project, targetPath string) (*shfs.EntryInfo, error)
	ReadDir(project, dirPath string) ([]shfs.DirEntry, error)
	StatFS(project string) (*shfs.FSStats, error)
	Symlink(project, target, linkPath string) (*metadata.FileMetadata, error)
	Readlink(project, linkPath string) (string, error)
	Link(project, existingPath, newPath string) (*metadata.FileMetadata, error)
	Chmod(project, targetPath string, mode uint32) error
	Chown(project, targetPath string, uid, gid uint32) error
	Chtimes(project, targetPath string, atime, mtime time.Time) error
	SetXAttr(project, targetPath, attr string, data []byte) error
	GetXAttr(project, targetPath, attr string) ([]byte, error)
	ListXAttr(project, targetPath string) ([]string, error)
	RemoveXAttr(project, targetPath, attr string) error
	ListMetadataRevisions(project string) ([]metadata.MetadataRevision, error)
	RollbackMetadata(project, commitSHA string) error
	DeleteProject(project string) error
}

type restHandler struct {
	client Client
	opts   Options
}

type restError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type projectResponse struct {
	Project string        `json:"project"`
	Stats   *shfs.FSStats `json:"stats"`
}

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

type ackResponse struct {
	Project string `json:"project"`
	Status  string `json:"status"`
}

type pathRequest struct {
	Path string `json:"path"`
}

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

type rollbackRequest struct {
	CommitSHA string `json:"commit_sha"`
}

func DefaultOptions() Options {
	return Options{
		BasePath:         defaultRESTBasePath,
		StreamChunkSize:  defaultRESTStreamChunk,
		MaxPatchBodySize: defaultRESTPatchBodySize,
	}
}

func NewHandler(hub *storage.StorHub, opts Options) (http.Handler, error) {
	if hub == nil {
		return nil, errors.New("storhub: REST handler requires a non-nil hub")
	}
	return newHandlerForClient(hub, opts)
}

func newHandlerForClient(client Client, opts Options) (http.Handler, error) {
	opts = opts.withDefaults()
	base := &restHandler{client: client, opts: opts}
	if opts.Auth == nil {
		return base, nil
	}
	auth, err := newAuthenticator(*opts.Auth)
	if err != nil {
		return nil, err
	}
	return &restAuthHandler{base: base, auth: auth, basePath: base.opts.BasePath}, nil
}

func (o Options) withDefaults() Options {
	if strings.TrimSpace(o.BasePath) == "" {
		o.BasePath = defaultRESTBasePath
	}
	o.BasePath = "/" + strings.Trim(strings.TrimSpace(o.BasePath), "/")
	if o.StreamChunkSize <= 0 {
		o.StreamChunkSize = defaultRESTStreamChunk
	}
	if o.MaxPatchBodySize <= 0 {
		o.MaxPatchBodySize = defaultRESTPatchBodySize
	}
	return o
}

func (h *restHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.serveUI(w, r) {
		return
	}
	if r.URL.Path == strings.TrimRight(h.opts.BasePath, "/") {
		h.writeJSON(w, http.StatusOK, map[string]any{
			"service":   "storhub-rest",
			"version":   "v1",
			"base_path": h.opts.BasePath,
		})
		return
	}

	project, resource, ok := h.parseRoute(r.URL.Path)
	if !ok {
		h.writeError(w, http.StatusNotFound, "not_found", "route not found")
		return
	}

	switch resource {
	case "":
		h.handleProject(w, r, project)
	case "nodes":
		h.handleNodes(w, r, project)
	case "children":
		h.handleChildren(w, r, project)
	case "content":
		h.handleContent(w, r, project)
	case "xattrs":
		h.handleXAttrs(w, r, project)
	case "xattrs/value":
		h.handleXAttrValue(w, r, project)
	case "revisions":
		h.handleRevisions(w, r, project)
	case "ops/create-file":
		h.handleCreateFile(w, r, project)
	case "ops/mkdir":
		h.handleMkdir(w, r, project)
	case "ops/rmdir":
		h.handleRmdir(w, r, project)
	case "ops/unlink":
		h.handleUnlink(w, r, project)
	case "ops/rename":
		h.handleRename(w, r, project)
	case "ops/link":
		h.handleLink(w, r, project)
	case "ops/symlink":
		h.handleSymlink(w, r, project)
	case "ops/chmod":
		h.handleChmod(w, r, project)
	case "ops/chown":
		h.handleChown(w, r, project)
	case "ops/utimes":
		h.handleUtimes(w, r, project)
	case "ops/rollback":
		h.handleRollback(w, r, project)
	default:
		h.writeError(w, http.StatusNotFound, "not_found", "route not found")
	}
}

func (h *restHandler) serveUI(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case "/":
		h.writeHTML(w, http.StatusOK, uiDocument)
		return true
	case "/ui":
		h.writeHTML(w, http.StatusOK, uiDocument)
		return true
	case "/ui/styles.css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		_, _ = io.WriteString(w, uiStyles)
		return true
	case "/ui/app.js":
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		_, _ = io.WriteString(w, uiScript)
		return true
	case "/ui/config.js":
		payload, err := json.Marshal(map[string]any{
			"basePath":    h.opts.BasePath,
			"authEnabled": h.opts.Auth != nil,
		})
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
			return true
		}
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		_, _ = fmt.Fprintf(w, "window.STORHUB_UI_CONFIG = %s;", payload)
		return true
	default:
		return false
	}
}

func (h *restHandler) parseRoute(requestPath string) (project, resource string, ok bool) {
	base := strings.TrimRight(h.opts.BasePath, "/")
	if !strings.HasPrefix(requestPath, base+"/") {
		return "", "", false
	}
	rest := strings.TrimPrefix(requestPath, base+"/")
	parts := strings.Split(rest, "/")
	if len(parts) < 2 || parts[0] != "projects" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	project = parts[1]
	if len(parts) == 2 {
		return project, "", true
	}
	return project, strings.Join(parts[2:], "/"), true
}

func (h *restHandler) handleProject(w http.ResponseWriter, r *http.Request, project string) {
	switch r.Method {
	case http.MethodGet:
		stats, err := h.client.StatFS(project)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, projectResponse{Project: project, Stats: stats})
	case http.MethodDelete:
		if err := h.client.DeleteProject(project); err != nil {
			h.writeMappedError(w, err)
			return
		}
		h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "deleted"})
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodDelete)
	}
}

func (h *restHandler) handleNodes(w http.ResponseWriter, r *http.Request, project string) {
	targetPath := r.URL.Query().Get("path")
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		entry, err := h.client.StatPath(project, targetPath)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		eTag := restEntryETag(entry)
		if h.ifNoneMatchSatisfied(r.Header.Get("If-None-Match"), eTag) {
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
		entry, err := h.client.StatPath(project, targetPath)
		if err != nil {
			h.writeMappedError(w, err)
			return
		}
		eTag := restEntryETag(entry)
		if err := h.requireMatch(r.Header.Get("If-Match"), eTag); err != nil {
			if !isPreconditionHeaderEmpty(err) {
				h.writeMappedError(w, err)
				return
			}
		}
		if entry.IsDir {
			if r.URL.Query().Get("recursive") == "true" {
				h.writeError(w, http.StatusNotImplemented, "recursive_delete_unsupported", "recursive directory deletion is not supported")
				return
			}
			err = h.client.Rmdir(project, targetPath)
		} else {
			err = h.client.DeleteFile(project, targetPath)
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

func (h *restHandler) handleChildren(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodGet {
		h.methodNotAllowed(w, http.MethodGet)
		return
	}
	dirPath := r.URL.Query().Get("path")
	entries, err := h.client.ReadDir(project, dirPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, entriesResponse{Project: project, Path: dirPath, Entries: entries})
}

func (h *restHandler) handleContent(w http.ResponseWriter, r *http.Request, project string) {
	filePath := r.URL.Query().Get("path")
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		h.handleContentRead(w, r, project, filePath)
	case http.MethodPut:
		h.handleContentReplace(w, r, project, filePath)
	case http.MethodPatch:
		h.handleContentPatch(w, r, project, filePath)
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPatch)
	}
}

func (h *restHandler) handleContentRead(w http.ResponseWriter, r *http.Request, project, filePath string) {
	entry, err := h.client.StatPath(project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if entry.IsDir {
		h.writeError(w, http.StatusConflict, "is_directory", fmt.Sprintf("path is a directory: %s", filePath))
		return
	}
	if entry.IsSymlink {
		target, readErr := h.client.Readlink(project, filePath)
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
	for offset := start; offset < end; offset += h.opts.StreamChunkSize {
		readLen := h.opts.StreamChunkSize
		if remaining := end - offset; remaining < readLen {
			readLen = remaining
		}
		chunk, readErr := h.client.ReadFileAt(project, filePath, offset, readLen)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return
		}
		if len(chunk) == 0 {
			break
		}
		if _, writeErr := w.Write(chunk); writeErr != nil {
			return
		}
	}
}

func (h *restHandler) handleContentReplace(w http.ResponseWriter, r *http.Request, project, filePath string) {
	entry, exists, err := h.lookupOptional(project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if exists {
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
		if entry.IsDir {
			h.writeError(w, http.StatusConflict, "is_directory", fmt.Sprintf("path is a directory: %s", filePath))
			return
		}
	} else if r.Header.Get("If-Match") != "" {
		h.writeMappedError(w, errPreconditionFailed("resource does not exist"))
		return
	}

	created := !exists
	if !exists {
		if _, err := h.client.CreateFile(project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
	} else if entry.IsSymlink {
		if err := h.client.DeleteFile(project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
		if _, err := h.client.CreateFile(project, filePath); err != nil {
			h.writeMappedError(w, err)
			return
		}
	} else if _, err := h.client.TruncateFile(project, filePath, 0); err != nil {
		h.writeMappedError(w, err)
		return
	}

	if err := h.streamWriteBody(project, filePath, r.Body, 0); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, filePath, ternaryStatus(created, http.StatusCreated, http.StatusOK))
}

func (h *restHandler) handleContentPatch(w http.ResponseWriter, r *http.Request, project, filePath string) {
	entry, err := h.client.StatPath(project, filePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.requireMatch(r.Header.Get("If-Match"), restEntryETag(entry)); err != nil {
		if !isPreconditionHeaderEmpty(err) {
			h.writeMappedError(w, err)
			return
		}
	}
	op := strings.TrimSpace(r.URL.Query().Get("op"))
	switch op {
	case "append":
		if err := h.streamAppendBody(project, filePath, r.Body); err != nil {
			h.writeMappedError(w, err)
			return
		}
	case "write":
		offset, parseErr := parseRequiredInt64(r.URL.Query().Get("offset"), "offset")
		if parseErr != nil {
			h.writeMappedError(w, parseErr)
			return
		}
		if err := h.streamWriteBody(project, filePath, r.Body, offset); err != nil {
			h.writeMappedError(w, err)
			return
		}
	case "patch":
		offset, parseErr := parseRequiredInt64(r.URL.Query().Get("offset"), "offset")
		if parseErr != nil {
			h.writeMappedError(w, parseErr)
			return
		}
		deleteSize, parseErr := parseRequiredInt64(r.URL.Query().Get("delete_size"), "delete_size")
		if parseErr != nil {
			h.writeMappedError(w, parseErr)
			return
		}
		edit, readErr := h.readPatchBody(w, r)
		if readErr != nil {
			h.writeMappedError(w, readErr)
			return
		}
		if _, err := h.client.PatchFile(project, filePath, offset, deleteSize, edit); err != nil {
			h.writeMappedError(w, err)
			return
		}
	case "truncate":
		size, parseErr := parseRequiredInt64(r.URL.Query().Get("size"), "size")
		if parseErr != nil {
			h.writeMappedError(w, parseErr)
			return
		}
		if _, err := h.client.TruncateFile(project, filePath, size); err != nil {
			h.writeMappedError(w, err)
			return
		}
	default:
		h.writeError(w, http.StatusBadRequest, "invalid_patch_op", "query parameter op must be one of append, write, patch, truncate")
		return
	}
	h.respondWithNode(w, project, filePath, http.StatusOK)
}

func (h *restHandler) handleXAttrs(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodGet {
		h.methodNotAllowed(w, http.MethodGet)
		return
	}
	targetPath := r.URL.Query().Get("path")
	names, err := h.client.ListXAttr(project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, xattrListResponse{Project: project, Path: targetPath, Names: names})
}

func (h *restHandler) handleXAttrValue(w http.ResponseWriter, r *http.Request, project string) {
	targetPath := r.URL.Query().Get("path")
	name := r.URL.Query().Get("name")
	switch r.Method {
	case http.MethodGet:
		value, err := h.client.GetXAttr(project, targetPath, name)
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
		if err := h.client.SetXAttr(project, targetPath, name, payload); err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := h.client.RemoveXAttr(project, targetPath, name); err != nil {
			h.writeMappedError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		h.methodNotAllowed(w, http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func (h *restHandler) handleRevisions(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodGet {
		h.methodNotAllowed(w, http.MethodGet)
		return
	}
	revisions, err := h.client.ListMetadataRevisions(project)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, revisionsResponse{Project: project, Revisions: revisions})
}

func (h *restHandler) handleCreateFile(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.client.CreateFile(project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, req.Path, http.StatusCreated)
}

func (h *restHandler) handleMkdir(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.client.Mkdir(project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, req.Path, http.StatusCreated)
}

func (h *restHandler) handleRmdir(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.client.Rmdir(project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleUnlink(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req pathRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.client.DeleteFile(project, req.Path); err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *restHandler) handleRename(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req renameRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.client.Rename(project, req.OldPath, req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, req.NewPath, http.StatusOK)
}

func (h *restHandler) handleLink(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req linkRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.client.Link(project, req.ExistingPath, req.NewPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, req.NewPath, http.StatusCreated)
}

func (h *restHandler) handleSymlink(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req symlinkRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err := h.client.Symlink(project, req.Target, req.LinkPath); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, req.LinkPath, http.StatusCreated)
}

func (h *restHandler) handleChmod(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req chmodRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.client.Chmod(project, req.Path, req.Mode); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, req.Path, http.StatusOK)
}

func (h *restHandler) handleChown(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req chownRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.client.Chown(project, req.Path, req.UID, req.GID); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, req.Path, http.StatusOK)
}

func (h *restHandler) handleUtimes(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req utimesRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.client.Chtimes(project, req.Path, req.Atime, req.Mtime); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.respondWithNode(w, project, req.Path, http.StatusOK)
}

func (h *restHandler) handleRollback(w http.ResponseWriter, r *http.Request, project string) {
	if r.Method != http.MethodPost {
		h.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req rollbackRequest
	if err := h.decodeJSON(r, &req); err != nil {
		h.writeMappedError(w, err)
		return
	}
	if err := h.client.RollbackMetadata(project, req.CommitSHA); err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, http.StatusOK, ackResponse{Project: project, Status: "rolled_back"})
}

func (h *restHandler) decodeJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return errBadRequest("request body is required")
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyMemory))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errBadRequest(fmt.Sprintf("invalid JSON body: %v", err))
	}
	if err := dec.Decode(new(struct{})); err != io.EOF {
		return errBadRequest("request body must contain a single JSON object")
	}
	return nil
}

func (h *restHandler) respondWithNode(w http.ResponseWriter, project, targetPath string, status int) {
	entry, err := h.client.StatPath(project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	h.writeJSON(w, status, nodeResponse{Project: project, Entry: entry, ETag: restEntryETag(entry)})
}

func (h *restHandler) lookupOptional(project, targetPath string) (*shfs.EntryInfo, bool, error) {
	entry, err := h.client.StatPath(project, targetPath)
	if err == nil {
		return entry, true, nil
	}
	if mappedStatus(err) == http.StatusNotFound {
		return nil, false, nil
	}
	return nil, false, err
}

func (h *restHandler) streamWriteBody(project, filePath string, body io.Reader, offset int64) error {
	buf := make([]byte, h.opts.StreamChunkSize)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, writeErr := h.client.WriteFileAt(project, filePath, offset, append([]byte(nil), buf[:n]...)); writeErr != nil {
				return writeErr
			}
			offset += int64(n)
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (h *restHandler) streamAppendBody(project, filePath string, body io.Reader) error {
	buf := make([]byte, h.opts.StreamChunkSize)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, appendErr := h.client.AppendFile(project, filePath, append([]byte(nil), buf[:n]...)); appendErr != nil {
				return appendErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (h *restHandler) readPatchBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(r.Body, h.opts.MaxPatchBodySize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > h.opts.MaxPatchBodySize {
		return nil, errPayloadTooLarge("patch payload exceeds the configured limit")
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

func (h *restHandler) methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	h.writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func (h *restHandler) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (h *restHandler) writeError(w http.ResponseWriter, status int, code, message string) {
	resp := restError{}
	resp.Error.Code = code
	resp.Error.Message = message
	h.writeJSON(w, status, resp)
}

func (h *restHandler) writeMappedError(w http.ResponseWriter, err error) {
	status := mappedStatus(err)
	code := mappedCode(status)
	h.writeError(w, status, code, err.Error())
}

func mappedStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	var apiErr *ghapi.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.NotFound():
			return http.StatusNotFound
		case apiErr.IsRetryable():
			return http.StatusBadGateway
		default:
			return http.StatusBadGateway
		}
	}
	var rerr *restStatusError
	if errors.As(err, &rerr) {
		return rerr.status
	}
	if errors.Is(err, storage.ErrProjectNotFound) || errors.Is(err, storage.ErrFileNotFound) {
		return http.StatusNotFound
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "not found"):
		return http.StatusNotFound
	case strings.Contains(message, "already exists"), strings.Contains(message, "not empty"), strings.Contains(message, "destination already exists"), strings.Contains(message, "is a directory"), strings.Contains(message, "not a directory"):
		return http.StatusConflict
	case strings.Contains(message, "parent directory does not exist"):
		return http.StatusConflict
	case strings.Contains(message, "required"), strings.Contains(message, "must be"), strings.Contains(message, "cannot"), strings.Contains(message, "invalid"):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func mappedCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusPreconditionFailed:
		return "precondition_failed"
	case http.StatusRequestEntityTooLarge:
		return "payload_too_large"
	case http.StatusRequestedRangeNotSatisfiable:
		return "range_not_satisfiable"
	case http.StatusMethodNotAllowed:
		return "method_not_allowed"
	case http.StatusNotImplemented:
		return "not_implemented"
	default:
		return "internal_error"
	}
}

func restEntryETag(entry *shfs.EntryInfo) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, entry.Path)
	_, _ = io.WriteString(hash, "|")
	_, _ = io.WriteString(hash, fmt.Sprintf("%d|%d|%d|%d|%d|%t|%t|%s|%s", entry.Inode, entry.Size, entry.Mode, entry.UID, entry.GID, entry.IsDir, entry.IsSymlink, entry.ModifiedAt.UTC().Format(time.RFC3339Nano), entry.ChangedAt.UTC().Format(time.RFC3339Nano)))
	if entry.SymlinkTarget != "" {
		_, _ = io.WriteString(hash, "|")
		_, _ = io.WriteString(hash, entry.SymlinkTarget)
	}
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
}

func detectContentType(filePath string) string {
	if ext := path.Ext(filePath); ext != "" {
		if typ := mime.TypeByExtension(ext); typ != "" {
			return typ
		}
	}
	return "application/octet-stream"
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

type restStatusError struct {
	status  int
	message string
}

func (e *restStatusError) Error() string { return e.message }

var errEmptyPrecondition = &restStatusError{status: 0, message: "precondition header missing"}

func isPreconditionHeaderEmpty(err error) bool {
	return errors.Is(err, errEmptyPrecondition)
}

func errBadRequest(message string) error {
	return &restStatusError{status: http.StatusBadRequest, message: message}
}

func errPreconditionFailed(message string) error {
	return &restStatusError{status: http.StatusPreconditionFailed, message: message}
}

func errPayloadTooLarge(message string) error {
	return &restStatusError{status: http.StatusRequestEntityTooLarge, message: message}
}

func errForbidden(message string) error {
	return &restStatusError{status: http.StatusForbidden, message: message}
}

func ternaryStatus(cond bool, yes, no int) int {
	if cond {
		return yes
	}
	return no
}

func bytesBody(data []byte) io.Reader {
	return bytes.NewReader(data)
}
