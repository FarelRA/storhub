package rest

import (
	"context"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/go-chi/chi/v5"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// Share redemption follows ONE pathway: the signed JWT is the credential
// and the source of truth. GET /shares/{token} verifies it statelessly and
// answers from claims - no registry lookup, so links survive restarts (the
// {token} segment here IS the token; the short registry ID only addresses the
// management plane under /projects/{p}/shares). Revocation: DELETE marks
// the share ID revoked in the handler's own registry (checked by the auth
// middleware and the redemption routes), killing the link immediately for
// this handler; revocation is per-handler by design - permanent revocation
// is key rotation.
//
// Naming rule: handle* for JSON routes, serve* for raw-byte streams only.
// handleShareInfo/handleShareDerive return documents; serveShareDownload
// streams bytes.
// handleShareInfo serves GET /shares/{token}: public share metadata.
func (h *restHandler) handleShareInfo(w http.ResponseWriter, r *http.Request) {
	token := chi.URLParam(r, "token")
	claims, err := h.parseShareToken(token)
	if err != nil || strings.TrimSpace(claims.Path) == "" || strings.TrimSpace(claims.Project) == "" {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	if h.isRevoked(claims.ID) {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	h.writeJSON(w, http.StatusOK, shareResponse{
		ID:        claims.ID,
		Project:   claims.Project,
		Path:      claims.Path,
		URL:       "/?share=" + url.QueryEscape(token),
		Token:     token,
		ExpiresAt: claims.ExpiresAt.Time.UTC().Format(time.RFC3339),
		IsDir:     claims.IsDir,
	})
}

// shareRedemptionContext mirrors what authMiddleware does for a share token
// used as a Bearer credential: the redemption routes run as the
// unauthenticated "nobody" visitor, scoped to the shared path. Without this
// the fs layer falls back to the SERVER PROCESS identity (and UID 0
// normalizes to admin), so a share would grant more than "what path
// permissions grant" - the exact drift this policy exists to prevent.
func (h *restHandler) shareRedemptionContext(r *http.Request, claims *shareClaims) context.Context {
	identity := shfs.WithIdentity(r.Context(), shfs.Identity{UID: nobodyUID, GID: nobodyGID})
	return context.WithValue(identity, clientCtxKey, newRestrictedClient(h.client, claims.Project, claims.Path))
}

// ---- Signed single-file download links -------------------------------------
//
// One mechanism reused, not a second token system: these are shareClaims
// JWTs verified by parseShareToken. Stateless by construction - no registry,
// self-expiring, valid across restarts within their window - and they
// delegate streaming to streamFileRange so Range/206 resume, ETag and HEAD
// come for free.

func (h *restHandler) serveShareDownload(w http.ResponseWriter, r *http.Request) {
	// Same single pathway as info: the token (query here) is the credential.
	// Shares are always download-capable after normalization; no dl flag check.
	claims, err := h.parseShareToken(r.URL.Query().Get("token"))
	if err != nil || h.isRevoked(claims.ID) {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	targetPath, err := h.resolveSharePath(claims, r.URL.Query().Get("path"))
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	r = r.WithContext(h.shareRedemptionContext(r, claims))
	h.serveDownloadPath(w, r, claims.Project, targetPath)
}

// handleShareDerive serves POST /shares/{token}/derive: mint a sub-capability.
// A handle* JSON route (not a byte stream).
//
// Parent credential, two spellings (both explicit): the {token} path
// segment carries the parent share ID with the signed JWT in ?token= or
// Authorization: Bearer (the management-plane spelling the info route
// documents), or the path segment carries the signed JWT itself (the
// redemption spelling the info/download routes use). The two never mix
// silently: an ID in the path must match the presented token's ID, and a
// JWT in the path must verify on its own.
func (h *restHandler) handleShareDerive(w http.ResponseWriter, r *http.Request) {
	parentToken := r.URL.Query().Get("token")
	if parentToken == "" {
		parentToken = requestBearerToken(r)
	}
	claims, err := h.parseShareToken(parentToken)
	if err != nil || h.isRevoked(claims.ID) {
		// Fall back to the redemption spelling: the path segment itself
		// is the signed parent JWT (no query or header credential).
		if parentToken == "" {
			if pathClaims, perr := h.parseShareToken(chi.URLParam(r, "token")); perr == nil && !h.isRevoked(pathClaims.ID) {
				claims, err = pathClaims, nil
			}
		}
	}
	if err != nil || h.isRevoked(claims.ID) {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	// Only the share's own token can derive children: the {token} path
	// segment must match the presented token's ID when it names an ID.
	// (When the path segment was the JWT itself it is already the
	// verified parent, so the ID comparison below is skipped.)
	token := chi.URLParam(r, "token")
	if parentToken != "" && token != "" && token != claims.ID {
		h.writeMappedError(w, errForbidden("share id mismatch"))
		return
	}
	// Derivation reads through the same nobody-identity, path-scoped client
	// as redemption: the visitor's DAC, not the server's.
	r = r.WithContext(h.shareRedemptionContext(r, claims))
	var req shareRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	// Target path defaults to parent path when empty (derive same file).
	targetRaw := strings.TrimSpace(req.Path)
	if targetRaw == "" {
		targetRaw = claims.Path
	}
	sharePath, err := canonicalSharePath(targetRaw)
	if err != nil {
		h.writeMappedError(w, errBadRequest("invalid share path"))
		return
	}
	if !hasPathPrefix(sharePath, claims.Path) {
		h.writeMappedError(w, errForbidden("access denied: path not shared"))
		return
	}
	remaining := time.Until(claims.ExpiresAt.Time)
	if remaining <= 0 {
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	expiresIn := remaining
	if ttlCap := h.opts.MaxShareTTL; ttlCap > 0 && expiresIn > ttlCap {
		expiresIn = ttlCap
	}
	// Stat to learn IsDir for new record (scoped + nobody identity, above).
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	entry, err := client.StatPathContext(r.Context(), claims.Project, sharePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	defer h.traceOp(r, "share-derive", claims.Project, sharePath)(&err)
	// A derived share is a sub-capability of its parent: ownership follows
	// the parent record when it is still in the registry, so the original
	// sharer keeps management rights. Unknown parents (e.g. after a
	// restart) belong to the redeeming visitor - the nobody identity.
	creatorUID, creatorAdmin := nobodyUID, false
	if parent, known := h.getLiveShare(claims.ID); known {
		creatorUID, creatorAdmin = parent.CreatorUID, parent.CreatorAdmin
	}
	record, err := h.newShareRecord(claims.Project, sharePath, entry.IsDir, expiresIn, creatorUID, creatorAdmin)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	w.Header().Set("Location", h.opts.BasePath+"/shares/"+url.PathEscape(record.ID))
	created := h.shareResponse(record)
	created.Token = record.Token
	h.writeJSON(w, http.StatusCreated, created)
}

func (h *restHandler) resolveSharePath(claims *shareClaims, rawPath string) (string, error) {
	targetPath := claims.Path
	if strings.TrimSpace(rawPath) != "" {
		canonicalPath, err := canonicalSharePath(rawPath)
		if err != nil {
			return "", err
		}
		targetPath = canonicalPath
	}
	if !hasPathPrefix(targetPath, claims.Path) {
		return "", errForbidden("access denied: path not shared")
	}
	return targetPath, nil
}

func (h *restHandler) serveDownloadPath(w http.ResponseWriter, r *http.Request, project, targetPath string) {
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	defer h.traceOp(r, "share-download", project, targetPath)(&err)
	entry, err := client.StatPathContext(r.Context(), project, targetPath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if entry.IsDir {
		err = &restStatusError{status: http.StatusNotImplemented, message: "directory download not yet implemented"}
		h.writeError(w, http.StatusNotImplemented, "not_implemented", "directory download not yet implemented")
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", path.Base(targetPath)))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if entry.IsSymlink {
		target, readErr := client.ReadlinkContext(r.Context(), project, targetPath)
		if readErr != nil {
			err = readErr
			h.writeMappedError(w, readErr)
			return
		}
		w.Header().Set("Content-Type", "application/symlink-target")
		w.Header().Set("Content-Length", strconv.Itoa(len(target)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.WriteString(w, target)
		return
	}
	eTag := restEntryETag(entry)
	w.Header().Set("ETag", eTag)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", detectContentType(targetPath))
	start, end, partial, rangeErr := parseByteRange(r.Header.Get("Range"), entry.Size)
	if rangeErr != nil {
		if strings.TrimSpace(r.Header.Get("Range")) != "" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", entry.Size))
			err = &restStatusError{status: http.StatusRequestedRangeNotSatisfiable, message: rangeErr.Error()}
			h.writeMappedError(w, err)
			return
		}
		start, end = 0, entry.Size
	}
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, entry.Size))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(end-start, 10))
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	w.WriteHeader(status)
	h.streamFileRange(w, r, project, targetPath, start, end)
}
