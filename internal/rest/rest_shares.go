package rest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
)

// restrictedClient wraps a Client and restricts access to a specific project and path.
// See shareRegistry for revocation lifecycle.
type readOnlyShare struct{}

func errReadOnly() error { return errForbidden("access denied: read-only share") }

func (readOnlyShare) CreateFileContext(_ context.Context, _, _ string) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) MkdirContext(_ context.Context, _, _ string) error {
	return errReadOnly()
}

func (readOnlyShare) DeleteFileContext(_ context.Context, _, _ string, _ ...shfs.MutateOption) error {
	return errReadOnly()
}

func (readOnlyShare) RmdirContext(_ context.Context, _, _ string, _ ...shfs.MutateOption) error {
	return errReadOnly()
}

func (readOnlyShare) RenameContext(_ context.Context, _, _, _ string, _ ...shfs.MutateOption) error {
	return errReadOnly()
}

func (readOnlyShare) CopyContext(_ context.Context, _, _, _ string) error {
	return errReadOnly()
}

func (readOnlyShare) CloneRange(_ context.Context, _, _ string, _ int64, _ string, _ int64, _ int64, _ ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) TruncateFileContext(_ context.Context, _, _ string, _ int64, _ ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) AppendFileContext(_ context.Context, _, _ string, _ []byte, _ ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) WriteFileAtContext(_ context.Context, _, _ string, _ int64, _ []byte, _ ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) PatchFileContext(_ context.Context, _, _ string, _, _ int64, _ []byte, _ ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) ReplaceFileFromReaderContext(_ context.Context, _, _ string, _ io.Reader, _ ...shfs.MutateOption) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) SymlinkContext(_ context.Context, _, _, _ string) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) LinkContext(_ context.Context, _, _, _ string) (*metadata.FileMeta, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) ChmodContext(_ context.Context, _, _ string, _ uint32) error {
	return errReadOnly()
}

func (readOnlyShare) ChownContext(_ context.Context, _, _ string, _, _ uint32) error {
	return errReadOnly()
}

func (readOnlyShare) ChtimesContext(_ context.Context, _, _ string, _, _ int64) error {
	return errReadOnly()
}

func (readOnlyShare) SetXAttrContext(_ context.Context, _, _, _ string, _ []byte, _ ...shfs.XAttrMode) error {
	return errReadOnly()
}

func (readOnlyShare) RemoveXAttrContext(_ context.Context, _, _, _ string) error {
	return errReadOnly()
}

func (readOnlyShare) RollbackMetadataContext(_ context.Context, _, _ string) error {
	return errReadOnly()
}

func (readOnlyShare) RevertPathContext(_ context.Context, _, _, _ string) error {
	return errReadOnly()
}

func (readOnlyShare) PruneContext(_ context.Context, _, _ string, _ int, _ bool) (*storage.PruneResult, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) DegradedProjects() ([]string, error) {
	return nil, errReadOnly()
}

func (readOnlyShare) ReEnableProject(_ string) error {
	return errReadOnly()
}

func (readOnlyShare) PressureSnapshot() (storage.PressureSnapshot, error) {
	return storage.PressureSnapshot{}, errReadOnly()
}

func (readOnlyShare) PressureFailureStreak(_ string) (uint64, error) { return 0, errReadOnly() }

func (readOnlyShare) PressurePendingDepth(_ string) (int, error) { return 0, errReadOnly() }

func (readOnlyShare) DeleteProjectContext(_ context.Context, _ string) error {
	return errReadOnly()
}

// DrainProjectContext is denied like every other mutation: share visitors
// are read-only, and their lane never publishes journaled work to drain.
func (readOnlyShare) DrainProjectContext(_ context.Context, _ string) error {
	return errReadOnly()
}

// Compile-time proof that the auth wrappers implement the FULL Client
// interface: a Client method added without a corresponding gate in either
// wrapper fails the build here instead of silently falling through to the
// raw client at runtime (clientFor fails closed, but a missing method on a
// wrapper that still satisfies Client via embedding would delegate by
// accident).
var (
	_ Client = (*restrictedClient)(nil)
)

// See readOnlyShare for the mutation-denial policy: every mutation is
// denied by the embedded struct; read methods enforce the shared-prefix
// check then delegate.
type restrictedClient struct {
	readOnlyShare
	underlying     Client
	allowedProject string
	allowedPath    string
}

func newRestrictedClient(underlying Client, project, path string) *restrictedClient {
	allowedPath, err := canonicalSharePath(path)
	if err != nil {
		allowedPath = strings.Trim(strings.TrimSpace(path), "/")
	}
	return &restrictedClient{
		underlying:     underlying,
		allowedProject: project,
		allowedPath:    allowedPath,
	}
}

func (c *restrictedClient) checkAccess(project, targetPath string) error {
	if project != c.allowedProject {
		return errForbidden("access denied: project not shared")
	}
	canonicalTargetPath, err := canonicalSharePath(targetPath)
	if err != nil {
		return errForbidden("access denied: path not shared")
	}
	if hasPathPrefix(canonicalTargetPath, c.allowedPath) {
		return nil
	}
	return errForbidden("access denied: path not shared")
}

// maxShareResolveHops bounds symlink chasing in the share lane, mirroring
// the kernel MAXSYMLINKS budget: a longer chain fails closed instead of
// serving.
const maxShareResolveHops = 40

// isShareScopeDenial reports whether err is a share-lane scope denial
// (escape or loop) as opposed to a backend failure (missing node,
// DAC refusal as nobody), so callers can fail closed on escapes while
// preserving benign shapes like dangling-link stat.
func isShareScopeDenial(err error) bool {
	var rerr *restStatusError
	if !errors.As(err, &rerr) {
		return false
	}
	return rerr.status == http.StatusForbidden
}

// resolveShareTarget follows symlinks hop by hop from a scope-checked
// request path and enforces the share prefix on every hop and the final
// target. A link inside the shared subtree pointing outside (absolute
// or relative, single or chained) resolves to a denied error instead of
// serving outside bytes or metadata. Dangling or DAC-refused targets
// surface their backend error unchanged: the link path itself passed the
// scope check, so the denial (if any) comes from the serve-time DAC.
//
// The check races a concurrent retarget between resolution and serve
// (check-then-use): the serve still runs as the nobody visitor, so an
// outside target additionally needs nobody DAC to leak bytes. Share
// creators (who place or retarget links) stay trusted for link-shape
// changes, exactly as for share creation itself.
func (c *restrictedClient) resolveShareTarget(ctx context.Context, project, targetPath string) (string, error) {
	if err := c.checkAccess(project, targetPath); err != nil {
		return "", err
	}
	current, err := canonicalSharePath(targetPath)
	if err != nil {
		return "", errForbidden("access denied: path not shared")
	}
	for range maxShareResolveHops {
		entry, err := c.underlying.StatPathContext(ctx, project, current)
		if err != nil {
			return "", err
		}
		if !entry.IsSymlink {
			return current, nil
		}
		target, err := c.underlying.ReadlinkContext(ctx, project, current)
		if err != nil {
			return "", err
		}
		next := target
		if !path.IsAbs(target) {
			next = path.Join(path.Dir(current), target)
		}
		canonical, err := canonicalSharePath(next)
		if err != nil {
			return "", errForbidden("access denied: path not shared")
		}
		if !hasPathPrefix(canonical, c.allowedPath) {
			return "", errForbidden("access denied: path not shared")
		}
		current = canonical
	}
	return "", errForbidden("access denied: too many levels of symbolic links")
}

func (c *restrictedClient) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	if _, err := c.resolveShareTarget(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.underlying.ReadFileAtContext(ctx, project, filePath, offset, length)
}

func (c *restrictedClient) StatPathContext(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	if err := c.checkAccess(project, targetPath); err != nil {
		return nil, err
	}
	entry, err := c.underlying.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if !entry.IsSymlink {
		return entry, nil
	}
	if _, rerr := c.resolveShareTarget(ctx, project, targetPath); rerr != nil {
		if isShareScopeDenial(rerr) {
			// Escaping or looping link: no outside metadata, not even
			// the link row.
			return nil, rerr
		}
		// Dangling or unreadable target: the link itself sits inside
		// the share, so its own row stays visible.
		return entry, nil
	}
	return entry, nil
}

func (c *restrictedClient) ReadDirContext(ctx context.Context, project, dirPath string) ([]shfs.DirEntry, error) {
	if _, err := c.resolveShareTarget(ctx, project, dirPath); err != nil {
		return nil, err
	}
	return c.underlying.ReadDirContext(ctx, project, dirPath)
}

// StatFS and revision listing are denied with share-specific messages:
// aggregate stats and history leak information beyond the shared subtree.
func (c *restrictedClient) StatFSContext(_ context.Context, _ string) (*shfs.FSStats, error) {
	return nil, errForbidden("access denied: share metadata is limited to the shared path")
}

func (c *restrictedClient) ListMetadataRevisionsContext(_ context.Context, _ string) ([]metadata.MetadataRevision, error) {
	return nil, errForbidden("access denied: share metadata is limited to the shared path")
}

// Share visitors never learn the project's revision: like stats and
// history, it is metadata beyond the shared subtree.
func (c *restrictedClient) RevisionContext(_ context.Context, _ string) (string, error) {
	return "", errForbidden("access denied: share metadata is limited to the shared path")
}

func (c *restrictedClient) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	if err := c.checkAccess(project, linkPath); err != nil {
		return "", err
	}
	target, err := c.underlying.ReadlinkContext(ctx, project, linkPath)
	if err != nil {
		return "", err
	}
	// The raw target string names a path: when resolution escapes the
	// share the string itself leaks outside structure, so it stays
	// denied. A backend failure from the resolver means every hop it
	// reached stayed in scope (dangling or DAC-refused tail), and the
	// first-hop string names an in-scope path.
	if _, rerr := c.resolveShareTarget(ctx, project, linkPath); rerr != nil {
		if isShareScopeDenial(rerr) {
			return "", rerr
		}
	}
	return target, nil
}

func (c *restrictedClient) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
	if _, err := c.resolveShareTarget(ctx, project, targetPath); err != nil {
		return nil, err
	}
	return c.underlying.GetXAttrContext(ctx, project, targetPath, attr)
}

func (c *restrictedClient) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	if _, err := c.resolveShareTarget(ctx, project, targetPath); err != nil {
		return nil, err
	}
	return c.underlying.ListXAttrContext(ctx, project, targetPath)
}

type shareRequest struct {
	Path string `json:"path"`
	// ExpiresInSeconds expresses the lifetime in plain seconds.
	ExpiresInSeconds int64 `json:"expires_in_seconds,omitempty"`
}

type shareResponse struct {
	ID          string `json:"id"`
	Project     string `json:"project"`
	Path        string `json:"path"`
	URL         string `json:"url"`
	DownloadURL string `json:"download_url,omitempty"`
	// Token is the signed share JWT, returned on creation (programmatic
	// bearer use) and echoed by the self-info route (the caller proving
	// the credential to render console URLs); listings never carry it
	// so capabilities do not leak through read endpoints.
	Token     string `json:"token,omitempty"`
	ExpiresAt string `json:"expires_at"`
	IsDir     bool   `json:"is_dir"`
}

type shareClaims struct {
	jwt.RegisteredClaims
	ID      string `json:"id"`
	Project string `json:"prj"`
	Path    string `json:"pth"`
	IsDir   bool   `json:"dir"`
}

type sharesResponse struct {
	Project string          `json:"project"`
	Shares  []shareResponse `json:"shares"`
}

// Router entry points (method-split): the router owns dispatch, so no
// switch-on-method remains.
func (h *restHandler) handleProjectSharesGet(w http.ResponseWriter, r *http.Request) {
	h.listProjectShares(w, r, chi.URLParam(r, "project"))
}

func (h *restHandler) handleProjectSharesPost(w http.ResponseWriter, r *http.Request) {
	h.createProjectShare(w, r, chi.URLParam(r, "project"))
}

func (h *restHandler) handleProjectShareGet(w http.ResponseWriter, r *http.Request) {
	h.getProjectShare(w, r, chi.URLParam(r, "project"), chi.URLParam(r, "shareID"))
}

func (h *restHandler) handleProjectShareDelete(w http.ResponseWriter, r *http.Request) {
	h.deleteProjectShare(w, r, chi.URLParam(r, "project"), chi.URLParam(r, "shareID"))
}

func (h *restHandler) createProjectShare(w http.ResponseWriter, r *http.Request, project string) {
	var req shareRequest
	if err := h.decodeJSON(r, &req, false); err != nil {
		h.writeMappedError(w, err)
		return
	}
	sharePath, err := canonicalSharePath(req.Path)
	if err != nil {
		h.writeMappedError(w, errBadRequest("invalid share path"))
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	spanStarted := h.traceStart(r, "share-create", project, sharePath)
	defer h.traceFinish(r, "share-create", project, sharePath, spanStarted, &err)
	entry, err := client.StatPathContext(r.Context(), project, sharePath)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if req.ExpiresInSeconds < 0 {
		err = errBadRequest("expires_in_seconds must be non-negative")
		h.writeMappedError(w, err)
		return
	}
	expiresIn := time.Duration(0)
	if req.ExpiresInSeconds > 0 {
		expiresIn = time.Duration(req.ExpiresInSeconds) * time.Second
	}
	if expiresIn <= 0 {
		expiresIn = h.opts.ShareTTL
	}
	if ttlCap := h.opts.MaxShareTTL; ttlCap > 0 && expiresIn > ttlCap {
		expiresIn = ttlCap
	}
	// Ownership gate: remember who minted the share so
	// list/get/delete can be restricted to creator ∪ admin. The identity is
	// the one attached by the auth middleware (process identity under
	// AllowAnonymous, where every caller is the operator anyway).
	creator := shfs.IdentityFromContext(r.Context())
	record, err := h.newShareRecord(project, sharePath, entry.IsDir, expiresIn, creator.UID, creator.Admin)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	// 201 with Location: a new resource was created and is addressable at
	// the project-shares collection, consistent with REST creation
	// semantics everywhere else in this API.
	w.Header().Set("Location", h.opts.BasePath+"/projects/"+url.PathEscape(project)+"/shares/"+url.PathEscape(record.ID))
	created := h.shareResponse(record)
	created.Token = record.Token
	h.writeJSON(w, http.StatusCreated, created)
}

func (h *restHandler) listProjectShares(w http.ResponseWriter, r *http.Request, project string) {
	var err error
	spanStarted := h.traceStart(r, "share-list", project, "")
	defer h.traceFinish(r, "share-list", project, "", spanStarted, &err)
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err = client.StatFSContext(r.Context(), project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	creator := shfs.IdentityFromContext(r.Context())
	shares := h.projectShareResponses(project, creator.UID, creator.Admin)
	h.writeJSON(w, http.StatusOK, sharesResponse{Project: project, Shares: shares})
}

// canManageShare is the ownership gate for the share management plane:
// only the creator of a share (or an admin) may list, read, or revoke it.
func canManageShare(record *shareRecord, callerUID uint32, callerAdmin bool) bool {
	return callerAdmin || callerUID == record.CreatorUID
}

func (h *restHandler) getProjectShare(w http.ResponseWriter, r *http.Request, project, shareID string) {
	var err error
	spanStarted := h.traceStart(r, "share-get", project, "", "share", shareID)
	defer h.traceFinish(r, "share-get", project, "", spanStarted, &err, "share", shareID)
	record, ok := h.getLiveShare(shareID)
	if !ok || record.Project != project {
		err = &restStatusError{status: http.StatusNotFound, message: "share not found"}
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err = client.StatFSContext(r.Context(), project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	caller := shfs.IdentityFromContext(r.Context())
	if !canManageShare(record, caller.UID, caller.Admin) {
		// 404, not 403: do not leak which share IDs exist in this project.
		err = &restStatusError{status: http.StatusNotFound, message: "share not found"}
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	// The single-get endpoint is a read surface: like the listing, it must
	// never re-expose the signed token (or mint credential-bearing URLs).
	record.Token = ""
	h.writeJSON(w, http.StatusOK, h.shareResponse(record))
}

func (h *restHandler) deleteProjectShare(w http.ResponseWriter, r *http.Request, project, shareID string) {
	var err error
	spanStarted := h.traceStart(r, "share-delete", project, "", "share", shareID)
	defer h.traceFinish(r, "share-delete", project, "", spanStarted, &err, "share", shareID)
	record, ok := h.getLiveShare(shareID)
	if !ok || record.Project != project {
		err = &restStatusError{status: http.StatusNotFound, message: "share not found"}
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	client, err := h.clientFor(r)
	if err != nil {
		h.writeMappedError(w, err)
		return
	}
	if _, err = client.StatFSContext(r.Context(), project); err != nil {
		h.writeMappedError(w, err)
		return
	}
	caller := shfs.IdentityFromContext(r.Context())
	if !canManageShare(record, caller.UID, caller.Admin) {
		err = &restStatusError{status: http.StatusNotFound, message: "share not found"}
		h.writeError(w, http.StatusNotFound, "not_found", "share not found")
		return
	}
	h.removeShare(record.ID)
	h.revokeShare(record.ID, record.ExpiresAt) // stateless redemption stops immediately
	h.sweep()
	// 204 like every other successful delete in this API (nodes, projects).
	w.WriteHeader(http.StatusNoContent)
}

func hasPathPrefix(targetPath, allowedPath string) bool {
	targetPath, err := canonicalSharePath(targetPath)
	if err != nil {
		return false
	}
	allowedPath, err = canonicalSharePath(allowedPath)
	if err != nil {
		return false
	}
	if allowedPath == "" {
		return true
	}
	return targetPath == allowedPath || strings.HasPrefix(targetPath, allowedPath+"/")
}

// canonicalSharePath cleans a user-supplied share path relative to the
// project root. A path that escapes the root ("../x", "a/../../b") is
// REJECTED, not silently resolved project-relative: the old leading-slash
// clean made the escape branch below dead code and quietly re-anchored
// traversal attempts inside the project.
func canonicalSharePath(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" {
		if rel := path.Clean(trimmed); rel == ".." || strings.HasPrefix(rel, "../") {
			return "", errBadRequest("path traversal is not allowed")
		}
	}
	clean := path.Clean("/" + trimmed)
	clean = strings.TrimPrefix(clean, "/")
	if clean == "." {
		return "", nil
	}
	return clean, nil
}

func (h *restHandler) parseShareToken(token string) (*shareClaims, error) {
	if h.shareSignKey == nil {
		return nil, errForbidden("share signing key not configured (pass --sharekey or serve with an auth file)")
	}
	uc := &unifiedClaims{}
	parsed, err := jwt.ParseWithClaims(strings.TrimSpace(token), uc, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodEd25519); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return h.shareSignKey.Public(), nil
	}, jwt.WithIssuer(restTokenIssuer), jwt.WithAudience(restTokenAudience), jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithTimeFunc(time.Now))
	if err != nil || !parsed.Valid || uc.Kind != "share" || uc.Project == "" {
		return nil, errForbidden("invalid or expired share token")
	}
	return &shareClaims{
		RegisteredClaims: uc.RegisteredClaims,
		ID:               uc.ID,
		Project:          uc.Project,
		Path:             uc.Path,
		IsDir:            uc.IsDir,
	}, nil
}

// maxLiveShares bounds live share records: like assetURLCacheCap (10k),
// far-future TTLs (MaxShareTTL default 7d) must not pin unbounded JWTs.
// shareSweepThreshold is kept as a metrics hint only (no gate).
const maxLiveShares = 10000

const shareSweepThreshold = 128

func newShareRegistry() *shareRegistry {
	return &shareRegistry{items: map[string]*shareRecord{}, revoked: map[string]time.Time{}}
}

func (h *restHandler) newShareRecord(project, sharePath string, isDir bool, expiresIn time.Duration, creatorUID uint32, creatorAdmin bool) (*shareRecord, error) {
	if h.shareSignKey == nil {
		return nil, errForbidden("share signing key not configured (pass --sharekey or serve with an auth file)")
	}
	id, err := newShareID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	expiresAt := now.Add(expiresIn)
	claims := unifiedClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    restTokenIssuer,
			Audience:  jwt.ClaimStrings{restTokenAudience},
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
		Kind:    "share",
		ID:      id,
		Project: project,
		Path:    sharePath,
		IsDir:   isDir,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	signedToken, err := token.SignedString(h.shareSignKey)
	if err != nil {
		return nil, err
	}
	record := &shareRecord{ID: id, Token: signedToken, Project: project, Path: sharePath, IsDir: isDir, CreatedAt: now, ExpiresAt: expiresAt, CreatorUID: creatorUID, CreatorAdmin: creatorAdmin}
	h.shares.mu.Lock()
	h.shares.items[id] = record
	// Oldest-expiry eviction on insert: live shares are capped, so a flood
	// of far-future shares evicts the soonest-expiring first.
	for len(h.shares.items) > maxLiveShares {
		oldest, oldestExp := "", time.Time{}
		first := true
		for sid, rec := range h.shares.items {
			if first || rec.ExpiresAt.Before(oldestExp) {
				oldest, oldestExp, first = sid, rec.ExpiresAt, false
			}
		}
		if oldest == "" {
			break
		}
		delete(h.shares.items, oldest)
	}
	h.shares.mu.Unlock()
	h.sweep()
	return record, nil
}

// getLiveShare is the pure read path: it never mutates the registry.
// Expired records simply miss; sweep() (on writes) reclaims them.
func (h *restHandler) getLiveShare(shareID string) (*shareRecord, bool) {
	h.shares.mu.RLock()
	record, ok := h.shares.items[shareID]
	h.shares.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if !record.ExpiresAt.After(time.Now()) {
		return nil, false
	}
	cp := *record
	return &cp, true
}

// sweep drops every expired share and self-expired revocation entry,
// unconditionally. It runs on every write (create/delete); the old
// len>=128 gate is gone (shareSweepThreshold remains only as a metrics
// hint). Revocation entries self-expire the same way: once the shadowed
// JWT would fail its own exp check, remembering the ID adds nothing.
func (h *restHandler) sweep() {
	now := time.Now()
	h.shares.mu.Lock()
	defer h.shares.mu.Unlock()
	for shareID, record := range h.shares.items {
		if !record.ExpiresAt.After(now) {
			delete(h.shares.items, shareID)
		}
	}
	for shareID, expiresAt := range h.shares.revoked {
		if !expiresAt.After(now) {
			delete(h.shares.revoked, shareID)
		}
	}
}

// sweepExpiredShares is kept for backward compatibility; new code uses sweep.
func (h *restHandler) sweepExpiredShares() { h.sweep() }

func (h *restHandler) removeShare(shareID string) {
	h.shares.mu.Lock()
	delete(h.shares.items, shareID)
	h.shares.mu.Unlock()
}

// projectShareResponses lists a project's live shares, restricted to the
// caller's management scope (creator ∪ admin). The signed token is stripped
// before rendering: listings never carry the credential (or its URLs).
// Pure read: expired entries are skipped, never deleted here; sweep()
// reclaims them on writes.
func (h *restHandler) projectShareResponses(project string, callerUID uint32, callerAdmin bool) []shareResponse {
	now := time.Now()
	h.shares.mu.RLock()
	defer h.shares.mu.RUnlock()
	shares := make([]shareResponse, 0)
	for _, record := range h.shares.items {
		if !record.ExpiresAt.After(now) {
			continue
		}
		if record.Project != project {
			continue
		}
		if !callerAdmin && callerUID != record.CreatorUID {
			continue
		}
		cp := *record
		cp.Token = "" // listings never carry the credential (or its URLs)
		shares = append(shares, h.shareResponse(&cp))
	}
	return shares
}

// shareResponse builds the public shape. Redemption URLs are minted ONLY
// from the signed token (creation responses carry it; listings cannot, so
// their URL fields stay empty - the token is the credential).
func (h *restHandler) shareResponse(record *shareRecord) shareResponse {
	resp := shareResponse{ID: record.ID, Project: record.Project, Path: record.Path, ExpiresAt: record.ExpiresAt.UTC().Format(time.RFC3339), IsDir: record.IsDir}
	if record.Token == "" {
		return resp
	}
	resp.URL = "/?share=" + url.QueryEscape(record.Token)
	if !record.IsDir {
		resp.DownloadURL = h.opts.BasePath + "/shares/" + url.PathEscape(record.ID) + "/download?token=" + url.QueryEscape(record.Token)
	}
	return resp
}

func newShareID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
