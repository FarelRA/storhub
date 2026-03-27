package rest

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
)

const defaultRESTTokenTTL = 12 * time.Hour

type AuthOptions struct {
	Realm           string
	Users           []User
	TokenSigningKey []byte
	TokenTTL        time.Duration
	Now             func() time.Time
}

type User struct {
	Username     string
	PasswordHash string
	Password     string
	UID          uint32
	PrimaryGID   uint32
	Groups       []uint32
	Admin        bool
	Disabled     bool
}

type restAuthHandler struct {
	base     *restHandler
	auth     *restAuthenticator
	basePath string
}

type restAuthenticator struct {
	realm    string
	users    map[string]User
	key      []byte
	tokenTTL time.Duration
	now      func() time.Time
}

type restPrincipal struct {
	Kind       string   `json:"kind"`
	Username   string   `json:"username"`
	UID        uint32   `json:"uid"`
	PrimaryGID uint32   `json:"primary_gid"`
	Groups     []uint32 `json:"groups"`
	Admin      bool     `json:"admin,omitempty"`
	ExpiresAt  int64    `json:"exp"`
}

type restLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type restLoginResponse struct {
	Token     string        `json:"token"`
	TokenType string        `json:"token_type"`
	ExpiresIn int64         `json:"expires_in"`
	Principal restPrincipal `json:"principal"`
}

func HashPassword(password string) (string, error) {
	if strings.TrimSpace(password) == "" {
		return "", errors.New("password is required")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	const iterations = 200000
	sum := derivePasswordHash([]byte(password), salt, iterations)
	return fmt.Sprintf("sha256$%d$%s$%s", iterations, hex.EncodeToString(salt), hex.EncodeToString(sum)), nil
}

func newAuthenticator(opts AuthOptions) (*restAuthenticator, error) {
	if len(opts.Users) == 0 {
		return nil, errors.New("rest auth requires at least one user")
	}
	if len(opts.TokenSigningKey) == 0 {
		return nil, errors.New("rest auth requires a token signing key")
	}
	if opts.TokenTTL <= 0 {
		opts.TokenTTL = defaultRESTTokenTTL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if strings.TrimSpace(opts.Realm) == "" {
		opts.Realm = "storhub"
	}
	users := make(map[string]User, len(opts.Users))
	for _, user := range opts.Users {
		if strings.TrimSpace(user.Username) == "" {
			return nil, errors.New("rest auth username is required")
		}
		if user.PasswordHash == "" {
			if strings.TrimSpace(user.Password) == "" {
				return nil, fmt.Errorf("rest auth user %q requires password or password hash", user.Username)
			}
			hash, err := HashPassword(user.Password)
			if err != nil {
				return nil, err
			}
			user.PasswordHash = hash
		}
		user.Password = ""
		user.Groups = uniqueGIDs(append(user.Groups, user.PrimaryGID))
		users[user.Username] = user
	}
	return &restAuthenticator{realm: opts.Realm, users: users, key: append([]byte(nil), opts.TokenSigningKey...), tokenTTL: opts.TokenTTL, now: opts.Now}, nil
}

func (h *restAuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.base.serveUI(w, r) {
		return
	}
	if strings.HasPrefix(r.URL.Path, "/shares/") {
		h.base.handleShareAccess(w, r)
		return
	}
	if r.URL.Path == strings.TrimRight(h.basePath, "/")+"/auth/login" {
		h.handleLogin(w, r)
		return
	}

	// Try to get bearer token
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(auth, "Bearer ") {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q`, h.auth.realm))
		h.base.writeError(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
		return
	}

	token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))

	// Try to parse as regular auth token first
	principal, err := h.auth.parseToken(token)
	if err == nil {
		// Regular auth token - use authorized client
		authed := *h.base
		authed.client = &authorizedClient{base: h.base.client, principal: principal}
		authed.ServeHTTP(w, r)
		return
	}

	// Try to parse as share token
	claims, err := h.base.parseShareToken(token)
	if err == nil {
		// Share token - use restricted client
		restricted := *h.base
		restricted.client = newRestrictedClient(h.base.client, claims.Project, claims.Path)
		restricted.ServeHTTP(w, r)
		return
	}

	// Neither token type worked
	w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q`, h.auth.realm))
	h.base.writeError(w, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
}

func (h *restAuthHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.base.methodNotAllowed(w, http.MethodPost)
		return
	}
	var req restLoginRequest
	if err := h.base.decodeJSON(r, &req); err != nil {
		h.base.writeMappedError(w, err)
		return
	}
	principal, token, ttl, err := h.auth.login(req.Username, req.Password)
	if err != nil {
		h.base.writeError(w, http.StatusUnauthorized, "invalid_credentials", "invalid username or password")
		return
	}
	h.base.writeJSON(w, http.StatusOK, restLoginResponse{Token: token, TokenType: "Bearer", ExpiresIn: int64(ttl.Seconds()), Principal: principal})
}

func (h *restAuthHandler) requireBearer(r *http.Request) (*restPrincipal, error) {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(auth, "Bearer ") {
		return nil, errors.New("missing bearer token")
	}
	return h.auth.parseToken(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")))
}

func (a *restAuthenticator) login(username, password string) (restPrincipal, string, time.Duration, error) {
	user, ok := a.users[username]
	if !ok || user.Disabled || !verifyPassword(password, user.PasswordHash) {
		return restPrincipal{}, "", 0, errors.New("invalid credentials")
	}
	principal := restPrincipal{Kind: "auth", Username: user.Username, UID: user.UID, PrimaryGID: user.PrimaryGID, Groups: append([]uint32(nil), user.Groups...), Admin: user.Admin, ExpiresAt: a.now().Add(a.tokenTTL).Unix()}
	token, err := a.signToken(principal)
	return principal, token, a.tokenTTL, err
}

func (a *restAuthenticator) signToken(principal restPrincipal) (string, error) {
	payload, err := json.Marshal(principal)
	if err != nil {
		return "", err
	}
	payloadPart := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, a.key)
	_, _ = mac.Write([]byte(payloadPart))
	sigPart := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payloadPart + "." + sigPart, nil
}

func (a *restAuthenticator) parseToken(token string) (*restPrincipal, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, errors.New("invalid bearer token")
	}
	mac := hmac.New(sha256.New, a.key)
	_, _ = mac.Write([]byte(parts[0]))
	expected := mac.Sum(nil)
	actual, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || subtle.ConstantTimeCompare(actual, expected) != 1 {
		return nil, errors.New("invalid bearer token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, errors.New("invalid bearer token")
	}
	var principal restPrincipal
	if err := json.Unmarshal(payload, &principal); err != nil {
		return nil, errors.New("invalid bearer token")
	}
	if principal.Kind != "auth" {
		return nil, errors.New("invalid bearer token")
	}
	if principal.ExpiresAt <= a.now().Unix() {
		return nil, errors.New("bearer token expired")
	}
	return &principal, nil
}

func derivePasswordHash(password, salt []byte, iterations int) []byte {
	sum := append([]byte(nil), salt...)
	sum = append(sum, password...)
	digest := sha256.Sum256(sum)
	result := digest[:]
	for i := 1; i < iterations; i++ {
		next := make([]byte, 0, len(result)+len(password)+len(salt))
		next = append(next, result...)
		next = append(next, password...)
		next = append(next, salt...)
		d := sha256.Sum256(next)
		result = d[:]
	}
	return append([]byte(nil), result...)
}

func verifyPassword(password, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "sha256" {
		return false
	}
	iterations := 0
	_, err := fmt.Sscanf(parts[1], "%d", &iterations)
	if err != nil || iterations <= 0 {
		return false
	}
	salt, err := hex.DecodeString(parts[2])
	if err != nil {
		return false
	}
	want, err := hex.DecodeString(parts[3])
	if err != nil {
		return false
	}
	got := derivePasswordHash([]byte(password), salt, iterations)
	return subtle.ConstantTimeCompare(got, want) == 1
}

func uniqueGIDs(groups []uint32) []uint32 {
	seen := map[uint32]struct{}{}
	result := make([]uint32, 0, len(groups))
	for _, gid := range groups {
		if _, ok := seen[gid]; ok {
			continue
		}
		seen[gid] = struct{}{}
		result = append(result, gid)
	}
	return result
}

type authorizedClient struct {
	base      Client
	principal *restPrincipal
}

func (c *authorizedClient) CreateFile(project, filePath string) (*metadata.FileMetadata, error) {
	if err := c.requireCreate(project, filePath); err != nil {
		return nil, err
	}
	return c.base.CreateFile(project, filePath)
}
func (c *authorizedClient) Mkdir(project, dirPath string) error {
	if err := c.requireCreate(project, dirPath); err != nil {
		return err
	}
	return c.base.Mkdir(project, dirPath)
}
func (c *authorizedClient) DeleteFile(project, filePath string) error {
	if err := c.requireParentWrite(project, filePath); err != nil {
		return err
	}
	return c.base.DeleteFile(project, filePath)
}
func (c *authorizedClient) Rmdir(project, dirPath string) error {
	if err := c.requireParentWrite(project, dirPath); err != nil {
		return err
	}
	if err := c.requireTraverse(project, dirPath); err != nil {
		return err
	}
	return c.base.Rmdir(project, dirPath)
}
func (c *authorizedClient) Rename(project, oldPath, newPath string) error {
	if err := c.requireParentWrite(project, oldPath); err != nil {
		return err
	}
	if err := c.requireParentWrite(project, newPath); err != nil {
		return err
	}
	if err := c.requireTraverse(project, oldPath); err != nil {
		return err
	}
	return c.base.Rename(project, oldPath, newPath)
}
func (c *authorizedClient) TruncateFile(project, filePath string, size int64) (*metadata.FileMetadata, error) {
	if err := c.requireNodeWrite(project, filePath); err != nil {
		return nil, err
	}
	return c.base.TruncateFile(project, filePath, size)
}
func (c *authorizedClient) AppendFile(project, filePath string, data []byte) (*metadata.FileMetadata, error) {
	if err := c.requireNodeWrite(project, filePath); err != nil {
		return nil, err
	}
	return c.base.AppendFile(project, filePath, data)
}
func (c *authorizedClient) WriteFileAt(project, filePath string, offset int64, data []byte) (*metadata.FileMetadata, error) {
	if err := c.requireNodeWrite(project, filePath); err != nil {
		return nil, err
	}
	return c.base.WriteFileAt(project, filePath, offset, data)
}
func (c *authorizedClient) PatchFile(project, filePath string, offset, deleteSize int64, edit []byte) (*metadata.FileMetadata, error) {
	if err := c.requireNodeWrite(project, filePath); err != nil {
		return nil, err
	}
	return c.base.PatchFile(project, filePath, offset, deleteSize, edit)
}
func (c *authorizedClient) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	if err := c.requireNodeRead(project, filePath); err != nil {
		return nil, err
	}
	return c.base.ReadFileAt(project, filePath, offset, length)
}
func (c *authorizedClient) StatPath(project, targetPath string) (*shfs.EntryInfo, error) {
	if err := c.requireTraverse(project, targetPath); err != nil {
		return nil, err
	}
	entry, err := c.base.StatPath(project, targetPath)
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return entry, nil
}
func (c *authorizedClient) ReadDir(project, dirPath string) ([]shfs.DirEntry, error) {
	entry, err := c.base.StatPath(project, dirPath)
	if err != nil {
		return nil, err
	}
	if err := c.requireTraverse(project, dirPath); err != nil {
		return nil, err
	}
	if !c.hasPerm(entry, permRead|permExec) {
		return nil, errForbidden("permission denied")
	}
	return c.base.ReadDir(project, dirPath)
}
func (c *authorizedClient) StatFS(project string) (*shfs.FSStats, error) {
	entry, err := c.base.StatPath(project, "")
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return c.base.StatFS(project)
}
func (c *authorizedClient) Symlink(project, target, linkPath string) (*metadata.FileMetadata, error) {
	if err := c.requireCreate(project, linkPath); err != nil {
		return nil, err
	}
	return c.base.Symlink(project, target, linkPath)
}
func (c *authorizedClient) Readlink(project, linkPath string) (string, error) {
	if err := c.requireTraverse(project, linkPath); err != nil {
		return "", err
	}
	entry, err := c.base.StatPath(project, linkPath)
	if err != nil {
		return "", err
	}
	if !c.canReadMetadata(entry) {
		return "", errForbidden("permission denied")
	}
	return c.base.Readlink(project, linkPath)
}
func (c *authorizedClient) Link(project, existingPath, newPath string) (*metadata.FileMetadata, error) {
	if err := c.requireNodeRead(project, existingPath); err != nil {
		return nil, err
	}
	if err := c.requireCreate(project, newPath); err != nil {
		return nil, err
	}
	return c.base.Link(project, existingPath, newPath)
}
func (c *authorizedClient) Chmod(project, targetPath string, mode uint32) error {
	entry, err := c.base.StatPath(project, targetPath)
	if err != nil {
		return err
	}
	if !c.principal.Admin && c.principal.UID != entry.UID {
		return errForbidden("permission denied")
	}
	return c.base.Chmod(project, targetPath, mode)
}
func (c *authorizedClient) Chown(project, targetPath string, uid, gid uint32) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.Chown(project, targetPath, uid, gid)
}
func (c *authorizedClient) Chtimes(project, targetPath string, atime, mtime time.Time) error {
	entry, err := c.base.StatPath(project, targetPath)
	if err != nil {
		return err
	}
	if !c.principal.Admin && c.principal.UID != entry.UID && !c.hasPerm(entry, permWrite) {
		return errForbidden("permission denied")
	}
	return c.base.Chtimes(project, targetPath, atime, mtime)
}
func (c *authorizedClient) SetXAttr(project, targetPath, attr string, data []byte) error {
	if err := c.requireNodeWrite(project, targetPath); err != nil {
		return err
	}
	return c.base.SetXAttr(project, targetPath, attr, data)
}
func (c *authorizedClient) GetXAttr(project, targetPath, attr string) ([]byte, error) {
	if err := c.requireNodeRead(project, targetPath); err != nil {
		return nil, err
	}
	return c.base.GetXAttr(project, targetPath, attr)
}
func (c *authorizedClient) ListXAttr(project, targetPath string) ([]string, error) {
	if err := c.requireNodeRead(project, targetPath); err != nil {
		return nil, err
	}
	return c.base.ListXAttr(project, targetPath)
}
func (c *authorizedClient) RemoveXAttr(project, targetPath, attr string) error {
	if err := c.requireNodeWrite(project, targetPath); err != nil {
		return err
	}
	return c.base.RemoveXAttr(project, targetPath, attr)
}
func (c *authorizedClient) ListMetadataRevisions(project string) ([]metadata.MetadataRevision, error) {
	entry, err := c.base.StatPath(project, "")
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return c.base.ListMetadataRevisions(project)
}
func (c *authorizedClient) RollbackMetadata(project, commitSHA string) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.RollbackMetadata(project, commitSHA)
}
func (c *authorizedClient) DeleteProject(project string) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.DeleteProject(project)
}

const (
	permRead  = 4
	permWrite = 2
	permExec  = 1
)

func (c *authorizedClient) requireCreate(project, filePath string) error {
	return c.requireParentWrite(project, filePath)
}

func (c *authorizedClient) requireNodeRead(project, filePath string) error {
	if err := c.requireTraverse(project, filePath); err != nil {
		return err
	}
	entry, err := c.base.StatPath(project, filePath)
	if err != nil {
		return err
	}
	if !c.hasPerm(entry, permRead) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireNodeWrite(project, filePath string) error {
	if err := c.requireTraverse(project, filePath); err != nil {
		return err
	}
	entry, err := c.base.StatPath(project, filePath)
	if err != nil {
		return err
	}
	if !c.hasPerm(entry, permWrite) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireParentWrite(project, filePath string) error {
	parent := parentRESTPath(filePath)
	if err := c.requireTraverse(project, parent); err != nil {
		return err
	}
	entry, err := c.base.StatPath(project, parent)
	if err != nil {
		return err
	}
	if !entry.IsDir || !c.hasPerm(entry, permWrite|permExec) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireTraverse(project, targetPath string) error {
	if c.principal.Admin {
		return nil
	}
	for _, dir := range ancestorRESTPaths(targetPath) {
		entry, err := c.base.StatPath(project, dir)
		if err != nil {
			return err
		}
		if !entry.IsDir || !c.hasPerm(entry, permExec) {
			return errForbidden("permission denied")
		}
	}
	return nil
}

func (c *authorizedClient) canReadMetadata(entry *shfs.EntryInfo) bool {
	if c.principal.Admin {
		return true
	}
	if entry.IsDir {
		return c.hasPerm(entry, permExec)
	}
	return c.hasPerm(entry, permRead)
}

func (c *authorizedClient) hasPerm(entry *shfs.EntryInfo, need int) bool {
	if c.principal.Admin {
		return true
	}
	bits := int(entry.Mode & 0o777)
	shift := 0
	if c.principal.UID == entry.UID {
		shift = 6
	} else if c.inGroup(entry.GID) {
		shift = 3
	}
	perm := (bits >> shift) & 0x7
	return perm&need == need
}

func (c *authorizedClient) inGroup(gid uint32) bool {
	if gid == c.principal.PrimaryGID {
		return true
	}
	for _, group := range c.principal.Groups {
		if group == gid {
			return true
		}
	}
	return false
}

func ancestorRESTPaths(targetPath string) []string {
	clean := strings.Trim(path.Clean("/"+strings.TrimSpace(targetPath)), "/")
	if clean == "" {
		return []string{""}
	}
	parts := strings.Split(clean, "/")
	paths := []string{""}
	current := ""
	for i := 0; i < len(parts)-1; i++ {
		if current == "" {
			current = parts[i]
		} else {
			current += "/" + parts[i]
		}
		paths = append(paths, current)
	}
	return paths
}

func parentRESTPath(targetPath string) string {
	clean := strings.Trim(path.Clean("/"+strings.TrimSpace(targetPath)), "/")
	if clean == "" {
		return ""
	}
	parent := path.Dir(clean)
	if parent == "." {
		return ""
	}
	return parent
}
