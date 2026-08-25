package rest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

const (
	defaultRESTTokenTTL = 12 * time.Hour
	restTokenIssuer     = "storhub"
	restTokenAudience   = "storhub-rest"
)

// dummyPasswordHash lazily builds a valid bcrypt hash of a value nobody
// logs in with; unknown users are verified against it so login timing does
// not enumerate usernames.
var dummyPasswordHash = sync.OnceValue(func() string {
	hash, err := bcrypt.GenerateFromPassword([]byte("storhub:"+strings.Repeat("x", 32)), bcrypt.DefaultCost)
	if err != nil {
		return ""
	}
	return "bcrypt$" + string(hash)
})

type AuthOptions struct {
	Realm           string
	Users           []User
	TokenSigningKey []byte
	TokenTTL        time.Duration
	Now             func() time.Time
}

type User struct {
	Username     string   `json:"username"`
	PasswordHash string   `json:"password_hash"`
	Password     string   `json:"password,omitempty"`
	UID          uint32   `json:"uid"`
	PrimaryGID   uint32   `json:"primary_gid"`
	Groups       []uint32 `json:"groups,omitempty"`
	Admin        bool     `json:"admin,omitempty"`
	Disabled     bool     `json:"disabled,omitempty"`
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
}

type restTokenClaims struct {
	restPrincipal
	jwt.RegisteredClaims
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
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return "bcrypt$" + string(hash), nil
}

func newAuthenticator(opts AuthOptions) (*restAuthenticator, error) {
	if len(opts.Users) == 0 {
		return nil, errors.New("rest auth requires at least one user")
	}
	if len(opts.TokenSigningKey) < 32 {
		return nil, errors.New("security constraint: token signing key must be at least 32 bytes")
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
		// Copy before appending: the caller owns the slice we were handed.
		groups := make([]uint32, 0, len(user.Groups)+1)
		groups = append(groups, user.Groups...)
		user.Groups = uniqueGIDs(append(groups, user.PrimaryGID))
		users[user.Username] = user
	}
	return &restAuthenticator{realm: opts.Realm, users: users, key: append([]byte(nil), opts.TokenSigningKey...), tokenTTL: opts.TokenTTL, now: opts.Now}, nil
}

func (a *restAuthenticator) login(username, password string) (restPrincipal, string, time.Duration, error) {
	user, ok := a.users[username]
	if !ok {
		// Verify against a dummy hash anyway: skipping bcrypt for unknown
		// users makes the response time reveal which usernames exist.
		verifyPassword(password, dummyPasswordHash())
		return restPrincipal{}, "", 0, errors.New("invalid credentials")
	}
	if user.Disabled || !verifyPassword(password, user.PasswordHash) {
		return restPrincipal{}, "", 0, errors.New("invalid credentials")
	}
	principal := restPrincipal{Kind: "auth", Username: user.Username, UID: user.UID, PrimaryGID: user.PrimaryGID, Groups: append([]uint32(nil), user.Groups...), Admin: user.Admin}
	token, err := a.signToken(principal)
	return principal, token, a.tokenTTL, err
}

func (a *restAuthenticator) signToken(principal restPrincipal) (string, error) {
	now := a.now()
	claims := restTokenClaims{
		restPrincipal: principal,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    restTokenIssuer,
			Audience:  jwt.ClaimStrings{restTokenAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(a.tokenTTL)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(a.key)
}

func (a *restAuthenticator) parseToken(token string) (*restPrincipal, error) {
	claims := &restTokenClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.key, nil
	},
		jwt.WithIssuer(restTokenIssuer),
		jwt.WithAudience(restTokenAudience),
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithTimeFunc(a.now),
	)
	if err != nil {
		return nil, errors.New("invalid bearer token")
	}
	principal := claims.restPrincipal
	if principal.Kind != "auth" || !parsed.Valid {
		return nil, errors.New("invalid bearer token")
	}
	return &principal, nil
}

func verifyPassword(password, encoded string) bool {
	if hash, ok := strings.CutPrefix(encoded, "bcrypt$"); ok {
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
	}
	return false
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

func (c *authorizedClient) CreateFileContext(ctx context.Context, project, filePath string) (*metadata.FileMeta, error) {
	if err := c.requireCreate(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.CreateFileContext(ctx, project, filePath)
}
func (c *authorizedClient) MkdirContext(ctx context.Context, project, dirPath string) error {
	if err := c.requireCreate(ctx, project, dirPath); err != nil {
		return err
	}
	return c.base.MkdirContext(ctx, project, dirPath)
}
func (c *authorizedClient) DeleteFileContext(ctx context.Context, project, filePath string, opts ...shfs.MutateOption) error {
	if err := c.requireParentWrite(ctx, project, filePath); err != nil {
		return err
	}
	return c.base.DeleteFileContext(ctx, project, filePath, opts...)
}
func (c *authorizedClient) RmdirContext(ctx context.Context, project, dirPath string, opts ...shfs.MutateOption) error {
	if err := c.requireParentWrite(ctx, project, dirPath); err != nil {
		return err
	}
	if err := c.requireTraverse(ctx, project, dirPath); err != nil {
		return err
	}
	return c.base.RmdirContext(ctx, project, dirPath, opts...)
}
func (c *authorizedClient) RenameContext(ctx context.Context, project, oldPath, newPath string) error {
	if err := c.requireParentWrite(ctx, project, oldPath); err != nil {
		return err
	}
	if err := c.requireParentWrite(ctx, project, newPath); err != nil {
		return err
	}
	if err := c.requireTraverse(ctx, project, oldPath); err != nil {
		return err
	}
	return c.base.RenameContext(ctx, project, oldPath, newPath)
}
func (c *authorizedClient) TruncateFileContext(ctx context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.TruncateFileContext(ctx, project, filePath, size, opts...)
}
func (c *authorizedClient) AppendFileContext(ctx context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.AppendFileContext(ctx, project, filePath, data, opts...)
}
func (c *authorizedClient) WriteFileAtContext(ctx context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.WriteFileAtContext(ctx, project, filePath, offset, data, opts...)
}
func (c *authorizedClient) PatchFileContext(ctx context.Context, project, filePath string, offset, deleteSize int64, edit []byte, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.PatchFileContext(ctx, project, filePath, offset, deleteSize, edit, opts...)
}
func (c *authorizedClient) ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (*metadata.FileMeta, error) {
	if err := c.requireNodeWrite(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.ReplaceFileFromReaderContext(ctx, project, filePath, body, opts...)
}
func (c *authorizedClient) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	if err := c.requireNodeRead(ctx, project, filePath); err != nil {
		return nil, err
	}
	return c.base.ReadFileAtContext(ctx, project, filePath, offset, length)
}
func (c *authorizedClient) StatPathContext(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	if err := c.requireTraverse(ctx, project, targetPath); err != nil {
		return nil, err
	}
	entry, err := c.base.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return entry, nil
}
func (c *authorizedClient) ReadDirContext(ctx context.Context, project, dirPath string) ([]shfs.DirEntry, error) {
	entry, err := c.base.StatPathContext(ctx, project, dirPath)
	if err != nil {
		return nil, err
	}
	if err := c.requireTraverse(ctx, project, dirPath); err != nil {
		return nil, err
	}
	if !c.hasPerm(entry, permRead|permExec) {
		return nil, errForbidden("permission denied")
	}
	return c.base.ReadDirContext(ctx, project, dirPath)
}
func (c *authorizedClient) StatFSContext(ctx context.Context, project string) (*shfs.FSStats, error) {
	entry, err := c.base.StatPathContext(ctx, project, "")
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return c.base.StatFSContext(ctx, project)
}
func (c *authorizedClient) SymlinkContext(ctx context.Context, project, target, linkPath string) (*metadata.FileMeta, error) {
	if err := c.requireCreate(ctx, project, linkPath); err != nil {
		return nil, err
	}
	return c.base.SymlinkContext(ctx, project, target, linkPath)
}
func (c *authorizedClient) ReadlinkContext(ctx context.Context, project, linkPath string) (string, error) {
	if err := c.requireTraverse(ctx, project, linkPath); err != nil {
		return "", err
	}
	entry, err := c.base.StatPathContext(ctx, project, linkPath)
	if err != nil {
		return "", err
	}
	if !c.canReadMetadata(entry) {
		return "", errForbidden("permission denied")
	}
	return c.base.ReadlinkContext(ctx, project, linkPath)
}
func (c *authorizedClient) LinkContext(ctx context.Context, project, existingPath, newPath string) (*metadata.FileMeta, error) {
	if err := c.requireNodeRead(ctx, project, existingPath); err != nil {
		return nil, err
	}
	if err := c.requireCreate(ctx, project, newPath); err != nil {
		return nil, err
	}
	return c.base.LinkContext(ctx, project, existingPath, newPath)
}
func (c *authorizedClient) ChmodContext(ctx context.Context, project, targetPath string, mode uint32) error {
	entry, err := c.base.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if !c.principal.Admin && c.principal.UID != entry.UID {
		return errForbidden("permission denied")
	}
	return c.base.ChmodContext(ctx, project, targetPath, mode)
}
func (c *authorizedClient) ChownContext(ctx context.Context, project, targetPath string, uid, gid uint32) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.ChownContext(ctx, project, targetPath, uid, gid)
}
func (c *authorizedClient) ChtimesContext(ctx context.Context, project, targetPath string, atime, mtime int64) error {
	entry, err := c.base.StatPathContext(ctx, project, targetPath)
	if err != nil {
		return err
	}
	if !c.principal.Admin && c.principal.UID != entry.UID && !c.hasPerm(entry, permWrite) {
		return errForbidden("permission denied")
	}
	return c.base.ChtimesContext(ctx, project, targetPath, atime, mtime)
}
func (c *authorizedClient) SetXAttrContext(ctx context.Context, project, targetPath, attr string, data []byte) error {
	if err := c.requireNodeWrite(ctx, project, targetPath); err != nil {
		return err
	}
	return c.base.SetXAttrContext(ctx, project, targetPath, attr, data)
}
func (c *authorizedClient) GetXAttrContext(ctx context.Context, project, targetPath, attr string) ([]byte, error) {
	if err := c.requireNodeRead(ctx, project, targetPath); err != nil {
		return nil, err
	}
	return c.base.GetXAttrContext(ctx, project, targetPath, attr)
}
func (c *authorizedClient) ListXAttrContext(ctx context.Context, project, targetPath string) ([]string, error) {
	if err := c.requireNodeRead(ctx, project, targetPath); err != nil {
		return nil, err
	}
	return c.base.ListXAttrContext(ctx, project, targetPath)
}
func (c *authorizedClient) RemoveXAttrContext(ctx context.Context, project, targetPath, attr string) error {
	if err := c.requireNodeWrite(ctx, project, targetPath); err != nil {
		return err
	}
	return c.base.RemoveXAttrContext(ctx, project, targetPath, attr)
}

// RevisionContext exposes the repo-level metadata revision; gated like
// other project-wide metadata (root readability).
func (c *authorizedClient) RevisionContext(ctx context.Context, project string) (string, error) {
	entry, err := c.base.StatPathContext(ctx, project, "")
	if err != nil {
		return "", err
	}
	if !c.canReadMetadata(entry) {
		return "", errForbidden("permission denied")
	}
	if rs, ok := c.base.(shfs.RevisionSource); ok {
		return rs.RevisionContext(ctx, project)
	}
	return "", errForbidden("revision preconditions unsupported by backend")
}

func (c *authorizedClient) ListMetadataRevisionsContext(ctx context.Context, project string) ([]metadata.MetadataRevision, error) {
	entry, err := c.base.StatPathContext(ctx, project, "")
	if err != nil {
		return nil, err
	}
	if !c.canReadMetadata(entry) {
		return nil, errForbidden("permission denied")
	}
	return c.base.ListMetadataRevisionsContext(ctx, project)
}
func (c *authorizedClient) RollbackMetadataContext(ctx context.Context, project, commitSHA string) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.RollbackMetadataContext(ctx, project, commitSHA)
}
func (c *authorizedClient) PurgeUntrackedContext(ctx context.Context, project string) (*storage.PurgeResult, error) {
	if !c.principal.Admin {
		return nil, errForbidden("permission denied")
	}
	return c.base.PurgeUntrackedContext(ctx, project)
}
func (c *authorizedClient) DeleteProjectContext(ctx context.Context, project string) error {
	if !c.principal.Admin {
		return errForbidden("permission denied")
	}
	return c.base.DeleteProjectContext(ctx, project)
}

const (
	permRead  = 4
	permWrite = 2
	permExec  = 1
)

func (c *authorizedClient) requireCreate(ctx context.Context, project, filePath string) error {
	return c.requireParentWrite(ctx, project, filePath)
}

func (c *authorizedClient) requireNodeRead(ctx context.Context, project, filePath string) error {
	if err := c.requireTraverse(ctx, project, filePath); err != nil {
		return err
	}
	entry, err := c.base.StatPathContext(ctx, project, filePath)
	if err != nil {
		return err
	}
	if !c.hasPerm(entry, permRead) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireNodeWrite(ctx context.Context, project, filePath string) error {
	if err := c.requireTraverse(ctx, project, filePath); err != nil {
		return err
	}
	entry, err := c.base.StatPathContext(ctx, project, filePath)
	if err != nil {
		return err
	}
	if !c.hasPerm(entry, permWrite) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireParentWrite(ctx context.Context, project, filePath string) error {
	parent := parentRESTPath(filePath)
	if err := c.requireTraverse(ctx, project, parent); err != nil {
		return err
	}
	entry, err := c.base.StatPathContext(ctx, project, parent)
	if err != nil {
		return err
	}
	if !entry.IsDir || !c.hasPerm(entry, permWrite|permExec) {
		return errForbidden("permission denied")
	}
	return nil
}

func (c *authorizedClient) requireTraverse(ctx context.Context, project, targetPath string) error {
	if c.principal.Admin {
		return nil
	}
	for _, dir := range ancestorRESTPaths(targetPath) {
		entry, err := c.base.StatPathContext(ctx, project, dir)
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
