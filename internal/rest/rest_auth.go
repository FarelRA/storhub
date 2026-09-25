package rest

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

const (
	defaultRESTTokenTTL = 12 * 720 * storcfg.PatienceUnit // 12 hours
	restTokenIssuer     = "storhub"
	restTokenAudience   = "storhubrest"
)

// tokenKindAuth and tokenKindShare name the two capabilities minted on the
// single signing key. The kind selects the lane: full identity for auth,
// one project path for shares.
const (
	tokenKindAuth  = "auth"
	tokenKindShare = "share"
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

// AuthOptions configures REST authentication: realm, users, and tokens.
type AuthOptions struct {
	Realm           string
	Users           []User
	TokenSigningKey []byte
	TokenTTL        time.Duration
	Now             func() time.Time
}

// User is one REST principal with credentials and identity mapping.
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
	edKey    ed25519.PrivateKey
	tokenTTL time.Duration
	now      func() time.Time
	// verify is a seam over verifyPassword so tests can pin the
	// constant-work property of login (unknown users must still pay a
	// bcrypt verification).
	verify func(password, encoded string) bool
}

type restPrincipal struct {
	Kind       string   `json:"kind"`
	Username   string   `json:"username"`
	UID        uint32   `json:"uid"`
	PrimaryGID uint32   `json:"primary_gid"`
	Groups     []uint32 `json:"groups"`
	Admin      bool     `json:"admin,omitempty"`
}

// unifiedClaims is the normalized shape: one EdDSA key, kind distinguishes
// capabilities. Auth tokens carry identity, share tokens carry project/path.
// Both use the same issuer/audience and are verified by the same key.
// Wire keys stay abbreviated (usr/gid/prj/pth/dir map the Go fields
// Username/PrimaryGID/Project/Path/IsDir); login JSON keeps the long names.
// The abbreviation is frozen: old tokens must keep verifying.
type unifiedClaims struct {
	jwt.RegisteredClaims
	Kind       string   `json:"kind"`
	Username   string   `json:"usr,omitempty"`
	UID        uint32   `json:"uid,omitempty"`
	PrimaryGID uint32   `json:"gid,omitempty"`
	Groups     []uint32 `json:"groups,omitempty"`
	Admin      bool     `json:"admin,omitempty"`
	ID         string   `json:"id,omitempty"`
	Project    string   `json:"prj,omitempty"`
	Path       string   `json:"pth,omitempty"`
	IsDir      bool     `json:"dir,omitempty"`
}

type restLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type restLoginResponse struct {
	Token     string `json:"token"`
	TokenType string `json:"token_type"`
	// ExpiresIn carries seconds. New lifetime fields use expires_in_seconds;
	// the login spelling is frozen for existing clients.
	ExpiresIn int64         `json:"expires_in"`
	Principal restPrincipal `json:"principal"`
}

// HashPassword hashes a password for storage in a User record.
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
	seed := sha256.Sum256(opts.TokenSigningKey)
	edKey := ed25519.NewKeyFromSeed(seed[:32])
	return &restAuthenticator{realm: opts.Realm, users: users, edKey: edKey, tokenTTL: opts.TokenTTL, now: opts.Now, verify: verifyPassword}, nil
}

func (a *restAuthenticator) login(username, password string) (restPrincipal, string, time.Duration, error) {
	user, ok := a.users[username]
	if !ok {
		// Verify against a dummy hash anyway: skipping bcrypt for unknown
		// users makes the response time reveal which usernames exist.
		a.verify(password, dummyPasswordHash())
		return restPrincipal{}, "", 0, errors.New("invalid credentials")
	}
	if user.Disabled || !a.verify(password, user.PasswordHash) {
		return restPrincipal{}, "", 0, errors.New("invalid credentials")
	}
	principal := restPrincipal{Kind: tokenKindAuth, Username: user.Username, UID: user.UID, PrimaryGID: user.PrimaryGID, Groups: append([]uint32(nil), user.Groups...), Admin: user.Admin}
	token, err := a.signToken(principal)
	return principal, token, a.tokenTTL, err
}

func (a *restAuthenticator) signToken(principal restPrincipal) (string, error) {
	now := a.now()
	claims := unifiedClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    restTokenIssuer,
			Audience:  jwt.ClaimStrings{restTokenAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(a.tokenTTL)),
		},
		Kind:       tokenKindAuth,
		Username:   principal.Username,
		UID:        principal.UID,
		PrimaryGID: principal.PrimaryGID,
		Groups:     append([]uint32(nil), principal.Groups...),
		Admin:      principal.Admin,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	return token.SignedString(a.edKey)
}

func (a *restAuthenticator) parseToken(token string) (*restPrincipal, error) {
	uc := &unifiedClaims{}
	parsed, err := jwt.ParseWithClaims(token, uc, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodEd25519); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.edKey.Public(), nil
	},
		jwt.WithIssuer(restTokenIssuer),
		jwt.WithAudience(restTokenAudience),
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithTimeFunc(a.now),
	)
	if err != nil || !parsed.Valid || uc.Kind != tokenKindAuth {
		return nil, errors.New("invalid bearer token")
	}
	return &restPrincipal{
		Kind:       tokenKindAuth,
		Username:   uc.Username,
		UID:        uc.UID,
		PrimaryGID: uc.PrimaryGID,
		Groups:     append([]uint32(nil), uc.Groups...),
		Admin:      uc.Admin,
	}, nil
}

// currentPrincipal re-reads the user record behind an already-verified
// token. Auth JWTs are otherwise irrevocable: without this check a
// disabled, demoted, or removed account would keep the access baked into
// its claims until expiry. The live record wins over the stale claims; ok
// is false when the account no longer exists or is disabled.
func (a *restAuthenticator) currentPrincipal(parsed *restPrincipal) (*restPrincipal, bool) {
	user, ok := a.users[parsed.Username]
	if !ok || user.Disabled {
		return nil, false
	}
	principal := *parsed
	principal.UID = user.UID
	principal.PrimaryGID = user.PrimaryGID
	principal.Groups = append([]uint32(nil), user.Groups...)
	principal.Admin = user.Admin
	return &principal, true
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
