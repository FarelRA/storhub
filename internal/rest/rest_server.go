package rest

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"github.com/FarelRA/storhub/internal/logging"
	storage "github.com/FarelRA/storhub/internal/storage"
	"log/slog"
	"net/http"
	"strings"
)

// DefaultOptions returns Options with production streaming and share defaults.
func DefaultOptions() Options {
	return Options{
		BasePath:         defaultRESTBasePath,
		StreamChunkSize:  defaultRESTStreamChunk,
		MaxPatchBodySize: defaultRESTPatchBodySize,
		ShareTTL:         defaultRESTShareTTL,
	}
}

// NewHandler builds the chi router over hub with opts; a nil hub is an error.
func NewHandler(hub *storage.StorHub, opts Options) (http.Handler, error) {
	if hub == nil {
		return nil, errors.New("storhub: REST handler requires a non-nil hub")
	}
	return newHandlerForClient(directClient{hub}, opts)
}

// directClient adapts *storage.StorHub to Client where the storage
// method is bare but the surface contract is fallible: admin gating
// needs an error channel, so these wrappers lift success into it.
// Everything else promotes from the embedded hub with no adapter.
type directClient struct {
	*storage.StorHub
}

func (d directClient) DegradedProjects() ([]string, error) {
	return d.StorHub.DegradedProjects(), nil
}

func (d directClient) PressureSnapshot() (storage.PressureSnapshot, error) {
	return d.StorHub.PressureSnapshot(), nil
}

func (d directClient) PressureFailureStreak(project string) (uint64, error) {
	return d.StorHub.PressureFailureStreak(project), nil
}

func (d directClient) PressurePendingDepth(project string) (int, error) {
	return d.StorHub.PressurePendingDepth(project), nil
}

// newRestHandler validates keys, applies defaults, derives the EdDSA share
// key, and builds the handler plus its authenticator (nil for anonymous).
// Split out of newHandlerForClient so route-table construction
// (registerRoutes inline above) and login handling read as separate steps.
func newRestHandler(client Client, opts Options) (*restHandler, *restAuthenticator, error) {
	if opts.Auth == nil && !opts.AllowAnonymous {
		return nil, nil, errors.New("security constraint: no Auth configured; set AllowAnonymous:true to serve unauthenticated traffic deliberately")
	}
	opts = opts.withDefaults()
	logger := logging.WithComponent(nil, "rest")
	if provider, ok := client.(interface{ Logger() *slog.Logger }); ok && provider.Logger() != nil {
		logger = logging.WithComponent(provider.Logger(), "rest")
	}
	// Unified key: all cards (auth and share) derive from TokenSigningKey.
	// This gives one EdDSA key for the whole API, share and login are just
	// different capabilities (kind) on the same JWT.
	if opts.Auth != nil {
		if len(opts.Auth.TokenSigningKey) < 32 {
			return nil, nil, errors.New("security constraint: token signing key must be at least 32 bytes")
		}
		if isWeakShareKey(opts.Auth.TokenSigningKey) {
			return nil, nil, errors.New("security constraint: token signing key is a known weak/default key")
		}
	}
	if len(opts.ShareSigningKey) > 0 {
		if len(opts.ShareSigningKey) < 32 {
			return nil, nil, errors.New("security constraint: share signing key must be at least 32 bytes")
		}
		if isWeakShareKey(opts.ShareSigningKey) {
			return nil, nil, errors.New("security constraint: share signing key is a known weak/default key")
		}
	}
	h := &restHandler{client: client, opts: opts, shares: newShareRegistry(), logger: logger}
	// Explicit share key wins; the auth key only fills the gap. Never
	// overwrite an explicit key: CLI authfile deploys set both, and the
	// explicit value is the operator's choice.
	if len(opts.ShareSigningKey) > 0 {
		seed := opts.ShareSigningKey
		if len(seed) > 32 {
			hash := sha256.Sum256(seed)
			seed = hash[:]
		}
		h.shareSignKey = ed25519.NewKeyFromSeed(seed[:32])
	} else if opts.Auth != nil && len(opts.Auth.TokenSigningKey) > 0 {
		seed := sha256.Sum256(opts.Auth.TokenSigningKey)
		h.shareSignKey = ed25519.NewKeyFromSeed(seed[:32])
		h.opts.ShareSigningKey = h.shareSignKey.Seed()
	}
	if opts.Auth == nil {
		return h, nil, nil
	}
	auth, err := newAuthenticator(*opts.Auth)
	if err != nil {
		return nil, nil, err
	}
	return h, auth, nil
}

func (o Options) withDefaults() Options {
	if strings.TrimSpace(o.BasePath) == "" {
		o.BasePath = defaultRESTBasePath
	}
	o.BasePath = "/" + strings.Trim(strings.TrimSpace(o.BasePath), "/")
	// "/" normalizes to an empty route pattern, which panics chi at
	// construction; treat it as "not set" and fall back to the default.
	if o.BasePath == "/" {
		o.BasePath = defaultRESTBasePath
	}
	if o.StreamChunkSize <= 0 {
		o.StreamChunkSize = defaultRESTStreamChunk
	}
	if o.MaxPatchBodySize <= 0 {
		o.MaxPatchBodySize = defaultRESTPatchBodySize
	}
	if o.ShareTTL <= 0 {
		o.ShareTTL = defaultRESTShareTTL
	}
	// A zero MaxShareTTL must not mean "unbounded": default it so the
	// client-requested-lifetime clamp is always armed.
	if o.MaxShareTTL <= 0 {
		o.MaxShareTTL = defaultRESTShareTTL
	}
	return o
}
