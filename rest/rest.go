// Package rest exposes the StorHub REST API as a public facade over
// internal/rest. It re-exports the option and identity types embedders need
// to configure the server, plus the three entry points: DefaultOptions for
// sane defaults, HashPassword for preparing AuthOptions credentials, and New
// to build the http.Handler.
package rest

import (
	"net/http"

	impl "github.com/FarelRA/storhub/internal/rest"
	"github.com/FarelRA/storhub/storhub"
)

type (
	// Options configures the REST server: listen-time behavior, body/patch
	// limits, share-token policy, and authentication. See
	// internal/rest.Options for the field-level contract.
	Options = impl.Options
	// AuthOptions describes the user database backing HTTP basic auth:
	// users, their POSIX identities, and token lifetime settings.
	AuthOptions = impl.AuthOptions
	// User is a single authenticated principal with its POSIX identity
	// (UID, primary GID, supplementary groups) used for authorization.
	User = impl.User
)

// DefaultOptions returns Options with all defaults applied; use it as the
// base and override individual fields rather than building a zero Options.
func DefaultOptions() Options {
	return impl.DefaultOptions()
}

// HashPassword bcrypt-hashes a plaintext password for use in
// AuthOptions.Users.PasswordHash.
func HashPassword(password string) (string, error) {
	return impl.HashPassword(password)
}

// New builds the REST HTTP handler serving hub's storage projects according
// to opts. Authentication is required unless opts explicitly opts into
// anonymous access; the returned handler is safe for concurrent use.
func New(hub *storhub.StorHub, opts Options) (http.Handler, error) {
	return impl.NewHandler(hub, opts)
}
