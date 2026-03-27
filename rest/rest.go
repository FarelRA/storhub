package rest

import (
	"net/http"

	impl "github.com/FarelRA/storhub/internal/rest"
	"github.com/FarelRA/storhub/storhub"
)

type (
	Options     = impl.Options
	AuthOptions = impl.AuthOptions
	User        = impl.User
)

func DefaultOptions() Options {
	return impl.DefaultOptions()
}

func HashPassword(password string) (string, error) {
	return impl.HashPassword(password)
}

func New(hub *storhub.StorHub, opts Options) (http.Handler, error) {
	return impl.NewHandler(hub, opts)
}
