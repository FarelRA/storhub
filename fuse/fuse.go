package fuse

import "github.com/FarelRA/storhub/storhub"

type (
	Options    = storhub.FUSEOptions
	Filesystem = storhub.StorHubFS
)

func DefaultOptions() Options {
	return storhub.DefaultFUSEOptions()
}

func New(hub *storhub.StorHub, project string, opts Options) (*Filesystem, error) {
	return hub.NewFUSE(project, opts)
}
