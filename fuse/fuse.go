// Package fuse is the public FUSE mount facade: default options and the
// New constructor over the internal FUSE implementation.
package fuse

import "github.com/FarelRA/storhub/storhub"

type (
	// Options configures a FUSE mount; see storhub.FUSEOptions.
	Options = storhub.FUSEOptions
	// Filesystem is a mounted FUSE filesystem; see storhub.FS.
	Filesystem = storhub.FS
)

// DefaultOptions returns Options with defaults applied.
func DefaultOptions() Options {
	return storhub.DefaultFUSEOptions()
}

// New mounts the project and returns its filesystem handle.
func New(hub *storhub.StorHub, project string, opts Options) (*Filesystem, error) {
	return hub.NewFUSE(project, opts)
}
