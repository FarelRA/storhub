// Package storhub is the public API facade for StorHub: a content-addressed
// file store backed by GitHub releases and metadata commits, with FUSE
// mount, REST server, and POSIX-style library access.
//
// The types below are aliases of internal implementations; they exist so
// embedders depend only on this package. Start with NewStorHub (or its
// Config/Context variants), then pass the client to DefaultFUSEOptions/New
// from the fuse and rest facades, or use the fs-style operations directly.
package storhub

import (
	"context"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	implfuse "github.com/FarelRA/storhub/internal/fusefs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
	impl "github.com/FarelRA/storhub/internal/storage"
)

type (
	// StorHub is the storage client: project/file/chunk operations over a
	// GitHub backend, with metadata versioning, rollback, and purge tools.
	StorHub = impl.StorHub
	// FUSEOptions configures a FUSE mount (cache location, atime policy,
	// overlay buffer sizing). See internal/fusefs.Options.
	FUSEOptions = implfuse.Options
	// StorHubFS is the mounted filesystem returned by the FUSE facade;
	// callers Unmount and Wait on it.
	StorHubFS = implfuse.Filesystem
	// ChunkInfo describes one stored chunk: its GitHub release asset name,
	// byte offset within the file, size, owning release tag, and digest.
	ChunkInfo = meta.ChunkInfo
	// FileMetadata is a stored regular file or symlink entry: mode, owner,
	// timestamps, xattrs, chunk list, and link target.
	FileMetadata = meta.FileMeta
	// ReleaseMetadata references a GitHub release that holds chunk assets.
	ReleaseMetadata = meta.ReleaseRef
	// RepoMetadata is the full metadata tree of a project: root directory,
	// files, directories, releases, chunks, counters, and stats.
	RepoMetadata = meta.RepoMetadata
	// MetadataRevision identifies one committed state of a project's
	// metadata history (git SHA plus commit time).
	MetadataRevision = meta.MetadataRevision
	// DirectoryMetadata is a directory entry; RootMetadata names the same
	// type when referring to the tree root.
	DirectoryMetadata = meta.DirMeta
	// RootMetadata is DirectoryMetadata viewed as the project root; both
	// names exist for call-site readability and are the same type.
	RootMetadata = meta.DirMeta
	// EntryInfo is the stat-style view of a path: kind, mode, size, owner,
	// timestamps, link count, and inode as surfaced by StatPath.
	EntryInfo = shfs.EntryInfo
	// DirEntry is one child name plus kind in a directory listing.
	DirEntry = shfs.DirEntry
	// FSStats aggregates project-wide counts (files, dirs, symlinks, bytes)
	// reported by the stats operation.
	FSStats = shfs.FSStats
	// NodeKind discriminates file system node types (file, symlink).
	NodeKind = meta.NodeKind
	// PurgeResult reports what PurgeUntracked removed and kept.
	PurgeResult = impl.PurgeResult
	// APIError is an error returned by the GitHub API layer, carrying the
	// HTTP status and parsed message.
	APIError = ghapi.APIError
	// Config tunes a StorHub client: API endpoint, transport, transfer
	// sizing, retry policy, logging, git cache, and test clocks.
	Config = storcfg.Config
)

const (
	// DefaultChunkSize is the default per-chunk size: the largest payload
	// that fits a single GitHub release asset.
	DefaultChunkSize = chunking.DefaultChunkSize
	// DefaultBufferSize is the default I/O buffer size used while
	// streaming uploads and downloads.
	DefaultBufferSize = chunking.DefaultBufferSize
	// MaxReleaseAssetSize is GitHub's hard ceiling on one release asset;
	// no chunk may exceed it.
	MaxReleaseAssetSize = chunking.MaxReleaseAssetSize
	// NodeKindFile marks regular-file entries in listings and stats.
	NodeKindFile = meta.NodeKindFile
	// NodeKindSymlink marks symlink entries in listings and stats.
	NodeKindSymlink = meta.NodeKindSymlink
)

var (
	// ErrFileNotFound reports a missing file path; alias of
	// storage.ErrFileNotFound.
	ErrFileNotFound = impl.ErrFileNotFound
	// ErrNotFound reports a missing path of any kind; alias of
	// fs.ErrNotFound.
	ErrNotFound = shfs.ErrNotFound
)

// NewStorHub creates a client for the given GitHub token using defaults.
func NewStorHub(token string) (*StorHub, error) {
	return impl.NewStorHub(token)
}

// NewStorHubWithConfig creates a client with explicit configuration; see
// DefaultConfig for a filled-in starting point.
func NewStorHubWithConfig(token string, cfg Config) (*StorHub, error) {
	return impl.NewStorHubWithConfig(token, cfg)
}

// NewStorHubWithContext creates a client whose lifetime is bounded by ctx:
// cancellation interrupts in-flight transfers and background maintenance.
func NewStorHubWithContext(ctx context.Context, token string, cfg Config) (*StorHub, error) {
	return impl.NewStorHubWithContext(ctx, token, cfg)
}

// DefaultFUSEOptions returns FUSEOptions with defaults applied; override
// individual fields rather than building a zero value.
func DefaultFUSEOptions() FUSEOptions {
	return implfuse.DefaultOptions()
}

// DefaultConfig returns Config with all defaults applied; use it as the base
// for NewStorHubWithConfig and NewStorHubWithContext.
func DefaultConfig() Config {
	return storcfg.Default()
}
