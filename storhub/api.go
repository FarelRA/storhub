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
	StorHub           = impl.StorHub
	FUSEOptions       = implfuse.Options
	StorHubFS         = implfuse.Filesystem
	ChunkInfo         = meta.ChunkInfo
	FileMetadata      = meta.FileMeta
	ReleaseMetadata   = meta.ReleaseRef
	RepoMetadata      = meta.RepoMetadata
	MetadataRevision  = meta.MetadataRevision
	DirectoryMetadata = meta.DirMeta
	RootMetadata      = meta.DirMeta
	EntryInfo         = shfs.EntryInfo
	DirEntry          = shfs.DirEntry
	FSStats           = shfs.FSStats
	NodeKind          = meta.NodeKind
	PurgeResult       = impl.PurgeResult
	APIError          = ghapi.APIError
	Config            = storcfg.Config
)

const (
	DefaultChunkSize              = chunking.DefaultChunkSize
	DefaultBufferSize             = chunking.DefaultBufferSize
	DefaultMaxConcurrentTransfers = chunking.DefaultMaxConcurrentTransfers
	MaxReleaseAssetSize           = chunking.MaxReleaseAssetSize
	NodeKindFile                  = meta.NodeKindFile
	NodeKindSymlink               = meta.NodeKindSymlink
)

var (
	ErrFileNotFound = impl.ErrFileNotFound
	ErrNotFound     = shfs.ErrNotFound
)

func NewStorHub(token string) (*StorHub, error) {
	return impl.NewStorHub(token)
}

func NewStorHubWithConfig(token string, cfg Config) (*StorHub, error) {
	return impl.NewStorHubWithConfig(token, cfg)
}

func NewStorHubWithContext(ctx context.Context, token string, cfg Config) (*StorHub, error) {
	return impl.NewStorHubWithContext(ctx, token, cfg)
}

func DefaultFUSEOptions() FUSEOptions {
	return implfuse.DefaultOptions()
}

func DefaultConfig() Config {
	return storcfg.Default()
}
