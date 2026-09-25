// Package storhub is the public API facade for StorHub: a content-addressed
// file store backed by GitHub releases and metadata commits, with FUSE
// mount, REST server, and POSIX-style library access.
//
// The types below are aliases of internal implementations; they exist so
// embedders depend only on this package. Start with NewStorHub (or its
// Config/Context variants), then use DefaultFUSEOptions with StorHub.NewFUSE
// for mounting, or DefaultRESTOptions with NewRESTHandler for HTTP serving,
// or use the fs-style operations directly.
package storhub

import (
	"context"
	"net/http"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	implfuse "github.com/FarelRA/storhub/internal/fusefs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
	implrest "github.com/FarelRA/storhub/internal/rest"
	impl "github.com/FarelRA/storhub/internal/storage"
)

type (
	// StorHub is the storage client: project/file/chunk operations over a
	// GitHub backend, with metadata versioning, rollback, and purge tools.
	StorHub = impl.StorHub
	// FUSEOptions configures a FUSE mount (cache location, atime policy,
	// overlay buffer sizing). See internal/fusefs.Options.
	FUSEOptions = implfuse.Options
	// FS is the mounted filesystem returned by the FUSE facade;
	// callers Unmount and Wait on it.
	FS = implfuse.Filesystem
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
	// DirectoryMetadata is a directory entry (including the root when
	// addressed via the root path).
	DirectoryMetadata = meta.DirMeta
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
	// PruneResult reports what a granular prune reclaimed.
	PruneResult = impl.PruneResult
	// ChunkGCResult reports what a chunk-GC scan found and a compaction
	// collected (or would collect under dryrun).
	ChunkGCResult = impl.ChunkGCResult
	// PressureSnapshot is the operator-visible pressure ledger: monotonic
	// totals plus per-project consecutive-failure streaks.
	PressureSnapshot = impl.PressureSnapshot
	// DegradedProjectError is the fail-fast refusal a degraded project
	// answers to new mutations. Match with errors.As.
	DegradedProjectError = impl.DegradedProjectError
	// PruneScope selects what a prune run reclaims (objects|assets|history|chunks|all).
	PruneScope = impl.PruneScope
	// APIError is an error returned by the GitHub API layer, carrying the
	// HTTP status and parsed message.
	APIError = ghapi.APIError
	// Config tunes a StorHub client: API endpoint, transport, transfer
	// sizing, retry policy, logging, git cache, and test clocks.
	Config = storcfg.Config
	// RESTOptions configures the REST server: listen-time behavior,
	// body/patch limits, share-token policy, and authentication. See
	// internal/rest.Options for the field-level contract.
	RESTOptions = implrest.Options
	// RESTAuthOptions describes the user database backing Bearer JWT auth:
	// users, their POSIX identities, and token lifetime settings.
	RESTAuthOptions = implrest.AuthOptions
	// RESTUser is a single authenticated principal with its POSIX identity
	// (UID, primary GID, supplementary groups) used for authorization.
	RESTUser = implrest.User
	// OpenMode is a bitmask describing how a session may be used (read,
	// write, create, truncate, append, exclusive). See internal/storage.
	OpenMode = impl.OpenMode
	// SessionOption decorates one OpenSession call (e.g. WithSessionTTL).
	SessionOption = impl.SessionOption
	// SessionHubOption tunes one hub's session policy (caps and TTLs).
	SessionHubOption = impl.SessionHubOption
	// SessionStat describes a live session handle: project, path, size,
	// dirty state, mode, and staleness against the committed revision.
	SessionStat = impl.SessionStat
	// StaleSessionError is the typed stale-handle error answered for
	// expired or unknown ids. Match with errors.As.
	StaleSessionError = impl.StaleSessionError
	// PruneRequest is the flag-free prune invocation: scope, keep
	// threshold, and dry-run flag.
	PruneRequest = impl.PruneRequest
	// PruneConflictError is the loud typed refusal a project answers
	// while a prune run holds its write fence. Match with errors.As.
	PruneConflictError = impl.PruneConflictError
	// ChunkGCRefusedError is the loud typed refusal a chunk-GC compaction
	// answers under doubt (live session, bad project). Match with
	// errors.As.
	ChunkGCRefusedError = impl.ChunkGCRefusedError
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
	// PruneObjects reclaims dangling objects during a prune run.
	// Aliased so callers (e.g. the CLI's argument validation) share one
	// vocabulary with storage instead of duplicating string literals.
	PruneObjects = impl.PruneObjects
	// PruneAssets reclaims release assets during a prune run.
	PruneAssets = impl.PruneAssets
	// PruneHistory reclaims superseded history during a prune run.
	PruneHistory = impl.PruneHistory
	// PruneChunks reclaims unreferenced chunks during a prune run.
	PruneChunks = impl.PruneChunks
	// PruneAll reclaims everything a prune run can reclaim.
	PruneAll = impl.PruneAll
	// SessionReadOnly opens a session for reads only.
	SessionReadOnly = impl.SessionReadOnly
	// SessionWriteOnly opens a session for writes only.
	SessionWriteOnly = impl.SessionWriteOnly
	// SessionReadWrite opens a session for reads and writes.
	SessionReadWrite = impl.SessionReadWrite
	// SessionCreate creates the file if missing (staged until commit).
	SessionCreate = impl.SessionCreate
	// SessionTruncate truncates an existing file to zero at open (staged
	// until commit). Requires a write bit.
	SessionTruncate = impl.SessionTruncate
	// SessionAppend forces every session write to the current end (staged
	// until commit). Requires a write bit.
	SessionAppend = impl.SessionAppend
	// SessionExclusive fails the open when the file already exists.
	// Requires SessionCreate.
	SessionExclusive = impl.SessionExclusive
)

var (
	// ErrNotFound reports a missing path of any kind (files, directories,
	// projects); every not-found failure in the library wraps it.
	ErrNotFound = shfs.ErrNotFound
	// ErrStaleSession matches any expired or unknown session handle id.
	// Prefer errors.As with *StaleSessionError when the reason matters.
	ErrStaleSession = impl.ErrStaleSession
	// ErrSessionProjectBusy reports the per-project session handle cap.
	ErrSessionProjectBusy = impl.ErrSessionProjectBusy
	// ErrSessionUserBusy reports the per-user session handle cap.
	ErrSessionUserBusy = impl.ErrSessionUserBusy
	// ErrSessionOwnerMismatch reports a session handle driven by a UID
	// other than the opener (admins bypass).
	ErrSessionOwnerMismatch = impl.ErrSessionOwnerMismatch
	// ErrSessionUnlinked reports syncing (or committing) a scratch
	// session handle that was never linked to a path.
	ErrSessionUnlinked = impl.ErrSessionUnlinked
	// ErrSessionLinked reports linking an already-pending name, or
	// linking a handle opened with a path.
	ErrSessionLinked = impl.ErrSessionLinked
	// ErrSessionPathGone reports a commit or sync for a handle whose
	// pinned inode lost its last name after open.
	ErrSessionPathGone = impl.ErrSessionPathGone
)

// MutateOption customizes a single storage mutation.
type MutateOption = shfs.MutateOption

// XAttrMode qualifies a SetXAttr request (create-only / replace-only),
// enforced atomically inside the storage transaction.
type XAttrMode = shfs.XAttrMode

const (
	// XAttrCreate fails SetXAttr with EEXIST when the name is present.
	XAttrCreate = shfs.XAttrCreate
	// XAttrReplace fails SetXAttr with ENODATA when the name is absent.
	XAttrReplace = shfs.XAttrReplace
)

// WithExpectedRevision upgrades a mutation into true compare-and-swap:
// storage re-verifies the project's remote metadata revision immediately
// before applying, failing with ErrPreconditionFailed when it moved.
func WithExpectedRevision(rev string) MutateOption {
	return shfs.WithExpectedRevision(rev)
}

// ParseOpenMode maps fopen-style strings ("r", "w", "a", "r+", "w+",
// "a+", optional "x" for exclusive) to a session OpenMode.
func ParseOpenMode(s string) (OpenMode, error) {
	return impl.ParseOpenMode(s)
}

// WithSessionTTL requests an idle TTL for a new session handle.
// Non-positive means the hub default; anything above the hub max cap is
// clamped to it.
func WithSessionTTL(d time.Duration) SessionOption {
	return impl.WithSessionTTL(d)
}

// RequestedTTL folds SessionOptions and reports the requested idle TTL:
// <=0 means "hub default".
func RequestedTTL(opts []SessionOption) time.Duration {
	return impl.RequestedTTL(opts)
}

// WithSessionMaxPerProject overrides the per-project session handle cap
// for one hub. Non-positive values are ignored.
func WithSessionMaxPerProject(n int) SessionHubOption {
	return impl.WithSessionMaxPerProject(n)
}

// WithSessionMaxPerUser overrides the per-user session handle cap for one
// hub. Non-positive values are ignored.
func WithSessionMaxPerUser(n int) SessionHubOption {
	return impl.WithSessionMaxPerUser(n)
}

// WithSessionDefaultTTL overrides the default session idle TTL for one
// hub. Non-positive values are ignored.
func WithSessionDefaultTTL(d time.Duration) SessionHubOption {
	return impl.WithSessionDefaultTTL(d)
}

// WithSessionMaxTTL overrides the max session TTL cap for one hub.
// Non-positive values are ignored.
func WithSessionMaxTTL(d time.Duration) SessionHubOption {
	return impl.WithSessionMaxTTL(d)
}

// ErrPreconditionFailed reports a failed compare-and-swap: the remote
// revision moved between observation and application.
var ErrPreconditionFailed = shfs.ErrPreconditionFailed

// NewStorHubWithContext creates a client whose lifetime is bounded by ctx:
// cancellation interrupts in-flight transfers and background maintenance.
// It is the only constructor.
func NewStorHubWithContext(ctx context.Context, token string, cfg Config) (*StorHub, error) {
	return impl.NewStorHubWithContext(ctx, token, cfg)
}

// DefaultFUSEOptions returns FUSEOptions with defaults applied; override
// individual fields rather than building a zero value.
func DefaultFUSEOptions() FUSEOptions {
	return implfuse.DefaultOptions()
}

// DefaultRESTOptions returns RESTOptions with all defaults applied; use it
// as the base and override individual fields rather than building a zero
// RESTOptions.
func DefaultRESTOptions() RESTOptions {
	return implrest.DefaultOptions()
}

// HashRESTPassword bcrypt-hashes a plaintext password for use in
// RESTAuthOptions.Users.PasswordHash.
func HashRESTPassword(password string) (string, error) {
	return implrest.HashPassword(password)
}

// NewRESTHandler builds the REST HTTP handler serving hub's storage projects
// according to opts. Authentication is required unless opts explicitly opts
// into anonymous access; the returned handler is safe for concurrent use.
func NewRESTHandler(hub *StorHub, opts RESTOptions) (http.Handler, error) {
	return implrest.NewHandler(hub, opts)
}

// DefaultConfig returns Config with all defaults applied; use it as the base
// for NewStorHubWithConfig and NewStorHubWithContext.
func DefaultConfig() Config {
	return storcfg.Default()
}
