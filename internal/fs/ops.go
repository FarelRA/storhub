package fs

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"sync"
	"sync/atomic"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Backend is the storage surface the fs Service verbs need.
type Backend interface {
	ValidateProjectName(project string) error
	EnsureRepoContext(ctx context.Context, project string) error
	LoadRepoMetadataContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error)
	GetOrCreateUploadReleaseContext(ctx context.Context, project string, repoMeta *meta.RepoMetadata, requiredSize int) (string, string, error)
	PatchFileWithMetadataContext(ctx context.Context, project, cleanName string, repoMeta *meta.RepoMetadata, fileMeta *meta.FileMeta, offset, deleteSize int64, edit []byte) (*meta.FileMeta, error)
	FillAssetRangeContext(ctx context.Context, project string, segment meta.ChunkInfo, dst []byte) error
	QueueAtimeUpdateContext(ctx context.Context, project, targetPath string, isDir bool, now int64)
	Logger() *slog.Logger
	Now() int64
	AtimePolicy() storcfg.AtimePolicy
	FileNotFound(path string) error
	DefaultFileMode(kind meta.NodeKind) uint32
	DefaultOwnerIDs() (uint32, uint32)
}

// Service implements the file verbs over a Backend with per-project state.
type Service struct {
	backend Backend

	// states is the per-project derived state (logger, mutation counter,
	// StatFS cache). A sync.Map keeps the per-op lookup lock-free: the hub
	// owns one long-lived Service, so every verb used to serialize on the
	// old stateMu twice per call. The atomic count bounds churn-driven
	// growth (see maxProjectStates); entries are otherwise reclaimed via
	// ForgetProject or with the hub.
	states     sync.Map // map[string]*projectState
	stateCount atomic.Int64
}

// maxProjectStates backstops the states map against project-churn growth:
// each entry is only a logger plus a small StatFS aggregate, but an
// unbounded map would pin one logger per project ever addressed. Inserts
// past the cap evict one arbitrary entry; the next op rebuilds it.
const maxProjectStates = 256

// NewService builds a Service over the given backend.
func NewService(backend Backend) *Service {
	return &Service{backend: backend}
}

// projectState carries the per-project derived state reused across FS
// operations: the lazily-built logger, a mutation counter that invalidates
// the StatFS cache, and the cached StatFS aggregate itself.
type projectState struct {
	project   string
	mu        sync.Mutex
	logger    *slog.Logger
	mutations atomic.Uint64
	statfs    *FSStats
	statfsSha string
	statfsGen uint64
	statfsAt  time.Time
}

// statfsCacheTTL bounds how long cached StatFS aggregates may serve even
// when neither the commit SHA nor the local mutation counter moved: verbs
// that mutate metadata outside this Service (xattr/symlink/chmod on the
// hub) bump neither key, so staleness is capped by time as well.
const statfsCacheTTL = 40 * storcfg.TickUnit

func (s *Service) state(project string) *projectState {
	if v, ok := s.states.Load(project); ok {
		return v.(*projectState)
	}
	st := &projectState{project: project}
	actual, loaded := s.states.LoadOrStore(project, st)
	if !loaded {
		if s.stateCount.Add(1) > maxProjectStates {
			s.evictOneState()
		}
		return st
	}
	return actual.(*projectState)
}

// evictOneState deletes an arbitrary entry to hold the cap. Best-effort
// backstop only: concurrent inserts may each evict once, so the count is
// approximate under contention.
func (s *Service) evictOneState() {
	s.states.Range(func(key, _ any) bool {
		s.states.Delete(key)
		s.stateCount.Add(-1)
		return false
	})
}

// ForgetProject drops the cached per-project state (logger, mutation
// counter, StatFS aggregate). Call it when a project is evicted or
// deleted so long-dead projects stop pinning loggers; the next op for the
// project rebuilds its state lazily.
func (s *Service) ForgetProject(project string) {
	if _, loaded := s.states.LoadAndDelete(project); loaded {
		s.stateCount.Add(-1)
	}
}

// log returns the per-project fs logger, built once per project.
func (p *projectState) log(backendLogger *slog.Logger) *slog.Logger {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.logger == nil {
		p.logger = logging.WithComponent(backendLogger, "fs").With("project", p.project)
	}
	return p.logger
}

// bump records that a metadata-mutating operation succeeded, so any cached
// StatFS aggregate for the project is stale from now on.
func (p *projectState) bump() {
	p.mutations.Add(1)
}

// withOp wraps one service verb with start/finish span lines and StatFS
// invalidation. A mutating verb passes mutating=true so a successful call
// bumps the mutation counter; read-only verbs pass false. It replaces the
// per-verb Debug+logFinish+bumpMutations boilerplate (13 copies) with one
// funnel.
//
// Span gating: the start line emits only when the project logger enables
// Debug, checked here before the message concat and record build, and the
// success finish line is gated the same way inside logFinishState.
// Failures always reach logging.Finish so the Error "<op> failed" line
// stays visible at the default level. Residual cost with Debug off is the
// call-site args slice plus interface boxing: the verb call sites pass
// []any literals, which are built before this funnel runs. Threading lazy
// arg builders through every call site would trade one closure allocation
// for that slice on each op, so the gate lives here and removes the concat,
// the slog record, and the finish-path slice growth instead, with no change
// to any call shape.
func (s *Service) withOp(project, op string, mutating bool, args []any, fn func() error) (err error) {
	state := s.state(project)
	started := time.Now().UTC()
	logger := state.log(s.backend.Logger())
	if logging.Enabled(logger, slog.LevelDebug) {
		logging.Start(logger, op, args...)
	}
	defer func() {
		if mutating && err == nil {
			state.bump()
		}
		s.logFinishState(state, op, started, err, args...)
	}()
	return fn()
}

func (s *Service) logFinishState(state *projectState, op string, started time.Time, err error, args ...any) {
	// Debug, not Info: per-op completion lines are a steady-state fire
	// hose on a mount (every stat/read/write), and the default level no
	// longer wants them. Failures pass through unguarded so the Error
	// line is reachable at the default level; only the success path stays
	// gated on Debug.
	logger := state.log(s.backend.Logger())
	if err == nil && !logging.Enabled(logger, slog.LevelDebug) {
		return
	}
	logging.Finish(logger, op, started, err, args...)
}

// RequireParentDirectory fails when the parent of filePath is missing.
func RequireParentDirectory(repo *meta.RepoMetadata, filePath string) error {
	if parent := ParentPath(filePath); parent != "" && !repo.HasDirectory(parent) {
		return fmt.Errorf("%w: parent directory does not exist: %s", ErrNotFound, parent)
	}
	return nil
}

// EntryFromFile builds the stat view of one file node. It is the single
// constructor behind the FUSE entry fills: every
// renderer calls this instead of re-spelling the field mapping.
func EntryFromFile(file *meta.FileMeta, path string, nlink int) *EntryInfo {
	kind := meta.NodeKindFile
	if file.Symlink != "" {
		kind = meta.NodeKindSymlink
	}
	return &EntryInfo{
		Path:          path,
		Kind:          kind,
		IsSymlink:     file.Symlink != "",
		Size:          file.Size,
		Inode:         file.Inode,
		Mode:          file.Mode,
		UID:           file.UID,
		GID:           file.GID,
		NLink:         uint32(nlink),
		CreatedAt:     file.UploadedAt,
		ModifiedAt:    file.ModifiedAt,
		AccessedAt:    file.AccessedAt,
		ChangedAt:     file.ChangedAt,
		SymlinkTarget: file.Symlink,
	}
}

// EntryFromDirectory builds the stat view of one directory node.
func EntryFromDirectory(dir *meta.DirMeta, path string, nlink int) *EntryInfo {
	return &EntryInfo{
		Path:       path,
		IsDir:      true,
		Inode:      dir.Inode,
		Mode:       dir.Mode,
		UID:        dir.UID,
		GID:        dir.GID,
		NLink:      uint32(nlink),
		CreatedAt:  dir.CreatedAt,
		ModifiedAt: dir.ModifiedAt,
		AccessedAt: dir.AccessedAt,
		ChangedAt:  dir.ChangedAt,
	}
}

// EntryFromDirEntry lifts a listing row into the full attribute view the
// kernel's entry cache wants. The listing already carries mode, owner,
// link count and timestamps, so no re-stat is needed.
func EntryFromDirEntry(e DirEntry, childPath string) *EntryInfo {
	return &EntryInfo{
		Path:       childPath,
		Kind:       e.Kind,
		IsDir:      e.IsDir,
		IsSymlink:  e.IsSymlink,
		Size:       e.Size,
		Inode:      e.Inode,
		Mode:       e.Mode,
		UID:        e.UID,
		GID:        e.GID,
		NLink:      e.NLink,
		CreatedAt:  e.CreatedAt,
		ModifiedAt: e.ModifiedAt,
		AccessedAt: e.AccessedAt,
		ChangedAt:  e.ChangedAt,
	}
}

// DirEntryFromDirectory builds a listing row for one directory node. It
// takes the entry by value (listing rows are copied into slices anyway)
// while EntryFromDirectory takes a pointer (it renders shared stored state
// without copying): the convention is value for row builders, pointer for
// view builders.
func DirEntryFromDirectory(dir meta.DirMeta, dirPath string, nlink int) DirEntry {
	return DirEntry{Name: path.Base(dirPath), Path: dirPath, IsDir: true, Inode: dir.Inode, Mode: dir.Mode, NLink: uint32(nlink), UID: dir.UID, GID: dir.GID, CreatedAt: dir.CreatedAt, ModifiedAt: dir.ModifiedAt, AccessedAt: dir.AccessedAt, ChangedAt: dir.ChangedAt}
}

// DirEntryFromFile builds a listing row for one file node.
func DirEntryFromFile(file meta.FileMeta, filePath string, nlink int) DirEntry {
	entry := DirEntry{Name: path.Base(filePath), Path: filePath, IsSymlink: file.Symlink != "", Size: file.Size, Inode: file.Inode, Mode: file.Mode, NLink: uint32(nlink), UID: file.UID, GID: file.GID, CreatedAt: file.UploadedAt, ModifiedAt: file.ModifiedAt, AccessedAt: file.AccessedAt, ChangedAt: file.ChangedAt}
	if file.Symlink != "" {
		entry.Kind = meta.NodeKindSymlink
	} else {
		entry.Kind = meta.NodeKindFile
	}
	return entry
}

// CountUniqueInodes counts distinct inodes across root, dirs, and files.
func CountUniqueInodes(repo *meta.RepoMetadata) int {
	seen := map[uint64]struct{}{repo.Root.Inode: {}}
	for _, dir := range repo.Dirs() {
		seen[dir.Inode] = struct{}{}
	}
	// Files() is the live map. AllFiles sorts a throwaway copy of every
	// name just to count, turning each StatFS miss into O(F log F).
	for _, file := range repo.Files() {
		seen[file.Inode] = struct{}{}
	}
	return len(seen)
}
