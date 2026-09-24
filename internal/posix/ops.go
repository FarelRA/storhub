package posix

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Backend is the storage contract the POSIX facade operates against. It is
// an alias of fs.Backend so both facades share exactly one interface
// definition; the extra methods posix itself never calls (patch, asset
// fills) are part of that single contract rather than a divergent copy.
//
// The subset this facade actually exercises is pinned below: adding a new
// backend call outside posixBackendUses must extend the subset alongside
// the caller, so the alias never silently widens what posix depends on.
type Backend = shfs.Backend

// posixBackendUses is the Backend subset the posix verbs call. It exists so
// the shared alias stays honest about which methods each facade needs.
type posixBackendUses interface {
	ValidateProjectName(project string) error
	EnsureRepoContext(ctx context.Context, project string) error
	LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error)
	UpdateRepoMetadataContext(ctx context.Context, project string, fn func(*meta.RepoMetadata) error, message string) (*meta.RepoMetadata, error)
	Logger() *slog.Logger
	Now() int64
	FileNotFound(path string) error
	DefaultFileMode(kind meta.NodeKind) uint32
	DefaultOwnerIDs() (uint32, uint32)
}

var _ posixBackendUses = Backend(nil)

// Service implements POSIX metadata verbs over a Backend.
type Service struct {
	backend Backend

	// loggers is the per-project logger cache. A sync.Map keeps the per-op
	// lookup lock-free; the atomic count bounds churn-driven growth (see
	// maxProjectLoggers). Entries are reclaimed via ForgetProject or with
	// the hub.
	loggers     sync.Map // map[string]*slog.Logger
	loggerCount atomic.Int64
}

// maxProjectLoggers backstops the loggers map against project-churn
// growth. Inserts past the cap evict one arbitrary entry; the next op
// rebuilds it.
const maxProjectLoggers = 256

// NewService builds a Service over the given backend.
func NewService(backend Backend) *Service {
	return &Service{backend: backend}
}

func (s *Service) logger(project string) *slog.Logger {
	if v, ok := s.loggers.Load(project); ok {
		return v.(*slog.Logger)
	}
	l := logging.WithComponent(s.backend.Logger(), "posix").With("project", project)
	actual, loaded := s.loggers.LoadOrStore(project, l)
	if !loaded {
		if s.loggerCount.Add(1) > maxProjectLoggers {
			s.loggers.Range(func(key, _ any) bool {
				s.loggers.Delete(key)
				s.loggerCount.Add(-1)
				return false
			})
		}
		return l
	}
	return actual.(*slog.Logger)
}

// ForgetProject drops the cached per-project logger so long-dead projects
// stop pinning loggers; the next op for the project rebuilds it lazily.
func (s *Service) ForgetProject(project string) {
	if _, loaded := s.loggers.LoadAndDelete(project); loaded {
		s.loggerCount.Add(-1)
	}
}

func (s *Service) logFinish(project, op string, started time.Time, err error, args ...any) {
	// Failures pass through unguarded so the Error "<op> failed" line is
	// reachable at the default level; only the success path stays gated
	// on Debug.
	logger := s.logger(project)
	if err == nil && !logging.Enabled(logger, slog.LevelDebug) {
		return
	}
	logging.Finish(logger, op, started, err, args...)
}

// withOp wraps one service verb with start/finish span lines. quiet,
// when non-nil, suppresses the finish line for expected hot-path outcomes
// (e.g. xattr ENODATA on GetXAttr). It replaces the per-verb Info+logFinish
// boilerplate with one Debug-level funnel.
//
// Span gating mirrors the fs Service funnel: the start line emits only
// when the project logger enables Debug, and the success finish line is
// gated the same way inside logFinish above.
// Residual cost with Debug off is the call-site args slice plus interface
// boxing, which is built before this funnel runs; see the fs withOp note
// for why the gate lives here instead of behind lazy arg builders.
func (s *Service) withOp(project, op string, args []any, fn func() error, quiet func(error) bool) (err error) {
	logger := s.logger(project)
	started := time.Now().UTC()
	if logging.Enabled(logger, slog.LevelDebug) {
		logging.Start(logger, op, args...)
	}
	defer func() {
		if quiet != nil && quiet(err) {
			return
		}
		s.logFinish(project, op, started, err, args...)
	}()
	return fn()
}

// entryForRepoPath builds the access-check view of a node from the repo
// state visible inside an update transaction.
func entryForRepoPath(repo *meta.RepoMetadata, targetPath string) (*shfs.EntryInfo, error) {
	if file := repo.FindFile(targetPath); file != nil {
		return shfs.EntryFromFile(file, targetPath, repo.FileNLink(targetPath)), nil
	}
	if dir := repo.GetDirectory(targetPath); dir != nil {
		return shfs.EntryFromDirectory(dir, targetPath, repo.DirNLink(targetPath)), nil
	}
	return nil, shfs.NotFound(targetPath)
}

// pathTxn carries the single in-transaction resolution: the concrete key,
// the traversed chain, and the live repo. Resolving once per transaction
// (instead of re-resolving per check) closes the authorize/mutate window
// without paying three resolves per chmod/chown/chtimes/xattr: the outer
// lookupPath pre-check is only a fast-fail, and everything inside the
// transaction consumes this one resolution.
type pathTxn struct {
	repo      *meta.RepoMetadata
	key       string
	traversed []string
}

// reauthorize re-runs the traversal DAC against the transaction's chain
// and the operation's ownership check against the live node. The
// pre-transaction snapshot exists for fast rejection; only this
// in-transaction check closes the window where a concurrent rename/replace
// could swap the node between authorize and mutate.
func (tx *pathTxn) reauthorize(ctx context.Context, check func(entry *shfs.EntryInfo) error) error {
	if err := shfs.CheckWalkResolved(ctx, tx.repo, tx.traversed); err != nil {
		return err
	}
	entry, err := entryForRepoPath(tx.repo, tx.key)
	if err != nil {
		return err
	}
	return check(entry)
}

// persistDir writes back a mutated directory work copy: the root lands on
// repo.Root, anything else via WriteDirDirect. File branches persist
// explicitly through UpdateFileFamily instead.
func (tx *pathTxn) persistDir(dir *meta.DirMeta) {
	if tx.key == "" {
		tx.repo.Root.Mode = dir.Mode
		tx.repo.Root.UID = dir.UID
		tx.repo.Root.GID = dir.GID
		tx.repo.Root.CreatedAt = dir.CreatedAt
		tx.repo.Root.ModifiedAt = dir.ModifiedAt
		tx.repo.Root.AccessedAt = dir.AccessedAt
		tx.repo.Root.ChangedAt = dir.ChangedAt
		tx.repo.Root.XAttrs = dir.XAttrs.Clone()
		return
	}
	tx.repo.WriteDirDirect(tx.key, *dir)
}

// loadWorkCopy resolves targetPath against the live transaction state
// (metadata verbs follow a final symlink) and returns work copies the
// callback may mutate. The traversal DAC runs in updatePathMetadataContext
// against the returned chain before the callback fires.
func (s *Service) loadWorkCopy(repo *meta.RepoMetadata, targetPath string) (tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta, err error) {
	key, traversed, err := shfs.StatResolveTracked(repo, targetPath)
	if err != nil {
		return nil, nil, nil, err
	}
	tx = &pathTxn{repo: repo, key: key, traversed: traversed}
	if key == "" {
		root := &meta.DirMeta{
			Inode:      repo.Root.Inode,
			Mode:       repo.Root.Mode,
			UID:        repo.Root.UID,
			GID:        repo.Root.GID,
			CreatedAt:  repo.Root.CreatedAt,
			ModifiedAt: repo.Root.ModifiedAt,
			AccessedAt: repo.Root.AccessedAt,
			ChangedAt:  repo.Root.ChangedAt,
			XAttrs:     repo.Root.XAttrs.Clone(),
		}
		return tx, nil, root, nil
	}
	if f := repo.FindFile(key); f != nil {
		work := f.Clone()
		return tx, &work, nil, nil
	}
	if d := repo.GetDirectory(key); d != nil {
		work := d.Clone()
		return tx, nil, &work, nil
	}
	return nil, nil, nil, s.backend.FileNotFound(key)
}

func (s *Service) updatePathMetadataContext(ctx context.Context, project, targetPath string, mutate func(*pathTxn, *meta.FileMeta, *meta.DirMeta) error) (err error) {
	return s.withOp(project, "update-path-metadata", []any{"path", targetPath}, func() error {
		_, err = s.backend.UpdateRepoMetadataContext(ctx, project, func(repo *meta.RepoMetadata) error {
			// Resolve once against the live transaction state, then run
			// the traversal DAC against the fresh chain. The callback
			// re-authorizes ownership via tx.reauthorize and persists
			// explicitly (UpdateFileFamily for files, tx.persistDir for
			// directories), so no compare-and-repersist dance is needed.
			tx, file, dir, err := s.loadWorkCopy(repo, targetPath)
			if err != nil {
				return err
			}
			if err := shfs.CheckWalkResolved(ctx, repo, tx.traversed); err != nil {
				return err
			}
			return mutate(tx, file, dir)
		}, fmt.Sprintf("storhub: update metadata for %s", targetPath))
		return err
	}, nil)
}

// ApplyMetadataPatchContext applies a metadata-only patch to targetPath.
// The has_mode/has_owner/has_times debug keys are kept verbatim: they are
// long-standing greppable keys on this verb, and renaming them to
// single-word form would churn every dashboard filtering on them.
func (s *Service) ApplyMetadataPatchContext(ctx context.Context, project, targetPath string, patch shfs.MetadataPatch) (err error) {
	err = s.withOp(project, "apply-metadata-patch", []any{"path", targetPath, "has_mode", patch.HasMode, "has_owner", patch.HasOwner, "has_times", patch.HasTimes}, func() error {
		if !patch.HasMode && !patch.HasOwner && !patch.HasTimes {
			return nil
		}
		entry, err := s.lookupEntryForAccess(ctx, project, targetPath)
		if err != nil {
			return err
		}
		if patch.HasOwner {
			if err := shfs.CanChown(ctx, entry, patch.UID, patch.GID); err != nil {
				return err
			}
		}
		return s.updatePathMetadataContext(ctx, project, targetPath, func(tx *pathTxn, file *meta.FileMeta, dir *meta.DirMeta) error {
			now := s.backend.Now()
			// Re-authorize against LIVE transaction state: the pre-transaction
			// lookup above is only a fast-fail. A concurrent rename/chmod
			// between authorize and apply must fail closed, exactly like the
			// chmod/chown/chtimes/xattr verbs.
			sanitizedMode := patch.Mode
			if err := tx.reauthorize(ctx, func(live *shfs.EntryInfo) error {
				if patch.HasOwner {
					if err := shfs.CanChown(ctx, live, patch.UID, patch.GID); err != nil {
						return err
					}
				}
				if patch.HasMode {
					liveCopy := *live
					if patch.HasOwner {
						liveCopy.UID = patch.UID
						liveCopy.GID = patch.GID
					}
					if err := shfs.CanChmod(ctx, &liveCopy); err != nil {
						return err
					}
					sanitizedMode = shfs.SanitizeChmodMode(ctx, &liveCopy, patch.Mode)
				}
				if patch.HasTimes {
					var atimePtr, mtimePtr *time.Time
					if !patch.ATime.IsZero() {
						t := patch.ATime
						atimePtr = &t
					}
					if !patch.MTime.IsZero() {
						t := patch.MTime
						mtimePtr = &t
					}
					if err := shfs.CanSetTimesValues(ctx, live, atimePtr, mtimePtr, now); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
			if file != nil {
				return UpdateFileFamily(tx.repo, file.Inode, func(current *meta.FileMeta) {
					if patch.HasOwner {
						current.UID = patch.UID
						current.GID = patch.GID
						if !shfs.IdentityFromContext(ctx).Admin {
							current.Mode &^= 0o6000
						}
					}
					if patch.HasMode {
						current.Mode = sanitizedMode
					}
					if patch.HasTimes {
						if !patch.ATime.IsZero() {
							current.AccessedAt = patch.ATime.UnixNano()
						}
						if !patch.MTime.IsZero() {
							current.ModifiedAt = patch.MTime.UnixNano()
						}
					}
					current.ChangedAt = now
				})
			}
			if patch.HasOwner {
				dir.UID = patch.UID
				dir.GID = patch.GID
				if !shfs.IdentityFromContext(ctx).Admin {
					dir.Mode &^= 0o6000
				}
			}
			if patch.HasMode {
				dir.Mode = sanitizedMode
			}
			if patch.HasTimes {
				if !patch.ATime.IsZero() {
					dir.AccessedAt = patch.ATime.UnixNano()
				}
				if !patch.MTime.IsZero() {
					dir.ModifiedAt = patch.MTime.UnixNano()
				}
			}
			dir.ChangedAt = now
			tx.persistDir(dir)
			return nil
		})
	}, nil)
	return err
}

// lookupPathResolved resolves a raw user path physically (metadata/xattr
// verbs follow a final symlink, so resolution is stat-style), enforces the
// traversal DAC of the real walk, and returns the concrete key, the
// traversed chain for *Resolved checks, and the node's snapshot.
func (s *Service) lookupPathResolved(ctx context.Context, project, targetPath string) (*meta.RepoMetadata, string, []string, *meta.FileMeta, *meta.DirMeta, error) {
	repo, _, err := s.backend.LoadRepoMetadataReadonlyContext(ctx, project)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	cleanPath, traversed, err := shfs.StatResolveTracked(repo, targetPath)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	if err := shfs.CheckWalkResolved(ctx, repo, traversed); err != nil {
		return nil, "", nil, nil, nil, err
	}
	if cleanPath == "" {
		root := meta.DirMeta{
			Inode:      repo.Root.Inode,
			Mode:       repo.Root.Mode,
			UID:        repo.Root.UID,
			GID:        repo.Root.GID,
			CreatedAt:  repo.Root.CreatedAt,
			ModifiedAt: repo.Root.ModifiedAt,
			AccessedAt: repo.Root.AccessedAt,
			ChangedAt:  repo.Root.ChangedAt,
			XAttrs:     repo.Root.XAttrs.Clone(),
		}
		return repo, cleanPath, traversed, nil, &root, nil
	}
	if file := repo.FindFile(cleanPath); file != nil {
		clone := file.Clone()
		return repo, cleanPath, traversed, &clone, nil, nil
	}
	if dir := repo.GetDirectory(cleanPath); dir != nil {
		clone := dir.Clone()
		return repo, cleanPath, traversed, nil, &clone, nil
	}
	return nil, cleanPath, traversed, nil, nil, s.backend.FileNotFound(cleanPath)
}

// lookupPath resolves a raw user path physically (metadata/xattr verbs
// follow a final symlink, so resolution is stat-style), enforces the
// traversal DAC of the real walk, and returns the concrete key plus the
// node's snapshot.
func (s *Service) lookupPath(ctx context.Context, project, targetPath string) (*meta.RepoMetadata, string, *meta.FileMeta, *meta.DirMeta, error) {
	repo, cleanPath, _, file, dir, err := s.lookupPathResolved(ctx, project, targetPath)
	if err != nil {
		return nil, cleanPath, nil, nil, err
	}
	return repo, cleanPath, file, dir, nil
}

func (s *Service) lookupEntryForAccess(ctx context.Context, project, targetPath string) (*shfs.EntryInfo, error) {
	repo, cleanPath, file, dir, err := s.lookupPath(ctx, project, targetPath)
	if err != nil {
		return nil, err
	}
	if file != nil {
		return shfs.EntryFromFile(file, cleanPath, repo.FileNLink(cleanPath)), nil
	}
	return shfs.EntryFromDirectory(dir, cleanPath, repo.DirNLink(cleanPath)), nil
}

// UpdateFileFamily applies mutate to every hardlink sibling of inode.
func UpdateFileFamily(repo *meta.RepoMetadata, inode uint64, mutate func(*meta.FileMeta)) error {
	names := repo.FindFilesByInode(inode)
	if len(names) == 0 {
		return fmt.Errorf("%w: inode family %d", shfs.ErrNotFound, inode)
	}
	updated := make(map[string]meta.FileMeta, len(names))
	for _, name := range names {
		file := repo.FindFile(name)
		if file == nil {
			continue
		}
		clone := file.Clone()
		mutate(&clone)
		updated[name] = clone
	}
	// WriteFileDirect: routing through UpsertFile would send family members
	// down the new-node path (RemoveFile erased the "existing" side),
	// letting creation defaults overwrite preserved values - including
	// authoritative epoch zeros.
	for _, name := range names {
		repo.RemoveFile(name)
	}
	// Deterministic write order: ranging over the map above would commit
	// siblings in a random sequence per call (harmless today since index
	// lists re-sort and size deltas commute, but needlessly
	// non-reproducible across runs and intent logs).
	for _, name := range names {
		clone, ok := updated[name]
		if !ok {
			continue
		}
		repo.WriteFileDirect(name, clone)
	}
	return nil
}

// TouchInodeFamilyChangedAt refreshes ctime across one inode family.
func TouchInodeFamilyChangedAt(repo *meta.RepoMetadata, inode uint64, now int64) error {
	return UpdateFileFamily(repo, inode, func(current *meta.FileMeta) {
		current.ChangedAt = now
	})
}
