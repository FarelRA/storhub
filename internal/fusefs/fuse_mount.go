package fusefs

import (
	"context"
	"fmt"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"log"
	"log/slog"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Hub is the storage surface a Filesystem mounts.
type Hub interface {
	StatPathContext(context.Context, string, string) (*shfs.EntryInfo, error)
	ReadDirContext(context.Context, string, string) ([]shfs.DirEntry, error)
	StatFSContext(context.Context, string) (*shfs.FSStats, error)
	CreateFileContext(context.Context, string, string) (*metadata.FileMeta, error)
	MkdirContext(context.Context, string, string) error
	UnlinkContext(context.Context, string, string) error
	RmdirContext(context.Context, string, string, ...shfs.MutateOption) error
	TruncateFileContext(context.Context, string, string, int64, ...shfs.MutateOption) (*metadata.FileMeta, error)
	ChmodContext(context.Context, string, string, uint32) error
	ChownContext(context.Context, string, string, uint32, uint32) error
	ChtimesContext(context.Context, string, string, int64, int64) error
	ChtimesExplicitContext(context.Context, string, string, *time.Time, *time.Time) error
	SymlinkContext(context.Context, string, string, string) (*metadata.FileMeta, error)
	ReadlinkContext(context.Context, string, string) (string, error)
	LinkContext(context.Context, string, string, string) (*metadata.FileMeta, error)
	GetXAttrContext(context.Context, string, string, string) ([]byte, error)
	SetXAttrContext(context.Context, string, string, string, []byte, ...shfs.XAttrMode) error
	ListXAttrContext(context.Context, string, string) ([]string, error)
	RemoveXAttrContext(context.Context, string, string, string) error
	ApplyMetadataPatchContext(context.Context, string, string, shfs.MetadataPatch) error
	DownloadFileContext(context.Context, string, string, string) error
	ReadFileAtContext(context.Context, string, string, int64, int64) ([]byte, error)
	PatchFileContext(context.Context, string, string, int64, int64, []byte, ...shfs.MutateOption) (*metadata.FileMeta, error)
	PatchFileRangesContext(context.Context, string, string, []shfs.RangeEdit) (*metadata.FileMeta, error)
	ReplaceFileContext(context.Context, string, string, string, ...shfs.MutateOption) (*metadata.FileMeta, error)
	LoadRepoMetadataReadonlyContext(context.Context, string) (*metadata.RepoMetadata, string, error)
	ReadPinnedFileContext(context.Context, string, *metadata.FileMeta, map[int64]metadata.ChunkInfo, int64, int64) ([]byte, error)
	UpdateRepoMetadataContext(context.Context, string, func(*metadata.RepoMetadata) error, string) (*metadata.RepoMetadata, error)
	RewriteFileRangesWithMetadataContext(context.Context, string, string, string, *metadata.RepoMetadata, *metadata.FileMeta, int64, []ByteRange) (*metadata.FileMeta, error)
	RenameContext(context.Context, string, string, string, ...shfs.MutateOption) error
	CloneRange(context.Context, string, string, int64, string, int64, int64, ...shfs.MutateOption) (*metadata.FileMeta, error)
	DrainProjectContext(context.Context, string) error
	Now() int64
	ChunkSize() int64
}

// readMountLockPid returns the pid recorded by the current or last holder,
// or zero when unreadable. Purely cosmetic: liveness is decided by the
// kernel lock, never by this value.
func readMountLockPid(lockPath string) int {
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return pid
}

// claimMountLock takes exclusive ownership of cacheDir for this process
// with an flock(2) LOCK_EX|LOCK_NB on the lockfile. A held lock refuses
// the claim - the holder may have commit snapshots in flight that our
// startup sweep would delete. flock is chosen over a pidfile because it
// closes every hole a pid heuristic leaves: a crashed holder's lock
// vanishes with the process (stale takeover is automatic), and two
// mounts inside one process conflict just like two processes, since each
// open file description owns its lock independently. The file itself is
// never unlinked: unlock-then-unlink lets a contender lock an orphaned
// inode while another recreates the path, producing two simultaneous
// holders.
func claimMountLock(lockPath string) (*os.File, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open mount lock %s: %w", lockPath, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		dir := path.Dir(lockPath)
		if holder := readMountLockPid(lockPath); holder > 0 {
			return nil, fmt.Errorf("cache dir %s is already locked by process %d", dir, holder)
		}
		return nil, fmt.Errorf("cache dir %s is already locked", dir)
	}
	// Best-effort diagnostics; the lock, not this text, is authoritative.
	if truncErr := f.Truncate(0); truncErr == nil {
		_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0)
	}
	return f, nil
}

// New builds an unmounted Filesystem for the project over hub.
func New(hub Hub, project string, opts Options) (*Filesystem, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	defaults := DefaultOptions()
	// Read-only mounts get long kernel timeouts (see readOnlyEntryTimeout);
	// an explicit Options timeout always wins.
	if opts.isReadOnly() {
		defaults.EntryTimeout = readOnlyEntryTimeout
		defaults.AttrTimeout = readOnlyAttrTimeout
	}
	if opts.EntryTimeout <= 0 {
		opts.EntryTimeout = defaults.EntryTimeout
	}
	if opts.AttrTimeout <= 0 {
		opts.AttrTimeout = defaults.AttrTimeout
	}
	if opts.NegativeTimeout <= 0 {
		opts.NegativeTimeout = defaults.NegativeTimeout
	}
	if opts.OverlayBufferSize <= 0 {
		opts.OverlayBufferSize = defaults.OverlayBufferSize
	}
	// An unset umask resolves to the protocol default; an explicit one
	// (including zero) is sanitized to permission bits and honored.
	if !opts.UmaskSet && opts.Umask == 0 {
		opts.Umask, opts.UmaskSet = defaultCallerUmask, true
	}
	opts.Umask &= 0o777
	if len(opts.ExtraMountOpts) == 0 {
		opts.ExtraMountOpts = append([]string(nil), defaults.ExtraMountOpts...)
	}
	if opts.Logger == nil {
		// Warn by default: a mount is a chatty process, and a debug-level
		// default burns CPU formatting per-operation lines nobody reads.
		// opts.Debug (--debug) is the explicit opt-in for the full trace.
		level := logging.LevelWarn
		if opts.Debug {
			level = logging.LevelDebug
		}
		opts.Logger = logging.WithComponent(logging.NewLogger(logging.Options{Level: level, Format: logging.FormatPretty, Color: true, Output: os.Stderr}), "fuse")
	}
	cacheDir, err := resolveCacheDir(opts.CacheDir, project)
	if err != nil {
		return nil, err
	}
	// Claim the directory before touching its contents. The sweep below
	// deletes leftover temps unconditionally, so without the lock a second
	// live mount sharing this cacheDir would silently destroy the first
	// one's in-flight commit snapshots; the lock turns that into an
	// explicit error instead.
	lockFile, err := claimMountLock(path.Join(cacheDir, mountLockFileName))
	if err != nil {
		return nil, err
	}
	// Construction past this point cannot fail, so no failed New leaves a
	// claim behind; Close releases it.
	sweepCacheDir(cacheDir, opts.Logger)
	// Startup replay: surface whatever earlier crashes quarantined so
	// operators (and RecoveryInventory callers) see it immediately.
	logRecoveryInventory(path.Join(cacheDir, "recovery"), opts.Logger)
	// Auto-redrive eligible overlays: full-image temps whose target is
	// provably untouched are re-uploaded (fail-closed per entry, never
	// fatal to the mount). See redriveRecoveryInventory. The timeout
	// bounds the whole pass (each hub call carries its own 5-minute
	// request timeout, but a large backlog must not stall the mount
	// indefinitely); expiry keeps every remaining entry quarantined for
	// the next mount.
	redriveCtx, cancel := context.WithTimeout(context.Background(), 120*storcfg.PatienceUnit)
	defer cancel()
	redriveRecoveryInventory(redriveCtx, hub, project, path.Join(cacheDir, "recovery"), opts.Logger)
	return newBareFilesystem(hub, project, opts, cacheDir, lockFile), nil
}

// resolveCacheDir applies the default cache directory and creates it with
// the overlay-appropriate mode.
func resolveCacheDir(cacheDir, project string) (string, error) {
	if strings.TrimSpace(cacheDir) == "" {
		cacheDir = path.Join(storcfg.CacheBase(), "fuse", project)
	}
	// 0700: the overlay temps are 0600, but their names and timestamps in
	// a world-readable directory would still leak activity; match the
	// recovery directory's mode.
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return "", fmt.Errorf("create fuse cache dir: %w", err)
	}
	return cacheDir, nil
}

// sweepCacheDir quarantines leftover overlay temps from a crashed previous
// mount into recovery/ instead of deleting them: handle* and inode*
// flat files may hold the only copy of SIGKILL-before-commit data.
// Nothing can reference them (no file is open yet), but the bytes survive
// for manual recovery. The recovery/ directory itself is preserved.
func sweepCacheDir(cacheDir string, logger *slog.Logger) {
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		// A sweep that could not run must be loud: leftover dirty temps
		// from a crashed mount stay unreported otherwise.
		logging.Error(logger, "startup sweep skipped; cache dir unreadable", "dir", cacheDir, "err", err)
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "recovery" || (!strings.HasPrefix(name, "inode") && !strings.HasPrefix(name, "handle")) {
			continue
		}
		quarantinePath(path.Join(cacheDir, name), logger)
	}
}

// newBareFilesystem builds the unmounted Filesystem value: maps, root
// node, and lock condition. New orchestrates validation, cache claiming,
// and recovery replay around it.
func newBareFilesystem(hub Hub, project string, opts Options, cacheDir string, lockFile *os.File) *Filesystem {
	fsys := &Filesystem{
		hub:          hub,
		project:      project,
		opts:         opts,
		nodes:        make(map[uint64]*storhubNode),
		inodePaths:   map[uint64]map[string]struct{}{1: {"": {}}},
		pathToInode:  map[string]uint64{"": 1},
		lockTable:    make(map[uint64][]lockRecord),
		writeStates:  make(map[uint64]*inodeWriteState),
		handles:      make(map[uint64]*storhubHandle),
		cacheDir:     cacheDir,
		lockFile:     lockFile,
		notifyQueued: make(map[notifyKey]struct{}),
		notifySlots:  make(chan struct{}, maxConcurrentNotifies),
		pinned:       make(map[pinnedKey]*pinnedContent),
		attaching:    make(map[*storhubNode]struct{}),
	}
	fsys.root = &storhubNode{fs: fsys, inode: 1, isDir: true}
	fsys.nodes[1] = fsys.root
	fsys.lockCond = sync.NewCond(&fsys.mu)
	fsys.stopInvalPoll = fsys.startFanoutPush()
	return fsys
}

// Mount mounts the filesystem at mountPoint and serves it.
func (s *Filesystem) Mount(mountPoint string) error {
	s.debugOp("mount start", "project", s.project, "target", mountPoint, "allow_other", s.opts.AllowOther, "cache_dir", s.cacheDir)
	options := &gofusefs.Options{
		EntryTimeout:    durationPtr(s.opts.EntryTimeout),
		AttrTimeout:     durationPtr(s.opts.AttrTimeout),
		NegativeTimeout: durationPtr(s.opts.NegativeTimeout),
		NullPermissions: true,
		RootStableAttr:  &gofusefs.StableAttr{Ino: 1, Gen: 1},
		Logger:          log.New(os.Stderr, "storhub/go-fuse: ", log.LstdFlags|log.Lmicroseconds),
	}
	options.Debug = s.opts.Debug
	options.AllowOther = s.opts.AllowOther
	options.MaxBackground = mountMaxBackground
	options.MaxWrite = mountMaxIOSize
	options.MaxReadAhead = readAheadBytes(s.hub.ChunkSize())
	options.Options = append([]string(nil), s.opts.ExtraMountOpts...)
	options.ExplicitDataCacheControl = true
	options.ExtraCapabilities = fuse.CAP_WRITEBACK_CACHE
	server, err := gofusefs.Mount(mountPoint, s.root, options)
	if err != nil {
		s.debugOp("mount failed", "project", s.project, "target", mountPoint, "err", err)
		return err
	}
	// Publish under mu like every other server access (connected(),
	// Wait, Unmount): concurrent notify readers must observe this write
	// through the mutex, never as a data race.
	s.mu.Lock()
	s.server = server
	s.unmounted = false
	s.mu.Unlock()
	s.debugOp("mount ready", "project", s.project, "target", mountPoint)
	return nil
}

// Wait blocks until the filesystem is unmounted.
func (s *Filesystem) Wait() {
	// Snapshot under the lock, then block outside it: server.Wait only
	// returns after Unmount completes, and Unmount needs this mutex.
	s.mu.RLock()
	server := s.server
	s.mu.RUnlock()
	if server != nil {
		server.Wait()
	}
}

// Unmount detaches the filesystem from its mount point.
func (s *Filesystem) Unmount() error {
	s.mu.Lock()
	server := s.server
	if server == nil || s.unmounted {
		s.mu.Unlock()
		return nil
	}
	s.unmounted = true
	s.mu.Unlock()
	s.debugOp("unmount start", "project", s.project)
	err := server.Unmount()
	if err != nil {
		s.mu.Lock()
		s.unmounted = false
		s.mu.Unlock()
		s.debugOp("unmount failed", "project", s.project, "err", err)
		return err
	}
	s.mu.Lock()
	if s.server == server {
		s.server = nil
	}
	s.mu.Unlock()
	s.debugOp("unmount complete", "project", s.project)
	return nil
}

// Close releases filesystem resources after unmount.
func (s *Filesystem) Close() error {
	s.debugOp("close start", "project", s.project)
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	s.mu.Unlock()
	// Stop fan-out first (unsubscribe the push subscription): no
	// invalidation may fire into teardown (its entry/content notifies
	// would race unmounting).
	if s.stopInvalPoll != nil {
		s.stopInvalPoll()
	}
	// A failed Unmount must not be swallowed - the mount may still
	// be live, which is exactly the state the quarantine-on-close path
	// defends. Data preservation still runs first; the error surfaces afterwards.
	unmountErr := s.Unmount()
	s.mu.Lock()
	handles := make([]*storhubHandle, 0, len(s.handles))
	for _, handle := range s.handles {
		handles = append(handles, handle)
	}
	writeStates := make([]*inodeWriteState, 0, len(s.writeStates))
	for _, writeState := range s.writeStates {
		writeStates = append(writeStates, writeState)
	}
	s.mu.Unlock()
	// Preserve uncommitted overlay data before tearing down; deleting it
	// would silently discard acknowledged writes. opMu is taken per state
	// so a quarantine cannot slip into the network window of an
	// in-flight commit and nil out its temp mid-flight. The wait is
	// bounded (closeOpMuTimeout per state): an in-flight Flush, Fsync,
	// Release, or O_SYNC commit holding opMu across minutes of upload
	// must not wedge Close. On timeout the state is left in place for
	// the startup sweep plus an error log naming the inode, so in-flight
	// work is never lost silently.
	for _, writeState := range writeStates {
		if !writeState.opMu.TryLock() {
			if !waitOpMuBounded(&writeState.opMu, closeOpMuTimeout) {
				s.errorOp("close state busy past bound, leaving overlay for startup sweep", "inode", writeState.inode, "path", writeState.pathForLog())
				continue
			}
		}
		if writeState.hasUncommittedChanges() {
			writeState.quarantineTempsReason(quarantineReasonClose)
		}
		unlockOpMu(&writeState.opMu)
	}
	for _, handle := range handles {
		handle.closeTemp()
	}
	// Release the ownership claim last so the directory stays exclusively
	// ours for the whole teardown, including temp quarantine above. The
	// lockfile stays on disk: unlocking is what releases ownership, and
	// unlinking would race a contender into locking an orphaned inode.
	if s.lockFile != nil {
		_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
		_ = s.lockFile.Close()
		s.lockFile = nil
	}
	s.debugOp("close complete", "project", s.project)
	if unmountErr != nil {
		s.errorOp("close unmount failed", "project", s.project, "err", unmountErr)
		return unmountErr
	}
	return nil
}
