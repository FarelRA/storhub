package fusefs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	xattrCreate     = 0x1
	xattrReplace    = 0x2
	renameNoReplace = 0x1
	renameExchange  = 0x2
	renameWhiteout  = 0x4
	mountMaxIOSize  = 1 * 1024 * 1024
	// Keep metadata operations responsive if slow readers fill the kernel's
	// asynchronous FUSE request queue. go-fuse defaults this to 12.
	mountMaxBackground = 256
	// maxReadAheadBytes caps the kernel readahead window. ChunkSize is a
	// transfer-alignment unit (default ~2 GiB), not a sane readahead:
	// telling the kernel it may read ahead a whole chunk per stream asked
	// for multi-GB page-cache pressure on big-RAM boxes. 1-4 MiB is the
	// normal FUSE window (MaxWrite is already capped at 1 MiB).
	maxReadAheadBytes = 4 << 20
)

// readAheadBytes returns the kernel readahead window for a given chunk
// size: the normal FUSE cap, never more than one chunk.
func readAheadBytes(chunkSize int64) int {
	size := normalizedChunkSize(chunkSize)
	if size > maxReadAheadBytes {
		size = maxReadAheadBytes
	}
	return int(size)
}

type Options struct {
	EntryTimeout    time.Duration
	AttrTimeout     time.Duration
	NegativeTimeout time.Duration
	// OverlayBufferSize is the size of each buffer allocated by overlay copy loops
	// (snapshot materialization, range reads). It bounds resident memory per
	// copying handle and is independent of the storage chunk size, which
	// only governs how dirty ranges align to remote chunks.
	OverlayBufferSize int64
	ExtraMountOpts    []string
	CacheDir          string
	AllowOther        bool
	Debug             bool
	Logger            *slog.Logger
}

// isReadOnly reports whether the caller configured a read-only mount by
// passing "ro" among the extra FUSE mount options.
func (o Options) isReadOnly() bool {
	for _, opt := range o.ExtraMountOpts {
		if opt == "ro" {
			return true
		}
	}
	return false
}

type Filesystem struct {
	hub     Hub
	project string
	opts    Options

	root   *storhubNode
	server *fuse.Server

	mu          sync.RWMutex
	nodes       map[uint64]*storhubNode
	inodePaths  map[uint64]map[string]struct{}
	pathToInode map[string]uint64
	lockTable   map[uint64][]lockRecord
	// lockCond wakes blocking (F_SETLKW) lock waiters whenever any lock
	// record changes; its Locker is s.mu so waiters re-check under it.
	lockCond    *sync.Cond
	writeStates map[uint64]*inodeWriteState
	handles     map[uint64]*storhubHandle
	nextHandle  atomic.Uint64
	cacheDir    string
	// lockFile holds this mount's flock(2) claim on cacheDir; unlocked
	// and closed on Close. The lockfile itself stays on disk by design.
	lockFile  *os.File
	closing   bool
	unmounted bool
	// invalCount counts kernel-cache invalidation requests issued by
	// mutation paths. Production mounts observe them as Notify
	// calls; tests read them via Invalidations.
	invalCount atomic.Uint64
	// notifyQueued holds the notifications already dispatched (pending or
	// in flight), guarded by notifyMu; a duplicate for the same target
	// coalesces into the pending one instead of piling up at the kernel.
	notifyMu     sync.Mutex
	notifyQueued map[notifyKey]struct{}
	// notifySlots bounds concurrent entry/delete notifications: each one
	// is a synchronous write to /dev/fuse that can stall under kernel
	// backpressure, and unbounded notify goroutines would queue faster
	// than they drain.
	notifySlots chan struct{}
}

// maxConcurrentNotifies bounds in-flight kernel cache notifications per
// filesystem. Notifications are small, self-resolving writes; eight
// concurrent writers is far past what the FUSE device consumes between
// scheduling ticks, while keeping the mutation path's backpressure gentle.
const maxConcurrentNotifies = 8

type storhubNode struct {
	gofusefs.Inode
	fs    *Filesystem
	inode uint64
	kind  metadata.NodeKind
	isDir bool
}

// OnForget evicts bookkeeping for a node the kernel has completely
// forgotten. Without this the nodes/inodePaths/pathToInode/lockTable maps
// grow without bound over the lifetime of a long-lived mount: every path
// ever looked up or listed stays resident. Bookkeeping is keyed by inode
// number, so if this eviction ever races a fresh lookup (go-fuse can fire
// spurious OnForget around RmChild/AddChild), the next ensureNode simply
// re-registers the paths and operations self-heal.
func (n *storhubNode) OnForget() {
	n.fs.forgetNodeBookkeeping(n)
}

func (s *Filesystem) forgetNodeBookkeeping(n *storhubNode) {
	if n == nil || n.inode == 1 {
		return // root lives for the mount's lifetime
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nodes[n.inode] != n {
		return // superseded by a newer incarnation for this inode number
	}
	delete(s.nodes, n.inode)
	delete(s.inodePaths, n.inode)
	for p, ino := range s.pathToInode {
		if ino == n.inode {
			delete(s.pathToInode, p)
		}
	}
	// Lock records die with the node only once nothing can still exercise
	// them: no open handle and no pending write state.
	if s.hasOpenHandleForLocked(n.inode) || s.writeStates[n.inode] != nil {
		return
	}
	delete(s.lockTable, n.inode)
}

// hasOpenHandleForLocked reports whether any unclosed handle exists for the
// inode; callers must hold s.mu for reading.
func (s *Filesystem) hasOpenHandleForLocked(inode uint64) bool {
	for _, handle := range s.handles {
		if handle.inode == inode && !handle.closed {
			return true
		}
	}
	return false
}

// Integration-test seam, deliberately exported: the storage↔FUSE
// integration suite lives in internal/storage (storhub_test.go) and must
// construct and drive handles/nodes across the package boundary. These
// aliases exist only for that suite; do not use them in production code.
type (
	TestNode   = storhubNode
	TestHandle = storhubHandle
)

// defaultOverlayBufferSize bounds each overlay copy-loop allocation when
// the embedder did not configure Options.OverlayBufferSize.
const defaultOverlayBufferSize = 128 * 1024

// readOnlyEntryTimeout/readOnlyAttrTimeout replace the 60s defaults for
// read-only mounts: nothing changes under their own users, so the only
// staleness source is remote updates, and a 60s revalidation wave re-walks
// a big tree thousands of times per minute forever. Ten minutes cuts that
// storm by 10x while bounding remote-visibility latency.
const (
	readOnlyEntryTimeout = 10 * time.Minute
	readOnlyAttrTimeout  = 10 * time.Minute
)

func DefaultOptions() Options {
	return Options{
		EntryTimeout:      60 * time.Second,
		AttrTimeout:       60 * time.Second,
		NegativeTimeout:   10 * time.Second,
		OverlayBufferSize: defaultOverlayBufferSize,
		ExtraMountOpts:    []string{"noatime"},
		Debug:             false,
	}
}

func (s *Filesystem) Options() Options {
	return s.opts
}

func (s *Filesystem) RootNode() *TestNode {
	return s.root
}

func (s *Filesystem) EnsureNodeForTest(ctx context.Context, entry *shfs.EntryInfo) *TestNode {
	return s.ensureNode(ctx, entry)
}

// ResetNodeForTest drops any cached node bound to the given path, so a
// subsequent EnsureNodeForTest observes hub-level identity changes (the
// inode of a deleted-and-recreated file differs). Real mounts get this
// for free: the kernel re-runs Lookup after invalidation instead of
// replaying a stale node handle.
func (s *Filesystem) ResetNodeForTest(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.pathToInode[path]; ok {
		delete(s.nodes, old)
	}
	delete(s.pathToInode, path)
}

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
	Now() int64
	ChunkSize() int64
}

// mountLockFileName arbitrates exclusive ownership of a FUSE cache
// directory via flock(2); its content is diagnostic only (last holder's
// pid as decimal text).
const mountLockFileName = ".storhub-mount.lock"

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
	cacheDir := opts.CacheDir
	if strings.TrimSpace(cacheDir) == "" {
		cacheDir = path.Join(storcfg.CacheBase(), "fuse", project)
	}
	// 0700: the overlay temps are 0600, but their names and timestamps in
	// a world-readable directory would still leak activity; match the
	// recovery directory's mode.
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return nil, fmt.Errorf("create fuse cache dir: %w", err)
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
	// Startup sweep: handle-* and inode-* flat files from a crashed
	// previous mount may hold the only copy of SIGKILL-before-commit
	// data, so they are QUARANTINED into recovery/ instead of deleted.
	// Nothing can reference them (no file is open yet), but the bytes
	// survive for manual recovery. The recovery/ directory itself is
	// preserved by design.
	// Construction past this point cannot fail, so no failed New leaves a
	// claim behind; Close releases it.
	if entries, err := os.ReadDir(cacheDir); err != nil {
		// A sweep that could not run must be loud: leftover dirty temps
		// from a crashed mount stay unreported otherwise.
		logging.Error(opts.Logger, "startup sweep skipped; cache dir unreadable", "dir", cacheDir, "err", err)
	} else {
		for _, entry := range entries {
			name := entry.Name()
			if name == "recovery" || (!strings.HasPrefix(name, "inode-") && !strings.HasPrefix(name, "handle-")) {
				continue
			}
			quarantinePath(path.Join(cacheDir, name), opts.Logger)
		}
	}
	// Startup replay: surface whatever earlier crashes quarantined so
	// operators (and RecoveryInventory callers) see it immediately.
	logRecoveryInventory(path.Join(cacheDir, "recovery"), opts.Logger)
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
	}
	fsys.root = &storhubNode{fs: fsys, inode: 1, isDir: true}
	fsys.nodes[1] = fsys.root
	fsys.lockCond = sync.NewCond(&fsys.mu)
	return fsys, nil
}

func (s *Filesystem) Mount(mountPoint string) error {
	s.debugf("mount start project=%s target=%s allow_other=%t cache_dir=%s", s.project, mountPoint, s.opts.AllowOther, s.cacheDir)
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
		s.debugf("mount failed project=%s target=%s err=%v", s.project, mountPoint, err)
		return err
	}
	s.server = server
	s.unmounted = false
	s.debugf("mount ready project=%s target=%s", s.project, mountPoint)
	return nil
}

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

func (s *Filesystem) Unmount() error {
	s.mu.Lock()
	server := s.server
	if server == nil || s.unmounted {
		s.mu.Unlock()
		return nil
	}
	s.unmounted = true
	s.mu.Unlock()
	s.debugf("unmount start project=%s", s.project)
	err := server.Unmount()
	if err != nil {
		s.mu.Lock()
		s.unmounted = false
		s.mu.Unlock()
		s.debugf("unmount failed project=%s err=%v", s.project, err)
		return err
	}
	s.mu.Lock()
	if s.server == server {
		s.server = nil
	}
	s.mu.Unlock()
	s.debugf("unmount complete project=%s", s.project)
	return nil
}

func (s *Filesystem) Close() error {
	s.debugf("close start project=%s", s.project)
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	s.mu.Unlock()
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
	// in-flight commit and nil out its temp mid-flight.
	for _, writeState := range writeStates {
		writeState.opMu.Lock()
		if writeState.hasUncommittedChanges() {
			writeState.quarantineTempsReason(quarantineReasonClose)
		}
		writeState.opMu.Unlock()
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
	s.debugf("close complete project=%s", s.project)
	if unmountErr != nil {
		s.errorf("close: unmount failed project=%s err=%v", s.project, unmountErr)
		return unmountErr
	}
	return nil
}

func (s *Filesystem) debugf(format string, args ...any) {
	if !s.opts.Debug || s.opts.Logger == nil {
		return
	}
	logging.Debug(s.opts.Logger, fmt.Sprintf(format, args...))
}

// errorf logs at error level even when no injected logger is configured:
// operational failures like data preservation must never be silently dropped.
func (s *Filesystem) errorf(format string, args ...any) {
	logger := s.opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logging.Error(logger, fmt.Sprintf(format, args...))
}

func (s *Filesystem) recoveryDir() string {
	return path.Join(s.cacheDir, "recovery")
}

// RecoveryEntry describes one quarantined overlay: uncommitted bytes that
// survived a commit failure or a crashed mount. The manifest sidecar
// (<saved>.json) records the original target path so the data can be
// replayed or manually recovered; without it the bytes are anonymous.
type RecoveryEntry struct {
	SavedPath  string `json:"saved_path"`
	OrigTemp   string `json:"orig_temp"`
	TargetPath string `json:"target_path,omitempty"`
	Reason     string `json:"reason"`
	Size       int64  `json:"size"`
	PID        int    `json:"pid"`
	CreatedAt  int64  `json:"created_unix_nano"`
}

// quarantine reasons recorded in manifests.
const (
	quarantineReasonCommitFailure = "commit-failure"
	quarantineReasonClose         = "close-dirty"
	quarantineReasonStartupSweep  = "startup-sweep"
)

// quarantineIntoDir persists tempPath into recoveryDir crash-safely: the
// file is fsynced before the rename, the manifest is written atomically
// (temp + fsync + rename), and the directory itself is fsynced after, so
// a crash at any point leaves either the old state or the complete new
// state - never a renamed file with lost bytes or a manifest without data.
// It returns the saved data path, or "" when nothing could be preserved
// (failures are logged, never fatal: the leftover stays for the next sweep).
func quarantineIntoDir(tempPath, recoveryDir, targetPath, reason string, logger *slog.Logger) string {
	if logger == nil {
		logger = slog.Default()
	}
	info, err := os.Stat(tempPath)
	if err != nil {
		logging.Error(logger, "quarantine skipped; leftover vanished", "path", tempPath, "err", err)
		return ""
	}
	if err := os.MkdirAll(recoveryDir, 0o700); err != nil {
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	// Flush the payload before the rename moves it: otherwise a crash
	// between rename and page writeback loses acknowledged writes.
	if f, err := os.OpenFile(tempPath, os.O_RDWR, 0o600); err == nil {
		syncErr := f.Sync()
		closeErr := f.Close()
		if syncErr != nil || closeErr != nil {
			logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "syncErr", syncErr, "closeErr", closeErr)
			return ""
		}
	} else {
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	stamp := time.Now().UnixNano()
	target := path.Join(recoveryDir, fmt.Sprintf("%s.%d", path.Base(tempPath), stamp))
	entry := RecoveryEntry{
		SavedPath:  target,
		OrigTemp:   tempPath,
		TargetPath: targetPath,
		Reason:     reason,
		Size:       info.Size(),
		PID:        os.Getpid(),
		CreatedAt:  stamp,
	}
	manifest, err := json.Marshal(entry)
	if err != nil {
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	manifestTmp, err := os.CreateTemp(recoveryDir, ".manifest-*.tmp")
	if err != nil {
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	manifestTmpName := manifestTmp.Name()
	if _, err := manifestTmp.Write(append(manifest, '\n')); err != nil {
		_ = manifestTmp.Close()
		_ = os.Remove(manifestTmpName)
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	if err := manifestTmp.Sync(); err != nil {
		_ = manifestTmp.Close()
		_ = os.Remove(manifestTmpName)
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	if err := manifestTmp.Close(); err != nil {
		_ = os.Remove(manifestTmpName)
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	if err := os.Rename(manifestTmpName, target+".json"); err != nil {
		_ = os.Remove(manifestTmpName)
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	if err := os.Rename(tempPath, target); err != nil {
		// The manifest is already durable; remove it so a manifest
		// never points at data that did not arrive.
		_ = os.Remove(target + ".json")
		logging.Error(logger, "quarantine failed; dirty overlay left in cache", "path", tempPath, "err", err)
		return ""
	}
	syncDir(recoveryDir)
	logging.Warn(logger, "quarantined dirty overlay for manual recovery", "path", tempPath, "saved", target, "reason", reason)
	return target
}

// syncDir fsyncs a directory so preceding renames inside it survive a
// crash. Best-effort: the quarantine above is already complete without it.
func syncDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}

// quarantinePath moves tempPath into <cacheDir>/recovery without needing
// a Filesystem (used by the startup sweep before one exists). Failures
// are logged, never fatal: the leftover stays for the next sweep.
func quarantinePath(tempPath string, logger *slog.Logger) {
	dir := path.Join(path.Dir(tempPath), "recovery")
	quarantineIntoDir(tempPath, dir, "", quarantineReasonStartupSweep, logger)
}

// quarantineFile moves tempPath into the recovery directory, which the
// mount-start sweep preserves. Called when a commit fails and the overlay
// holds the only copy of data the application has already written.
// targetPath is the file the bytes belong to ("" when unknown); reason
// records which path triggered the quarantine.
func (s *Filesystem) quarantineFile(tempPath, targetPath, reason string) {
	if reason == "" {
		reason = quarantineReasonCommitFailure
	}
	saved := quarantineIntoDir(tempPath, s.recoveryDir(), targetPath, reason, s.opts.Logger)
	if saved == "" {
		s.errorf("quarantine failed; dirty overlay left in cache path=%s", tempPath)
		return
	}
	s.errorf("commit failed; dirty overlay quarantined for manual recovery path=%s saved=%s", tempPath, saved)
}

// RecoveryInventory replays the recovery directory: every quarantined
// overlay from earlier commit failures or crash sweeps, newest last.
// Manifest-less files (pre-hardening leftovers, manual drops) are reported
// with best-effort stat so nothing is silently hidden.
func (s *Filesystem) RecoveryInventory() ([]RecoveryEntry, error) {
	return readRecoveryInventory(s.recoveryDir())
}

func readRecoveryInventory(recoveryDir string) ([]RecoveryEntry, error) {
	entries, err := os.ReadDir(recoveryDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []RecoveryEntry
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".manifest-") {
			continue
		}
		full := path.Join(recoveryDir, name)
		if raw, err := os.ReadFile(full + ".json"); err == nil {
			var entry RecoveryEntry
			if err := json.Unmarshal(raw, &entry); err == nil && entry.SavedPath != "" {
				entry.SavedPath = full
				out = append(out, entry)
				continue
			}
		}
		var size int64
		if info, err := e.Info(); err == nil {
			size = info.Size()
		}
		out = append(out, RecoveryEntry{SavedPath: full, OrigTemp: name, Reason: "unknown", Size: size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SavedPath < out[j].SavedPath })
	return out, nil
}

// logRecoveryInventory replays quarantined state at startup: operators see
// what survived the last crash instead of discovering it by accident.
func logRecoveryInventory(recoveryDir string, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	inventory, err := readRecoveryInventory(recoveryDir)
	if err != nil {
		logging.Error(logger, "recovery inventory scan failed", "dir", recoveryDir, "err", err)
		return
	}
	if len(inventory) == 0 {
		return
	}
	var total int64
	targets := make([]string, 0, len(inventory))
	for _, e := range inventory {
		total += e.Size
		target := e.TargetPath
		if target == "" {
			target = e.OrigTemp
		}
		targets = append(targets, target)
	}
	logging.Warn(logger, "replaying quarantined overlays from previous run; manual recovery may be needed",
		"dir", recoveryDir, "files", len(inventory), "bytes", total, "targets", strings.Join(targets, ","))
}

func (s *Filesystem) Invalidate() {
	// Snapshot the nodes under the lock, but issue kernel notifications
	// after releasing it: NotifyContent writes to the FUSE connection and
	// must not run while filesystem bookkeeping is locked.
	s.mu.RLock()
	nodes := make([]*storhubNode, 0, len(s.nodes))
	for ino, node := range s.nodes {
		if ino == 1 {
			continue
		}
		nodes = append(nodes, node)
	}
	s.mu.RUnlock()
	for _, node := range nodes {
		safeNotifyContent(node)
	}
}

func (s *Filesystem) pathForInode(inode uint64) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for p := range s.inodePaths[inode] {
		return p
	}
	return ""
}

// safePath resolves the node's current path for hub operations. A
// pathless node is normally the root (inode 1); any other pathless node
// has been deleted or renamed away, and "" would make every hub call
// below silently address the root directory. Such nodes report
// ESTALE instead.
func (n *storhubNode) safePath() (string, syscall.Errno) {
	targetPath := n.currentPath()
	if targetPath == "" && n.inode != 1 {
		return "", syscall.ESTALE
	}
	return targetPath, 0
}

func (s *Filesystem) nodeForPathLocked(targetPath string) *storhubNode {
	if targetPath == "" {
		return s.root
	}
	if ino, exists := s.pathToInode[targetPath]; exists {
		return s.nodes[ino]
	}
	return nil
}

func (s *Filesystem) rememberPath(inode uint64, targetPath string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inodePaths[inode] == nil {
		s.inodePaths[inode] = make(map[string]struct{})
	}
	s.inodePaths[inode][targetPath] = struct{}{}
	s.pathToInode[targetPath] = inode
}

func (s *Filesystem) dropPath(inode uint64, targetPath string) string {
	s.mu.Lock()
	paths := s.inodePaths[inode]
	if _, ok := paths[targetPath]; !ok {
		for p := range paths {
			s.mu.Unlock()
			return p
		}
		s.mu.Unlock()
		return ""
	}
	if len(paths) == 1 {
		delete(s.pathToInode, targetPath)
		delete(s.inodePaths, inode)
		s.mu.Unlock()
		return ""
	}
	delete(paths, targetPath)
	delete(s.pathToInode, targetPath)
	for p := range paths {
		s.mu.Unlock()
		return p
	}
	s.mu.Unlock()
	return ""
}

func (s *Filesystem) rebindHandlesAfterPathChange(inode uint64, oldPath, newPath string) {
	s.mu.RLock()
	handles := make([]*storhubHandle, 0, len(s.handles))
	for _, handle := range s.handles {
		if handle.inode == inode {
			handles = append(handles, handle)
		}
	}
	writeState := s.writeStates[inode]
	s.mu.RUnlock()
	for _, handle := range handles {
		handle.mu.Lock()
		if handle.path == oldPath {
			if newPath != "" {
				// The inode still has a registered path (a hardlink
				// survived the unlink, or the rename target's inode kept
				// another link): follow it instead of detaching, so
				// writes through open fds land in the surviving name
				// (POSIX).
				handle.path = newPath
			} else {
				// The path now belongs to a different inode and nothing
				// references this one anymore. Detach like an
				// unlinked-but-open file: reads keep serving the handle's
				// own snapshot instead of silently switching to the
				// replacement's content.
				handle.path = ""
				handle.deleted = true
			}
		}
		handle.mu.Unlock()
	}
	if writeState != nil {
		writeState.mu.Lock()
		if writeState.path == oldPath {
			if newPath != "" {
				writeState.path = newPath
			} else {
				writeState.path = ""
				writeState.deleted = true
			}
		}
		writeState.mu.Unlock()
	}
}

func (s *Filesystem) remapPaths(oldPath, newPath string) {
	s.mu.Lock()
	for inode, paths := range s.inodePaths {
		for current := range paths {
			if shfs.IsParentOrSame(oldPath, current) {
				remapped := shfs.RemapPath(oldPath, newPath, current)
				delete(paths, current)
				delete(s.pathToInode, current)
				paths[remapped] = struct{}{}
				s.pathToInode[remapped] = inode
			}
		}
	}
	handles := make([]*storhubHandle, 0, len(s.handles))
	for _, handle := range s.handles {
		handles = append(handles, handle)
	}
	s.mu.Unlock()
	for _, handle := range handles {
		handle.mu.Lock()
		if shfs.IsParentOrSame(oldPath, handle.path) {
			handle.path = shfs.RemapPath(oldPath, newPath, handle.path)
		}
		handle.mu.Unlock()
	}
	s.mu.RLock()
	writeStates := make([]*inodeWriteState, 0, len(s.writeStates))
	for _, writeState := range s.writeStates {
		writeStates = append(writeStates, writeState)
	}
	s.mu.RUnlock()
	for _, writeState := range writeStates {
		writeState.mu.Lock()
		if shfs.IsParentOrSame(oldPath, writeState.path) {
			writeState.path = shfs.RemapPath(oldPath, newPath, writeState.path)
		}
		writeState.mu.Unlock()
	}
}

func (s *Filesystem) materializeHandlesForPath(ctx context.Context, inode uint64, targetPath string) error {
	s.mu.RLock()
	handles := make([]*storhubHandle, 0, len(s.handles))
	for _, handle := range s.handles {
		if handle.inode == inode {
			handles = append(handles, handle)
		}
	}
	s.mu.RUnlock()
	for _, handle := range handles {
		handle.mu.Lock()
		// Snapshot every handle of this inode lacking private bytes:
		// after the swap they may be detached from their path, and the
		// old content becomes unfetchable.
		needsSnapshot := handle.temp == nil && handle.writeState == nil
		handle.mu.Unlock()
		if !needsSnapshot {
			continue
		}
		if err := handle.materializePath(ctx, targetPath); err != nil {
			return err
		}
	}
	s.mu.RLock()
	writeState := s.writeStates[inode]
	s.mu.RUnlock()
	if writeState != nil {
		writeState.mu.Lock()
		needsSnapshot := writeState.path == targetPath && !writeState.deleted
		if needsSnapshot {
			if err := writeState.snapshotBaseLocked(ctx, targetPath); err != nil {
				writeState.mu.Unlock()
				return err
			}
		}
		writeState.mu.Unlock()
	}
	return nil
}

// Invalidations reports how many kernel-cache invalidation requests this
// filesystem has issued. Every namespace or attribute mutation must move
// this counter; the 60s entry/attr timeouts turn a missed invalidation
// into a minute of stale reads.
func (s *Filesystem) Invalidations() uint64 {
	return s.invalCount.Load()
}

func (s *Filesystem) ensureNode(ctx context.Context, entry *shfs.EntryInfo) *storhubNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node := s.nodes[entry.Inode]; node != nil {
		if s.inodePaths[entry.Inode] == nil {
			s.inodePaths[entry.Inode] = make(map[string]struct{})
		}
		s.inodePaths[entry.Inode][entry.Path] = struct{}{}
		s.pathToInode[entry.Path] = entry.Inode
		return node
	}
	node := &storhubNode{fs: s, inode: entry.Inode, kind: entry.Kind, isDir: entry.IsDir}
	s.nodes[entry.Inode] = node
	s.inodePaths[entry.Inode] = map[string]struct{}{entry.Path: {}}
	s.pathToInode[entry.Path] = entry.Inode
	return node
}

func (n *storhubNode) stableAttr() gofusefs.StableAttr {
	mode := uint32(syscall.S_IFREG)
	if n.isDir {
		mode = syscall.S_IFDIR
	} else if n.kind == metadata.NodeKindSymlink {
		mode = syscall.S_IFLNK
	}
	return gofusefs.StableAttr{Mode: mode, Ino: n.inode, Gen: 1}
}

func (n *storhubNode) currentPath() string {
	return n.fs.pathForInode(n.inode)
}

// defaultCallerUmask is applied to creation modes because the FUSE
// protocol does not transmit the caller's umask. Without it,
// ApplyCreateMode would be a no-op and `touch` would create 0666
// (world-writable) files.
const defaultCallerUmask = 0o022

func (s *Filesystem) callerContext(ctx context.Context) context.Context {
	ctx = shfs.WithSuppressedAtime(ctx)
	if caller, ok := fuse.FromContext(ctx); ok && caller != nil {
		return shfs.WithIdentity(ctx, shfs.Identity{UID: caller.Uid, GID: caller.Gid, PID: caller.Pid, Umask: defaultCallerUmask, Admin: caller.Uid == 0})
	}
	return ctx
}

func (n *storhubNode) attachChild(ctx context.Context, child *storhubNode) (ino *gofusefs.Inode) {
	defer func() {
		// go-fuse panics on malformed trees; degrade to "no cached child"
		// loudly instead of taking the request goroutine down.
		if recover() != nil {
			n.fs.debugf("attachChild recovered from panic path=%s inode=%d", n.currentPath(), child.inode)
			ino = nil
		}
	}()
	if root := n.Root(); root == nil || root.Operations() == nil {
		return nil
	}
	ino = child.EmbeddedInode()
	if ino != nil && ino.Operations() != nil && ino.StableAttr().Ino != 0 {
		return ino
	}
	return n.NewInode(ctx, child, child.stableAttr())
}

// errnoFromError maps storage-layer errors onto POSIX errnos. The ladder
// is ordered most-specific first: raw Errno passthrough (except ECANCELED,
// which the kernel must see as EINTR), context cancellation/deadline,
// then the fs sentinel family, with EIO as the honest catch-all for
// anything unmapped (never success, never ENOENT).
func errnoFromError(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		// ECANCELED (async cancellation) is not a file error; report it
		// as an interrupt so callers retry instead of caching a bogus
		// per-file failure.
		if errno == syscall.ECANCELED {
			return syscall.EINTR
		}
		return errno
	}
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return syscall.ETIMEDOUT
	}
	// Cancellations and timeouts that lost their error chain (%v
	// formatting, status strings) still read as what they are.
	lowered := strings.ToLower(err.Error())
	if strings.Contains(lowered, "context canceled") {
		return syscall.EINTR
	}
	if strings.Contains(lowered, "deadline exceeded") {
		return syscall.ETIMEDOUT
	}
	switch {
	case errors.Is(err, shfs.ErrAlreadyExists):
		return syscall.EEXIST
	case errors.Is(err, shfs.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, shfs.ErrIsDirectory):
		return syscall.EISDIR
	case errors.Is(err, shfs.ErrNotDirectory):
		return syscall.ENOTDIR
	case errors.Is(err, shfs.ErrNotFound):
		return syscall.ENOENT
	case errors.Is(err, shfs.ErrInvalidSymlink):
		return syscall.EINVAL
	case errors.Is(err, shfs.ErrXAttrNotFound):
		return syscall.ENODATA
	case errors.Is(err, shfs.ErrCorrupted):
		return errCorruptedErrno
	default:
		return syscall.EIO
	}
}

func validateProject(project string) error {
	project = strings.TrimSpace(project)
	if project == "" {
		return errors.New("project is required")
	}
	if len(project) > 100 {
		return fmt.Errorf("project name too long: %d", len(project))
	}
	if project == "." || project == ".." {
		return fmt.Errorf("invalid project name: %s", project)
	}
	for _, ch := range project {
		allowed := ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-'
		if !allowed {
			return fmt.Errorf("invalid project name: %s", project)
		}
	}
	if strings.HasPrefix(project, ".") || strings.HasSuffix(project, ".") {
		return fmt.Errorf("invalid project name: %s", project)
	}
	return nil
}

// normalizedChunkSize delegates to the chunking package's single clamping
// definition.
func normalizedChunkSize(chunkSize int64) int64 {
	return chunking.NormalizedSize(chunkSize)
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func durationPtr(v time.Duration) *time.Duration { return &v }
