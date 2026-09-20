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

// Options configures a FUSE mount: timeouts, buffers, and mount flags.
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
	// Umask masks creation modes because the FUSE protocol does not
	// transmit the caller umask. UmaskSet distinguishes an explicit
	// zero mask (no masking) from an unset option; unset resolves to
	// 0o022 in New and DefaultOptions. Only the low 9 bits apply.
	Umask    uint32
	UmaskSet bool
}

// EffectiveUmask reports the creation-mode mask for this configuration:
// the explicit Umask when set (or non-zero), else the protocol default
// 0o022. Pure resolution, no I/O: safe to call from tests and CLIs.
func (o Options) EffectiveUmask() uint32 {
	if o.UmaskSet || o.Umask != 0 {
		return o.Umask & 0o777
	}
	return defaultCallerUmask
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

// Filesystem is a mounted FUSE filesystem over one project.
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
	// attaching tracks nodes whose go-fuse inode is being initialized
	// (attachChild → NewInode) so kernel upcalls never touch a
	// half-initialized inode: NotifyContent racing initInode is a data
	// race inside go-fuse plus a nil-pointer panic. Skipping is sound,
	// not lossy: attach (lookup/create reply) hands the kernel fresh
	// state synchronously, so there is nothing to invalidate yet.
	// Dedicated small mutex, never held with s.mu, so it cannot join
	// the filesystem lock graph.
	attachMu  sync.Mutex
	attaching map[*storhubNode]struct{}
	// pinnedMu guards pinned, the shared open-time content layouts (see
	// pinnedKey in fuse_file.go). Sharing one immutable layout across
	// handles of the same file version bounds the per-open metadata cost.
	pinnedMu sync.Mutex
	pinned   map[pinnedKey]*pinnedContent
	// stopInvalPoll ends the cross-surface fan-out loop started in New;
	// nil until started (tests driving pollInvalidationsOnce directly
	// never start it). Stopped early in Close so no notification fires
	// into teardown.
	stopInvalPoll func()
	// Notify observability (F7): parked-notify gauge plus counters.
	// notifyParked counts notifies currently blocked on a slot;
	// notifyParkedTotal counts every slot wait observed; notifyCoalesced
	// counts duplicates folded into a pending notify. Behavior is
	// unchanged: backpressure stays by design, only observed.
	notifyParked      atomic.Int64
	notifyParkedTotal atomic.Uint64
	notifyCoalesced   atomic.Uint64
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
	// Delete via the reverse index: O(paths-of-inode), not O(tracked
	// paths). Scanning the whole pathToInode map per forget turned every
	// kernel forget into a full-map pass under s.mu.
	if paths, ok := s.inodePaths[n.inode]; ok {
		for p := range paths {
			delete(s.pathToInode, p)
		}
		delete(s.inodePaths, n.inode)
	}
	// Lock records die with the node only once nothing can still exercise
	// them: no open handle and no pending write state.
	if s.hasOpenHandleForLocked(n.inode) || s.writeStates[n.inode] != nil {
		return
	}
	delete(s.lockTable, n.inode)
}

// hasOpenHandleForLocked reports whether any unclosed handle exists for the
// inode; callers must hold s.mu for reading. Closure is read under each
// handle's own lock: Releases run mutually concurrent (RELEASE is not
// synchronized with close), so scanning h.closed bare races a racing
// closeTemp on another handle of the same inode.
func (s *Filesystem) hasOpenHandleForLocked(inode uint64) bool {
	for _, handle := range s.handles {
		if handle.inode == inode && !handle.isClosed() {
			return true
		}
	}
	return false
}

// isClosed reports handle closure under the handle lock. See
// hasOpenHandleForLocked for the racing counterpart.
func (h *storhubHandle) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// TestNode is an integration-test seam, deliberately exported: the
// storage-FUSE integration suite lives in internal/storage
// (storhub_test.go) and must construct and drive handles/nodes across the
// package boundary. These aliases exist only for that suite; do not use
// them in production code.
type TestNode = storhubNode

// TestHandle is the handle half of the integration-test seam; see TestNode.
type TestHandle = storhubHandle

// defaultOverlayBufferSize bounds each overlay copy-loop allocation when
// the embedder did not configure Options.OverlayBufferSize.
const defaultOverlayBufferSize = 128 * 1024

// readOnlyEntryTimeout/readOnlyAttrTimeout replace the 60s defaults for
// read-only mounts: nothing changes under their own users, so the only
// staleness source is remote updates, and a 60s revalidation wave re-walks
// a big tree thousands of times per minute forever. Ten minutes cuts that
// storm by 10x while bounding remote-visibility latency.
const (
	readOnlyEntryTimeout = 120 * storcfg.PatienceUnit
	readOnlyAttrTimeout  = 120 * storcfg.PatienceUnit
)

// DefaultOptions returns Options with defaults applied.
func DefaultOptions() Options {
	return Options{
		EntryTimeout:      12 * storcfg.PatienceUnit,
		AttrTimeout:       12 * storcfg.PatienceUnit,
		NegativeTimeout:   2 * storcfg.PatienceUnit,
		OverlayBufferSize: defaultOverlayBufferSize,
		ExtraMountOpts:    []string{"noatime"},
		Debug:             false,
	}
}

// Options returns the effective mount options of the filesystem.
func (s *Filesystem) Options() Options {
	return s.opts
}

// RootNode returns the root node for tests; see TestNode.
func (s *Filesystem) RootNode() *TestNode {
	return s.root
}

// EnsureNodeForTest materializes the node for an entry; see TestNode.
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
// mount into recovery/ instead of deleting them: handle-* and inode-*
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
		if name == "recovery" || (!strings.HasPrefix(name, "inode-") && !strings.HasPrefix(name, "handle-")) {
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
	// Publish under mu like every other server access (connected(),
	// Wait, Unmount): concurrent notify readers must observe this write
	// through the mutex, never as a data race.
	s.mu.Lock()
	s.server = server
	s.unmounted = false
	s.mu.Unlock()
	s.debugf("mount ready project=%s target=%s", s.project, mountPoint)
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

// opReleaseMu guards opReleaseGen/opReleaseCh: the commit-release
// broadcast (Phase E3, F4). The mutex is held only for the integer bump
// plus channel swap, never across network I/O, so signaling can never
// wedge a committer.
var (
	opReleaseMu  sync.Mutex
	opReleaseGen uint64
	opReleaseCh  = make(chan struct{})
	// opReleaseWaiters counts Close waiters parked (or about to park)
	// in waitOpMuBounded. Releases skip the channel swap when no
	// waiter exists, so uncontended commit-path unlocks cost one
	// integer bump and zero allocations (allocation parity on the
	// FUSE hot path is budget-gated). Registration happens before
	// the first snapshot: any release after a failed TryLock observes
	// the waiter, so no park can miss its wake; a wedged committer
	// still falls back to the loud timeout.
	opReleaseWaiters atomic.Int64
)

// signalOpRelease publishes one commit-state generation. Every opMu
// release on the inode commit path calls it right after unlocking
// (see unlockOpMu). Channel-swap broadcast: waiters hold the previous
// channel, which closes exactly once here, so no waiter can miss a
// release that happened after its snapshot. A release with no waiter
// only bumps the counter; a waiter that somehow misses a signal falls
// back to the closeOpMuTimeout expiry in waitOpMuBounded, which is loud
// (quarantine plus error log), never silent.
func signalOpRelease() {
	opReleaseMu.Lock()
	opReleaseGen++
	if opReleaseWaiters.Load() == 0 {
		opReleaseMu.Unlock()
		return
	}
	prev := opReleaseCh
	opReleaseCh = make(chan struct{})
	opReleaseMu.Unlock()
	close(prev)
}

// opReleaseWait snapshots the current broadcast generation and channel.
// The caller re-checks its lock after snapshotting, then parks on the
// channel: any release after the snapshot closes it.
func opReleaseWait() (<-chan struct{}, uint64) {
	opReleaseMu.Lock()
	defer opReleaseMu.Unlock()
	return opReleaseCh, opReleaseGen
}

// unlockOpMu releases an inode write-state opMu and publishes the
// commit-release broadcast. Use at every opMu release site (including
// short holders outside the commit network window) so a Close waiter
// parked in waitOpMuBounded never sleeps past a release: a spurious
// wakeup costs one TryLock, a missed one costs up to the loud timeout.
func unlockOpMu(mu *sync.Mutex) {
	mu.Unlock()
	signalOpRelease()
}

// waitOpMuParkedHook, when non-nil, runs each time waitOpMuBounded is
// about to park on the broadcast. Test-only observation point: it lets
// the Close-during-commit test prove the waiter was actually parked
// before the fake committer releases, so the test cannot pass by
// release-before-park luck. Nil in production; stored atomically so
// parallel suites never race on it.
var waitOpMuParkedHook atomic.Pointer[func()]

// closeOpMuTimeout bounds how long Close waits for one writeState's opMu
// before quarantining-and-returning. Documented bound: 5 seconds per
// state, so Close latency tracks state count, never the slowest commit.
// It stays as the loud backstop behind the commit-release broadcast:
// timeout expiry means the committer is wedged, which must stay
// fail-loud (quarantine plus error log in Close).
const closeOpMuTimeout = 1 * storcfg.PatienceUnit

// waitOpMuBounded acquires mu before timeout, polling nothing. It waits
// on the commit-release broadcast (signalOpRelease at every opMu release) and
// retries the TryLock on each wake, so Close latency tracks the actual
// commit end instead of a tick. Reports whether the lock was acquired
// (caller must Unlock on true). The timeout is only the wedged-committer
// backstop: expiry returns false and Close quarantines loudly.
func waitOpMuBounded(mu *sync.Mutex, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	if mu.TryLock() {
		return true
	}
	// Register before the first snapshot so every release after the
	// failed TryLock above observes this waiter and swaps the
	// channel (see opReleaseWaiters). Unregistered on every return.
	opReleaseWaiters.Add(1)
	defer opReleaseWaiters.Add(-1)
	for {
		ch, _ := opReleaseWait()
		// Re-check after the snapshot: a release racing the snapshot
		// already closed the previous channel, and the lock may be
		// free now. Without this a release in the window parks us
		// until the next signal.
		if mu.TryLock() {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return mu.TryLock()
		}
		if hook := waitOpMuParkedHook.Load(); hook != nil {
			(*hook)()
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// pathForLog snapshots the writeState path for error logs without racing
// committers: best effort under mu.
func (w *inodeWriteState) pathForLog() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.path
}

// Close releases filesystem resources after unmount.
func (s *Filesystem) Close() error {
	s.debugf("close start project=%s", s.project)
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
				s.errorf("close: state busy past bound, leaving overlay for startup sweep inode=%d path=%s", writeState.inode, writeState.pathForLog())
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

// log returns the mount logger, never nil. Operational failures must
// never be silently dropped, and several paths historically logged with a
// nil *slog.Logger (which routes nowhere useful); thread this instead.
func (s *Filesystem) log() *slog.Logger {
	if s == nil || s.opts.Logger == nil {
		return slog.Default()
	}
	return s.opts.Logger
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
// FullImage + Fingerprint make an entry eligible for startup auto-redrive:
// a temp proven to hold the complete file image whose target still matches
// the recorded fingerprint is re-uploaded automatically. Anything else
// (range fragments, unknown provenance, changed targets) stays for manual
// recovery — redriving a partial temp as a full file, or over a changed
// target, would destroy data instead of rescuing it.
type RecoveryEntry struct {
	SavedPath  string `json:"saved_path"`
	OrigTemp   string `json:"orig_temp"`
	TargetPath string `json:"target_path,omitempty"`
	Reason     string `json:"reason"`
	Size       int64  `json:"size"`
	PID        int    `json:"pid"`
	CreatedAt  int64  `json:"created_unix_nano"`
	// FullImage reports the temp holds the complete file image
	// (tempAuthoritative or dirty ranges covering [0, logicalSize) at
	// quarantine time). Only full images are auto-redrive candidates.
	FullImage bool `json:"full_image,omitempty"`
	// Ranges records the dirty spans covered, for manual recovery
	// context on range fragments (which are never auto-redriven).
	Ranges [][2]int64 `json:"ranges,omitempty"`
	// BaseSize/LogicalSize are the overlay's sizes at quarantine time,
	// for manual recovery context.
	BaseSize    int64 `json:"base_size,omitempty"`
	LogicalSize int64 `json:"logical_size,omitempty"`
	// Pending records a metadata patch (chmod/chown/utimes) that was
	// staged alongside the data commit and never landed. Redrive applies
	// it after the data, under the same fingerprint guard.
	Pending *shfs.MetadataPatch `json:"pending,omitempty"`
	// Fingerprint pins the target's identity at quarantine time. The
	// redrive compares it against the live target and proceeds only on
	// an exact match: any concurrent modification refuses loudly and
	// the entry stays quarantined.
	Fingerprint *targetFingerprint `json:"fingerprint,omitempty"`
}

// targetFingerprint is the compare-and-swap token for auto-redrive: every
// content mutation stamps ChangedAt (and replace-family ops reassign the
// inode), so an exact match proves the target is untouched since quarantine.
// Same-second same-size rewrites can theoretically slip (mtime granularity);
// the redrive logs loudly so even that case is auditable.
type targetFingerprint struct {
	Size       int64  `json:"size"`
	Inode      uint64 `json:"inode"`
	ModifiedAt int64  `json:"modified_at"`
	ChangedAt  int64  `json:"changed_at"`
}

// quarantineIntent carries what quarantineIntoDir records in the sidecar
// beyond the payload itself. Zero value = unknown provenance: manual
// recovery only, never auto-redrive.
type quarantineIntent struct {
	fullImage   bool
	ranges      [][2]int64
	baseSize    int64
	logicalSize int64
	pending     shfs.MetadataPatch
	hasPending  bool
	fingerprint *targetFingerprint
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
	return quarantineIntoDirWithIntent(tempPath, recoveryDir, targetPath, reason, quarantineIntent{}, logger)
}

// quarantineIntoDirWithIntent is quarantineIntoDir plus the redrive intent
// for the sidecar: whether the temp is a proven full image, the dirty
// spans and sizes for manual context, and the target fingerprint for the
// startup compare-and-swap. A zero intent records unknown provenance:
// manual recovery only, never auto-redrive.
func quarantineIntoDirWithIntent(tempPath, recoveryDir, targetPath, reason string, intent quarantineIntent, logger *slog.Logger) string {
	if logger == nil {
		logger = slog.Default()
	}
	info, err := os.Stat(tempPath)
	if err != nil {
		logging.Error(logger, "quarantine skipped; leftover vanished", "path", tempPath, "err", err)
		return ""
	}
	if err := os.MkdirAll(recoveryDir, 0o700); err != nil {
		return failQuarantine(logger, tempPath, "err", err)
	}
	// Flush the payload before the rename moves it: otherwise a crash
	// between rename and page writeback loses acknowledged writes.
	if f, err := os.OpenFile(tempPath, os.O_RDWR, 0o600); err == nil {
		syncErr := f.Sync()
		closeErr := f.Close()
		if syncErr != nil || closeErr != nil {
			return failQuarantine(logger, tempPath, "syncErr", syncErr, "closeErr", closeErr)
		}
	} else {
		return failQuarantine(logger, tempPath, "err", err)
	}
	stamp := time.Now().UnixNano()
	target := path.Join(recoveryDir, fmt.Sprintf("%s.%d", path.Base(tempPath), stamp))
	entry := RecoveryEntry{
		SavedPath:   target,
		OrigTemp:    tempPath,
		TargetPath:  targetPath,
		Reason:      reason,
		Size:        info.Size(),
		PID:         os.Getpid(),
		CreatedAt:   stamp,
		FullImage:   intent.fullImage,
		Ranges:      intent.ranges,
		BaseSize:    intent.baseSize,
		LogicalSize: intent.logicalSize,
		Fingerprint: intent.fingerprint,
	}
	if intent.hasPending {
		pending := intent.pending
		entry.Pending = &pending
	}
	manifest, err := json.Marshal(entry)
	if err != nil {
		return failQuarantine(logger, tempPath, "err", err)
	}
	manifestTmp, err := os.CreateTemp(recoveryDir, ".manifest-*.tmp")
	if err != nil {
		return failQuarantine(logger, tempPath, "err", err)
	}
	manifestTmpName := manifestTmp.Name()
	if _, err := manifestTmp.Write(append(manifest, '\n')); err != nil {
		_ = manifestTmp.Close()
		_ = os.Remove(manifestTmpName)
		return failQuarantine(logger, tempPath, "err", err)
	}
	if err := manifestTmp.Sync(); err != nil {
		_ = manifestTmp.Close()
		_ = os.Remove(manifestTmpName)
		return failQuarantine(logger, tempPath, "err", err)
	}
	if err := manifestTmp.Close(); err != nil {
		_ = os.Remove(manifestTmpName)
		return failQuarantine(logger, tempPath, "err", err)
	}
	if err := os.Rename(manifestTmpName, target+".json"); err != nil {
		_ = os.Remove(manifestTmpName)
		return failQuarantine(logger, tempPath, "err", err)
	}
	if err := os.Rename(tempPath, target); err != nil {
		// The manifest is already durable; remove it so a manifest
		// never points at data that did not arrive.
		_ = os.Remove(target + ".json")
		return failQuarantine(logger, tempPath, "err", err)
	}
	syncDir(recoveryDir)
	logging.Warn(logger, "quarantined dirty overlay for manual recovery", "path", tempPath, "saved", target, "reason", reason)
	return target
}

// failQuarantine logs a quarantine failure (the dirty overlay stays in
// the cache for the next sweep) and returns "".
func failQuarantine(logger *slog.Logger, tempPath string, args ...any) string {
	args = append([]any{"path", tempPath}, args...)
	logging.Error(logger, "quarantine failed; dirty overlay left in cache", args...)
	return ""
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
func (s *Filesystem) quarantineFile(tempPath, targetPath, reason string, intent quarantineIntent) {
	if reason == "" {
		reason = quarantineReasonCommitFailure
	}
	saved := quarantineIntoDirWithIntent(tempPath, s.recoveryDir(), targetPath, reason, intent, s.opts.Logger)
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

// redriveRecoveryInventory auto-redrives eligible quarantined overlays:
// entries whose target still matches the recorded fingerprint are
// re-uploaded (full images replace, recorded spans patch, bare size
// changes truncate; staged metadata patches apply after the data), then
// committed via the hub's normal path, verified, and only then removed
// from recovery/. Replace journals the op like any acknowledged write,
// so crash-durability past this point is the standard journal story.
// Anything else is kept with a loud warning: range fragments without a
// fingerprint, unknown provenance, changed targets, and any
// upload/commit error. A failed redrive never fails the mount and
// never deletes quarantine data — the entry simply waits for the next
// mount or manual recovery.
func redriveRecoveryInventory(ctx context.Context, hub Hub, project, recoveryDir string, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	inventory, err := readRecoveryInventory(recoveryDir)
	if err != nil {
		logging.Error(logger, "recovery redrive scan failed", "dir", recoveryDir, "err", err)
		return
	}
	for _, entry := range inventory {
		redriveRecoveryEntry(ctx, hub, project, entry, logger)
	}
}

func redriveRecoveryEntry(ctx context.Context, hub Hub, project string, entry RecoveryEntry, logger *slog.Logger) {
	target, saved := entry.TargetPath, entry.SavedPath
	if target == "" || entry.Fingerprint == nil {
		return // manual recovery only; inventoried (not silent) by the caller
	}
	live, err := hub.StatPathContext(ctx, project, target)
	if err != nil || live == nil {
		logging.Warn(logger, "recovery redrive refused: target unreadable, keeping quarantine", "target", target, "saved", saved)
		return
	}
	fp := entry.Fingerprint
	if live.Size != fp.Size || live.Inode != fp.Inode || live.ModifiedAt != fp.ModifiedAt || live.ChangedAt != fp.ChangedAt {
		logging.Warn(logger, "recovery redrive refused: target changed since quarantine, keeping quarantine",
			"target", target, "saved", saved, "live_size", live.Size, "live_inode", live.Inode)
		return
	}
	// The fingerprint matched: the target is exactly what quarantine saw.
	// Redrive by temp kind. A full image replaces the file; recorded spans
	// patch it; a bare size change truncates it. Anything undescribed stays
	// manual: redriving bytes the sidecar cannot account for would invent
	// content.
	switch {
	case entry.FullImage:
		if _, err := hub.ReplaceFileContext(ctx, project, target, saved); err != nil {
			logging.Warn(logger, "recovery redrive replace failed, keeping quarantine", "target", target, "saved", saved, "err", err)
			return
		}
	case len(entry.Ranges) > 0:
		if !redriveRanges(ctx, hub, project, entry, logger) {
			return
		}
	case entry.LogicalSize != entry.BaseSize:
		if _, err := hub.TruncateFileContext(ctx, project, target, entry.LogicalSize); err != nil {
			logging.Warn(logger, "recovery redrive truncate failed, keeping quarantine", "target", target, "saved", saved, "err", err)
			return
		}
		if _, err := hub.StatPathContext(ctx, project, target); err != nil {
			logging.Warn(logger, "recovery redrive truncate left no target, keeping quarantine", "target", target, "saved", saved, "err", err)
			return
		}
	default:
		return // nothing described: manual recovery only
	}
	if entry.Pending != nil {
		if err := hub.ApplyMetadataPatchContext(ctx, project, target, *entry.Pending); err != nil {
			// Data landed but the metadata patch did not: keep the entry
			// (with its payload) so the next mount retries the patch
			// instead of declaring victory on half-applied state.
			logging.Warn(logger, "recovery redrive data landed but metadata patch failed, keeping quarantine", "target", target, "saved", saved, "err", err)
			return
		}
	}
	// Verify before deleting: the redrive must be observable, or the
	// quarantine data (the only copy) stays.
	after, err := hub.StatPathContext(ctx, project, target)
	if err != nil || after == nil {
		logging.Warn(logger, "recovery redrive verify failed, keeping quarantine", "target", target, "saved", saved, "err", err)
		return
	}
	if err := os.Remove(saved); err != nil && !os.IsNotExist(err) {
		logging.Warn(logger, "recovery redrive cleanup failed (data is committed; remove manually)", "target", target, "saved", saved, "err", err)
		return
	}
	if err := os.Remove(saved + ".json"); err != nil && !os.IsNotExist(err) {
		// The payload is gone but its sidecar survived: the next mount
		// inventories a manifest without data (kept, warned, never
		// redriven — the redrive requires the payload to exist).
		// Loud here so the orphan is removed, not wondered at.
		logging.Warn(logger, "recovery redrive sidecar cleanup failed (payload committed; remove sidecar manually)", "target", target, "saved", saved+".json", "err", err)
		return
	}
	logging.Warn(logger, "recovery redrive committed quarantined overlay", "target", target)
}

// redriveRanges replays recorded dirty spans from the saved temp through
// the patch verb: each span's bytes are read from the temp at the recorded
// offsets and patched over the same offsets. Reads use ReadAt per span so
// a huge temp never loads fully into memory for a small dirty set. Spans
// outside the temp file refuse the whole entry (a truncated temp must
// never redrive partial ranges). Reports whether the caller may proceed
// to verification.
func redriveRanges(ctx context.Context, hub Hub, project string, entry RecoveryEntry, logger *slog.Logger) bool {
	target, saved := entry.TargetPath, entry.SavedPath
	f, err := os.Open(saved)
	if err != nil {
		logging.Warn(logger, "recovery redrive refused: payload unreadable, keeping quarantine", "target", target, "saved", saved, "err", err)
		return false
	}
	defer func() { _ = f.Close() }()
	var size int64
	if info, err := f.Stat(); err == nil {
		size = info.Size()
	} else {
		logging.Warn(logger, "recovery redrive refused: payload unstatable, keeping quarantine", "target", target, "saved", saved, "err", err)
		return false
	}
	edits := make([]shfs.RangeEdit, 0, len(entry.Ranges))
	for _, span := range entry.Ranges {
		start, end := span[0], span[1]
		if start < 0 || end < start || end > size {
			logging.Warn(logger, "recovery redrive refused: span outside payload, keeping quarantine",
				"target", target, "saved", saved, "span", span)
			return false
		}
		edit := make([]byte, end-start)
		if _, err := f.ReadAt(edit, start); err != nil {
			logging.Warn(logger, "recovery redrive refused: span unreadable, keeping quarantine",
				"target", target, "saved", saved, "span", span, "err", err)
			return false
		}
		edits = append(edits, shfs.RangeEdit{Start: start, DeleteSize: end - start, Data: edit})
	}
	if _, err := hub.PatchFileRangesContext(ctx, project, target, edits); err != nil {
		logging.Warn(logger, "recovery redrive patch failed, keeping quarantine", "target", target, "saved", saved, "err", err)
		return false
	}
	return true
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

// Invalidate drops cached kernel entries after external mutation.
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
		// Serialize path rebinding against in-flight commits: commit
		// holds opMu across its DAC window plus network window, so
		// taking opMu here (order opMu before mu, matching commit)
		// closes the stale-path race. Snapshot was taken without
		// holding opMu, so no lock cycle with committers.
		writeState.opMu.Lock()
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
		unlockOpMu(&writeState.opMu)
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
		// Same opMu-before-mu order as commit, closing the directory
		// remap race the same way as the single-path rebind above.
		writeState.opMu.Lock()
		writeState.mu.Lock()
		if shfs.IsParentOrSame(oldPath, writeState.path) {
			writeState.path = shfs.RemapPath(oldPath, newPath, writeState.path)
		}
		writeState.mu.Unlock()
		unlockOpMu(&writeState.opMu)
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

func (s *Filesystem) ensureNode(_ context.Context, entry *shfs.EntryInfo) *storhubNode {
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
		// Supplementary groups come from the host NSS lookup: the FUSE
		// protocol carries uid/gid only, and without them the DAC judges
		// a multi-group caller by primary group alone. The lookup fails
		// open (empty groups on error), so this line never newly denies.
		groups, _ := shfs.LookupUserGroups(caller.Uid)
		return shfs.WithIdentity(ctx, shfs.Identity{UID: caller.Uid, GID: caller.Gid, PID: caller.Pid, Groups: groups, Umask: s.opts.EffectiveUmask(), Admin: caller.Uid == 0})
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
	// Mark for the duration of NewInode: kernel upcalls issued
	// concurrently (content/entry notifications from commit and fan-out
	// paths) must skip this node until go-fuse finishes initializing it.
	// Deferred unmark runs even if NewInode panics (the outer recover
	// degrades to "no cached child"); without it a panicking attach
	// would suppress that node's invalidations forever.
	n.fs.attachMu.Lock()
	n.fs.attaching[child] = struct{}{}
	n.fs.attachMu.Unlock()
	defer func() {
		n.fs.attachMu.Lock()
		delete(n.fs.attaching, child)
		n.fs.attachMu.Unlock()
	}()
	return n.NewInode(ctx, child, child.stableAttr())
}

// errnoFromError maps storage-layer errors onto POSIX errnos. The ladder
// is ordered most-specific first: raw Errno passthrough (except ECANCELED,
// which the kernel must see as EINTR), context cancellation/deadline via
// errors.Is, then the fs sentinel family, with EIO as the honest catch-all
// for anything unmapped (never success, never ENOENT).
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
	// formatting, status strings) still read as what they are. This stays
	// behind the errors.Is checks above: chained errors never reach the
	// string scan.
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
