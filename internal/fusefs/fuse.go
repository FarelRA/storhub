package fusefs

import (
	"context"
	"errors"
	"fmt"
	chunking "github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	"github.com/hanwen/go-fuse/v2/fuse"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
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

// mountLockFileName arbitrates exclusive ownership of a FUSE cache
// directory via flock(2); its content is diagnostic only (last holder's
// pid as decimal text).
const mountLockFileName = ".storhub-mount.lock"

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

// quarantine reasons recorded in manifests.
const (
	quarantineReasonCommitFailure = "commitfailure"
	quarantineReasonClose         = "closedirty"
	quarantineReasonStartupSweep  = "startupsweep"
)

// defaultCallerUmask is applied to creation modes because the FUSE
// protocol does not transmit the caller's umask. Without it,
// ApplyCreateMode would be a no-op and `touch` would create 0666
// (world-writable) files.
const defaultCallerUmask = 0o022

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
