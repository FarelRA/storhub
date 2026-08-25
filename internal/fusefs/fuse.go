package fusefs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"os"
	"path"
	"runtime"
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
)

var (
	notifyEntryFunc  = func(node *storhubNode, name string) { _ = node.NotifyEntry(name) }
	notifyDeleteFunc = func(parent *storhubNode, name string, child *storhubNode) {
		_ = parent.NotifyDelete(name, child.EmbeddedInode())
	}
)

type Options struct {
	EntryTimeout    time.Duration
	AttrTimeout     time.Duration
	NegativeTimeout time.Duration
	CleanupInterval time.Duration
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
	// protectedTemps holds paths of in-flight commit snapshots
	// (inode-commit-*, inode-ranges-*) so the janitor cannot unlink a
	// file that a network step is still consuming by path.
	protectedMu    sync.Mutex
	protectedTemps map[string]struct{}
	stopJanitor    chan struct{}
	janitorDone    chan struct{}
	closing        bool
	unmounted      bool
}

type lockRecord struct {
	owner uint64
	lock  fuse.FileLock
}

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

// dropAllLocksForInode removes every lock record for the inode and wakes
// blocked waiters. Called when the last open handle disappears.
func (s *Filesystem) dropAllLocksForInode(inode uint64) {
	s.lockCond.L.Lock()
	_, existed := s.lockTable[inode]
	delete(s.lockTable, inode)
	if existed {
		// A released lock may unblock F_SETLKW waiters.
		s.lockCond.Broadcast()
	}
	s.lockCond.L.Unlock()
}

type storhubHandle struct {
	fs    *Filesystem
	inode uint64
	id    uint64
	flags uint32

	// pinned holds the content identity captured at open time: the file
	// entry plus the chunk descriptors it referenced. The lazy read
	// fallback resolves bytes against it, so renames or unlinks after
	// open can never change what this handle returns (POSIX open
	// semantics). Pure metadata - a few dozen bytes even for large files;
	// no network at pin time.
	pinned *pinnedContent

	mu         sync.Mutex
	temp       *os.File
	tempPath   string
	path       string
	closed     bool
	deleted    bool
	owners     map[uint64]struct{}
	writeState *inodeWriteState
}

// pinnedContent is an immutable, self-contained view of one file's data
// layout. Chunk assets are content-addressed, so the descriptors stay
// valid for the handle's lifetime regardless of later metadata changes.
type pinnedContent struct {
	file   metadata.FileMeta
	chunks map[int64]metadata.ChunkInfo
}
type inodeWriteState struct {
	fs    *Filesystem
	inode uint64

	opMu              sync.Mutex
	mu                sync.Mutex
	temp              *os.File
	tempPath          string
	baseTemp          *os.File
	baseTempPath      string
	path              string
	closed            bool
	deleted           bool
	refs              int
	baseSize          int64
	logicalSize       int64
	dirtyRanges       []ByteRange
	tempAuthoritative bool
	pending           shfs.MetadataPatch
}

type ByteRange struct {
	Start int64
	End   int64
}

type writeBootstrap struct {
	baseSize int64
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

func DefaultOptions() Options {
	return Options{
		EntryTimeout:      60 * time.Second,
		AttrTimeout:       60 * time.Second,
		NegativeTimeout:   10 * time.Second,
		CleanupInterval:   30 * time.Second,
		OverlayBufferSize: defaultOverlayBufferSize,
		ExtraMountOpts:    []string{"noatime"},
		Debug:             true,
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
	SetXAttrContext(context.Context, string, string, string, []byte) error
	ListXAttrContext(context.Context, string, string) ([]string, error)
	RemoveXAttrContext(context.Context, string, string, string) error
	ApplyMetadataPatchContext(context.Context, string, string, shfs.MetadataPatch) error
	DownloadFileContext(context.Context, string, string, string) error
	ReadFileAtContext(context.Context, string, string, int64, int64) ([]byte, error)
	PatchFileContext(context.Context, string, string, int64, int64, []byte, ...shfs.MutateOption) (*metadata.FileMeta, error)
	ReplaceFileContext(context.Context, string, string, string, ...shfs.MutateOption) (*metadata.FileMeta, error)
	LoadRepoMetadataReadonlyContext(context.Context, string) (*metadata.RepoMetadata, string, error)
	ReadPinnedFileContext(context.Context, string, *metadata.FileMeta, map[int64]metadata.ChunkInfo, int64, int64) ([]byte, error)
	UpdateRepoMetadataContext(context.Context, string, func(*metadata.RepoMetadata) error, string) (*metadata.RepoMetadata, error)
	RewriteFileRangesWithMetadataContext(context.Context, string, string, string, *metadata.RepoMetadata, *metadata.FileMeta, int64, []ByteRange) (*metadata.FileMeta, error)
	RenameContext(context.Context, string, string, string) error
	Now() int64
	ChunkSize() int64
}

func New(hub Hub, project string, opts Options) (*Filesystem, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	defaults := DefaultOptions()
	if opts.EntryTimeout <= 0 {
		opts.EntryTimeout = defaults.EntryTimeout
	}
	if opts.AttrTimeout <= 0 {
		opts.AttrTimeout = defaults.AttrTimeout
	}
	if opts.NegativeTimeout <= 0 {
		opts.NegativeTimeout = defaults.NegativeTimeout
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = defaults.CleanupInterval
	}
	if opts.OverlayBufferSize <= 0 {
		opts.OverlayBufferSize = defaults.OverlayBufferSize
	}
	if len(opts.ExtraMountOpts) == 0 {
		opts.ExtraMountOpts = append([]string(nil), defaults.ExtraMountOpts...)
	}
	if opts.Logger == nil {
		opts.Logger = logging.WithComponent(logging.NewLogger(logging.Options{Level: logging.LevelDebug, Format: logging.FormatPretty, Color: true, Output: os.Stderr}), "fuse")
	}
	cacheDir := opts.CacheDir
	if strings.TrimSpace(cacheDir) == "" {
		cacheDir = path.Join(storcfg.CacheBase(), "fuse", project)
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create fuse cache dir: %w", err)
	}
	// Startup sweep: overlay temps from a crashed previous mount are
	// pure garbage (nothing can reference them before any file is
	// opened); the recovery/ quarantine is preserved by design.
	if entries, err := os.ReadDir(cacheDir); err == nil {
		for _, entry := range entries {
			name := entry.Name()
			if name == "recovery" || !strings.HasPrefix(name, "inode-") {
				continue
			}
			if err := os.RemoveAll(path.Join(cacheDir, name)); err != nil {
				logging.Warn(opts.Logger, "fuse startup sweep failed", "path", name, "err", err)
			}
		}
	}
	fsys := &Filesystem{
		hub:         hub,
		project:     project,
		opts:        opts,
		nodes:       make(map[uint64]*storhubNode),
		inodePaths:  map[uint64]map[string]struct{}{1: {"": {}}},
		pathToInode: map[string]uint64{"": 1},
		lockTable:   make(map[uint64][]lockRecord),
		writeStates: make(map[uint64]*inodeWriteState),
		handles:     make(map[uint64]*storhubHandle),
		cacheDir:    cacheDir,
		stopJanitor: make(chan struct{}),
		janitorDone: make(chan struct{}),
	}
	fsys.root = &storhubNode{fs: fsys, inode: 1, isDir: true}
	fsys.nodes[1] = fsys.root
	fsys.lockCond = sync.NewCond(&fsys.mu)
	go fsys.runJanitor()
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
	options.MaxReadAhead = int(s.hub.ChunkSize())
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
	if s.server != nil {
		s.server.Wait()
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
	close(s.stopJanitor)
	s.mu.Unlock()
	<-s.janitorDone
	_ = s.Unmount()
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
	// would silently discard acknowledged writes.
	for _, writeState := range writeStates {
		if writeState.hasUncommittedChanges() {
			writeState.quarantineTemps()
		}
	}
	for _, handle := range handles {
		handle.closeTemp()
	}
	s.debugf("close complete project=%s", s.project)
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

// quarantineFile moves tempPath into the recovery directory, where the cache
// janitor will not touch it. Called when a commit fails and the overlay holds
// the only copy of data the application has already written.
func (s *Filesystem) quarantineFile(tempPath string) {
	dir := s.recoveryDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		s.errorf("quarantine failed; dirty overlay left in cache path=%s mkdir err=%v", tempPath, err)
		return
	}
	target := path.Join(dir, fmt.Sprintf("%s.%d", path.Base(tempPath), time.Now().UnixNano()))
	if err := os.Rename(tempPath, target); err != nil {
		s.errorf("quarantine failed; dirty overlay left in cache path=%s rename err=%v", tempPath, err)
		return
	}
	s.errorf("commit failed; dirty overlay quarantined for manual recovery path=%s saved=%s", tempPath, target)
}

// soleWriteStateRef reports whether state is registered and held by exactly
// one open handle. Caller must not hold state.mu.
func (s *Filesystem) soleWriteStateRef(state *inodeWriteState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.writeStates[state.inode]
	return current == state && state.refs <= 1
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

func (s *Filesystem) writeStateForInode(inode uint64) *inodeWriteState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.writeStates[inode]
}

func (s *Filesystem) applyPendingSize(entry *shfs.EntryInfo) {
	if entry == nil {
		return
	}
	state := s.writeStateForInode(entry.Inode)
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleted {
		return
	}
	state.overlayEntryLocked(entry)
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
			// The path now belongs to a different inode. Detach like an
			// unlinked-but-open file: reads keep serving the handle's
			// own snapshot instead of silently switching to the
			// replacement's content.
			handle.path = ""
			handle.deleted = true
		}
		handle.mu.Unlock()
	}
	if writeState != nil {
		writeState.mu.Lock()
		if writeState.path == oldPath {
			writeState.path = ""
			writeState.deleted = true
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

func (s *Filesystem) notifyEntryForPath(dirPath, name string) {
	s.mu.RLock()
	node := s.nodeForPathLocked(dirPath)
	s.mu.RUnlock()
	safeNotifyEntry(node, name)
}

func (n *storhubNode) notifyEntry(name string) {
	n.fs.notifyEntryForPath(n.currentPath(), name)
}

func (n *storhubNode) notifyDelete(name string, childInode uint64) {
	n.fs.mu.RLock()
	child := n.fs.nodes[childInode]
	n.fs.mu.RUnlock()
	safeNotifyDelete(n, name, child)
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

func (s *Filesystem) callerContext(ctx context.Context) context.Context {
	ctx = shfs.WithSuppressedAtime(ctx)
	if caller, ok := fuse.FromContext(ctx); ok && caller != nil {
		return shfs.WithIdentity(ctx, shfs.Identity{UID: caller.Uid, GID: caller.Gid, PID: caller.Pid, Admin: caller.Uid == 0})
	}
	return ctx
}

func (n *storhubNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	childPath := path.Join(n.currentPath(), name)
	n.fs.debugf("lookup path=%s child=%s", n.currentPath(), name)
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if err != nil {
		if out != nil {
			out.SetEntryTimeout(n.fs.opts.NegativeTimeout)
		}
		return nil, errnoFromError(err)
	}
	n.fs.applyPendingSize(entry)
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	return ino, 0
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

func (n *storhubNode) Readdir(ctx context.Context) (gofusefs.DirStream, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	n.fs.debugf("readdir path=%s", n.currentPath())
	entries, err := n.fs.hub.ReadDirContext(ctx, n.fs.project, n.currentPath())
	if err != nil {
		return nil, errnoFromError(err)
	}
	result := make([]fuse.DirEntry, 0, len(entries)+2)
	result = append(result, fuse.DirEntry{Name: ".", Ino: n.inode, Mode: syscall.S_IFDIR})
	parentIno := uint64(1)
	if _, parent := n.Parent(); parent != nil {
		if ino := parent.StableAttr().Ino; ino != 0 {
			parentIno = ino
		}
	}
	result = append(result, fuse.DirEntry{Name: "..", Ino: parentIno, Mode: syscall.S_IFDIR})
	for _, entry := range entries {
		mode := uint32(syscall.S_IFREG)
		if entry.IsDir {
			mode = syscall.S_IFDIR
		} else if entry.IsSymlink {
			mode = syscall.S_IFLNK
		}
		result = append(result, fuse.DirEntry{Name: entry.Name, Ino: entry.Inode, Mode: mode})
	}
	return gofusefs.NewListDirStream(result), 0
}

func (n *storhubNode) Getattr(ctx context.Context, f gofusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	n.fs.debugf("getattr path=%s inode=%d", n.currentPath(), n.inode)
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, n.currentPath())
	if err != nil {
		return errnoFromError(err)
	}
	n.fs.applyPendingSize(entry)
	fillAttr(&out.Attr, entry)
	out.SetTimeout(n.fs.opts.AttrTimeout)
	_ = f
	return 0
}

func (n *storhubNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	stats, err := n.fs.hub.StatFSContext(ctx, n.fs.project)
	if err != nil {
		return errnoFromError(err)
	}
	out.Files = uint64(stats.Inodes)
	out.Bfree = uint64(1 << 30)
	out.Bavail = out.Bfree
	out.Blocks = uint64(maxInt64(stats.Bytes/4096+1, 1))
	out.Bsize = 4096
	out.NameLen = 255
	out.Frsize = 4096
	return 0
}

func (n *storhubNode) Open(ctx context.Context, flags uint32) (gofusefs.FileHandle, uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetPath := n.currentPath()
	n.fs.debugf("open start path=%s inode=%d flags=%#x", targetPath, n.inode, flags)
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, n.currentPath())
	if err != nil {
		return nil, 0, errnoFromError(err)
	}
	if entry.IsDir {
		return nil, 0, syscall.EISDIR
	}
	if entry.IsSymlink {
		return nil, 0, syscall.ELOOP
	}
	// Pin the content layout at open time from the (cached) readonly
	// metadata view: a file entry clone plus the chunk descriptors it
	// references. Pure metadata copying - no network beyond what the
	// stat already did. If the file vanished between stat and pin, the
	// open fails with ENOENT rather than degrading to path-live reads,
	// which would reintroduce the rename-over race this pin prevents.
	repoMeta, _, metaErr := n.fs.hub.LoadRepoMetadataReadonlyContext(ctx, n.fs.project)
	if metaErr != nil {
		return nil, 0, errnoFromError(metaErr)
	}
	file := repoMeta.FindFile(targetPath)
	if file == nil {
		return nil, 0, syscall.ENOENT
	}
	pin := &pinnedContent{
		file:   file.Clone(),
		chunks: make(map[int64]metadata.ChunkInfo, len(file.Chunks)),
	}
	for _, id := range file.Chunks {
		if chunk, ok := repoMeta.Chunks[id]; ok {
			pin.chunks[id] = chunk
		}
	}
	h, err := n.fs.newHandle(ctx, n.inode, targetPath, flags, nil)
	if err != nil {
		return nil, 0, errnoFromError(err)
	}
	h.pinned = pin
	if err != nil {
		return nil, 0, errnoFromError(err)
	}
	n.fs.debugf("open path=%s inode=%d flags=%#x", targetPath, n.inode, flags)
	return h, 0, 0
}

func (n *storhubNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	n.fs.debugf("access path=%s inode=%d mask=%#x", n.currentPath(), n.inode, mask)
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, n.currentPath())
	if err != nil {
		return errnoFromError(err)
	}
	need := 0
	if mask&0x4 != 0 {
		need |= shfs.AccessRead
	}
	if mask&0x2 != 0 {
		need |= shfs.AccessWrite
	}
	if mask&0x1 != 0 {
		need |= shfs.AccessExec
	}
	if need == 0 {
		return 0
	}
	id := shfs.IdentityFromContext(ctx)
	if err := shfs.CanAccessEntry(id, entry, need); err != nil {
		return errnoFromError(err)
	}
	return 0
}

func (n *storhubNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*gofusefs.Inode, gofusefs.FileHandle, uint32, syscall.Errno) {
	ctx = shfs.WithCreateMode(n.fs.callerContext(ctx), mode)
	childPath := path.Join(n.currentPath(), name)
	n.fs.debugf("create start path=%s flags=%#x mode=%#o", childPath, flags, mode)
	file, err := n.fs.hub.CreateFileContext(ctx, n.fs.project, childPath)
	if err != nil {
		n.fs.debugf("create failed path=%s step=create err=%v", childPath, err)
		return nil, nil, 0, errnoFromError(err)
	}
	nlink := n.fs.nlinkForEntry(childPath)
	entry := entryInfoFromFile(file, childPath, nlink)
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	h, err := n.fs.newHandle(ctx, entry.Inode, childPath, flags, &writeBootstrap{baseSize: entry.Size})
	if err != nil {
		n.fs.debugf("create failed path=%s step=open-handle err=%v", childPath, err)
		return nil, nil, 0, errnoFromError(err)
	}
	n.fs.debugf("create path=%s inode=%d flags=%#x mode=%#o", childPath, entry.Inode, flags, mode)
	return ino, h, 0, 0
}

func (n *storhubNode) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	_ = dev
	switch mode & syscall.S_IFMT {
	case 0, syscall.S_IFREG:
		inode, _, _, errno := n.Create(ctx, name, syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY, mode&0o7777, out)
		return inode, errno
	case syscall.S_IFIFO, syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFSOCK:
		return nil, syscall.ENOTSUP
	default:
		return nil, syscall.ENOTSUP
	}
}

func (n *storhubNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	ctx = shfs.WithCreateMode(n.fs.callerContext(ctx), mode)
	childPath := path.Join(n.currentPath(), name)
	if err := n.fs.hub.MkdirContext(ctx, n.fs.project, childPath); err != nil {
		return nil, errnoFromError(err)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if err != nil {
		return nil, errnoFromError(err)
	}
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	n.fs.debugf("mkdir path=%s mode=%#o", childPath, mode)
	return ino, 0
}

func (n *storhubNode) Unlink(ctx context.Context, name string) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	childPath := path.Join(n.currentPath(), name)
	entry, _ := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if entry != nil {
		if err := n.fs.materializeHandlesForPath(ctx, entry.Inode, childPath); err != nil {
			return errnoFromError(err)
		}
	}
	if err := n.fs.hub.UnlinkContext(ctx, n.fs.project, childPath); err != nil {
		return errnoFromError(err)
	}
	if entry != nil {
		remaining := n.fs.dropPath(entry.Inode, childPath)
		n.fs.rebindHandlesAfterPathChange(entry.Inode, childPath, remaining)
		n.notifyDelete(name, entry.Inode)
	} else {
		n.notifyEntry(name)
	}
	n.fs.debugf("unlink path=%s", childPath)
	return 0
}

func (n *storhubNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	childPath := path.Join(n.currentPath(), name)
	entry, _ := n.fs.hub.StatPathContext(ctx, n.fs.project, childPath)
	if err := n.fs.hub.RmdirContext(ctx, n.fs.project, childPath); err != nil {
		return errnoFromError(err)
	}
	if entry != nil {
		n.fs.dropPath(entry.Inode, childPath)
		n.notifyDelete(name, entry.Inode)
	} else {
		n.notifyEntry(name)
	}
	n.fs.debugf("rmdir path=%s", childPath)
	return 0
}

func (n *storhubNode) Rename(ctx context.Context, name string, newParent gofusefs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	oldPath := path.Join(n.currentPath(), name)
	parentNode, ok := newParent.(*storhubNode)
	if !ok {
		return syscall.EINVAL
	}
	newPath := path.Join(parentNode.currentPath(), newName)
	if flags&renameExchange != 0 || flags&renameWhiteout != 0 {
		return syscall.EINVAL
	}
	oldEntry, _ := n.fs.hub.StatPathContext(ctx, n.fs.project, oldPath)
	newEntry, _ := n.fs.hub.StatPathContext(ctx, n.fs.project, newPath)
	// Rename semantics (including POSIX replacement) live in exactly one
	// place: the fs service behind the hub.
	if flags&renameNoReplace != 0 && newEntry != nil {
		return syscall.EEXIST
	}
	// A handle open on the replaced target must keep serving its own
	// snapshot: materialize before the metadata swap removes the path.
	if newEntry != nil && (oldEntry == nil || newEntry.Inode != oldEntry.Inode) {
		if err := n.fs.materializeHandlesForPath(ctx, newEntry.Inode, newPath); err != nil {
			return errnoFromError(err)
		}
	}
	if err := n.fs.hub.RenameContext(ctx, n.fs.project, oldPath, newPath); err != nil {
		return errnoFromError(err)
	}
	if oldEntry != nil {
		n.fs.remapPaths(oldPath, newPath)
	}
	if newEntry != nil && (oldEntry == nil || newEntry.Inode != oldEntry.Inode) {
		remaining := n.fs.dropPath(newEntry.Inode, newPath)
		n.fs.rebindHandlesAfterPathChange(newEntry.Inode, newPath, remaining)
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, newPath)
	if err == nil {
		n.fs.rememberPath(entry.Inode, newPath)
	}
	n.fs.debugf("rename old=%s new=%s flags=%#x", oldPath, newPath, flags)
	return 0
}

func (n *storhubNode) Setattr(ctx context.Context, f gofusefs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	targetPath := n.currentPath()
	usedLocalSize := false
	localSize := int64(0)
	state := n.fs.writeStateForInode(n.inode)
	if handle, ok := f.(*storhubHandle); ok && handle.writeState != nil {
		state = handle.writeState
	}
	if size, ok := in.GetSize(); ok && !n.isDir {
		if state != nil {
			state.opMu.Lock()
			state.mu.Lock()
			err := state.setSizeLocked(int64(size))
			if err == nil {
				usedLocalSize = true
				localSize = state.logicalSize
			}
			state.mu.Unlock()
			state.opMu.Unlock()
			if err != nil {
				return errnoFromError(err)
			}
		} else {
			if _, err := n.fs.hub.TruncateFileContext(ctx, n.fs.project, targetPath, int64(size)); err != nil {
				return errnoFromError(err)
			}
		}
	}
	if mode, ok := in.GetMode(); ok {
		if state != nil && !n.isDir {
			state.opMu.Lock()
			state.mu.Lock()
			state.pending.HasMode = true
			state.pending.Mode = mode & 0o7777
			state.mu.Unlock()
			state.opMu.Unlock()
		} else {
			if err := n.fs.hub.ChmodContext(ctx, n.fs.project, targetPath, mode&0o7777); err != nil {
				return errnoFromError(err)
			}
		}
	}
	uid, uidOK := in.GetUID()
	gid, gidOK := in.GetGID()
	if uidOK || gidOK {
		entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
		if err != nil {
			return errnoFromError(err)
		}
		if state != nil && !n.isDir {
			state.opMu.Lock()
			state.mu.Lock()
			state.overlayEntryLocked(entry)
			if !uidOK {
				uid = entry.UID
			}
			if !gidOK {
				gid = entry.GID
			}
			state.pending.HasOwner = true
			state.pending.UID = uid
			state.pending.GID = gid
			state.mu.Unlock()
			state.opMu.Unlock()
		} else {
			if !uidOK {
				uid = entry.UID
			}
			if !gidOK {
				gid = entry.GID
			}
			if err := n.fs.hub.ChownContext(ctx, n.fs.project, targetPath, uid, gid); err != nil {
				return errnoFromError(err)
			}
		}
	}
	atime, atimeOK := in.GetATime()
	mtime, mtimeOK := in.GetMTime()
	if atimeOK || mtimeOK {
		entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
		if err != nil {
			return errnoFromError(err)
		}
		if !atimeOK {
			atime = time.Unix(entry.AccessedAt, 0)
		}
		if !mtimeOK {
			mtime = time.Unix(entry.ModifiedAt, 0)
		}
		if state != nil && !n.isDir {
			state.opMu.Lock()
			state.mu.Lock()
			state.overlayEntryLocked(entry)
			if !atimeOK {
				atime = time.Unix(entry.AccessedAt, 0)
			}
			if !mtimeOK {
				mtime = time.Unix(entry.ModifiedAt, 0)
			}
			state.pending.HasTimes = true
			state.pending.ATime = atime
			state.pending.MTime = mtime
			state.mu.Unlock()
			state.opMu.Unlock()
		} else {
			// go-fuse already resolves UTIME_NOW to time.Now(); ok=false
			// is UTIME_OMIT. Passing pointers preserves explicit epoch
			// timestamps that ChtimesContext's omit-on-zero would rewrite.
			var atimePtr, mtimePtr *time.Time
			if atimeOK {
				t := atime
				atimePtr = &t
			}
			if mtimeOK {
				t := mtime
				mtimePtr = &t
			}
			if err := n.fs.hub.ChtimesExplicitContext(ctx, n.fs.project, targetPath, atimePtr, mtimePtr); err != nil {
				return errnoFromError(err)
			}
		}
	}
	entry, err := n.fs.hub.StatPathContext(ctx, n.fs.project, targetPath)
	if err != nil {
		return errnoFromError(err)
	}
	if usedLocalSize {
		entry.Size = localSize
	} else {
		n.fs.applyPendingSize(entry)
	}
	if state != nil && !n.isDir {
		state.mu.Lock()
		state.overlayEntryLocked(entry)
		state.mu.Unlock()
	}
	fillAttr(&out.Attr, entry)
	out.SetTimeout(n.fs.opts.AttrTimeout)
	n.fs.debugf("setattr path=%s valid=%#x", targetPath, in.Valid)
	_ = f
	return 0
}

func (n *storhubNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	childPath := path.Join(n.currentPath(), name)
	file, err := n.fs.hub.SymlinkContext(ctx, n.fs.project, target, childPath)
	if err != nil {
		return nil, errnoFromError(err)
	}
	nlink := n.fs.nlinkForEntry(childPath)
	entry := entryInfoFromFile(file, childPath, nlink)
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	return ino, 0
}

func (n *storhubNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	target, err := n.fs.hub.ReadlinkContext(ctx, n.fs.project, n.currentPath())
	if err != nil {
		return nil, errnoFromError(err)
	}
	return []byte(target), 0
}

func (n *storhubNode) Link(ctx context.Context, target gofusefs.InodeEmbedder, name string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	targetNode, ok := target.(*storhubNode)
	if !ok {
		return nil, syscall.EINVAL
	}
	linked, err := n.fs.hub.LinkContext(ctx, n.fs.project, targetNode.currentPath(), path.Join(n.currentPath(), name))
	if err != nil {
		return nil, errnoFromError(err)
	}
	linkPath := path.Join(n.currentPath(), name)
	nlink := n.fs.nlinkForEntry(linkPath)
	entry := entryInfoFromFile(linked, linkPath, nlink)
	child := n.fs.ensureNode(ctx, entry)
	ino := n.attachChild(ctx, child)
	fillEntryOut(out, entry, n.fs.opts)
	return ino, 0
}

func (n *storhubNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	data, err := n.fs.hub.GetXAttrContext(ctx, n.fs.project, n.currentPath(), attr)
	if err != nil {
		return 0, errnoFromError(err)
	}
	if len(dest) == 0 {
		return uint32(len(data)), 0
	}
	if len(dest) < len(data) {
		return uint32(len(data)), syscall.ERANGE
	}
	copy(dest, data)
	return uint32(len(data)), 0
}

func (n *storhubNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	if flags != 0 {
		_, err := n.fs.hub.GetXAttrContext(ctx, n.fs.project, n.currentPath(), attr)
		exists := err == nil
		if err != nil && errnoFromError(err) != syscall.ENODATA {
			return errnoFromError(err)
		}
		if flags&xattrCreate != 0 && exists {
			return syscall.EEXIST
		}
		if flags&xattrReplace != 0 && !exists {
			return syscall.ENODATA
		}
	}
	if err := n.fs.hub.SetXAttrContext(ctx, n.fs.project, n.currentPath(), attr, data); err != nil {
		return errnoFromError(err)
	}
	return 0
}

func (n *storhubNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	ctx = n.fs.callerContext(ctx)
	attrs, err := n.fs.hub.ListXAttrContext(ctx, n.fs.project, n.currentPath())
	if err != nil {
		return 0, errnoFromError(err)
	}
	payload := []byte(strings.Join(attrs, "\x00"))
	if len(attrs) > 0 {
		payload = append(payload, 0)
	}
	if len(dest) == 0 {
		return uint32(len(payload)), 0
	}
	if len(dest) < len(payload) {
		return uint32(len(payload)), syscall.ERANGE
	}
	copy(dest, payload)
	return uint32(len(payload)), 0
}

func (n *storhubNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	ctx = n.fs.callerContext(ctx)
	if err := n.fs.hub.RemoveXAttrContext(ctx, n.fs.project, n.currentPath(), attr); err != nil {
		return errnoFromError(err)
	}
	return 0
}

func (s *Filesystem) newHandle(ctx context.Context, inode uint64, targetPath string, flags uint32, bootstrap *writeBootstrap) (*storhubHandle, error) {
	if strings.TrimSpace(targetPath) == "" {
		targetPath = s.pathForInode(inode)
	}
	h := &storhubHandle{fs: s, inode: inode, flags: flags, id: s.nextHandle.Add(1), path: targetPath, owners: make(map[uint64]struct{})}
	s.mu.Lock()
	s.handles[h.id] = h
	s.mu.Unlock()
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_TRUNC) != 0 {
		writeState, err := s.acquireWriteState(ctx, inode, h.path, bootstrap)
		if err != nil {
			h.closeTemp()
			s.mu.Lock()
			delete(s.handles, h.id)
			s.mu.Unlock()
			return nil, err
		}
		h.writeState = writeState
		if flags&syscall.O_TRUNC != 0 && bootstrap == nil {
			writeState.mu.Lock()
			if err := writeState.setSizeLocked(0); err != nil {
				writeState.mu.Unlock()
				s.releaseWriteState(writeState)
				s.mu.Lock()
				delete(s.handles, h.id)
				s.mu.Unlock()
				return nil, err
			}
			writeState.mu.Unlock()
		}
	}
	if h.writeState == nil {
		h.writeState = s.attachWriteState(inode)
	}
	return h, nil
}

func (h *storhubHandle) materializePath(ctx context.Context, targetPath string) error {
	h.mu.Lock()
	if h.temp != nil {
		h.mu.Unlock()
		return nil
	}
	temp, err := os.CreateTemp(h.fs.cacheDir, "handle-*")
	if err != nil {
		h.mu.Unlock()
		return err
	}
	h.temp = temp
	h.tempPath = temp.Name()
	h.path = targetPath
	h.mu.Unlock()

	// Network calls without lock
	entry, err := h.fs.hub.StatPathContext(ctx, h.fs.project, targetPath)
	if err != nil {
		// Propagate: silently treating stat failure as "empty file"
		// would serve EOF for a readable handle whose remote stat
		// merely hiccupped (same contract as the writeState twin).
		if rmErr := temp.Close(); rmErr != nil {
			h.fs.errorf("materialize cleanup failed path=%s temp=%s err=%v", targetPath, temp.Name(), rmErr)
		}
		if rmErr := os.Remove(temp.Name()); rmErr != nil {
			h.fs.errorf("materialize cleanup failed path=%s temp=%s err=%v", targetPath, temp.Name(), rmErr)
		}
		h.mu.Lock()
		h.temp = nil
		h.tempPath = ""
		h.mu.Unlock()
		return err
	}
	if entry.Size > 0 {
		if dlErr := h.fs.hub.DownloadFileContext(ctx, h.fs.project, targetPath, temp.Name()); dlErr != nil {
			if err := temp.Close(); err != nil {
				logging.Error(nil, "failed to close temp file after download error", "path", temp.Name(), "err", err)
			}
			if err := os.Remove(temp.Name()); err != nil {
				logging.Error(nil, "failed to remove temp file after download error", "path", temp.Name(), "err", err)
			}
			h.mu.Lock()
			h.temp = nil
			h.tempPath = ""
			h.mu.Unlock()
			return dlErr
		}
		h.mu.Lock()
		if _, seekErr := h.temp.Seek(0, 0); seekErr != nil {
			h.mu.Unlock()
			return seekErr
		}
		h.mu.Unlock()
	}
	return nil
}

func (s *Filesystem) attachWriteState(inode uint64) *inodeWriteState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ws := s.writeStates[inode]; ws != nil {
		ws.refs++
		return ws
	}
	return nil
}

func (s *Filesystem) acquireWriteState(ctx context.Context, inode uint64, targetPath string, bootstrap *writeBootstrap) (*inodeWriteState, error) {
	s.mu.Lock()
	if existing := s.writeStates[inode]; existing != nil {
		existing.refs++
		s.mu.Unlock()
		return existing, nil
	}
	state := &inodeWriteState{fs: s, inode: inode, path: targetPath, refs: 1}
	s.writeStates[inode] = state
	s.mu.Unlock()
	var err error
	if bootstrap != nil {
		err = state.materializeBootstrap(bootstrap.baseSize)
	} else {
		err = state.materialize(ctx)
	}
	if err != nil {
		s.mu.Lock()
		if s.writeStates[inode] == state {
			delete(s.writeStates, inode)
		}
		s.mu.Unlock()
		state.closeTemp()
		return nil, err
	}
	return state, nil
}

func (w *inodeWriteState) materializeBootstrap(size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.temp != nil {
		return nil
	}
	temp, err := os.CreateTemp(w.fs.cacheDir, "inode-*")
	if err != nil {
		return err
	}
	w.temp = temp
	w.tempPath = temp.Name()
	w.baseSize = size
	w.logicalSize = size
	w.tempAuthoritative = size == 0
	if err := w.temp.Truncate(size); err != nil {
		return err
	}
	return nil
}

func (s *Filesystem) releaseWriteState(state *inodeWriteState) {
	if state == nil {
		return
	}
	shouldClose := false
	s.mu.Lock()
	if current := s.writeStates[state.inode]; current == state {
		state.refs--
		if state.refs <= 0 {
			delete(s.writeStates, state.inode)
			shouldClose = true
		}
	}
	s.mu.Unlock()
	if shouldClose {
		state.closeTemp()
	}
}

func (w *inodeWriteState) materialize(ctx context.Context) error {
	w.mu.Lock()
	if w.temp != nil {
		w.mu.Unlock()
		return nil
	}
	path := w.path
	temp, err := os.CreateTemp(w.fs.cacheDir, "inode-*")
	if err != nil {
		w.mu.Unlock()
		return err
	}
	tempPath := temp.Name()
	w.temp = temp
	w.tempPath = tempPath
	w.mu.Unlock()

	// Network call without lock
	entry, err := w.fs.hub.StatPathContext(ctx, w.fs.project, path)
	if err != nil {
		// Propagate: silently treating stat failure as "empty file"
		// would swap an open handle's content to EOF.
		if rmErr := os.Remove(tempPath); rmErr != nil {
			w.fs.errorf("materialize cleanup failed path=%s temp=%s err=%v", path, tempPath, rmErr)
		}
		w.mu.Lock()
		w.temp = nil
		w.tempPath = ""
		w.mu.Unlock()
		return err
	}
	w.mu.Lock()
	w.baseSize = entry.Size
	w.logicalSize = entry.Size
	truncErr := w.temp.Truncate(entry.Size)
	w.mu.Unlock()
	if truncErr != nil {
		return truncErr
	}
	return nil
}

func (w *inodeWriteState) snapshotBaseLocked(ctx context.Context, targetPath string) error {
	if w.baseTemp != nil {
		return nil
	}
	baseTemp, err := os.CreateTemp(w.fs.cacheDir, "inode-base-*")
	if err != nil {
		return err
	}
	baseTempPath := baseTemp.Name()

	// Release lock before network calls
	w.mu.Unlock()
	entry, statErr := w.fs.hub.StatPathContext(ctx, w.fs.project, targetPath)
	if statErr == nil && entry.Size > 0 {
		if dlErr := w.fs.hub.DownloadFileContext(ctx, w.fs.project, targetPath, baseTempPath); dlErr != nil {
			_ = baseTemp.Close()
			_ = os.Remove(baseTempPath)
			w.mu.Lock()
			return dlErr
		}
	}
	w.mu.Lock()
	// Re-check: another goroutine may have already set this
	if w.baseTemp != nil {
		if err := baseTemp.Close(); err != nil {
			logging.Error(nil, "failed to close base snapshot temp (duplicate)", "path", baseTempPath, "err", err)
		}
		if err := os.Remove(baseTempPath); err != nil {
			logging.Error(nil, "failed to remove base snapshot temp (duplicate)", "path", baseTempPath, "err", err)
		}
		return nil
	}
	w.baseTemp = baseTemp
	w.baseTempPath = baseTempPath
	return nil
}

func (w *inodeWriteState) clearBaseSnapshotLocked() {
	if w.baseTemp != nil {
		if err := w.baseTemp.Close(); err != nil {
			logging.Error(nil, "failed to close base snapshot temp", "err", err)
		}
		w.baseTemp = nil
	}
	if w.baseTempPath != "" {
		if err := os.Remove(w.baseTempPath); err != nil {
			logging.Error(nil, "failed to remove base snapshot temp", "path", w.baseTempPath, "err", err)
		}
		w.baseTempPath = ""
	}
}

func (w *inodeWriteState) refreshBaseSnapshotLocked() error {
	if w.baseTemp == nil {
		return nil
	}
	if err := w.baseTemp.Truncate(w.logicalSize); err != nil {
		return err
	}
	buf := make([]byte, w.fs.copyPageSize())
	for _, dirty := range w.dirtyRanges {
		for offset := dirty.Start; offset < dirty.End; {
			want := int64(len(buf))
			if remaining := dirty.End - offset; want > remaining {
				want = remaining
			}
			n, err := w.temp.ReadAt(buf[:want], offset)
			if err != nil && err != io.EOF {
				return err
			}
			if n == 0 {
				return io.ErrNoProgress
			}
			if _, err := w.baseTemp.WriteAt(buf[:n], offset); err != nil {
				return err
			}
			offset += int64(n)
		}
	}
	return nil
}

func (w *inodeWriteState) markDirtyLocked(start, end int64) {
	if start < 0 {
		start = 0
	}
	if end <= start {
		return
	}
	merged := make([]ByteRange, 0, len(w.dirtyRanges)+1)
	inserted := false
	for _, existing := range w.dirtyRanges {
		if existing.End < start {
			merged = append(merged, existing)
			continue
		}
		if end < existing.Start {
			if !inserted {
				merged = append(merged, ByteRange{Start: start, End: end})
				inserted = true
			}
			merged = append(merged, existing)
			continue
		}
		if existing.Start < start {
			start = existing.Start
		}
		if existing.End > end {
			end = existing.End
		}
	}
	if !inserted {
		merged = append(merged, ByteRange{Start: start, End: end})
	}
	w.dirtyRanges = merged
}

func (w *inodeWriteState) truncateDirtyRangesLocked(size int64) {
	trimmed := make([]ByteRange, 0, len(w.dirtyRanges))
	for _, existing := range w.dirtyRanges {
		if existing.Start >= size {
			continue
		}
		if existing.End > size {
			existing.End = size
		}
		trimmed = append(trimmed, existing)
	}
	w.dirtyRanges = trimmed
}

func (w *inodeWriteState) coversRangeLocked(start, end int64) bool {
	if start >= end {
		return true
	}
	covered := start
	for _, existing := range w.dirtyRanges {
		if existing.End <= covered {
			continue
		}
		if existing.Start > covered {
			return false
		}
		covered = existing.End
		if covered >= end {
			return true
		}
	}
	return covered >= end
}

func (w *inodeWriteState) setSizeLocked(size int64) error {
	if size < 0 {
		return syscall.EINVAL
	}
	if w.temp == nil {
		if err := w.ensureTempLocked(); err != nil {
			return err
		}
	}
	oldSize := w.logicalSize
	w.logicalSize = size
	if err := w.temp.Truncate(size); err != nil {
		return err
	}
	if size == 0 {
		w.tempAuthoritative = true
		w.clearBaseSnapshotLocked()
	} else if size > oldSize && !w.tempAuthoritative {
		// Regrown bytes must read as zeros (POSIX); the temp file may
		// hold stale data left over from before a shrink. Zero-fill the
		// regrown region and mark it dirty so the commit uploads it.
		buf := make([]byte, 32*1024)
		for offset := oldSize; offset < size; {
			n := int64(len(buf))
			if remaining := size - offset; remaining < n {
				n = remaining
			}
			if _, err := w.temp.WriteAt(buf[:n], offset); err != nil {
				return err
			}
			offset += n
		}
		w.markDirtyLocked(oldSize, size)
	}
	w.truncateDirtyRangesLocked(size)
	return nil
}

func (w *inodeWriteState) ensureTempLocked() error {
	if w.temp != nil {
		return nil
	}
	temp, err := os.CreateTemp(w.fs.cacheDir, "inode-*")
	if err != nil {
		return err
	}
	w.temp = temp
	w.tempPath = temp.Name()
	if err := w.temp.Truncate(w.logicalSize); err != nil {
		return err
	}
	return nil
}

// readIntoLocked fills dest with the file content visible at off, serving
// dirty ranges from the overlay temp file and clean ranges from the base
// snapshot or the hub. Caller must hold w.mu.
func (w *inodeWriteState) readIntoLocked(ctx context.Context, dest []byte, off int64) (int, error) {
	if off >= w.logicalSize || len(dest) == 0 {
		return 0, nil
	}
	limit := int64(len(dest))
	if max := w.logicalSize - off; limit > max {
		limit = max
	}
	filled := int64(0)
	visibleBaseSize := minInt64(w.baseSize, w.logicalSize)
	for filled < limit {
		segmentStart := off + filled
		dirty, dirtyRange := w.nextDirtyRangeLocked(segmentStart)
		if dirty {
			chunkEnd := dirtyRange.End
			if chunkEnd > off+limit {
				chunkEnd = off + limit
			}
			if w.temp == nil {
				if err := w.ensureTempLocked(); err != nil {
					return 0, err
				}
			}
			n, err := w.temp.ReadAt(dest[filled:filled+(chunkEnd-segmentStart)], segmentStart)
			if err != nil && !errors.Is(err, io.EOF) {
				return int(filled), err
			}
			filled += int64(n)
			if int64(n) < chunkEnd-segmentStart {
				for i := filled; i < chunkEnd-off; i++ {
					dest[i] = 0
				}
				filled = chunkEnd - off
			}
			continue
		}
		cleanEnd := off + limit
		if dirtyRange.Start >= 0 && dirtyRange.Start < cleanEnd {
			cleanEnd = dirtyRange.Start
		}
		if segmentStart >= visibleBaseSize {
			for i := filled; i < cleanEnd-off; i++ {
				dest[i] = 0
			}
			filled = cleanEnd - off
			continue
		}
		readEnd := cleanEnd
		if readEnd > visibleBaseSize {
			readEnd = visibleBaseSize
		}
		if w.baseTemp != nil {
			n, err := w.baseTemp.ReadAt(dest[filled:filled+(readEnd-segmentStart)], segmentStart)
			if err != nil && !errors.Is(err, io.EOF) {
				return int(filled), err
			}
			if n == 0 {
				return int(filled), io.ErrNoProgress
			}
			filled += int64(n)
			continue
		}
		data, err := w.readBaseRangeLocked(ctx, segmentStart, readEnd-segmentStart)
		if err != nil {
			return int(filled), err
		}
		if len(data) == 0 {
			return int(filled), io.ErrNoProgress
		}
		copy(dest[filled:], data)
		filled += int64(len(data))
	}
	return int(filled), nil
}

func (w *inodeWriteState) readBaseRangeLocked(ctx context.Context, offset, length int64) ([]byte, error) {
	if length <= 0 {
		return []byte{}, nil
	}
	path := w.path
	w.mu.Unlock()
	data, err := w.fs.hub.ReadFileAtContext(shfs.WithSuppressedAtime(ctx), w.fs.project, path, offset, length)
	w.mu.Lock()
	return data, err
}

func (w *inodeWriteState) nextDirtyRangeLocked(offset int64) (bool, ByteRange) {
	for _, existing := range w.dirtyRanges {
		if offset >= existing.Start && offset < existing.End {
			return true, existing
		}
		if existing.Start > offset {
			return false, existing
		}
	}
	return false, ByteRange{Start: -1, End: -1}
}

func (w *inodeWriteState) dirtyBytesLocked() int64 {
	total := int64(0)
	for _, dirty := range w.dirtyRanges {
		total += dirty.End - dirty.Start
	}
	return total
}

func (w *inodeWriteState) hasPendingMetadataLocked() bool {
	return w.pending.HasMode || w.pending.HasOwner || w.pending.HasTimes
}

func (w *inodeWriteState) overlayEntryLocked(entry *shfs.EntryInfo) {
	if entry == nil {
		return
	}
	if w.pending.HasMode {
		entry.Mode = w.pending.Mode
	}
	if w.pending.HasOwner {
		entry.UID = w.pending.UID
		entry.GID = w.pending.GID
	}
	if w.pending.HasTimes {
		entry.AccessedAt = w.pending.ATime.Unix()
		entry.ModifiedAt = w.pending.MTime.Unix()
	}
	entry.Size = w.logicalSize
	entry.ChangedAt = max(entry.ChangedAt, w.fs.hub.Now())
}

func (w *inodeWriteState) plannedRangesLocked() []ByteRange {
	chunkSize := normalizedChunkSize(w.fs.hub.ChunkSize())
	planned := make([]ByteRange, 0, len(w.dirtyRanges))
	for _, dirty := range w.dirtyRanges {
		start := dirty.Start
		if start < w.baseSize {
			start = (start / chunkSize) * chunkSize
		}
		end := dirty.End
		if end < w.baseSize {
			end = ((end + chunkSize - 1) / chunkSize) * chunkSize
			if end > w.baseSize {
				end = w.baseSize
			}
		}
		if end < dirty.End {
			end = dirty.End
		}
		planned = mergeByteRange(planned, ByteRange{Start: start, End: end})
	}
	return planned
}

func mergeByteRange(existing []ByteRange, next ByteRange) []ByteRange {
	if next.End <= next.Start {
		return existing
	}
	merged := make([]ByteRange, 0, len(existing)+1)
	inserted := false
	for _, current := range existing {
		if current.End < next.Start {
			merged = append(merged, current)
			continue
		}
		if next.End < current.Start {
			if !inserted {
				merged = append(merged, next)
				inserted = true
			}
			merged = append(merged, current)
			continue
		}
		if current.Start < next.Start {
			next.Start = current.Start
		}
		if current.End > next.End {
			next.End = current.End
		}
	}
	if !inserted {
		merged = append(merged, next)
	}
	return merged
}

func totalByteRanges(ranges []ByteRange) int64 {
	total := int64(0)
	for _, dirty := range ranges {
		total += dirty.End - dirty.Start
	}
	return total
}

// shouldReplaceLocked decides whether pending writes escalate to a
// full-file replace (one atomic remote rewrite) instead of ranged patching.
//
// The ladder, checked top to bottom — every arm is live policy, tuned for
// when ranged patching costs more than replacing:
//
//   - dirty ≥ 75% of the file: patching would rewrite most of the file a
//     range at a time; replace once instead.
//   - ≥12 dirty ranges covering ≥ 1/3 of the file: per-range overhead
//     (one metadata commit each) dominates at this fragmentation.
//   - dirty ≥ 50% of the file: half the file rewritten is the break-even
//     point where replace wins unconditionally.
func (w *inodeWriteState) shouldReplaceLocked(planned []ByteRange) bool {
	fileSize := maxInt64(w.baseSize, w.logicalSize)
	if fileSize == 0 {
		return false
	}
	dirtyBytes := w.dirtyBytesLocked()
	if dirtyBytes*4 >= fileSize*3 {
		return true
	}
	// Note: an earlier draft also had "planned >= 24 && dirtyBytes*2 >=
	// fileSize"; it was strictly subsumed by the final arm and removed.
	if len(planned) >= 12 && dirtyBytes*3 >= fileSize {
		return true
	}
	if dirtyBytes*2 >= fileSize {
		return true
	}
	return false
}

func (w *inodeWriteState) shouldChunkRewriteLocked(planned []ByteRange) bool {
	if len(planned) == 0 {
		return false
	}
	if len(planned) >= 4 {
		return true
	}
	if len(planned) >= 2 && totalByteRanges(planned) < maxInt64(w.baseSize, w.logicalSize)/2 {
		return true
	}
	return false
}

func (w *inodeWriteState) writeRangeToLocked(ctx context.Context, out *os.File, start, end int64) error {
	buf := make([]byte, w.fs.copyPageSize())
	for offset := start; offset < end; {
		want := int64(len(buf))
		if remaining := end - offset; want > remaining {
			want = remaining
		}
		n, err := w.readIntoLocked(ctx, buf[:want], offset)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		if _, err := out.WriteAt(buf[:n], offset); err != nil {
			return err
		}
		offset += int64(n)
	}
	return nil
}

func (w *inodeWriteState) writeWorkingRangeToLocked(out *os.File, start, end int64) error {
	buf := make([]byte, w.fs.copyPageSize())
	for offset := start; offset < end; {
		want := int64(len(buf))
		if remaining := end - offset; want > remaining {
			want = remaining
		}
		n, err := w.temp.ReadAt(buf[:want], offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			for i := range buf[:want] {
				buf[i] = 0
			}
			n = int(want)
		}
		if int64(n) < want {
			for i := n; i < int(want); i++ {
				buf[i] = 0
			}
			n = int(want)
		}
		if _, err := out.WriteAt(buf[:n], offset); err != nil {
			return err
		}
		offset += int64(n)
	}
	return nil
}

func (w *inodeWriteState) createCommittedSnapshotLocked(ctx context.Context) (string, error) {
	temp, err := os.CreateTemp(w.fs.cacheDir, "inode-commit-*")
	if err != nil {
		return "", err
	}
	if err := temp.Truncate(w.logicalSize); err != nil {
		if closeErr := temp.Close(); closeErr != nil {
			logging.Error(nil, "failed to close commit snapshot temp after truncate error", "path", temp.Name(), "closeErr", closeErr, "err", err)
		}
		if removeErr := os.Remove(temp.Name()); removeErr != nil {
			logging.Error(nil, "failed to remove commit snapshot temp after truncate error", "path", temp.Name(), "removeErr", removeErr, "err", err)
		}
		return "", err
	}
	if w.tempAuthoritative || w.coversRangeLocked(0, w.logicalSize) {
		if err := w.writeWorkingRangeToLocked(temp, 0, w.logicalSize); err != nil {
			if closeErr := temp.Close(); closeErr != nil {
				logging.Error(nil, "failed to close commit snapshot temp after write error", "path", temp.Name(), "closeErr", closeErr, "err", err)
			}
			if removeErr := os.Remove(temp.Name()); removeErr != nil {
				logging.Error(nil, "failed to remove commit snapshot temp after write error", "path", temp.Name(), "removeErr", removeErr, "err", err)
			}
			return "", err
		}
	} else if err := w.writeRangeToLocked(ctx, temp, 0, w.logicalSize); err != nil {
		if closeErr := temp.Close(); closeErr != nil {
			logging.Error(nil, "failed to close commit snapshot temp after range write error", "path", temp.Name(), "closeErr", closeErr, "err", err)
		}
		if removeErr := os.Remove(temp.Name()); removeErr != nil {
			logging.Error(nil, "failed to remove commit snapshot temp after range write error", "path", temp.Name(), "removeErr", removeErr, "err", err)
		}
		return "", err
	}
	if err := temp.Close(); err != nil {
		if removeErr := os.Remove(temp.Name()); removeErr != nil {
			logging.Error(nil, "failed to remove commit snapshot temp after close error", "path", temp.Name(), "removeErr", removeErr, "err", err)
		}
		return "", err
	}
	return temp.Name(), nil
}

func (w *inodeWriteState) replaceInputPathLocked(ctx context.Context) (string, bool, error) {
	if w.temp != nil && w.tempPath != "" && (w.tempAuthoritative || w.coversRangeLocked(0, w.logicalSize)) {
		if err := w.temp.Truncate(w.logicalSize); err != nil {
			return "", false, err
		}
		return w.tempPath, false, nil
	}
	snapshotPath, err := w.createCommittedSnapshotLocked(ctx)
	if err != nil {
		return "", false, err
	}
	return snapshotPath, true, nil
}

func (w *inodeWriteState) createRangeSnapshotLocked(ctx context.Context, ranges []ByteRange) (string, error) {
	temp, err := os.CreateTemp(w.fs.cacheDir, "inode-ranges-*")
	if err != nil {
		return "", err
	}
	if err := temp.Truncate(w.logicalSize); err != nil {
		if closeErr := temp.Close(); closeErr != nil {
			logging.Error(nil, "failed to close range snapshot temp after truncate error", "path", temp.Name(), "closeErr", closeErr, "err", err)
		}
		if removeErr := os.Remove(temp.Name()); removeErr != nil {
			logging.Error(nil, "failed to remove range snapshot temp after truncate error", "path", temp.Name(), "removeErr", removeErr, "err", err)
		}
		return "", err
	}
	buf := make([]byte, w.fs.copyPageSize())
	for _, r := range ranges {
		for offset := r.Start; offset < r.End; {
			want := int64(len(buf))
			if remaining := r.End - offset; want > remaining {
				want = remaining
			}
			n, err := w.readIntoLocked(ctx, buf[:want], offset)
			if err != nil {
				if closeErr := temp.Close(); closeErr != nil {
					logging.Error(nil, "failed to close range snapshot temp after read error", "path", temp.Name(), "closeErr", closeErr, "err", err)
				}
				if removeErr := os.Remove(temp.Name()); removeErr != nil {
					logging.Error(nil, "failed to remove range snapshot temp after read error", "path", temp.Name(), "removeErr", removeErr, "err", err)
				}
				return "", err
			}
			if n == 0 {
				break
			}
			if _, err := temp.WriteAt(buf[:n], offset); err != nil {
				if closeErr := temp.Close(); closeErr != nil {
					logging.Error(nil, "failed to close range snapshot temp after write error", "path", temp.Name(), "closeErr", closeErr, "err", err)
				}
				if removeErr := os.Remove(temp.Name()); removeErr != nil {
					logging.Error(nil, "failed to remove range snapshot temp after write error", "path", temp.Name(), "removeErr", removeErr, "err", err)
				}
				return "", err
			}
			offset += int64(n)
		}
	}
	if err := temp.Close(); err != nil {
		if removeErr := os.Remove(temp.Name()); removeErr != nil {
			logging.Error(nil, "failed to remove range snapshot temp after close error", "path", temp.Name(), "removeErr", removeErr, "err", err)
		}
		return "", err
	}
	return temp.Name(), nil
}

func (w *inodeWriteState) closeTemp() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	if w.temp != nil {
		if err := w.temp.Close(); err != nil {
			logging.Error(nil, "failed to close write state temp file", "err", err)
		}
	}
	if w.tempPath != "" {
		if err := os.Remove(w.tempPath); err != nil {
			logging.Error(nil, "failed to remove write state temp file", "path", w.tempPath, "err", err)
		}
	}
	if w.baseTemp != nil {
		if err := w.baseTemp.Close(); err != nil {
			logging.Error(nil, "failed to close write state base temp file", "err", err)
		}
	}
	if w.baseTempPath != "" {
		if err := os.Remove(w.baseTempPath); err != nil {
			logging.Error(nil, "failed to remove write state base temp file", "path", w.baseTempPath, "err", err)
		}
	}
	w.mu.Unlock()
}

// quarantineTemps moves the overlay temp (the only copy of uncommitted
// written data) into the recovery directory; the re-downloadable base snapshot
// is discarded as usual. Marks the state closed so a later closeTemp is a no-op.
func (w *inodeWriteState) quarantineTemps() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	temp := w.temp
	tempPath := w.tempPath
	baseTemp := w.baseTemp
	baseTempPath := w.baseTempPath
	w.temp = nil
	w.tempPath = ""
	w.baseTemp = nil
	w.baseTempPath = ""
	w.mu.Unlock()
	if baseTemp != nil {
		_ = baseTemp.Close()
	}
	if baseTempPath != "" {
		_ = os.Remove(baseTempPath)
	}
	if temp != nil {
		_ = temp.Close()
	}
	if tempPath != "" {
		w.fs.quarantineFile(tempPath)
	}
}

// hasUncommittedChanges reports whether the overlay holds data that has never
// been successfully committed.
func (w *inodeWriteState) hasUncommittedChanges() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.dirtyRanges) > 0 || w.logicalSize != w.baseSize || w.hasPendingMetadataLocked()
}

func (h *storhubHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if writeState := h.writeState; writeState != nil {
		writeState.mu.Lock()
		buf := make([]byte, len(dest))
		n, err := writeState.readIntoLocked(ctx, buf, off)
		writeState.mu.Unlock()
		if err != nil {
			return nil, errnoFromError(err)
		}
		return fuse.ReadResultData(buf[:n]), 0
	}
	h.mu.Lock()
	temp := h.temp
	h.mu.Unlock()
	if temp != nil {
		buf := make([]byte, len(dest))
		n, err := temp.ReadAt(buf, off)
		if err != nil && !errors.Is(err, os.ErrClosed) && !errors.Is(err, io.EOF) {
			return nil, errnoFromError(err)
		}
		return fuse.ReadResultData(buf[:n]), 0
	}
	data, err := h.readFromPinned(ctx, off, int64(len(dest)))
	if err != nil {
		return nil, errnoFromError(err)
	}
	return fuse.ReadResultData(data), 0
}

// readFromPinned resolves bytes against the content layout captured at
// open time. The pinned file entry and chunk descriptors are immutable,
// and chunk assets are content-addressed, so every later read of this
// handle sees exactly the content that existed when it was opened -
// regardless of renames, replacements, or unlinks that happened in
// between. Zero network at pin time; asset ranges only on actual reads.
func (h *storhubHandle) readFromPinned(ctx context.Context, off, length int64) ([]byte, error) {
	h.mu.Lock()
	pin := h.pinned
	h.mu.Unlock()
	if pin == nil {
		return nil, syscall.EIO
	}
	return h.fs.hub.ReadPinnedFileContext(ctx, h.fs.project, &pin.file, pin.chunks, off, length)
}

func (h *storhubHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	_ = ctx
	if h.writeState == nil {
		// No write state exists for this handle: only read-oriented
		// materialized snapshots (e.g. displaced handles) land here. The
		// kernel never routes WRITE to such handles under enforced mode
		// flags, and historically this branch wrote into the snapshot temp
		// without dirty tracking — the data was acknowledged but silently
		// discarded at commit. Fail loudly instead of pretending the write
		// landed.
		h.fs.debugf("write rejected path=%s inode=%d reason=no-write-state", h.path, h.inode)
		return 0, syscall.EIO
	}
	h.writeState.opMu.Lock()
	defer h.writeState.opMu.Unlock()
	h.writeState.mu.Lock()
	defer h.writeState.mu.Unlock()
	if h.flags&syscall.O_APPEND != 0 {
		off = h.writeState.logicalSize
	}
	if h.writeState.temp == nil {
		if err := h.writeState.ensureTempLocked(); err != nil {
			return 0, errnoFromError(err)
		}
	}
	if off > h.writeState.logicalSize {
		h.writeState.markDirtyLocked(h.writeState.logicalSize, off)
		if err := h.writeState.temp.Truncate(off); err != nil {
			return 0, errnoFromError(err)
		}
		h.writeState.logicalSize = off
	}
	n, err := h.writeState.temp.WriteAt(data, off)
	if err != nil {
		return uint32(n), errnoFromError(err)
	}
	end := off + int64(n)
	if end > h.writeState.logicalSize {
		h.writeState.logicalSize = end
		if err := h.writeState.temp.Truncate(end); err != nil {
			return uint32(n), errnoFromError(err)
		}
	}
	h.writeState.markDirtyLocked(off, end)
	h.fs.debugf("write path=%s inode=%d off=%d bytes=%d", h.writeState.path, h.inode, off, n)
	return uint32(n), 0
}

// fallocate mode flags from linux/falloc.h, kept here because the only
// consumers are the overlay handlers below.
const (
	fallocFlKeepSize      = 0x01
	fallocFlPunchHole     = 0x02
	fallocFlNoHideStale   = 0x04
	fallocFlCollapseRange = 0x08
	fallocFlZeroRange     = 0x10
	fallocFlInsertRange   = 0x20
	fallocFlUnshare       = 0x40
)

// Allocate implements fs.FileAllocater: posix_fallocate-style space
// reservation on the open handle. Only plain allocation and
// FALLOC_FL_KEEP_SIZE map cleanly onto the overlay temp file - blocks
// are reserved locally so disk exhaustion surfaces at allocation time
// instead of mid-write. Hole punching and range collapsing would
// rewrite chunk layout semantics the overlay cannot express, so they
// return EOPNOTSUPP rather than pretending. A read-only handle has
// nothing to allocate against and returns EBADF, matching POSIX.
func (h *storhubHandle) Allocate(ctx context.Context, off uint64, size uint64, mode uint32) syscall.Errno {
	_ = ctx
	if h.writeState == nil {
		return syscall.EBADF
	}
	if off > math.MaxInt64 || size > math.MaxInt64 {
		return syscall.EINVAL
	}
	start := int64(off)
	length := int64(size)
	if start+length < start {
		return syscall.EINVAL
	}
	switch {
	case mode&^(fallocFlKeepSize|fallocFlPunchHole|fallocFlNoHideStale|fallocFlCollapseRange|fallocFlZeroRange|fallocFlInsertRange|fallocFlUnshare) != 0:
		return syscall.EINVAL
	case mode&(fallocFlPunchHole|fallocFlCollapseRange|fallocFlInsertRange|fallocFlUnshare|fallocFlZeroRange) != 0:
		if mode&fallocFlPunchHole != 0 && mode&fallocFlKeepSize == 0 {
			return syscall.EINVAL
		}
		return syscall.EOPNOTSUPP
	}
	h.writeState.opMu.Lock()
	defer h.writeState.opMu.Unlock()
	h.writeState.mu.Lock()
	defer h.writeState.mu.Unlock()
	if err := h.writeState.ensureTempLocked(); err != nil {
		return errnoFromError(err)
	}
	if err := reserveSpace(h.writeState.temp, mode, start, length); err != nil {
		return errnoFromError(err)
	}
	end := start + length
	if mode&fallocFlKeepSize == 0 && end > h.writeState.logicalSize {
		h.writeState.markDirtyLocked(h.writeState.logicalSize, end)
		h.writeState.logicalSize = end
	}
	h.fs.debugf("fallocate path=%s inode=%d off=%d size=%d mode=%#x", h.path, h.inode, start, length, mode)
	return 0
}

// retrieveKernelCache has been removed.
// The kernel guarantees it sends FUSE_WRITE for dirty pages (including mmap)
// before FUSE_RELEASE via filemap_write_and_wait_range in fuse_flush().
// The opMu lock in commit() ensures all FUSE_WRITE handlers complete before
// the commit runs, so dirtyRanges is always populated correctly.

func (h *storhubHandle) Flush(ctx context.Context) syscall.Errno {
	_ = ctx
	return 0
}

func (h *storhubHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	_ = flags
	h.fs.debugf("fsync path=%s inode=%d", h.path, h.inode)
	return h.commit(ctx)
}

func (h *storhubHandle) Release(ctx context.Context) syscall.Errno {
	h.fs.debugf("release path=%s inode=%d", h.path, h.inode)
	errno := h.commit(ctx)
	h.releaseTrackedLocks()
	if errno != 0 {
		// The commit failed; the overlay temps hold the only copy of data
		// the application already wrote (Flush returns 0 by design, so the
		// application considers these writes acknowledged). Preserve them
		// for manual recovery instead of deleting them.
		h.quarantineTemps()
		if h.writeState != nil && h.fs.soleWriteStateRef(h.writeState) {
			h.writeState.quarantineTemps()
		}
	} else {
		h.closeTemp()
	}
	h.fs.mu.Lock()
	delete(h.fs.handles, h.id)
	noHandlesLeft := !h.fs.hasOpenHandleForLocked(h.inode)
	h.fs.mu.Unlock()
	if noHandlesLeft {
		// POSIX: all of a process's locks on a file are gone once it has
		// no open descriptor left. The kernel enforces this itself for the
		// owning process; mirroring it here prevents locks from other
		// (crashed or sloppy) owners from lingering on the inode forever.
		// go-fuse does not surface FUSE_RELEASE's LockOwner (fs API v2.11),
		// so per-fd close semantics cannot be mirrored exactly — last
		// close is the guarantee that matters in practice.
		h.fs.dropAllLocksForInode(h.inode)
	}
	if h.writeState != nil {
		h.fs.releaseWriteState(h.writeState)
		h.writeState = nil
	}
	_ = ctx
	return errno
}

// quarantineTemps moves this handle's temp snapshot into the recovery
// directory instead of deleting it. Used on commit failure.
func (h *storhubHandle) quarantineTemps() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	temp := h.temp
	tempPath := h.tempPath
	h.temp = nil
	h.tempPath = ""
	h.mu.Unlock()
	if temp != nil {
		_ = temp.Close()
	}
	if tempPath != "" {
		h.fs.quarantineFile(tempPath)
	}
}

func (h *storhubHandle) commit(ctx context.Context) syscall.Errno {
	if h.writeState == nil {
		// Read-only or detached handle: provably nothing to commit.
		return 0
	}
	h.mu.Lock()
	handlePath := h.path
	h.mu.Unlock()
	h.writeState.opMu.Lock()
	defer h.writeState.opMu.Unlock()
	h.writeState.mu.Lock()
	if len(h.writeState.dirtyRanges) == 0 && h.writeState.logicalSize == h.writeState.baseSize && !h.writeState.hasPendingMetadataLocked() {
		h.writeState.mu.Unlock()
		return 0
	}
	if h.writeState.deleted || strings.TrimSpace(handlePath) == "" {
		h.writeState.mu.Unlock()
		return 0
	}
	targetPath := handlePath
	baseSize := h.writeState.baseSize
	logicalSize := h.writeState.logicalSize
	pending := h.writeState.pending
	return h.commitTemp(ctx, targetPath, baseSize, logicalSize, pending)
}

// commitTemp handles all temp-based commit paths (truncate, chunk-rewrite, replace, patch).
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu as needed.
func (h *storhubHandle) commitTemp(ctx context.Context, targetPath string, baseSize, logicalSize int64, pending shfs.MetadataPatch) syscall.Errno {
	if len(h.writeState.dirtyRanges) == 0 {
		h.writeState.mu.Unlock()
		if logicalSize != baseSize {
			h.fs.debugf("commit truncate path=%s inode=%d size=%d", targetPath, h.inode, logicalSize)
			if _, err := h.fs.hub.TruncateFileContext(ctx, h.fs.project, targetPath, logicalSize); err != nil {
				h.fs.debugf("commit failed path=%s inode=%d step=truncate err=%v", targetPath, h.inode, err)
				return errnoFromError(err)
			}
		}
		if errno := h.applyMetadataPatch(ctx, targetPath, pending); errno != 0 {
			return errno
		}
		h.writeState.mu.Lock()
		h.writeState.commitCacheRefreshLocked(logicalSize)
		h.writeState.mu.Unlock()
		h.fs.notifyKernelContentChanged(h.inode)
		h.writeState.mu.Lock()
		h.writeState.pending = shfs.MetadataPatch{}
		h.writeState.mu.Unlock()
		h.fs.notifyEntryForPath(shfs.ParentPath(targetPath), path.Base(targetPath))
		return 0
	}
	planned := h.writeState.plannedRangesLocked()
	// Rung order: chunk-rewrite (rebuild the touched chunks wholesale) is
	// preferred when fragmentation is high but the affected byte volume is
	// modest; full replace when the ladder in shouldReplaceLocked says
	// ranged work cannot pay; otherwise patch each dirty range in place.
	if h.writeState.shouldChunkRewriteLocked(planned) {
		return h.commitChunkRewrite(ctx, targetPath, logicalSize, planned, pending)
	}
	if h.writeState.shouldReplaceLocked(planned) {
		return h.commitReplace(ctx, targetPath, logicalSize, planned, pending)
	}
	return h.commitPatch(ctx, targetPath, baseSize, logicalSize, h.writeState.dirtyRanges, pending)
}

// commitChunkRewrite handles the chunk-rewrite path.
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu.
func (h *storhubHandle) commitChunkRewrite(ctx context.Context, targetPath string, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch) syscall.Errno {
	snapshotPath, err := h.writeState.createRangeSnapshotLocked(ctx, planned)
	baseSize := h.writeState.baseSize
	h.writeState.mu.Unlock()
	if err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=range-snapshot err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	releaseSnapshot := h.fs.protectTemp(snapshotPath)
	defer func() {
		_ = os.Remove(snapshotPath)
		releaseSnapshot()
	}()
	h.fs.debugf("commit chunk-rewrite path=%s inode=%d base=%d size=%d ranges=%d", targetPath, h.inode, baseSize, logicalSize, len(planned))
	repoMeta, _, err := h.fs.hub.LoadRepoMetadataReadonlyContext(ctx, h.fs.project)
	if err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=load-metadata err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	fileMeta := repoMeta.FindFile(targetPath)
	if fileMeta == nil {
		h.fs.debugf("commit failed path=%s inode=%d step=find-file err=not found", targetPath, h.inode)
		return syscall.ENOENT
	}
	if _, err := h.fs.hub.RewriteFileRangesWithMetadataContext(ctx, h.fs.project, targetPath, snapshotPath, repoMeta, fileMeta, logicalSize, planned); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=chunk-rewrite err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	h.writeState.mu.Lock()
	h.writeState.commitCacheRefreshLocked(logicalSize)
	h.writeState.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending)
}

// commitReplace handles the full-file replace path.
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu.
func (h *storhubHandle) commitReplace(ctx context.Context, targetPath string, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch) syscall.Errno {
	snapshotPath, cleanupSnapshot, err := h.writeState.replaceInputPathLocked(ctx)
	baseSize := h.writeState.baseSize
	dirtyCount := len(h.writeState.dirtyRanges)
	h.writeState.mu.Unlock()
	if err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=full-snapshot err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	var releaseSnapshot func()
	if cleanupSnapshot {
		releaseSnapshot = h.fs.protectTemp(snapshotPath)
		defer func() {
			_ = os.Remove(snapshotPath)
			releaseSnapshot()
		}()
	}
	h.fs.debugf("commit replace path=%s inode=%d base=%d size=%d dirty_ranges=%d", targetPath, h.inode, baseSize, logicalSize, dirtyCount)
	if _, err := h.fs.hub.ReplaceFileContext(ctx, h.fs.project, targetPath, snapshotPath); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=replace err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	h.writeState.mu.Lock()
	h.writeState.commitCacheRefreshLocked(logicalSize)
	h.writeState.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending)
}

// removeDirtyRangeLocked drops [start,end) from the dirty set after a range
// was successfully committed, so a retry only re-applies the remainder.
// Caller must hold w.mu.
func (w *inodeWriteState) removeDirtyRangeLocked(start, end int64) {
	kept := w.dirtyRanges[:0]
	for _, r := range w.dirtyRanges {
		if r.End <= start || r.Start >= end {
			kept = append(kept, r)
			continue
		}
		if r.Start < start {
			kept = append(kept, ByteRange{Start: r.Start, End: start})
		}
		if r.End > end {
			kept = append(kept, ByteRange{Start: end, End: r.End})
		}
	}
	w.dirtyRanges = kept
}

// commitPatch handles the partial patch path.
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu.
func (h *storhubHandle) commitPatch(ctx context.Context, targetPath string, baseSize, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch) syscall.Errno {
	edits := make([][]byte, len(planned))
	for i, dirty := range planned {
		buf := make([]byte, dirty.End-dirty.Start)
		n, err := h.writeState.readIntoLocked(ctx, buf, dirty.Start)
		if err != nil {
			h.writeState.mu.Unlock()
			return errnoFromError(err)
		}
		for j := n; j < len(buf); j++ {
			buf[j] = 0
		}
		edits[i] = buf
	}
	h.writeState.mu.Unlock()
	h.fs.debugf("commit patch path=%s inode=%d base=%d size=%d ranges=%d", targetPath, h.inode, baseSize, logicalSize, len(planned))
	for i := len(planned) - 1; i >= 0; i-- {
		dirty := planned[i]
		deleteSize := dirty.End - dirty.Start
		if dirty.Start >= baseSize {
			deleteSize = 0
		} else if maxDelete := baseSize - dirty.Start; deleteSize > maxDelete {
			deleteSize = maxDelete
		}
		if _, err := h.fs.hub.PatchFileContext(ctx, h.fs.project, targetPath, dirty.Start, deleteSize, edits[i]); err != nil {
			h.fs.debugf("commit failed path=%s inode=%d step=patch offset=%d err=%v", targetPath, h.inode, dirty.Start, err)
			return errnoFromError(err)
		}
		// Mark this range applied so a retry resumes instead of
		// re-applying already-committed edits (which would duplicate
		// bytes).
		h.writeState.mu.Lock()
		h.writeState.removeDirtyRangeLocked(dirty.Start, dirty.End)
		h.writeState.mu.Unlock()
	}
	if logicalSize != baseSize {
		appendOnly := len(planned) == 1 && planned[0].Start >= baseSize && logicalSize == planned[0].End
		if !appendOnly {
			if _, err := h.fs.hub.TruncateFileContext(ctx, h.fs.project, targetPath, logicalSize); err != nil {
				h.fs.debugf("commit failed path=%s inode=%d step=post-patch-truncate err=%v", targetPath, h.inode, err)
				return errnoFromError(err)
			}
		}
	}
	h.writeState.mu.Lock()
	if err := h.writeState.refreshBaseSnapshotLocked(); err != nil {
		h.fs.debugf("commit cache refresh failed path=%s inode=%d step=patch-cache err=%v", targetPath, h.inode, err)
		h.writeState.clearBaseSnapshotLocked()
	}
	h.writeState.baseSize = logicalSize
	h.writeState.dirtyRanges = nil
	h.writeState.tempAuthoritative = false
	if err := h.writeState.temp.Truncate(logicalSize); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=local-truncate err=%v", targetPath, h.inode, err)
		h.writeState.mu.Unlock()
		return errnoFromError(err)
	}
	h.fs.notifyKernelContentChanged(h.inode)
	h.writeState.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending)
}

// commitCacheRefreshLocked updates the cached write state after a successful remote write.
// Caller must hold h.writeState.mu.
func (w *inodeWriteState) commitCacheRefreshLocked(logicalSize int64) {
	if err := w.refreshBaseSnapshotLocked(); err != nil {
		w.clearBaseSnapshotLocked()
	}
	w.baseSize = logicalSize
	w.dirtyRanges = nil
	w.tempAuthoritative = false
}

// commitPostUpdate applies pending metadata, clears it, evicts inode cache, and notifies.
// Caller must NOT hold h.writeState.mu.
func (h *storhubHandle) commitPostUpdate(ctx context.Context, targetPath string, pending shfs.MetadataPatch) syscall.Errno {
	if errno := h.applyMetadataPatch(ctx, targetPath, pending); errno != 0 {
		return errno
	}
	h.writeState.mu.Lock()
	h.writeState.pending = shfs.MetadataPatch{}
	h.writeState.mu.Unlock()
	h.fs.notifyKernelContentChanged(h.inode)
	h.fs.notifyEntryForPath(shfs.ParentPath(targetPath), path.Base(targetPath))
	return 0
}

func (h *storhubHandle) closeTemp() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	if h.temp != nil {
		if err := h.temp.Close(); err != nil {
			logging.Error(nil, "failed to close handle temp file", "err", err)
		}
	}
	if h.tempPath != "" {
		if err := os.Remove(h.tempPath); err != nil {
			logging.Error(nil, "failed to remove handle temp file", "path", h.tempPath, "err", err)
		}
	}
}

func (h *storhubHandle) applyMetadataPatch(ctx context.Context, targetPath string, patch shfs.MetadataPatch) syscall.Errno {
	if !patch.HasMode && !patch.HasOwner && !patch.HasTimes {
		return 0
	}
	if err := h.fs.hub.ApplyMetadataPatchContext(ctx, h.fs.project, targetPath, patch); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=metadata-patch err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	return 0
}

func (h *storhubHandle) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	_ = ctx
	_ = flags
	h.fs.mu.RLock()
	defer h.fs.mu.RUnlock()
	locks := h.fs.lockTable[h.inode]
	for _, existing := range locks {
		if lockConflicts(existing, owner, *lk) {
			*out = existing.lock
			return 0
		}
	}
	out.Typ = syscall.F_UNLCK
	return 0
}

func (h *storhubHandle) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	_ = ctx
	_ = flags
	errno := h.fs.setLock(h.inode, owner, *lk)
	if errno == 0 {
		h.trackLockOwner(owner, lk.Typ)
	}
	return errno
}

// Setlkw blocks until the lock can be granted. Waiters sleep on the
// filesystem's lock condition variable and are woken by any lock-table
// change (or context cancellation) instead of polling.
func (h *storhubHandle) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	// flags carries FUSE_LK intent knobs beyond shared/exclusive type.
	// Blocking semantics here are driven by lk.Type alone; honoring the
	// remaining bits stays deferred until go-fuse round-trips them (see
	// Getlk/Setlk, same discard).
	_ = flags
	if ctx != nil && ctx.Done() != nil {
		// Wake the waiter promptly on cancellation.
		context.AfterFunc(ctx, h.fs.lockCond.Broadcast)
	}
	s := h.fs
	s.lockCond.L.Lock()
	defer s.lockCond.L.Unlock()
	for {
		errno := s.setLockLocked(h.inode, owner, *lk)
		if errno == 0 {
			h.trackLockOwner(owner, lk.Typ)
			return 0
		}
		if err := ctx.Err(); err != nil {
			return errnoFromError(err)
		}
		s.lockCond.Wait()
	}
}

func (s *Filesystem) setLock(inode, owner uint64, lk fuse.FileLock) syscall.Errno {
	s.lockCond.L.Lock()
	defer s.lockCond.L.Unlock()
	errno := s.setLockLocked(inode, owner, lk)
	if errno == 0 {
		// A release may unblock F_SETLKW waiters.
		s.lockCond.Broadcast()
	}
	return errno
}

// setLockLocked applies a lock operation; callers must hold lockCond's
// locker (s.mu).
func (s *Filesystem) setLockLocked(inode, owner uint64, lk fuse.FileLock) syscall.Errno {
	locks := s.lockTable[inode]
	if lk.Typ == syscall.F_UNLCK {
		filtered := locks[:0]
		for _, existing := range locks {
			if existing.owner != owner {
				filtered = append(filtered, existing)
				continue
			}
			for _, segment := range subtractLock(existing.lock, lk) {
				filtered = append(filtered, lockRecord{owner: existing.owner, lock: segment})
			}
		}
		s.lockTable[inode] = filtered
		return 0
	}
	for _, existing := range locks {
		if lockConflicts(existing, owner, lk) {
			return syscall.EAGAIN
		}
	}
	filtered := locks[:0]
	for _, existing := range locks {
		if existing.owner == owner {
			for _, segment := range subtractLock(existing.lock, lk) {
				filtered = append(filtered, lockRecord{owner: existing.owner, lock: segment})
			}
			continue
		}
		filtered = append(filtered, existing)
	}
	s.lockTable[inode] = append(filtered, lockRecord{owner: owner, lock: lk})
	return 0
}

func lockConflicts(existing lockRecord, owner uint64, requested fuse.FileLock) bool {
	if requested.Typ == syscall.F_UNLCK || existing.owner == owner || !locksOverlap(existing.lock, requested) {
		return false
	}
	if existing.lock.Typ == syscall.F_RDLCK && requested.Typ == syscall.F_RDLCK {
		return false
	}
	return true
}

func (h *storhubHandle) trackLockOwner(owner uint64, typ uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if typ == syscall.F_UNLCK {
		delete(h.owners, owner)
		return
	}
	h.owners[owner] = struct{}{}
}

func (h *storhubHandle) releaseTrackedLocks() {
	h.mu.Lock()
	owners := make([]uint64, 0, len(h.owners))
	for owner := range h.owners {
		owners = append(owners, owner)
	}
	h.mu.Unlock()
	for _, owner := range owners {
		// Best-effort unlock during handle teardown: POSIX locks vanish
		// with the process anyway, so there is nobody left to report to.
		_ = h.fs.setLock(h.inode, owner, fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_UNLCK})
	}
}

func locksOverlap(a, b fuse.FileLock) bool {
	aEnd := a.End
	bEnd := b.End
	if aEnd == 0 {
		aEnd = ^uint64(0)
	}
	if bEnd == 0 {
		bEnd = ^uint64(0)
	}
	return a.Start <= bEnd && b.Start <= aEnd
}

func subtractLock(existing, cut fuse.FileLock) []fuse.FileLock {
	if !locksOverlap(existing, cut) {
		return []fuse.FileLock{existing}
	}
	existingEnd := existing.End
	cutEnd := cut.End
	if existingEnd == 0 {
		existingEnd = ^uint64(0)
	}
	if cutEnd == 0 {
		cutEnd = ^uint64(0)
	}
	segments := make([]fuse.FileLock, 0, 2)
	if cut.Start > existing.Start {
		left := existing
		left.End = cut.Start - 1
		segments = append(segments, left)
	}
	if cutEnd < existingEnd {
		right := existing
		right.Start = cutEnd + 1
		if existing.End == 0 {
			right.End = 0
		} else {
			right.End = existing.End
		}
		segments = append(segments, right)
	}
	return segments
}

func (s *Filesystem) notifyKernelContentChanged(inode uint64) {
	s.mu.Lock()
	node := s.nodes[inode]
	s.mu.Unlock()
	safeNotifyContent(node)
}

// fsConnected reports whether the filesystem is currently served over a
// FUSE connection. Kernel cache notifications are meaningless without a
// mount, and calling them on a detached filesystem - as tests do when
// they drive nodes directly - panics inside go-fuse on the nil
// connection state. Callers skip notification entirely in that case.
var fsConnectedFunc = (*Filesystem).connected

func (s *Filesystem) connected() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.server != nil
}

func fsConnected(fs *Filesystem) bool {
	return fsConnectedFunc(fs)
}

func safeNotifyContent(node *storhubNode) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logging.Error(nil, "panic in NotifyContent", "panic", r, "stack", string(buf[:n]))
		}
	}()
	if node == nil || !fsConnected(node.fs) {
		return
	}
	_ = node.NotifyContent(0, 0)
}

func safeNotifyEntry(node *storhubNode, name string) {
	if node == nil || !fsConnected(node.fs) {
		return
	}
	entryFn := notifyEntryFunc
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				logging.Error(nil, "panic in NotifyEntry", "panic", r, "stack", string(buf[:n]))
			}
		}()
		entryFn(node, name)
	}()
}

func safeNotifyDelete(parent *storhubNode, name string, child *storhubNode) {
	if parent == nil || !fsConnected(parent.fs) {
		return
	}
	entryFn := notifyEntryFunc
	deleteFn := notifyDeleteFunc
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				logging.Error(nil, "panic in NotifyDelete", "panic", r, "stack", string(buf[:n]))
			}
		}()
		if child == nil {
			entryFn(parent, name)
			return
		}
		deleteFn(parent, name, child)
	}()
}

func (s *Filesystem) runJanitor() {
	defer close(s.janitorDone)
	ticker := time.NewTicker(s.opts.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopJanitor:
			return
		case <-ticker.C:
			s.cleanupExpiredCache()
		}
	}
}

// protectTemp marks a commit snapshot as janitor-immune until the returned
// release func runs. Every creator of inode-commit-*/inode-ranges-* temps
// must wrap its consumption window; otherwise a slow metadata load or
// upload lets cleanupExpiredCache delete the file mid-commit.
// nlinkForEntry reports the hard-link count for a freshly created entry.
// A metadata load failure is logged and reported as 1 rather than silently
// fabricated as 0; getattr refreshes the value on the next lookup anyway.
func (s *Filesystem) nlinkForEntry(entryPath string) int {
	repo, _, err := s.hub.LoadRepoMetadataReadonlyContext(context.Background(), s.project)
	if err != nil {
		s.errorf("nlink lookup failed path=%s err=%v", entryPath, err)
		return 1
	}
	if repo == nil {
		return 1
	}
	if n := repo.FileNLink(entryPath); n > 0 {
		return n
	}
	return 1
}

func (s *Filesystem) protectTemp(p string) func() {
	s.protectedMu.Lock()
	if s.protectedTemps == nil {
		s.protectedTemps = make(map[string]struct{})
	}
	s.protectedTemps[p] = struct{}{}
	s.protectedMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.protectedMu.Lock()
			delete(s.protectedTemps, p)
			s.protectedMu.Unlock()
		})
	}
}

func (s *Filesystem) cleanupExpiredCache() {
	s.mu.RLock()
	handles := make([]*storhubHandle, 0, len(s.handles))
	for _, handle := range s.handles {
		handles = append(handles, handle)
	}
	writeStates := make([]*inodeWriteState, 0, len(s.writeStates))
	for _, writeState := range s.writeStates {
		writeStates = append(writeStates, writeState)
	}
	s.mu.RUnlock()
	openTemps := make(map[string]struct{}, len(handles)+len(writeStates))
	for _, handle := range handles {
		handle.mu.Lock()
		if handle.tempPath != "" {
			openTemps[handle.tempPath] = struct{}{}
		}
		handle.mu.Unlock()
	}
	for _, writeState := range writeStates {
		writeState.mu.Lock()
		if writeState.tempPath != "" {
			openTemps[writeState.tempPath] = struct{}{}
		}
		// Base snapshots feed in-flight commits; deleting them mid-commit
		// corrupts the upload.
		if writeState.baseTempPath != "" {
			openTemps[writeState.baseTempPath] = struct{}{}
		}
		writeState.mu.Unlock()
	}
	s.protectedMu.Lock()
	for p := range s.protectedTemps {
		openTemps[p] = struct{}{}
	}
	s.protectedMu.Unlock()
	graceThreshold := time.Now().Add(-5 * time.Minute)
	entries, err := os.ReadDir(s.cacheDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			// Subdirectories (e.g. recovery/) are never cache temp files.
			continue
		}
		fullPath := path.Join(s.cacheDir, entry.Name())
		if _, ok := openTemps[fullPath]; ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(graceThreshold) {
			_ = os.Remove(fullPath)
		}
	}
}

func fillEntryOut(out *fuse.EntryOut, entry *shfs.EntryInfo, opts Options) {
	if out == nil || entry == nil {
		return
	}
	fillAttr(&out.Attr, entry)
	out.SetAttrTimeout(opts.AttrTimeout)
	out.SetEntryTimeout(opts.EntryTimeout)
}

func fillAttr(attr *fuse.Attr, entry *shfs.EntryInfo) {
	attr.Ino = entry.Inode
	attr.Size = uint64(maxInt64(entry.Size, 0))
	attr.Blocks = uint64(maxInt64((entry.Size+511)/512, 0))
	attr.Owner = fuse.Owner{Uid: entry.UID, Gid: entry.GID}
	attr.Nlink = entry.NLink
	attr.Blksize = 4096
	atime := time.Unix(entry.AccessedAt, 0)
	mtime := time.Unix(entry.ModifiedAt, 0)
	ctime := time.Unix(entry.ChangedAt, 0)
	attr.SetTimes(&atime, &mtime, &ctime)
	mode := entry.Mode & 0o7777
	if entry.IsDir {
		mode |= syscall.S_IFDIR
	} else if entry.IsSymlink {
		mode |= syscall.S_IFLNK
	} else {
		mode |= syscall.S_IFREG
	}
	attr.Mode = mode
}

// errnoFromError maps storage-layer errors onto POSIX errnos. The ladder
// is ordered most-specific first: raw Errno passthrough, context
// cancellation/deadline, then the fs sentinel family, with EIO as the
// honest catch-all for anything unmapped (never success, never ENOENT).
func errnoFromError(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	if errors.Is(err, context.DeadlineExceeded) {
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
	default:
		return syscall.EIO
	}
}

func entryInfoFromFile(file *metadata.FileMeta, path string, nlink int) *shfs.EntryInfo {
	kind := metadata.NodeKindFile
	if file.Symlink != "" {
		kind = metadata.NodeKindSymlink
	}
	return &shfs.EntryInfo{
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

// copyPageSize returns the buffer size for overlay copy loops. It is the
// configured OverlayBufferSize (default applied at mount), capped by the normalized
// chunk size so it can never exceed a single chunk window.
func (s *Filesystem) copyPageSize() int64 {
	size := s.opts.OverlayBufferSize
	if size <= 0 {
		size = defaultOverlayBufferSize
	}
	if capSize := normalizedChunkSize(s.hub.ChunkSize()); size > capSize {
		return capSize
	}
	return size
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
