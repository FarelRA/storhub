package fusefs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	chunking "github.com/FarelRA/storhub/internal/chunking"
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
	EntryTimeout           time.Duration
	AttrTimeout            time.Duration
	NegativeTimeout        time.Duration
	CleanupInterval        time.Duration
	PageSize               int64
	MaxConcurrentTransfers int
	ExtraMountOpts         []string
	CacheDir               string
	AllowOther             bool
	Debug                  bool
	Logger                 *slog.Logger
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
	writeStates map[uint64]*inodeWriteState
	handles     map[uint64]*storhubHandle
	nextHandle  atomic.Uint64
	cacheDir    string
	stopJanitor chan struct{}
	janitorDone chan struct{}
	closing     bool
	unmounted   bool
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

type storhubHandle struct {
	fs    *Filesystem
	inode uint64
	id    uint64
	flags uint32

	mu         sync.Mutex
	temp       *os.File
	tempPath   string
	path       string
	closed     bool
	deleted    bool
	owners     map[uint64]struct{}
	writeState *inodeWriteState
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
	stream            *streamingUploadState
	streamCh          chan uploadTask
	streamDone        chan struct{}
	streamErr         atomic.Value
	streamDraining    bool
	streamCtx         context.Context
	streamCancel      context.CancelFunc
	pending           shfs.MetadataPatch
}

type streamingUploadState struct {
	releaseTag string
	uploadURL  string
	path       string
	prepared   bool
	prepareMu  sync.Mutex
	chunks     []metadata.ChunkInfo
	tail       []byte
	tailOffset int64
	uploaded   int64
	nextIndex  int
	chunkSize  int64
}

type ByteRange struct {
	Start int64
	End   int64
}

type uploadTask struct {
	index  int
	offset int64
	data   []byte
}

type writeBootstrap struct {
	baseSize int64
}

type (
	TestNode   = storhubNode
	TestHandle = storhubHandle
)

func DefaultOptions() Options {
	return Options{
		EntryTimeout:           60 * time.Second,
		AttrTimeout:            60 * time.Second,
		NegativeTimeout:        10 * time.Second,
		CleanupInterval:        30 * time.Second,
		PageSize:               128 * 1024,
		MaxConcurrentTransfers: 8,
		ExtraMountOpts:         []string{"noatime"},
		Debug:                  true,
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

type Hub interface {
	StatPathContext(context.Context, string, string) (*shfs.EntryInfo, error)
	ReadDirContext(context.Context, string, string) ([]shfs.DirEntry, error)
	StatFSContext(context.Context, string) (*shfs.FSStats, error)
	CreateFileContext(context.Context, string, string) (*metadata.FileMetadata, error)
	MkdirContext(context.Context, string, string) error
	UnlinkContext(context.Context, string, string) error
	RmdirContext(context.Context, string, string) error
	TruncateFileContext(context.Context, string, string, int64) (*metadata.FileMetadata, error)
	ChmodContext(context.Context, string, string, uint32) error
	ChownContext(context.Context, string, string, uint32, uint32) error
	ChtimesContext(context.Context, string, string, time.Time, time.Time) error
	SymlinkContext(context.Context, string, string, string) (*metadata.FileMetadata, error)
	ReadlinkContext(context.Context, string, string) (string, error)
	LinkContext(context.Context, string, string, string) (*metadata.FileMetadata, error)
	GetXAttrContext(context.Context, string, string, string) ([]byte, error)
	SetXAttrContext(context.Context, string, string, string, []byte) error
	ListXAttrContext(context.Context, string, string) ([]string, error)
	RemoveXAttrContext(context.Context, string, string, string) error
	ApplyMetadataPatchContext(context.Context, string, string, shfs.MetadataPatch) error
	DownloadFileContext(context.Context, string, string, string) error
	ReadFileAtContext(context.Context, string, string, int64, int64) ([]byte, error)
	PrepareReplaceContext(context.Context, string, string, int) (string, string, error)
	UploadChunkDataContext(context.Context, string, string, string, int, int64, []byte) (metadata.ChunkInfo, error)
	FinalizeReplaceChunksContext(context.Context, string, string, string, int64, []metadata.ChunkInfo) (*metadata.FileMetadata, error)
	FillChunkRangeContext(context.Context, string, metadata.ChunkInfo, []byte) error
	PatchFileContext(context.Context, string, string, int64, int64, []byte) (*metadata.FileMetadata, error)
	ReplaceFileContext(context.Context, string, string, string) (*metadata.FileMetadata, error)
	LoadRepoMetadataReadonlyContext(context.Context, string) (*metadata.RepoMetadata, string, error)
	UpdateRepoMetadataContext(context.Context, string, func(*metadata.RepoMetadata) error, string) (*metadata.RepoMetadata, error)
	RewriteFileRangesWithMetadataContext(context.Context, string, string, string, *metadata.RepoMetadata, *metadata.FileMetadata, int64, []ByteRange) (*metadata.FileMetadata, error)
	Now() time.Time
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
	if opts.PageSize <= 0 {
		opts.PageSize = defaults.PageSize
	}
	if len(opts.ExtraMountOpts) == 0 {
		opts.ExtraMountOpts = append([]string(nil), defaults.ExtraMountOpts...)
	}
	if opts.Logger == nil {
		opts.Logger = logging.WithComponent(logging.NewLogger(logging.Options{Level: logging.LevelDebug, Format: logging.FormatPretty, Color: true, Output: os.Stderr}), "fuse")
	}
	cacheDir := opts.CacheDir
	if strings.TrimSpace(cacheDir) == "" {
		cacheDir = path.Join(os.TempDir(), "storhub-fuse", project)
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create fuse cache dir: %w", err)
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
	options.MountOptions.Debug = s.opts.Debug
	options.MountOptions.AllowOther = s.opts.AllowOther
	options.MountOptions.MaxBackground = mountMaxBackground
	options.MountOptions.MaxWrite = mountMaxIOSize
	options.MountOptions.MaxReadAhead = int(s.hub.ChunkSize()) * s.opts.MaxConcurrentTransfers
	options.MountOptions.Options = append([]string(nil), s.opts.ExtraMountOpts...)
	options.MountOptions.ExplicitDataCacheControl = true
	options.MountOptions.ExtraCapabilities = fuse.CAP_WRITEBACK_CACHE
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
	s.mu.Unlock()
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

func (s *Filesystem) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ino, node := range s.nodes {
		if ino == 1 {
			continue
		}
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

func (s *Filesystem) pendingSizeForInode(inode uint64) (int64, bool) {
	state := s.writeStateForInode(inode)
	if state == nil {
		return 0, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleted {
		return 0, false
	}
	return state.logicalSize, true
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
			handle.path = newPath
			handle.deleted = newPath == ""
		}
		handle.mu.Unlock()
	}
	if writeState != nil {
		writeState.mu.Lock()
		if writeState.path == oldPath {
			writeState.path = newPath
			writeState.deleted = newPath == ""
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
		needsSnapshot := handle.path == targetPath && handle.temp == nil && handle.writeState == nil
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
		if recover() != nil {
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
	h, err := n.fs.newHandle(ctx, n.inode, targetPath, flags, nil)
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
	entry := entryInfoFromFile(file)
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
	if err := n.fs.renameWithReplace(ctx, oldPath, newPath, flags); err != nil {
		return errnoFromError(err)
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
			atime = entry.AccessedAt
		}
		if !mtimeOK {
			mtime = entry.ModifiedAt
		}
		if state != nil && !n.isDir {
			state.opMu.Lock()
			state.mu.Lock()
			state.overlayEntryLocked(entry)
			if !atimeOK {
				atime = entry.AccessedAt
			}
			if !mtimeOK {
				mtime = entry.ModifiedAt
			}
			state.pending.HasTimes = true
			state.pending.ATime = atime
			state.pending.MTime = mtime
			state.mu.Unlock()
			state.opMu.Unlock()
		} else {
			if err := n.fs.hub.ChtimesContext(ctx, n.fs.project, targetPath, atime, mtime); err != nil {
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
	entry := entryInfoFromFile(file)
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
	entry := entryInfoFromFile(linked)
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
	return h, nil
}

func (h *storhubHandle) materialize(ctx context.Context) error {
	if h.writeState != nil {
		return nil
	}
	return h.materializePath(ctx, h.path)
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
	if err == nil && entry.Size > 0 {
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

func (s *Filesystem) hasOtherWriteHandle(inode uint64, excludeID uint64) bool {
	type entry struct {
		id    uint64
		inode uint64
		ws    bool
	}
	s.mu.RLock()
	entries := make([]entry, 0, len(s.handles))
	for id, handle := range s.handles {
		entries = append(entries, entry{id, handle.inode, handle.writeState != nil})
	}
	s.mu.RUnlock()
	for _, e := range entries {
		if e.id != excludeID && e.inode == inode && e.ws {
			return true
		}
	}
	return false
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
	w.temp = temp
	w.tempPath = temp.Name()
	w.mu.Unlock()

	// Network call without lock
	entry, err := w.fs.hub.StatPathContext(ctx, w.fs.project, path)
	if err == nil {
		w.mu.Lock()
		w.baseSize = entry.Size
		w.logicalSize = entry.Size
		if truncErr := w.temp.Truncate(entry.Size); truncErr != nil {
			w.mu.Unlock()
			return truncErr
		}
		w.mu.Unlock()
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
	bufSize := normalizedChunkSize(w.fs.hub.ChunkSize())
	if bufSize <= 0 {
		bufSize = w.fs.opts.PageSize
	}
	buf := make([]byte, bufSize)
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
	if w.stream != nil {
		if size == w.logicalSize {
			return nil
		}
		if err := w.materializeStreamToTempLocked(context.Background()); err != nil {
			return err
		}
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
	} else if size > oldSize && oldSize < size && !w.tempAuthoritative {
		w.tempAuthoritative = false
	}
	if size > oldSize {
		w.markDirtyLocked(oldSize, size)
		return nil
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

func (w *inodeWriteState) canStreamSequentialWriteLocked() bool {
	return !w.deleted && w.tempAuthoritative && len(w.dirtyRanges) == 0
}

func (w *inodeWriteState) releaseTempFile() {
	if w.temp != nil {
		if err := w.temp.Close(); err != nil {
			logging.Error(nil, "failed to close temp file", "path", w.tempPath, "err", err)
		}
		w.temp = nil
	}
	if w.tempPath != "" {
		if err := os.Remove(w.tempPath); err != nil {
			logging.Error(nil, "failed to remove temp file", "path", w.tempPath, "err", err)
		}
		w.tempPath = ""
	}
	if w.baseTemp != nil {
		if err := w.baseTemp.Close(); err != nil {
			logging.Error(nil, "failed to close base temp file", "err", err)
		}
		w.baseTemp = nil
	}
	if w.baseTempPath != "" {
		if err := os.Remove(w.baseTempPath); err != nil {
			logging.Error(nil, "failed to remove base temp file", "path", w.baseTempPath, "err", err)
		}
		w.baseTempPath = ""
	}
	w.tempAuthoritative = false
}

func (w *inodeWriteState) tryStreamWriteLocked(ctx context.Context, data []byte, off int64, handleID uint64) (uint32, bool, error) {
	if len(data) == 0 {
		return 0, false, nil
	}
	if w.stream == nil {
		if w.fs.hasOtherWriteHandle(w.inode, handleID) {
			return 0, false, nil
		}
		if !w.canStreamSequentialWriteLocked() {
			return 0, false, nil
		}
		if off < w.logicalSize {
			return 0, false, nil
		}
		chunkSize := normalizedChunkSize(w.fs.hub.ChunkSize())
		if off > w.logicalSize {
			w.logicalSize = off
		}
		w.stream = &streamingUploadState{
			chunkSize:  chunkSize,
			tailOffset: off,
			path:       w.path,
		}
		if cap(data) <= int(chunkSize) {
			w.stream.tail = data[:0]
		}
		w.streamCh = make(chan uploadTask, 8)
		w.streamDone = make(chan struct{})
		w.streamCtx, w.streamCancel = context.WithCancel(context.Background())
		go w.uploadLoop(w.streamCtx)
		w.releaseTempFile()
		// Re-check: another writer may have appeared during init.
		if w.fs.hasOtherWriteHandle(w.inode, handleID) {
			w.materializeStreamToTempLocked(ctx)
			return 0, false, nil
		}
	}
	if w.streamCh == nil || w.streamDraining {
		return 0, false, nil
	}
	if err := w.streamErr.Load(); err != nil {
		w.materializeStreamToTempLocked(ctx)
		return 0, false, nil
	}
	if off != w.logicalSize {
		if w.streamCh != nil {
			if len(w.stream.tail) > 0 {
				w.mu.Unlock()
				select {
				case w.streamCh <- uploadTask{
					index:  w.stream.nextIndex,
					offset: w.stream.tailOffset,
					data:   w.stream.tail,
				}:
				case <-w.streamDone:
					w.mu.Lock()
					w.materializeStreamToTempLocked(ctx)
					return 0, false, nil
				}
				w.mu.Lock()
				w.stream.nextIndex++
				w.stream.uploaded += int64(len(w.stream.tail))
				w.stream.tail = nil
			}
		}
		w.logicalSize = off
		w.stream.tailOffset = off
	}
	w.stream.tail = append(w.stream.tail, data...)
	w.logicalSize += int64(len(data))
	for int64(len(w.stream.tail)) >= w.stream.chunkSize {
		task := uploadTask{
			index:  w.stream.nextIndex,
			offset: w.stream.tailOffset,
			data:   w.stream.tail[:w.stream.chunkSize],
		}
		w.stream.nextIndex++
		w.stream.uploaded += w.stream.chunkSize
		w.stream.tailOffset += w.stream.chunkSize
		w.stream.tail = w.stream.tail[w.stream.chunkSize:]

		w.mu.Unlock()
		select {
		case w.streamCh <- task:
		case <-w.streamDone:
			w.mu.Lock()
			w.stream.tail = append(task.data, w.stream.tail...)
			w.stream.tailOffset -= int64(len(task.data))
			w.stream.uploaded -= int64(len(task.data))
			w.stream.nextIndex--
			w.materializeStreamToTempLocked(ctx)
			return 0, false, nil
		}
		w.mu.Lock()
	}
	return uint32(len(data)), true, nil
}

func (w *inodeWriteState) uploadLoop(ctx context.Context) {
	defer close(w.streamDone)
	for task := range w.streamCh {
		if err := w.streamErr.Load(); err != nil {
			break
		}
		w.stream.prepareMu.Lock()
		if !w.stream.prepared {
			releaseTag, uploadURL, err := w.fs.hub.PrepareReplaceContext(ctx, w.fs.project, w.stream.path, 1)
			if err != nil {
				w.streamErr.Store(err)
				w.stream.prepareMu.Unlock()
				break
			}
			w.stream.releaseTag = releaseTag
			w.stream.uploadURL = uploadURL
			w.stream.prepared = true
		}
		w.stream.prepareMu.Unlock()
		chunk, err := w.fs.hub.UploadChunkDataContext(
			ctx, w.fs.project,
			w.stream.releaseTag, w.stream.uploadURL,
			task.index, task.offset, task.data,
		)
		if err != nil {
			w.streamErr.Store(err)
			break
		}
		w.stream.prepareMu.Lock()
		w.stream.chunks = append(w.stream.chunks, chunk)
		w.stream.prepareMu.Unlock()
	}
}

func (w *inodeWriteState) flushStreamingChunksLocked(ctx context.Context, force bool) error {
	if w.stream == nil || w.streamDraining {
		return nil
	}
	// If the stream has errored (e.g. upload failure), recover any uploaded
	// data to the temp file so the caller sees a clean shutdown.
	if err := w.streamErr.Load(); err != nil {
		return w.materializeStreamToTempLocked(ctx)
	}
	if force && len(w.stream.tail) > 0 && w.streamCh != nil {
		w.mu.Unlock()
		select {
		case w.streamCh <- uploadTask{
			index:  w.stream.nextIndex,
			offset: w.stream.tailOffset,
			data:   w.stream.tail,
		}:
		case <-w.streamDone:
			w.mu.Lock()
			return w.materializeStreamToTempLocked(ctx)
		}
		w.mu.Lock()
		w.stream.nextIndex++
		w.stream.uploaded += int64(len(w.stream.tail))
		w.stream.tail = nil
	}
	if w.streamCh != nil {
		w.streamDraining = true
		ch := w.streamCh
		done := w.streamDone
		w.mu.Unlock()
		close(ch)
		<-done
		w.mu.Lock()
		w.streamCh = nil
		w.streamDone = nil
		w.streamDraining = false
	}
	// NOTE: w.stream is intentionally kept non-nil here so that the caller
	// (commit) can still read releaseTag and chunks for finalization.
	// The caller is responsible for setting w.stream = nil after finalization.
	return nil
}

func (w *inodeWriteState) materializeStreamToTempLocked(ctx context.Context) error {
	if w.stream == nil || w.streamDraining {
		return nil
	}
	w.streamDraining = true
	ch := w.streamCh
	done := w.streamDone
	cancel := w.streamCancel
	w.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	// materializeStreamToTemp will acquire w.mu internally
	err := w.materializeStreamToTemp(ctx)
	w.mu.Lock()
	return err
}

// materializeStreamToTemp downloads all uploaded chunks and finalizes
// the stream into the temp file. w.mu must NOT be held.
func (w *inodeWriteState) materializeStreamToTemp(ctx context.Context) error {
	w.mu.Lock()

	w.streamCh = nil
	w.streamDone = nil
	w.streamCancel = nil
	w.streamCtx = nil
	w.streamDraining = false

	if w.stream == nil {
		w.mu.Unlock()
		return nil
	}

	// Snapshot all needed state under lock, then release before network I/O
	chunks := append([]metadata.ChunkInfo(nil), w.stream.chunks...)
	tail := append([]byte(nil), w.stream.tail...)
	tailOff := w.stream.tailOffset
	logicalSize := w.logicalSize
	project := w.fs.project
	hub := w.fs.hub

	if len(chunks) == 0 && len(tail) == 0 {
		w.stream = nil
		w.mu.Unlock()
		return nil
	}

	if err := w.ensureTempLocked(); err != nil {
		w.stream = nil
		w.mu.Unlock()
		return err
	}
	if err := w.temp.Truncate(logicalSize); err != nil {
		w.stream = nil
		w.mu.Unlock()
		return err
	}
	temp := w.temp
	w.mu.Unlock()

	for _, chunk := range chunks {
		buf := make([]byte, chunk.Size)
		if err := hub.FillChunkRangeContext(ctx, project, chunk, buf); err != nil {
			return err
		}
		if _, err := temp.WriteAt(buf, chunk.Offset); err != nil {
			return err
		}
	}
	if len(tail) > 0 {
		if _, err := temp.WriteAt(tail, tailOff); err != nil {
			return err
		}
	}

	w.mu.Lock()
	w.dirtyRanges = []ByteRange{{Start: 0, End: logicalSize}}
	w.tempAuthoritative = true
	w.stream = nil
	w.mu.Unlock()

	return nil
}

func (w *inodeWriteState) readIntoLocked(ctx context.Context, dest []byte, off int64) (int, error) {
	return w.readIntoInternalLocked(ctx, dest, off)
}

func (w *inodeWriteState) readIntoExactLocked(ctx context.Context, dest []byte, off int64) (int, error) {
	return w.readIntoInternalLocked(ctx, dest, off)
}

func (w *inodeWriteState) readIntoInternalLocked(ctx context.Context, dest []byte, off int64) (int, error) {
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
		entry.AccessedAt = w.pending.ATime
		entry.ModifiedAt = w.pending.MTime
	}
	entry.Size = w.logicalSize
	entry.ChangedAt = maxTime(entry.ChangedAt, w.fs.hub.Now())
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (w *inodeWriteState) plannedRangesLocked() []ByteRange {
	chunkSize := normalizedChunkSize(w.fs.hub.ChunkSize())
	if chunkSize <= 0 {
		chunkSize = w.fs.opts.PageSize
	}
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

func (w *inodeWriteState) shouldReplaceLocked(planned []ByteRange) bool {
	fileSize := maxInt64(w.baseSize, w.logicalSize)
	if fileSize == 0 {
		return false
	}
	dirtyBytes := w.dirtyBytesLocked()
	if dirtyBytes*4 >= fileSize*3 {
		return true
	}
	if len(planned) >= 24 && dirtyBytes*2 >= fileSize {
		return true
	}
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
	bufSize := normalizedChunkSize(w.fs.hub.ChunkSize())
	if bufSize <= 0 {
		bufSize = w.fs.opts.PageSize
	}
	buf := make([]byte, bufSize)
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
	bufSize := normalizedChunkSize(w.fs.hub.ChunkSize())
	if bufSize <= 0 {
		bufSize = w.fs.opts.PageSize
	}
	buf := make([]byte, bufSize)
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
	buf := make([]byte, normalizedChunkSize(w.fs.hub.ChunkSize()))
	if len(buf) == 0 {
		buf = make([]byte, w.fs.opts.PageSize)
	}
	for _, r := range ranges {
		for offset := r.Start; offset < r.End; {
			want := int64(len(buf))
			if remaining := r.End - offset; want > remaining {
				want = remaining
			}
			n, err := w.readIntoExactLocked(ctx, buf[:want], offset)
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

func (h *storhubHandle) readFromHub(ctx context.Context, off, length int64) ([]byte, error) {
	h.mu.Lock()
	if length <= 0 {
		h.mu.Unlock()
		return []byte{}, nil
	}
	path := h.path
	project := h.fs.project
	hub := h.fs.hub
	maxConcurrent := h.fs.opts.MaxConcurrentTransfers
	chunkSize := h.fs.hub.ChunkSize()
	h.mu.Unlock()

	var ranges []struct{ off, length int64 }
	for pos := off; pos < off+length; {
		chunkEnd := (pos/chunkSize + 1) * chunkSize
		if chunkEnd > off+length {
			chunkEnd = off + length
		}
		ranges = append(ranges, struct{ off, length int64 }{pos, chunkEnd - pos})
		pos = chunkEnd
	}

	if len(ranges) == 1 || maxConcurrent <= 1 {
		data, err := hub.ReadFileAtContext(ctx, project, path, off, length)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		return data, nil
	}

	results := make([][]byte, len(ranges))
	sem := make(chan struct{}, maxConcurrent)
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var firstErr error
	var setErr sync.Once

	for i, r := range ranges {
		sem <- struct{}{}
		wg.Add(1)
		go func(idx int, rOff, rLen int64) {
			defer wg.Done()
			defer func() { <-sem }()
			d, err := hub.ReadFileAtContext(fetchCtx, project, path, rOff, rLen)
			if err != nil && !errors.Is(err, io.EOF) {
				setErr.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}
			results[idx] = d
		}(i, r.off, r.length)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	totalLen := int64(0)
	for _, d := range results {
		totalLen += int64(len(d))
	}
	merged := make([]byte, 0, totalLen)
	for _, d := range results {
		merged = append(merged, d...)
	}
	if int64(len(merged)) < length {
		length = int64(len(merged))
	}
	return merged[:length], nil
}

func (w *inodeWriteState) closeTemp() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	if w.streamCh != nil && !w.streamDraining {
		w.streamDraining = true
		ch := w.streamCh
		done := w.streamDone
		cancel := w.streamCancel
		w.mu.Unlock()
		close(ch)
		if cancel != nil {
			cancel()
		}
		<-done
		w.mu.Lock()
		w.streamCh = nil
		w.streamDone = nil
		w.streamCancel = nil
		w.streamCtx = nil
		w.stream = nil
		w.streamDraining = false
	} else if w.streamCancel != nil {
		w.streamCancel()
		w.streamCancel = nil
		w.streamCtx = nil
	}
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

func (h *storhubHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if writeState := h.writeState; writeState != nil {
		writeState.mu.Lock()
		if writeState.stream != nil {
			if err := writeState.materializeStreamToTempLocked(ctx); err != nil {
				writeState.mu.Unlock()
				return nil, errnoFromError(err)
			}
		}
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
	data, err := h.readFromHub(ctx, off, int64(len(dest)))
	if err != nil {
		return nil, errnoFromError(err)
	}
	return fuse.ReadResultData(data), 0
}

func (h *storhubHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if h.writeState == nil {
		if err := h.materialize(ctx); err != nil {
			return 0, errnoFromError(err)
		}
	}
	if h.writeState != nil {
		h.writeState.opMu.Lock()
		defer h.writeState.opMu.Unlock()
		h.writeState.mu.Lock()
		defer h.writeState.mu.Unlock()
		if h.flags&syscall.O_APPEND != 0 {
			off = h.writeState.logicalSize
		}
		if n, streamed, err := h.writeState.tryStreamWriteLocked(ctx, data, off, h.id); streamed {
			if err != nil {
				return 0, errnoFromError(err)
			}
			h.fs.debugf("stream write path=%s inode=%d off=%d bytes=%d", h.writeState.path, h.inode, off, n)
			return n, 0
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
	if err := h.materialize(ctx); err != nil {
		return 0, errnoFromError(err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.flags&syscall.O_APPEND != 0 {
		info, statErr := h.temp.Stat()
		if statErr != nil {
			return 0, errnoFromError(statErr)
		}
		off = info.Size()
	}
	n, err := h.temp.WriteAt(data, off)
	if err != nil {
		return uint32(n), errnoFromError(err)
	}
	h.fs.debugf("write path=%s inode=%d off=%d bytes=%d", h.path, h.inode, off, n)
	return uint32(n), 0
}

// retrieveKernelCache has been removed.
// The kernel guarantees it sends FUSE_WRITE for dirty pages (including mmap)
// before FUSE_RELEASE via filemap_write_and_wait_range in fuse_flush().
// The opMu lock in commit() ensures all FUSE_WRITE handlers complete before
// the commit runs, so dirtyRanges is always populated correctly.

func (h *storhubHandle) Flush(ctx context.Context) syscall.Errno {
	if h.writeState != nil {
		h.writeState.opMu.Lock()
		defer h.writeState.opMu.Unlock()
		h.writeState.mu.Lock()
		defer h.writeState.mu.Unlock()
		if h.writeState.stream != nil {
			if err := h.writeState.flushStreamingChunksLocked(ctx, true); err != nil {
				return errnoFromError(err)
			}
		}
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
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
	h.closeTemp()
	h.fs.mu.Lock()
	delete(h.fs.handles, h.id)
	h.fs.mu.Unlock()
	if h.writeState != nil {
		h.fs.releaseWriteState(h.writeState)
		h.writeState = nil
	}
	_ = ctx
	return errno
}

func (h *storhubHandle) commit(ctx context.Context) syscall.Errno {
	if h.writeState == nil {
		h.mu.Lock()
		if h.temp == nil || h.deleted || strings.TrimSpace(h.path) == "" {
			h.mu.Unlock()
			return 0
		}
		h.mu.Unlock()
		return 0
	}
	h.mu.Lock()
	handlePath := h.path
	h.mu.Unlock()
	h.writeState.opMu.Lock()
	defer h.writeState.opMu.Unlock()
	h.writeState.mu.Lock()
	if len(h.writeState.dirtyRanges) == 0 && h.writeState.logicalSize == h.writeState.baseSize && !h.writeState.hasPendingMetadataLocked() && h.writeState.stream == nil {
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
	if h.writeState.stream != nil {
		if err := h.writeState.flushStreamingChunksLocked(ctx, true); err != nil {
			h.writeState.mu.Unlock()
			h.fs.debugf("commit failed path=%s inode=%d step=stream-flush err=%v", targetPath, h.inode, err)
			return errnoFromError(err)
		}
		if len(h.writeState.dirtyRanges) > 0 {
			return h.commitTemp(ctx, targetPath, baseSize, logicalSize, pending)
		}
		return h.commitStreamReplace(ctx, targetPath, logicalSize, pending)
	}
	return h.commitTemp(ctx, targetPath, baseSize, logicalSize, pending)
}

// commitStreamReplace handles the streaming-chunks path.
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu.
func (h *storhubHandle) commitStreamReplace(ctx context.Context, targetPath string, logicalSize int64, pending shfs.MetadataPatch) syscall.Errno {
	releaseTag := h.writeState.stream.releaseTag
	chunks := append([]metadata.ChunkInfo(nil), h.writeState.stream.chunks...)
	h.writeState.mu.Unlock()
	h.fs.debugf("commit stream-replace path=%s inode=%d base=%d size=%d chunks=%d", targetPath, h.inode, h.writeState.baseSize, logicalSize, len(chunks))
	if _, err := h.fs.hub.FinalizeReplaceChunksContext(ctx, h.fs.project, targetPath, releaseTag, logicalSize, chunks); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=stream-replace err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	h.writeState.mu.Lock()
	h.writeState.baseSize = logicalSize
	h.writeState.dirtyRanges = nil
	h.writeState.tempAuthoritative = false
	h.writeState.stream = nil
	h.writeState.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending)
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
	if h.writeState.shouldChunkRewriteLocked(planned) {
		return h.commitChunkRewrite(ctx, targetPath, logicalSize, planned, pending)
	}
	if h.writeState.shouldReplaceLocked(planned) {
		return h.commitReplace(ctx, targetPath, logicalSize, planned, pending)
	}
	return h.commitPatch(ctx, targetPath, baseSize, logicalSize, planned, pending)
}

// commitChunkRewrite handles the chunk-rewrite path.
// Caller must hold h.writeState.mu. Releases and re-acquires h.writeState.mu.
func (h *storhubHandle) commitChunkRewrite(ctx context.Context, targetPath string, logicalSize int64, planned []ByteRange, pending shfs.MetadataPatch) syscall.Errno {
	snapshotPath, err := h.writeState.createRangeSnapshotLocked(ctx, planned)
	h.writeState.mu.Unlock()
	if err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=range-snapshot err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	defer os.Remove(snapshotPath)
	h.fs.debugf("commit chunk-rewrite path=%s inode=%d base=%d size=%d ranges=%d", targetPath, h.inode, h.writeState.baseSize, logicalSize, len(planned))
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
	h.writeState.mu.Unlock()
	if err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=full-snapshot err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	if cleanupSnapshot {
		defer os.Remove(snapshotPath)
	}
	h.fs.debugf("commit replace path=%s inode=%d base=%d size=%d dirty_ranges=%d", targetPath, h.inode, h.writeState.baseSize, logicalSize, len(h.writeState.dirtyRanges))
	if _, err := h.fs.hub.ReplaceFileContext(ctx, h.fs.project, targetPath, snapshotPath); err != nil {
		h.fs.debugf("commit failed path=%s inode=%d step=replace err=%v", targetPath, h.inode, err)
		return errnoFromError(err)
	}
	h.writeState.mu.Lock()
	h.writeState.commitCacheRefreshLocked(logicalSize)
	h.writeState.mu.Unlock()
	return h.commitPostUpdate(ctx, targetPath, pending)
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

func (h *storhubHandle) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	_ = flags
	for {
		if errno := h.fs.setLock(h.inode, owner, *lk); errno == 0 {
			h.trackLockOwner(owner, lk.Typ)
			return 0
		}
		select {
		case <-ctx.Done():
			return errnoFromError(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (s *Filesystem) setLock(inode, owner uint64, lk fuse.FileLock) syscall.Errno {
	s.mu.Lock()
	defer s.mu.Unlock()
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
		h.fs.setLock(h.inode, owner, fuse.FileLock{Start: 0, End: 0, Typ: syscall.F_UNLCK})
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

func safeNotifyContent(node *storhubNode) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			logging.Error(nil, "panic in NotifyContent", "panic", r, "stack", string(buf[:n]))
		}
	}()
	if node == nil {
		return
	}
	_ = node.NotifyContent(0, 0)
}

func safeNotifyEntry(node *storhubNode, name string) {
	if node == nil {
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
	if parent == nil {
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
		writeState.mu.Unlock()
	}
	graceThreshold := time.Now().Add(-5 * time.Minute)
	entries, err := os.ReadDir(s.cacheDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
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

func (s *Filesystem) renameWithReplace(ctx context.Context, oldPath, newPath string, flags uint32) error {
	if flags&renameExchange != 0 || flags&renameWhiteout != 0 {
		return syscall.EINVAL
	}
	oldEntry, _ := s.hub.StatPathContext(ctx, s.project, oldPath)
	newEntry, _ := s.hub.StatPathContext(ctx, s.project, newPath)
	if newEntry != nil && (oldEntry == nil || newEntry.Inode != oldEntry.Inode) {
		if err := s.materializeHandlesForPath(ctx, newEntry.Inode, newPath); err != nil {
			return err
		}
	}
	_, err := s.hub.UpdateRepoMetadataContext(ctx, s.project, func(meta *metadata.RepoMetadata) error {
		if err := shfs.CheckParentWrite(ctx, meta, oldPath); err != nil {
			return err
		}
		if err := shfs.CheckParentWrite(ctx, meta, newPath); err != nil {
			return err
		}
		if err := shfs.CheckTraverse(ctx, meta, oldPath); err != nil {
			return err
		}
		if parent := shfs.ParentPath(newPath); parent != "" && !meta.HasDirectory(parent) {
			return fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
		}
		if flags&renameNoReplace != 0 && (meta.FindFile(newPath) != nil || meta.HasDirectory(newPath)) {
			return shfs.AlreadyExists(newPath)
		}
		if file := meta.FindFile(oldPath); file != nil {
			if existingDir := meta.GetDirectory(newPath); existingDir != nil {
				return shfs.AlreadyExists(newPath)
			}
			meta.RemoveFile(newPath)
			renamed := file.Clone()
			renamed.Name = newPath
			renamed.ChangedAt = s.hub.Now()
			meta.RemoveFile(oldPath)
			meta.UpsertFile(renamed, s.hub.Now())
			return nil
		}
		if !meta.HasDirectory(oldPath) {
			return shfs.NotFound(oldPath)
		}
		if shfs.IsParentOrSame(oldPath, newPath) {
			return fmt.Errorf("cannot move directory %s into itself %s", oldPath, newPath)
		}
		if file := meta.FindFile(newPath); file != nil {
			return shfs.AlreadyExists(newPath)
		}
		if dir := meta.GetDirectory(newPath); dir != nil {
			childDirs, childFiles := meta.DirectoryChildren(newPath)
			if len(childDirs) > 0 || len(childFiles) > 0 {
				return shfs.NotEmpty(newPath)
			}
			meta.RemoveDirectory(dir.Path)
		}
		for i := range meta.Directories {
			if shfs.IsParentOrSame(oldPath, meta.Directories[i].Path) {
				meta.Directories[i].Path = shfs.RemapPath(oldPath, newPath, meta.Directories[i].Path)
				meta.Directories[i].ModifiedAt = s.hub.Now()
				meta.Directories[i].ChangedAt = s.hub.Now()
			}
		}
		for i := range meta.Releases {
			for j := range meta.Releases[i].Files {
				if shfs.IsParentOrSame(oldPath, meta.Releases[i].Files[j].Name) {
					meta.Releases[i].Files[j].Name = shfs.RemapPath(oldPath, newPath, meta.Releases[i].Files[j].Name)
					meta.Releases[i].Files[j].ChangedAt = s.hub.Now()
				}
			}
		}
		meta.RecomputeStats()
		return nil
	}, fmt.Sprintf("storhub: rename %s to %s", oldPath, newPath))
	if err != nil {
		return err
	}
	if oldEntry != nil {
		s.remapPaths(oldPath, newPath)
	}
	if newEntry != nil && (oldEntry == nil || newEntry.Inode != oldEntry.Inode) {
		remaining := s.dropPath(newEntry.Inode, newPath)
		s.rebindHandlesAfterPathChange(newEntry.Inode, newPath, remaining)
	}
	return err
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
	attr.SetTimes(&entry.AccessedAt, &entry.ModifiedAt, &entry.ChangedAt)
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

func errnoFromError(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errno, ok := err.(syscall.Errno); ok {
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

func entryInfoFromFile(file *metadata.FileMetadata) *shfs.EntryInfo {
	return &shfs.EntryInfo{
		Path:          file.Name,
		Kind:          file.Kind,
		IsSymlink:     file.Kind == metadata.NodeKindSymlink,
		Size:          file.Size,
		Inode:         file.Inode,
		Mode:          file.Mode,
		UID:           file.UID,
		GID:           file.GID,
		NLink:         file.NLink,
		CreatedAt:     file.UploadedAt,
		ModifiedAt:    file.ModifiedAt,
		AccessedAt:    file.AccessedAt,
		ChangedAt:     file.ChangedAt,
		SymlinkTarget: file.SymlinkTarget,
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
		if !(ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-') {
			return fmt.Errorf("invalid project name: %s", project)
		}
	}
	if strings.HasPrefix(project, ".") || strings.HasSuffix(project, ".") {
		return fmt.Errorf("invalid project name: %s", project)
	}
	return nil
}

func normalizedChunkSize(chunkSize int64) int64 {
	if chunkSize <= 0 {
		chunkSize = chunking.DefaultChunkSize
	}
	const maxReleaseAssetSize = int64(2 * 1024 * 1024 * 1024)
	if chunkSize > maxReleaseAssetSize {
		return maxReleaseAssetSize
	}
	return chunkSize
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
