package fusefs

import (
	"context"
	shfs "github.com/FarelRA/storhub/internal/fs"
	metadata "github.com/FarelRA/storhub/internal/metadata"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"syscall"
)

type storhubNode struct {
	gofusefs.Inode
	fs    *Filesystem
	inode uint64
	// kind spans file/symlink only (the wire omits a dir kind), so
	// isDir is load-bearing rather than derivable: directory nodes
	// carry an empty kind.
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
// closeTemp on another handle of the same inode. Accepted cost: the scan
// is O(open handles) under the FS write lock per kernel forget. A
// per-inode open-handle refcount would avoid it, but the scan is
// allocation-free and forgets are kernel-paced (one per evicted inode,
// not per I/O), so measured forget latency stays in the microsecond
// range at thousands of handles; revisit with a refcount if a profile
// ever shows forgetNodeBookkeeping hot.
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

// Invalidations reports how many kernel-cache invalidation requests this
// filesystem has issued. Every namespace or attribute mutation must move
// this counter; the 60s entry/attr timeouts turn a missed invalidation
// into a minute of stale reads.
//
// Two networks share this counter, deliberately split: mutation paths
// push entry/content notifications synchronously (notifyEntryForPath and
// friends), while the fan-out subscription pulls cross-mount publishes
// through pollInvalidationsOnce. Push covers our own writes, fan-out
// covers everyone else's; neither subsumes the other.
func (s *Filesystem) Invalidations() uint64 {
	return s.invalCount.Load()
}

func (s *Filesystem) pathForInode(inode uint64) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for p := range s.inodePaths[inode] {
		return p
	}
	return ""
}

// safePath resolves the node's current path for hub operations: the one
// spelling every operation uses. A pathless node is normally the root
// (inode 1); any other pathless node has been deleted or renamed away,
// and "" would make every hub call below silently address the root
// directory. Such nodes report ESTALE instead.
func (n *storhubNode) safePath() (string, syscall.Errno) {
	targetPath := n.fs.pathForInode(n.inode)
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

func (s *Filesystem) rememberPathLocked(inode uint64, targetPath string) {
	if s.inodePaths[inode] == nil {
		s.inodePaths[inode] = make(map[string]struct{})
	}
	s.inodePaths[inode][targetPath] = struct{}{}
	s.pathToInode[targetPath] = inode
}

func (s *Filesystem) rememberPath(inode uint64, targetPath string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rememberPathLocked(inode, targetPath)
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

// repathHandles rewrites handle paths after a namespace move: every
// handle matching match follows remap to its replacement. An empty
// replacement detaches the handle into unlinked-but-open semantics: reads
// keep serving its own snapshot instead of silently switching to the
// replacement's content. When the inode instead keeps a registered path
// (a hardlink survived the unlink), handles follow it so writes through
// open fds land in the surviving name (POSIX). Handles snapshot under a
// read lock first; handle locks are leaf locks, so the graph stays
// acyclic.
func (s *Filesystem) repathHandles(match func(inode uint64, path string) bool, remap func(string) string) {
	s.mu.RLock()
	handles := make([]*storhubHandle, 0, len(s.handles))
	for _, handle := range s.handles {
		handles = append(handles, handle)
	}
	s.mu.RUnlock()
	for _, handle := range handles {
		handle.mu.Lock()
		if match(handle.inode, handle.path) {
			if next := remap(handle.path); next != "" {
				handle.path = next
			} else {
				handle.path = ""
				handle.deleted = true
			}
		}
		handle.mu.Unlock()
	}
}

// repathWriteStates rewrites write-state paths the same way repathHandles
// rewrites handle paths. Lock order is always opMu before mu, matching
// commit: commit holds opMu across its DAC window plus network window, so
// taking opMu here closes the stale-path race, and the snapshot above was
// taken without holding opMu, so no lock cycle with committers. The order
// is stated here once for every path rebind.
func (s *Filesystem) repathWriteStates(match func(inode uint64, path string) bool, remap func(string) string) {
	s.mu.RLock()
	states := make([]*inodeWriteState, 0, len(s.writeStates))
	for _, state := range s.writeStates {
		states = append(states, state)
	}
	s.mu.RUnlock()
	for _, state := range states {
		state.opMu.Lock()
		state.mu.Lock()
		if match(state.inode, state.path) {
			if next := remap(state.path); next != "" {
				state.path = next
			} else {
				state.path = ""
				state.deleted = true
			}
		}
		state.mu.Unlock()
		s.unlockOpMu(&state.opMu)
	}
}

func (s *Filesystem) rebindHandlesAfterPathChange(inode uint64, oldPath, newPath string) {
	match := func(id uint64, path string) bool { return id == inode && path == oldPath }
	remap := func(string) string { return newPath }
	s.repathHandles(match, remap)
	s.repathWriteStates(match, remap)
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
	s.mu.Unlock()
	match := func(_ uint64, path string) bool { return shfs.IsParentOrSame(oldPath, path) }
	remap := func(path string) string { return shfs.RemapPath(oldPath, newPath, path) }
	s.repathHandles(match, remap)
	s.repathWriteStates(match, remap)
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

func (s *Filesystem) ensureNode(_ context.Context, entry *shfs.EntryInfo) *storhubNode {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node := s.nodes[entry.Inode]; node != nil {
		s.rememberPathLocked(entry.Inode, entry.Path)
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
	// Gen stays 1: the backend has no inode generation, identity is the
	// inode number alone. The mount root pins the same Gen in Mount.
	return gofusefs.StableAttr{Mode: mode, Ino: n.inode, Gen: 1}
}

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
		if r := recover(); r != nil {
			n.fs.errorOp("attachChild failed", "path", n.fs.pathForInode(n.inode), "inode", child.inode, "err", r)
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
