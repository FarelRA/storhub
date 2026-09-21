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
			n.fs.debugOp("attachChild recovered from panic", "path", n.currentPath(), "inode", child.inode)
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
