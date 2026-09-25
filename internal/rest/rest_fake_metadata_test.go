package rest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	storage "github.com/FarelRA/storhub/internal/storage"
)

func (c *fakeRESTClient) StatPathContext(ctx context.Context, project, targetPath string) (*EntryInfo, error) {
	c.mu.Lock()
	c.recordIdentityLocked(ctx)
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	clean, err := cleanRESTPathAllowRoot(targetPath)
	if err != nil {
		return nil, err
	}
	if node, ok := p.files[clean]; ok {
		entry := *node.entry
		entry.Size = int64(len(node.data.bytes))
		entry.NLink = node.data.nlink
		if node.data.kind == NodeKindSymlink {
			entry.IsSymlink = true
			entry.Kind = NodeKindSymlink
			entry.SymlinkTarget = node.data.target
			entry.Size = int64(len(node.data.target))
		}
		return &entry, nil
	}
	if node, ok := p.dirs[clean]; ok {
		entry := *node.entry
		return &entry, nil
	}
	return nil, shfs.NotFound(clean)
}

func (c *fakeRESTClient) ReadDirContext(_ context.Context, project, dirPath string) ([]DirEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	clean, err := cleanRESTPathAllowRoot(dirPath)
	if err != nil {
		return nil, err
	}
	if _, ok := p.files[clean]; ok {
		return nil, shfs.NotDirectory(clean)
	}
	if _, ok := p.dirs[clean]; !ok {
		return nil, shfs.NotFound(clean)
	}
	entries := []DirEntry{}
	for name, node := range p.dirs {
		if name != "" && parentPath(name) == clean {
			entries = append(entries, DirEntry{Name: path.Base(name), Path: name, IsDir: true, Inode: node.entry.Inode, Mode: node.entry.Mode, NLink: node.entry.NLink})
		}
	}
	for name, node := range p.files {
		if parentPath(name) == clean {
			entries = append(entries, DirEntry{Name: path.Base(name), Path: name, IsSymlink: node.data.kind == NodeKindSymlink, Kind: node.data.kind, Size: int64(len(node.data.bytes)), Inode: node.entry.Inode, Mode: node.entry.Mode, NLink: node.data.nlink})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

func (c *fakeRESTClient) StatFSContext(_ context.Context, project string) (*FSStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	bytesTotal := int64(0)
	for _, node := range p.files {
		if node.data.kind == NodeKindSymlink {
			bytesTotal += int64(len(node.data.target))
			continue
		}
		bytesTotal += int64(len(node.data.bytes))
	}
	inodes := len(p.dirs)
	seen := map[uint64]struct{}{}
	for _, node := range p.files {
		seen[node.entry.Inode] = struct{}{}
	}
	inodes += len(seen)
	return &FSStats{Files: len(p.files), Directories: len(p.dirs), Inodes: inodes, Bytes: bytesTotal, Releases: len(p.revisions), Assets: len(p.files)}, nil
}

// --- links (symlink/readlink/link verbs) ---

func (c *fakeRESTClient) SymlinkContext(_ context.Context, project, target, linkPath string) (*FileMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, clean, err := c.prepareFileCreate(project, linkPath)
	if err != nil {
		return nil, err
	}
	now := c.tick()
	node := &fakeRESTNode{
		entry: &EntryInfo{Path: clean, Kind: NodeKindSymlink, IsSymlink: true, Size: int64(len(target)), Inode: c.allocInode(), Mode: 0o777, UID: 1000, GID: 1000, NLink: 1, CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now, SymlinkTarget: target},
		xattr: map[string][]byte{},
		data:  &fakeRESTData{kind: NodeKindSymlink, target: target, nlink: 1},
	}
	p.files[clean] = node
	c.recordRevisionLocked(p, "symlink "+clean)
	return &FileMetadata{Symlink: target, Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) ReadlinkContext(_ context.Context, project, linkPath string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, _, err := c.requireReadableFile(project, linkPath)
	if err != nil {
		return "", err
	}
	if node.data.kind != NodeKindSymlink {
		return "", shfs.InvalidSymlink(linkPath)
	}
	return node.data.target, nil
}

func (c *fakeRESTClient) LinkContext(_ context.Context, project, existingPath, newPath string) (*FileMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, clean, err := c.prepareFileCreate(project, newPath)
	if err != nil {
		return nil, err
	}
	source, sourcePath, err := c.requireReadableFileLocked(p, existingPath)
	if err != nil {
		return nil, err
	}
	if source.data.kind != NodeKindFile {
		return nil, errors.New("hard links only support regular files: " + sourcePath)
	}
	source.data.nlink++
	now := c.tick()
	node := &fakeRESTNode{entry: &EntryInfo{Path: clean, Size: int64(len(source.data.bytes)), Inode: source.entry.Inode, Mode: source.entry.Mode, UID: source.entry.UID, GID: source.entry.GID, NLink: source.data.nlink, CreatedAt: source.entry.CreatedAt, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, xattr: cloneBytesMap(source.xattr), data: source.data}
	p.files[clean] = node
	c.syncLinksLocked(p, source.data)
	c.recordRevisionLocked(p, "link "+clean)
	return &FileMetadata{Inode: node.entry.Inode}, nil
}

// --- metadata verbs (chmod/chown/utimes) ---

func (c *fakeRESTClient) ChmodContext(_ context.Context, project, targetPath string, mode uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	now := c.tick()
	node.entry.Mode = mode
	node.entry.ChangedAt = now
	return nil
}

func (c *fakeRESTClient) ChownContext(_ context.Context, project, targetPath string, uid, gid uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	now := c.tick()
	// The all-ones value is the chown(2) leave-unchanged sentinel the
	// real backend honors; the fake mirrors it so keep-spelling tests do
	// not corrupt the seeded tree.
	const keepOwner = ^uint32(0)
	if uid != keepOwner {
		node.entry.UID = uid
	}
	if gid != keepOwner {
		node.entry.GID = gid
	}
	node.entry.Mode &^= 0o6000
	node.entry.ChangedAt = now
	return nil
}

func (c *fakeRESTClient) ChtimesContext(_ context.Context, project, targetPath string, atime, mtime int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	now := c.tick()
	// Caller stamps arrive as Unix nanoseconds and persist verbatim: no
	// truncation, matching the ns storage contract.
	node.entry.AccessedAt = atime
	node.entry.ModifiedAt = mtime
	node.entry.ChangedAt = now
	return nil
}

func (c *fakeRESTClient) ChtimesExplicitContext(_ context.Context, project, targetPath string, atime, mtime *time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	now := c.tick()
	if atime != nil {
		node.entry.AccessedAt = atime.UnixNano()
	}
	if mtime != nil {
		node.entry.ModifiedAt = mtime.UnixNano()
	}
	node.entry.ChangedAt = now
	return nil
}

// --- xattrs ---

func (c *fakeRESTClient) SetXAttrContext(_ context.Context, project, targetPath, attr string, data []byte, _ ...shfs.XAttrMode) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	node.xattr[attr] = append([]byte(nil), data...)
	return nil
}

func (c *fakeRESTClient) GetXAttrContext(_ context.Context, project, targetPath, attr string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return nil, err
	}
	value, ok := node.xattr[attr]
	if !ok {
		return nil, shfs.XAttrNotFound(targetPath)
	}
	return append([]byte(nil), value...), nil
}

func (c *fakeRESTClient) RemoveXAttrContext(_ context.Context, project, targetPath, attr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	if _, ok := node.xattr[attr]; !ok {
		return shfs.XAttrNotFound(targetPath)
	}
	delete(node.xattr, attr)
	return nil
}

func (c *fakeRESTClient) ListXAttrContext(_ context.Context, project, targetPath string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(node.xattr))
	for name := range node.xattr {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// --- ops (revisions/rollback/purge/revert/prune/delete-project) ---

func (c *fakeRESTClient) ListMetadataRevisionsContext(_ context.Context, project string) ([]MetadataRevision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	return append([]MetadataRevision(nil), p.revisions...), nil
}

func (c *fakeRESTClient) RollbackMetadataContext(_ context.Context, project, commitSHA string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return err
	}
	for _, revision := range p.revisions {
		if revision.CommitSHA == commitSHA {
			c.rollbacks = append(c.rollbacks, commitSHA)
			return nil
		}
	}
	return shfs.NotFound(fmt.Sprintf("revision %s", commitSHA))
}

func (c *fakeRESTClient) PruneContext(_ context.Context, project, scope string, _ int, dryRun bool) (*storage.PruneResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.getExistingProject(project); err != nil {
		return nil, err
	}
	// Mirror the storage layer's scope contract so tests cannot mask a
	// missing REST-layer validation with an over-permissive fake.
	switch scope {
	case "objects", "assets", "history", "chunks", "all":
	default:
		return nil, fmt.Errorf("unknown prune scope %q (want objects|assets|history|chunks|all)", scope)
	}
	return &storage.PruneResult{Scope: storage.PruneScope(scope), DryRun: dryRun}, nil
}

func (c *fakeRESTClient) DegradedProjects() ([]string, error) {
	return nil, nil
}

func (c *fakeRESTClient) ReEnableProject(_ string) error {
	return nil
}

func (c *fakeRESTClient) PressureSnapshot() (storage.PressureSnapshot, error) {
	return storage.PressureSnapshot{}, nil
}

func (c *fakeRESTClient) PressureFailureStreak(_ string) (uint64, error) { return 0, nil }

func (c *fakeRESTClient) PressurePendingDepth(_ string) (int, error) { return 0, nil }

func (c *fakeRESTClient) RevertPathContext(_ context.Context, project, path, commitSHA string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.getExistingProject(project); err != nil {
		return err
	}
	c.revertPaths = append(c.revertPaths, path+"@"+commitSHA)
	return nil
}

func (c *fakeRESTClient) DeleteProjectContext(_ context.Context, project string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.projects[project]; !ok {
		return ErrNotFound
	}
	delete(c.projects, project)
	c.deleted[project] = true
	return nil
}

// --- fake internals (fixture tree, fault switches, call records) ---

func (c *fakeRESTClient) project(project string) *fakeRESTProject {
	p, ok := c.projects[project]
	if ok {
		return p
	}
	p = &fakeRESTProject{dirs: map[string]*fakeRESTNode{"": {entry: &EntryInfo{Path: "", IsDir: true, Inode: 1, Mode: 0o755, UID: 1000, GID: 1000, NLink: 1, CreatedAt: c.now, ModifiedAt: c.now, AccessedAt: c.now, ChangedAt: c.now}, xattr: map[string][]byte{}}}, files: make(map[string]*fakeRESTNode), revisions: []MetadataRevision{{CommitSHA: "init", Message: "init", CommittedAt: c.now}}}
	c.projects[project] = p
	return p
}

func (c *fakeRESTClient) getExistingProject(project string) (*fakeRESTProject, error) {
	p, ok := c.projects[project]
	if !ok {
		return nil, ErrNotFound
	}
	return p, nil
}

func (c *fakeRESTClient) prepareFileCreate(project, filePath string) (*fakeRESTProject, string, error) {
	p := c.project(project)
	clean, err := cleanRESTPath(filePath)
	if err != nil {
		return nil, "", err
	}
	if _, ok := p.files[clean]; ok {
		return nil, "", shfs.AlreadyExists(clean)
	}
	if _, ok := p.dirs[clean]; ok {
		return nil, "", shfs.AlreadyExists(clean)
	}
	if parent := parentPath(clean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return nil, "", fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
		}
	}
	return p, clean, nil
}

func (c *fakeRESTClient) requireWritableFile(project, filePath string) (*fakeRESTNode, string, error) {
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, "", err
	}
	return c.requireWritableFileLocked(p, filePath)
}

func (c *fakeRESTClient) requireWritableFileLocked(p *fakeRESTProject, filePath string) (*fakeRESTNode, string, error) {
	node, clean, err := c.requireReadableFileLocked(p, filePath)
	if err != nil {
		return nil, "", err
	}
	if node.data.kind == NodeKindSymlink {
		return nil, "", shfs.InvalidSymlink(clean)
	}
	return node, clean, nil
}

func (c *fakeRESTClient) requireReadableFile(project, filePath string) (*fakeRESTNode, string, error) {
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, "", err
	}
	return c.requireReadableFileLocked(p, filePath)
}

func (c *fakeRESTClient) requireReadableFileLocked(p *fakeRESTProject, filePath string) (*fakeRESTNode, string, error) {
	clean, err := cleanRESTPath(filePath)
	if err != nil {
		return nil, "", err
	}
	node, ok := p.files[clean]
	if !ok {
		return nil, "", shfs.NotFound(clean)
	}
	return node, clean, nil
}

func (c *fakeRESTClient) lookupNode(project, targetPath string) (*fakeRESTNode, error) {
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	clean, err := cleanRESTPathAllowRoot(targetPath)
	if err != nil {
		return nil, err
	}
	if node, ok := p.files[clean]; ok {
		return node, nil
	}
	if node, ok := p.dirs[clean]; ok {
		return node, nil
	}
	return nil, shfs.NotFound(clean)
}

func (c *fakeRESTClient) syncLinksLocked(p *fakeRESTProject, data *fakeRESTData) {
	for _, node := range p.files {
		if node.data == data {
			node.entry.NLink = data.nlink
			node.entry.Size = int64(len(data.bytes))
			node.entry.ModifiedAt = c.now
			node.entry.ChangedAt = c.now
		}
	}
}

func (c *fakeRESTClient) touchDataLocked(p *fakeRESTProject, data *fakeRESTData, now int64) {
	c.now = now
	for _, node := range p.files {
		if node.data == data {
			node.entry.Size = int64(len(data.bytes))
			node.entry.ModifiedAt = now
			node.entry.ChangedAt = now
		}
	}
	c.recordRevisionLocked(p, "write")
}

func (c *fakeRESTClient) recordRevisionLocked(p *fakeRESTProject, message string) {
	// Git-shaped object ids: the REST layer validates commit SHAs as hex,
	// so the fake must not hand out values its own client would reject.
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", message, len(p.revisions))))
	p.revisions = append([]MetadataRevision{{CommitSHA: hex.EncodeToString(sum[:20]), Message: message, CommittedAt: c.now}}, p.revisions...)
}

func (c *fakeRESTClient) allocInode() uint64 {
	ino := c.nextInode
	c.nextInode++
	return ino
}

// tick advances the fake clock by one nanosecond and reports it, so every
// minted entry stamp is a distinct Unix-nanosecond value.
func (c *fakeRESTClient) tick() int64 {
	c.now++
	return c.now
}

func cloneBytesMap(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

func cleanRESTPath(raw string) (string, error) {
	clean, err := cleanRESTPathAllowRoot(raw)
	if err != nil {
		return "", err
	}
	if clean == "" {
		return "", errors.New("path is required")
	}
	return clean, nil
}

func cleanRESTPathAllowRoot(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "/" {
		return "", nil
	}
	clean := path.Clean("/" + raw)
	clean = strings.TrimPrefix(clean, "/")
	if clean == "." {
		return "", nil
	}
	if strings.HasPrefix(clean, "../") || clean == ".." {
		return "", errors.New("invalid path")
	}
	return clean, nil
}

func parentPath(p string) string {
	if p == "" {
		return ""
	}
	parent := path.Dir(p)
	if parent == "." {
		return ""
	}
	return parent
}
