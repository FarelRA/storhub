package rest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func (c *fakeRESTClient) CreateFileContext(_ context.Context, project, filePath string) (*FileMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, clean, err := c.prepareFileCreate(project, filePath)
	if err != nil {
		return nil, err
	}
	now := c.tick()
	node := &fakeRESTNode{
		entry: &EntryInfo{Path: clean, Size: 0, Inode: c.allocInode(), Mode: 0o644, UID: 1000, GID: 1000, NLink: 1, CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now},
		xattr: map[string][]byte{},
		data:  &fakeRESTData{bytes: []byte{}, nlink: 1, kind: NodeKindFile},
	}
	p.files[clean] = node
	c.recordRevisionLocked(p, "create "+clean)
	return &FileMetadata{Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) MkdirContext(_ context.Context, project, dirPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.project(project)
	clean, err := cleanRESTPath(dirPath)
	if err != nil {
		return err
	}
	if clean == "" {
		return nil
	}
	if _, ok := p.dirs[clean]; ok {
		return shfs.AlreadyExists(clean)
	}
	if _, ok := p.files[clean]; ok {
		return shfs.AlreadyExists(clean)
	}
	if parent := parentPath(clean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
		}
	}
	now := c.tick()
	p.dirs[clean] = &fakeRESTNode{entry: &EntryInfo{Path: clean, IsDir: true, Inode: c.allocInode(), Mode: 0o755, UID: 1000, GID: 1000, NLink: 1, CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, xattr: map[string][]byte{}}
	c.recordRevisionLocked(p, "mkdir "+clean)
	return nil
}

func (c *fakeRESTClient) DeleteFileContext(_ context.Context, project, filePath string, opts ...shfs.MutateOption) error {
	c.recordOpts(opts)
	if err := c.consumeOptErr(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return err
	}
	clean, err := cleanRESTPath(filePath)
	if err != nil {
		return err
	}
	node, ok := p.files[clean]
	if !ok {
		if _, ok := p.dirs[clean]; ok {
			return shfs.IsDirectory(clean)
		}
		return shfs.NotFound(clean)
	}
	delete(p.files, clean)
	if node.data != nil && node.data.nlink > 0 {
		node.data.nlink--
		c.syncLinksLocked(p, node.data)
	}
	c.recordRevisionLocked(p, "delete "+clean)
	return nil
}

func (c *fakeRESTClient) RmdirContext(_ context.Context, project, dirPath string, opts ...shfs.MutateOption) error {
	c.recordOpts(opts)
	if err := c.consumeOptErr(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return err
	}
	clean, err := cleanRESTPath(dirPath)
	if err != nil {
		return err
	}
	if clean == "" {
		return fmt.Errorf("cannot remove root directory")
	}
	if _, ok := p.files[clean]; ok {
		return shfs.NotDirectory(clean)
	}
	if _, ok := p.dirs[clean]; !ok {
		return shfs.NotFound(clean)
	}
	for name := range p.dirs {
		if parentPath(name) == clean {
			return shfs.NotEmpty(clean)
		}
	}
	for name := range p.files {
		if parentPath(name) == clean {
			return shfs.NotEmpty(clean)
		}
	}
	delete(p.dirs, clean)
	c.recordRevisionLocked(p, "rmdir "+clean)
	return nil
}

func (c *fakeRESTClient) RenameContext(_ context.Context, project, oldPath, newPath string, opts ...shfs.MutateOption) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return err
	}
	oldClean, err := cleanRESTPath(oldPath)
	if err != nil {
		return err
	}
	newClean, err := cleanRESTPath(newPath)
	if err != nil {
		return err
	}
	if _, ok := p.files[newClean]; ok {
		// RENAME_NOREPLACE is enforced here; plain renames replace the
		// destination like the real backend.
		if shfs.ApplyMutateOptions(opts).NoReplace() {
			return shfs.AlreadyExists(newClean)
		}
		delete(p.files, newClean)
	}
	if _, ok := p.dirs[newClean]; ok {
		return shfs.AlreadyExists(newClean)
	}
	if parent := parentPath(newClean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
		}
	}
	if node, ok := p.files[oldClean]; ok {
		delete(p.files, oldClean)
		now := c.tick()
		node.entry.Path = newClean
		node.entry.ChangedAt = now
		node.entry.ModifiedAt = now
		p.files[newClean] = node
		c.recordRevisionLocked(p, "rename "+oldClean+" to "+newClean)
		return nil
	}
	if _, ok := p.dirs[oldClean]; !ok {
		return shfs.NotFound(oldClean)
	}
	now := c.tick()
	updatedDirs := make(map[string]*fakeRESTNode, len(p.dirs))
	for name, node := range p.dirs {
		if name == oldClean || strings.HasPrefix(name, oldClean+"/") {
			remapped := strings.TrimPrefix(name, oldClean)
			name = newClean + remapped
			node.entry.Path = name
			node.entry.ChangedAt = now
			node.entry.ModifiedAt = now
		}
		updatedDirs[name] = node
	}
	p.dirs = updatedDirs
	updatedFiles := make(map[string]*fakeRESTNode, len(p.files))
	for name, node := range p.files {
		if strings.HasPrefix(name, oldClean+"/") {
			remapped := strings.TrimPrefix(name, oldClean)
			name = newClean + remapped
			node.entry.Path = name
			node.entry.ChangedAt = now
			node.entry.ModifiedAt = now
		}
		updatedFiles[name] = node
	}
	p.files = updatedFiles
	c.recordRevisionLocked(p, "rename "+oldClean+" to "+newClean)
	return nil
}

func (c *fakeRESTClient) CopyContext(_ context.Context, project, srcPath, dstPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return err
	}
	srcClean, err := cleanRESTPath(srcPath)
	if err != nil {
		return err
	}
	dstClean, err := cleanRESTPath(dstPath)
	if err != nil {
		return err
	}
	if srcClean == dstClean {
		return shfs.AlreadyExists(srcClean)
	}
	if _, ok := p.files[dstClean]; ok {
		return shfs.AlreadyExists(dstClean)
	}
	if _, ok := p.dirs[dstClean]; ok {
		return shfs.AlreadyExists(dstClean)
	}
	if parent := parentPath(dstClean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
		}
	}
	if node, ok := p.files[srcClean]; ok {
		now := c.tick()
		cloned := *node
		cloned.entry = &shfs.EntryInfo{
			Path:       dstClean,
			IsDir:      node.entry.IsDir,
			IsSymlink:  node.entry.IsSymlink,
			Size:       node.entry.Size,
			Inode:      c.nextInode,
			Mode:       node.entry.Mode,
			UID:        node.entry.UID,
			GID:        node.entry.GID,
			CreatedAt:  now,
			ModifiedAt: now,
			ChangedAt:  now,
		}
		c.nextInode++
		p.files[dstClean] = &cloned
		c.recordRevisionLocked(p, "copy "+srcClean+" to "+dstClean)
		return nil
	}
	if _, ok := p.dirs[srcClean]; !ok {
		return shfs.NotFound(srcClean)
	}
	if strings.HasPrefix(dstClean, srcClean+"/") {
		return fmt.Errorf("cannot copy directory %s into itself %s", srcClean, dstClean)
	}
	now := c.tick()
	dirsToAdd := make(map[string]*fakeRESTNode)
	for name, node := range p.dirs {
		if name == srcClean || strings.HasPrefix(name, srcClean+"/") {
			remapped := strings.TrimPrefix(name, srcClean)
			newName := dstClean + remapped
			if _, exists := p.dirs[newName]; exists {
				return shfs.AlreadyExists(newName)
			}
			if _, exists := p.files[newName]; exists {
				return shfs.AlreadyExists(newName)
			}
			cloned := *node
			cloned.entry = &shfs.EntryInfo{
				Path:       newName,
				IsDir:      true,
				Inode:      c.nextInode,
				Mode:       node.entry.Mode,
				UID:        node.entry.UID,
				GID:        node.entry.GID,
				CreatedAt:  now,
				ModifiedAt: now,
				ChangedAt:  now,
			}
			c.nextInode++
			dirsToAdd[newName] = &cloned
		}
	}
	for k, v := range dirsToAdd {
		p.dirs[k] = v
	}
	filesToAdd := make(map[string]*fakeRESTNode)
	for name, node := range p.files {
		if strings.HasPrefix(name, srcClean+"/") {
			remapped := strings.TrimPrefix(name, srcClean)
			newName := dstClean + remapped
			if _, exists := p.dirs[newName]; exists {
				return shfs.AlreadyExists(newName)
			}
			if _, exists := p.files[newName]; exists {
				return shfs.AlreadyExists(newName)
			}
			cloned := *node
			cloned.entry = &shfs.EntryInfo{
				Path:       newName,
				IsDir:      node.entry.IsDir,
				IsSymlink:  node.entry.IsSymlink,
				Size:       node.entry.Size,
				Inode:      c.nextInode,
				Mode:       node.entry.Mode,
				UID:        node.entry.UID,
				GID:        node.entry.GID,
				CreatedAt:  now,
				ModifiedAt: now,
				ChangedAt:  now,
			}
			c.nextInode++
			filesToAdd[newName] = &cloned
		}
	}
	for k, v := range filesToAdd {
		p.files[k] = v
	}
	c.recordRevisionLocked(p, "copy "+srcClean+" to "+dstClean)
	return nil
}

// CloneRangeContext implements the range-clone Client method with the
// core's observable semantics: memmove snapshot (the source span is
// captured before any destination byte lands), pwrite destination
// behavior with zero-filled gaps, creation of a missing destination, and
// the length-0 validated no-op (missing dst answers NotFound).
func (c *fakeRESTClient) CloneRange(_ context.Context, project, src string, srcOff int64, dst string, dstOff int64, length int64, opts ...shfs.MutateOption) (*FileMetadata, error) {
	c.recordOpts(opts)
	if err := c.consumeOptErr(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	srcClean, err := cleanRESTPath(src)
	if err != nil {
		return nil, err
	}
	dstClean, err := cleanRESTPath(dst)
	if err != nil {
		return nil, err
	}
	srcNode, ok := p.files[srcClean]
	if !ok {
		return nil, shfs.NotFound(srcClean)
	}
	if srcOff < 0 || dstOff < 0 || length < 0 {
		return nil, errors.New("clone offsets and length must be non-negative")
	}
	if srcOff > int64(len(srcNode.data.bytes)) || length > int64(len(srcNode.data.bytes))-srcOff {
		return nil, errors.New("clone source range exceeds file size")
	}
	dstNode, dstExists := p.files[dstClean]
	if length == 0 {
		if !dstExists {
			return nil, shfs.NotFound(dstClean)
		}
		return &FileMetadata{Size: dstNode.entry.Size, Inode: dstNode.entry.Inode}, nil
	}
	if !dstExists {
		if _, ok := p.dirs[dstClean]; ok {
			return nil, shfs.IsDirectory(dstClean)
		}
		if parent := parentPath(dstClean); parent != "" {
			if _, ok := p.dirs[parent]; !ok {
				return nil, fmt.Errorf("%w: parent directory does not exist: %s", shfs.ErrNotFound, parent)
			}
		}
		now := c.tick()
		dstNode = &fakeRESTNode{
			entry: &shfs.EntryInfo{
				Path: dstClean, Size: 0, Inode: c.nextInode, Mode: srcNode.entry.Mode,
				UID: srcNode.entry.UID, GID: srcNode.entry.GID,
				CreatedAt: now, ModifiedAt: now, ChangedAt: now,
			},
			data: &fakeRESTData{bytes: []byte{}, nlink: 1, kind: NodeKindFile},
		}
		c.nextInode++
		p.files[dstClean] = dstNode
	}
	seg := append([]byte(nil), srcNode.data.bytes[srcOff:srcOff+length]...)
	content := append([]byte(nil), dstNode.data.bytes...)
	if need := dstOff + length; int64(len(content)) < need {
		content = append(content, make([]byte, need-int64(len(content)))...)
	}
	copy(content[dstOff:], seg)
	dstNode.data.bytes = content
	dstNode.entry.Size = int64(len(content))
	now := c.tick()
	c.touchDataLocked(p, dstNode.data, now)
	c.recordRevisionLocked(p, "clone-range "+srcClean+" to "+dstClean)
	return &FileMetadata{Size: int64(len(content)), Inode: dstNode.entry.Inode}, nil
}

func (c *fakeRESTClient) TruncateFileContext(_ context.Context, project, filePath string, size int64, opts ...shfs.MutateOption) (*FileMetadata, error) {
	c.recordOpts(opts)
	if err := c.consumeOptErr(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	node, _, err := c.requireWritableFile(project, filePath)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, errors.New("truncate size must be non-negative")
	}
	if int64(len(node.data.bytes)) > size {
		node.data.bytes = append([]byte(nil), node.data.bytes[:size]...)
	} else if int64(len(node.data.bytes)) < size {
		node.data.bytes = append(append([]byte(nil), node.data.bytes...), make([]byte, size-int64(len(node.data.bytes)))...)
	}
	node.entry.Mode &^= 0o6000
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	updates := c.tick()
	c.touchDataLocked(p, node.data, updates)
	return &FileMetadata{Size: size, Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) AppendFileContext(_ context.Context, project, filePath string, data []byte, opts ...shfs.MutateOption) (*FileMetadata, error) {
	c.recordOpts(opts)
	if err := c.consumeOptErr(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	node, _, err := c.requireWritableFile(project, filePath)
	if err != nil {
		return nil, err
	}
	node.data.bytes = append(node.data.bytes, data...)
	node.entry.Mode &^= 0o6000
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	updates := c.tick()
	c.touchDataLocked(p, node.data, updates)
	return &FileMetadata{Size: int64(len(node.data.bytes)), Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) WriteFileAtContext(_ context.Context, project, filePath string, offset int64, data []byte, opts ...shfs.MutateOption) (*FileMetadata, error) {
	c.recordOpts(opts)
	if err := c.consumeOptErr(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	node, _, err := c.requireWritableFile(project, filePath)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, errors.New("write offset must be non-negative")
	}
	content := append([]byte(nil), node.data.bytes...)
	if offset > int64(len(content)) {
		content = append(content, make([]byte, offset-int64(len(content)))...)
	}
	end := offset + int64(len(data))
	if end > int64(len(content)) {
		grown := make([]byte, end)
		copy(grown, content)
		content = grown
	}
	copy(content[offset:end], data)
	node.data.bytes = content
	node.entry.Mode &^= 0o6000
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	updates := c.tick()
	c.touchDataLocked(p, node.data, updates)
	return &FileMetadata{Size: int64(len(content)), Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) ReplaceFileFromReaderContext(ctx context.Context, project, filePath string, body io.Reader, opts ...shfs.MutateOption) (*FileMetadata, error) {
	c.recordOpts(opts)
	if err := c.consumeOptErr(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	if fail := c.takeReplaceFailure(); fail != nil {
		return nil, fail
	}
	return c.WriteFileAtContext(ctx, project, filePath, 0, data)
}

func (c *fakeRESTClient) takeReplaceFailure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.failReplaceFromReader
	c.failReplaceFromReader = nil
	return err
}

func (c *fakeRESTClient) PatchFileContext(_ context.Context, project, filePath string, offset, deleteSize int64, edit []byte, opts ...shfs.MutateOption) (*FileMetadata, error) {
	c.recordOpts(opts)
	if err := c.consumeOptErr(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	node, _, err := c.requireWritableFile(project, filePath)
	if err != nil {
		return nil, err
	}
	content := append([]byte(nil), node.data.bytes...)
	if offset < 0 || deleteSize < 0 || offset+deleteSize > int64(len(content)) {
		return nil, errors.New("invalid patch range")
	}
	patched := append(append(content[:offset:offset], edit...), content[offset+deleteSize:]...)
	node.data.bytes = patched
	node.entry.Mode &^= 0o6000
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	updates := c.tick()
	c.touchDataLocked(p, node.data, updates)
	return &FileMetadata{Size: int64(len(patched)), Inode: node.entry.Inode}, nil
}

// --- content reads (nodes/children/content GET, revisions list) ---

func (c *fakeRESTClient) ReadFileAtContext(ctx context.Context, project, filePath string, offset, length int64) ([]byte, error) {
	c.mu.Lock()
	c.recordIdentityLocked(ctx)
	defer c.mu.Unlock()
	c.readCalls = append(c.readCalls, readCall{path: filePath, offset: offset, length: length})
	node, _, err := c.requireReadableFile(project, filePath)
	if err != nil {
		return nil, err
	}
	if offset > int64(len(node.data.bytes)) {
		return nil, io.EOF
	}
	end := offset + length
	if end > int64(len(node.data.bytes)) {
		end = int64(len(node.data.bytes))
	}
	return append([]byte(nil), node.data.bytes[offset:end]...), nil
}

func (c *fakeRESTClient) takeReadCalls() []readCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	calls := append([]readCall(nil), c.readCalls...)
	c.readCalls = nil
	return calls
}
