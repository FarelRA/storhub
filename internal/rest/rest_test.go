package rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRESTFilesystemWorkflow(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	putResp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/readme.txt", strings.NewReader("hello"), nil, http.StatusCreated)
	var putNode nodeResponse
	decodeJSONBody(t, putResp, &putNode)
	if putNode.Entry == nil || putNode.Entry.Path != "docs/readme.txt" || putNode.Entry.Size != 5 {
		t.Fatalf("unexpected put node: %+v", putNode)
	}
	if putNode.ETag == "" {
		t.Fatal("expected etag on put response")
	}

	contentResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/readme.txt", nil, nil, http.StatusOK)
	if got := string(readBody(t, contentResp)); got != "hello" {
		t.Fatalf("unexpected content: %q", got)
	}
	if contentResp.Header.Get("ETag") == "" {
		t.Fatal("expected content etag")
	}

	rangeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/readme.txt", nil, map[string]string{"Range": "bytes=1-3"}, http.StatusPartialContent)
	if got := string(readBody(t, rangeResp)); got != "ell" {
		t.Fatalf("unexpected ranged content: %q", got)
	}

	mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/readme.txt&op=write&offset=1", strings.NewReader("a"), map[string]string{"If-Match": putNode.ETag}, http.StatusOK)
	nodeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/readme.txt", nil, nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, nodeResp, &node)
	writeETag := node.ETag

	mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/readme.txt&op=append", strings.NewReader("!"), map[string]string{"If-Match": writeETag}, http.StatusOK)
	nodeResp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/readme.txt", nil, nil, http.StatusOK)
	decodeJSONBody(t, nodeResp, &node)
	appendETag := node.ETag

	mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/readme.txt&op=truncate&size=3", nil, map[string]string{"If-Match": appendETag}, http.StatusOK)
	contentResp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/readme.txt", nil, nil, http.StatusOK)
	if got := string(readBody(t, contentResp)); got != "hal" {
		t.Fatalf("unexpected truncated content: %q", got)
	}

	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/xattrs/value?path=docs/readme.txt&name=user.flag", strings.NewReader("warm"), nil, http.StatusNoContent)
	attrsResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/xattrs?path=docs/readme.txt", nil, nil, http.StatusOK)
	var attrs xattrListResponse
	decodeJSONBody(t, attrsResp, &attrs)
	if len(attrs.Names) != 1 || attrs.Names[0] != "user.flag" {
		t.Fatalf("unexpected xattrs: %+v", attrs)
	}
	attrValueResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/xattrs/value?path=docs/readme.txt&name=user.flag", nil, nil, http.StatusOK)
	if got := string(readBody(t, attrValueResp)); got != "warm" {
		t.Fatalf("unexpected xattr value: %q", got)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chmod", chmodRequest{Path: "docs/readme.txt", Mode: 0o600}, http.StatusOK)
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/chown", chownRequest{Path: "docs/readme.txt", UID: 7, GID: 9}, http.StatusOK)
	stamp := time.Unix(100, 0).UTC()
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/utimes", utimesRequest{Path: "docs/readme.txt", Atime: stamp, Mtime: stamp}, http.StatusOK)

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/symlink", symlinkRequest{Target: "docs/readme.txt", LinkPath: "docs/link.txt"}, http.StatusCreated)
	symlinkContent := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/link.txt", nil, nil, http.StatusOK)
	if got := string(readBody(t, symlinkContent)); got != "docs/readme.txt" {
		t.Fatalf("unexpected symlink body: %q", got)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/link", linkRequest{ExistingPath: "docs/readme.txt", NewPath: "docs/hard.txt"}, http.StatusCreated)
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/rename", renameRequest{OldPath: "docs/hard.txt", NewPath: "docs/final.txt"}, http.StatusOK)

	childrenResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/children?path=docs", nil, nil, http.StatusOK)
	var children entriesResponse
	decodeJSONBody(t, childrenResp, &children)
	if len(children.Entries) != 3 {
		t.Fatalf("unexpected children count: %+v", children)
	}
	if names := []string{children.Entries[0].Name, children.Entries[1].Name, children.Entries[2].Name}; strings.Join(names, ",") != "final.txt,link.txt,readme.txt" {
		t.Fatalf("unexpected child names: %v", names)
	}

	revisionsResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/revisions", nil, nil, http.StatusOK)
	var revisions revisionsResponse
	decodeJSONBody(t, revisionsResp, &revisions)
	if len(revisions.Revisions) == 0 {
		t.Fatal("expected revisions")
	}
	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/rollback", rollbackRequest{CommitSHA: revisions.Revisions[0].CommitSHA}, http.StatusOK)

	projectResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo", nil, nil, http.StatusOK)
	var project projectResponse
	decodeJSONBody(t, projectResp, &project)
	if project.Stats == nil || project.Stats.Files != 3 || project.Stats.Directories != 2 {
		t.Fatalf("unexpected project stats: %+v", project)
	}
}

func TestRESTPreconditionsAndDeleteErrors(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	mustJSONRequest(t, handler, http.MethodPost, "/api/v1/projects/demo/ops/mkdir", pathRequest{Path: "docs"}, http.StatusCreated)
	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/file.txt", strings.NewReader("payload"), nil, http.StatusCreated)

	resp := mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=docs/file.txt", strings.NewReader("again"), map[string]string{"If-None-Match": "*"}, http.StatusPreconditionFailed)
	assertErrorCode(t, resp, "precondition_failed")

	nodeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/file.txt", nil, nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, nodeResp, &node)
	resp = mustRequest(t, handler, http.MethodPatch, "/api/v1/projects/demo/content?path=docs/file.txt&op=append", strings.NewReader("!"), map[string]string{"If-Match": "\"wrong\""}, http.StatusPreconditionFailed)
	assertErrorCode(t, resp, "precondition_failed")

	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/content?path=docs/file.txt", nil, map[string]string{"Range": "bytes=999-1000"}, http.StatusRequestedRangeNotSatisfiable)
	assertErrorCode(t, resp, "range_not_satisfiable")

	dirResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs", nil, nil, http.StatusOK)
	var dirNode nodeResponse
	decodeJSONBody(t, dirResp, &dirNode)
	resp = mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs", nil, map[string]string{"If-Match": dirNode.ETag}, http.StatusConflict)
	assertErrorCode(t, resp, "conflict")

	resp = mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs/file.txt", nil, map[string]string{"If-Match": node.ETag}, http.StatusNoContent)
	if body := strings.TrimSpace(string(readBody(t, resp))); body != "" {
		t.Fatalf("expected empty body on delete, got %q", body)
	}
	resp = mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=docs/file.txt", nil, nil, http.StatusNotFound)
	assertErrorCode(t, resp, "not_found")
	resp = mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo/nodes?path=docs", nil, nil, http.StatusNoContent)
	if body := strings.TrimSpace(string(readBody(t, resp))); body != "" {
		t.Fatalf("expected empty body on directory delete, got %q", body)
	}
}

func TestRESTProjectDeleteAndConditionalNodeRead(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}

	rootResp := mustRequest(t, handler, http.MethodGet, "/api/v1", nil, nil, http.StatusOK)
	var root map[string]any
	decodeJSONBody(t, rootResp, &root)
	if root["version"] != "v1" {
		t.Fatalf("unexpected root payload: %+v", root)
	}

	mustRequest(t, handler, http.MethodPut, "/api/v1/projects/demo/content?path=hello.txt", strings.NewReader("hi"), nil, http.StatusCreated)
	nodeResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=hello.txt", nil, nil, http.StatusOK)
	var node nodeResponse
	decodeJSONBody(t, nodeResp, &node)
	notModified := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo/nodes?path=hello.txt", nil, map[string]string{"If-None-Match": node.ETag}, http.StatusNotModified)
	if body := strings.TrimSpace(string(readBody(t, notModified))); body != "" {
		t.Fatalf("expected empty 304 body, got %q", body)
	}

	mustRequest(t, handler, http.MethodDelete, "/api/v1/projects/demo", nil, nil, http.StatusOK)
	deletedResp := mustRequest(t, handler, http.MethodGet, "/api/v1/projects/demo", nil, nil, http.StatusNotFound)
	assertErrorCode(t, deletedResp, "not_found")
}

func TestRESTUISurfacesDocumentAndConfig(t *testing.T) {
	client := newFakeRESTClient()
	handler, err := newHandlerForClient(client, Options{})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	root := mustRequest(t, handler, http.MethodGet, "/", nil, nil, http.StatusOK)
	if body := string(readBody(t, root)); !strings.Contains(body, "StorHub Console") || !strings.Contains(body, "/ui/app.js") {
		t.Fatalf("unexpected ui body: %q", body)
	}
	config := mustRequest(t, handler, http.MethodGet, "/ui/config.js", nil, nil, http.StatusOK)
	if body := string(readBody(t, config)); !strings.Contains(body, "authEnabled") || !strings.Contains(body, "/api/v1") {
		t.Fatalf("unexpected config body: %q", body)
	}
	authed, err := newHandlerForClient(client, Options{Auth: &AuthOptions{TokenSigningKey: []byte("secret"), Users: []User{{Username: "admin", Password: "pass", UID: 0, PrimaryGID: 0, Admin: true}}}})
	if err != nil {
		t.Fatalf("new authed handler: %v", err)
	}
	authedConfig := mustRequest(t, authed, http.MethodGet, "/ui/config.js", nil, nil, http.StatusOK)
	if body := string(readBody(t, authedConfig)); !strings.Contains(body, "true") {
		t.Fatalf("expected auth-enabled config: %q", body)
	}
}

func mustJSONRequest(t *testing.T, handler http.Handler, method, target string, payload any, wantStatus int) *http.Response {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return mustRequest(t, handler, method, target, bytes.NewReader(body), map[string]string{"Content-Type": "application/json"}, wantStatus)
}

func mustRequest(t *testing.T, handler http.Handler, method, target string, body io.Reader, headers map[string]string, wantStatus int) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	if resp.StatusCode != wantStatus {
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("unexpected status for %s %s: got %d want %d body=%s", method, target, resp.StatusCode, wantStatus, strings.TrimSpace(string(data)))
	}
	return resp
}

func decodeJSONBody(t *testing.T, resp *http.Response, dst any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		t.Fatalf("decode body: %v", err)
	}
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return data
}

func assertErrorCode(t *testing.T, resp *http.Response, want string) {
	t.Helper()
	defer resp.Body.Close()
	var payload restError
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if payload.Error.Code != want {
		t.Fatalf("unexpected error code: got %q want %q", payload.Error.Code, want)
	}
}

type fakeRESTClient struct {
	mu        sync.Mutex
	nextInode uint64
	projects  map[string]*fakeRESTProject
	deleted   map[string]bool
	now       time.Time
	rollbacks []string
}

type fakeRESTProject struct {
	dirs      map[string]*fakeRESTNode
	files     map[string]*fakeRESTNode
	revisions []MetadataRevision
}

type fakeRESTNode struct {
	entry *EntryInfo
	xattr map[string][]byte
	data  *fakeRESTData
}

type fakeRESTData struct {
	bytes  []byte
	nlink  uint32
	kind   NodeKind
	target string
}

func newFakeRESTClient() *fakeRESTClient {
	return &fakeRESTClient{
		nextInode: 2,
		projects:  make(map[string]*fakeRESTProject),
		deleted:   make(map[string]bool),
		now:       time.Unix(10, 0).UTC(),
	}
}

func (c *fakeRESTClient) CreateFile(project, filePath string) (*FileMetadata, error) {
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
	return &FileMetadata{Name: clean, Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) Mkdir(project, dirPath string) error {
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
		return errors.New("directory already exists: " + clean)
	}
	if _, ok := p.files[clean]; ok {
		return errors.New("file already exists at path: " + clean)
	}
	if parent := parentPath(clean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return errors.New("parent directory does not exist: " + parent)
		}
	}
	now := c.tick()
	p.dirs[clean] = &fakeRESTNode{entry: &EntryInfo{Path: clean, IsDir: true, Inode: c.allocInode(), Mode: 0o755, UID: 1000, GID: 1000, NLink: 1, CreatedAt: now, ModifiedAt: now, AccessedAt: now, ChangedAt: now}, xattr: map[string][]byte{}}
	c.recordRevisionLocked(p, "mkdir "+clean)
	return nil
}

func (c *fakeRESTClient) DeleteFile(project, filePath string) error {
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
			return errors.New("is a directory: " + clean)
		}
		return errors.New("file not found: " + clean)
	}
	delete(p.files, clean)
	if node.data != nil && node.data.nlink > 0 {
		node.data.nlink--
		c.syncLinksLocked(p, node.data)
	}
	c.recordRevisionLocked(p, "delete "+clean)
	return nil
}

func (c *fakeRESTClient) Rmdir(project, dirPath string) error {
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
		return errors.New("cannot remove root directory")
	}
	if _, ok := p.files[clean]; ok {
		return errors.New("not a directory: " + clean)
	}
	if _, ok := p.dirs[clean]; !ok {
		return errors.New("directory not found: " + clean)
	}
	for name := range p.dirs {
		if parentPath(name) == clean {
			return errors.New("directory not empty: " + clean)
		}
	}
	for name := range p.files {
		if parentPath(name) == clean {
			return errors.New("directory not empty: " + clean)
		}
	}
	delete(p.dirs, clean)
	c.recordRevisionLocked(p, "rmdir "+clean)
	return nil
}

func (c *fakeRESTClient) Rename(project, oldPath, newPath string) error {
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
		return errors.New("destination already exists: " + newClean)
	}
	if _, ok := p.dirs[newClean]; ok {
		return errors.New("destination already exists: " + newClean)
	}
	if parent := parentPath(newClean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return errors.New("parent directory does not exist: " + parent)
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
		return errors.New("path not found: " + oldClean)
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

func (c *fakeRESTClient) TruncateFile(project, filePath string, size int64) (*FileMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, clean, err := c.requireWritableFile(project, filePath)
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
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	updates := c.tick()
	c.touchDataLocked(p, node.data, updates)
	return &FileMetadata{Name: clean, Size: size, Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) AppendFile(project, filePath string, data []byte) (*FileMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, clean, err := c.requireWritableFile(project, filePath)
	if err != nil {
		return nil, err
	}
	node.data.bytes = append(node.data.bytes, data...)
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	updates := c.tick()
	c.touchDataLocked(p, node.data, updates)
	return &FileMetadata{Name: clean, Size: int64(len(node.data.bytes)), Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) WriteFileAt(project, filePath string, offset int64, data []byte) (*FileMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, clean, err := c.requireWritableFile(project, filePath)
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
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	updates := c.tick()
	c.touchDataLocked(p, node.data, updates)
	return &FileMetadata{Name: clean, Size: int64(len(content)), Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) PatchFile(project, filePath string, offset, deleteSize int64, edit []byte) (*FileMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, clean, err := c.requireWritableFile(project, filePath)
	if err != nil {
		return nil, err
	}
	content := append([]byte(nil), node.data.bytes...)
	if offset < 0 || deleteSize < 0 || offset+deleteSize > int64(len(content)) {
		return nil, errors.New("invalid patch range")
	}
	patched := append(append(content[:offset:offset], edit...), content[offset+deleteSize:]...)
	node.data.bytes = patched
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	updates := c.tick()
	c.touchDataLocked(p, node.data, updates)
	return &FileMetadata{Name: clean, Size: int64(len(patched)), Inode: node.entry.Inode}, nil
}

func (c *fakeRESTClient) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
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

func (c *fakeRESTClient) StatPath(project, targetPath string) (*EntryInfo, error) {
	c.mu.Lock()
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
	return nil, errors.New("path not found: " + clean)
}

func (c *fakeRESTClient) ReadDir(project, dirPath string) ([]DirEntry, error) {
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
		return nil, errors.New("not a directory: " + clean)
	}
	if _, ok := p.dirs[clean]; !ok {
		return nil, errors.New("directory not found: " + clean)
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

func (c *fakeRESTClient) StatFS(project string) (*FSStats, error) {
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

func (c *fakeRESTClient) Symlink(project, target, linkPath string) (*FileMetadata, error) {
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
	return &FileMetadata{Name: clean, Kind: NodeKindSymlink, Inode: node.entry.Inode, SymlinkTarget: target}, nil
}

func (c *fakeRESTClient) Readlink(project, linkPath string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, _, err := c.requireReadableFile(project, linkPath)
	if err != nil {
		return "", err
	}
	if node.data.kind != NodeKindSymlink {
		return "", errors.New("path is not a symlink: " + linkPath)
	}
	return node.data.target, nil
}

func (c *fakeRESTClient) Link(project, existingPath, newPath string) (*FileMetadata, error) {
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
	return &FileMetadata{Name: clean, Inode: node.entry.Inode, NLink: node.data.nlink}, nil
}

func (c *fakeRESTClient) Chmod(project, targetPath string, mode uint32) error {
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

func (c *fakeRESTClient) Chown(project, targetPath string, uid, gid uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	now := c.tick()
	node.entry.UID = uid
	node.entry.GID = gid
	node.entry.ChangedAt = now
	return nil
}

func (c *fakeRESTClient) Chtimes(project, targetPath string, atime, mtime time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	now := c.tick()
	node.entry.AccessedAt = atime
	node.entry.ModifiedAt = mtime
	node.entry.ChangedAt = now
	return nil
}

func (c *fakeRESTClient) SetXAttr(project, targetPath, attr string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	node.xattr[attr] = append([]byte(nil), data...)
	return nil
}

func (c *fakeRESTClient) GetXAttr(project, targetPath, attr string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return nil, err
	}
	value, ok := node.xattr[attr]
	if !ok {
		return nil, errors.New("xattr not found: " + targetPath)
	}
	return append([]byte(nil), value...), nil
}

func (c *fakeRESTClient) ListXAttr(project, targetPath string) ([]string, error) {
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

func (c *fakeRESTClient) RemoveXAttr(project, targetPath, attr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, err := c.lookupNode(project, targetPath)
	if err != nil {
		return err
	}
	if _, ok := node.xattr[attr]; !ok {
		return errors.New("xattr not found: " + targetPath)
	}
	delete(node.xattr, attr)
	return nil
}

func (c *fakeRESTClient) ListMetadataRevisions(project string) ([]MetadataRevision, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.getExistingProject(project)
	if err != nil {
		return nil, err
	}
	return append([]MetadataRevision(nil), p.revisions...), nil
}

func (c *fakeRESTClient) RollbackMetadata(project, commitSHA string) error {
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
	return errors.New("revision not found: " + commitSHA)
}

func (c *fakeRESTClient) DeleteProject(project string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.projects[project]; !ok {
		return ErrProjectNotFound
	}
	delete(c.projects, project)
	c.deleted[project] = true
	return nil
}

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
		return nil, ErrProjectNotFound
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
		return nil, "", errors.New("file already exists: " + clean)
	}
	if _, ok := p.dirs[clean]; ok {
		return nil, "", errors.New("path already exists: " + clean)
	}
	if parent := parentPath(clean); parent != "" {
		if _, ok := p.dirs[parent]; !ok {
			return nil, "", errors.New("parent directory does not exist: " + parent)
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
		return nil, "", errors.New("cannot write symlink: " + clean)
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
		return nil, "", errors.New("file not found: " + clean)
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
	return nil, errors.New("path not found: " + clean)
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

func (c *fakeRESTClient) touchDataLocked(p *fakeRESTProject, data *fakeRESTData, now time.Time) {
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
	p.revisions = append([]MetadataRevision{{CommitSHA: message + "-sha", Message: message, CommittedAt: c.now}}, p.revisions...)
}

func (c *fakeRESTClient) allocInode() uint64 {
	ino := c.nextInode
	c.nextInode++
	return ino
}

func (c *fakeRESTClient) tick() time.Time {
	c.now = c.now.Add(time.Second)
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
