package storage

// Mock repo, contents, and commit-history routes plus the file-tree index.
import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (m *mockGitHub) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Name    string `json:"name"`
		Private bool   `json:"private"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.repos[payload.Name]; exists {
		m.writeAlreadyExists(w, "Repository", "name", payload.Name)
		return
	}
	m.repos[payload.Name] = &mockRepo{
		name:          payload.Name,
		private:       payload.Private,
		nextReleaseID: 1,
		nextBlobID:    1,
		nextCommitID:  1,
		releasesByTag: make(map[string]*mockRelease),
		releasesByID:  make(map[int64]*mockRelease),
		assets:        make(map[int64]*mockAsset),
		assetsByTag:   make(map[string]map[int64]*mockAsset),
		files:         make(map[string]*mockFile),
		dirChildren:   make(map[string]map[string]bool),
		commitsByPath: make(map[string][]mockCommit),
		commitTime:    make(map[string]time.Time),
	}
	m.writeJSON(w, http.StatusCreated, map[string]any{"name": payload.Name})
}

// indexMockDirChild records filePath's leaf and every ancestor directory in
// dirChildren: dirChildren[dir][name] reports whether name is a directory.
// A directory entry always wins over a file entry of the same name.
func indexMockDirChild(repo *mockRepo, filePath string) {
	segments := strings.Split(filePath, "/")
	for i := range segments {
		dir := strings.Join(segments[:i], "/")
		child := segments[i]
		isDir := i < len(segments)-1
		if repo.dirChildren == nil {
			repo.dirChildren = make(map[string]map[string]bool)
		}
		kids := repo.dirChildren[dir]
		if kids == nil {
			kids = make(map[string]bool)
			repo.dirChildren[dir] = kids
		}
		kids[child] = kids[child] || isDir
	}
}

// unindexMockDirChild drops filePath's leaf, then prunes ancestors left
// childless (a directory with no children does not exist on GitHub).
func unindexMockDirChild(repo *mockRepo, filePath string) {
	segments := strings.Split(filePath, "/")
	if len(segments) == 0 {
		return
	}
	dir := strings.Join(segments[:len(segments)-1], "/")
	if kids := repo.dirChildren[dir]; kids != nil {
		delete(kids, segments[len(segments)-1])
	}
	// Walk up: a directory whose index entry is now empty vanishes, which
	// may empty its own parent in turn.
	for d := dir; d != ""; {
		if kids := repo.dirChildren[d]; kids != nil && len(kids) == 0 {
			delete(repo.dirChildren, d)
		} else {
			break
		}
		parent, base := parentDirBase(d)
		if kids := repo.dirChildren[parent]; kids != nil {
			delete(kids, base)
		}
		d = parent
	}
}

func parentDirBase(dir string) (parent, base string) {
	if i := strings.LastIndex(dir, "/"); i >= 0 {
		return dir[:i], dir[i+1:]
	}
	return "", dir
}

// recordMockCommitLocked appends a newest-first commit on filePath's history
// with a 40-hex SHA and maintains the commitTime index. Caller holds m.mu.
func (m *mockGitHub) recordMockCommitLocked(repo *mockRepo, filePath, message string, data []byte, deleted bool) string {
	_ = m
	commitSHA := mockCommitSHA(repo.nextCommitID)
	repo.nextCommitID++
	when := time.Unix(1700000000+repo.nextCommitID, 0).UTC()
	repo.commitsByPath[filePath] = append([]mockCommit{{
		sha: commitSHA, message: message, path: filePath,
		data: append([]byte(nil), data...), when: when, deleted: deleted,
	}}, repo.commitsByPath[filePath]...)
	if repo.commitTime == nil {
		repo.commitTime = make(map[string]time.Time)
	}
	repo.commitTime[commitSHA] = when
	return commitSHA
}

func (m *mockGitHub) handleRepos(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "repos" || parts[1] != m.owner {
		http.NotFound(w, r)
		return
	}
	repo := m.repo(parts[2])
	if repo == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "repo not found"})
		return
	}

	switch {
	case len(parts) == 3 && r.Method == http.MethodGet:
		m.writeJSON(w, http.StatusOK, map[string]any{"name": repo.name})
	case len(parts) >= 5 && parts[3] == "contents":
		m.handleContents(w, r, repo, strings.Join(parts[4:], "/"))
	case len(parts) == 4 && parts[3] == "commits" && r.Method == http.MethodGet:
		m.handleListCommits(w, r, repo)
	case len(parts) == 4 && parts[3] == "releases" && r.Method == http.MethodPost:
		m.handleCreateRelease(w, r, repo)
	case len(parts) == 4 && parts[3] == "releases" && r.Method == http.MethodGet:
		m.handleListReleases(w, r, repo)
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "tags" && r.Method == http.MethodGet:
		m.handleGetReleaseByTag(w, repo, parts[5])
	case len(parts) == 5 && parts[3] == "releases" && r.Method == http.MethodDelete:
		m.handleDeleteRelease(w, repo, parts[4])
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "assets" && r.Method == http.MethodGet:
		m.handleDownloadAsset(w, r, repo, parts[5])
	case len(parts) == 6 && parts[3] == "releases" && parts[5] == "assets" && r.Method == http.MethodGet:
		m.handleListReleaseAssets(w, r, repo, parts[4])
	case len(parts) == 6 && parts[3] == "releases" && parts[4] == "assets" && r.Method == http.MethodDelete:
		m.handleDeleteAsset(w, repo, parts[5])
	case len(parts) == 3 && r.Method == http.MethodDelete:
		m.handleDeleteRepo(w, parts[2])
	default:
		http.NotFound(w, r)
	}
}

func (m *mockGitHub) handleContents(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	cleanPath := strings.TrimPrefix(filePath, "/")
	switch r.Method {
	case http.MethodGet:
		m.handleGetContent(w, r, repo, cleanPath)
	case http.MethodPut:
		m.handlePutContent(w, r, repo, cleanPath)
	case http.MethodDelete:
		m.handleDeleteContent(w, r, repo, cleanPath)
	default:
		http.NotFound(w, r)
	}
}

// handleDeleteContent removes a file, verifying the caller's blob sha matches
// (GitHub's optimistic-concurrency precondition for deletes).
func (m *mockGitHub) handleDeleteContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	var payload struct {
		Message string `json:"message"`
		SHA     string `json:"sha"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	// Mirror GitHub: contents DELETE requires both a commit message and
	// the current blob sha; either missing is a 422, never a silent delete.
	if payload.Message == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"message\" wasn't supplied."})
		return
	}
	if payload.SHA == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"sha\" wasn't supplied."})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := repo.files[filePath]
	if current == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	if payload.SHA != current.sha {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "sha does not match"})
		return
	}
	delete(repo.files, filePath)
	unindexMockDirChild(repo, filePath)
	// Mirror GitHub: the delete is itself a commit on the path's history
	// (revision lists after a delete must show it), and the path is gone
	// from that commit onward.
	commitSHA := m.recordMockCommitLocked(repo, filePath, payload.Message, nil, true)
	m.writeJSON(w, http.StatusOK, map[string]any{"commit": map[string]any{"sha": commitSHA}})
}

func (m *mockGitHub) handleGetContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ref := r.URL.Query().Get("ref")

	// Check if client wants raw content
	acceptRaw := r.Header.Get("Accept") == "application/vnd.github.raw"

	if ref != "" && ref != "HEAD" && ref != defaultBranch {
		// Real GitHub resolves ?ref= against the commit graph and 404s a
		// ref it does not know; it never silently serves HEAD bytes for an
		// unknown/stale SHA. "HEAD" and the default branch name
		// are valid refs and resolve to current state below.
		data, known, present := m.contentAtRefLocked(repo, filePath, ref)
		switch {
		case !known:
			m.writeJSON(w, http.StatusNotFound, map[string]any{"message": fmt.Sprintf("No commit found for SHA: %s", ref)})
			return
		case present:
			m.serveFileContentLocked(w, filePath, data, acceptRaw)
			return
		case repo.files[filePath] != nil:
			// A file now (or again) at HEAD that did not exist at ref:
			// GitHub 404s the historical read.
			m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
			return
			// Otherwise the path may be a directory prefix: fall through
			// to the HEAD listing (the mock keeps no per-commit trees).
		}
	}
	file := repo.files[filePath]
	if file == nil {
		// Not an exact file: it may be a directory prefix. GitHub's contents
		// API returns an array of entries for a directory. Prune's object
		// enumeration relies on this, so mirror it - including GitHub's
		// hard 1000-entry ceiling, past which the API 403s "too large"
		// rather than truncating.
		if entries, ok := m.dirEntriesLocked(repo, filePath); ok {
			if len(entries) > contentsListingCap {
				m.writeJSON(w, http.StatusForbidden, map[string]any{"message": "too large"})
				return
			}
			m.writeJSON(w, http.StatusOK, entries)
			return
		}
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	m.serveFileContentLocked(w, filePath, file.data, acceptRaw)
}

// contentAtRefLocked resolves filePath at commit ref the way GitHub does:
// the bytes the path carried in that commit's tree. known=false means ref
// is not a commit the mock knows at all (GitHub: 404 "No commit found for
// SHA" - the ref is resolved across EVERY path's history, since GitHub
// accepts any valid SHA, not just commits that touched this file).
// known=true, present=false means the path did not exist (or was deleted)
// at that commit (GitHub: 404 "Not Found").
func (m *mockGitHub) contentAtRefLocked(repo *mockRepo, filePath, ref string) (data []byte, known, present bool) {
	// O(1) ref resolution via the commitTime index (was: a scan of every
	// path's history per ?ref= GET).
	refWhen, known := repo.commitTime[ref]
	if !known {
		return nil, false, false
	}
	commits := repo.commitsByPath[filePath]
	if len(commits) == 0 {
		// No recorded history for this path: it predates every commit the
		// mock knows (seeded state), so HEAD bytes are its content at any
		// valid ref.
		if f := repo.files[filePath]; f != nil {
			return f.data, true, true
		}
		return nil, true, false
	}
	// commitsByPath is newest-first; the content at ref is the newest
	// entry at or before ref's timestamp.
	for _, c := range commits {
		if !c.when.After(refWhen) {
			if c.deleted {
				return nil, true, false
			}
			return c.data, true, true
		}
	}
	// Every change to this path postdates ref: it did not exist then.
	return nil, true, false
}

// dirEntriesLocked returns the immediate children of a directory prefix
// from the dirChildren index (was: a scan of every file per ListDir).
// ok=false when nothing lives under it. Caller holds m.mu.
func (m *mockGitHub) dirEntriesLocked(repo *mockRepo, dirPath string) ([]map[string]any, bool) {
	// Preserve the historical contract: the root (""/"/") never lists.
	// GitHub has no root-contents listing on this route and prune's
	// enumeration always passes a real directory prefix.
	if strings.Trim(dirPath, "/") == "" {
		return nil, false
	}
	dir := strings.TrimSuffix(dirPath, "/")
	kids, ok := repo.dirChildren[dir]
	if !ok || len(kids) == 0 {
		return nil, false
	}
	names := make([]string, 0, len(kids))
	for name := range kids {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]map[string]any, 0, len(names))
	for _, name := range names {
		if kids[name] {
			out = append(out, map[string]any{"name": name, "path": dir + "/" + name, "type": "dir"})
			continue
		}
		p := dir + "/" + name
		sha := ""
		if f := repo.files[p]; f != nil {
			sha = f.sha
		}
		out = append(out, map[string]any{"name": name, "path": p, "type": "file", "sha": sha})
	}
	return out, true
}

func (m *mockGitHub) handlePutContent(w http.ResponseWriter, r *http.Request, repo *mockRepo, filePath string) {
	var payload struct {
		Message string `json:"message"`
		Content string `json:"content"`
		SHA     string `json:"sha"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	// Mirror GitHub's request validation: a contents PUT
	// without a commit message 422s, and invalid base64 is a 422 - not
	// the 400/200 the old mock answered with.
	if payload.Message == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"message\" wasn't supplied."})
		return
	}
	data, err := base64.StdEncoding.DecodeString(payload.Content)
	if err != nil {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"content\" is not valid base64-encoded data."})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current := repo.files[filePath]
	// Mirror GitHub: a sha-less PUT onto an EXISTING path is not a silent
	// overwrite, it is a create collision - 422 with the "sha wasn't
	// supplied" request-validation body. The old 200 made the
	// production benign-concurrent-create branch unreachable in tests.
	if current != nil && payload.SHA == "" {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "Invalid request.\n\n\"sha\" wasn't supplied."})
		return
	}
	if current != nil && payload.SHA != "" && payload.SHA != current.sha {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "sha does not match"})
		return
	}
	if current == nil && payload.SHA != "" {
		m.writeJSON(w, http.StatusConflict, map[string]any{"message": "file does not exist"})
		return
	}
	// Compute real git blob SHA instead of fake counter-based SHA
	blobSHA := computeGitBlobSHA(data)
	m.putMockFileLocked(repo, filePath, data)
	commitSHA := m.recordMockCommitLocked(repo, filePath, payload.Message, data, false)
	// Mirror GitHub: creating a new file answers 201, updating 200.
	status := http.StatusOK
	if current == nil {
		status = http.StatusCreated
	}
	m.writeJSON(w, status, map[string]any{
		"content": map[string]any{
			"name": filepath.Base(filePath),
			"path": filePath,
			"sha":  blobSHA,
		},
		"commit": map[string]any{
			"sha": commitSHA,
		},
	})
}

func (m *mockGitHub) handleListCommits(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	filePath := r.URL.Query().Get("path")
	m.mu.Lock()
	defer m.mu.Unlock()
	commits := repo.commitsByPath[filePath]
	m.writePaginationLinks(w, r, len(commits))
	pageItems := paginateSlice(commits, r.URL.Query())
	response := make([]map[string]any, 0, len(pageItems))
	for _, commit := range pageItems {
		response = append(response, map[string]any{
			"sha": commit.sha,
			"commit": map[string]any{
				"message": commit.message,
				"author":  map[string]any{"date": commit.when.Format(time.RFC3339)},
			},
		})
	}
	m.writeJSON(w, http.StatusOK, response)
}
