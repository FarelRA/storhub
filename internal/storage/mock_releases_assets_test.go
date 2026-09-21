package storage

// Mock release and asset routes (upload, download, delete).
import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func (m *mockGitHub) handleCreateRelease(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	var payload struct {
		TagName string `json:"tag_name"`
		Name    string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: creating a tag that already exists 422s instead of
	// overwriting. The production double-create race depends on this.
	if existing := repo.releasesByTag[payload.TagName]; existing != nil {
		m.writeAlreadyExists(w, "Release", "tag_name", payload.TagName)
		return
	}
	release := &mockRelease{
		id:        repo.nextReleaseID,
		tag:       payload.TagName,
		name:      payload.Name,
		uploadURL: fmt.Sprintf("%s/upload/%s/%s{?name}", m.server.URL, repo.name, payload.TagName),
	}
	repo.nextReleaseID++
	repo.releasesByTag[release.tag] = release
	repo.releasesByID[release.id] = release
	m.writeJSON(w, http.StatusCreated, map[string]any{
		"id":         release.id,
		"tag_name":   release.tag,
		"name":       release.name,
		"upload_url": release.uploadURL,
		"draft":      false,
	})
}

func (m *mockGitHub) handleGetReleaseByTag(w http.ResponseWriter, repo *mockRepo, tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	release := repo.releasesByTag[tag]
	if release == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	m.writeJSON(w, http.StatusOK, map[string]any{
		"id":         release.id,
		"tag_name":   release.tag,
		"name":       release.name,
		"upload_url": release.uploadURL,
		"draft":      false,
		"assets":     m.embeddedAssetsLocked(repo, release.tag),
	})
}

func (m *mockGitHub) handleListReleases(w http.ResponseWriter, r *http.Request, repo *mockRepo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	releases := make([]*mockRelease, 0, len(repo.releasesByTag))
	for _, release := range repo.releasesByTag {
		releases = append(releases, release)
	}
	// Mirror GitHub: the releases list is ordered by created_at DESC, not
	// by tag. IDs are minted in creation order, so descending
	// ID is the same sequence; tag-string order ("v10" < "v9") is not.
	sort.Slice(releases, func(i, j int) bool { return releases[i].id > releases[j].id })
	m.writePaginationLinks(w, r, len(releases))
	pageReleases := paginateSlice(releases, r.URL.Query())
	response := make([]map[string]any, 0, len(pageReleases))
	for _, release := range pageReleases {
		assets := m.embeddedAssetsLocked(repo, release.tag)
		response = append(response, map[string]any{
			"id":         release.id,
			"tag_name":   release.tag,
			"name":       release.name,
			"upload_url": release.uploadURL,
			"draft":      false,
			"assets":     assets,
		})
	}
	m.writeJSON(w, http.StatusOK, response)
}

func (m *mockGitHub) handleDeleteRelease(w http.ResponseWriter, repo *mockRepo, rawID string) {
	releaseID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	release := repo.releasesByID[releaseID]
	if release == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	delete(repo.releasesByID, releaseID)
	delete(repo.releasesByTag, release.tag)
	delete(repo.assetsByTag, release.tag)
	for id, asset := range repo.assets {
		if asset.releaseTag == release.tag {
			delete(repo.assets, id)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) handleUpload(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 {
		http.NotFound(w, r)
		return
	}
	repo := m.repo(parts[1])
	if repo == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "repo not found"})
		return
	}
	name := r.URL.Query().Get("name")
	data, err := io.ReadAll(r.Body)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: uploads target a release that must exist.
	if repo.releasesByTag[parts[2]] == nil {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "release not found"})
		return
	}
	// Fault injection: one forced name collision for the sink retry test.
	if m.faults.collideNext[parts[1]+"/"+parts[2]] {
		delete(m.faults.collideNext, parts[1]+"/"+parts[2])
		m.writeAlreadyExists(w, "ReleaseAsset", "name", name)
		return
	}
	// Mirror GitHub: asset names are unique per release; a duplicate 422s.
	// The per-tag index answers both the dup check and the count below in
	// one pass (was: two full asset scans per upload, O(N^2) to fill).
	byTag := repo.assetsByTag[parts[2]]
	for _, asset := range byTag {
		if asset.name == name {
			m.writeAlreadyExists(w, "ReleaseAsset", "name", name)
			return
		}
	}
	// Mirror GitHub: a release holds at most releaseAssetCap assets;
	// further uploads 422 with a file_count body (live shape from
	// storhub-web v18).
	if len(byTag) >= releaseAssetCap {
		m.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"message": "Validation Failed",
			"errors": []map[string]any{{
				"resource": "ReleaseAsset",
				"code":     "custom",
				"field":    "file_count",
				"message":  "file_count limited to 1000 assets per release",
			}},
		})
		return
	}
	asset := &mockAsset{id: globalMockAssetID.Add(1), name: name, releaseTag: parts[2], data: append([]byte(nil), data...)}
	repo.assets[asset.id] = asset
	tagAssetLocked(repo, asset)
	m.writeJSON(w, http.StatusCreated, map[string]any{"id": asset.id, "name": asset.name})
}

func (m *mockGitHub) handleDownloadAsset(w http.ResponseWriter, r *http.Request, repo *mockRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	asset, ok := repo.assets[assetID]
	if !ok {
		m.mu.Unlock()
		// Include the ID: under concurrent append storms a 404 without
		// it is undebuggable (which upload's asset went missing?).
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": fmt.Sprintf("asset not found: %d", assetID)})
		return
	}
	name, size := asset.name, len(asset.data)
	m.mu.Unlock()
	// Mirror GitHub: only the octet-stream GET is a redirect to a
	// short-lived signed CDN URL (range fetches then hit the CDN directly
	// without auth headers). Any other Accept answers with the asset's
	// metadata JSON - never the bytes.
	if r.Header.Get("Accept") != "application/octet-stream" {
		m.writeJSON(w, http.StatusOK, map[string]any{
			"id":                   assetID,
			"name":                 name,
			"size":                 size,
			"content_type":         "application/octet-stream",
			"browser_download_url": fmt.Sprintf("%s/cdn/%d", m.server.URL, assetID),
		})
		return
	}
	w.Header().Set("Location", m.mintSignedCDNURL(assetID))
	w.WriteHeader(http.StatusFound)
}

func (m *mockGitHub) handleDeleteAsset(w http.ResponseWriter, repo *mockRepo, rawID string) {
	assetID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		m.writeJSON(w, http.StatusBadRequest, map[string]any{"message": err.Error()})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	asset, ok := repo.assets[assetID]
	if !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "asset not found"})
		return
	}
	untagAssetLocked(repo, asset.releaseTag, assetID)
	delete(repo.assets, assetID)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockGitHub) handleDeleteRepo(w http.ResponseWriter, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Mirror GitHub: deleting an unknown repo 404s. The client
	// treats 404 on delete as success; the old unconditional 204 hid the
	// lost-response-retry divergence.
	if _, ok := m.repos[name]; !ok {
		m.writeJSON(w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return
	}
	delete(m.repos, name)
	w.WriteHeader(http.StatusNoContent)
}
