package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Release is one GitHub release with its assets.
type Release struct {
	ID        int64   `json:"id"`
	TagName   string  `json:"tag_name"`
	Name      string  `json:"name"`
	UploadURL string  `json:"upload_url"`
	Draft     bool    `json:"draft"`
	Assets    []Asset `json:"assets"`
}

// Asset is one GitHub release asset file.
type Asset struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// GetAuthenticatedUser returns the login of the token owner.
func (c *Client) GetAuthenticatedUser(ctx context.Context) (string, error) {
	var user struct {
		Login string `json:"login"`
	}
	if err := c.getJSON(ctx, c.apiURL("/user"), &user); err != nil {
		return "", err
	}
	if user.Login == "" {
		return "", fmt.Errorf("authenticated user response missing login")
	}
	return user.Login, nil
}

// CreateRepo creates the storage repository for the project.
func (c *Client) CreateRepo(ctx context.Context, project, description string, private, autoInit bool) error {
	body := map[string]any{
		"name":        project,
		"description": description,
		"private":     private,
		"auto_init":   autoInit,
	}
	// Repository creation is not idempotent; never blind-retry it.
	resp, err := c.doJSONWithRetryable(ctx, http.MethodPost, c.apiURL("/user/repos"), body, false)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// RepoExists reports whether the owner/project repository exists.
// The success body is drained before close so keep-alive connections
// can be reused instead of stalling on an unread body.
func (c *Client) RepoExists(ctx context.Context, owner, project string) (bool, error) {
	resp, err := c.doJSON(ctx, http.MethodGet, c.apiURL(fmt.Sprintf("/repos/%s/%s", owner, project)), nil)
	if err != nil {
		var apiErr *APIError
		if errorAs(err, &apiErr) && apiErr.NotFound() {
			return false, nil
		}
		return false, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	return true, nil
}

// ListReleases lists the releases of the repository.
func (c *Client) ListReleases(ctx context.Context, owner, project string) ([]Release, error) {
	return paginateGET[Release](ctx, c, func(page int) string {
		return c.apiURL(fmt.Sprintf("/repos/%s/%s/releases?per_page=%d&page=%d", owner, project, pageSize, page))
	})
}

// ListReleaseAssets returns every asset of one release, paginating through
// the dedicated endpoint. The asset array embedded in release objects is
// truncated near the ceiling (storhub-web v18: 980 embedded vs 1000 true),
// so capacity decisions must use this, never len(release.Assets).
func (c *Client) ListReleaseAssets(ctx context.Context, owner, project string, releaseID int64) ([]Asset, error) {
	return paginateGET[Asset](ctx, c, func(page int) string {
		return c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/%d/assets?per_page=%d&page=%d", owner, project, releaseID, pageSize, page))
	})
}

// GetReleaseByTag returns the release with the given tag.
func (c *Client) GetReleaseByTag(ctx context.Context, owner, project, tag string) (*Release, error) {
	var release Release
	if err := c.getJSON(ctx, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/tags/%s", owner, project, url.PathEscape(tag))), &release); err != nil {
		return nil, err
	}
	return &release, nil
}

// CreateRelease creates a release with the given tag and name.
func (c *Client) CreateRelease(ctx context.Context, owner, project, tag, name string) (*Release, error) {
	requestBody := map[string]any{"tag_name": tag, "name": name, "body": "", "draft": false}
	// Release creation is not idempotent; never blind-retry it.
	resp, err := c.doJSONWithRetryable(ctx, http.MethodPost, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases", owner, project)), requestBody, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var release Release
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, fmt.Errorf("decode release response: %w", err)
	}
	return &release, nil
}

// DeleteReleaseByID deletes the release with the given ID.
func (c *Client) DeleteReleaseByID(ctx context.Context, owner, project string, releaseID int64) error {
	return c.deleteByURL(ctx, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/%d", owner, project, releaseID)))
}

// DeleteAssetByID deletes the release asset with the given ID.
func (c *Client) DeleteAssetByID(ctx context.Context, owner, project string, assetID int64) error {
	return c.deleteByURL(ctx, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/assets/%d", owner, project, assetID)))
}

// deleteByURL performs one retryable DELETE with no body. The two
// resource deletes differed only in their URL format string; sharing
// this keeps the retryable-delete profile (read class, default accept,
// no content type) in exactly one place.
func (c *Client) deleteByURL(ctx context.Context, endpoint string) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, endpoint, func() (io.Reader, error) {
		return nil, nil
	}, requestRead, true, "", "", "", 0, false)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// DeleteRepo deletes the owner/project repository.
func (c *Client) DeleteRepo(ctx context.Context, owner, project string) error {
	resp, err := c.doJSONWithRetryable(ctx, http.MethodDelete, c.apiURL(fmt.Sprintf("/repos/%s/%s", owner, project)), nil, true)
	if err != nil {
		// Real GitHub 404s a DELETE for an unknown repo. The delete is
		// retryable, so a lost response after the first delete landed
		// re-sends against a repo that is already gone: gone IS success
		// for an idempotent delete, and reporting failure would wedge
		// DeleteProject on a retry loop against a deleted project.
		var apiErr *APIError
		if errorAs(err, &apiErr) && apiErr.NotFound() {
			return nil
		}
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// FindAssetIDByName returns the asset ID for name in the tagged release.
func (c *Client) FindAssetIDByName(ctx context.Context, owner, project, tag, name string) (int64, error) {
	release, err := c.GetReleaseByTag(ctx, owner, project, tag)
	if err != nil {
		return 0, err
	}
	// Scan the dedicated list-assets endpoint, never the array embedded in
	// the release object: that embed truncates near 1000 assets (see
	// ListReleaseAssets), so a name past the ceiling would be missed.
	assets, err := c.ListReleaseAssets(ctx, owner, project, release.ID)
	if err != nil {
		return 0, err
	}
	for _, asset := range assets {
		if asset.Name == name {
			return asset.ID, nil
		}
	}
	return 0, fmt.Errorf("asset not found by name")
}
