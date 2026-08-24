package github

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/logging"
)

const (
	maxAPIErrorBodyBytes  = 64 << 10
	pageSize              = 100
	defaultRequestTimeout = 5 * time.Minute
	defaultBaseRetryDelay = 500 * time.Millisecond
	defaultMaxRetryDelay  = 8 * time.Second
)

type Client struct {
	token          string
	apiBaseURL     string
	apiVersion     string
	client         *http.Client
	noFollow       *http.Client
	cdn            *http.Client
	maxRetries     int
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
	sleep          func(context.Context, time.Duration) error
	logger         *slog.Logger
	governor       *rateGovernor

	assetMu   sync.Mutex
	assetURLs map[int64]cachedAssetURL
}

type Release struct {
	ID        int64   `json:"id"`
	TagName   string  `json:"tag_name"`
	Name      string  `json:"name"`
	UploadURL string  `json:"upload_url"`
	Draft     bool    `json:"draft"`
	Assets    []Asset `json:"assets"`
}

type Asset struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type Commit struct {
	SHA         string
	Message     string
	CommittedAt time.Time
}

type requestOptions struct { //nolint:revive // internal request options bundle
	stream      bool
	contentType string
	accept      string
	contentSize int64
	rangeHeader string
	retryable   bool
	// assetUpload marks content-generating release-asset POSTs, which
	// draw from the governor's stricter content-creation window.
	assetUpload bool
	// noFollow returns redirect responses to the caller instead of
	// following them, used to capture signed asset CDN URLs.
	noFollow bool
}

type cachedAssetURL struct {
	url     string
	expires time.Time
}

type putContentResponse struct {
	Content struct {
		SHA string `json:"sha"`
	} `json:"content"`
	Commit struct {
		SHA string `json:"sha"`
	} `json:"commit"`
}

type commitResponse struct {
	SHA    string `json:"sha"`
	Commit struct {
		Message string `json:"message"`
		Author  struct {
			Date time.Time `json:"date"`
		} `json:"author"`
	} `json:"commit"`
}

// putFileRequest is the GitHub contents-API create/update payload.
type putFileRequest struct {
	Message string `json:"message"`
	SHA     string `json:"sha,omitempty"`
	Content string `json:"content"` // base64-encoded file bytes
}

func NewClient(token string, cfg storcfg.Config) *Client {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultRequestTimeout}
	}
	sleep := cfg.Sleep
	if sleep == nil {
		sleep = storcfg.SleepWithContext
	}
	baseDelay := cfg.BaseRetryDelay
	if baseDelay <= 0 {
		baseDelay = defaultBaseRetryDelay
	}
	maxDelay := cfg.MaxRetryDelay
	if maxDelay <= 0 {
		maxDelay = defaultMaxRetryDelay
	}
	return &Client{
		token:          token,
		apiBaseURL:     strings.TrimRight(cfg.APIBaseURL, "/"),
		apiVersion:     cfg.APIVersion,
		client:         client,
		maxRetries:     cfg.MaxRetries,
		baseRetryDelay: baseDelay,
		maxRetryDelay:  maxDelay,
		sleep:          sleep,
		logger:         logging.WithComponent(cfg.Logger, "github"),
		governor:       newRateGovernor(cfg, cfg.Logger, sleep),
		assetURLs:      make(map[int64]cachedAssetURL),
		noFollow:       noRedirectClient(client),
		cdn:            bareCDNClient(client),
	}
}

// noRedirectClient returns a copy of client that surfaces redirect
// responses instead of following them.
func noRedirectClient(client *http.Client) *http.Client {
	noFollow := *client
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &noFollow
}

// bareCDNClient returns a copy of client without the request timeout for
// streaming range fetches against signed CDN URLs. No auth header is ever
// attached to these requests; cancellation is context-driven.
func bareCDNClient(client *http.Client) *http.Client {
	cdn := *client
	cdn.Timeout = 0
	return &cdn
}

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

func (c *Client) RepoExists(ctx context.Context, owner, project string) (bool, error) {
	resp, err := c.doJSON(ctx, http.MethodGet, c.apiURL(fmt.Sprintf("/repos/%s/%s", owner, project)), nil)
	if err != nil {
		var apiErr *APIError
		if errorAs(err, &apiErr) && apiErr.NotFound() {
			return false, nil
		}
		return false, err
	}
	_ = resp.Body.Close()
	return true, nil
}

func (c *Client) ListReleases(ctx context.Context, owner, project string) ([]Release, error) {
	releases := make([]Release, 0)
	for page := 1; ; page++ {
		var batch []Release
		endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/releases?per_page=%d&page=%d", owner, project, pageSize, page))
		if err := c.getJSON(ctx, endpoint, &batch); err != nil {
			return nil, err
		}
		releases = append(releases, batch...)
		if len(batch) < pageSize {
			return releases, nil
		}
	}
}

func (c *Client) GetReleaseByTag(ctx context.Context, owner, project, tag string) (*Release, error) {
	var release Release
	if err := c.getJSON(ctx, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/tags/%s", owner, project, url.PathEscape(tag))), &release); err != nil {
		return nil, err
	}
	return &release, nil
}

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

func (c *Client) DeleteReleaseByID(ctx context.Context, owner, project string, releaseID int64) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/%d", owner, project, releaseID)), func() (io.Reader, error) {
		return nil, nil
	}, requestOptions{retryable: true})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

func (c *Client) DeleteAssetByID(ctx context.Context, owner, project string, assetID int64) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/assets/%d", owner, project, assetID)), func() (io.Reader, error) {
		return nil, nil
	}, requestOptions{retryable: true})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

func (c *Client) DeleteRepo(ctx context.Context, owner, project string) error {
	resp, err := c.doJSONWithRetryable(ctx, http.MethodDelete, c.apiURL(fmt.Sprintf("/repos/%s/%s", owner, project)), nil, true)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

func (c *Client) UploadAsset(ctx context.Context, owner, project, releaseTag, uploadURL, assetName string, reader io.ReadSeeker, size int64) (int64, error) {
	cleanURL := strings.Split(uploadURL, "{")[0]
	parsed, err := url.Parse(cleanURL)
	if err != nil {
		return 0, fmt.Errorf("parse upload url: %w", err)
	}
	query := parsed.Query()
	query.Set("name", assetName)
	parsed.RawQuery = query.Encode()
	endpoint := parsed.String()

	started := time.Now().UTC()
	assetID, err := c.uploadAssetAttempt(ctx, endpoint, assetName, reader, size)
	if err == nil {
		logging.Info(c.logger, "upload asset complete", "asset", assetName, "size", size, "elapsed", time.Now().UTC().Sub(started))
		return assetID, nil
	}
	// Fail loudly: silently reusing a pre-existing asset with the same name
	// would hand the caller an unverified ID (possibly stale or partial
	// content) as if the fresh bytes had been stored. Callers that want
	// name-based reuse can compose FindAssetIDByName themselves.
	logging.Warn(c.logger, "upload asset failed", "asset", assetName, "size", size, "elapsed", time.Now().UTC().Sub(started), "err", err)
	return 0, fmt.Errorf("upload asset: %w", err)
}

// DownloadAssetStream fetches [start,end] bytes of an asset. The returned
// size is the server-declared Content-Length and may be -1 when the server
// does not declare a length; readers must therefore rely on their own byte
// accounting rather than on the reported size.
//
// The octet-stream GET is a redirect to a short-lived signed CDN URL. That
// resolution is cached per asset, so reading a large file in many range
// requests costs one core-API call per TTL window instead of one per read;
// range fetches themselves hit the CDN directly without auth headers.
func (c *Client) DownloadAssetStream(ctx context.Context, owner, project string, assetID, start, end int64) (io.ReadCloser, int64, error) {
	rangeHeader := ""
	switch {
	case start >= 0 && end >= start:
		rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
	default:
		// No silent full-asset fallback: an invalid or unset range is a
		// caller bug and must surface as an error.
		return nil, 0, fmt.Errorf("download asset %d: invalid byte range [%d,%d]", assetID, start, end)
	}
	for attempt := 0; attempt < 2; attempt++ {
		cdnURL, cached := c.cachedAssetURL(assetID)
		if cached {
			body, size, status, err := c.fetchCDNRange(ctx, cdnURL.url, rangeHeader)
			if err == nil {
				return body, size, nil
			}
			if !isCDNRejection(status) {
				return nil, 0, err
			}
			// Signed URL expired or revoked: drop it and re-resolve once.
			c.invalidateAssetURL(assetID)
		}
		resp, err := c.doRequest(ctx, http.MethodGet, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/assets/%d", owner, project, assetID)), func() (io.Reader, error) {
			return nil, nil
		}, requestOptions{accept: "application/octet-stream", rangeHeader: rangeHeader, retryable: true, stream: true, noFollow: true})
		if err != nil {
			return nil, 0, fmt.Errorf("download asset: %w", err)
		}
		if resp.StatusCode == http.StatusFound {
			location := resp.Header.Get("Location")
			_ = resp.Body.Close()
			if location == "" {
				return nil, 0, fmt.Errorf("download asset %d: redirect missing location", assetID)
			}
			if resolved, err := resp.Request.URL.Parse(location); err == nil {
				location = resolved.String()
			}
			c.storeAssetURL(assetID, location)
			body, size, status, fetchErr := c.fetchCDNRange(ctx, location, rangeHeader)
			if fetchErr == nil {
				return body, size, nil
			}
			if !isCDNRejection(status) || attempt > 0 {
				return nil, 0, fetchErr
			}
			c.invalidateAssetURL(assetID)
			continue
		}
		// Non-redirect response: treat it as the final answer (also keeps
		// test servers that stream bytes directly working unchanged).
		if resp.StatusCode >= 400 {
			apiErr := decodeAPIError(resp)
			_ = resp.Body.Close()
			return nil, 0, apiErr
		}
		return resp.Body, resp.ContentLength, nil
	}
	return nil, 0, fmt.Errorf("download asset %d: exhausted cdn re-resolution attempts", assetID)
}

// fetchCDNRange performs one unauthenticated range GET against a signed
// CDN URL. The status code is returned alongside so callers can tell
// expired-signature rejections apart from other failures.
func (c *Client) fetchCDNRange(ctx context.Context, url, rangeHeader string) (io.ReadCloser, int64, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("create cdn request: %w", err)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := c.cdn.Do(req)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("cdn request: %w", err)
	}
	if resp.StatusCode >= 400 {
		defer func() { _ = resp.Body.Close() }()
		return nil, 0, resp.StatusCode, fmt.Errorf("cdn range fetch: unexpected status %d", resp.StatusCode)
	}
	return resp.Body, resp.ContentLength, resp.StatusCode, nil
}

// isCDNRejection reports whether a CDN status indicates the signed URL is
// no longer usable and should be re-resolved through the API.
func isCDNRejection(status int) bool {
	return status == http.StatusForbidden || status == http.StatusNotFound ||
		status == http.StatusBadRequest || status == http.StatusGone
}

func (c *Client) cachedAssetURL(assetID int64) (cachedAssetURL, bool) {
	c.assetMu.Lock()
	defer c.assetMu.Unlock()
	cached, ok := c.assetURLs[assetID]
	return cached, ok && c.governor.now().Before(cached.expires)
}

func (c *Client) storeAssetURL(assetID int64, rawURL string) {
	parsed, err := url.Parse(rawURL)
	expires := time.Now().Add(60 * time.Second)
	if err == nil {
		query := parsed.Query()
		if stamped, dateErr := time.Parse("20060102T150405Z", query.Get("X-Amz-Date")); dateErr == nil {
			if lifetime, secErr := strconv.ParseInt(query.Get("X-Amz-Expires"), 10, 64); secErr == nil && lifetime > 0 {
				// Retire the URL before its real expiry with margin.
				expires = stamped.Add(time.Duration(lifetime)*time.Second - 30*time.Second)
			}
		}
	}
	c.assetMu.Lock()
	defer c.assetMu.Unlock()
	c.assetURLs[assetID] = cachedAssetURL{url: rawURL, expires: expires}
}

func (c *Client) invalidateAssetURL(assetID int64) {
	c.assetMu.Lock()
	defer c.assetMu.Unlock()
	delete(c.assetURLs, assetID)
}

func (c *Client) GetFileContent(ctx context.Context, owner, project, filePath, ref string) ([]byte, string, error) {
	endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", owner, project, escapeContentPath(filePath)))
	if ref != "" {
		endpoint += "?ref=" + url.QueryEscape(ref)
	}
	resp, err := c.doRequest(ctx, http.MethodGet, endpoint, func() (io.Reader, error) { return nil, nil }, requestOptions{
		accept:    "application/vnd.github.raw",
		retryable: true,
	})
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read content: %w", err)
	}
	sha := computeGitBlobSHA(data)
	return data, sha, nil
}

func computeGitBlobSHA(data []byte) string {
	header := fmt.Sprintf("blob %d\x00", len(data))
	h := sha1.New()
	h.Write([]byte(header))
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// PutFileContent creates or updates filePath with payload, using previousSHA
// as an optimistic-concurrency precondition (empty means create). It returns
// the commit SHA and the new blob content SHA.
func (c *Client) PutFileContent(ctx context.Context, owner, project, filePath string, payload []byte, previousSHA, message string) (string, string, error) {
	body, err := json.Marshal(putFileRequest{
		Message: message,
		SHA:     previousSHA,
		Content: base64.StdEncoding.EncodeToString(payload),
	})
	if err != nil {
		return "", "", fmt.Errorf("marshal put-file request: %w", err)
	}
	endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", owner, project, escapeContentPath(filePath)))
	resp, err := c.doRequest(ctx, http.MethodPut, endpoint, func() (io.Reader, error) {
		return bytes.NewReader(body), nil
	}, requestOptions{
		contentType: "application/json",
		accept:      "application/vnd.github+json",
		contentSize: int64(len(body)),
		retryable:   true,
	})
	if err != nil {
		return "", "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var result putContentResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", fmt.Errorf("decode content response: %w", err)
	}
	return result.Commit.SHA, result.Content.SHA, nil
}

func (c *Client) ListFileCommits(ctx context.Context, owner, project, filePath string) ([]Commit, error) {
	commits := make([]Commit, 0)
	escapedPath := url.QueryEscape(filePath)
	for page := 1; ; page++ {
		var batch []commitResponse
		endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/commits?path=%s&per_page=%d&page=%d", owner, project, escapedPath, pageSize, page))
		if err := c.getJSON(ctx, endpoint, &batch); err != nil {
			return nil, err
		}
		for _, item := range batch {
			commits = append(commits, Commit{SHA: item.SHA, Message: item.Commit.Message, CommittedAt: item.Commit.Author.Date.UTC()})
		}
		if len(batch) < pageSize {
			return commits, nil
		}
	}
}

func (c *Client) FindAssetIDByName(ctx context.Context, owner, project, tag, name string) (int64, error) {
	release, err := c.GetReleaseByTag(ctx, owner, project, tag)
	if err != nil {
		return 0, err
	}
	for _, asset := range release.Assets {
		if asset.Name == name {
			return asset.ID, nil
		}
	}
	return 0, fmt.Errorf("asset not found by name")
}

func (c *Client) doJSON(ctx context.Context, method, endpoint string, body any) (*http.Response, error) {
	return c.doJSONWithRetryable(ctx, method, endpoint, body, isRetrySafeMethod(method))
}

func (c *Client) doJSONWithRetryable(ctx context.Context, method, endpoint string, body any, retryable bool) (*http.Response, error) {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
	}
	return c.doRequest(ctx, method, endpoint, func() (io.Reader, error) {
		if payload == nil {
			return nil, nil
		}
		return bytes.NewReader(payload), nil
	}, requestOptions{contentType: "application/json", accept: "application/vnd.github+json", contentSize: int64(len(payload)), retryable: retryable})
}

func (c *Client) getJSON(ctx context.Context, endpoint string, out any) error {
	resp, err := c.doJSON(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) doRequest(ctx context.Context, method, endpoint string, bodyFactory func() (io.Reader, error), opts requestOptions) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		started := time.Now().UTC()
		logging.Debug(c.logger, "http request start", "method", method, "url", endpoint, "attempt", attempt+1, "retryable", opts.retryable)
		release, err := c.governor.acquire(ctx, methodCost(method), opts.assetUpload)
		if err != nil {
			return nil, err
		}
		reader, err := bodyFactory()
		if err != nil {
			release()
			return nil, err
		}
		if opts.contentSize == 0 && reader != nil {
			reader = http.NoBody
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
		if err != nil {
			release()
			return nil, fmt.Errorf("create request: %w", err)
		}
		c.applyHeaders(req, opts)
		if opts.contentSize >= 0 {
			req.ContentLength = opts.contentSize
		}
		client := c.client
		switch {
		case opts.noFollow:
			client = c.noFollow
		case opts.stream && client.Timeout > 0:
			// Streams must not be killed by the client-wide timeout; the
			// request context governs cancellation instead.
			noTimeout := *client
			noTimeout.Timeout = 0
			client = &noTimeout
		}
		resp, err := client.Do(req)
		release()
		if err == nil {
			c.governor.observe(resp.Header)
			apiErr := decodeAPIError(resp)
			if apiErr == nil {
				logging.Debug(c.logger, "http request complete", "method", method, "url", endpoint, "attempt", attempt+1, "status", resp.StatusCode, "elapsed", time.Now().UTC().Sub(started))
				return resp, nil
			}
			_ = resp.Body.Close()
			lastErr = apiErr
			// Endpoints without rate-limit headers (uploads) still get an
			// accurate reset time from the governor's last snapshot.
			if apiErr.RateLimited && apiErr.RateLimitReset.IsZero() {
				if snap := c.governor.snapshot(); snap.seen && c.governor.now().Before(snap.resetAt) {
					apiErr.RateLimitReset = snap.resetAt
					if snap.remaining <= c.governor.cfg.reserve {
						apiErr.Primary = true
					}
				}
			}
			logging.Warn(c.logger, "http request api error", "method", method, "url", endpoint, "attempt", attempt+1, "status", apiErr.StatusCode, "elapsed", time.Now().UTC().Sub(started), "retryable", apiErr.IsRetryable(), "rate_limited", apiErr.RateLimited, "primary", apiErr.Primary, "retry_after", apiErr.RetryAfter, "rate_reset", apiErr.RateLimitReset, "err", apiErr)
			if attempt == c.maxRetries || !opts.retryable || !apiErr.IsRetryable() {
				return nil, apiErr
			}
			delay := c.retryDelay(attempt, apiErr)
			// Primary exhaustion is not a backoff situation: the budget
			// is gone until reset. Waiting longer than maxWait allows is
			// refused up front instead of pretending an 8s retry helps.
			if apiErr.RateLimited && delay > c.governor.cfg.maxWait {
				return nil, apiErr
			}
			logging.Warn(c.logger, "http retry sleep", "method", method, "url", endpoint, "attempt", attempt+1, "delay", delay)
			if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}
		lastErr = fmt.Errorf("perform request: %w", err)
		logging.Warn(c.logger, "http request transport error", "method", method, "url", endpoint, "attempt", attempt+1, "elapsed", time.Now().UTC().Sub(started), "retryable", isRetryableNetworkError(err), "err", err)
		if attempt == c.maxRetries || !opts.retryable || !isRetryableNetworkError(err) {
			return nil, lastErr
		}
		delay := c.retryDelay(attempt, nil)
		logging.Warn(c.logger, "http retry sleep", "method", method, "url", endpoint, "attempt", attempt+1, "delay", delay)
		if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
			return nil, sleepErr
		}
	}
	return nil, lastErr
}

func (c *Client) uploadAssetAttempt(ctx context.Context, endpoint, assetName string, reader io.ReadSeeker, size int64) (int64, error) {
	resp, err := c.doRequest(ctx, http.MethodPost, endpoint, func() (io.Reader, error) {
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind upload reader: %w", err)
		}
		return reader, nil
		// Retries are safe: a release-asset upload is one atomic PUT - no
		// asset exists unless the full body lands - and the bodyFactory
		// rewinds the reader for every attempt. A lost response after
		// finalize can leave an orphan under this attempt's random name;
		// unreferenced assets are exactly what PurgeUntracked removes.
	}, requestOptions{contentType: "application/octet-stream", accept: "application/vnd.github+json", contentSize: size, retryable: true, assetUpload: true})
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	var asset struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&asset); err != nil {
		return 0, fmt.Errorf("decode asset response: %w", err)
	}
	if asset.ID == 0 {
		return 0, fmt.Errorf("asset response missing id for %s", assetName)
	}
	return asset.ID, nil
}

func (c *Client) applyHeaders(req *http.Request, opts requestOptions) {
	accept := opts.accept
	if accept == "" {
		accept = "application/vnd.github+json"
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-GitHub-Api-Version", c.apiVersion)
	if opts.contentType != "" {
		req.Header.Set("Content-Type", opts.contentType)
	}
	if opts.rangeHeader != "" {
		req.Header.Set("Range", opts.rangeHeader)
	}
}

// rateLimitMarkers are substrings GitHub uses when rejecting requests on
// rate-limit grounds. The API endpoint advertises this via headers, but
// the upload endpoint (uploads.github.com) sends none at all - the body
// is the only signal there.
var rateLimitMarkers = []string{
	"api rate limit exceeded",
	"secondary rate limit",
	"abuse detection mechanism",
}

func decodeAPIError(resp *http.Response) *APIError {
	if resp.StatusCode < http.StatusBadRequest {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxAPIErrorBodyBytes))
	var payload struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(body, &payload)
	err := &APIError{StatusCode: resp.StatusCode, Message: payload.Message, Body: string(body), Headers: resp.Header.Clone()}
	if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); retryAfter > 0 {
		err.RetryAfter = retryAfter
	}
	marker := containsAny(strings.ToLower(payload.Message+" "+string(body)), rateLimitMarkers)
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		err.RateLimited = true
		if reset, ok := parseUnixTime(resp.Header.Get("X-RateLimit-Reset")); ok {
			err.RateLimitReset = reset
			err.Primary = true
		}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		err.RateLimited = true
	}
	// Header-less rejections (uploads.github.com): the body text is the
	// only evidence of rate limiting. These are not provably primary -
	// the honest classification is secondary-style pacing.
	if !err.RateLimited && marker {
		err.RateLimited = true
	}
	return err
}

func containsAny(haystack string, needles []string) bool {
	for _, needle := range needles {
		if strings.Contains(haystack, needle) {
			return true
		}
	}
	return false
}

// retryDelay computes the wait before the next attempt. Rate-limit
// rejections follow GitHub's documented guidance instead of the generic
// exponential backoff: primary exhaustion means waiting for
// x-ratelimit-reset (the doRequest caller refuses waits beyond maxWait),
// a present Retry-After is honored exactly, and other secondary limits
// wait at least one minute with exponential growth.
func (c *Client) retryDelay(attempt int, apiErr *APIError) time.Duration {
	if apiErr != nil && apiErr.RateLimited {
		if !apiErr.RateLimitReset.IsZero() {
			// Wait for the documented reset; the floor only prevents a
			// hot loop when the clock has already passed it.
			return maxDuration(time.Until(apiErr.RateLimitReset)+c.baseRetryDelay, c.baseRetryDelay)
		}
		if apiErr.RetryAfter > 0 {
			return apiErr.RetryAfter
		}
		return minDuration(60*time.Second<<attempt, 15*time.Minute)
	}
	if apiErr != nil && apiErr.RetryAfter > 0 {
		return c.boundedWait(nonNegativeDelay(apiErr.RetryAfter))
	}
	base := float64(c.baseRetryDelay)
	delay := time.Duration(base * math.Pow(2, float64(attempt)))
	if delay > c.maxRetryDelay {
		delay = c.maxRetryDelay
	}
	if delay <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int63n(int64(delay/4 + 1)))
	return delay + jitter
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (c *Client) apiURL(path string) string {
	return c.apiBaseURL + path
}

func isRetrySafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func nonNegativeDelay(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	return delay
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if strings.TrimSpace(value) == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		return when.Sub(now)
	}
	return 0
}

func parseUnixTime(value string) (time.Time, bool) {
	seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}

func isRetryableNetworkError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errorAs(err, &netErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF)
}

func errorAs(err error, target any) bool {
	return err != nil && errors.As(err, target)
}

// boundedWait caps server-provided wait hints (Retry-After, rate-limit
// reset) at maxRetryDelay so a hostile or misconfigured header cannot stall
// callers invisibly for minutes.
func (c *Client) boundedWait(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	if c.maxRetryDelay > 0 && d > c.maxRetryDelay {
		return c.maxRetryDelay
	}
	return d
}

// escapeContentPath URL-escapes each path segment; raw '#', '?' or '%'
// characters in filenames would otherwise truncate or rewrite the request.
func escapeContentPath(filePath string) string {
	cleaned := path.Clean(strings.TrimLeft(filePath, "/"))
	segments := strings.Split(cleaned, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return strings.Join(segments, "/")
}
