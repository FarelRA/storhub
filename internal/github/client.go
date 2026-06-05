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
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/internal/logging"
)

const (
	maxAPIErrorBodyBytes = 64 << 10
	pageSize             = 100
)

type Client struct {
	token          string
	apiBaseURL     string
	apiVersion     string
	client         *http.Client
	maxRetries     int
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
	sleep          func(context.Context, time.Duration) error
	logger         *slog.Logger
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

type requestOptions struct {
	contentType string
	accept      string
	contentSize int64
	rangeHeader string
	retryable   bool
}

type content struct {
	SHA         string `json:"sha"`
	Content     string `json:"content"`
	Encoding    string `json:"encoding"`
	DownloadURL string `json:"download_url"`
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

func NewClient(token string, cfg storcfg.Config) *Client {
	return &Client{
		token:          token,
		apiBaseURL:     strings.TrimRight(cfg.APIBaseURL, "/"),
		apiVersion:     cfg.APIVersion,
		client:         cfg.HTTPClient,
		maxRetries:     cfg.MaxRetries,
		baseRetryDelay: cfg.BaseRetryDelay,
		maxRetryDelay:  cfg.MaxRetryDelay,
		sleep:          cfg.Sleep,
		logger:         logging.WithComponent(cfg.Logger, "github"),
	}
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
	resp, err := c.doJSONWithRetryable(ctx, http.MethodPost, c.apiURL("/user/repos"), body, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
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
	resp.Body.Close()
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
	if err := c.getJSON(ctx, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/tags/%s", owner, project, tag)), &release); err != nil {
		return nil, err
	}
	return &release, nil
}

func (c *Client) CreateRelease(ctx context.Context, owner, project, tag, name string) (*Release, error) {
	requestBody := map[string]any{"tag_name": tag, "name": name, "body": "", "draft": false}
	resp, err := c.doJSONWithRetryable(ctx, http.MethodPost, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases", owner, project)), requestBody, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var release Release
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, fmt.Errorf("decode release response: %w", err)
	}
	return &release, nil
}

func (c *Client) DeleteReleaseByID(ctx context.Context, owner, project string, releaseID int64) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/%d", owner, project, releaseID)), func() (io.Reader, error) {
		return nil, nil
	}, requestOptions{retryable: false})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *Client) DeleteAssetByID(ctx context.Context, owner, project string, assetID int64) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/assets/%d", owner, project, assetID)), func() (io.Reader, error) {
		return nil, nil
	}, requestOptions{retryable: false})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *Client) DeleteRepo(ctx context.Context, owner, project string) error {
	resp, err := c.doJSON(ctx, http.MethodDelete, c.apiURL(fmt.Sprintf("/repos/%s/%s", owner, project)), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
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
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		started := time.Now().UTC()
		assetID, err := c.uploadAssetAttempt(ctx, endpoint, assetName, reader, size)
		if err == nil {
			logging.Info(c.logger, "upload asset complete", "asset", assetName, "size", size, "attempt", attempt+1, "elapsed", time.Now().UTC().Sub(started))
			return assetID, nil
		}
		logging.Warn(c.logger, "upload asset attempt failed", "asset", assetName, "size", size, "attempt", attempt+1, "elapsed", time.Now().UTC().Sub(started), "err", err)
		if existingID, resolveErr := c.FindAssetIDByName(ctx, owner, project, releaseTag, assetName); resolveErr == nil && existingID != 0 {
			logging.Warn(c.logger, "upload asset reused existing asset after failure", "asset", assetName, "asset_id", existingID, "attempt", attempt+1)
			return existingID, nil
		}
		var apiErr *APIError
		if !(errorAs(err, &apiErr) && apiErr.IsRetryable()) && !isRetryableNetworkError(err) {
			return 0, fmt.Errorf("upload asset: %w", err)
		}
		if attempt == c.maxRetries {
			return 0, fmt.Errorf("upload asset: %w", err)
		}
		delay := c.retryDelay(attempt, apiErr)
		logging.Warn(c.logger, "upload asset retry sleep", "asset", assetName, "attempt", attempt+1, "delay", delay, "err", err)
		if sleepErr := c.sleep(ctx, delay); sleepErr != nil {
			return 0, sleepErr
		}
	}
	return 0, fmt.Errorf("upload asset: exhausted retries")
}

func (c *Client) DownloadAssetStream(ctx context.Context, owner, project string, assetID, start, end int64) (io.ReadCloser, int64, error) {
	rangeHeader := ""
	if start >= 0 && end >= start {
		rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
	}
	resp, err := c.doRequest(ctx, http.MethodGet, c.apiURL(fmt.Sprintf("/repos/%s/%s/releases/assets/%d", owner, project, assetID)), func() (io.Reader, error) {
		return nil, nil
	}, requestOptions{accept: "application/octet-stream", rangeHeader: rangeHeader, retryable: true})
	if err != nil {
		return nil, 0, fmt.Errorf("download asset: %w", err)
	}
	return resp.Body, resp.ContentLength, nil
}

func (c *Client) GetFileContent(ctx context.Context, owner, project, filePath, ref string) ([]byte, string, error) {
	endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", owner, project, path.Clean(filePath)))
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
	defer resp.Body.Close()
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

func (c *Client) PutFileContent(ctx context.Context, owner, project, filePath string, payload []byte, previousSHA, message string) (string, string, error) {
	msgJSON, err := json.Marshal(message)
	if err != nil {
		return "", "", fmt.Errorf("marshal message: %w", err)
	}
	var buf bytes.Buffer
	buf.WriteString(`{"message":`)
	buf.Write(msgJSON)
	if previousSHA != "" {
		shaJSON, _ := json.Marshal(previousSHA)
		buf.WriteString(`,"sha":`)
		buf.Write(shaJSON)
	}
	buf.WriteString(`,"content":"`)
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	if _, err := encoder.Write(payload); err != nil {
		return "", "", fmt.Errorf("base64 encode payload: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return "", "", fmt.Errorf("close base64 encoder: %w", err)
	}
	buf.WriteString(`"}`)
	endpoint := c.apiURL(fmt.Sprintf("/repos/%s/%s/contents/%s", owner, project, path.Clean(filePath)))
	resp, err := c.doRequest(ctx, http.MethodPut, endpoint, func() (io.Reader, error) {
		return bytes.NewReader(buf.Bytes()), nil
	}, requestOptions{
		contentType: "application/json",
		accept:      "application/vnd.github+json",
		contentSize: int64(buf.Len()),
		retryable:   true,
	})
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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
		reader, err := bodyFactory()
		if err != nil {
			return nil, err
		}
		if opts.contentSize == 0 && reader != nil {
			reader = http.NoBody
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		c.applyHeaders(req, opts)
		if opts.contentSize >= 0 {
			req.ContentLength = opts.contentSize
		}
		resp, err := c.client.Do(req)
		if err == nil {
			apiErr := decodeAPIError(resp)
			if apiErr == nil {
				logging.Debug(c.logger, "http request complete", "method", method, "url", endpoint, "attempt", attempt+1, "status", resp.StatusCode, "elapsed", time.Now().UTC().Sub(started))
				return resp, nil
			}
			resp.Body.Close()
			lastErr = apiErr
			logging.Warn(c.logger, "http request api error", "method", method, "url", endpoint, "attempt", attempt+1, "status", apiErr.StatusCode, "elapsed", time.Now().UTC().Sub(started), "retryable", apiErr.IsRetryable(), "rate_limited", apiErr.RateLimited, "retry_after", apiErr.RetryAfter, "rate_reset", apiErr.RateLimitReset, "err", apiErr)
			if attempt == c.maxRetries || !opts.retryable || !apiErr.IsRetryable() {
				return nil, apiErr
			}
			delay := c.retryDelay(attempt, apiErr)
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
	}, requestOptions{contentType: "application/octet-stream", accept: "application/vnd.github+json", contentSize: size, retryable: false})
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
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
	if resp.Header.Get("X-RateLimit-Remaining") == "0" {
		err.RateLimited = true
		if reset, ok := parseUnixTime(resp.Header.Get("X-RateLimit-Reset")); ok {
			err.RateLimitReset = reset
		}
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		err.RateLimited = true
	}
	return err
}

func (c *Client) retryDelay(attempt int, apiErr *APIError) time.Duration {
	if apiErr != nil {
		if apiErr.RetryAfter > 0 {
			return nonNegativeDelay(apiErr.RetryAfter)
		}
		if apiErr.RateLimited && !apiErr.RateLimitReset.IsZero() {
			return nonNegativeDelay(time.Until(apiErr.RateLimitReset))
		}
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
	var netErr net.Error
	if errorAs(err, &netErr) {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF)
}

func errorAs(err error, target any) bool {
	return err != nil && errors.As(err, target)
}
